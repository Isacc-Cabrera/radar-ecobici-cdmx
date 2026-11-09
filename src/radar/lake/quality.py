"""
Autor: Isacc Cabrera
Descripción: Cadena de calidad de silver. Cada eslabón recibe un DataFrame, separa las filas que
    no pasan (van a cuarentena con su motivo) y entrega el resto al siguiente. Orden fijo:
    SchemaCheck (campos presentes y enteros >= 0; además tipa las columnas) → KnownStationCheck
    (station_id existe en station_information; agrega capacity) → CapacityRuleCheck (bicis +
    bicis dañadas + muelles + muelles dañados == capacity) → DedupCheck (una fila por
    (station_id, last_reported); los duplicados se cuentan, NO van a cuarentena: son minutos sin
    cambios). La entrada es "bronze aplanado": todas las columnas como texto, para que un tipo
    raro llegue a SchemaCheck en vez de romper la lectura.
Patrón: Chain of Responsibility (QualityCheck.set_next / handle)
Fecha de creación: 2026-11-09
Cambios:
    - 2026-11-09 · Isacc Cabrera · Creación (paso 7).
"""

import json
from abc import ABC, abstractmethod
from typing import Any

import polars as pl

COUNT_COLUMNS = (
    "num_bikes_available",
    "num_bikes_disabled",
    "num_docks_available",
    "num_docks_disabled",
)
FLAG_COLUMNS = ("is_installed", "is_renting", "is_returning")
OPTIONAL_FLAGS = ("is_charging",)
INT_COLUMNS = ("snapshot_last_updated", "last_reported", *COUNT_COLUMNS)
RAW_COLUMNS = ("station_id", *INT_COLUMNS, *FLAG_COLUMNS, *OPTIONAL_FLAGS)
QUARANTINE_COLUMNS = (*RAW_COLUMNS, "capacity", "motivo")
QUARANTINE_SCHEMA = dict.fromkeys(QUARANTINE_COLUMNS, pl.String)

_INT_RE = r"^\d+$"
_TRUE = ("1", "true")
_FALSE = ("0", "false")


def to_text(value: Any) -> str | None:
    """Convierte un valor del JSON a texto sin perder su tipo original.

    Args:
        value: Valor tal como vino del feed.

    Returns:
        None si falta; ``"true"``/``"false"`` para bool; ``str`` para int y texto; JSON para lo
        demás (un float ``7.0`` queda ``"7.0"`` y SchemaCheck lo rechaza por no ser entero).
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | str):
        return str(value)
    return json.dumps(value)


def flatten_snapshot(payload: dict[str, Any]) -> list[dict[str, str | None]]:
    """Una fila de texto por estación, con ``snapshot_last_updated`` del snapshot.

    Args:
        payload: JSON de ``station_status``.

    Returns:
        Filas con exactamente ``RAW_COLUMNS``.
    """
    lu = to_text(payload.get("last_updated"))
    rows = []
    for station in payload.get("data", {}).get("stations", []):
        row = {c: to_text(station.get(c)) for c in RAW_COLUMNS}
        row["snapshot_last_updated"] = lu
        rows.append(row)
    return rows


def raw_frame(rows: list[dict[str, str | None]]) -> pl.DataFrame:
    """DataFrame de texto con las columnas de bronze aplanado."""
    return pl.DataFrame(rows, schema=dict.fromkeys(RAW_COLUMNS, pl.String), orient="row")


def empty_quarantine() -> pl.DataFrame:
    """Cuarentena vacía con su esquema fijo (todo texto)."""
    return pl.DataFrame(schema=QUARANTINE_SCHEMA)


def to_quarantine(df: pl.DataFrame, motivo: str) -> pl.DataFrame:
    """Lleva filas rechazadas al esquema de cuarentena (texto + ``motivo``).

    Args:
        df: Filas rechazadas (tipadas o no).
        motivo: ``esquema`` | ``estacion_desconocida`` | ``regla_capacidad``.

    Returns:
        DataFrame con ``QUARANTINE_COLUMNS``.
    """
    cols = []
    for c in QUARANTINE_COLUMNS[:-1]:
        if c not in df.columns:
            cols.append(pl.lit(None, dtype=pl.String).alias(c))
        elif df.schema[c] == pl.Boolean:
            expr = pl.when(pl.col(c)).then(pl.lit("true")).otherwise(pl.lit("false"))
            cols.append(pl.when(pl.col(c).is_null()).then(None).otherwise(expr).alias(c))
        else:
            cols.append(pl.col(c).cast(pl.String).alias(c))
    return df.select(*cols, pl.lit(motivo).alias("motivo"))


class QualityCheck(ABC):
    """Eslabón de la cadena: separa lo que falla y pasa el resto al siguiente."""

    motivo: str = ""

    def __init__(self) -> None:
        self._next: QualityCheck | None = None

    def set_next(self, check: "QualityCheck") -> "QualityCheck":
        """Encadena ``check`` después de este eslabón.

        Args:
            check: Siguiente eslabón.

        Returns:
            ``check``, para encadenar ``a.set_next(b).set_next(c)``.
        """
        self._next = check
        return check

    @abstractmethod
    def check(self, df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Regresa ``(pasan, fallan)`` para este eslabón."""

    def handle(self, df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Aplica este eslabón y los siguientes.

        Args:
            df: Filas que llegan a este eslabón.

        Returns:
            ``(validas, cuarentena)`` acumuladas de aquí al final de la cadena.
        """
        good, bad = self.check(df)
        quarantine = to_quarantine(bad, self.motivo) if bad.height else empty_quarantine()
        if self._next is None:
            return good, quarantine
        valid, rest = self._next.handle(good)
        return valid, pl.concat([quarantine, rest], how="vertical")


class SchemaCheck(QualityCheck):
    """Campos requeridos presentes, enteros >= 0 y banderas 0/1/true/false; además tipa."""

    motivo = "esquema"

    def check(self, df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Ver ``QualityCheck``."""
        lower = {c: pl.col(c).str.to_lowercase() for c in (*FLAG_COLUMNS, *OPTIONAL_FLAGS)}
        ok = pl.col("station_id").is_not_null() & (pl.col("station_id").str.len_chars() > 0)
        for c in INT_COLUMNS:
            ok = ok & pl.col(c).is_not_null() & pl.col(c).str.contains(_INT_RE)
        for c in FLAG_COLUMNS:
            ok = ok & lower[c].is_in([*_TRUE, *_FALSE])
        for c in OPTIONAL_FLAGS:
            ok = ok & (pl.col(c).is_null() | lower[c].is_in([*_TRUE, *_FALSE]))
        flagged = df.with_columns(ok.fill_null(False).alias("_ok"))
        good = flagged.filter(pl.col("_ok")).drop("_ok")
        bad = flagged.filter(~pl.col("_ok")).drop("_ok")
        typed = good.with_columns(
            *(pl.col(c).cast(pl.Int64) for c in INT_COLUMNS),
            *(lower[c].is_in(list(_TRUE)) for c in FLAG_COLUMNS),
            *(
                pl.when(pl.col(c).is_null())
                .then(None)
                .otherwise(lower[c].is_in(list(_TRUE)))
                .alias(c)
                for c in OPTIONAL_FLAGS
            ),
        )
        return typed, bad


class KnownStationCheck(QualityCheck):
    """``station_id`` debe existir en la station_information vigente; agrega ``capacity``.

    Args:
        capacities: ``{station_id: capacity}`` de la station_information vigente.
    """

    motivo = "estacion_desconocida"

    def __init__(self, capacities: dict[str, int]) -> None:
        super().__init__()
        self._capacities = pl.DataFrame(
            {"station_id": list(capacities), "capacity": list(capacities.values())},
            schema={"station_id": pl.String, "capacity": pl.Int64},
        )

    def check(self, df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Ver ``QualityCheck``."""
        known = pl.col("station_id").is_in(self._capacities["station_id"].implode())
        good = df.filter(known).join(self._capacities, on="station_id", how="left")
        return good, df.filter(~known)


class CapacityRuleCheck(QualityCheck):
    """bicis + bicis dañadas + muelles + muelles dañados == capacity.

    Attributes:
        evaluadas: Filas que llegaron a la regla.
        cumplen: Filas que la cumplen.
    """

    motivo = "regla_capacidad"

    def __init__(self) -> None:
        super().__init__()
        self.evaluadas = 0
        self.cumplen = 0

    def check(self, df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Ver ``QualityCheck``."""
        total = pl.sum_horizontal(*(pl.col(c) for c in COUNT_COLUMNS))
        ok = total == pl.col("capacity")
        good, bad = df.filter(ok), df.filter(~ok)
        self.evaluadas += df.height
        self.cumplen += good.height
        return good, bad

    @property
    def tasa(self) -> float:
        """Proporción que cumple (1.0 si no se evaluó nada)."""
        return round(self.cumplen / self.evaluadas, 6) if self.evaluadas else 1.0


class DedupCheck(QualityCheck):
    """Una fila por ``(station_id, last_reported)``; la primera por snapshot.

    Attributes:
        duplicadas: Filas descartadas por repetidas (no van a cuarentena).
    """

    motivo = "duplicado"

    def __init__(self) -> None:
        super().__init__()
        self.duplicadas = 0

    def check(self, df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Ver ``QualityCheck``. Los duplicados se descartan y cuentan, no se rechazan."""
        ordered = df.sort("snapshot_last_updated", "station_id", "last_reported")
        unique = ordered.unique(
            subset=["station_id", "last_reported"], keep="first", maintain_order=True
        )
        self.duplicadas += df.height - unique.height
        return unique, df.clear()


def build_chain(
    capacities: dict[str, int],
) -> tuple[QualityCheck, CapacityRuleCheck, DedupCheck]:
    """Arma la cadena en su orden fijo.

    Args:
        capacities: ``{station_id: capacity}`` vigente.

    Returns:
        ``(primer eslabón, CapacityRuleCheck, DedupCheck)``; los dos últimos para leer sus
        contadores al final.
    """
    capacity = CapacityRuleCheck()
    dedup = DedupCheck()
    first = SchemaCheck()
    first.set_next(KnownStationCheck(capacities)).set_next(capacity).set_next(dedup)
    return first, capacity, dedup
