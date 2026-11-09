"""
Autor: Isacc Cabrera
Descripción: Job bronze → silver de una hora UTC. Lee los station_status_*.json.gz de
    bronze/station_status/dt=…/hour=…, los aplana (una fila por estación por snapshot), aplica la
    cadena de calidad y escribe silver/station_status/…/part-0.parquet (válidas, ordenadas por
    station_id y last_reported) y silver/quarantine/…/part-0.parquet (con "motivo"). La capacidad
    sale de la station_information vigente EN ESA HORA (la versión más reciente con last_updated
    <= fin de la hora; si no hay, la más antigua): así reprocesar una hora vieja siempre da lo
    mismo aunque el feed cambie después. También reconstruye
    silver/station_information/station_information.parquet desde la versión más reciente.
    Nota: Parquet guarda los timestamp con resolución mínima de ms; al leer silver se pasa
    schema=SILVER_STATUS_SCHEMA para recuperar timestamp[s, UTC].
    CLI: --hour YYYY-MM-DDTHH|auto, --check-idempotent, --backfill, --print-auto-hour, --json.
Patrón: Template Method (BronzeToSilverJob implementa extract/validate de TransformJob) +
    Chain of Responsibility (radar.lake.quality)
Fecha de creación: 2026-11-09
Cambios:
    - 2026-11-09 · Isacc Cabrera · Creación (paso 7).
"""

import argparse
import gzip
import json
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa

from radar.collector.hashing import stations_hash
from radar.config import Settings, get_settings
from radar.lake.job import MANIFEST, JobOutput, TransformJob
from radar.lake.quality import (
    COUNT_COLUMNS,
    FLAG_COLUMNS,
    OPTIONAL_FLAGS,
    QUARANTINE_COLUMNS,
    build_chain,
    flatten_snapshot,
    raw_frame,
)
from radar.logging import configure_logging, get_logger

log = get_logger(__name__)

AUTO_DELAY = timedelta(minutes=10)
HOUR_FMT = "%Y-%m-%dT%H"

SILVER_STATUS_SCHEMA = pa.schema(
    [
        ("station_id", pa.string()),
        ("snapshot_last_updated", pa.timestamp("s", tz="UTC")),
        ("last_reported", pa.timestamp("s", tz="UTC")),
        ("num_bikes_available", pa.int16()),
        ("num_bikes_disabled", pa.int16()),
        ("num_docks_available", pa.int16()),
        ("num_docks_disabled", pa.int16()),
        ("capacity", pa.int16()),
        ("is_installed", pa.bool_()),
        ("is_renting", pa.bool_()),
        ("is_returning", pa.bool_()),
        ("is_charging", pa.bool_()),
    ]
)

SILVER_STATION_INFO_SCHEMA = pa.schema(
    [
        ("station_id", pa.string()),
        ("short_name", pa.string()),
        ("short_name_norm", pa.string()),
        ("name", pa.string()),
        ("lat", pa.float64()),
        ("lon", pa.float64()),
        ("capacity", pa.int16()),
        ("info_hash", pa.string()),
    ]
)

QUARANTINE_SCHEMA = pa.schema([(c, pa.string()) for c in QUARANTINE_COLUMNS])


def normalize_short_name(code: str | None) -> str | None:
    """Normaliza un código de estación para emparejar con los viajes históricos.

    Args:
        code: Código como ``"085"`` o ``"266-267"``.

    Returns:
        Sin ceros a la izquierda si es solo dígitos (``"085"`` → ``"85"``); igual si no.
    """
    if code is None:
        return None
    code = code.strip()
    return str(int(code)) if code.isdigit() else code


def parse_hour(value: str) -> datetime:
    """``YYYY-MM-DDTHH`` → inicio de la hora en UTC."""
    return datetime.strptime(value, HOUR_FMT).replace(tzinfo=UTC)


def auto_hour(now: datetime | None = None) -> datetime:
    """La hora más reciente que terminó hace al menos 10 minutos (nunca la hora en curso).

    Args:
        now: Hora actual (inyectable).

    Returns:
        Inicio de esa hora en UTC.
    """
    ref = (now or datetime.now(UTC)) - AUTO_DELAY
    return ref.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)


def _read_gz_json(path: Path) -> dict[str, Any]:
    data: dict[str, Any] = json.loads(gzip.decompress(path.read_bytes()))
    return data


@dataclass
class HourInput:
    """Entrada de una hora: snapshots bronze y capacidades vigentes."""

    payloads: list[dict[str, Any]]
    capacities: dict[str, int]


class BronzeToSilverJob(TransformJob):
    """Transforma una hora de bronze en silver + cuarentena.

    Args:
        settings: Configuración con ``data_dir``.
    """

    def __init__(self, settings: Settings) -> None:
        self.data_dir = settings.data_dir
        self._infos: list[tuple[int, dict[str, Any]]] | None = None

    # --- rutas -------------------------------------------------------------------------------
    def _part(self, base: str, hour: datetime) -> Path:
        return (
            self.data_dir / base / f"dt={hour:%Y-%m-%d}" / f"hour={hour:%H}"
        )  # misma partición en bronze y silver

    def bronze_files(self, hour: datetime) -> list[Path]:
        """Snapshots bronze de la hora (sin temporales), en orden de nombre."""
        return sorted(self._part("bronze/station_status", hour).glob("station_status_*.json.gz"))

    def bronze_hours(self) -> list[datetime]:
        """Todas las horas que tienen al menos un snapshot bronze."""
        root = self.data_dir / "bronze" / "station_status"
        hours = []
        for d in root.glob("dt=*/hour=*"):
            if any(d.glob("station_status_*.json.gz")):
                hours.append(
                    datetime.strptime(f"{d.parent.name[3:]}T{d.name[5:]}", HOUR_FMT).replace(
                        tzinfo=UTC
                    )
                )
        return sorted(hours)

    def manifest_path(self, hour: datetime) -> Path:
        """Ruta del ``_manifest.json`` silver de la hora."""
        return self._part("silver/station_status", hour) / MANIFEST

    # --- station_information -----------------------------------------------------------------
    def _information_versions(self) -> list[tuple[int, dict[str, Any]]]:
        if self._infos is None:
            root = self.data_dir / "bronze" / "station_information"
            versions = []
            for path in root.glob("dt=*/station_information_*.json.gz"):
                payload = _read_gz_json(path)
                versions.append((int(payload["last_updated"]), payload))
            self._infos = sorted(versions, key=lambda v: v[0])
        return self._infos

    def information_at(self, hour: datetime) -> dict[str, Any]:
        """Versión de station_information vigente al final de ``hour``.

        Raises:
            FileNotFoundError: Si no hay ninguna versión en bronze.
        """
        versions = self._information_versions()
        if not versions:
            raise FileNotFoundError("no hay station_information en bronze")
        end = int((hour + timedelta(hours=1)).timestamp())
        before = [p for lu, p in versions if lu <= end]
        return before[-1] if before else versions[0][1]

    def information_table(self) -> pa.Table:
        """station_information más reciente como tabla silver."""
        payload = self._information_versions()[-1][1]
        info_hash = stations_hash(payload)
        stations = sorted(payload["data"]["stations"], key=lambda s: str(s["station_id"]))
        rows = {
            "station_id": [str(s["station_id"]) for s in stations],
            "short_name": [s.get("short_name") for s in stations],
            "short_name_norm": [normalize_short_name(s.get("short_name")) for s in stations],
            "name": [s.get("name") for s in stations],
            "lat": [float(s["lat"]) for s in stations],
            "lon": [float(s["lon"]) for s in stations],
            "capacity": [int(s["capacity"]) for s in stations],
            "info_hash": [info_hash] * len(stations),
        }
        return pa.Table.from_pydict(rows, schema=SILVER_STATION_INFO_SCHEMA)

    # --- Template Method ---------------------------------------------------------------------
    def extract(self, partition: str) -> HourInput:
        """Lee los snapshots de la hora y las capacidades vigentes."""
        hour = parse_hour(partition)
        payloads = [_read_gz_json(p) for p in self.bronze_files(hour)]
        info = self.information_at(hour)
        capacities = {str(s["station_id"]): int(s["capacity"]) for s in info["data"]["stations"]}
        return HourInput(payloads, capacities)

    def validate(self, partition: str, data: HourInput) -> JobOutput:
        """Aplana, corre la cadena de calidad y arma tablas + manifiesto."""
        hour = parse_hour(partition)
        rows = [row for p in data.payloads for row in flatten_snapshot(p)]
        first, capacity, dedup = build_chain(data.capacities)
        valid, quarantine = first.handle(raw_frame(rows))
        valid_table = _silver_table(valid)
        q = quarantine.sort(*QUARANTINE_COLUMNS, nulls_last=True)
        quarantine_table = pa.Table.from_pydict(
            {c: q[c].to_list() for c in QUARANTINE_COLUMNS}, schema=QUARANTINE_SCHEMA
        )
        status_dir = self._part("silver/station_status", hour)
        manifest = {
            "hora": partition,
            "archivos_bronze": len(data.payloads),
            "filas_leidas": len(rows),
            "filas_validas": valid_table.num_rows,
            "filas_cuarentena": quarantine_table.num_rows,
            "filas_duplicadas": dedup.duplicadas,
            "tasa_regla_capacidad": capacity.tasa,
        }
        return JobOutput(
            tables={
                status_dir: valid_table,
                self._part("silver/quarantine", hour): quarantine_table,
            },
            manifest=manifest,
            manifest_dir=status_dir,
            sha_of=status_dir,
            sha_key="sha256_validas",
            extra_files={
                self.data_dir
                / "silver"
                / "station_information"
                / "station_information.parquet": self.information_table()
            },
        )


def _silver_table(valid: pl.DataFrame) -> pa.Table:
    """Filas válidas tipadas → tabla con ``SILVER_STATUS_SCHEMA``, ordenada."""
    cols = [c.name for c in SILVER_STATUS_SCHEMA]
    ordered = valid.sort("station_id", "last_reported").select(cols)
    data = {
        "station_id": ordered["station_id"].to_list(),
        **{c: ordered[c].to_list() for c in ("snapshot_last_updated", "last_reported")},
        **{c: ordered[c].to_list() for c in (*COUNT_COLUMNS, "capacity")},
        **{c: ordered[c].to_list() for c in (*FLAG_COLUMNS, *OPTIONAL_FLAGS)},
    }
    arrays = []
    for field in SILVER_STATUS_SCHEMA:
        values = data[field.name]
        if pa.types.is_timestamp(field.type):
            arrays.append(pa.array(values, type=pa.int64()).cast(field.type))
        else:
            arrays.append(pa.array(values, type=field.type))
    return pa.Table.from_arrays(arrays, schema=SILVER_STATUS_SCHEMA)


def _check_manifest(manifest: dict[str, Any]) -> bool:
    tasa = manifest["tasa_regla_capacidad"]
    return manifest["filas_validas"] > 0 and 0.0 <= tasa <= 1.0


def main(argv: list[str] | None = None) -> int:
    """CLI ``python -m radar.lake.bronze_to_silver``.

    Args:
        argv: Argumentos (None = ``sys.argv``).

    Returns:
        0 ok; 2 uso; 3 check fallido (manifiestos distintos, 0 filas válidas o tasa
        fuera de [0, 1]).
    """
    parser = argparse.ArgumentParser(description="Bronze → silver de una hora UTC.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--hour", metavar="YYYY-MM-DDTHH|auto")
    group.add_argument("--backfill", action="store_true")
    group.add_argument("--print-auto-hour", action="store_true")
    parser.add_argument("--check-idempotent", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    settings = get_settings()
    configure_logging(settings.log_level)

    if args.print_auto_hour:
        print(auto_hour().strftime(HOUR_FMT))
        return 0
    job = BronzeToSilverJob(settings)

    if args.backfill:
        limit = auto_hour()
        pending = [
            h for h in job.bronze_hours() if h <= limit and not job.manifest_path(h).exists()
        ]
        manifests = [job.run(h.strftime(HOUR_FMT)) for h in pending]
        for m in manifests:
            log.info("silver_hora_procesada", **m)
        out: dict[str, Any] = {"horas": [m["hora"] for m in manifests], "manifiestos": manifests}
        if args.json:
            print(json.dumps(out, sort_keys=True))
        return 0

    try:
        hour = auto_hour() if args.hour == "auto" else parse_hour(args.hour)
    except ValueError:
        parser.error("--hour debe ser YYYY-MM-DDTHH o auto")
    partition = hour.strftime(HOUR_FMT)
    if args.check_idempotent:
        same, first, second = job.check_idempotent(partition)
        ok = same and _check_manifest(second)
        out = {"hora": partition, "idempotente": same, "manifiesto": second, "ok": ok}
    else:
        manifest = job.run(partition)
        ok = manifest["filas_validas"] > 0
        out = {"hora": partition, "manifiesto": manifest, "ok": ok}
    log.info("silver_hora_procesada", hora=partition, ok=ok)
    if args.json:
        print(json.dumps(out, sort_keys=True))
    return 0 if ok else 3


if __name__ == "__main__":
    sys.exit(main())
