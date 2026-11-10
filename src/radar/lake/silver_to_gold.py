"""
Autor: Isacc Cabrera
Descripción: Job silver → gold de un día UTC y reporte SQL analítico.
    gold/features_base: rejilla de 5 min por estación (solo en las horas que tienen silver), con
    el último estado observado hacia atrás (join "as-of"): silver guarda un renglón por CAMBIO de
    estado, así que el valor vigente a las 10:05 es el último reporte con snapshot <= 10:05.
    gold/station_hour: agregados por estación y hora calculados SOBRE la rejilla (media, mín y máx
    de bicis, media de muelles, % del tiempo vacía y llena), así una estación que no cambió en
    toda la hora pesa igual que una que cambió 30 veces; n_obs = reportes distintos en silver.
    Manifiesto sin marcas de tiempo en gold/station_hour/dt=…/_manifest.json.
    --sql-report ejecuta en DuckDB cada consulta de un .sql (separadas por "-- name: <nombre>")
    sobre vistas read_parquet del lake y escribe un Markdown con el SQL y las primeras 20 filas.
    CLI: --day YYYY-MM-DD|auto (el día de la hora "auto" del paso 7), --check-idempotent, --json,
    --sql-report <archivo.sql> --out <archivo.md>.
Patrón: Template Method (SilverToGoldJob implementa extract/validate de TransformJob)
Fecha de creación: 2026-11-09
Cambios:
    - 2026-11-09 · Isacc Cabrera · Creación (paso 8).
"""

import argparse
import json
import re
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from radar.config import Settings, get_settings
from radar.lake.bronze_to_silver import SILVER_STATUS_SCHEMA, auto_hour
from radar.lake.job import MANIFEST, JobOutput, TransformJob
from radar.logging import configure_logging, get_logger

log = get_logger(__name__)

GRID = timedelta(minutes=5)
DAY_FMT = "%Y-%m-%d"
_HOUR_DIR = re.compile(r"^hour=(\d{2})$")  # deja fuera hour=HH.tmp / .old
_NAME_RE = re.compile(r"^--\s*name:\s*(\S+)\s*$", re.MULTILINE)
REPORT_ROWS = 20

STATION_HOUR_SCHEMA = pa.schema(
    [
        ("station_id", pa.string()),
        ("hora_utc", pa.timestamp("s", tz="UTC")),
        ("bikes_mean", pa.float64()),
        ("bikes_min", pa.int16()),
        ("bikes_max", pa.int16()),
        ("docks_mean", pa.float64()),
        ("n_obs", pa.int32()),
        ("pct_vacia", pa.float64()),
        ("pct_llena", pa.float64()),
    ]
)

FEATURES_BASE_SCHEMA = pa.schema(
    [
        ("station_id", pa.string()),
        ("ts_utc", pa.timestamp("s", tz="UTC")),
        ("bikes_available", pa.int16()),
        ("docks_available", pa.int16()),
        ("capacity", pa.int16()),
        ("is_renting", pa.bool_()),
    ]
)

_TS = pl.Datetime("us", "UTC")


def parse_day(value: str) -> date:
    """``YYYY-MM-DD`` → fecha."""
    return datetime.strptime(value, DAY_FMT).date()


def auto_day(now: datetime | None = None) -> date:
    """Día UTC de la hora ``auto`` (la última hora cerrada hace >= 10 min)."""
    return auto_hour(now).date()


def to_arrow(df: pl.DataFrame, schema: pa.Schema) -> pa.Table:
    """DataFrame de polars → tabla Arrow con el esquema exacto (tipos y orden de columnas).

    Args:
        df: Datos con al menos las columnas de ``schema``.
        schema: Esquema destino.

    Returns:
        Tabla casteada a ``schema``.
    """
    table = df.select(schema.names).to_arrow(compat_level=pl.CompatLevel.oldest())
    return table.cast(schema)


def build_features_base(obs: pl.DataFrame, hours: list[datetime]) -> pl.DataFrame:
    """Rejilla de 5 min por estación con el último estado observado.

    Args:
        obs: Silver del día (``station_id``, ``snapshot_last_updated``, conteos, ``capacity``,
            ``is_renting``).
        hours: Inicio de cada hora que tiene silver; la rejilla solo cubre esas horas.

    Returns:
        Columnas de ``FEATURES_BASE_SCHEMA``, ordenadas por ``station_id`` y ``ts_utc``. Un punto
        sin ningún reporte previo en el día se descarta (no se inventa).
    """
    if obs.is_empty() or not hours:
        return pl.DataFrame(schema={n: _polars_type(n) for n in FEATURES_BASE_SCHEMA.names})
    steps = int(timedelta(hours=1) / GRID)
    ts = pl.Series("ts_utc", [h + GRID * k for h in hours for k in range(steps)], dtype=_TS)
    stations = obs.select(pl.col("station_id").unique().sort())
    grid = stations.join(ts.to_frame(), how="cross").sort("ts_utc", "station_id")
    right = (
        obs.with_columns(pl.col("snapshot_last_updated").cast(_TS))
        .sort("snapshot_last_updated", "station_id")
        .select(
            "station_id",
            "snapshot_last_updated",
            pl.col("num_bikes_available").alias("bikes_available"),
            pl.col("num_docks_available").alias("docks_available"),
            "capacity",
            "is_renting",
        )
    )
    joined = grid.join_asof(
        right,
        left_on="ts_utc",
        right_on="snapshot_last_updated",
        by="station_id",
        strategy="backward",
        check_sortedness=False,
    )
    return (
        joined.filter(pl.col("bikes_available").is_not_null())
        .select(FEATURES_BASE_SCHEMA.names)
        .sort("station_id", "ts_utc")
    )


def build_station_hour(base: pl.DataFrame, obs: pl.DataFrame) -> pl.DataFrame:
    """Agregados por estación y hora sobre la rejilla.

    Args:
        base: Salida de ``build_features_base``.
        obs: Silver del día (para ``n_obs``).

    Returns:
        Columnas de ``STATION_HOUR_SCHEMA``, ordenadas por ``station_id`` y ``hora_utc``.
    """
    if base.is_empty():
        return pl.DataFrame(schema={n: _polars_type(n) for n in STATION_HOUR_SCHEMA.names})
    agg = (
        base.with_columns(pl.col("ts_utc").dt.truncate("1h").alias("hora_utc"))
        .group_by("station_id", "hora_utc")
        .agg(
            pl.col("bikes_available").mean().round(4).alias("bikes_mean"),
            pl.col("bikes_available").min().alias("bikes_min"),
            pl.col("bikes_available").max().alias("bikes_max"),
            pl.col("docks_available").mean().round(4).alias("docks_mean"),
            (pl.col("bikes_available") == 0).mean().round(6).alias("pct_vacia"),
            (pl.col("docks_available") == 0).mean().round(6).alias("pct_llena"),
        )
    )
    counts = (
        obs.with_columns(pl.col("snapshot_last_updated").cast(_TS).dt.truncate("1h"))
        .group_by("station_id", pl.col("snapshot_last_updated").alias("hora_utc"))
        .len("n_obs")
    )
    return (
        agg.join(counts, on=["station_id", "hora_utc"], how="left")
        .with_columns(pl.col("n_obs").fill_null(0))
        .select(STATION_HOUR_SCHEMA.names)
        .sort("station_id", "hora_utc")
    )


def _polars_type(name: str) -> pl.DataType:
    types: dict[str, pl.DataType] = {
        "station_id": pl.String(),
        "ts_utc": _TS,
        "hora_utc": _TS,
        "is_renting": pl.Boolean(),
        "bikes_mean": pl.Float64(),
        "docks_mean": pl.Float64(),
        "pct_vacia": pl.Float64(),
        "pct_llena": pl.Float64(),
    }
    return types.get(name, pl.Int64())


class SilverToGoldJob(TransformJob):
    """Transforma un día UTC de silver en gold/station_hour + gold/features_base.

    Args:
        settings: Configuración con ``data_dir``.
    """

    def __init__(self, settings: Settings) -> None:
        self.data_dir = settings.data_dir

    def _gold_dir(self, table: str, day: date) -> Path:
        return self.data_dir / "gold" / table / f"dt={day:%Y-%m-%d}"

    def manifest_path(self, day: date) -> Path:
        """Ruta del ``_manifest.json`` gold del día."""
        return self._gold_dir("station_hour", day) / MANIFEST

    def silver_hours(self, day: date) -> dict[datetime, Path]:
        """Horas del día con silver terminado (``part-0.parquet``), sin temporales.

        Returns:
            ``{inicio de la hora UTC: archivo}`` en orden.
        """
        root = self.data_dir / "silver" / "station_status" / f"dt={day:%Y-%m-%d}"
        found = {}
        for d in root.glob("hour=*"):
            m = _HOUR_DIR.match(d.name)
            part = d / "part-0.parquet"
            if m and part.exists():
                start = datetime(day.year, day.month, day.day, int(m.group(1)), tzinfo=UTC)
                found[start] = part
        return dict(sorted(found.items()))

    def extract(self, partition: str) -> tuple[list[datetime], pl.DataFrame]:
        """Lee todas las horas silver del día."""
        hours = self.silver_hours(parse_day(partition))
        tables = [pq.read_table(p, schema=SILVER_STATUS_SCHEMA) for p in hours.values()]
        joined = pa.concat_tables(tables) if tables else SILVER_STATUS_SCHEMA.empty_table()
        obs = pl.from_arrow(joined)
        assert isinstance(obs, pl.DataFrame)
        return list(hours), obs

    def validate(self, partition: str, data: tuple[list[datetime], pl.DataFrame]) -> JobOutput:
        """Arma la rejilla, los agregados y el manifiesto."""
        day = parse_day(partition)
        hours, obs = data
        base = build_features_base(obs, hours)
        station_hour = build_station_hour(base, obs)
        sh_dir = self._gold_dir("station_hour", day)
        manifest = {
            "dia": partition,
            "horas_silver": len(hours),
            "filas_station_hour": station_hour.height,
            "filas_features_base": base.height,
        }
        return JobOutput(
            tables={
                sh_dir: to_arrow(station_hour, STATION_HOUR_SCHEMA),
                self._gold_dir("features_base", day): to_arrow(base, FEATURES_BASE_SCHEMA),
            },
            manifest=manifest,
            manifest_dir=sh_dir,
            sha_of=sh_dir,
            sha_key="sha256_station_hour",
        )


# --- Reporte SQL -----------------------------------------------------------------------------
def split_queries(text: str) -> list[tuple[str, str]]:
    """Separa un archivo SQL por los comentarios ``-- name: <nombre>``.

    Args:
        text: Contenido del ``.sql``; lo que está antes del primer ``-- name:`` (encabezado) se
            ignora.

    Returns:
        ``[(nombre, sql)]`` en el orden del archivo, sin el ``;`` final.

    Raises:
        ValueError: Si no hay ninguna consulta con nombre o alguna está vacía.
    """
    marks = list(_NAME_RE.finditer(text))
    if not marks:
        raise ValueError("el archivo no tiene consultas '-- name: <nombre>'")
    queries = []
    for i, m in enumerate(marks):
        end = marks[i + 1].start() if i + 1 < len(marks) else len(text)
        sql = text[m.end() : end].strip().rstrip(";").strip()
        if not sql:
            raise ValueError(f"la consulta {m.group(1)!r} está vacía")
        queries.append((m.group(1), sql))
    return queries


def lake_views(data_dir: Path) -> dict[str, str]:
    """Vistas de DuckDB sobre el lake: ``{nombre: patrón read_parquet}``.

    Solo incluye las que tienen al menos un archivo, para que una consulta sobre una capa
    inexistente falle con "tabla no existe" en vez de un error de glob.
    """
    patterns = {
        "silver_status": "silver/station_status/dt=*/hour=*/part-0.parquet",
        "quarantine": "silver/quarantine/dt=*/hour=*/part-0.parquet",
        "station_information": "silver/station_information/station_information.parquet",
        "silver_trips": "silver/trips/month=*/part-0.parquet",
        "gold_station_hour": "gold/station_hour/dt=*/part-0.parquet",
        "gold_features_base": "gold/features_base/dt=*/part-0.parquet",
        "trip_demand": "gold/trip_demand/trip_demand.parquet",
    }
    return {
        name: (data_dir / pattern).as_posix()
        for name, pattern in patterns.items()
        if any(p for p in data_dir.glob(pattern) if ".tmp" not in p.as_posix())
    }


def _cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        value = round(value, 4)
    return str(value).replace("|", "\\|")


def run_sql_report(data_dir: Path, sql_text: str) -> tuple[str, list[dict[str, Any]]]:
    """Ejecuta cada consulta y arma el Markdown.

    Args:
        data_dir: Raíz del lake.
        sql_text: Contenido del archivo ``.sql``.

    Returns:
        ``(markdown, [{"nombre", "filas"}])``.

    Raises:
        duckdb.Error: Si alguna consulta falla.
    """
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    views = lake_views(data_dir)
    for name, pattern in views.items():
        con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{pattern}')")
    lines = [
        "# Reporte SQL analítico",
        "",
        "Generado con `uv run python -m radar.lake.silver_to_gold --sql-report sql/analitica.sql"
        " --out docs/reportes/analitica.md` (DuckDB sobre el Parquet del lake, horas en UTC salvo"
        " que la columna diga `local`).",
        "",
        f"Vistas disponibles: {', '.join(f'`{v}`' for v in views)}.",
        "",
    ]
    summary = []
    for name, sql in split_queries(sql_text):
        # Vía Arrow: fetchall() con TIMESTAMPTZ exige pytz, que no es dependencia del proyecto.
        table = con.sql(sql).to_arrow_table()
        columns = table.column_names
        rows = [tuple(r.values()) for r in table.to_pylist()]
        summary.append({"nombre": name, "filas": len(rows)})
        lines += [f"## {name}", "", "```sql", sql, "```", ""]
        lines.append(f"{len(rows)} filas" + (f"; primeras {REPORT_ROWS}:" if rows else "."))
        lines.append("")
        if rows:
            lines.append("| " + " | ".join(columns) + " |")
            lines.append("|" + "---|" * len(columns))
            for row in rows[:REPORT_ROWS]:
                lines.append("| " + " | ".join(_cell(v) for v in row) + " |")
            lines.append("")
    con.close()
    return "\n".join(lines), summary


def main(argv: list[str] | None = None) -> int:
    """CLI ``python -m radar.lake.silver_to_gold``.

    Args:
        argv: Argumentos (None = ``sys.argv``).

    Returns:
        0 ok; 2 uso; 3 check fallido (manifiestos distintos, ``filas_station_hour`` = 0 o una
        consulta SQL que falla).
    """
    parser = argparse.ArgumentParser(description="Silver → gold de un día UTC y reporte SQL.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--day", metavar="YYYY-MM-DD|auto")
    group.add_argument("--sql-report", type=Path, metavar="ARCHIVO.sql")
    parser.add_argument("--out", type=Path, metavar="ARCHIVO.md")
    parser.add_argument("--check-idempotent", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    if args.sql_report and not args.out:
        parser.error("--sql-report requiere --out")
    settings = get_settings()
    configure_logging(settings.log_level)

    if args.sql_report:
        try:
            markdown, summary = run_sql_report(
                settings.data_dir, args.sql_report.read_text(encoding="utf-8")
            )
        except (duckdb.Error, ValueError) as exc:
            log.error("reporte_sql_fallo", error=str(exc))
            return 3
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(markdown + "\n", encoding="utf-8")
        log.info("reporte_sql_escrito", archivo=str(args.out), consultas=len(summary))
        if args.json:
            print(json.dumps({"consultas": summary, "archivo": str(args.out)}, sort_keys=True))
        return 0

    try:
        day = auto_day() if args.day == "auto" else parse_day(args.day)
    except ValueError:
        parser.error("--day debe ser YYYY-MM-DD o auto")
    partition = day.strftime(DAY_FMT)
    job = SilverToGoldJob(settings)
    if args.check_idempotent:
        same, _, manifest = job.check_idempotent(partition)
        ok = same and manifest["filas_station_hour"] > 0
        out: dict[str, Any] = {"dia": partition, "idempotente": same, "manifiesto": manifest}
    else:
        manifest = job.run(partition)
        ok = manifest["filas_station_hour"] > 0
        out = {"dia": partition, "manifiesto": manifest}
    out["ok"] = ok
    log.info("gold_dia_procesado", dia=partition, ok=ok)
    if args.json:
        print(json.dumps(out, sort_keys=True))
    return 0 if ok else 3


if __name__ == "__main__":
    sys.exit(main())
