"""
Autor: Isacc Cabrera
Descripción: Viajes históricos de ECOBICI (CSV mensual de datos abiertos) en el lake.
    --probe: pide solo los primeros 2 KB (Range) de TRIPS_CSV_URL y revisa el encabezado; si no
    coincide busca el CSV más reciente en la página de datos abiertos y lo sugiere (no toca .env).
    --download: copia el CSV a data/bronze/trips/ (único lugar donde viven los datos personales;
    se omite si ya existe con el mismo tamaño; bronze nunca se reescribe).
    --to-silver: LISTA BLANCA de 6 columnas (estación y fecha/hora de retiro y arribo); género,
    edad y bici nunca se leen. Fechas dd/mm/yyyy HH:MM:SS en America/Mexico_City → UTC. Lee el
    archivo como utf-8-sig y si falla como latin-1. Partición silver/trips/month=YYYY-MM (el mes
    del archivo, así reprocesar un mes reemplaza solo ese mes).
    --demand: gold/trip_demand/trip_demand.parquet con salidas y llegadas promedio por
    short_name_norm, día de la semana (0 = lunes) y hora local, y tasa_match = % de los extremos
    de viaje cuyo código existe en station_information.short_name_norm.
    --benchmark: la misma agregación en DuckDB y en pandas, cada motor en su propio subproceso
    (3 corridas), sobre el Parquet de silver y sobre el CSV de bronze; mide segundos de reloj y
    memoria pico y escribe docs/reportes/benchmark_duckdb_pandas.md.
Patrón: Template Method (TripsToSilverJob implementa extract/validate de TransformJob)
Fecha de creación: 2026-11-09
Cambios:
    - 2026-11-09 · Isacc Cabrera · Creación (paso 8).
"""

import argparse
import io
import json
import os
import platform
import re
import statistics
import subprocess
import sys
import time
from collections import Counter
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from radar.config import Settings, get_settings
from radar.lake.bronze_to_silver import normalize_short_name
from radar.lake.job import JobOutput, TransformJob, write_parquet
from radar.logging import configure_logging, get_logger

log = get_logger(__name__)

EXPECTED_HEADER = (
    "Genero_Usuario,Edad_Usuario,Bici,Ciclo_Estacion_Retiro,Fecha_Retiro,Hora_Retiro,"
    "Ciclo_EstacionArribo,Fecha_Arribo,Hora_Arribo"
)
WHITELIST = (
    "Ciclo_Estacion_Retiro",
    "Fecha_Retiro",
    "Hora_Retiro",
    "Ciclo_EstacionArribo",
    "Fecha_Arribo",
    "Hora_Arribo",
)
PII_COLUMNS = ("Genero_Usuario", "Edad_Usuario", "Bici")
OPEN_DATA_URL = "https://ecobici.cdmx.gob.mx/datos-abiertos/"
LOCAL_TZ = "America/Mexico_City"
DT_FMT = "%d/%m/%Y %H:%M:%S"
PROBE_BYTES = 2048
BENCH_RUNS = 3
BENCH_REPORT = Path("docs/reportes/benchmark_duckdb_pandas.md")
_MONTH_RE = re.compile(r"(\d{4})-(\d{2})")
_CSV_HREF = re.compile(r"href=[\"']([^\"']+\.csv)[\"']", re.IGNORECASE)

SILVER_TRIPS_SCHEMA = pa.schema(
    [
        ("estacion_retiro", pa.string()),
        ("estacion_retiro_norm", pa.string()),
        ("retiro_utc", pa.timestamp("s", tz="UTC")),
        ("estacion_arribo", pa.string()),
        ("estacion_arribo_norm", pa.string()),
        ("arribo_utc", pa.timestamp("s", tz="UTC")),
    ]
)

TRIP_DEMAND_SCHEMA = pa.schema(
    [
        ("short_name_norm", pa.string()),
        ("dow", pa.int8()),
        ("hour_local", pa.int8()),
        ("salidas_prom", pa.float64()),
        ("llegadas_prom", pa.float64()),
        ("tasa_match", pa.float64()),
    ]
)


# --- Fuente: probe, descubrimiento y descarga -----------------------------------------------
def http_client(settings: Settings) -> httpx.Client:
    """Cliente HTTP con el timeout del proyecto y un User-Agent genérico."""
    return httpx.Client(
        timeout=settings.http_timeout_seconds,
        follow_redirects=True,
        headers={"User-Agent": "radar-ecobici/0.1"},
    )


def header_line(chunk: bytes) -> str:
    """Primera línea de un trozo del CSV, sin BOM ni fin de línea."""
    text = chunk.decode("utf-8-sig", errors="replace")
    return text.splitlines()[0].strip() if text else ""


def probe(url: str, client: httpx.Client) -> tuple[bool, str]:
    """Descarga solo los primeros bytes del CSV y compara el encabezado.

    Args:
        url: URL del CSV mensual.
        client: Cliente HTTP.

    Returns:
        ``(encabezado correcto, encabezado leído)``.

    Raises:
        httpx.HTTPError: Si la petición falla o responde con error.
    """
    with client.stream("GET", url, headers={"Range": f"bytes=0-{PROBE_BYTES - 1}"}) as resp:
        resp.raise_for_status()
        chunk = b""
        for part in resp.iter_bytes():  # aunque el servidor ignore Range, solo leemos 2 KB
            chunk += part
            if len(chunk) >= PROBE_BYTES:
                break
    line = header_line(chunk)
    return line == EXPECTED_HEADER, line


def latest_csv_url(html: str, base: str = OPEN_DATA_URL) -> str | None:
    """El enlace ``.csv`` más reciente de la página de datos abiertos (mayor ``AAAA-MM``).

    Args:
        html: HTML de la página.
        base: URL de la página, para resolver enlaces relativos.

    Returns:
        URL absoluta o None si no hay enlaces con ``AAAA-MM`` en el nombre.
    """
    dated = []
    for href in _CSV_HREF.findall(html):
        m = _MONTH_RE.search(Path(urlparse(href).path).name)
        if m:
            dated.append((m.group(0), urljoin(base, href)))
    return max(dated)[1] if dated else None


def file_name(url: str) -> str:
    """Nombre del archivo al final de la URL."""
    return Path(urlparse(url).path).name


def download(url: str, dest_dir: Path, client: httpx.Client) -> tuple[str, Path, int]:
    """Copia el CSV a ``dest_dir`` de forma atómica.

    Args:
        url: URL del CSV.
        dest_dir: ``data/bronze/trips``.
        client: Cliente HTTP.

    Returns:
        ``(estado, ruta, bytes)`` con estado ``descargado`` u ``omitido`` (ya existe con el mismo
        tamaño que anuncia el servidor).

    Raises:
        FileExistsError: Si el archivo existe con OTRO tamaño (bronze no se reescribe).
        httpx.HTTPError: Si la descarga falla.
    """
    dest = dest_dir / file_name(url)
    with client.stream("GET", url) as resp:
        resp.raise_for_status()
        size = int(resp.headers.get("Content-Length", "-1"))
        if dest.exists():
            if dest.stat().st_size == size:
                return "omitido", dest, size
            raise FileExistsError(f"{dest} existe con otro tamaño; bronze no se reescribe")
        dest_dir.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(dest.name + ".tmp")
        with tmp.open("wb") as fh:
            for part in resp.iter_bytes(1 << 20):
                fh.write(part)
            fh.flush()
            os.fsync(fh.fileno())
    os.replace(tmp, dest)
    return "descargado", dest, dest.stat().st_size


# --- Silver ----------------------------------------------------------------------------------
def decode_csv(raw: bytes) -> tuple[str, str]:
    """Texto del CSV: ``utf-8-sig`` y, si falla, ``latin-1``.

    Returns:
        ``(texto, codificación usada)``.
    """
    try:
        return raw.decode("utf-8-sig"), "utf-8-sig"
    except UnicodeDecodeError:
        return raw.decode("latin-1"), "latin-1"


def month_of(name: str) -> str:
    """``AAAA-MM`` del nombre del archivo (``public_data_web_2026-10.csv`` → ``2026-10``).

    Raises:
        ValueError: Si el nombre no trae el mes.
    """
    m = _MONTH_RE.search(name)
    if not m:
        raise ValueError(f"el nombre {name!r} no trae AAAA-MM")
    return m.group(0)


def _local_to_utc(fecha: str, hora: str) -> pl.Expr:
    text = pl.col(fecha).str.strip_chars() + " " + pl.col(hora).str.strip_chars()
    return (
        text.str.strptime(pl.Datetime("us"), DT_FMT, strict=False)
        .dt.replace_time_zone(LOCAL_TZ, ambiguous="earliest", non_existent="null")
        .dt.convert_time_zone("UTC")
    )


def _norm(col: str) -> pl.Expr:
    return pl.col(col).map_elements(normalize_short_name, return_dtype=pl.String)


def parse_trips(text: str) -> tuple[pl.DataFrame, int]:
    """CSV de viajes → filas silver (solo columnas de la lista blanca).

    Args:
        text: Contenido del CSV ya decodificado.

    Returns:
        ``(viajes válidos ordenados, filas leídas)``. Una fila sin fecha/hora interpretable o sin
        código de estación se descarta (se cuenta en el manifiesto).

    Raises:
        ValueError: Si al CSV le falta alguna columna de la lista blanca.
    """
    try:
        raw = pl.read_csv(
            io.BytesIO(text.encode("utf-8")), columns=list(WHITELIST), infer_schema=False
        )
    except pl.exceptions.ColumnNotFoundError as exc:
        raise ValueError(f"al CSV le falta una columna de la lista blanca: {exc}") from exc
    df = raw.select(
        pl.col("Ciclo_Estacion_Retiro").str.strip_chars().alias("estacion_retiro"),
        _local_to_utc("Fecha_Retiro", "Hora_Retiro").alias("retiro_utc"),
        pl.col("Ciclo_EstacionArribo").str.strip_chars().alias("estacion_arribo"),
        _local_to_utc("Fecha_Arribo", "Hora_Arribo").alias("arribo_utc"),
    )
    ok = (
        pl.col("retiro_utc").is_not_null()
        & pl.col("arribo_utc").is_not_null()
        & (pl.col("estacion_retiro").str.len_chars() > 0)
        & (pl.col("estacion_arribo").str.len_chars() > 0)
    )
    valid = (
        df.filter(ok.fill_null(False))
        .with_columns(
            _norm("estacion_retiro").alias("estacion_retiro_norm"),
            _norm("estacion_arribo").alias("estacion_arribo_norm"),
        )
        .select(SILVER_TRIPS_SCHEMA.names)
        .sort("retiro_utc", "estacion_retiro", "arribo_utc", "estacion_arribo")
    )
    return valid, raw.height


def _to_arrow(df: pl.DataFrame, schema: pa.Schema) -> pa.Table:
    return df.select(schema.names).to_arrow(compat_level=pl.CompatLevel.oldest()).cast(schema)


class TripsToSilverJob(TransformJob):
    """Un CSV de bronze/trips → silver/trips/month=AAAA-MM (sin datos personales).

    Args:
        settings: Configuración con ``data_dir``.
    """

    def __init__(self, settings: Settings) -> None:
        self.data_dir = settings.data_dir

    def bronze_csvs(self) -> list[Path]:
        """CSV completos en ``data/bronze/trips`` (sin temporales)."""
        return sorted((self.data_dir / "bronze" / "trips").glob("*.csv"))

    def extract(self, partition: str) -> tuple[str, str]:
        """Lee y decodifica el CSV ``partition`` (nombre del archivo)."""
        return decode_csv((self.data_dir / "bronze" / "trips" / partition).read_bytes())

    def validate(self, partition: str, data: tuple[str, str]) -> JobOutput:
        """Aplica la lista blanca, convierte a UTC y arma el manifiesto."""
        text, encoding = data
        valid, leidas = parse_trips(text)
        month = month_of(partition)
        out_dir = self.data_dir / "silver" / "trips" / f"month={month}"
        manifest = {
            "archivo": partition,
            "mes": month,
            "codificacion": encoding,
            "filas_leidas": leidas,
            "filas_validas": valid.height,
            "filas_descartadas": leidas - valid.height,
        }
        return JobOutput(
            tables={out_dir: _to_arrow(valid, SILVER_TRIPS_SCHEMA)},
            manifest=manifest,
            manifest_dir=out_dir,
            sha_of=out_dir,
            sha_key="sha256_viajes",
        )


# --- Gold: demanda ---------------------------------------------------------------------------
def match_stats(codes: pl.Series, known: set[str]) -> tuple[float, list[str]]:
    """Qué tanto se emparejan los códigos de los viajes con ``station_information``.

    Args:
        codes: Un código normalizado por extremo de viaje (retiro y arribo).
        known: ``short_name_norm`` de station_information.

    Returns:
        ``(tasa de extremos emparejados, códigos sin emparejar ordenados)``. Los compuestos como
        ``"266-267"`` se conservan tal cual: emparejan solo si así existen en el feed.
    """
    if codes.is_empty():
        return 0.0, []
    matched = codes.is_in(sorted(known))
    tasa = round(int(matched.sum()) / codes.len(), 6)
    return tasa, sorted(codes.filter(~matched).unique().to_list())


def month_dates(months: list[str]) -> list[date]:
    """Todas las fechas de los meses ``AAAA-MM`` dados, en orden."""
    out = []
    for month in sorted(set(months)):
        day = date(int(month[:4]), int(month[5:7]), 1)
        while day.strftime("%Y-%m") == month:
            out.append(day)
            day += timedelta(days=1)
    return out


def build_demand(
    trips: pl.DataFrame, known: set[str], months: list[str]
) -> tuple[pl.DataFrame, dict[str, Any]]:
    """Salidas y llegadas promedio por estación, día de la semana y hora local.

    Args:
        trips: Silver de viajes (todas las particiones).
        known: ``short_name_norm`` de station_information.
        months: Meses ``AAAA-MM`` cubiertos por silver/trips (uno por archivo).

    Returns:
        ``(tabla con TRIP_DEMAND_SCHEMA, resumen)``. Una salida cuenta si su fecha local cae
        dentro de esos meses (igual una llegada): el CSV de octubre trae viajes que salieron en
        septiembre o antes y llegaron en octubre. El promedio divide entre cuántas fechas de ese
        día de la semana tienen los meses (no entre días con viajes).
    """
    dates = month_dates(months)
    n_days = Counter(d.weekday() for d in dates)
    dias = pl.DataFrame(
        {"dow": list(n_days), "n_dias": list(n_days.values())},
        schema={"dow": pl.Int8, "n_dias": pl.Int64},
    )

    def by_hour(code: str, ts: str, name: str) -> tuple[pl.DataFrame, int]:
        local = pl.col(ts).dt.convert_time_zone(LOCAL_TZ)
        frame = trips.select(
            pl.col(code).alias("short_name_norm"),
            local.dt.date().alias("fecha"),
            (local.dt.weekday() - 1).cast(pl.Int8).alias("dow"),
            local.dt.hour().cast(pl.Int8).alias("hour_local"),
        )
        inside = frame.filter(pl.col("fecha").is_in(dates))
        counts = inside.group_by("short_name_norm", "dow", "hour_local").len(name)
        return counts, frame.height - inside.height

    salidas, fuera_salidas = by_hour("estacion_retiro_norm", "retiro_utc", "salidas")
    llegadas, fuera_llegadas = by_hour("estacion_arribo_norm", "arribo_utc", "llegadas")
    codes = pl.concat([trips["estacion_retiro_norm"], trips["estacion_arribo_norm"]])
    tasa, unmatched = match_stats(codes, known)
    demand = (
        salidas.join(
            llegadas, on=["short_name_norm", "dow", "hour_local"], how="full", coalesce=True
        )
        .fill_null(0)
        .join(dias, on="dow", how="left")
        .with_columns(
            (pl.col("salidas") / pl.col("n_dias")).round(4).alias("salidas_prom"),
            (pl.col("llegadas") / pl.col("n_dias")).round(4).alias("llegadas_prom"),
            pl.lit(tasa).alias("tasa_match"),
        )
        .select(TRIP_DEMAND_SCHEMA.names)
        .sort("short_name_norm", "dow", "hour_local")
    )
    summary = {
        "viajes": trips.height,
        "meses": sorted(set(months)),
        "salidas_fuera_de_mes": fuera_salidas,
        "llegadas_fuera_de_mes": fuera_llegadas,
        "filas_demanda": demand.height,
        "tasa_match": tasa,
        "codigos_sin_emparejar": len(unmatched),
        "ejemplos_sin_emparejar": unmatched[:10],
    }
    return demand, summary


def write_demand(settings: Settings) -> dict[str, Any]:
    """Lee silver/trips y station_information y escribe gold/trip_demand de forma atómica.

    Raises:
        FileNotFoundError: Si falta silver/trips o station_information.
    """
    files = sorted((settings.data_dir / "silver" / "trips").glob("month=*/part-0.parquet"))
    info_path = settings.data_dir / "silver/station_information/station_information.parquet"
    if not files or not info_path.exists():
        raise FileNotFoundError("faltan silver/trips o silver/station_information")
    trips = pl.from_arrow(
        pa.concat_tables([pq.read_table(f, schema=SILVER_TRIPS_SCHEMA) for f in files])
    )
    assert isinstance(trips, pl.DataFrame)
    known = set(pq.read_table(info_path, columns=["short_name_norm"]).column(0).to_pylist())
    known.discard(None)
    months = [f.parent.name.removeprefix("month=") for f in files]
    demand, summary = build_demand(trips, known, months)
    out = settings.data_dir / "gold" / "trip_demand" / "trip_demand.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(out.name + ".tmp")
    write_parquet(_to_arrow(demand, TRIP_DEMAND_SCHEMA), tmp)
    os.replace(tmp, out)
    return summary


# --- Benchmark DuckDB vs pandas --------------------------------------------------------------
def peak_memory_mb() -> float:
    """Memoria pico del proceso actual en MB (Windows: ``peak_wset``; Linux: ``ru_maxrss``)."""
    if sys.platform == "win32":
        import psutil

        return float(psutil.Process().memory_info().peak_wset) / 2**20
    import resource

    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # Linux: KB


def current_memory_mb() -> float:
    """Memoria residente actual en MB."""
    import psutil

    return float(psutil.Process().memory_info().rss) / 2**20


_DUCK_PARQUET = f"""
SELECT estacion_retiro_norm AS estacion,
       isodow(retiro_utc AT TIME ZONE '{LOCAL_TZ}') - 1 AS dow,
       hour(retiro_utc AT TIME ZONE '{LOCAL_TZ}') AS hora,
       COUNT(*) AS viajes,
       AVG(epoch(arribo_utc) - epoch(retiro_utc)) / 60 AS duracion_min
FROM read_parquet('{{src}}')
GROUP BY ALL
"""

_DUCK_CSV = f"""
WITH v AS (
  SELECT Ciclo_Estacion_Retiro AS estacion,
         strptime(Fecha_Retiro || ' ' || Hora_Retiro, '{DT_FMT}') AS retiro,
         strptime(Fecha_Arribo || ' ' || Hora_Arribo, '{DT_FMT}') AS arribo
  FROM read_csv('{{src}}', header = true, all_varchar = true, encoding = '{{enc}}')
)
SELECT estacion, isodow(retiro) - 1 AS dow, hour(retiro) AS hora, COUNT(*) AS viajes,
       AVG(epoch(arribo) - epoch(retiro)) / 60 AS duracion_min
FROM v GROUP BY ALL
"""


def _bench_duckdb(source: str, src: str, enc: str) -> tuple[int, int, float]:
    import duckdb

    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    duck_enc = "utf-8" if enc.startswith("utf-8") else enc  # DuckDB salta el BOM solo
    sql = (_DUCK_PARQUET if source == "parquet" else _DUCK_CSV).format(src=src, enc=duck_enc)
    groups, viajes, dur = con.execute(
        f"SELECT COUNT(*), SUM(viajes), SUM(duracion_min * viajes) FROM ({sql})"
    ).fetchone() or (0, 0, 0.0)
    return int(groups), int(viajes), float(dur)


def _bench_pandas(source: str, src: str, enc: str) -> tuple[int, int, float]:
    import glob

    import pandas as pd

    if source == "parquet":
        cols = ["estacion_retiro_norm", "retiro_utc", "arribo_utc"]
        df = pd.concat([pd.read_parquet(f, columns=cols) for f in sorted(glob.glob(src))])
        local = df["retiro_utc"].dt.tz_convert(LOCAL_TZ)
        estacion = df["estacion_retiro_norm"]
        dur = (df["arribo_utc"] - df["retiro_utc"]).dt.total_seconds() / 60
    else:
        csv = pd.read_csv(src, usecols=list(WHITELIST), dtype=str, encoding=enc)
        local = pd.to_datetime(csv["Fecha_Retiro"] + " " + csv["Hora_Retiro"], format=DT_FMT)
        arribo = pd.to_datetime(csv["Fecha_Arribo"] + " " + csv["Hora_Arribo"], format=DT_FMT)
        estacion = csv["Ciclo_Estacion_Retiro"]
        dur = (arribo - local).dt.total_seconds() / 60
    frame = pd.DataFrame(
        {"estacion": estacion, "dow": local.dt.day_of_week, "hora": local.dt.hour, "dur": dur}
    )
    g = frame.groupby(["estacion", "dow", "hora"]).agg(
        viajes=("dur", "size"), duracion_min=("dur", "mean")
    )
    return len(g), int(g["viajes"].sum()), float((g["duracion_min"] * g["viajes"]).sum())


def bench_engine(engine: str, source: str, src: str, enc: str) -> dict[str, Any]:
    """Corre UNA agregación en este proceso y mide tiempo y memoria (lo llama el subproceso).

    Args:
        engine: ``duckdb`` | ``pandas``.
        source: ``parquet`` | ``csv``.
        src: Patrón de archivos Parquet o ruta del CSV.
        enc: Codificación del CSV.

    Returns:
        ``{segundos, base_mb, pico_mb, grupos, viajes, duracion_total}``.
    """
    import duckdb  # noqa: F401  (se importan antes de medir para no contar el import)
    import pandas  # noqa: F401

    base = current_memory_mb()
    start = time.perf_counter()
    run = _bench_duckdb if engine == "duckdb" else _bench_pandas
    groups, viajes, dur = run(source, src, enc)
    seconds = time.perf_counter() - start
    return {
        "segundos": round(seconds, 3),
        "base_mb": round(base, 1),
        "pico_mb": round(peak_memory_mb(), 1),
        "grupos": groups,
        "viajes": viajes,
        "duracion_total": round(dur, 1),
    }


def run_benchmark(settings: Settings) -> dict[str, Any]:
    """Corre cada motor y fuente en subprocesos separados y escribe el reporte.

    Raises:
        FileNotFoundError: Si faltan silver/trips o el CSV de bronze.
    """
    parquet = (settings.data_dir / "silver" / "trips" / "month=*" / "part-0.parquet").as_posix()
    csvs = TripsToSilverJob(settings).bronze_csvs()
    if not csvs or not list(settings.data_dir.glob("silver/trips/month=*/part-0.parquet")):
        raise FileNotFoundError("corre antes --download --to-silver")
    csv = csvs[-1]
    with csv.open("rb") as fh:
        _, enc = decode_csv(fh.read())
    sources = {"parquet": parquet, "csv": csv.as_posix()}
    results: dict[str, dict[str, Any]] = {}
    for source, src in sources.items():
        for engine in ("duckdb", "pandas"):
            runs = []
            for _ in range(BENCH_RUNS):
                cmd = [sys.executable, "-m", "radar.lake.trips", "--bench-engine", engine]
                cmd += ["--bench-source", source, "--bench-src", src, "--bench-encoding", enc]
                done = subprocess.run(cmd, capture_output=True, text=True, check=True)
                runs.append(json.loads(done.stdout))
            results[f"{source}/{engine}"] = {
                "segundos_mediana": statistics.median(r["segundos"] for r in runs),
                "segundos": [r["segundos"] for r in runs],
                "base_mb": max(r["base_mb"] for r in runs),
                "pico_mb": max(r["pico_mb"] for r in runs),
                "grupos": runs[0]["grupos"],
                "viajes": runs[0]["viajes"],
                "duracion_total": runs[0]["duracion_total"],
            }
    BENCH_REPORT.parent.mkdir(parents=True, exist_ok=True)
    BENCH_REPORT.write_text(benchmark_markdown(results, csv), encoding="utf-8")
    return results


def benchmark_markdown(results: dict[str, dict[str, Any]], csv: Path) -> str:
    """Reporte Markdown del benchmark."""
    lines = [
        "# Benchmark DuckDB vs pandas",
        "",
        f"Generado el {date.today().isoformat()} con `uv run python -m radar.lake.trips"
        " --benchmark`.",
        "",
        "**Agregación:** viajes y duración media por estación de retiro, día de la semana y hora"
        " local. Cada motor corre en su propio subproceso (memoria limpia), "
        f"{BENCH_RUNS} veces; se reporta la mediana del tiempo de reloj (`time.perf_counter`, "
        "lectura + agregación, sin contar los imports) y la memoria pico del proceso "
        "(`psutil` `peak_wset` en Windows, `ru_maxrss` en Linux). *Base* = memoria tras importar"
        " las librerías, antes de leer datos.",
        "",
        f"**Máquina:** {platform.platform()} · Python {platform.python_version()} · "
        f"{os.cpu_count()} CPUs lógicos. **Datos:** `{csv.name}` (bronze, CSV) y su versión"
        " silver en Parquet.",
        "",
        "| Fuente | Motor | Segundos (mediana) | Corridas | Base MB | Pico MB | Pico − base MB"
        " | Grupos | Viajes |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for key, r in results.items():
        source, engine = key.split("/")
        lines.append(
            f"| {source} | {engine} | {r['segundos_mediana']:.3f} | "
            f"{', '.join(f'{s:.3f}' for s in r['segundos'])} | {r['base_mb']:.1f} | "
            f"{r['pico_mb']:.1f} | {r['pico_mb'] - r['base_mb']:.1f} | {r['grupos']} | "
            f"{r['viajes']} |"
        )
    lines.append("")
    for source in ("parquet", "csv"):
        d, p = results[f"{source}/duckdb"], results[f"{source}/pandas"]
        same = (d["grupos"], d["viajes"]) == (p["grupos"], p["viajes"]) and abs(
            d["duracion_total"] - p["duracion_total"]
        ) <= 1.0
        speed = p["segundos_mediana"] / d["segundos_mediana"] if d["segundos_mediana"] else 0
        lines.append(
            f"- **{source}:** mismos resultados en ambos motores: {'sí' if same else 'NO'}; "
            f"DuckDB {speed:.1f}× más rápido y "
            f"{p['pico_mb'] - p['base_mb'] - (d['pico_mb'] - d['base_mb']):.0f} MB menos de"
            " memoria de trabajo que pandas."
        )
    return "\n".join(lines) + "\n"


# --- CLI -------------------------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    """CLI ``python -m radar.lake.trips``.

    Args:
        argv: Argumentos (None = ``sys.argv``).

    Returns:
        0 ok; 2 uso; 3 check fallido (encabezado distinto, descarga o conversión imposible).
    """
    parser = argparse.ArgumentParser(description="Viajes históricos de ECOBICI en el lake.")
    parser.add_argument("--probe", action="store_true")
    parser.add_argument("--download", action="store_true")
    parser.add_argument("--to-silver", action="store_true")
    parser.add_argument("--demand", action="store_true")
    parser.add_argument("--benchmark", action="store_true")
    parser.add_argument("--url", help="sobrescribe TRIPS_CSV_URL")
    parser.add_argument("--json", action="store_true")
    for hidden in ("--bench-engine", "--bench-source", "--bench-src", "--bench-encoding"):
        parser.add_argument(hidden, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)

    if args.bench_engine:  # modo subproceso del benchmark: solo JSON a stdout
        print(
            json.dumps(
                bench_engine(
                    args.bench_engine, args.bench_source, args.bench_src, args.bench_encoding
                )
            )
        )
        return 0
    if not any((args.probe, args.download, args.to_silver, args.demand, args.benchmark)):
        parser.error("indica al menos una acción: --probe --download --to-silver --demand ...")
    settings = get_settings()
    configure_logging(settings.log_level)
    url = args.url or settings.trips_csv_url
    out: dict[str, Any] = {}
    ok = True

    if args.probe or args.download:
        if not url:
            log.error("falta_trips_csv_url")
            return 3
        with http_client(settings) as client:
            if args.probe:
                try:
                    good, line = probe(url, client)
                except httpx.HTTPError as exc:
                    good, line = False, f"error: {exc}"
                out["probe"] = {"url": url, "ok": good, "encabezado": line}
                if not good:
                    ok = False
                    try:
                        page = client.get(OPEN_DATA_URL)
                        out["probe"]["sugerencia"] = latest_csv_url(page.text)
                    except httpx.HTTPError as exc:
                        out["probe"]["sugerencia"] = f"error: {exc}"
                    log.error("encabezado_inesperado", **out["probe"])
                else:
                    log.info("probe_ok", url=url)
            if args.download and ok:
                try:
                    estado, path, size = download(url, settings.data_dir / "bronze/trips", client)
                except (httpx.HTTPError, FileExistsError) as exc:
                    log.error("descarga_fallo", error=str(exc))
                    return 3
                out["download"] = {"estado": estado, "archivo": path.as_posix(), "bytes": size}
                log.info("descarga", **out["download"])

    if args.to_silver and ok:
        job = TripsToSilverJob(settings)
        names = [p.name for p in job.bronze_csvs()]
        if not names:
            log.error("sin_csv_en_bronze_trips")
            return 3
        try:
            out["to_silver"] = [job.run(n) for n in names]
        except ValueError as exc:
            log.error("conversion_fallo", error=str(exc))
            return 3
        for m in out["to_silver"]:
            log.info("viajes_silver", **m)

    if args.demand and ok:
        try:
            out["demand"] = write_demand(settings)
        except FileNotFoundError as exc:
            log.error("demanda_fallo", error=str(exc))
            return 3
        log.info("demanda_gold", **out["demand"])

    if args.benchmark and ok:
        try:
            out["benchmark"] = run_benchmark(settings)
        except subprocess.CalledProcessError as exc:
            log.error("benchmark_fallo", error=str(exc), stderr=(exc.stderr or "")[-2000:])
            return 3
        except FileNotFoundError as exc:
            log.error("benchmark_fallo", error=str(exc))
            return 3
        log.info("benchmark_escrito", archivo=BENCH_REPORT.as_posix())

    if args.json:
        print(json.dumps(out, sort_keys=True, ensure_ascii=False))
    return 0 if ok else 3


if __name__ == "__main__":
    sys.exit(main())
