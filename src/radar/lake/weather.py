"""
Autor: Isacc Cabrera
Descripción: Clima horario de Open-Meteo (gratis, sin llave) para el centro de CDMX
    (WEATHER_LAT/WEATHER_LON). Descarga 7 días pasados + 1 de pronóstico en UTC y hace un upsert
    en silver/weather/weather.parquet por ts_utc: lee lo que ya había, combina, se queda con el
    valor MÁS RECIENTE de cada hora (un pronóstico de ayer se reemplaza por la observación de
    hoy) y escribe de forma atómica (temporal + os.replace). Así correrlo dos veces nunca
    duplica horas. Los reintentos usan la RetryPolicy del recolector (4xx sin reintento, 5xx y
    timeouts con backoff exponencial + full jitter). Para horas de más de 92 días existe
    archive-api.open-meteo.com; no se necesita en esta entrega.
Patrón: Strategy (RetryPolicy inyectada) + Dependency Injection (cliente httpx y sleep)
Fecha de creación: 2026-11-10
Cambios:
    - 2026-11-10 · Isacc Cabrera · Creación (paso 9).
"""

import os
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import polars as pl
import pyarrow as pa

from radar.collector.gbfs_client import USER_AGENT
from radar.collector.retry import RetryPolicy
from radar.config import Settings
from radar.lake.job import write_parquet
from radar.logging import get_logger

log = get_logger(__name__)

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
VARIABLES = ("temperature_2m", "precipitation", "relative_humidity_2m", "wind_speed_10m")
PAST_DAYS = 7
FORECAST_DAYS = 1

WEATHER_SCHEMA = pa.schema(
    [
        ("ts_utc", pa.timestamp("ms", tz="UTC")),  # Parquet guarda como mínimo ms
        *[(v, pa.float64()) for v in VARIABLES],
    ]
)


class WeatherError(Exception):
    """Open-Meteo no respondió con datos válidos después de los reintentos."""


def weather_path(settings: Settings) -> Path:
    """Ruta de la tabla silver del clima (sin partición, blueprint §4)."""
    return settings.data_dir / "silver" / "weather" / "weather.parquet"


def request_params(settings: Settings) -> dict[str, str | int | float]:
    """Parámetros de la consulta a Open-Meteo.

    Args:
        settings: Configuración (``weather_lat``, ``weather_lon``).

    Returns:
        Parámetros de la URL de ``/v1/forecast``.
    """
    return {
        "latitude": settings.weather_lat,
        "longitude": settings.weather_lon,
        "hourly": ",".join(VARIABLES),
        "past_days": PAST_DAYS,
        "forecast_days": FORECAST_DAYS,
        "timezone": "UTC",
    }


def fetch_weather(
    settings: Settings,
    http: httpx.Client,
    policy: RetryPolicy,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Descarga el clima horario con reintentos.

    Args:
        settings: Configuración.
        http: Cliente httpx (en pruebas, interceptado por respx).
        policy: Estrategia de reintentos (la misma del recolector).
        sleep: Función de espera (falsa en pruebas).

    Returns:
        El JSON de Open-Meteo.

    Raises:
        WeatherError: Si un 4xx no es reintentable, se agotan los reintentos o falta ``hourly``.
    """
    attempt = 0
    while True:
        status: int | None = None
        retry_after: str | None = None
        error: Exception | None = None
        try:
            response = http.get(
                FORECAST_URL, params=request_params(settings), headers={"User-Agent": USER_AGENT}
            )
            status = response.status_code
            if status == 200:
                payload: dict[str, Any] = response.json()
                if "time" not in payload.get("hourly", {}):
                    raise WeatherError("respuesta de Open-Meteo sin hourly.time")
                return payload
            retry_after = response.headers.get("Retry-After")
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            error = exc
        decision = policy.decide(attempt, status, retry_after, error)
        log.warning(
            "clima_intento_fallido",
            intento=attempt,
            http_status=status,
            motivo=decision.reason,
            reintenta=decision.retry,
        )
        if not decision.retry:
            raise WeatherError(f"Open-Meteo no disponible ({decision.reason})")
        sleep(decision.delay_s)
        attempt += 1


def to_frame(payload: dict[str, Any]) -> pl.DataFrame:
    """Convierte ``hourly`` de Open-Meteo en un DataFrame con ``ts_utc`` en UTC.

    Args:
        payload: JSON de Open-Meteo pedido con ``timezone=UTC`` (horas ``YYYY-MM-DDTHH:MM``).

    Returns:
        Una fila por hora; las horas sin ningún valor (todo null) se descartan.
    """
    hourly = payload["hourly"]
    data: dict[str, Any] = {"ts_utc": hourly["time"]}
    for var in VARIABLES:
        data[var] = hourly.get(var, [None] * len(hourly["time"]))
    df = pl.DataFrame(data, schema_overrides={v: pl.Float64 for v in VARIABLES})
    return df.with_columns(
        pl.col("ts_utc")
        .str.strptime(pl.Datetime("us"), "%Y-%m-%dT%H:%M")
        .dt.replace_time_zone("UTC")
        .dt.cast_time_unit("us")
    ).filter(~pl.all_horizontal([pl.col(v).is_null() for v in VARIABLES]))


def upsert(existing: pl.DataFrame | None, new: pl.DataFrame) -> pl.DataFrame:
    """Combina por ``ts_utc``; si una hora está en ambos, gana ``new`` (lo más reciente).

    Args:
        existing: Lo que ya había en silver (None si es la primera vez).
        new: Lo recién descargado.

    Returns:
        Una fila por ``ts_utc``, ordenada.
    """
    frames = [new.with_columns(pl.lit(1).alias("_orden"))]
    if existing is not None and existing.height:
        frames.append(existing.select(new.columns).with_columns(pl.lit(0).alias("_orden")))
    both = pl.concat(frames, how="vertical")
    return (
        both.sort(["ts_utc", "_orden"])
        .unique(subset="ts_utc", keep="last", maintain_order=True)
        .drop("_orden")
        .sort("ts_utc")
    )


def read_weather(path: Path) -> pl.DataFrame | None:
    """Lee la tabla silver del clima, o None si todavía no existe."""
    if not path.exists():
        return None
    df = pl.read_parquet(path)
    return df.with_columns(pl.col("ts_utc").dt.cast_time_unit("us").dt.convert_time_zone("UTC"))


def write_weather(df: pl.DataFrame, path: Path) -> None:
    """Escribe la tabla de forma atómica (temporal + ``os.replace``).

    Args:
        df: Tabla del clima.
        path: Destino ``silver/weather/weather.parquet``.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    table = df.to_arrow(compat_level=pl.CompatLevel.oldest()).cast(WEATHER_SCHEMA)
    tmp = path.with_suffix(".parquet.tmp")
    write_parquet(table, tmp)
    os.replace(tmp, path)


def update_weather(
    settings: Settings,
    http: httpx.Client,
    policy: RetryPolicy,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Descarga el clima y hace el upsert en silver.

    Args:
        settings: Configuración.
        http: Cliente httpx.
        policy: Estrategia de reintentos.
        sleep: Función de espera.

    Returns:
        Resumen: ``filas_descargadas``, ``filas_antes``, ``filas_total``, ``desde``, ``hasta``.
    """
    path = weather_path(settings)
    new = to_frame(fetch_weather(settings, http, policy, sleep))
    existing = read_weather(path)
    merged = upsert(existing, new)
    write_weather(merged, path)
    summary = {
        "filas_descargadas": new.height,
        "filas_antes": 0 if existing is None else existing.height,
        "filas_total": merged.height,
        "desde": merged["ts_utc"].min().isoformat() if merged.height else None,  # type: ignore[union-attr]
        "hasta": merged["ts_utc"].max().isoformat() if merged.height else None,  # type: ignore[union-attr]
    }
    log.info("clima_actualizado", **summary)
    return summary
