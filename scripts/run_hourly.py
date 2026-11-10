"""
Autor: Isacc Cabrera
Descripción: Pipeline horario idempotente de una hora UTC: bronze → silver → clima → gold →
    checks de calidad. Es la lógica que llaman los DAGs de Airflow (una etapa por tarea) y
    también el fallback si Airflow no está disponible (agendarlo con un loop o el Programador de
    tareas de Windows). Etapas: --stage silver|clima|gold|calidad|todo.
    --hour auto = la hora más reciente terminada hace ≥ 10 min cuyo hueco máximo de heartbeat
    no supera QUALITY_MAX_GAP_MINUTES (siempre el umbral por defecto, nunca --max-gap-minutes,
    para que elegir la hora no dependa del check), buscando hasta 24 h atrás.
    Checks de calidad: huecos_heartbeat (hueco más largo entre corridas del recolector dentro
    de la hora, contando los bordes) y regla_capacidad (% de filas que no cumplen la regla, del
    manifiesto silver). Un check o una etapa fallida imprime check_fallido=<nombre> y sale 3.
    --info refresca station_information por hash (DAG diario). Si LAKE_SYNC_S3=true, al final
    sincroniza bronze, silver y gold con SeaweedFS.
Patrón: Facade (una sola entrada para las etapas de radar.lake)
Fecha de creación: 2026-11-10
Cambios:
    - 2026-11-10 · Isacc Cabrera · Creación (paso 9).
"""

import argparse
import gzip
import json
import sys
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from radar.collector.gbfs_client import GbfsClient
from radar.collector.hashing import stations_hash
from radar.collector.heartbeat import Heartbeat, HeartbeatWriter
from radar.collector.retry import ExponentialJitterPolicy
from radar.collector.sinks import LocalFileSink
from radar.config import Settings, get_settings
from radar.lake.bronze_to_silver import HOUR_FMT, BronzeToSilverJob, auto_hour, parse_hour
from radar.lake.silver_to_gold import DAY_FMT, SilverToGoldJob
from radar.lake.store import LAYERS, layer_files, make_lake_store, sync_files
from radar.lake.weather import WeatherError, update_weather
from radar.logging import configure_logging, get_logger

log = get_logger(__name__)

STAGES = ("silver", "clima", "gold", "calidad")
AUTO_LOOKBACK_HOURS = 24
OK_RESULTS = ("guardado", "sin_cambios")
AIRFLOW_FMT = "%Y-%m-%dT%H:00:00+00:00"


# --------------------------------------------------------------------------- heartbeat
def heartbeat_times(settings: Settings, hour: datetime) -> list[datetime]:
    """Inicios de las corridas exitosas de ``station_status`` dentro de la hora.

    Cuenta ``guardado`` y ``sin_cambios`` de cualquier sink (local o kafka): ambos prueban que
    el recolector corrió. ``error`` no cuenta.

    Args:
        settings: Configuración con ``data_dir``.
        hour: Inicio de la hora (UTC).

    Returns:
        Tiempos ordenados dentro de ``[hour, hour + 1 h)``.
    """
    path = settings.data_dir / "bronze" / "heartbeat" / f"dt={hour:%Y-%m-%d}.jsonl"
    if not path.exists():
        return []
    end = hour + timedelta(hours=1)
    times = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if row.get("feed") != "station_status" or row.get("result") not in OK_RESULTS:
            continue
        ts = datetime.fromisoformat(row["ts_utc"])
        if hour <= ts < end:
            times.append(ts)
    return sorted(times)


def max_gap_minutes(times: list[datetime], hour: datetime) -> float:
    """Hueco más largo (minutos) entre corridas, contando desde el inicio y hasta el fin de la hora.

    Contar los bordes detecta un recolector que arrancó a media hora o que se cayó antes del
    final. Sin ninguna corrida, el hueco es la hora completa (60).

    Args:
        times: Inicios de corrida ordenados, dentro de la hora.
        hour: Inicio de la hora.

    Returns:
        Minutos del hueco más largo.
    """
    points = [hour, *times, hour + timedelta(hours=1)]
    gaps = [(b - a).total_seconds() / 60 for a, b in zip(points, points[1:], strict=False)]
    return round(max(gaps), 2)


def choose_auto_hour(settings: Settings, now: datetime | None = None) -> datetime | None:
    """La hora ``auto``: terminada hace ≥ 10 min y con heartbeat sin huecos grandes.

    Usa SIEMPRE ``settings.quality_max_gap_minutes`` (el umbral por defecto).

    Args:
        settings: Configuración.
        now: Hora actual (inyectable).

    Returns:
        Inicio de la hora en UTC, o None si ninguna de las últimas 24 h califica.
    """
    start = auto_hour(now)
    for back in range(AUTO_LOOKBACK_HOURS):
        hour = start - timedelta(hours=back)
        times = heartbeat_times(settings, hour)
        if times and max_gap_minutes(times, hour) <= settings.quality_max_gap_minutes:
            return hour
    return None


# --------------------------------------------------------------------------- calidad
def quality_checks(
    settings: Settings, hour: datetime, max_gap: float, max_fail_pct: float
) -> dict[str, dict[str, Any]]:
    """Corre los checks de calidad de la hora.

    Args:
        settings: Configuración.
        hour: Inicio de la hora.
        max_gap: Hueco máximo permitido en minutos (``--max-gap-minutes``).
        max_fail_pct: % máximo de filas que no cumplen la regla de capacidad.

    Returns:
        ``{nombre: {"ok": bool, ...detalle}}`` para ``huecos_heartbeat`` y ``regla_capacidad``.
    """
    gap = max_gap_minutes(heartbeat_times(settings, hour), hour)
    checks: dict[str, dict[str, Any]] = {
        "huecos_heartbeat": {"ok": gap <= max_gap, "hueco_min": gap, "umbral_min": max_gap}
    }
    manifest_path = BronzeToSilverJob(settings).manifest_path(hour)
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        fail_pct = round(100 * (1 - manifest["tasa_regla_capacidad"]), 3)
        checks["regla_capacidad"] = {
            "ok": fail_pct <= max_fail_pct,
            "pct_incumple": fail_pct,
            "umbral_pct": max_fail_pct,
        }
    else:
        checks["regla_capacidad"] = {"ok": False, "motivo": "sin_manifiesto_silver"}
    return checks


# --------------------------------------------------------------------------- etapas
def _http(settings: Settings) -> httpx.Client:
    return httpx.Client(timeout=settings.http_timeout_seconds, follow_redirects=True)


def _policy(settings: Settings) -> ExponentialJitterPolicy:
    return ExponentialJitterPolicy(
        base_s=settings.retry_base_seconds, cap_s=settings.retry_cap_seconds
    )


def stage_silver(settings: Settings, hour: datetime) -> tuple[bool, dict[str, Any]]:
    """Bronze → silver de la hora (reemplaza su partición)."""
    manifest = BronzeToSilverJob(settings).run(hour.strftime(HOUR_FMT))
    return manifest["filas_validas"] > 0, manifest


def stage_weather(settings: Settings, hour: datetime) -> tuple[bool, dict[str, Any]]:
    """Clima de Open-Meteo (upsert; no depende de la hora)."""
    with _http(settings) as http:
        summary = update_weather(settings, http, _policy(settings))
    return summary["filas_total"] > 0, summary


def stage_gold(settings: Settings, hour: datetime) -> tuple[bool, dict[str, Any]]:
    """Silver → gold del día UTC de la hora (reemplaza la partición del día)."""
    manifest = SilverToGoldJob(settings).run(hour.strftime(DAY_FMT))
    return manifest["filas_station_hour"] > 0, manifest


STAGE_FUNCS: dict[str, Callable[[Settings, datetime], tuple[bool, dict[str, Any]]]] = {
    "silver": stage_silver,
    "clima": stage_weather,
    "gold": stage_gold,
}


def run_pipeline(
    settings: Settings, hour: datetime, stage: str, max_gap: float, max_fail_pct: float
) -> dict[str, Any]:
    """Corre una etapa o todas (``todo``) en orden y junta el resultado.

    Una etapa que falla (excepción o 0 filas) detiene las siguientes; los checks de calidad no
    detienen nada, solo se reportan.

    Args:
        settings: Configuración.
        hour: Inicio de la hora.
        stage: ``silver``, ``clima``, ``gold``, ``calidad`` o ``todo``.
        max_gap: Umbral de ``huecos_heartbeat``.
        max_fail_pct: Umbral de ``regla_capacidad``.

    Returns:
        ``hora``, ``etapas``, ``checks``, ``checks_fallidos`` y ``ok``.
    """
    stages = STAGES if stage == "todo" else (stage,)
    out: dict[str, Any] = {"hora": hour.strftime(HOUR_FMT), "etapas": {}, "checks": {}}
    failed: list[str] = []
    for name in stages:
        if name == "calidad":
            checks = quality_checks(settings, hour, max_gap, max_fail_pct)
            out["checks"] = checks
            failed += [c for c, r in checks.items() if not r["ok"]]
            continue
        try:
            ok, detail = STAGE_FUNCS[name](settings, hour)
        except (FileNotFoundError, WeatherError, httpx.HTTPError, ValueError) as exc:
            ok, detail = False, {"error": f"{type(exc).__name__}: {exc}"}
        out["etapas"][name] = {"ok": ok, **detail}
        log.info("etapa_terminada", etapa=name, hora=out["hora"], ok=ok)
        if not ok:
            failed.append(f"etapa_{name}")
            break
    out["checks_fallidos"] = failed
    out["ok"] = not failed
    return out


def sync_all(settings: Settings) -> dict[str, dict[str, int]]:
    """Sube las tres capas a SeaweedFS (idempotente; nunca ``bronze/trips/``)."""
    result = {}
    for layer in LAYERS:
        store = make_lake_store(settings, layer, "s3")
        res = sync_files(store, settings.data_dir / layer, layer_files(settings, layer))
        result[layer] = res.resumen()
    return result


# --------------------------------------------------------------------------- station_information
def latest_info_hash(settings: Settings) -> str | None:
    """``sha8`` de la versión MÁS RECIENTE (mayor ``last_updated``) guardada en bronze.

    Se compara contra la última versión y no contra todas: si el feed vuelve a una versión
    vieja, eso también es un cambio y se guarda, porque silver usa la más reciente.

    Args:
        settings: Configuración con ``data_dir``.

    Returns:
        El ``sha8`` del nombre del archivo, o None si bronze no tiene ninguna versión.
    """
    root = settings.data_dir / "bronze" / "station_information"
    latest: tuple[int, str] | None = None
    for path in root.glob("dt=*/station_information_*.json.gz"):
        last_updated = int(json.loads(gzip.decompress(path.read_bytes()))["last_updated"])
        sha8 = path.name.removeprefix("station_information_").removesuffix(".json.gz")
        if latest is None or (last_updated, sha8) > latest:
            latest = (last_updated, sha8)
    return latest[1] if latest else None


def refresh_information(settings: Settings, client: GbfsClient) -> dict[str, Any]:
    """Descarga station_information y la guarda en bronze si su hash difiere del último (D-005).

    Escribe una línea de heartbeat (sink ``local``) como lo hace el recolector.

    Args:
        settings: Configuración.
        client: Cliente GBFS (con reintentos).

    Returns:
        ``sha8``, ``resultado`` (``guardado`` | ``sin_cambios``) y ``estaciones``.
    """
    started = datetime.now(UTC)
    fetched = client.fetch("station_information")
    payload = fetched.payload
    sha8 = stations_hash(payload)[:8]
    resultado = "sin_cambios"
    if sha8 != latest_info_hash(settings):
        written = LocalFileSink(settings.data_dir).write_information(fetched.raw, payload, sha8)
        resultado = "guardado" if written == "guardado" else "sin_cambios"
    n = len(payload.get("data", {}).get("stations", []))
    HeartbeatWriter(settings.data_dir).append(
        Heartbeat(
            ts_utc=started.isoformat(),
            feed="station_information",
            result="guardado" if resultado == "guardado" else "sin_cambios",
            http_status=fetched.http_status,
            n_stations=n,
            duration_ms=fetched.duration_ms,
            error=None,
            last_updated=int(payload.get("last_updated", 0)) or None,
            sink="local",
        )
    )
    return {"sha8": sha8, "resultado": resultado, "estaciones": n}


# --------------------------------------------------------------------------- CLI
def _emit(out: dict[str, Any], as_json: bool) -> None:
    if as_json:
        print(json.dumps(out, sort_keys=True, default=str))
    else:
        for name in out.get("checks_fallidos", []):
            print(f"check_fallido={name}")


def main(argv: list[str] | None = None) -> int:
    """CLI ``scripts/run_hourly.py``.

    Args:
        argv: Argumentos (None = ``sys.argv``).

    Returns:
        0 ok; 2 uso; 3 check o etapa fallida, o ninguna hora ``auto`` disponible.
    """
    parser = argparse.ArgumentParser(description="Pipeline horario: silver, clima, gold, calidad.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--hour", metavar="YYYY-MM-DDTHH|auto")
    group.add_argument("--print-auto-hour", action="store_true")
    group.add_argument("--print-airflow-logical-date", action="store_true")
    group.add_argument("--info", action="store_true")
    parser.add_argument("--stage", choices=(*STAGES, "todo"), default="todo")
    settings = get_settings()
    parser.add_argument("--max-gap-minutes", type=float, default=settings.quality_max_gap_minutes)
    parser.add_argument(
        "--max-capacity-fail-pct", type=float, default=settings.quality_max_capacity_fail_pct
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    configure_logging(settings.log_level)

    if args.info:
        with _http(settings) as http:
            info = refresh_information(settings, GbfsClient(settings, http, _policy(settings)))
        log.info("station_information_refrescada", **info)
        if args.json:
            print(json.dumps(info, sort_keys=True))
        return 0

    if args.hour and args.hour != "auto":
        try:
            hour = parse_hour(args.hour)
        except ValueError:
            parser.error("--hour debe ser YYYY-MM-DDTHH o auto")
    else:
        chosen = choose_auto_hour(settings)
        if chosen is None:
            log.error("sin_hora_auto", umbral_min=settings.quality_max_gap_minutes)
            print("check_fallido=sin_hora_auto")
            return 3
        hour = chosen

    if args.print_auto_hour:
        print(hour.strftime(HOUR_FMT))
        return 0
    if args.print_airflow_logical_date:
        print((hour + timedelta(hours=1)).strftime(AIRFLOW_FMT))
        return 0

    out = run_pipeline(settings, hour, args.stage, args.max_gap_minutes, args.max_capacity_fail_pct)
    if settings.lake_sync_s3:
        out["sync"] = sync_all(settings)
    log.info("pipeline_horario", hora=out["hora"], ok=out["ok"], fallidos=out["checks_fallidos"])
    _emit(out, args.json)
    return 0 if out["ok"] else 3


if __name__ == "__main__":
    sys.exit(main())
