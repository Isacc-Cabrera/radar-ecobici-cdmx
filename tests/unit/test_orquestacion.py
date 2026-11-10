"""
Autor: Isacc Cabrera
Descripción: Pruebas del paso 9, sin red: clima de Open-Meteo con upsert (sin ts_utc
    repetidos, gana el dato más reciente) y reintentos con la RetryPolicy; huecos de heartbeat
    contando los bordes de la hora; elección de la hora auto con el umbral por defecto; checks de
    calidad que salen 3 e imprimen su nombre; fecha lógica de Airflow = hora auto + 1 h;
    refresco de station_information por hash; y que los DAGs no tengan lógica (solo llaman a
    run_hourly.py con el venv del proyecto).
Patrón: N/A (pruebas)
Fecha de creación: 2026-11-10
Cambios:
    - 2026-11-10 · Isacc Cabrera · Creación (paso 9).
"""

import ast
import copy
import importlib.util
import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType
from typing import Any

import httpx
import pyarrow.parquet as pq
import pytest
import respx

from radar.collector.gbfs_client import GbfsClient
from radar.collector.retry import ExponentialJitterPolicy
from radar.config import Settings
from radar.lake.weather import FORECAST_URL, WeatherError, update_weather, weather_path

ROOT = Path(__file__).resolve().parents[2]
HOUR = datetime(2026, 11, 8, 18, tzinfo=UTC)
NOW = datetime(2026, 11, 8, 19, 15, tzinfo=UTC)  # auto_hour(NOW) = 18 h
GBFS_URL = "https://gbfs.example.test/gbfs/gbfs.json"
INFO_URL = "https://gbfs.example.test/gbfs/es/station_information.json"


def _load_run_hourly() -> ModuleType:
    spec = importlib.util.spec_from_file_location("run_hourly", ROOT / "scripts/run_hourly.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


rh = _load_run_hourly()


def _meteo(start: str, hours: int, temp: float) -> dict[str, Any]:
    first = datetime.fromisoformat(start)
    times = [(first + timedelta(hours=i)).strftime("%Y-%m-%dT%H:%M") for i in range(hours)]
    return {
        "hourly": {
            "time": times,
            "temperature_2m": [temp] * hours,
            "precipitation": [0.0] * hours,
            "relative_humidity_2m": [50.0] * hours,
            "wind_speed_10m": [5.0] * hours,
        }
    }


def _policy() -> ExponentialJitterPolicy:
    return ExponentialJitterPolicy(base_s=1.0, cap_s=4.0, max_attempts=3)


def _heartbeats(settings: Settings, rows: list[tuple[datetime, str, str]]) -> None:
    path = settings.data_dir / "bronze" / "heartbeat" / f"dt={rows[0][0]:%Y-%m-%d}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as handle:
        for ts, feed, result in rows:
            line = {"ts_utc": ts.isoformat(), "feed": feed, "result": result, "sink": "kafka"}
            handle.write(json.dumps(line) + "\n")


def _every_minute(settings: Settings, hour: datetime, skip: range = range(0)) -> None:
    rows = [
        (hour + timedelta(minutes=m, seconds=5), "station_status", "guardado")
        for m in range(60)
        if m not in skip
    ]
    _heartbeats(settings, rows)


# --- clima ------------------------------------------------------------------------------------
def test_clima_dos_corridas_no_repiten_ts_y_gana_lo_mas_reciente(settings: Settings) -> None:
    sleeps: list[float] = []
    with respx.mock(assert_all_mocked=True) as mock, httpx.Client() as http:
        route = mock.get(FORECAST_URL)
        route.mock(return_value=httpx.Response(200, json=_meteo("2026-11-08T00:00", 24, 10.0)))
        first = update_weather(settings, http, _policy(), sleeps.append)
        route.mock(return_value=httpx.Response(200, json=_meteo("2026-11-08T12:00", 24, 20.0)))
        second = update_weather(settings, http, _policy(), sleeps.append)
        assert route.calls.last.request.url.params["timezone"] == "UTC"
        assert route.calls.last.request.url.params["latitude"] == "19.4326"
    table = pq.read_table(weather_path(settings))
    ts = table.column("ts_utc").to_pylist()
    assert (first["filas_total"], second["filas_total"]) == (24, 36)
    assert len(ts) == len(set(ts)) == 36
    temps = dict(zip(ts, table.column("temperature_2m").to_pylist(), strict=True))
    assert temps[datetime(2026, 11, 8, 11, tzinfo=UTC)] == 10.0
    assert temps[datetime(2026, 11, 8, 12, tzinfo=UTC)] == 20.0  # el pronóstico se reemplazó
    assert str(table.schema.field("ts_utc").type) == "timestamp[ms, tz=UTC]"
    assert not sleeps


def test_clima_reintenta_5xx_y_no_reintenta_4xx(settings: Settings) -> None:
    sleeps: list[float] = []
    with respx.mock(assert_all_mocked=True) as mock, httpx.Client() as http:
        route = mock.get(FORECAST_URL)
        route.side_effect = [
            httpx.Response(503),
            httpx.Response(200, json=_meteo("2026-11-08T00:00", 2, 15.0)),
        ]
        assert update_weather(settings, http, _policy(), sleeps.append)["filas_total"] == 2
        assert len(sleeps) == 1
        route.side_effect = None
        route.mock(return_value=httpx.Response(400))
        with pytest.raises(WeatherError):
            update_weather(settings, http, _policy(), sleeps.append)
    assert len(sleeps) == 1  # el 400 no esperó ni reintentó


# --- heartbeat y hora auto ---------------------------------------------------------------------
def test_hueco_cuenta_los_bordes_de_la_hora() -> None:
    times = [HOUR + timedelta(minutes=m) for m in (5, 6, 30)]
    assert rh.max_gap_minutes(times, HOUR) == 30.0  # 30 → 60
    assert rh.max_gap_minutes([HOUR + timedelta(minutes=m) for m in range(60)], HOUR) == 1.0
    assert rh.max_gap_minutes([], HOUR) == 60.0


def test_heartbeat_ignora_errores_otro_feed_y_otra_hora(settings: Settings) -> None:
    _heartbeats(
        settings,
        [
            (HOUR + timedelta(minutes=1), "station_status", "guardado"),
            (HOUR + timedelta(minutes=2), "station_status", "sin_cambios"),
            (HOUR + timedelta(minutes=3), "station_status", "error"),
            (HOUR + timedelta(minutes=4), "station_information", "guardado"),
            (HOUR + timedelta(minutes=61), "station_status", "guardado"),
        ],
    )
    assert rh.heartbeat_times(settings, HOUR) == [
        HOUR + timedelta(minutes=1),
        HOUR + timedelta(minutes=2),
    ]


def test_hora_auto_salta_horas_con_huecos_usando_el_umbral_por_defecto(
    settings: Settings,
) -> None:
    _every_minute(settings, HOUR - timedelta(hours=1))  # 17 h completa
    _every_minute(settings, HOUR, skip=range(10, 30))  # 18 h con un hueco de 21 min
    assert rh.choose_auto_hour(settings, NOW) == HOUR - timedelta(hours=1)
    assert rh.choose_auto_hour(settings, NOW + timedelta(days=2)) is None


# --- CLI y checks de calidad ------------------------------------------------------------------
@pytest.fixture
def cli(settings: Settings, monkeypatch: pytest.MonkeyPatch) -> Callable[[list[str]], int]:
    monkeypatch.setattr(rh, "get_settings", lambda: settings)
    monkeypatch.setattr(rh, "auto_hour", lambda now=None: HOUR)
    return rh.main  # type: ignore[no-any-return]


def test_calidad_con_hueco_cero_sale_3_e_imprime_el_check(
    settings: Settings, cli: Callable[[list[str]], int], capsys: pytest.CaptureFixture[str]
) -> None:
    _every_minute(settings, HOUR)
    assert cli(["--print-auto-hour"]) == 0
    assert capsys.readouterr().out.strip() == "2026-11-08T18"
    code = cli(["--hour", "2026-11-08T18", "--stage", "calidad", "--max-gap-minutes", "0"])
    out = capsys.readouterr().out
    assert code == 3
    assert "check_fallido=huecos_heartbeat" in out
    assert "check_fallido=regla_capacidad" in out  # no hay manifiesto silver de esa hora


def test_calidad_json_lista_los_checks_fallidos(
    settings: Settings, cli: Callable[[list[str]], int], capsys: pytest.CaptureFixture[str]
) -> None:
    _every_minute(settings, HOUR)
    manifest = settings.data_dir / "silver/station_status/dt=2026-11-08/hour=18/_manifest.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_text(json.dumps({"tasa_regla_capacidad": 0.9}), encoding="utf-8")
    code = cli(["--hour", "2026-11-08T18", "--stage", "calidad", "--json"])
    out = json.loads(capsys.readouterr().out)
    assert code == 3
    assert out["checks_fallidos"] == ["regla_capacidad"]  # 10 % > 5 %; el hueco de 1 min pasa
    assert out["checks"]["regla_capacidad"]["pct_incumple"] == 10.0


def test_fecha_logica_de_airflow_es_la_hora_auto_mas_una(
    settings: Settings, cli: Callable[[list[str]], int], capsys: pytest.CaptureFixture[str]
) -> None:
    _every_minute(settings, HOUR)
    assert cli(["--print-airflow-logical-date"]) == 0
    assert capsys.readouterr().out.strip() == "2026-11-08T19:00:00+00:00"


def test_sin_hora_auto_sale_3(
    cli: Callable[[list[str]], int], capsys: pytest.CaptureFixture[str]
) -> None:
    assert cli(["--print-auto-hour"]) == 3
    assert "sin_hora_auto" in capsys.readouterr().out


def test_etapa_silver_sin_bronze_falla_y_detiene_las_siguientes(settings: Settings) -> None:
    out = rh.run_pipeline(settings, HOUR, "todo", 10.0, 5.0)
    assert out["checks_fallidos"] == ["etapa_silver"]
    assert list(out["etapas"]) == ["silver"]


# --- station_information (DAG diario) ---------------------------------------------------------
def test_info_guarda_solo_si_cambia_el_hash(settings: Settings, load_fixture: Any) -> None:
    settings = settings.model_copy(update={"gbfs_url": GBFS_URL})
    with respx.mock(assert_all_mocked=True) as mock, httpx.Client() as http:
        mock.get(GBFS_URL).mock(return_value=httpx.Response(200, json=load_fixture("gbfs.json")))
        info = load_fixture("station_information.json")
        mock.get(INFO_URL).mock(return_value=httpx.Response(200, json=info))
        client = GbfsClient(settings, http, _policy(), sleep=lambda _s: None)
        first = rh.refresh_information(settings, client)
        second = rh.refresh_information(settings, client)
    assert (first["resultado"], second["resultado"]) == ("guardado", "sin_cambios")
    assert first["sha8"] == second["sha8"]
    files = list((settings.data_dir / "bronze/station_information").glob("dt=*/*.json.gz"))
    assert [f.name for f in files] == [f"station_information_{first['sha8']}.json.gz"]
    lines = (settings.data_dir / "bronze/heartbeat").glob("*.jsonl")
    assert sum(len(p.read_text(encoding="utf-8").splitlines()) for p in lines) == 2


def test_info_guarda_si_el_feed_vuelve_a_una_version_vieja(
    settings: Settings, load_fixture: Any
) -> None:
    settings = settings.model_copy(update={"gbfs_url": GBFS_URL})
    a = load_fixture("station_information.json")
    b = copy.deepcopy(a)
    b["last_updated"] = a["last_updated"] + 100
    b["data"]["stations"][0]["capacity"] += 1
    a_otra_vez = copy.deepcopy(a)
    a_otra_vez["last_updated"] = a["last_updated"] + 86_400  # al día siguiente
    with respx.mock(assert_all_mocked=True) as mock, httpx.Client() as http:
        mock.get(GBFS_URL).mock(return_value=httpx.Response(200, json=load_fixture("gbfs.json")))
        route = mock.get(INFO_URL)
        client = GbfsClient(settings, http, _policy(), sleep=lambda _s: None)
        results = []
        for payload in (a, b, a_otra_vez):
            route.mock(return_value=httpx.Response(200, json=payload))
            results.append(rh.refresh_information(settings, client))
    assert [r["resultado"] for r in results] == ["guardado", "guardado", "guardado"]
    assert results[0]["sha8"] == results[2]["sha8"] != results[1]["sha8"]
    assert rh.latest_info_hash(settings) == results[0]["sha8"]


# --- DAGs ---------------------------------------------------------------------------------------
def _bash_commands(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    commands = []
    for node in ast.walk(tree):
        if isinstance(node, ast.keyword) and node.arg == "bash_command":
            commands.append(ast.unparse(node.value))
    return commands


def test_dags_solo_llaman_run_hourly_con_el_venv_del_proyecto() -> None:
    horario = (ROOT / "dags/horario.py").read_text(encoding="utf-8")
    assert 'dag_id="horario"' in horario and 'schedule="0 * * * *"' in horario
    assert "logical_date - macros.timedelta(hours=1)" in horario
    for stage in ("silver", "clima", "gold", "calidad"):
        assert f'etapa("{stage}"' in horario or f', "{stage}")' in horario
    diario = (ROOT / "dags/diario.py").read_text(encoding="utf-8")
    assert 'dag_id="diario"' in diario and "--info" in diario
    for path in (ROOT / "dags/horario.py", ROOT / "dags/diario.py"):
        cmds = _bash_commands(path)
        assert cmds, path
        source = path.read_text(encoding="utf-8")
        assert "/opt/radar/.venv/bin/python" in source and "run_hourly.py" in source
