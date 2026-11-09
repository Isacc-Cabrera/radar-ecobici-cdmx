"""
Autor: Isacc Cabrera
Descripción: Pruebas del runner del recolector con un cliente falso (sin red): guardado,
    sin_cambios, feed congelado, errores que no detienen el loop, refresco de
    station_information por hash y por station_id desconocido, e idempotencia de bronze.
Patrón: N/A (pruebas)
Fecha de creación: 2026-11-09
Cambios:
    - 2026-11-09 · Isacc Cabrera · Creación (paso 3).
"""

import copy
import glob
import gzip
import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from radar.collector.gbfs_client import FeedUnavailableError, FetchResult
from radar.collector.heartbeat import HeartbeatWriter
from radar.collector.runner import CollectorRunner
from radar.collector.sinks import LocalFileSink, make_sink, status_path
from radar.config import Settings

Loader = Callable[[str], Any]


class FakeClient:
    """Cliente falso: devuelve los payloads o excepciones encolados por feed."""

    def __init__(self, responses: dict[str, list[Any]]) -> None:
        self.responses = responses
        self.calls: list[str] = []

    def fetch(self, name: str) -> FetchResult:
        self.calls.append(name)
        queue = self.responses[name]
        item = queue.pop(0) if len(queue) > 1 else queue[0]
        if isinstance(item, Exception):
            raise item
        raw = json.dumps(item).encode("utf-8")
        return FetchResult(item, raw, 200, 5)


class FakeClock:
    """Reloj controlable."""

    def __init__(self) -> None:
        self.now = datetime(2026, 11, 8, 18, 0, 30, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


def heartbeats(data_dir: Path) -> list[dict[str, Any]]:
    """Lee todas las líneas de heartbeat escritas."""
    lines: list[dict[str, Any]] = []
    for path in sorted((data_dir / "bronze" / "heartbeat").glob("*.jsonl")):
        lines.extend(json.loads(x) for x in path.read_text(encoding="utf-8").splitlines())
    return lines


def make_runner(
    settings: Settings, client: FakeClient, clock: FakeClock | None = None
) -> CollectorRunner:
    return CollectorRunner(
        settings,
        client,
        LocalFileSink(settings.data_dir),
        HeartbeatWriter(settings.data_dir),
        clock=clock or FakeClock(),
        sleep=lambda _s: None,
    )


@pytest.fixture
def info(load_fixture: Loader) -> dict[str, Any]:
    return load_fixture("station_information.json")


@pytest.fixture
def status(load_fixture: Loader) -> dict[str, Any]:
    return load_fixture("station_status.json")


def test_snapshot_nuevo_se_guarda_en_bronze_con_heartbeat(
    settings: Settings, info: dict[str, Any], status: dict[str, Any]
) -> None:
    runner = make_runner(
        settings, FakeClient({"station_information": [info], "station_status": [status]})
    )
    assert runner.run_once() == "guardado"
    path = status_path(settings.data_dir, 1794160800)
    assert path.as_posix().endswith("dt=2026-11-08/hour=18/station_status_1794160800.json.gz")
    assert json.loads(gzip.decompress(path.read_bytes())) == status
    feeds = [(h["feed"], h["result"]) for h in heartbeats(settings.data_dir)]
    assert ("station_information", "guardado") in feeds
    assert ("station_status", "guardado") in feeds


def test_last_updated_repetido_da_sin_cambios_y_no_escribe(
    settings: Settings, info: dict[str, Any], status: dict[str, Any]
) -> None:
    runner = make_runner(
        settings, FakeClient({"station_information": [info], "station_status": [status]})
    )
    runner.run_once()
    assert runner.tick_status() == "sin_cambios"
    files = glob.glob(str(settings.data_dir / "bronze/station_status/**/*.json.gz"), recursive=True)
    assert len(files) == 1
    assert heartbeats(settings.data_dir)[-1]["result"] == "sin_cambios"


def test_feed_congelado_genera_alerta_y_heartbeat_error(
    settings: Settings,
    info: dict[str, Any],
    status: dict[str, Any],
) -> None:
    clock = FakeClock()
    client = FakeClient({"station_information": [info], "station_status": [status]})
    runner = make_runner(settings, client, clock)
    runner.run_once()
    clock.advance(settings.stale_alert_seconds + 1)
    assert runner.tick_status() == "error"
    status_hbs = [h for h in heartbeats(settings.data_dir) if h["feed"] == "station_status"]
    assert status_hbs[-1]["result"] == "error"
    assert status_hbs[-1]["error"] == "feed_congelado"
    # el id desconocido "999" solo provocó un refresco extra, no uno por cada tick
    assert client.calls.count("station_information") == 2


def test_excepcion_del_cliente_queda_en_heartbeat_y_el_loop_sigue(
    settings: Settings, info: dict[str, Any], status: dict[str, Any]
) -> None:
    client = FakeClient(
        {
            "station_information": [info],
            "station_status": [FeedUnavailableError("sin red", 503), status],
        }
    )
    runner = make_runner(settings, client)
    runner.run_forever(max_ticks=2)
    results = [h["result"] for h in heartbeats(settings.data_dir) if h["feed"] == "station_status"]
    assert results == ["error", "guardado"]
    errors = [h for h in heartbeats(settings.data_dir) if h["result"] == "error"]
    assert errors[0]["http_status"] == 503


def test_station_information_solo_se_guarda_si_cambia_el_hash(
    settings: Settings, info: dict[str, Any], status: dict[str, Any]
) -> None:
    misma_con_otro_last_updated = copy.deepcopy(info)
    misma_con_otro_last_updated["last_updated"] += 86400
    cambiada = copy.deepcopy(misma_con_otro_last_updated)
    cambiada["data"]["stations"][0]["capacity"] += 1
    client = FakeClient(
        {
            "station_information": [info, misma_con_otro_last_updated, cambiada],
            "station_status": [status],
        }
    )
    runner = make_runner(settings, client)
    assert runner.refresh_information(force=True) == "guardado"
    assert runner.refresh_information(force=True) == "sin_cambios"
    assert runner.refresh_information(force=True) == "guardado"
    versions = glob.glob(
        str(settings.data_dir / "bronze/station_information/**/*.json.gz"), recursive=True
    )
    assert len(versions) == 2


def test_station_id_desconocido_fuerza_refresco_de_information(
    settings: Settings, info: dict[str, Any], status: dict[str, Any]
) -> None:
    client = FakeClient({"station_information": [info], "station_status": [status]})
    runner = make_runner(settings, client)
    runner.run_once()  # status trae "999", que no está en information
    assert client.calls == ["station_information", "station_status", "station_information"]
    path = status_path(settings.data_dir, 1794160800)
    assert path.exists()  # D-005: se guarda igual aunque traiga un id desconocido


def test_bronze_es_idempotente_y_no_sobrescribe(settings: Settings, status: dict[str, Any]) -> None:
    sink = LocalFileSink(settings.data_dir)
    raw = json.dumps(status).encode("utf-8")
    assert sink.write_status(raw, status) == "guardado"
    assert sink.write_status(b"otro contenido", status) == "ya_existia"
    path = status_path(settings.data_dir, 1794160800)
    assert gzip.decompress(path.read_bytes()) == raw


def test_make_sink_rechaza_valores_desconocidos(settings: Settings) -> None:
    with pytest.raises(ValueError, match="SINK desconocido"):
        make_sink(settings.model_copy(update={"sink": "ftp"}))


def test_estado_persiste_entre_reinicios(
    settings: Settings, info: dict[str, Any], status: dict[str, Any]
) -> None:
    make_runner(
        settings, FakeClient({"station_information": [info], "station_status": [status]})
    ).run_once()
    reiniciado = make_runner(
        settings, FakeClient({"station_information": [info], "station_status": [status]})
    )
    reiniciado.refresh_information(force=True)
    assert reiniciado.tick_status() == "sin_cambios"
