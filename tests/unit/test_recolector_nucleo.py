"""
Autor: Isacc Cabrera
Descripción: Pruebas sin red del núcleo del recolector: descubrimiento GBFS, política de
    reintentos (503, 429 con Retry-After, 404) y hash de station_information. respx intercepta
    todo HTTP; el sleep es falso y registra las esperas; el random lleva semilla.
Patrón: N/A (pruebas)
Fecha de creación: 2026-11-09
Cambios:
    - 2026-11-09 · Isacc Cabrera · Creación (paso 2).
"""

import copy
import json
import random
from collections.abc import Callable
from typing import Any

import httpx
import pytest
import respx

from radar.collector.gbfs_client import (
    FeedClientError,
    FeedSchemaError,
    FeedUnavailableError,
    GbfsClient,
)
from radar.collector.hashing import find_unknown_station_ids, station_ids, stations_hash
from radar.collector.retry import ExponentialJitterPolicy, parse_retry_after
from radar.config import Settings

GBFS_URL = "https://gbfs.example.test/gbfs/gbfs.json"
STATUS_URL = "https://gbfs.example.test/gbfs/es/station_status.json"

Loader = Callable[[str], Any]


class FakeSleep:
    """Sleep falso que registra cada espera en lugar de dormir."""

    def __init__(self) -> None:
        self.waits: list[float] = []

    def __call__(self, seconds: float) -> None:
        self.waits.append(seconds)


def make_client(settings: Settings, sleep: FakeSleep, max_attempts: int = 6) -> GbfsClient:
    """Arma un GbfsClient de prueba con política determinista."""
    cfg = settings.model_copy(update={"gbfs_url": GBFS_URL, "gbfs_lang": "es"})
    policy = ExponentialJitterPolicy(
        base_s=cfg.retry_base_seconds,
        cap_s=cfg.retry_cap_seconds,
        max_attempts=max_attempts,
        rng=random.Random(42),
    )
    return GbfsClient(cfg, httpx.Client(), policy, sleep=sleep)


def test_descubrimiento_devuelve_urls_del_idioma(settings: Settings, load_fixture: Loader) -> None:
    with respx.mock(assert_all_mocked=True) as mock:
        mock.get(GBFS_URL).respond(200, json=load_fixture("gbfs.json"))
        feeds = make_client(settings, FakeSleep()).discover()
    assert feeds["station_status"] == STATUS_URL
    assert feeds["station_information"].endswith("/es/station_information.json")


def test_descubrimiento_falla_si_falta_el_idioma(settings: Settings, load_fixture: Loader) -> None:
    client = make_client(settings, FakeSleep())
    client._settings = client._settings.model_copy(update={"gbfs_lang": "fr"})
    with respx.mock(assert_all_mocked=True) as mock:
        mock.get(GBFS_URL).respond(200, json=load_fixture("gbfs.json"))
        with pytest.raises(FeedSchemaError):
            client.discover()


def test_503_dos_veces_y_luego_200_espera_dos_veces_con_jitter(
    settings: Settings, load_fixture: Loader
) -> None:
    sleep = FakeSleep()
    with respx.mock(assert_all_mocked=True) as mock:
        mock.get(GBFS_URL).respond(200, json=load_fixture("gbfs.json"))
        route = mock.get(STATUS_URL)
        route.side_effect = [
            httpx.Response(503),
            httpx.Response(503),
            httpx.Response(200, json=load_fixture("station_status.json")),
        ]
        result = make_client(settings, sleep).fetch("station_status")
    assert result.http_status == 200
    assert result.payload["last_updated"] == 1794160800
    assert len(sleep.waits) == 2
    for attempt, wait in enumerate(sleep.waits):
        assert 0 <= wait <= min(300, settings.retry_base_seconds * 2**attempt)


def test_429_respeta_retry_after(settings: Settings, load_fixture: Loader) -> None:
    sleep = FakeSleep()
    with respx.mock(assert_all_mocked=True) as mock:
        mock.get(GBFS_URL).respond(200, json=load_fixture("gbfs.json"))
        mock.get(STATUS_URL).side_effect = [
            httpx.Response(429, headers={"Retry-After": "7"}),
            httpx.Response(200, json=load_fixture("station_status.json")),
        ]
        make_client(settings, sleep).fetch("station_status")
    assert sleep.waits == [7.0]


def test_404_no_reintenta_y_lanza_error_de_cliente(
    settings: Settings, load_fixture: Loader
) -> None:
    sleep = FakeSleep()
    with respx.mock(assert_all_mocked=True) as mock:
        mock.get(GBFS_URL).respond(200, json=load_fixture("gbfs.json"))
        route = mock.get(STATUS_URL).respond(404)
        with pytest.raises(FeedClientError):
            make_client(settings, sleep).fetch("station_status")
    assert route.call_count == 1
    assert sleep.waits == []


def test_timeouts_agotan_reintentos_y_lanzan_no_disponible(settings: Settings) -> None:
    sleep = FakeSleep()
    with respx.mock(assert_all_mocked=True) as mock:
        route = mock.get(GBFS_URL).mock(side_effect=httpx.ConnectTimeout("sin red"))
        with pytest.raises(FeedUnavailableError):
            make_client(settings, sleep, max_attempts=3).discover()
    assert route.call_count == 3
    assert len(sleep.waits) == 2


def test_espera_nunca_supera_el_tope_de_300_s() -> None:
    policy = ExponentialJitterPolicy(base_s=2, cap_s=300, max_attempts=50, rng=random.Random(1))
    waits = [policy.decide(n, 503, None, None).delay_s for n in range(20)]
    assert max(waits) <= 300


def test_retry_after_acepta_fecha_http() -> None:
    from datetime import UTC, datetime

    now = datetime(2026, 11, 9, 12, 0, 0, tzinfo=UTC)
    assert parse_retry_after("Mon, 09 Nov 2026 12:00:30 GMT", now=now) == 30.0
    assert parse_retry_after("basura") is None


def test_hash_ignora_last_updated_pero_detecta_cambio_de_capacidad(load_fixture: Loader) -> None:
    info = load_fixture("station_information.json")
    otro_momento = copy.deepcopy(info)
    otro_momento["last_updated"] = info["last_updated"] + 3600
    otro_momento["data"]["stations"].reverse()  # el orden tampoco importa
    assert stations_hash(info) == stations_hash(otro_momento)

    cambiada = copy.deepcopy(info)
    cambiada["data"]["stations"][0]["capacity"] += 1
    assert stations_hash(info) != stations_hash(cambiada)


def test_detecta_station_id_desconocido(load_fixture: Loader) -> None:
    known = station_ids(load_fixture("station_information.json"))
    status = load_fixture("station_status.json")
    assert find_unknown_station_ids(status, known) == {"999"}


def test_cuerpo_crudo_se_conserva_tal_cual(settings: Settings, load_fixture: Loader) -> None:
    body = json.dumps(load_fixture("station_status.json")).encode("utf-8")
    with respx.mock(assert_all_mocked=True) as mock:
        mock.get(GBFS_URL).respond(200, json=load_fixture("gbfs.json"))
        mock.get(STATUS_URL).respond(200, content=body)
        result = make_client(settings, FakeSleep()).fetch("station_status")
    assert result.raw == body
