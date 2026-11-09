"""
Autor: Isacc Cabrera
Descripción: Pruebas del diseño de mensajes Kafka y del adaptador KafkaSink con un productor
    falso (sin broker): un mensaje por estación con key station_id, mensaje crudo que se
    descomprime a los bytes originales, espera de confirmación y error si una entrega falla.
Patrón: N/A (pruebas)
Fecha de creación: 2026-11-09
Cambios:
    - 2026-11-09 · Isacc Cabrera · Creación (paso 4).
"""

import gzip
import json
from collections.abc import Callable
from typing import Any

import pytest

from radar.collector.sinks import information_path
from radar.config import Settings
from radar.streaming.messages import raw_message, snapshot_id, station_messages
from radar.streaming.producer import (
    KafkaDeliveryError,
    KafkaSink,
    expected_partitions,
    producer_config,
)

Loader = Callable[[str], Any]


class FakeProducer:
    """Productor falso: guarda lo producido y confirma (o falla) en flush."""

    def __init__(self, fail_keys: set[bytes] | None = None, pending: int = 0) -> None:
        self.produced: list[dict[str, Any]] = []
        self._callbacks: list[tuple[bytes, Any]] = []
        self._fail_keys = fail_keys or set()
        self._pending = pending

    def produce(
        self,
        topic: str,
        value: bytes,
        key: bytes,
        headers: list[tuple[str, bytes]],
        on_delivery: Any,
    ) -> None:
        self.produced.append({"topic": topic, "key": key, "value": value, "headers": headers})
        self._callbacks.append((key, on_delivery))

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        for key, callback in self._callbacks:
            callback("broker caído" if key in self._fail_keys else None, None)
        self._callbacks.clear()
        return self._pending


def test_un_mensaje_por_estacion_con_key_station_id(load_fixture: Loader) -> None:
    payload = load_fixture("station_status.json")
    messages = station_messages(payload)
    assert [m.key for m in messages] == [b"1", b"2", b"3", b"999"]
    for msg in messages:
        body = json.loads(msg.value)
        assert body["feed_last_updated"] == 1794160800
        assert body["snapshot_id"] == "station_status_1794160800"
        assert body["station_id"] == msg.key.decode()


def test_mensaje_crudo_se_descomprime_a_los_bytes_originales(load_fixture: Loader) -> None:
    raw = json.dumps(load_fixture("station_status.json")).encode("utf-8")
    msg = raw_message(raw, 1794160800)
    assert msg.key == b"station_status"
    assert gzip.decompress(msg.value) == raw
    assert ("last_updated", b"1794160800") in msg.headers
    assert ("content-encoding", b"gzip") in msg.headers
    assert raw_message(raw, 1794160800).value == msg.value  # determinista (mtime=0)


def test_snapshot_id_coincide_con_el_nombre_bronze() -> None:
    assert snapshot_id(1794160800) == "station_status_1794160800"


def test_kafka_sink_publica_crudo_y_estaciones_en_sus_topics(
    settings: Settings, load_fixture: Loader
) -> None:
    payload = load_fixture("station_status.json")
    producer = FakeProducer()
    sink = KafkaSink(settings, producer=producer)
    assert sink.write_status(json.dumps(payload).encode(), payload) == "guardado"
    topics = [p["topic"] for p in producer.produced]
    assert topics == ["station_status_raw"] + ["station_status"] * 4


def test_kafka_sink_falla_si_una_entrega_no_se_confirma(
    settings: Settings, load_fixture: Loader
) -> None:
    payload = load_fixture("station_status.json")
    sink = KafkaSink(settings, producer=FakeProducer(fail_keys={b"3"}))
    with pytest.raises(KafkaDeliveryError):
        sink.write_status(json.dumps(payload).encode(), payload)


def test_kafka_sink_falla_si_quedan_mensajes_pendientes(
    settings: Settings, load_fixture: Loader
) -> None:
    payload = load_fixture("station_status.json")
    sink = KafkaSink(settings, producer=FakeProducer(pending=2))
    with pytest.raises(KafkaDeliveryError):
        sink.write_status(json.dumps(payload).encode(), payload)


def test_station_information_va_directo_a_bronze_local(
    settings: Settings, load_fixture: Loader
) -> None:
    info = load_fixture("station_information.json")
    producer = FakeProducer()
    sink = KafkaSink(settings, producer=producer)
    assert sink.write_information(json.dumps(info).encode(), info, "abcd1234") == "guardado"
    assert information_path(settings.data_dir, int(info["last_updated"]), "abcd1234").exists()
    assert producer.produced == []


def test_config_del_productor_es_segura_y_topics_esperados(settings: Settings) -> None:
    config = producer_config(settings)
    assert config["acks"] == "all"
    assert config["enable.idempotence"] is True
    assert expected_partitions(settings) == {"station_status": 3, "station_status_raw": 1}
