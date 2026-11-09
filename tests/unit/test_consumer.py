"""
Autor: Isacc Cabrera
Descripción: Pruebas del consumidor BronzeWriter con un consumidor Kafka falso: el commit va
    después de escribir, un duplicado deja un solo archivo, una escritura fallida no hace commit
    y --exit-when-idle termina el loop.
Patrón: N/A (pruebas)
Fecha de creación: 2026-11-09
Cambios:
    - 2026-11-09 · Isacc Cabrera · Creación (paso 5).
"""

import glob
import gzip
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from radar.collector.sinks import LocalFileSink, status_path
from radar.config import Settings
from radar.streaming.consumer import BronzeWriter
from radar.streaming.messages import raw_message

Loader = Callable[[str], Any]


class FakeMessage:
    """Imita ``confluent_kafka.Message``."""

    def __init__(self, value: bytes, headers: list[tuple[str, bytes]], offset: int = 0) -> None:
        self._value = value
        self._headers = headers
        self._offset = offset

    def error(self) -> Any:
        return None

    def value(self) -> bytes:
        return self._value

    def headers(self) -> list[tuple[str, bytes]]:
        return self._headers

    def offset(self) -> int:
        return self._offset


class FakeConsumer:
    """Imita ``confluent_kafka.Consumer``: entrega mensajes encolados y registra commits."""

    def __init__(self, messages: list[FakeMessage] | None = None) -> None:
        self.queue = list(messages or [])
        self.commits: list[int] = []

    def poll(self, timeout: float) -> FakeMessage | None:
        return self.queue.pop(0) if self.queue else None

    def commit(self, message: FakeMessage, asynchronous: bool) -> None:
        assert asynchronous is False
        self.commits.append(message.offset())

    def close(self) -> None:
        pass


class BrokenSink(LocalFileSink):
    """Sink que falla al escribir (disco lleno simulado)."""

    def write_status(self, raw: bytes, payload: dict[str, Any]) -> Any:
        raise OSError("disco lleno")


def make_message(load_fixture: Loader, offset: int = 0) -> tuple[FakeMessage, bytes]:
    raw = json.dumps(load_fixture("station_status.json")).encode("utf-8")
    kafka_msg = raw_message(raw, 1794160800)
    return FakeMessage(kafka_msg.value, kafka_msg.headers, offset), raw


def bronze_files(data_dir: Path) -> list[str]:
    return glob.glob(str(data_dir / "bronze/station_status/**/*.json.gz"), recursive=True)


def test_escribe_bronze_identico_y_hace_commit_despues(
    settings: Settings, load_fixture: Loader
) -> None:
    msg, raw = make_message(load_fixture, offset=7)
    consumer = FakeConsumer()
    writer = BronzeWriter(consumer, LocalFileSink(settings.data_dir))
    assert writer.process(msg) == "guardado"
    assert gzip.decompress(status_path(settings.data_dir, 1794160800).read_bytes()) == raw
    assert consumer.commits == [7]


def test_mismo_mensaje_dos_veces_deja_un_archivo_y_dos_commits(
    settings: Settings, load_fixture: Loader
) -> None:
    first, _ = make_message(load_fixture, offset=1)
    again, _ = make_message(load_fixture, offset=2)
    consumer = FakeConsumer()
    writer = BronzeWriter(consumer, LocalFileSink(settings.data_dir))
    assert writer.process(first) == "guardado"
    assert writer.process(again) == "ya_existia"
    assert len(bronze_files(settings.data_dir)) == 1
    assert consumer.commits == [1, 2]
    assert (writer.stats.escritos, writer.stats.ya_existian) == (1, 1)


def test_si_escribir_falla_no_hay_commit(settings: Settings, load_fixture: Loader) -> None:
    msg, _ = make_message(load_fixture)
    consumer = FakeConsumer()
    writer = BronzeWriter(consumer, BrokenSink(settings.data_dir))
    assert writer.process(msg) is None
    assert consumer.commits == []
    assert writer.stats.fallidos == 1


def test_header_last_updated_distinto_al_cuerpo_no_se_confirma(
    settings: Settings, load_fixture: Loader
) -> None:
    msg, _ = make_message(load_fixture)
    bad = FakeMessage(msg.value(), [("last_updated", b"1"), ("content-encoding", b"gzip")])
    consumer = FakeConsumer()
    assert BronzeWriter(consumer, LocalFileSink(settings.data_dir)).process(bad) is None
    assert consumer.commits == []
    assert bronze_files(settings.data_dir) == []


def test_exit_when_idle_termina_y_resume(settings: Settings, load_fixture: Loader) -> None:
    msg, _ = make_message(load_fixture)
    consumer = FakeConsumer([msg])
    ticks = iter(float(x) for x in range(100))
    writer = BronzeWriter(consumer, LocalFileSink(settings.data_dir))
    stats = writer.run(exit_when_idle=3, monotonic=lambda: next(ticks))
    assert (stats.consumidos, stats.escritos) == (1, 1)
