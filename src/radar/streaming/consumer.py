"""
Autor: Isacc Cabrera
Descripción: Consumidor del grupo bronze-writer. Lee station_status_raw, descomprime el snapshot
    y lo escribe en bronze con LocalFileSink (atómico + fsync, nombre = last_updated). SOLO
    después de escribir hace commit del offset: si el proceso muere antes, el mensaje se vuelve
    a leer al reiniciar (at-least-once) y la escritura idempotente evita duplicados. Si escribir
    falla, no hay commit. CLI: sin flags corre para siempre; --exit-when-idle S termina tras S
    segundos sin mensajes e imprime un resumen JSON.
Patrón: Dependency Injection (consumidor y sink inyectados) + Ports & Adapters (reusa el
    adaptador LocalFileSink del puerto SnapshotSink)
Fecha de creación: 2026-11-09
Cambios:
    - 2026-11-09 · Isacc Cabrera · Creación (paso 5).
"""

import argparse
import gzip
import json
import signal
import sys
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from types import FrameType
from typing import Any, Protocol, cast

from radar.collector.sinks import LocalFileSink, WriteResult
from radar.config import Settings, get_settings
from radar.logging import configure_logging, get_logger

log = get_logger(__name__)


class MessageLike(Protocol):
    """Lo que usamos de ``confluent_kafka.Message``."""

    def error(self) -> Any:
        """Error del mensaje o None."""
        ...

    def value(self) -> bytes | None:
        """Cuerpo."""
        ...

    def headers(self) -> list[tuple[str, bytes]] | None:
        """Headers."""
        ...

    def offset(self) -> int | None:
        """Offset dentro de la partición."""
        ...


class ConsumerLike(Protocol):
    """Lo que usamos de ``confluent_kafka.Consumer`` (permite un falso en pruebas)."""

    def poll(self, timeout: float) -> Any:
        """Siguiente mensaje o None."""
        ...

    def commit(self, message: Any, asynchronous: bool) -> Any:
        """Confirma el offset de ``message``."""
        ...

    def close(self) -> None:
        """Sale del grupo."""
        ...


@dataclass
class Stats:
    """Resumen de una corrida del consumidor.

    Attributes:
        consumidos: Mensajes leídos sin error.
        escritos: Archivos bronze nuevos.
        ya_existian: Mensajes cuyo archivo ya estaba (duplicados colapsados).
        fallidos: Mensajes cuya escritura falló (sin commit).
    """

    consumidos: int = 0
    escritos: int = 0
    ya_existian: int = 0
    fallidos: int = 0


def consumer_config(settings: Settings) -> dict[str, Any]:
    """Configuración del consumidor: commit manual y lectura desde el inicio.

    Args:
        settings: Configuración con bootstrap y group id.

    Returns:
        Diccionario para ``confluent_kafka.Consumer``.
    """
    return {
        "bootstrap.servers": settings.kafka_bootstrap,
        "group.id": settings.kafka_group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }


def _header(msg: MessageLike, name: str) -> bytes | None:
    for key, value in msg.headers() or []:
        if key == name:
            return value
    return None


class BronzeWriter:
    """Escribe en bronze cada snapshot crudo y luego confirma su offset.

    Args:
        consumer: Consumidor ya suscrito a ``station_status_raw``.
        sink: Adaptador que escribe bronze en disco.
    """

    def __init__(self, consumer: ConsumerLike, sink: LocalFileSink) -> None:
        self._consumer = consumer
        self._sink = sink
        self.stats = Stats()

    def process(self, msg: MessageLike) -> WriteResult | None:
        """Procesa un mensaje: descomprime, escribe y SOLO después hace commit.

        Args:
            msg: Mensaje de ``station_status_raw``.

        Returns:
            ``"guardado"``/``"ya_existia"``, o None si el mensaje traía error o la escritura
            falló (en ese caso no hay commit y se reintenta al reiniciar).
        """
        if msg.error() is not None:
            log.warning("kafka_mensaje_con_error", error=str(msg.error()))
            return None
        self.stats.consumidos += 1
        try:
            value = msg.value() or b""
            raw = gzip.decompress(value) if _header(msg, "content-encoding") == b"gzip" else value
            payload = json.loads(raw)
            header_lu = _header(msg, "last_updated")
            if header_lu is not None and int(header_lu) != int(payload["last_updated"]):
                raise ValueError("header last_updated no coincide con el cuerpo")
            result = self._sink.write_status(raw, payload)
        except Exception as exc:  # noqa: BLE001 - se registra y NO se hace commit
            self.stats.fallidos += 1
            log.error("bronze_escritura_fallida", offset=msg.offset(), error=str(exc))
            return None
        self._consumer.commit(message=msg, asynchronous=False)
        if result == "guardado":
            self.stats.escritos += 1
        else:
            self.stats.ya_existian += 1
        log.info("bronze_escrito", resultado=result, last_updated=payload["last_updated"])
        return result

    def run(
        self,
        exit_when_idle: float | None = None,
        should_stop: Callable[[], bool] = lambda: False,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> Stats:
        """Loop de consumo.

        Args:
            exit_when_idle: Segundos sin mensajes para terminar; None = nunca.
            should_stop: Señal externa de paro (SIGTERM).
            monotonic: Reloj monotónico (inyectable).

        Returns:
            El resumen acumulado.
        """
        last_seen = monotonic()
        while not should_stop():
            msg = self._consumer.poll(1.0)
            if msg is None:
                if exit_when_idle is not None and monotonic() - last_seen >= exit_when_idle:
                    break
                continue
            last_seen = monotonic()
            self.process(msg)
        return self.stats


def main(argv: list[str] | None = None) -> int:
    """CLI ``python -m radar.streaming.consumer [--exit-when-idle S] [--min-messages N]``.

    Args:
        argv: Argumentos (None = ``sys.argv``).

    Returns:
        0 si todo bien; 3 si se consumieron menos de ``--min-messages``.
    """
    parser = argparse.ArgumentParser(description="Consumidor bronze-writer.")
    parser.add_argument("--exit-when-idle", type=float, default=None, metavar="S")
    parser.add_argument("--min-messages", type=int, default=0, metavar="N")
    args = parser.parse_args(argv)
    settings = get_settings()
    configure_logging(settings.log_level)

    from confluent_kafka import Consumer

    consumer = Consumer(consumer_config(settings))
    consumer.subscribe([settings.kafka_topic_raw])
    stop = {"flag": False}

    def _on_term(_signum: int, _frame: FrameType | None) -> None:
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _on_term)
    log.info("bronze_writer_inicia", grupo=settings.kafka_group_id, topic=settings.kafka_topic_raw)
    writer = BronzeWriter(cast(ConsumerLike, consumer), LocalFileSink(settings.data_dir))
    try:
        stats = writer.run(args.exit_when_idle, should_stop=lambda: stop["flag"])
    finally:
        consumer.close()
    print(json.dumps(asdict(stats)))
    return 3 if stats.consumidos < args.min_messages else 0


if __name__ == "__main__":
    sys.exit(main())
