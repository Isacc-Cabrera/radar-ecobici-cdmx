"""
Autor: Isacc Cabrera
Descripción: Productor Kafka. KafkaSink es otro adaptador del puerto SnapshotSink: el recolector
    no cambia, solo recibe este sink cuando SINK=kafka. write_status publica el snapshot crudo
    (station_status_raw) y un mensaje por estación (station_status), espera la confirmación del
    broker (flush) y falla si alguna entrega falló, así el heartbeat "guardado" significa "Kafka
    lo tiene". Config segura: acks=all + idempotencia (sin duplicados por reintentos del
    productor). station_information se guarda directo en disco con LocalFileSink. El CLI
    `--ensure-topics --check` crea y describe los topics.
Patrón: Ports & Adapters (KafkaSink adapta SnapshotSink) + Dependency Injection (productor
    inyectable para probar sin broker)
Fecha de creación: 2026-11-09
Cambios:
    - 2026-11-09 · Isacc Cabrera · Creación (paso 4).
"""

import argparse
import json
import sys
from typing import Any, Protocol, cast

from radar.collector.sinks import LocalFileSink, WriteResult
from radar.config import Settings, get_settings
from radar.logging import configure_logging, get_logger
from radar.streaming.messages import KafkaMessage, raw_message, station_messages

log = get_logger(__name__)

RETENTION_MS = 7 * 24 * 3600 * 1000
FLUSH_TIMEOUT_S = 30.0


class KafkaDeliveryError(RuntimeError):
    """Uno o más mensajes no fueron confirmados por el broker."""


class ProducerLike(Protocol):
    """Lo mínimo que usamos de ``confluent_kafka.Producer`` (permite un falso en pruebas)."""

    def produce(
        self,
        topic: str,
        value: bytes,
        key: bytes,
        headers: list[tuple[str, bytes]],
        on_delivery: Any,
    ) -> None:
        """Encola un mensaje."""
        ...

    def poll(self, timeout: float) -> int:
        """Atiende callbacks de entrega pendientes."""
        ...

    def flush(self, timeout: float) -> int:
        """Espera las confirmaciones; regresa cuántos mensajes siguen pendientes."""
        ...


def producer_config(settings: Settings) -> dict[str, Any]:
    """Configuración del productor.

    Args:
        settings: Configuración con ``kafka_bootstrap``.

    Returns:
        Diccionario para ``confluent_kafka.Producer``.
    """
    return {
        "bootstrap.servers": settings.kafka_bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 50,
        "client.id": "radar-collector",
    }


class KafkaSink:
    """Adaptador del puerto ``SnapshotSink`` que publica en Kafka.

    Args:
        settings: Configuración (topics, bootstrap, data_dir).
        producer: Productor a usar; si es None se crea uno real de confluent-kafka.
    """

    name = "kafka"

    def __init__(self, settings: Settings, producer: ProducerLike | None = None) -> None:
        if producer is None:
            from confluent_kafka import Producer

            # cast: los stubs de confluent-kafka tipan produce() distinto, pero es compatible
            producer = cast(ProducerLike, Producer(producer_config(settings)))
        self._producer: ProducerLike = producer
        self._topic_status = settings.kafka_topic_status
        self._topic_raw = settings.kafka_topic_raw
        self._local = LocalFileSink(settings.data_dir)

    def _send(self, topic: str, msg: KafkaMessage, errors: list[str]) -> None:
        def on_delivery(err: Any, _msg: Any) -> None:
            if err is not None:
                errors.append(str(err))

        while True:
            try:
                self._producer.produce(
                    topic,
                    value=msg.value,
                    key=msg.key,
                    headers=msg.headers,
                    on_delivery=on_delivery,
                )
                return
            except BufferError:
                # cola local llena: atiende entregas y reintenta
                self._producer.poll(1.0)

    def write_status(self, raw: bytes, payload: dict[str, Any]) -> WriteResult:
        """Publica el crudo y las estaciones; espera confirmación. Ver ``SnapshotSink``.

        Raises:
            KafkaDeliveryError: Si algún mensaje no se confirmó o el flush expiró.
        """
        errors: list[str] = []
        last_updated = int(payload["last_updated"])
        self._send(self._topic_raw, raw_message(raw, last_updated), errors)
        stations = station_messages(payload)
        for msg in stations:
            self._send(self._topic_status, msg, errors)
        pending = self._producer.flush(FLUSH_TIMEOUT_S)
        if pending or errors:
            raise KafkaDeliveryError(
                f"pendientes={pending} errores={len(errors)} primero={errors[:1]}"
            )
        log.info("kafka_publicado", last_updated=last_updated, estaciones=len(stations))
        return "guardado"

    def write_information(self, raw: bytes, payload: dict[str, Any], sha8: str) -> WriteResult:
        """``station_information`` va directo a bronze local. Ver ``SnapshotSink``."""
        return self._local.write_information(raw, payload, sha8)


def ensure_topics(settings: Settings, admin: Any) -> dict[str, int]:
    """Crea los topics si faltan y regresa sus particiones actuales.

    Args:
        settings: Configuración con los nombres de topic.
        admin: ``confluent_kafka.admin.AdminClient``.

    Returns:
        ``{topic: número de particiones}`` según el broker.
    """
    from confluent_kafka import KafkaError
    from confluent_kafka.admin import NewTopic

    wanted = expected_partitions(settings)
    new = [
        NewTopic(
            t, num_partitions=p, replication_factor=1, config={"retention.ms": str(RETENTION_MS)}
        )
        for t, p in wanted.items()
    ]
    for topic, future in admin.create_topics(new, request_timeout=30).items():
        try:
            future.result()
            log.info("topic_creado", topic=topic)
        except Exception as exc:  # noqa: BLE001 - KafkaException envuelve el código
            code = exc.args[0].code() if exc.args and hasattr(exc.args[0], "code") else None
            if code != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
            log.info("topic_ya_existia", topic=topic)
    metadata = admin.list_topics(timeout=30)
    return {t: len(metadata.topics[t].partitions) for t in wanted if t in metadata.topics}


def expected_partitions(settings: Settings) -> dict[str, int]:
    """Particiones esperadas por topic (blueprint §5).

    Args:
        settings: Configuración con los nombres de topic.

    Returns:
        ``{station_status: 3, station_status_raw: 1}`` con los nombres configurados.
    """
    return {settings.kafka_topic_status: 3, settings.kafka_topic_raw: 1}


def main(argv: list[str] | None = None) -> int:
    """CLI ``python -m radar.streaming.producer --ensure-topics --check``.

    Args:
        argv: Argumentos (None = ``sys.argv``).

    Returns:
        0 si los topics tienen las particiones esperadas, 3 si no, 2 si el uso es inválido.
    """
    parser = argparse.ArgumentParser(description="Administra los topics de Radar ECOBICI.")
    parser.add_argument("--ensure-topics", action="store_true", help="crea los topics si faltan")
    parser.add_argument("--check", action="store_true", help="valida particiones (sale 3 si no)")
    args = parser.parse_args(argv)
    if not (args.ensure_topics or args.check):
        parser.error("usa --ensure-topics y/o --check")
    settings = get_settings()
    configure_logging(settings.log_level)

    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
    if args.ensure_topics:
        actual = ensure_topics(settings, admin)
    else:
        metadata = admin.list_topics(timeout=30)
        actual = {
            t: len(metadata.topics[t].partitions)
            for t in expected_partitions(settings)
            if t in metadata.topics
        }
    expected = expected_partitions(settings)
    ok = actual == expected
    print(json.dumps({"bootstrap": settings.kafka_bootstrap, "topics": actual, "ok": ok}))
    if args.check and not ok:
        log.error("topics_no_coinciden", esperado=expected, actual=actual)
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
