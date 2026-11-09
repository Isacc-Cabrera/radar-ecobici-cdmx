"""
Autor: Isacc Cabrera
Descripción: Diseño de los mensajes Kafka (blueprint §5). Convierte un snapshot de
    station_status en (a) un mensaje por estación para el topic station_status, con key =
    station_id para que cada estación caiga siempre en la misma partición y conserve su orden, y
    (b) un mensaje crudo comprimido para station_status_raw, que el consumidor bronze-writer usa
    para escribir bronze byte a byte igual. Funciones puras: sin red, fáciles de probar.
Patrón: N/A (funciones puras de transformación)
Fecha de creación: 2026-11-09
Cambios:
    - 2026-11-09 · Isacc Cabrera · Creación (paso 4).
"""

import gzip
import json
from dataclasses import dataclass, field
from typing import Any

RAW_KEY = b"station_status"


@dataclass(frozen=True)
class KafkaMessage:
    """Un mensaje listo para ``Producer.produce``.

    Attributes:
        key: Llave del mensaje (decide la partición).
        value: Cuerpo del mensaje.
        headers: Metadatos ``(nombre, bytes)``.
    """

    key: bytes
    value: bytes
    headers: list[tuple[str, bytes]] = field(default_factory=list)


def snapshot_id(last_updated: int) -> str:
    """Identificador del snapshot, igual al nombre base del archivo bronze.

    Args:
        last_updated: ``last_updated`` del feed.

    Returns:
        ``"station_status_<last_updated>"``.
    """
    return f"station_status_{last_updated}"


def station_messages(payload: dict[str, Any]) -> list[KafkaMessage]:
    """Un mensaje JSON por estación para el topic ``station_status``.

    Args:
        payload: JSON decodificado de ``station_status``.

    Returns:
        Lista de mensajes con key ``station_id`` y valor = campos de la estación +
        ``feed_last_updated`` + ``snapshot_id``.
    """
    last_updated = int(payload["last_updated"])
    sid = snapshot_id(last_updated)
    messages: list[KafkaMessage] = []
    for station in payload["data"]["stations"]:
        body = {**station, "feed_last_updated": last_updated, "snapshot_id": sid}
        messages.append(
            KafkaMessage(
                key=str(station["station_id"]).encode("utf-8"),
                value=json.dumps(body, ensure_ascii=False, sort_keys=True).encode("utf-8"),
            )
        )
    return messages


def raw_message(raw: bytes, last_updated: int) -> KafkaMessage:
    """Mensaje con el snapshot crudo comprimido para ``station_status_raw``.

    ``mtime=0`` hace que los mismos bytes de entrada den los mismos bytes comprimidos.

    Args:
        raw: Cuerpo HTTP tal como llegó.
        last_updated: ``last_updated`` del feed (va en un header).

    Returns:
        Mensaje con key ``station_status`` y headers ``last_updated`` y ``content-encoding``.
    """
    return KafkaMessage(
        key=RAW_KEY,
        value=gzip.compress(raw, mtime=0),
        headers=[
            ("last_updated", str(last_updated).encode("ascii")),
            ("content-encoding", b"gzip"),
        ],
    )
