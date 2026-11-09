"""
Autor: Isacc Cabrera
Descripción: Huella (hash) del contenido de station_information para detectar si cambió
    (D-005). Se calcula sobre la lista de estaciones en forma canónica (ordenada y sin
    last_updated ni ttl), porque last_updated puede cambiar aunque las estaciones sean iguales.
    También detecta station_id desconocidos en station_status.
Patrón: N/A (funciones puras)
Fecha de creación: 2026-11-09
Cambios:
    - 2026-11-09 · Isacc Cabrera · Creación (paso 2).
"""

import hashlib
import json
from typing import Any


def canonical_stations(payload: dict[str, Any]) -> bytes:
    """Serializa las estaciones de forma determinista.

    Args:
        payload: JSON de ``station_information`` con ``data.stations``.

    Returns:
        Bytes UTF-8 de la lista ordenada por ``station_id`` con llaves ordenadas.
    """
    stations = sorted(payload["data"]["stations"], key=lambda s: str(s["station_id"]))
    text = json.dumps(stations, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return text.encode("utf-8")


def stations_hash(payload: dict[str, Any]) -> str:
    """Calcula el sha256 hexadecimal del contenido canónico de las estaciones.

    Args:
        payload: JSON de ``station_information``.

    Returns:
        Hash de 64 caracteres hexadecimales.
    """
    return hashlib.sha256(canonical_stations(payload)).hexdigest()


def station_ids(payload: dict[str, Any]) -> set[str]:
    """Devuelve el conjunto de ``station_id`` de un feed (como texto).

    Args:
        payload: JSON de ``station_status`` o ``station_information``.

    Returns:
        Conjunto de identificadores.
    """
    return {str(s["station_id"]) for s in payload["data"]["stations"]}


def find_unknown_station_ids(status_payload: dict[str, Any], known_ids: set[str]) -> set[str]:
    """Encuentra estaciones de ``station_status`` que no existen en ``station_information``.

    Args:
        status_payload: JSON de ``station_status``.
        known_ids: ``station_id`` conocidos según la última ``station_information``.

    Returns:
        Los ``station_id`` desconocidos (vacío si todos se conocen).
    """
    return station_ids(status_payload) - known_ids
