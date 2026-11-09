"""
Autor: Isacc Cabrera
Descripción: Destinos de los snapshots del recolector. SnapshotSink es el puerto (la interfaz
    que el recolector conoce) y LocalFileSink el adaptador que escribe bronze en disco: gzip
    del cuerpo crudo, escritura atómica y nombre de archivo = last_updated del feed, de modo
    que reescribir el mismo snapshot no duplica nada (idempotencia por nombre). make_sink elige
    el adaptador según SINK; el paso 4 agrega KafkaSink sin tocar el recolector.
Patrón: Ports & Adapters (SnapshotSink / LocalFileSink) + Factory (make_sink)
Fecha de creación: 2026-11-09
Cambios:
    - 2026-11-09 · Isacc Cabrera · Creación con LocalFileSink (paso 3).
"""

import gzip
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol

from radar.config import Settings

WriteResult = Literal["guardado", "ya_existia"]


class SnapshotSink(Protocol):
    """Puerto: a dónde van los snapshots crudos del feed."""

    name: str

    def write_status(self, raw: bytes, payload: dict[str, Any]) -> WriteResult:
        """Guarda un snapshot de ``station_status``.

        Args:
            raw: Cuerpo HTTP tal como llegó.
            payload: JSON decodificado (de aquí sale ``last_updated``).

        Returns:
            ``"guardado"`` si se escribió, ``"ya_existia"`` si ese snapshot ya estaba.
        """
        ...

    def write_information(self, raw: bytes, payload: dict[str, Any], sha8: str) -> WriteResult:
        """Guarda una versión de ``station_information``.

        Args:
            raw: Cuerpo HTTP tal como llegó.
            payload: JSON decodificado.
            sha8: Primeros 8 caracteres del hash de estaciones (parte del nombre).

        Returns:
            ``"guardado"`` o ``"ya_existia"``.
        """
        ...


def partition_of(last_updated: int) -> tuple[str, str]:
    """Calcula la partición ``(dt, hour)`` en UTC a partir de ``last_updated``.

    Args:
        last_updated: Época en segundos del feed.

    Returns:
        Tupla ``("YYYY-MM-DD", "HH")``.
    """
    moment = datetime.fromtimestamp(last_updated, tz=UTC)
    return moment.strftime("%Y-%m-%d"), moment.strftime("%H")


def status_path(data_dir: Path, last_updated: int) -> Path:
    """Ruta bronze de un snapshot de ``station_status`` (blueprint §4)."""
    dt, hour = partition_of(last_updated)
    return (
        data_dir
        / "bronze"
        / "station_status"
        / f"dt={dt}"
        / f"hour={hour}"
        / f"station_status_{last_updated}.json.gz"
    )


def information_path(data_dir: Path, last_updated: int, sha8: str) -> Path:
    """Ruta bronze de una versión de ``station_information`` (blueprint §4)."""
    dt, _ = partition_of(last_updated)
    return (
        data_dir
        / "bronze"
        / "station_information"
        / f"dt={dt}"
        / f"station_information_{sha8}.json.gz"
    )


def write_atomic_gzip(path: Path, raw: bytes) -> WriteResult:
    """Escribe ``raw`` comprimido sin dejar archivos a medias y sin sobrescribir.

    Escribe en ``<ruta>.tmp``, hace ``fsync`` y renombra con ``os.replace`` (atómico en el
    mismo disco). Si el destino ya existe no lo toca: bronze nunca se modifica.

    Args:
        path: Ruta final.
        raw: Bytes a comprimir.

    Returns:
        ``"guardado"`` o ``"ya_existia"``.
    """
    if path.exists():
        return "ya_existia"
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "wb") as handle:
        handle.write(gzip.compress(raw, mtime=0))  # mtime=0: mismos bytes => mismo archivo
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    return "guardado"


class LocalFileSink:
    """Adaptador que escribe bronze en el disco local.

    Args:
        data_dir: Raíz del lake (``Settings.data_dir``).
    """

    name = "local"

    def __init__(self, data_dir: Path) -> None:
        self._data_dir = data_dir

    def write_status(self, raw: bytes, payload: dict[str, Any]) -> WriteResult:
        """Guarda el snapshot en ``bronze/station_status/dt=/hour=/``. Ver ``SnapshotSink``."""
        return write_atomic_gzip(status_path(self._data_dir, int(payload["last_updated"])), raw)

    def write_information(self, raw: bytes, payload: dict[str, Any], sha8: str) -> WriteResult:
        """Guarda la versión en ``bronze/station_information/dt=/``. Ver ``SnapshotSink``."""
        path = information_path(self._data_dir, int(payload["last_updated"]), sha8)
        return write_atomic_gzip(path, raw)


def make_sink(settings: Settings) -> SnapshotSink:
    """Fabrica el sink configurado en ``SINK``.

    Args:
        settings: Configuración con ``sink`` y ``data_dir``.

    Returns:
        El adaptador correspondiente.

    Raises:
        ValueError: Si ``SINK`` no es un valor conocido.
    """
    if settings.sink == "local":
        return LocalFileSink(settings.data_dir)
    raise ValueError(f"SINK desconocido: {settings.sink!r}")
