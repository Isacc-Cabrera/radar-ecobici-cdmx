"""
Autor: Isacc Cabrera
Descripción: Registro de ejecución (heartbeat) del recolector, D-006. Cada corrida agrega una
    línea JSON a bronze/heartbeat/dt=YYYY-MM-DD.jsonl con hora, feed, resultado
    (sin_cambios | guardado | error), código HTTP, número de estaciones y duración. Así se
    distingue "el feed no cambió" de "el recolector no corrió" (un hueco en el archivo).
Patrón: N/A
Fecha de creación: 2026-11-09
Cambios:
    - 2026-11-09 · Isacc Cabrera · Creación (paso 3).
"""

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

Result = Literal["sin_cambios", "guardado", "error"]


@dataclass(frozen=True)
class Heartbeat:
    """Una línea del heartbeat (blueprint §4, bronze/heartbeat).

    Attributes:
        ts_utc: Inicio de la corrida en ISO-8601 UTC.
        feed: ``station_status`` o ``station_information``.
        result: ``sin_cambios``, ``guardado`` o ``error``.
        http_status: Código HTTP, o None si no hubo respuesta.
        n_stations: Número de estaciones en la respuesta, o None.
        duration_ms: Duración de la corrida.
        error: ``feed_congelado`` o el mensaje de la excepción, o None.
        last_updated: ``last_updated`` del feed, o None.
        sink: ``local`` o ``kafka``.
    """

    ts_utc: str
    feed: str
    result: Result
    http_status: int | None
    n_stations: int | None
    duration_ms: int
    error: str | None
    last_updated: int | None
    sink: str


class HeartbeatWriter:
    """Agrega líneas de heartbeat al archivo del día.

    Args:
        data_dir: Raíz del lake.
    """

    def __init__(self, data_dir: Path) -> None:
        self._dir = data_dir / "bronze" / "heartbeat"

    def append(self, hb: Heartbeat) -> Path:
        """Escribe ``hb`` como una línea JSON al final del archivo de su fecha (UTC).

        Args:
            hb: Registro a agregar.

        Returns:
            Ruta del archivo escrito.
        """
        self._dir.mkdir(parents=True, exist_ok=True)
        path = self._dir / f"dt={hb.ts_utc[:10]}.jsonl"
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(asdict(hb), ensure_ascii=False) + "\n")
            handle.flush()
        return path
