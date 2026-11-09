"""
Autor: Isacc Cabrera
Descripción: Loop principal del recolector. Cada STATUS_INTERVAL_SECONDS (60 s, D-005) descarga
    station_status y lo manda al sink; station_information se descarga al arrancar, cada
    INFO_REFRESH_HOURS y cuando aparece un station_id desconocido, y solo se guarda si cambió su
    hash. Cada corrida deja un heartbeat. Ninguna excepción detiene el loop: se registra como
    heartbeat "error" y se sigue. Con RUN_ONCE=true hace una vuelta y sale (para verificar).
Patrón: Dependency Injection (cliente, sink, heartbeat, reloj y sleep se inyectan)
Fecha de creación: 2026-11-09
Cambios:
    - 2026-11-09 · Isacc Cabrera · Creación (paso 3).
"""

import json
import sys
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

import httpx

from radar.collector.gbfs_client import FeedError, FetchResult, GbfsClient
from radar.collector.hashing import find_unknown_station_ids, station_ids, stations_hash
from radar.collector.heartbeat import Heartbeat, HeartbeatWriter, Result
from radar.collector.retry import ExponentialJitterPolicy
from radar.collector.sinks import SnapshotSink, make_sink
from radar.config import Settings, get_settings
from radar.logging import configure_logging, get_logger

log = get_logger(__name__)


class FeedFetcher(Protocol):
    """Lo único que el runner necesita del cliente GBFS (facilita usar un falso en pruebas)."""

    def fetch(self, name: str) -> FetchResult:
        """Descarga un feed por nombre."""
        ...


@dataclass
class CollectorState:
    """Estado persistido en ``data/state/collector.json`` (no es dato crudo).

    Attributes:
        last_status_last_updated: Último ``last_updated`` visto de ``station_status``.
        last_status_change_utc: Cuándo cambió por última vez (ISO-8601 UTC).
        info_hash: Hash de la última ``station_information`` guardada.
        info_refreshed_utc: Cuándo se consultó ``station_information`` por última vez.
    """

    last_status_last_updated: int | None = None
    last_status_change_utc: str | None = None
    info_hash: str | None = None
    info_refreshed_utc: str | None = None

    @classmethod
    def load(cls, path: Path) -> "CollectorState":
        """Lee el estado; si no existe o está corrupto, empieza vacío."""
        try:
            return cls(**json.loads(path.read_text(encoding="utf-8")))
        except (FileNotFoundError, ValueError, TypeError):
            return cls()

    def save(self, path: Path) -> None:
        """Guarda el estado de forma atómica."""
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(json.dumps(asdict(self)), encoding="utf-8")
        tmp.replace(path)


def _utc_now() -> datetime:
    """Hora actual en UTC (inyectable en pruebas)."""
    return datetime.now(UTC)


class CollectorRunner:
    """Orquesta las descargas, el sink y el heartbeat.

    Args:
        settings: Configuración del recolector.
        client: Algo con ``fetch(nombre)`` (``GbfsClient`` en producción).
        sink: Destino de los snapshots.
        heartbeat: Escritor del heartbeat.
        clock: Función que da la hora UTC actual.
        sleep: Función de espera.
        monotonic: Reloj monotónico para agendar cada intervalo.
    """

    def __init__(
        self,
        settings: Settings,
        client: FeedFetcher,
        sink: SnapshotSink,
        heartbeat: HeartbeatWriter,
        clock: Callable[[], datetime] = _utc_now,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._settings = settings
        self._client = client
        self._sink = sink
        self._heartbeat = heartbeat
        self._clock = clock
        self._sleep = sleep
        self._monotonic = monotonic
        self._state_path = settings.data_dir / "state" / "collector.json"
        self.state = CollectorState.load(self._state_path)
        self.known_ids: set[str] = set()
        self._unknown_reported: set[str] = set()

    # ------------------------------------------------------------------ station_status
    def tick_status(self) -> Result:
        """Una corrida de ``station_status``: descarga, decide, guarda y deja heartbeat.

        Returns:
            El resultado registrado en el heartbeat.
        """
        started = self._clock()
        try:
            fetched = self._client.fetch("station_status")
        except Exception as exc:  # el loop nunca muere: todo error queda en el heartbeat
            return self._record_error("station_status", started, exc)

        payload = fetched.payload
        last_updated = int(payload["last_updated"])
        n_stations = len(payload["data"]["stations"])
        error: str | None = None

        if last_updated == self.state.last_status_last_updated:
            result: Result = "sin_cambios"
            if self._is_stale(started):
                result, error = "error", "feed_congelado"
                log.warning(
                    "feed_congelado",
                    last_updated=last_updated,
                    desde=self.state.last_status_change_utc,
                )
        else:
            written = self._sink.write_status(fetched.raw, payload)
            result = "guardado" if written == "guardado" else "sin_cambios"
            self.state.last_status_last_updated = last_updated
            self.state.last_status_change_utc = started.isoformat()
            self.state.save(self._state_path)

        self._write_heartbeat(
            "station_status", started, result, fetched, n_stations, error, last_updated
        )

        unknown = find_unknown_station_ids(payload, self.known_ids) if self.known_ids else set()
        new_unknown = unknown - self._unknown_reported
        if new_unknown:
            # D-005: se guardó igual; se registra y se refresca information una sola vez por id
            # nuevo, para no consultar el feed cada minuto si la estación sigue sin registrarse.
            log.warning("station_id_desconocido", ids=sorted(new_unknown))
            self._unknown_reported |= new_unknown
            self.refresh_information(force=True)
        return result

    def _is_stale(self, now: datetime) -> bool:
        """Indica si ``last_updated`` lleva congelado más de ``STALE_ALERT_SECONDS``."""
        if self.state.last_status_change_utc is None:
            return False
        since = datetime.fromisoformat(self.state.last_status_change_utc)
        return (now - since).total_seconds() > self._settings.stale_alert_seconds

    # ------------------------------------------------------------- station_information
    def refresh_information(self, force: bool = False) -> Result | None:
        """Consulta ``station_information`` si toca (o si ``force``) y la guarda si cambió.

        Args:
            force: Consultar aunque no haya pasado ``INFO_REFRESH_HOURS``.

        Returns:
            El resultado del heartbeat, o None si no tocaba consultar.
        """
        started = self._clock()
        if not force and not self._info_due(started):
            return None
        try:
            fetched = self._client.fetch("station_information")
            payload = fetched.payload
            digest = stations_hash(payload)
        except Exception as exc:
            return self._record_error("station_information", started, exc)

        self.known_ids = station_ids(payload)
        result: Result = "sin_cambios"
        if digest != self.state.info_hash:
            written = self._sink.write_information(fetched.raw, payload, digest[:8])
            result = "guardado" if written == "guardado" else "sin_cambios"
            self.state.info_hash = digest
            log.info("station_information_nueva_version", sha8=digest[:8], resultado=written)
        self.state.info_refreshed_utc = started.isoformat()
        self.state.save(self._state_path)
        self._write_heartbeat(
            "station_information",
            started,
            result,
            fetched,
            len(self.known_ids),
            None,
            int(payload.get("last_updated", 0)) or None,
        )
        return result

    def _info_due(self, now: datetime) -> bool:
        """Indica si ya pasaron ``INFO_REFRESH_HOURS`` desde la última consulta."""
        if self.state.info_refreshed_utc is None or not self.known_ids:
            return True
        since = datetime.fromisoformat(self.state.info_refreshed_utc)
        return (now - since).total_seconds() >= self._settings.info_refresh_hours * 3600

    # ------------------------------------------------------------------ heartbeat
    def _write_heartbeat(
        self,
        feed: str,
        started: datetime,
        result: Result,
        fetched: FetchResult | None,
        n_stations: int | None,
        error: str | None,
        last_updated: int | None,
    ) -> None:
        """Arma y escribe una línea de heartbeat."""
        duration_ms = int((self._clock() - started).total_seconds() * 1000)
        if fetched is not None:
            duration_ms = max(duration_ms, fetched.duration_ms)
        self._heartbeat.append(
            Heartbeat(
                ts_utc=started.isoformat(),
                feed=feed,
                result=result,
                http_status=fetched.http_status if fetched else None,
                n_stations=n_stations,
                duration_ms=duration_ms,
                error=error,
                last_updated=last_updated,
                sink=self._sink.name,
            )
        )
        log.info("heartbeat", feed=feed, resultado=result, estaciones=n_stations, error=error)

    def _record_error(self, feed: str, started: datetime, exc: Exception) -> Result:
        """Registra una excepción como heartbeat ``error`` sin propagarla."""
        log.error("corrida_fallida", feed=feed, error=repr(exc))
        status = exc.http_status if isinstance(exc, FeedError) else None
        duration_ms = int((self._clock() - started).total_seconds() * 1000)
        try:
            self._heartbeat.append(
                Heartbeat(
                    ts_utc=started.isoformat(),
                    feed=feed,
                    result="error",
                    http_status=status,
                    n_stations=None,
                    duration_ms=duration_ms,
                    error=f"{type(exc).__name__}: {exc}"[:500],
                    last_updated=None,
                    sink=self._sink.name,
                )
            )
        except OSError as io_exc:  # ni el disco lleno debe matar el proceso
            log.error("heartbeat_no_escrito", error=repr(io_exc))
        return "error"

    # ------------------------------------------------------------------ loops
    def run_once(self) -> Result:
        """Una vuelta completa: ``station_information`` forzada + un ``station_status``."""
        self.refresh_information(force=True)
        return self.tick_status()

    def run_forever(self, max_ticks: int | None = None) -> None:
        """Corre cada ``STATUS_INTERVAL_SECONDS`` para siempre (o ``max_ticks`` en pruebas).

        Agenda con reloj monotónico: si una corrida tarda 5 s, la espera siguiente es de 55 s,
        así el ritmo se mantiene en 60 s y no se va corriendo.

        Args:
            max_ticks: Número de vueltas antes de salir (solo para pruebas).
        """
        interval = self._settings.status_interval_seconds
        self.refresh_information(force=True)
        next_run = self._monotonic()
        ticks = 0
        while max_ticks is None or ticks < max_ticks:
            try:
                self.tick_status()
                self.refresh_information()
            except Exception as exc:  # defensa final: nada sale del loop
                log.error("error_inesperado_en_loop", error=repr(exc))
            ticks += 1
            next_run += interval
            delay = next_run - self._monotonic()
            if delay < 0:  # nos atrasamos (p. ej. la laptop se suspendió): reagenda desde ahora
                next_run = self._monotonic()
                delay = 0.0
            if max_ticks is None or ticks < max_ticks:
                self._sleep(delay)


def build_runner(settings: Settings) -> CollectorRunner:
    """Arma el runner de producción con sus dependencias reales.

    Args:
        settings: Configuración.

    Returns:
        Runner listo para ``run_once`` o ``run_forever``.
    """
    http = httpx.Client(timeout=settings.http_timeout_seconds, follow_redirects=True)
    policy = ExponentialJitterPolicy(
        base_s=settings.retry_base_seconds, cap_s=settings.retry_cap_seconds
    )
    client = GbfsClient(settings, http, policy)
    return CollectorRunner(
        settings, client, make_sink(settings), HeartbeatWriter(settings.data_dir)
    )


def main() -> int:
    """Punto de entrada: ``python -m radar.collector.runner``.

    Returns:
        Con ``RUN_ONCE=true``: 0 si la vuelta fue ``guardado``/``sin_cambios``, 1 si fue
        ``error``. Sin ``RUN_ONCE`` no regresa (loop infinito).
    """
    settings = get_settings()
    configure_logging(settings.log_level)
    log.info(
        "recolector_inicia",
        sink=settings.sink,
        intervalo_s=settings.status_interval_seconds,
        run_once=settings.run_once,
    )
    runner = build_runner(settings)
    if settings.run_once:
        result = runner.run_once()
        return 0 if result in ("guardado", "sin_cambios") else 1
    runner.run_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
