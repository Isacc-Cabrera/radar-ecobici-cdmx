"""
Autor: Isacc Cabrera
Descripción: Cliente del feed GBFS de ECOBICI. Descubre las URLs de station_status y
    station_information desde el índice gbfs.json (nunca se escriben a mano) y descarga cada
    feed aplicando la política de reintentos inyectada. Devuelve el cuerpo crudo (para bronze)
    y el JSON decodificado (para la lógica del recolector).
Patrón: Strategy (recibe una RetryPolicy) + Dependency Injection (http, policy y sleep inyectados)
Fecha de creación: 2026-11-09
Cambios:
    - 2026-11-09 · Isacc Cabrera · Creación (paso 2).
"""

import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import httpx

from radar.collector.retry import RetryPolicy
from radar.config import Settings
from radar.logging import get_logger

USER_AGENT = "radar-ecobici-cdmx/0.1 (+https://github.com/Isacc-Cabrera/radar-ecobici-cdmx)"
REQUIRED_FEEDS = ("station_status", "station_information")

log = get_logger(__name__)


class FeedError(Exception):
    """Error base del cliente GBFS.

    Attributes:
        http_status: Código HTTP de la última respuesta, si hubo.
    """

    def __init__(self, message: str, http_status: int | None = None) -> None:
        super().__init__(message)
        self.http_status = http_status


class FeedSchemaError(FeedError):
    """El índice o el feed no tienen la forma esperada (idioma o feeds faltantes, JSON roto)."""


class FeedClientError(FeedError):
    """Respuesta 4xx no reintentable: el error está de nuestro lado (URL, permisos)."""


class FeedUnavailableError(FeedError):
    """Se agotaron los reintentos ante 5xx, 429 o fallas de red."""


@dataclass(frozen=True)
class FetchResult:
    """Respuesta exitosa de un feed.

    Attributes:
        payload: JSON decodificado.
        raw: Cuerpo HTTP tal como llegó (lo que se guarda en bronze).
        http_status: Código HTTP (200).
        duration_ms: Duración total, incluyendo reintentos y esperas.
    """

    payload: dict[str, Any]
    raw: bytes
    http_status: int
    duration_ms: int


class GbfsClient:
    """Cliente GBFS con descubrimiento de feeds y reintentos.

    Args:
        settings: Configuración (``gbfs_url``, ``gbfs_lang``).
        http: Cliente httpx ya configurado (en pruebas, interceptado por respx).
        policy: Estrategia de reintentos.
        sleep: Función de espera (``time.sleep`` en producción, falsa en pruebas).
    """

    def __init__(
        self,
        settings: Settings,
        http: httpx.Client,
        policy: RetryPolicy,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._settings = settings
        self._http = http
        self._policy = policy
        self._sleep = sleep
        self._feeds: dict[str, str] | None = None

    def discover(self) -> dict[str, str]:
        """Lee ``gbfs.json`` y devuelve ``{nombre_feed: url}`` para el idioma configurado.

        Returns:
            Diccionario de feeds del idioma ``GBFS_LANG``.

        Raises:
            FeedSchemaError: Si falta el idioma o alguno de los feeds requeridos.
        """
        result = self._get(self._settings.gbfs_url)
        languages = result.payload.get("data", {})
        lang = self._settings.gbfs_lang
        if lang not in languages:
            raise FeedSchemaError(f"idioma '{lang}' ausente en gbfs.json", result.http_status)
        feeds = {f["name"]: f["url"] for f in languages[lang].get("feeds", [])}
        missing = [name for name in REQUIRED_FEEDS if name not in feeds]
        if missing:
            raise FeedSchemaError(f"feeds faltantes en gbfs.json: {missing}", result.http_status)
        self._feeds = feeds
        log.info("gbfs_descubierto", idioma=lang, feeds=sorted(feeds))
        return feeds

    def fetch(self, name: str) -> FetchResult:
        """Descarga un feed por nombre, descubriendo las URLs la primera vez.

        Args:
            name: Nombre del feed (``station_status`` o ``station_information``).

        Returns:
            El resultado con cuerpo crudo y JSON.

        Raises:
            FeedClientError: Ante un 4xx no reintentable (se olvida el índice para redescubrirlo).
            FeedUnavailableError: Si se agotan los reintentos.
            FeedSchemaError: Si el feed no existe o su JSON no es válido.
        """
        feeds = self._feeds if self._feeds is not None else self.discover()
        if name not in feeds:
            raise FeedSchemaError(f"feed '{name}' no está en el índice")
        try:
            return self._get(feeds[name])
        except FeedClientError:
            self._feeds = None  # la URL pudo cambiar: el siguiente fetch redescubre
            raise

    def _get(self, url: str) -> FetchResult:
        """GET con la política de reintentos; devuelve solo respuestas 200 con JSON válido."""
        started = time.perf_counter()
        attempt = 0
        while True:
            status: int | None = None
            retry_after: str | None = None
            error: Exception | None = None
            try:
                response = self._http.get(url, headers={"User-Agent": USER_AGENT})
                status = response.status_code
                if status == 200:
                    return self._parse(response, started)
                retry_after = response.headers.get("Retry-After")
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                error = exc
            decision = self._policy.decide(attempt, status, retry_after, error)
            log.warning(
                "gbfs_intento_fallido",
                url=url,
                intento=attempt,
                http_status=status,
                motivo=decision.reason,
                reintenta=decision.retry,
                espera_s=round(decision.delay_s, 2),
            )
            if not decision.retry:
                if status is not None and 400 <= status < 500 and status != 429:
                    raise FeedClientError(f"HTTP {status} en {url}", status)
                raise FeedUnavailableError(f"feed no disponible ({decision.reason})", status)
            self._sleep(decision.delay_s)
            attempt += 1

    @staticmethod
    def _parse(response: httpx.Response, started: float) -> FetchResult:
        """Decodifica el JSON de una respuesta 200 y mide la duración."""
        raw = response.content
        try:
            payload = json.loads(raw)
        except ValueError as exc:
            raise FeedSchemaError(f"JSON inválido: {exc}", response.status_code) from exc
        if not isinstance(payload, dict):
            raise FeedSchemaError("el feed no es un objeto JSON", response.status_code)
        duration_ms = int((time.perf_counter() - started) * 1000)
        return FetchResult(payload, raw, response.status_code, duration_ms)
