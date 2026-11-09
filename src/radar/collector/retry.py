"""
Autor: Isacc Cabrera
Descripción: Política de reintentos del recolector. Decide, ante cada falla, si se reintenta y
    cuánto se espera: 4xx no se reintenta (el error es nuestro), 429 respeta Retry-After, y 5xx,
    timeouts o errores de conexión usan backoff exponencial con "full jitter" y tope de 300 s.
Patrón: Strategy (RetryPolicy es la interfaz; ExponentialJitterPolicy una estrategia concreta)
Fecha de creación: 2026-11-09
Cambios:
    - 2026-11-09 · Isacc Cabrera · Creación (paso 2).
"""

import random
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Protocol

import httpx


@dataclass(frozen=True)
class RetryDecision:
    """Resultado de consultar la política tras una falla.

    Attributes:
        retry: True si se debe intentar otra vez.
        delay_s: Segundos a esperar antes del siguiente intento.
        reason: Motivo legible para los logs.
    """

    retry: bool
    delay_s: float
    reason: str


class RetryPolicy(Protocol):
    """Interfaz de una política de reintentos (Strategy)."""

    def decide(
        self,
        attempt: int,
        status: int | None,
        retry_after: str | None,
        error: Exception | None,
    ) -> RetryDecision:
        """Decide qué hacer después del intento fallido número ``attempt`` (empieza en 0).

        Args:
            attempt: Índice del intento que acaba de fallar (0 = el primero).
            status: Código HTTP recibido, o None si no hubo respuesta.
            retry_after: Valor del encabezado ``Retry-After`` si vino.
            error: Excepción de red si no hubo respuesta.

        Returns:
            La decisión de reintentar y cuánto esperar.
        """
        ...


def parse_retry_after(value: str | None, now: datetime | None = None) -> float | None:
    """Convierte un ``Retry-After`` (segundos o fecha HTTP) a segundos de espera.

    Args:
        value: Valor crudo del encabezado.
        now: Hora de referencia para el formato de fecha (por omisión, ahora en UTC).

    Returns:
        Segundos a esperar (nunca negativos), o None si el valor no se pudo interpretar.
    """
    if value is None:
        return None
    text = value.strip()
    if text.isdigit():
        return float(text)
    try:
        when = parsedate_to_datetime(text)
    except (TypeError, ValueError):
        return None
    reference = now or datetime.now(UTC)
    return max(0.0, (when - reference).total_seconds())


@dataclass
class ExponentialJitterPolicy:
    """Backoff exponencial con full jitter: espera = uniforme(0, min(tope, base · 2^intento)).

    El jitter reparte los reintentos en el tiempo para no golpear al servidor todos a la vez
    cuando se recupera. ``rng`` es inyectable para que las pruebas sean deterministas.

    Attributes:
        base_s: Espera base en segundos.
        cap_s: Tope de espera en segundos (D-006: 5 minutos).
        max_attempts: Número máximo de intentos totales antes de rendirse.
        rng: Generador aleatorio (con semilla en pruebas).
    """

    base_s: float
    cap_s: float
    max_attempts: int = 6
    rng: random.Random = field(default_factory=random.Random)

    def decide(
        self,
        attempt: int,
        status: int | None,
        retry_after: str | None,
        error: Exception | None,
    ) -> RetryDecision:
        """Aplica las reglas de D-006 al intento fallido. Ver ``RetryPolicy.decide``."""
        if attempt + 1 >= self.max_attempts:
            return RetryDecision(False, 0.0, "reintentos_agotados")
        if status == 429:
            seconds = parse_retry_after(retry_after)
            if seconds is not None:
                return RetryDecision(True, min(self.cap_s, seconds), "429_retry_after")
            return RetryDecision(True, self._backoff(attempt), "429_sin_retry_after")
        if status is not None and 400 <= status < 500:
            return RetryDecision(False, 0.0, f"http_{status}_no_reintentable")
        if status is not None and status >= 500:
            return RetryDecision(True, self._backoff(attempt), f"http_{status}")
        if isinstance(error, httpx.TimeoutException | httpx.TransportError):
            return RetryDecision(True, self._backoff(attempt), type(error).__name__)
        return RetryDecision(False, 0.0, "error_no_reintentable")

    def _backoff(self, attempt: int) -> float:
        """Calcula la espera con full jitter para el intento dado."""
        return self.rng.uniform(0.0, min(self.cap_s, self.base_s * 2**attempt))
