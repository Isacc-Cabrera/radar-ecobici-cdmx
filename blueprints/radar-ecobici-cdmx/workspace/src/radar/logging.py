"""
Autor: Isacc Cabrera
Descripción: Configura structlog para emitir logs JSON a stderr. stdout queda libre para las
    salidas --json de los CLI, que los comandos Verify parsean.
Patrón: Singleton (configuración global idempotente del logger)
Fecha de creación: 2026-11-08
Cambios:
    - 2026-11-08 · Isacc Cabrera · Creación desde el blueprint.
"""

import logging
import sys
from typing import Any

import structlog

_state = {"configured": False}


def configure_logging(level: str = "INFO") -> None:
    """Configura structlog una sola vez por proceso.

    Args:
        level: Nivel mínimo de log (DEBUG, INFO, WARNING o ERROR).
    """
    if _state["configured"]:
        return
    numeric_level = logging.getLevelNamesMapping().get(level.upper(), logging.INFO)
    logging.basicConfig(stream=sys.stderr, level=numeric_level, format="%(message)s")
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=True,
    )
    _state["configured"] = True


def get_logger(name: str) -> Any:
    """Devuelve un logger estructurado con el nombre del módulo enlazado.

    Args:
        name: Nombre lógico del módulo que registra (normalmente ``__name__``).

    Returns:
        Un logger de structlog con el campo ``logger`` fijado.
    """
    return structlog.get_logger().bind(logger=name)
