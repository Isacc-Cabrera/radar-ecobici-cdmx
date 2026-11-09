"""
Autor: Isacc Cabrera
Descripción: Fixtures compartidas de pytest: carga de respuestas GBFS grabadas (sin red) y
    Settings aislados que ignoran el .env real y escriben en un directorio temporal.
Patrón: Factory (fixtures que fabrican objetos de prueba)
Fecha de creación: 2026-11-08
Cambios:
    - 2026-11-08 · Isacc Cabrera · Creación desde el blueprint.
"""

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def read_fixture(name: str) -> Any:
    """Lee un JSON grabado de tests/fixtures/.

    Args:
        name: Nombre del archivo dentro de tests/fixtures/.

    Returns:
        El contenido decodificado del JSON.
    """
    return json.loads((FIXTURES_DIR / name).read_text(encoding="utf-8"))


@pytest.fixture
def load_fixture() -> Callable[[str], Any]:
    """Devuelve la función que carga un JSON grabado de tests/fixtures/."""
    return read_fixture


@pytest.fixture
def settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    """Settings aislados: sin .env real, con data_dir dentro de tmp_path."""
    from radar.config import Settings

    monkeypatch.chdir(tmp_path)
    return Settings(_env_file=None, data_dir=tmp_path / "data")
