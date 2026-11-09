"""
Autor: Isacc Cabrera
Descripción: Verifica la regla de encabezado obligatorio: todo archivo fuente (py, ts/tsx, mjs,
    sql, Dockerfile, yaml, tf, sh) declara autor, descripción, patrón, fecha y cambios.
Patrón: N/A (prueba de convención)
Fecha de creación: 2026-11-08
Cambios:
    - 2026-11-08 · Isacc Cabrera · Creación desde el blueprint.
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SOURCE_DIRS = (
    "src",
    "tests",
    "scripts",
    "dags",
    "sql",
    "docker",
    "infra",
    "frontend/src",
    ".github",
)
SOURCE_FILES = ("docker-compose.yml", "frontend/vite.config.ts", "frontend/scripts/smoke.mjs")
EXTENSIONS = {".py", ".ts", ".tsx", ".mjs", ".sql", ".tf", ".sh", ".yml", ".yaml"}
EXCLUDED_PARTS = {"node_modules", ".terraform", "fixtures", "__pycache__", "dist"}
REQUIRED_KEYS = (
    "Autor: Isacc Cabrera",
    "Descripción:",
    "Patrón:",
    "Fecha de creación:",
    "Cambios:",
)
HEADER_LINES = 25


def _is_source(path: Path) -> bool:
    """Indica si una ruta es un archivo fuente sujeto a la regla de encabezado."""
    if any(part in EXCLUDED_PARTS for part in path.parts):
        return False
    return path.suffix in EXTENSIONS or path.name.endswith(".Dockerfile")


def _source_files() -> list[Path]:
    """Enumera los archivos fuente del proyecto (nunca recorre blueprints/)."""
    found: list[Path] = []
    for name in SOURCE_DIRS:
        base = ROOT / name
        if base.is_dir():
            found.extend(p for p in base.rglob("*") if p.is_file() and _is_source(p))
    found.extend(ROOT / name for name in SOURCE_FILES if (ROOT / name).is_file())
    return sorted(set(found))


def test_every_source_file_has_the_mandatory_header() -> None:
    """Cada archivo fuente trae los cinco campos del encabezado en sus primeras líneas."""
    missing: list[str] = []
    for path in _source_files():
        head = "\n".join(path.read_text(encoding="utf-8").splitlines()[:HEADER_LINES])
        if not all(key in head for key in REQUIRED_KEYS):
            missing.append(path.relative_to(ROOT).as_posix())
    assert missing == [], f"Archivos sin encabezado obligatorio: {missing}"
