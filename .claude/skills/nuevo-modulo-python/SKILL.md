---
name: nuevo-modulo-python
description: Crea un módulo Python nuevo en src/radar o scripts/ con el encabezado obligatorio, el patrón de diseño nombrado, docstrings Google en español y su prueba sin red. Úsala al decir "crea el módulo", "agrega un archivo en radar/", "nuevo script" o "nueva prueba".
---

# Nuevo módulo Python

## Cuándo usarla
- Cualquier archivo `.py` nuevo en `src/radar/**`, `scripts/**`, `dags/**` o `tests/**`.

## Pasos
1. Explica al usuario en 3–6 líneas: qué concepto usa el módulo, la alternativa principal y por
   qué se eligió esta (regla de trabajo D-008).
2. Escribe el encabezado obligatorio (ver `.claude/rules/encabezados.md`) con `Patrón:` real.
3. Lee configuración solo de `radar.config.Settings`; loguea con `radar.logging.get_logger`.
4. Si es CLI: `argparse`, `configure_logging(settings.log_level)` primero, `--json` a stdout,
   salida 0/2/3 (ok / uso / check fallido).
5. Escribe la prueba en `tests/unit/test_<tema>.py` con fixtures de `tests/fixtures/` y sin red.
6. Corre el gate.

## Verify
```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy   # expect: exit 0
uv run pytest tests/unit/test_encabezados.py                        # expect: 1 passed
```

## No hagas
- No uses imports relativos ni `sys.path`; no leas `os.environ` fuera de `config.py`.
- No agregues dependencias a `pyproject.toml` sin justificarlas en el commit.
