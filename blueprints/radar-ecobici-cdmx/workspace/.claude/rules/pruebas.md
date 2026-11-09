---
description: Convenciones de pruebas con pytest
paths:
  - "tests/**"
---

# Pruebas

- `uv run pytest` corre sin red: HTTP con `respx` (`respx.mock(assert_all_mocked=True)`), Kafka
  con objetos falsos que imitan `produce`/`poll`/`commit`, S3 con `LocalLakeStore`, Ollama con un
  embebedor falso.
- Las respuestas del feed vienen de `tests/fixtures/*.json` vía la fixture `load_fixture`. En esos
  datos: la estación `"3"` viola la regla de capacidad (5+0+6+0=11 ≠ 12), la `"999"` no existe en
  `station_information` y la `"2"` tiene 0 bicis. `last_updated` = 1794160800
  (2026-11-08T18:00:00Z → partición `dt=2026-11-08/hour=18`).
- Usa la fixture `settings` (sin `.env` real, `data_dir` en `tmp_path`); nunca escribas en `data/`.
- Nada de `time.sleep` real: inyecta un `sleep` falso que registra las esperas.
- Una prueba por comportamiento con nombre que lo describe (`test_429_respeta_retry_after`).
- Unitarias en `tests/unit/`, la API completa con `TestClient` en `tests/integration/`.
- Nombres de archivo de prueba únicos en todo `tests/` (no hay `__init__.py`).
- Toda prueba nueva lleva el encabezado obligatorio.
- Prohibido `pytest.skip`/`xfail` para esconder una falla: el gate exige 0 failed y 0 skipped.
