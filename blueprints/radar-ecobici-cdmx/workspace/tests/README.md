# tests/

`uv run pytest` — todas las pruebas corren sin red (respuestas GBFS grabadas en `fixtures/`).

- `unit/` — lógica pura: reintentos, hash, sinks, calidad, features, modelos, detectores.
- `integration/` — la API completa con `TestClient` y dependencias sustituidas.
- `fixtures/` — JSON grabados del feed GBFS (`gbfs.json`, `station_status.json`,
  `station_information.json`).
- `unit/test_encabezados.py` — exige el encabezado obligatorio en todo archivo fuente.
