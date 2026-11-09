---
description: Convenciones del pipeline Python (recolector, streaming, lake, ML, API)
paths:
  - "src/radar/**"
  - "scripts/**"
  - "dags/**"
---

# Pipeline Python

- Configuración solo vía `radar.config.Settings` (pydantic-settings, `env_file=".env"`,
  `extra="ignore"`, `env_ignore_empty=True`). Nadie más lee `os.environ`.
- Imports absolutos `from radar.x import y`. Los scripts se corren con `uv run python scripts/x.py`
  y funcionan porque `uv sync` instala el paquete `radar` en modo editable.
- Todo CLI: `argparse`; `configure_logging(settings.log_level)` primero; resultado legible por
  máquina con `--json` a stdout; logs a stderr. Salida 0 = ok, 2 = error de uso, 3 = check fallido.
- Tiempos en UTC dentro del lake (`datetime.now(UTC)`); la hora local `America/Mexico_City` solo
  existe en features (hora del día, día de la semana, festivos).
- Rutas del lake (siempre relativas a `Settings.data_dir`):
  `bronze/station_status/dt=YYYY-MM-DD/hour=HH/station_status_<last_updated>.json.gz`,
  `bronze/station_information/dt=YYYY-MM-DD/station_information_<sha8>.json.gz`,
  `bronze/heartbeat/dt=YYYY-MM-DD.jsonl`,
  `silver/station_status/dt=YYYY-MM-DD/hour=HH/part-0.parquet` (y `silver/quarantine/...` igual),
  `gold/station_hour/dt=YYYY-MM-DD/part-0.parquet`. Tablas pequeñas sin partición
  (`silver/station_information/station_information.parquet`, `silver/weather/weather.parquet`,
  `gold/trip_demand/trip_demand.parquet`) y viajes `silver/trips/month=YYYY-MM/part-0.parquet`:
  ver blueprint §4. `dt`/`hour` salen del `last_updated` del feed en UTC.
- Bronze: escritura atómica (archivo temporal + `fsync` + `os.replace`); si el destino existe, no
  se toca (idempotencia por nombre). Nunca se borra.
- Silver/gold: cada job reemplaza su partición completa (escribe en `*.tmp/` y renombra) y deja
  `_manifest.json` sin marcas de tiempo, para que dos corridas den el mismo manifiesto.
- Errores de red: `RetryPolicy` (Strategy) — 4xx sin reintento salvo 429 (`Retry-After`), 5xx y
  timeouts con backoff exponencial + full jitter, tope 300 s. `sleep` y `random` inyectables.
- Patrones nombrados en el docstring: `SnapshotSink`/`LakeStore` (Ports & Adapters), fábricas
  `make_sink`/`make_lake_store` (Factory), `RetryPolicy`/`Predictor`/detectores (Strategy),
  checks de silver (Chain of Responsibility), `TransformJob` (Template Method), repositorio de la
  API (Repository), `Depends` (Dependency Injection), `ModelHolder` (Singleton en el lifespan).
- Los DAGs no contienen lógica: llaman `/opt/radar/.venv/bin/python /opt/radar/scripts/run_hourly.py`.
- Nunca escribas viajes crudos fuera de `data/bronze/trips/`; silver usa lista blanca de columnas.
