# scripts/

Scripts de operación que se corren con `uv run python scripts/<archivo>.py` (o `bash` para `.sh`).

| Script | Para qué | Paso |
|---|---|---|
| `demo_recovery.sh` | Demo de recuperación del consumidor Kafka | 5 |
| `lake_setup.py` | Identidades S3, buckets y prueba de mínimo privilegio en SeaweedFS | 6 |
| `sync_lake.py` | Sube bronze/silver/gold locales a SeaweedFS (nunca viajes crudos) | 6 |
| `run_hourly.py` | Pipeline horario + checks de calidad (fallback de Airflow) | 9 |
| `sync_gold_to_s3.sh` | Sube gold a AWS S3 (solo tras aprobar costos) | 16 |
