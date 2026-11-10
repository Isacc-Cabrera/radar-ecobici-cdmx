# Runbook de Radar ECOBICI CDMX

Qué hacer cuando algo falla. Todos los comandos se corren en **Git Bash** desde la raíz del repo.
Regla de oro: **nunca** `docker compose down -v` (borra los volúmenes de Kafka, SeaweedFS,
Postgres de Airflow y MLflow). Para reiniciar un servicio se usa `restart` o `up -d <servicio>`.

| Servicio | Perfil | Qué hace |
|---|---|---|
| `collector-kafka` | `streaming` | consulta el feed GBFS cada 60 s y publica en Kafka |
| `kafka` | `streaming` | broker KRaft (sin ZooKeeper) |
| `bronze-writer` | `streaming` | consume `station_status_raw` y escribe `data/bronze/` |
| `seaweedfs` | `lake` | S3 local (réplica del lake) |
| `airflow` + `postgres-airflow` | `orquestacion` | DAGs `horario` y `diario` |
| `mlflow` | `ml`, `api` | tracking y registro del modelo |
| `api` | `api` | FastAPI en http://localhost:8000 |

Vista rápida de todo:

```bash
docker compose --profile streaming --profile lake --profile orquestacion --profile ml --profile api ps
```

## El recolector se detuvo

Síntomas: `/health` muestra `segundos_desde_ultimo_dato` creciendo; no aparecen archivos nuevos en
`data/bronze/station_status/dt=<hoy>/hour=<HH>/`; el heartbeat del día deja de crecer.

1. ¿Está vivo el contenedor?
   ```bash
   docker compose --profile streaming ps collector-kafka bronze-writer
   docker compose logs --tail 50 collector-kafka
   ```
2. ¿Qué dicen las últimas corridas? (`result`: `guardado`, `sin_cambios` o `error`)
   ```bash
   tail -5 "data/bronze/heartbeat/dt=$(date -u +%F).jsonl"
   ```
   - `error` con `http_status` 5xx o vacío: el feed de ECOBICI falla; el recolector reintenta solo
     (backoff exponencial con tope de 300 s). No hay que hacer nada salvo esperar.
   - `error="feed_congelado"`: el feed responde pero `last_updated` no cambia desde hace más de
     `STALE_ALERT_SECONDS` (600 s). Es un problema de ECOBICI, no nuestro; se documenta.
   - Sin líneas nuevas: el proceso no corre. Reinícialo:
     ```bash
     docker compose --profile streaming up -d collector-kafka
     ```
3. Si el recolector publica pero no aparecen archivos en bronze, el problema es el consumidor:
   ```bash
   docker compose logs --tail 50 bronze-writer
   docker compose --profile streaming restart bronze-writer
   ```
   El consumidor confirma el offset **después** de escribir, así que al reiniciar recupera lo que
   quedó en Kafka (retención 7 días) sin duplicar archivos.
4. Si la laptop se suspendió, no hay nada que recuperar: esas horas quedan sin datos y el pipeline
   las excluye (ver "Huecos en el heartbeat").

## Kafka no arranca

Síntomas: `collector-kafka` o `bronze-writer` reinician en bucle; logs con
`Connection refused` o `Broker transport failure` hacia `kafka:9092` / `localhost:9094`.

1. Estado y logs del broker:
   ```bash
   docker compose --profile streaming ps kafka
   docker compose logs --tail 50 kafka
   ```
2. Si el healthcheck no pasa, reinícialo y espera a que quede `healthy`:
   ```bash
   docker compose --profile streaming up -d --wait kafka
   ```
3. Comprueba que existen los topics y el grupo del consumidor (`MSYS_NO_PATHCONV=1` evita que
   Git Bash convierta `/opt/...` en una ruta de Windows):
   ```bash
   MSYS_NO_PATHCONV=1 docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
   MSYS_NO_PATHCONV=1 docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group bronze-writer
   ```
   `LAG` alto con el consumidor vivo = está alcanzando; `LAG` creciendo con el consumidor caído =
   reinicia `bronze-writer`.
4. Puerto ocupado (`9094` ya en uso): otro proceso lo tiene. `netstat -ano | findstr 9094` en
   PowerShell para encontrarlo.
5. **Plan B sin Kafka:** el recolector puede escribir directo a bronze con el sink local
   (`SINK=local`): `docker compose up -d collector`. No corras los dos recolectores a la vez.

## Huecos en el heartbeat

Un hueco es un tramo de más de `QUALITY_MAX_GAP_MINUTES` (10) sin corridas exitosas del
recolector dentro de una hora. Pasa cuando la laptop se suspende o Docker se detiene.

1. Ver la cobertura de una hora y el check de calidad:
   ```bash
   uv run python scripts/run_hourly.py --hour 2026-11-10T14 --stage calidad --json
   ```
   `checks.huecos_heartbeat.hueco_min` dice cuántos minutos duró el hueco más largo.
2. Consecuencias (por diseño, no hay que corregir nada):
   - `--hour auto` salta las horas con huecos y elige la más reciente completa.
   - El entrenamiento (`radar.ml.train`) excluye esas horas: la rejilla repetiría un valor viejo.
3. Reprocesar horas pendientes cuando el recolector vuelve:
   ```bash
   uv run python -m radar.lake.bronze_to_silver --backfill --json
   uv run python -m radar.lake.silver_to_gold --day "$(date -u +%F)" --json
   ```
4. Evitar huecos durante una demo: desactiva la suspensión de Windows mientras corre el recolector.

## SeaweedFS no responde

Síntomas: `scripts/sync_lake.py` falla con `EndpointConnectionError` o `AccessDenied`.

1. Estado y logs:
   ```bash
   docker compose --profile lake ps seaweedfs
   docker compose logs --tail 50 seaweedfs
   ```
2. `docker/seaweedfs/s3.json` debe ser un **archivo**. Si Docker creó un directorio con ese
   nombre (se levantó SeaweedFS antes de generarlo), bórralo y regenéralo:
   ```bash
   uv run python scripts/lake_setup.py render
   docker compose --profile lake up -d --wait seaweedfs
   ```
3. `AccessDenied` al subir: las llaves de `.env` no coinciden con `s3.json`. Vuelve a renderizar y
   comprueba la política:
   ```bash
   uv run python scripts/lake_setup.py render
   uv run python scripts/lake_setup.py check-policy
   ```
4. El lake local (`data/`) es la fuente de verdad; SeaweedFS es una réplica. Mientras no responda,
   el pipeline sigue funcionando con `LAKE_SYNC_S3=false`.

## La API responde sin modelo

Síntomas: `GET /health` devuelve `"modelo_cargado": false, "fuente_modelo": "ninguno"`;
`/predict/{id}` y `/predictions` responden 503 `modelo_no_disponible`.

La API carga el modelo **una vez al arrancar**: primero el campeón de MLflow
(`radar-bikes-t60@champion`), si no el respaldo `models/champion.joblib`. Si ninguno estaba
disponible en ese momento, sigue sin modelo hasta que se reinicie.

1. ¿Está MLflow arriba y tiene el alias?
   ```bash
   docker compose --profile ml ps mlflow
   curl -sf localhost:5000/health
   uv run python -c "import mlflow; mlflow.set_tracking_uri('http://localhost:5000'); print(mlflow.MlflowClient().get_model_version_by_alias('radar-bikes-t60', 'champion').version)"
   ```
2. ¿Existe el respaldo local?
   ```bash
   ls -la models/champion.joblib
   ```
3. Si falta el modelo, entrena (registra el campeón y escribe el respaldo):
   ```bash
   docker compose --profile ml up -d --wait mlflow
   uv run python -m radar.ml.train --json
   ```
4. Reinicia la API para que lo cargue y verifica:
   ```bash
   docker compose --profile api restart api
   docker compose logs --tail 20 api
   curl -s localhost:8000/health
   ```
   En los logs, `modelo_cargado fuente=mlflow` o `fuente=archivo` confirma de dónde salió.

Si `/stations` responde 503 `datos_no_disponibles`, el problema no es el modelo sino que no hay
silver: corre el pipeline horario (`uv run python scripts/run_hourly.py --hour auto --stage todo`).
