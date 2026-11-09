# Epic 02: Lake, orquestación, modelo y API

> Después de este epic el bronze se transforma cada hora en silver y gold validados, el clima y los
> viajes históricos enriquecen las features, un modelo LightGBM registrado en MLflow supera (o no,
> honestamente) al baseline, y una API FastAPI sirve estaciones y predicciones.

| | |
|---|---|
| **Epic id** | `02-lake-modelo-api` |
| **Tasks** | `E2-T1` … `E2-T6` (pasos 7–12 de `blueprint.md` §9) |
| **Depends on** | `01-ingesta-streaming` |
| **Unlocks** | `03-producto-entrega` |
| **Parallel with** | nada: cada task consume la capa anterior |

You do not need any other file to complete this epic. Everything below is repeated here on purpose.

---

## Stack

Python 3.12 (uv) · polars + pyarrow (Parquet) · DuckDB · pandas 3 (ML) · Airflow 3.4 standalone
(contenedor, venv aislado) · Open-Meteo · scikit-learn + LightGBM · MLflow 3.18 · FastAPI +
Pydantic + uvicorn · pytest. Shell: **Git Bash**, siempre desde la raíz del repo. Versiones en
`pyproject.toml` + `uv.lock`. Idioma: español (identificadores en inglés).

| Task | Command |
|---|---|
| Lint · formato · tipos | `uv run ruff check . && uv run ruff format --check . && uv run mypy` |
| Pruebas (una) | `uv run pytest tests/unit/test_quality.py` |
| Silver de una hora | `uv run python -m radar.lake.bronze_to_silver --hour auto --check-idempotent --json` |
| Gold de un día | `uv run python -m radar.lake.silver_to_gold --day auto --check-idempotent --json` |
| Pipeline horario | `uv run python scripts/run_hourly.py --hour auto --stage todo --json` |
| Airflow | `docker compose --profile orquestacion up -d --build --wait airflow` → http://localhost:8080 |
| MLflow | `docker compose --profile ml up -d --wait mlflow` → http://localhost:5000 |
| Entrenar | `uv run python -m radar.ml.train --json` |
| Notebook | `uv run jupyter nbconvert --to html --execute notebooks/01_eda.ipynb --output-dir data/_verify` |
| Sync a SeaweedFS | `uv run python scripts/sync_lake.py --layer silver\|gold --check-idempotent --json` |

**Gate:** `uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest`
pasa antes de marcar cualquier task como hecho.

Los servicios `seaweedfs`, `postgres-airflow`, `airflow` y `mlflow` ya están en
`docker-compose.yml` (lo copió el Bootstrap). El recolector Kafka del epic 01 debe seguir corriendo:
verifícalo con `docker compose --profile streaming ps` antes de empezar.

## Directory subtree

```
src/radar/lake/
  store.py                       # existe (E1-T6), solo lectura
  job.py, quality.py, bronze_to_silver.py   # NUEVOS (E2-T1)
  silver_to_gold.py, trips.py    # NUEVOS (E2-T2)
  weather.py                     # NUEVO (E2-T3)
src/radar/ml/
  features.py                    # NUEVO (E2-T4)
  models.py, evaluate.py, train.py   # NUEVOS (E2-T5)
src/radar/api/
  schemas.py, repository.py, deps.py, main.py   # NUEVOS (E2-T6)
src/radar/collector/{gbfs_client,hashing,retry,sinks}.py   # existen (E1), solo lectura
scripts/run_hourly.py            # NUEVO (E2-T3); scripts/sync_lake.py existe (E1-T6)
dags/{horario.py, diario.py}     # NUEVOS (E2-T3)
docker/airflow.Dockerfile        # NUEVO (E2-T3)
sql/analitica.sql                # NUEVO (E2-T2)
notebooks/01_eda.ipynb           # NUEVO (E2-T4)
docs/{data-lake.md (E2-T2), modelo.md (E2-T4, editado en E2-T5)}
docs/reportes/                   # generado: benchmark, analitica, comparacion_modelos, real_vs_pred, 01_eda
tests/unit/{test_quality, test_bronze_to_silver, test_gold_trips, test_features, test_models}.py   # NUEVOS
tests/integration/test_api.py    # NUEVO (E2-T6)
```

Fuera de este subárbol no se toca nada. Si un task parece requerir otro archivo, detente y reporta.

## Data model touched here

Todo bajo `data/`, en UTC, Parquet escrito con `pyarrow`:

| Entity | Fields this epic adds or reads | Notes |
|---|---|---|
| `silver/station_status/dt=…/hour=…/part-0.parquet` | `station_id` (string), `snapshot_last_updated`, `last_reported` (timestamp s UTC), `num_bikes_available`, `num_bikes_disabled`, `num_docks_available`, `num_docks_disabled`, `capacity` (int16), `is_installed`, `is_renting`, `is_returning`, `is_charging` (bool) | (`station_id`, `last_reported`) único |
| `silver/quarantine/dt=…/hour=…/part-0.parquet` | columnas aplanadas + `motivo` (`esquema` \| `estacion_desconocida` \| `regla_capacidad`) | nunca se borra |
| `silver/station_information/station_information.parquet` | `station_id`, `short_name`, `short_name_norm`, `name`, `lat`, `lon`, `capacity`, `info_hash` | se reconstruye del bronze más reciente |
| `silver/trips/month=YYYY-MM/part-0.parquet` | `estacion_retiro`, `estacion_retiro_norm`, `retiro_utc`, `estacion_arribo`, `estacion_arribo_norm`, `arribo_utc` | SIN `Genero_Usuario`, `Edad_Usuario`, `Bici` |
| `silver/weather/weather.parquet` | `ts_utc` (único), `temperature_2m`, `precipitation`, `relative_humidity_2m`, `wind_speed_10m` | upsert |
| `gold/station_hour/dt=…/part-0.parquet` | `station_id`, `hora_utc`, `bikes_mean`, `bikes_min`, `bikes_max`, `docks_mean`, `n_obs`, `pct_vacia`, `pct_llena` | |
| `gold/features_base/dt=…/part-0.parquet` | `station_id`, `ts_utc` (rejilla 5 min), `bikes_available`, `docks_available`, `capacity`, `is_renting` | |
| `gold/trip_demand/trip_demand.parquet` | `short_name_norm`, `dow`, `hour_local`, `salidas_prom`, `llegadas_prom`, `tasa_match` | |
| `_manifest.json` (silver) | `hora`, `archivos_bronze`, `filas_leidas`, `filas_validas`, `filas_cuarentena`, `filas_duplicadas`, `tasa_regla_capacidad`, `sha256_validas` | llaves ordenadas, sin marcas de tiempo |
| `_manifest.json` (gold) | `dia`, `filas_station_hour`, `filas_features_base`, `sha256_station_hour` | ídem |
| MLflow | experimento `radar-ecobici`; modelo `radar-bikes-t60` alias `champion`; respaldo `models/champion.joblib` | |

Regla de capacidad: `num_bikes_available + num_bikes_disabled + num_docks_available +
num_docks_disabled == capacity`. CSV de viajes: encabezado `Genero_Usuario,Edad_Usuario,Bici,Ciclo_Estacion_Retiro,Fecha_Retiro,Hora_Retiro,Ciclo_EstacionArribo,Fecha_Arribo,Hora_Arribo`,
fechas `dd/mm/yyyy`, horas `HH:MM:SS`, códigos de estación como `"085"` o `"266-267"`.

## Contracts

**Consumed** — already exists, do not rebuild:

| From | Interface | Guarantee |
|---|---|---|
| `01-ingesta-streaming` | `radar.config.Settings`, `get_settings()` | lee `.env`, secretos opcionales |
| `01-ingesta-streaming` | `radar.collector.retry.ExponentialJitterPolicy`, `radar.collector.gbfs_client.GbfsClient`, `radar.collector.hashing.stations_hash` | 4xx sin reintento, 429 con `Retry-After`, full jitter tope 300 s |
| `01-ingesta-streaming` | `radar.lake.store.make_lake_store`, `scripts/sync_lake.py` | subida idempotente; nunca `bronze/trips/` |
| `01-ingesta-streaming` | bronze + heartbeat en `data/bronze/` | append-only, layout de arriba |

**Produced** — later epics depend on exactly these signatures:

| Export | Signature | Used by |
|---|---|---|
| `src/radar/ml/features.py` → `build_inference_frame`, `FEATURES` | `(base_recent, weather, demand, now_utc) -> pd.DataFrame` | `03` (anomalías vía repositorio) |
| `src/radar/ml/models.py` → `Predictor` | `fit(X, y)`, `predict(X) -> np.ndarray` | `03` |
| `src/radar/api/schemas.py` → `StationOut`, `PredictionOut`, `HistoryPointOut`, `AnomalyOut`, `HealthOut` | modelos Pydantic de `blueprint.md` §5 (repetidos en E2-T6) | `03` (mapa, anomalías) |
| `src/radar/api/repository.py` → `StationRepository` | `stations()`, `station(id)`, `history(id, hours)`, `inference_frame()`, `last_data_age_seconds()`, `anomalies()` | `03` (E3-T3 edita `anomalies()`) |
| `src/radar/api/main.py` → `app` | rutas `/health`, `/stations`, `/stations/{station_id}/history`, `/predict/{station_id}`, `/predictions`, `/anomalies`, `/docs` | `03` |

## Conventions that bite in this area

- Toda transformación hereda de `TransformJob` (Template Method) y reemplaza su partición completa;
  el manifiesto no lleva marcas de tiempo, por eso dos corridas dan el mismo manifiesto.
- `--hour auto` = la hora más reciente que terminó hace ≥ 10 min (nunca la hora en curso).
- Los DAGs no tienen lógica: llaman `/opt/radar/.venv/bin/python /opt/radar/scripts/run_hourly.py`.
  Airflow 3: `from airflow.sdk import DAG` y `from airflow.providers.standard.operators.bash import BashOperator`.
- MLflow no lee `.env`: `mlflow.set_tracking_uri(settings.mlflow_tracking_uri)` explícito.
- Hora local `America/Mexico_City` solo en features; en el lake todo es UTC.
- CLI: salida 0 ok, 2 uso (argparse), 3 check fallido; `--json` a stdout, logs a stderr.

Full project rules: `CLAUDE.md`. Area rules: `.claude/rules/python-pipeline.md`,
`.claude/rules/pruebas.md`, `.claude/rules/infra-costos.md`, `.claude/rules/encabezados.md`.

---

## Tasks

Listed in the same order as `tasks.json`. That order is the build order — work top to bottom and do
not re-rank by priority or by what looks quick.

### `E2-T1` — Bronze a silver con cadena de calidad y cuarentena

**Depends on:** `E1-T6` · **Priority:** p0 — metadata para recortes, no un orden de ejecución · **Etapa 3b — bronze → silver** · día sugerido: lun 9

**Meta:** Cada hora de bronze se convierte en Parquet silver validado, con cuarentena para las filas que fallan, deduplicación y un manifiesto idéntico entre corridas.

**Files**
- `src/radar/lake/job.py`
- `src/radar/lake/quality.py`
- `src/radar/lake/bronze_to_silver.py`
- `tests/unit/test_quality.py`
- `tests/unit/test_bronze_to_silver.py`

**Implementación**
- **`job.py`** — **Template Method**: `class TransformJob(ABC)` con `run(partition) -> dict` fijo:
  `extract()` → `validate()` → `load()` (escribe en `<partición>.tmp/`, `os.replace` sobre la
  partición final, `_manifest.json` con llaves ordenadas y sin marcas de tiempo) → devuelve el
  manifiesto. Las subclases solo implementan los tres pasos. `check_idempotent(partition)` corre
  `run` dos veces y compara manifiestos.
- **`quality.py`** — **Chain of Responsibility**: `class QualityCheck(ABC)` con
  `set_next(check)` y `handle(df) -> (validas, cuarentena)`; eslabones en este orden:
  `SchemaCheck` (campos del §4 presentes, enteros ≥ 0; si no → `motivo="esquema"`) →
  `KnownStationCheck` (`station_id` en la última `station_information`; si no →
  `motivo="estacion_desconocida"`) → `CapacityRuleCheck` (`num_bikes_available +
  num_bikes_disabled + num_docks_available + num_docks_disabled == capacity`; si no →
  `motivo="regla_capacidad"`) → `DedupCheck` (descarta duplicados de `(station_id, last_reported)`
  y los cuenta en `filas_duplicadas`; los duplicados NO van a cuarentena: son minutos sin cambios).
- **`bronze_to_silver.py`** — `BronzeToSilverJob(TransformJob)` para una hora UTC: lee los
  `station_status_*.json.gz` de `dt=…/hour=…`, aplana (una fila por estación por snapshot, con
  `snapshot_last_updated`), aplica la cadena, escribe `silver/station_status/…/part-0.parquet` y
  `silver/quarantine/…/part-0.parquet` (columna `motivo`) con polars + pyarrow. También reconstruye
  `silver/station_information/station_information.parquet` desde el bronze de station_information
  más reciente. Manifiesto: `hora`, `archivos_bronze`, `filas_leidas`, `filas_validas`,
  `filas_cuarentena`, `filas_duplicadas`, `tasa_regla_capacidad` (cumplen / evaluadas por la
  regla), `sha256_validas` (sha256 del Parquet válido ordenado por `station_id, last_reported`).
  CLI: `--hour YYYY-MM-DDTHH|auto` (`auto` = la hora más reciente que terminó hace ≥ 10 min),
  `--check-idempotent`, `--backfill` (toda hora completa con bronze y sin `_manifest.json`),
  `--print-auto-hour`, `--json`. Con `--check-idempotent` sale 3 si los manifiestos difieren, si
  `filas_validas == 0` o si `tasa_regla_capacidad` no está en [0, 1].

**Explicación para la exposición** (dásela al usuario en 3–6 líneas ANTES de escribir el código)
- **Medallion:** bronze = crudo y completo; silver = limpio y tipado; gold = listo para negocio.
  Cada capa se reconstruye desde la anterior.
- **Cuarentena en vez de borrar:** las filas malas son evidencia (así se descubrió la regla de
  capacidad con la estación 311); se pueden revisar y reprocesar si la regla cambia.
- **Chain of Responsibility:** cada check decide sobre lo que le llega y pasa el resto; agregar un
  check es agregar un eslabón. **Template Method:** el orden extraer → validar → cargar idempotente
  vive en un solo lugar.
- **Particionar por fecha/hora:** reprocesar una hora toca una sola carpeta y las consultas leen solo lo necesario.

**Acceptance**

Copiado de este task en `tasks.json` (mismo texto; aquí con énfasis markdown). Cada uno lo decide un comando de abajo, en esta máquina, durante el build.

1. **WHEN** `uv run pytest tests/unit/test_quality.py tests/unit/test_bronze_to_silver.py` runs **THE SYSTEM SHALL** exit 0 with 0 failed.
2. **WHEN** the fixture snapshot `tests/fixtures/station_status.json` is transformed **THE SYSTEM SHALL** quarantine station `"3"` with `motivo="regla_capacidad"` (5+0+6+0 is not 12) and station `"999"` with `motivo="estacion_desconocida"`, and keep stations `"1"` and `"2"` in silver.
3. **WHEN** two rows share `(station_id, last_reported)` **THE SYSTEM SHALL** keep exactly one in silver and count the other in `filas_duplicadas` without quarantining it.
4. **WHEN** a row lacks a required field or carries a non-integer count **THE SYSTEM SHALL** quarantine it with `motivo="esquema"` before any other check runs.
5. **WHEN** `uv run python -m radar.lake.bronze_to_silver --hour auto --check-idempotent --json` runs **THE SYSTEM SHALL** process the latest hour that ended at least 10 minutes ago twice and exit 0 only if both manifests are identical, `filas_validas` is above 0 and `tasa_regla_capacidad` lies between 0 and 1.
6. **WHEN** `uv run python -m radar.lake.bronze_to_silver --backfill --json` runs **THE SYSTEM SHALL** process every complete hour that has bronze files and no `_manifest.json`, and exit 0.

**Verify** — cada comando, en orden, desde la raíz del proyecto. Cada uno sale 0 cuando el task es correcto; el último en salir 0 es lo que lo vuelve `done`.

```bash
uv run pytest tests/unit/test_quality.py tests/unit/test_bronze_to_silver.py tests/unit/test_encabezados.py
uv run ruff check . && uv run ruff format --check . && uv run mypy
uv run python -m radar.lake.bronze_to_silver --hour auto --check-idempotent --json
uv run python -m radar.lake.bronze_to_silver --backfill --json
uv run python scripts/sync_lake.py --layer silver --check-idempotent --json
```

**Checkpoint**

```bash
git add -A && git commit -m "feat(E2-T1): bronze a silver con cadena de calidad, cuarentena y deduplicación"
git tag step-07-silver
```

Corre ambos (commit y tag) después de que el último comando `Verify` salga 0, antes de empezar el siguiente task. Las líneas posteriores al tag son aserciones que solo son decidibles después del commit. Nunca inventes el tag: es el campo `checkpoint` de `tasks.json`.

**Cierre de etapa** (skill `cerrar-etapa`): resumen + estas preguntas de checkpoint para el usuario + propuesta de entrada de bitácora (qué decidí · alternativas · por qué · consecuencias).

- ¿Qué va en bronze que no va en silver?
- ¿Por qué cuarentena en lugar de borrar?
- ¿Por qué particionar por fecha y hora?

Retos que cubre: 38 (Data Lake), 9 (limpieza).

### `E2-T2` — Gold horario, viajes históricos, SQL analítico y benchmark

**Depends on:** `E2-T1` · **Priority:** p0 — metadata para recortes, no un orden de ejecución · **Etapa 3c — gold, viajes, SQL y benchmark** · día sugerido: lun 9

**Meta:** Existen las tablas gold (`station_hour`, `features_base`, `trip_demand`), los viajes de octubre sin PII en silver, el reporte SQL y la comparativa DuckDB vs pandas.

**Files**
- `src/radar/lake/silver_to_gold.py`
- `src/radar/lake/trips.py`
- `tests/unit/test_gold_trips.py`
- `sql/analitica.sql`
- `docs/data-lake.md`

**Implementación**
- **`silver_to_gold.py`** — `SilverToGoldJob(TransformJob)` por día UTC: `gold/station_hour`
  (por estación y hora: media/mín/máx de bicis y docks, `n_obs`, `pct_vacia`, `pct_llena`) y
  `gold/features_base` (rejilla de 5 min por estación, último valor observado hacia adelante, con
  `capacity`). CLI `--day YYYY-MM-DD|auto` (día de la hora `auto` del paso 7), `--check-idempotent`,
  `--json`, y `--sql-report <archivo.sql> --out <archivo.md>`: separa el archivo por comentarios
  `-- name: <nombre>`, ejecuta cada consulta en DuckDB (vistas sobre `read_parquet('data/…/**/*.parquet')`)
  y escribe un Markdown con el SQL y las primeras 20 filas de cada resultado.
- **`trips.py`** — `--probe` (GET con `Range: bytes=0-2047` a `TRIPS_CSV_URL`; sale 3 si el
  encabezado no es `Genero_Usuario,Edad_Usuario,Bici,Ciclo_Estacion_Retiro,Fecha_Retiro,Hora_Retiro,Ciclo_EstacionArribo,Fecha_Arribo,Hora_Arribo`),
  `TRIPS_CSV_URL` por defecto es la URL verificada el 2026-11-08 (HTTP 200, ~96 MB, encabezado
  confirmado). **Respaldo si `--probe` falla:** descarga `https://ecobici.cdmx.gob.mx/datos-abiertos/`,
  extrae los enlaces `href="...\.csv"`, toma el más reciente (mayor `AAAA-MM` en el nombre),
  vuelve a correr `--probe` con esa URL y actualízala en `.env` (no en `.env.example`): el
  nombre de archivo cambia de formato de un mes a otro.
  `--download` (a `data/bronze/trips/<nombre del archivo de la URL>`, omite si existe con el mismo
  tamaño), `--to-silver` (lista blanca: `Ciclo_Estacion_Retiro, Fecha_Retiro, Hora_Retiro,
  Ciclo_EstacionArribo, Fecha_Arribo, Hora_Arribo`; fechas `dd/mm/yyyy` + `HH:MM:SS` en
  `America/Mexico_City` → UTC; lee con `utf-8-sig` y reintenta con `latin-1`), `--demand`
  (`gold/trip_demand/trip_demand.parquet`: salidas y llegadas promedio por `short_name`, día de la
  semana y hora local, + `tasa_match` contra `station_information.short_name`), `--benchmark`
  (misma agregación grande en DuckDB y en pandas, cada motor en su propio subproceso; tiempo con
  `time.perf_counter`, memoria pico con `psutil.Process().memory_info()` — `peak_wset` en Windows,
  `resource.getrusage` en Linux — y reporte en `docs/reportes/benchmark_duckdb_pandas.md`).
  Normalización de código de estación: `strip()`, quitar ceros a la izquierda en códigos simples
  (`"085"` → `"85"`, igual para `short_name`), códigos compuestos (`"266-267"`) se conservan y se
  reportan como no emparejados.
- **`sql/analitica.sql`** — encabezado `--` y consultas con `-- name:`: estaciones que más se
  vacían (JOIN silver ↔ station_information), patrón por hora/día (GROUP BY … HAVING
  `COUNT(*) >= 30`), estaciones por encima del promedio (subconsulta con `IN (SELECT`) y ranking
  por hora (`RANK() OVER (`).
- **`docs/data-lake.md`** — `## Bronze`, `## Silver`, `## Gold`, `## Seguridad` (identidades,
  mínimo privilegio, PII de viajes nunca sale del bronze local), y los resultados del reporte SQL
  y del benchmark.

**Explicación para la exposición** (dásela al usuario en 3–6 líneas ANTES de escribir el código)
- **Parquet vs CSV:** columnar (lees solo las columnas que usas), tipado, comprimido; CSV es texto
  sin tipos que hay que reinterpretar en cada lectura.
- **DuckDB vs pandas:** DuckDB ejecuta SQL vectorizado fuera de memoria sobre Parquet; pandas carga
  todo a RAM. El benchmark lo mide, no lo supone.
- **PII:** lista blanca de columnas (no lista negra): una columna nueva en el CSV no se cuela a silver.

**Acceptance**

Copiado de este task en `tasks.json` (mismo texto; aquí con énfasis markdown). Cada uno lo decide un comando de abajo, en esta máquina, durante el build.

1. **WHEN** `uv run pytest tests/unit/test_gold_trips.py` runs **THE SYSTEM SHALL** exit 0 with 0 failed.
2. **WHEN** the station code `"085"` is normalized **THE SYSTEM SHALL** return `"85"`, keep `"266-267"` unchanged, and count `"266-267"` as unmatched against `station_information.short_name`.
3. **WHEN** `uv run python -m radar.lake.trips --download --to-silver --demand --json` runs **THE SYSTEM SHALL** keep the raw CSV only under `data/bronze/trips/` and write `data/silver/trips/month=*/part-0.parquet` without the columns `Genero_Usuario`, `Edad_Usuario` and `Bici`.
4. **WHEN** `uv run python -m radar.lake.silver_to_gold --day auto --check-idempotent --json` runs **THE SYSTEM SHALL** build `gold/station_hour` and `gold/features_base` for that UTC day twice and exit 0 only if both manifests are identical and `filas_station_hour` is above 0.
5. **WHEN** `uv run python -m radar.lake.trips --benchmark` runs **THE SYSTEM SHALL** write `docs/reportes/benchmark_duckdb_pandas.md` with wall-clock seconds and peak memory for DuckDB and for pandas.
6. **WHEN** `uv run python -m radar.lake.silver_to_gold --sql-report sql/analitica.sql --out docs/reportes/analitica.md` runs **THE SYSTEM SHALL** execute every query in the file, which contains a JOIN, a HAVING clause, an `IN (SELECT` subquery and an `OVER (` window, and exit 0.

**Verify** — cada comando, en orden, desde la raíz del proyecto. Cada uno sale 0 cuando el task es correcto; el último en salir 0 es lo que lo vuelve `done`.

```bash
uv run pytest tests/unit/test_gold_trips.py tests/unit/test_encabezados.py
uv run ruff check . && uv run ruff format --check . && uv run mypy
uv run python -m radar.lake.trips --probe
uv run python -m radar.lake.bronze_to_silver --backfill --json
uv run python -m radar.lake.trips --download --to-silver --demand --json
uv run python -c "import glob, sys, pyarrow.parquet as pq; f = glob.glob('data/silver/trips/month=*/part-0.parquet'); sys.exit(0 if f and not set(pq.read_schema(f[0]).names) & {'Genero_Usuario', 'Edad_Usuario', 'Bici'} else 1)"
uv run python -m radar.lake.silver_to_gold --day auto --check-idempotent --json
uv run python -m radar.lake.trips --benchmark && test -s docs/reportes/benchmark_duckdb_pandas.md
uv run python -m radar.lake.silver_to_gold --sql-report sql/analitica.sql --out docs/reportes/analitica.md
uv run python -c "import sys; t = open('sql/analitica.sql', encoding='utf-8').read().upper(); sys.exit(0 if all(k in t for k in ['JOIN', 'HAVING', 'IN (SELECT', 'OVER (']) else 1)"
uv run python -c "import sys; t = open('docs/data-lake.md', encoding='utf-8').read().splitlines(); sys.exit(0 if all(h in t for h in ['## Bronze', '## Silver', '## Gold', '## Seguridad']) else 1)"
uv run python scripts/sync_lake.py --layer gold --check-idempotent --json
```

**Checkpoint**

```bash
git add -A && git commit -m "feat(E2-T2): gold horario, viajes sin PII, SQL analítico y benchmark"
git tag step-08-gold-viajes
```

Corre ambos (commit y tag) después de que el último comando `Verify` salga 0, antes de empezar el siguiente task. Las líneas posteriores al tag son aserciones que solo son decidibles después del commit. Nunca inventes el tag: es el campo `checkpoint` de `tasks.json`.

**Cierre de etapa** (skill `cerrar-etapa`): resumen + estas preguntas de checkpoint para el usuario + propuesta de entrada de bitácora (qué decidí · alternativas · por qué · consecuencias).

- ¿Por qué Parquet y no CSV?
- ¿Qué mostró el benchmark DuckDB vs pandas y por qué?

Retos que cubre: 38, 17 (DuckDB), 13 (SQL), 9 (limpieza), Infraestructura en 3 capas (parcial).

### `E2-T3` — Pipeline horario con clima, checks de calidad y Airflow

**Depends on:** `E2-T2` · **Priority:** p1 — metadata para recortes, no un orden de ejecución · **Etapa 4 — Orquestación con Airflow (time-box 3 h)** · día sugerido: mar 10

**Meta:** Un pipeline horario idempotente (bronze → silver → clima → gold → calidad) corre como script y como DAGs `horario`/`diario` en Airflow 3 standalone.

**Files**
- `src/radar/lake/weather.py`
- `scripts/run_hourly.py`
- `dags/horario.py`
- `dags/diario.py`
- `docker/airflow.Dockerfile`

**Implementación**
**Time-box estricto de 3 h** para la parte de Airflow (comandos 5–7 del Verify). Construye primero
`weather.py` y `run_hourly.py`: son el *fallback* y la lógica real. Si a las 3 h Airflow no pasa,
DETENTE y reporta al usuario: propón el fallback (`scripts/run_hourly.py` agendado con un loop o
el Programador de tareas de Windows) y documenta la decisión como D-011 en la bitácora. Solo el
usuario puede autorizar cerrar el paso con el fallback (es el recorte #4 de la tabla de prioridades).

- **`src/radar/lake/weather.py`** — `fetch_weather(settings, http)`: GET
  `https://api.open-meteo.com/v1/forecast?latitude=19.4326&longitude=-99.1332&hourly=temperature_2m,precipitation,relative_humidity_2m,wind_speed_10m&past_days=7&forecast_days=1&timezone=UTC`
  (lat/lon de `WEATHER_LAT`/`WEATHER_LON`; usa `RetryPolicy` del paso 2). Upsert en
  `silver/weather/weather.parquet` por `ts_utc` (lee, combina, deduplica conservando lo más
  reciente, escribe atómico). Para horas de más de 92 días usar
  `https://archive-api.open-meteo.com/v1/archive` (no se necesita en esta entrega).
- **`scripts/run_hourly.py`** — `--hour YYYY-MM-DDTHH|auto`, `--stage silver|clima|gold|calidad|todo`,
  `--max-gap-minutes` (default `QUALITY_MAX_GAP_MINUTES`), `--max-capacity-fail-pct` (default
  `QUALITY_MAX_CAPACITY_FAIL_PCT`), `--print-auto-hour` (imprime `YYYY-MM-DDTHH`),
  `--print-airflow-logical-date` (imprime la hora `auto` + 1 h como `YYYY-MM-DDTHH:00:00+00:00`,
  porque el DAG procesa la hora anterior a su `logical_date`), `--info` (refresca station_information
  por hash con `GbfsClient` + `stations_hash`, para el DAG diario), `--json`. `auto` = la hora más
  reciente terminada hace ≥ 10 min cuyo hueco máximo de heartbeat (`guardado` o `sin_cambios`, de
  cualquier sink) no supera `QUALITY_MAX_GAP_MINUTES` de `Settings` — SIEMPRE el umbral por
  defecto, nunca el valor de `--max-gap-minutes`, para que elegir la hora sea independiente del
  check —, buscando hasta 24 h atrás (sale 3 si no hay ninguna). `--print-auto-hour` imprime
  exactamente esa hora. Checks de `calidad`: `huecos_heartbeat` (minutos del hueco más largo
  dentro de la hora > `--max-gap-minutes` → falla) y `regla_capacidad`
  (`100 × (1 − tasa_regla_capacidad)` > `--max-capacity-fail-pct` → falla). Un check fallido
  imprime su nombre en stdout (`check_fallido=huecos_heartbeat`; con `--json`,
  `{"checks_fallidos": ["huecos_heartbeat"]}`) y el proceso sale **3**. Si `LAKE_SYNC_S3=true`, al final sincroniza las tres capas.
- **`dags/horario.py`** — `from airflow.sdk import DAG` y
  `from airflow.providers.standard.operators.bash import BashOperator` (Airflow 3);
  `schedule="0 * * * *"`, `start_date=datetime(2026, 11, 8, tzinfo=UTC)`, `catchup=False`,
  `max_active_runs=1`, `default_args={"retries": 2, "retry_delay": timedelta(minutes=5)}`. En Airflow 3
  un cron usa `CronTriggerTimetable` (intervalo de longitud cero), así que la hora procesada es la
  ANTERIOR a `logical_date`. Tareas `bronze_a_silver >> clima >> silver_a_gold >> calidad`, cada una:
  `/opt/radar/.venv/bin/python /opt/radar/scripts/run_hourly.py --hour "{{ (logical_date - macros.timedelta(hours=1)).strftime('%Y-%m-%dT%H') }}" --stage <etapa>`.
- **`dags/diario.py`** — `schedule="15 6 * * *"`, una tarea `--info`.
- **`docker/airflow.Dockerfile`** — exactamente:
  ```dockerfile
  # Autor: Isacc Cabrera
  # Descripción: Airflow 3 standalone con un venv aislado del proyecto en /opt/radar/.venv
  #   (las dependencias del proyecto no tocan el entorno de Airflow ni sus constraints).
  # Patrón: N/A
  # Fecha de creación: 2026-11-10
  # Cambios:
  #     - 2026-11-10 · Isacc Cabrera · Creación (paso 9).
  FROM apache/airflow:3.4.0-python3.12
  USER root
  RUN mkdir -p /opt/radar && chown airflow:root /opt/radar
  USER airflow
  RUN pip install --no-cache-dir uv==0.13.7
  ENV UV_PYTHON_DOWNLOADS=never UV_LINK_MODE=copy UV_PROJECT_ENVIRONMENT=/opt/radar/.venv
  WORKDIR /opt/radar
  COPY --chown=airflow:root pyproject.toml uv.lock .python-version ./
  RUN uv sync --frozen --no-dev --no-install-project --extra lake
  COPY --chown=airflow:root src ./src
  COPY --chown=airflow:root scripts ./scripts
  RUN uv sync --frozen --no-dev --no-editable --extra lake
  WORKDIR /opt/airflow
  ```
  El servicio `airflow` de `docker-compose.yml` ya define `standalone`, `LocalExecutor`, Postgres
  y el healthcheck. **Receta verificada solo en diseño: verificar antes de invertir más de 3 h.**
- **Backfill (demo para la exposición, no es gate):** dentro del contenedor
  `airflow backfill create --dag-id horario --from-date <inicio> --to-date <fin>` (sintaxis de
  Airflow 3; verificar con `airflow backfill create --help`); equivalente sin Airflow:
  repetir `run_hourly.py --hour <H> --stage todo` por cada hora.

**Explicación para la exposición** (dásela al usuario en 3–6 líneas ANTES de escribir el código)
- **DAG / task / schedule:** grafo dirigido sin ciclos de tareas con dependencias; el schedule
  crea una corrida por intervalo (`data_interval_start` = la hora que se procesa).
- **Backfill:** crear corridas para intervalos pasados; funciona porque cada tarea es idempotente
  (reprocesar una hora reemplaza su partición).
- **Reintentos:** Airflow reintenta una tarea fallida 2 veces con 5 min de espera.
- **Kafka no reemplaza a Airflow:** Kafka mueve eventos continuamente; Airflow agenda trabajos
  por lotes con dependencias, reintentos e historial. Standalone + LocalExecutor + Postgres es la
  opción más ligera que no usa SQLite para metadatos (la laptop tiene 31.5 GB y 8 núcleos).

**Acceptance**

Copiado de este task en `tasks.json` (mismo texto; aquí con énfasis markdown). Cada uno lo decide un comando de abajo, en esta máquina, durante el build.

1. **WHEN** `uv run python scripts/run_hourly.py --hour auto --stage todo --json` runs **THE SYSTEM SHALL** run bronze to silver, Open-Meteo weather for lat 19.4326 / lon -99.1332, silver to gold and the quality checks for the chosen hour, and exit 0.
2. **WHEN** the hour printed by `run_hourly.py --print-auto-hour` (chosen with the default threshold) is checked with `--stage calidad --max-gap-minutes 0` **THE SYSTEM SHALL** run the `calidad` stage, exit with code 3 and print the failed check name `huecos_heartbeat` (the deliberately failing quality check).
3. **WHEN** the weather stage runs twice **THE SYSTEM SHALL** leave `data/silver/weather/weather.parquet` with at least one row and unique `ts_utc` values.
4. **WHEN** `docker compose --profile orquestacion up -d --build --wait airflow` runs **THE SYSTEM SHALL** report Airflow standalone (LocalExecutor, Postgres metadata) healthy on port 8080.
5. **WHEN** `airflow dags list -o json` runs inside the `airflow` container **THE SYSTEM SHALL** list the DAG ids `horario` and `diario`.
6. **WHEN** `airflow dags test horario` runs inside the container with the logical date printed by `run_hourly.py --print-airflow-logical-date` (the `auto` hour plus 1 h) **THE SYSTEM SHALL** exit 0.

**Verify** — cada comando, en orden, desde la raíz del proyecto. Cada uno sale 0 cuando el task es correcto; el último en salir 0 es lo que lo vuelve `done`.

```bash
uv run pytest tests/unit/test_encabezados.py
uv run ruff check . && uv run ruff format --check . && uv run mypy
uv run python scripts/run_hourly.py --hour auto --stage todo --json
uv run python scripts/run_hourly.py --hour auto --stage todo --json
H="$(uv run python scripts/run_hourly.py --print-auto-hour)"; out="$(uv run python scripts/run_hourly.py --hour "$H" --stage calidad --max-gap-minutes 0 2>&1)"; rc=$?; test $rc -eq 3 && echo "$out" | grep -q huecos_heartbeat
uv run python -c "import sys, pyarrow.parquet as pq; t = pq.read_table('data/silver/weather/weather.parquet').column('ts_utc').to_pylist(); sys.exit(0 if t and len(t) == len(set(t)) else 1)"
docker compose --profile orquestacion up -d --build --wait airflow
docker compose --profile orquestacion exec -T airflow airflow dags list -o json | uv run python -c "import json, sys; ids = {d['dag_id'] for d in json.load(sys.stdin)}; sys.exit(0 if {'horario', 'diario'} <= ids else 1)"
uv run python scripts/run_hourly.py --print-airflow-logical-date > data/_verify/hora.txt
MSYS_NO_PATHCONV=1 docker compose --profile orquestacion exec -T airflow bash -c 'airflow dags test horario "$(cat /opt/radar/data/_verify/hora.txt)"'
```

**Checkpoint**

```bash
git add -A && git commit -m "feat(E2-T3): pipeline horario con clima, checks de calidad y DAGs de Airflow"
git tag step-09-orquestacion
```

Corre ambos (commit y tag) después de que el último comando `Verify` salga 0, antes de empezar el siguiente task. Las líneas posteriores al tag son aserciones que solo son decidibles después del commit. Nunca inventes el tag: es el campo `checkpoint` de `tasks.json`.

**Cierre de etapa** (skill `cerrar-etapa`): resumen + estas preguntas de checkpoint para el usuario + propuesta de entrada de bitácora (qué decidí · alternativas · por qué · consecuencias).

- Si corro el mismo DAG del mismo día dos veces, ¿qué debe pasar?
- ¿Qué es un backfill?

Retos que cubre: 28 (Airflow), 18 (automatización). 📣 Post #3 (mié 11): "De datos crudos a datos confiables: bronze, silver y gold" — imagen: una fila real cruda → limpia → agregada; aprendizaje: la regla de capacidad descubierta con la estación 311.

### `E2-T4` — Notebook EDA y features sin fuga de datos

**Depends on:** `E2-T3` · **Priority:** p0 — metadata para recortes, no un orden de ejecución · **Etapa 5a — EDA y features** · día sugerido: mar 10

**Meta:** Existe un notebook EDA ejecutable con gráficas Plotly y conclusiones, y un módulo de features (lags, rolling, calendario, clima, demanda) probado contra fuga de datos.

**Files**
- `notebooks/01_eda.ipynb`
- `src/radar/ml/features.py`
- `tests/unit/test_features.py`
- `docs/modelo.md`

**Implementación**
- **`src/radar/ml/features.py`** — `build_training_frame(base, weather, demand) -> pd.DataFrame` y
  `build_inference_frame(base_recent, weather, demand, now_utc) -> pd.DataFrame` (la última fila
  por estación, mismas columnas sin el target). Rejilla de 5 min por estación desde
  `gold/features_base`. Features: `bikes_available`, `capacity`, `lag_15`, `lag_30`, `lag_60`
  (valor 3, 6 y 12 pasos antes), `roll_mean_30`, `roll_mean_60` (ventanas que terminan en t, solo
  pasado), `hour_local`, `dow`, `is_weekend`, `is_holiday` (`holidays.country_holidays("MX")`)
  — todo en hora local `America/Mexico_City` —, `temperature_2m`, `precipitation` (la última hora
  ≤ t), `demanda_salidas`, `demanda_llegadas` (de `gold/trip_demand` por `short_name`, día y hora
  local; vienen de octubre, otro periodo ⇒ sin fuga). Target `y_t60` = `bikes_available` 12 pasos
  después en la misma estación; filas sin target se descartan. Constante `FEATURES: list[str]`.
- **`tests/unit/test_features.py`** — marco sintético: alterar valores posteriores a t no cambia
  ninguna feature en t; alineación de `y_t60`; festivos.
- **`notebooks/01_eda.ipynb`** — secciones Markdown: Introducción, Objetivos, Datos (fuentes y
  capas), Calidad (cobertura del heartbeat, tasa de la regla de capacidad, cuarentena), Mapa
  (`plotly.express.scatter_map` de estaciones coloreadas por % de bicis, sin token), Series de
  tiempo (5 estaciones más variables), Patrón por hora/día, Clima vs uso, Conclusiones. Lee el lake
  con rutas relativas a la raíz del repo (el kernel arranca en `notebooks/`: usa
  `Path.cwd().parent` si existe `../pyproject.toml`).
- **`docs/modelo.md`** — `## Objetivo` (t+60 por estación), `## Features` (tabla con fórmula y
  fuente) y `## Revisión de fuga de datos` (cada feature: por qué no ve el futuro; riesgos: clima
  de pronóstico vs observado, demanda de otro periodo, deduplicación que cambia la rejilla).

**Explicación para la exposición** (dásela al usuario en 3–6 líneas ANTES de escribir el código)
- **Por qué no se separa al azar:** en series de tiempo mezclar futuro en entrenamiento infla las
  métricas; se valida siempre con datos posteriores a los de entrenamiento.
- **Fuga de datos aquí:** cualquier feature que use información posterior a t (una media centrada,
  el clima de una hora futura, el target mismo desplazado mal).
- **Lags y rolling:** resumen el pasado reciente de la estación; el modelo aprende la tendencia.

**Acceptance**

Copiado de este task en `tasks.json` (mismo texto; aquí con énfasis markdown). Cada uno lo decide un comando de abajo, en esta máquina, durante el build.

1. **WHEN** `uv run pytest tests/unit/test_features.py` runs **THE SYSTEM SHALL** exit 0 with 0 failed.
2. **WHEN** input values at times later than t are altered **THE SYSTEM SHALL** produce identical feature values at t for `lag_15`, `lag_30`, `lag_60`, `roll_mean_30` and `roll_mean_60`.
3. **WHEN** the training frame is built on the 5-minute grid **THE SYSTEM SHALL** set `y_t60` to the `bikes_available` value 12 steps later for the same station and drop rows without a target.
4. **WHEN** calendar features are built **THE SYSTEM SHALL** mark 2026-12-25 with `is_holiday=1` and 2026-11-10 with `is_holiday=0`, computing `hour_local` and `dow` in `America/Mexico_City`.
5. **WHEN** `uv run jupyter nbconvert --to html --execute notebooks/01_eda.ipynb --output-dir data/_verify` runs **THE SYSTEM SHALL** execute every cell on the real lake and exit 0.
6. **WHEN** `docs/modelo.md` is read **THE SYSTEM SHALL** contain the sections `## Objetivo`, `## Features` and `## Revisión de fuga de datos`.

**Verify** — cada comando, en orden, desde la raíz del proyecto. Cada uno sale 0 cuando el task es correcto; el último en salir 0 es lo que lo vuelve `done`.

```bash
uv run pytest tests/unit/test_features.py tests/unit/test_encabezados.py
uv run ruff check . && uv run ruff format --check . && uv run mypy
uv run jupyter nbconvert --to html --execute notebooks/01_eda.ipynb --output-dir data/_verify
uv run python -c "import sys; t = open('docs/modelo.md', encoding='utf-8').read().splitlines(); sys.exit(0 if all(h in t for h in ['## Objetivo', '## Features', '## Revisión de fuga de datos']) else 1)"
```

**Checkpoint**

```bash
git add -A && git commit -m "feat(E2-T4): notebook EDA y features sin fuga de datos"
git tag step-10-eda-features
```

Corre ambos (commit y tag) después de que el último comando `Verify` salga 0, antes de empezar el siguiente task. Las líneas posteriores al tag son aserciones que solo son decidibles después del commit. Nunca inventes el tag: es el campo `checkpoint` de `tasks.json`.

**Cierre de etapa** (skill `cerrar-etapa`): resumen + estas preguntas de checkpoint para el usuario + propuesta de entrada de bitácora (qué decidí · alternativas · por qué · consecuencias).

- ¿Dónde podría haber fuga de datos en estas features?
- ¿Por qué la demanda de viajes de octubre no es fuga para predecir noviembre?

Retos que cubre: 45 (series de tiempo), 12 (notebook), 19 (Plotly).

### `E2-T5` — Entrenar baseline, lineal y LightGBM con MLflow

**Depends on:** `E2-T4` · **Priority:** p0 — metadata para recortes, no un orden de ejecución · **Etapa 5b — Entrenamiento y MLflow** · día sugerido: mar 10

**Meta:** Baseline, regresión lineal y ≥3 configuraciones de LightGBM se comparan con TimeSeriesSplit, todo queda en MLflow y el ganador está registrado como `radar-bikes-t60@champion` con un respaldo local.

**Files**
- `src/radar/ml/models.py`
- `src/radar/ml/train.py`
- `src/radar/ml/evaluate.py`
- `tests/unit/test_models.py`
- `docs/modelo.md`

**Implementación**
- **`models.py`** — **Strategy**: `class Predictor(Protocol)` con `name: str`,
  `fit(X: pd.DataFrame, y: pd.Series) -> None`, `predict(X) -> np.ndarray`. Estrategias:
  `PersistenceBaseline` (predice `bikes_available` actual), `LastWeekBaseline` (mismo momento 7
  días antes; si no hay datos suficientes, `available = False` y se reporta "no disponible"),
  `LinearPredictor` (`Pipeline(StandardScaler, LinearRegression)`), `LightGBMPredictor(params)`
  con 3 configuraciones fijas: `lightgbm_a` (`num_leaves=15, learning_rate=0.05, n_estimators=300`),
  `lightgbm_b` (`num_leaves=31, learning_rate=0.05, n_estimators=500, min_child_samples=50`),
  `lightgbm_c` (`num_leaves=63, learning_rate=0.03, n_estimators=800, subsample=0.8,
  subsample_freq=1, colsample_bytree=0.8`). Todas recortan la predicción a `[0, capacity]`.
  `RadarPyfuncModel(mlflow.pyfunc.PythonModel)` envuelve cualquier `Predictor`.
- **`evaluate.py`** — `time_series_folds(df, n_splits=4)` (sobre el tiempo, no sobre filas: cada
  fold valida estrictamente después de entrenar), `metrics(y, yhat) -> {"mae", "rmse", "r2"}`,
  `overfit_gap(train_mae, valid_mae)`, `plot_real_vs_pred(...)` (Plotly HTML).
- **`train.py`** — CLI `python -m radar.ml.train [--json]`: arma el marco con `features.py`,
  evalúa cada estrategia con los folds, `mlflow.set_tracking_uri(settings.mlflow_tracking_uri)`
  (explícito: MLflow no lee `.env`), experimento `radar-ecobici`, un run por estrategia con
  params y métricas por fold y promedio; elige el menor MAE medio; reentrena con todo; lo registra
  como `MODEL_NAME` (`radar-bikes-t60`) con alias `champion`; guarda `joblib` en
  `MODEL_FALLBACK_PATH` (`models/champion.joblib`); escribe
  `docs/reportes/comparacion_modelos.md` y `docs/reportes/real_vs_pred.html`; imprime
  `{"modelos": {nombre: {"mae", "rmse", "r2", "mae_train"}}, "ganador", "mejora_vs_baseline_pct"}`.
- **`docs/modelo.md` (editar)** — `## Comparación de modelos` (tabla real copiada del reporte,
  mejora sobre el baseline en %, o explicación honesta si no la supera), `## Sobreajuste`,
  `## Limitaciones` (pocos días de datos, sin rebalanceo en los viajes, clima observado) y
  `## Exportar el notebook` (HTML con nbconvert; PDF imprimiendo ese HTML desde el navegador —
  `--to pdf` requiere LaTeX y `--to webpdf` requiere Chromium, ninguno se instala).
- Exporta: `uv run jupyter nbconvert --to html --execute notebooks/01_eda.ipynb --output-dir docs/reportes`.

**Explicación para la exposición** (dásela al usuario en 3–6 líneas ANTES de escribir el código)
- **Por qué un baseline:** "la estación tendrá en 1 h lo que tiene ahora" es difícil de vencer a
  60 min; si el modelo no lo supera, no aporta. La mejora en % es el número del post.
- **TimeSeriesSplit:** cada fold entrena con el pasado y valida con el futuro inmediato.
- **Sobreajuste:** MAE de entrenamiento mucho menor que el de validación ⇒ memoriza; por eso las
  configuraciones de LightGBM van de simple a compleja.
- **MLflow:** tracking (params, métricas, artefactos por run) + registry (versión con alias
  `champion` que la API carga). Limitación honesta: pocos días ⇒ el modelo no ve semanas completas.

**Acceptance**

Copiado de este task en `tasks.json` (mismo texto; aquí con énfasis markdown). Cada uno lo decide un comando de abajo, en esta máquina, durante el build.

1. **WHEN** `uv run pytest tests/unit/test_models.py` runs **THE SYSTEM SHALL** exit 0 with 0 failed, including a check that every fold's validation timestamps are later than all of its training timestamps.
2. **WHEN** `docker compose --profile ml up -d --wait mlflow` runs **THE SYSTEM SHALL** report MLflow healthy at http://localhost:5000.
3. **WHEN** `uv run python -m radar.ml.train --json` runs **THE SYSTEM SHALL** exit 0 and print MAE, RMSE and R² for `baseline`, `lineal` and at least 3 `lightgbm_*` configurations.
4. **WHEN** training finishes **THE SYSTEM SHALL** register a version of `radar-bikes-t60` with alias `champion` in MLflow and write the fallback file `models/champion.joblib`.
5. **WHEN** training finishes **THE SYSTEM SHALL** write `docs/reportes/comparacion_modelos.md` and `docs/reportes/real_vs_pred.html`, and `docs/modelo.md` SHALL contain `## Comparación de modelos`.
6. **WHEN** `uv run jupyter nbconvert --to html --execute notebooks/01_eda.ipynb --output-dir docs/reportes` runs **THE SYSTEM SHALL** exit 0 and leave `docs/reportes/01_eda.html`.

**Verify** — cada comando, en orden, desde la raíz del proyecto. Cada uno sale 0 cuando el task es correcto; el último en salir 0 es lo que lo vuelve `done`.

```bash
uv run pytest tests/unit/test_models.py tests/unit/test_encabezados.py
uv run ruff check . && uv run ruff format --check . && uv run mypy
docker compose --profile ml up -d --wait mlflow
uv run python -m radar.ml.train --json > data/_verify/train.json
uv run python -c "import json, sys; m = json.load(open('data/_verify/train.json', encoding='utf-8'))['modelos']; ok = {'baseline', 'lineal'} <= set(m) and sum(k.startswith('lightgbm_') for k in m) >= 3 and all({'mae', 'rmse', 'r2'} <= set(v) for v in m.values()); sys.exit(0 if ok else 1)"
uv run python -c "import sys, mlflow; mlflow.set_tracking_uri('http://localhost:5000'); v = mlflow.MlflowClient().get_model_version_by_alias('radar-bikes-t60', 'champion'); sys.exit(0 if v.version else 1)"
test -s models/champion.joblib && test -s docs/reportes/comparacion_modelos.md && test -s docs/reportes/real_vs_pred.html
grep -qx '## Comparación de modelos' docs/modelo.md
uv run jupyter nbconvert --to html --execute notebooks/01_eda.ipynb --output-dir docs/reportes && test -s docs/reportes/01_eda.html
```

**Checkpoint**

```bash
git add -A && git commit -m "feat(E2-T5): baseline, lineal y LightGBM con TimeSeriesSplit y MLflow"
git tag step-11-modelo-mlflow
```

Corre ambos (commit y tag) después de que el último comando `Verify` salga 0, antes de empezar el siguiente task. Las líneas posteriores al tag son aserciones que solo son decidibles después del commit. Nunca inventes el tag: es el campo `checkpoint` de `tasks.json`.

**Cierre de etapa** (skill `cerrar-etapa`): resumen + estas preguntas de checkpoint para el usuario + propuesta de entrada de bitácora (qué decidí · alternativas · por qué · consecuencias).

- ¿Cuánto superé al baseline (en %)?
- ¿Dónde podría haber fuga de datos?
- ¿Qué indica la diferencia entre MAE de entrenamiento y de validación?

Retos que cubre: 45, 22 (regresión, 3 modelos), 33 (MLflow), 12 (notebook), 37 (MLOps, parcial). 📣 Post #4 (jue 12): "Mi modelo predice cuántas bicis habrá en una estación dentro de 1 hora" — imagen: `docs/reportes/real_vs_pred.html` (captura); aprendizaje: por qué no se valida al azar y cuánto superó al baseline (con el número).

### `E2-T6` — API FastAPI con repositorio, DI y modelo cargado una vez

**Depends on:** `E2-T5` · **Priority:** p0 — metadata para recortes, no un orden de ejecución · **Etapa 6a — API FastAPI** · día sugerido: mié 11

**Meta:** La API sirve `/health`, `/stations`, `/stations/{station_id}/history`, `/predict/{station_id}`, `/predictions` y `/anomalies` (stub) con esquemas Pydantic, el modelo cargado una vez en el lifespan y CORS para el mapa.

**Files**
- `src/radar/api/schemas.py`
- `src/radar/api/repository.py`
- `src/radar/api/deps.py`
- `src/radar/api/main.py`
- `tests/integration/test_api.py`

**Implementación**
- **`schemas.py`** — los modelos Pydantic exactos de §5 (`HealthOut`, `StationOut` con
  `anomalia: bool = False`, `HistoryPointOut`, `PredictionOut`, `AnomalyOut`, `ErrorDetail`).
- **`repository.py`** — **Repository**: `StationRepository(data_dir)` lee
  `silver/station_information/station_information.parquet`, las particiones silver más recientes
  (última fila por estación), la historia de las últimas N horas y el marco de inferencia
  (`features.build_inference_frame`). Métodos: `stations()`, `station(id)`, `history(id, hours)`,
  `inference_frame()`, `last_data_age_seconds()`, `anomalies() -> list[AnomalyOut]` (devuelve `[]`
  hasta el paso 15). Cachea lecturas 30 s.
- **`deps.py`** — **Dependency Injection** + **Singleton en el lifespan**: `ModelHolder` con
  `load(settings)`: intenta `mlflow.pyfunc.load_model("models:/radar-bikes-t60@champion")` con la
  URI de `settings`; si falla, `joblib.load(MODEL_FALLBACK_PATH)`; si tampoco, sin modelo
  (`fuente="ninguno"`). Proveedores `get_settings_dep`, `get_repository`, `get_model_holder`
  (leen `request.app.state`).
- **`main.py`** — `lifespan`: `configure_logging`, crea `ModelHolder` UNA vez y lo guarda en
  `app.state`; `CORSMiddleware(allow_origins=API_CORS_ORIGINS.split(","), allow_methods=["GET"])`;
  rutas de §5 con `Depends`; clase de predicción con umbral ±3 (`se_vacia`/`estable`/`se_llena`);
  errores con `HTTPException(status_code, detail={"codigo": …, "mensaje": …})`.
- **`tests/integration/test_api.py`** — `TestClient` con `app.dependency_overrides` (repositorio
  falso con 3 estaciones y un `ModelHolder` falso que cuenta sus cargas).

**Explicación para la exposición** (dásela al usuario en 3–6 líneas ANTES de escribir el código)
- **REST:** recursos (`/stations`) con verbos HTTP y códigos estándar (200, 404, 422, 503).
- **Cargar el modelo al arrancar:** deserializar un modelo cuesta cientos de ms; hacerlo por
  petición multiplica la latencia. El lifespan lo carga una vez (Singleton) y `Depends` lo inyecta.
- **Repository:** la API no sabe que los datos son Parquet; mañana puede ser una base sin tocar rutas.
- **Unitaria vs integración:** unitaria prueba una función aislada; integración prueba la app
  completa (rutas + validación + DI) con `TestClient`, sin red.

**Acceptance**

Copiado de este task en `tasks.json` (mismo texto; aquí con énfasis markdown). Cada uno lo decide un comando de abajo, en esta máquina, durante el build.

1. **WHEN** `uv run pytest tests/integration/test_api.py` runs **THE SYSTEM SHALL** exit 0 with 0 failed.
2. **WHEN** `GET /health` is requested **THE SYSTEM SHALL** return 200 with the fields `status`, `modelo_cargado`, `fuente_modelo` and `segundos_desde_ultimo_dato`.
3. **WHEN** `GET /predict/{station_id}` is requested for an unknown id **THE SYSTEM SHALL** return 404 with `detail.codigo` equal to `estacion_no_encontrada`.
4. **WHEN** `GET /stations/{station_id}/history?hours=0` is requested **THE SYSTEM SHALL** return 422.
5. **WHEN** several prediction requests are served **THE SYSTEM SHALL** have loaded the model exactly once, during the lifespan.
6. **WHEN** a CORS preflight arrives from `http://localhost:5173` **THE SYSTEM SHALL** answer with `access-control-allow-origin: http://localhost:5173`, and `GET /docs` SHALL return 200 on the real app with real lake data and a non-empty `/stations` list.

**Verify** — cada comando, en orden, desde la raíz del proyecto. Cada uno sale 0 cuando el task es correcto; el último en salir 0 es lo que lo vuelve `done`.

```bash
uv run pytest tests/integration/test_api.py tests/unit/test_encabezados.py
uv run ruff check . && uv run ruff format --check . && uv run mypy
uv run python -c "import sys; from fastapi.testclient import TestClient; from radar.api.main import app; c = TestClient(app); c.__enter__(); h = c.get('/health'); s = c.get('/stations'); d = c.get('/docs'); c.__exit__(None, None, None); sys.exit(0 if h.status_code == 200 and s.status_code == 200 and len(s.json()) > 0 and d.status_code == 200 else 1)"
```

**Checkpoint**

```bash
git add -A && git commit -m "feat(E2-T6): API FastAPI con repositorio, DI y modelo cargado en el lifespan"
git tag step-12-api
```

Corre ambos (commit y tag) después de que el último comando `Verify` salga 0, antes de empezar el siguiente task. Las líneas posteriores al tag son aserciones que solo son decidibles después del commit. Nunca inventes el tag: es el campo `checkpoint` de `tasks.json`.

**Cierre de etapa** (skill `cerrar-etapa`): resumen + estas preguntas de checkpoint para el usuario + propuesta de entrada de bitácora (qué decidí · alternativas · por qué · consecuencias).

- ¿Cómo evito recargar el modelo en cada petición?
- ¿Qué devuelve la API si pido una estación que no existe y por qué ese código?

Retos que cubre: 47 (API de inferencia), Framework de backend, Webservice, Pruebas unitarias*.


---

## Epic acceptance

The epic is done when every task is `done` **and**:

1. **WHEN** the hourly pipeline runs twice for the same hour **THE SYSTEM SHALL** leave identical silver manifests (idempotent reprocessing).
2. **WHEN** the real app starts with the registered model **THE SYSTEM SHALL** serve `/health` 200 and a non-empty `/stations`.

```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest
uv run python -m radar.lake.bronze_to_silver --hour auto --check-idempotent --json
uv run python -c "import sys; from fastapi.testclient import TestClient; from radar.api.main import app; c = TestClient(app); c.__enter__(); h = c.get('/health'); s = c.get('/stations'); c.__exit__(None, None, None); sys.exit(0 if h.status_code == 200 and s.status_code == 200 and len(s.json()) > 0 else 1)"
```

## Pitfalls

- **Procesar la hora en curso** — sus archivos siguen llegando y el manifiesto cambia entre corridas;
  `auto` ya lo evita, no lo "mejores".
- **Fuga de datos** — una media móvil centrada o el clima de una hora futura inflan el MAE de
  validación; las pruebas de E2-T4 lo detectan.
- **Airflow consume la tarde** — time-box de 3 h; después se detiene y se propone el fallback.
- **PII** — si una columna nueva aparece en el CSV, la lista blanca la deja fuera; no cambies a lista negra.
- **Validar al azar** — `TimeSeriesSplit` siempre; un `train_test_split` aleatorio es un defecto.

## Before moving on

- [ ] Every task in this epic is `done` in `tasks.json` — no task left `in_progress`.
- [ ] Every `verify` command of every task in this epic passed, not just the first one.
- [ ] No `verify` command was edited, and none was skipped because a file it names did not exist.
- [ ] **Every task in this epic has its `checkpoint` tag in version control** — `step-07-silver` … `step-12-api` aparecen en `git tag -l 'step-*'`.
- [ ] Gate command passes clean, run from the project root.
- [ ] Every "Produced" contract above exists with the stated signature.
- [ ] No file outside the subtree was modified.
- [ ] `.env.example` ya trae todas las variables de este epic (no se agregó ninguna nueva).
- [ ] One commit per task, each with its task id in the conventional scope, each followed by its checkpoint tag.
