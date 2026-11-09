# Radar ECOBICI CDMX — Blueprint

> Generado por The Architect el 2026-11-08
> Shape: data-pipeline-analytics (primaria) + capa de servicio API/mapa (secundaria) · `knowledge/shapes/data-pipeline-analytics.md`
> Runtime track: Python 3.12 (pipeline, ML, API) + TypeScript/React (mapa) · `knowledge/runtime-tracks/python.md`, `knowledge/runtime-tracks/ts-node.md`
> Emission mode: bundle (18 pasos ≥ 12)
> Blueprint version: 1
> Versions last verified: 2026-11-08 — ver §11 para la procedencia de cada paquete
> Idioma: español (es-MX). Código, comandos, rutas, variables y palabras EARS (`WHEN`, `THE SYSTEM SHALL`) en inglés.

**Cómo se usa este bundle.** El proyecto vive en
`C:\Users\angel\Proyectos_Linkedin\ECO_BIKES\radar-ecobici-cdmx` (la "raíz"). Este bundle está en
`blueprints/radar-ecobici-cdmx/` dentro de la raíz y **se versiona** con el proyecto. Lo primero que
hace el builder es correr el Bootstrap de §10, que copia el contenido de `workspace/` a la raíz con
una copia no destructiva. Después, `tasks.json` decide el siguiente paso y cada `epics/NN-*.md` es
autocontenido. Todos los comandos corren en **Git Bash** desde la raíz.

---

## 1. Project Overview & Non-Goals

### Vision

Radar ECOBICI CDMX recolecta cada 60 segundos el estado de las ~677 estaciones del sistema de
bicicletas públicas de la Ciudad de México desde su feed público GBFS (Lyft), lo pasa por Kafka a un
data lake medallion (bronze crudo → silver validado con la regla de capacidad → gold agregado por
hora), entrena un modelo que predice cuántas bicis tendrá cada estación dentro de 60 minutos, y lo
sirve con una API FastAPI a un mapa 3D de la CDMX donde la altura de cada columna es el número de
bicis y el color dice si la estación "se vacía", está "estable" o "se llena".

Es el proyecto integrador de Angel Isacc Cabrera Leal para Ingeniería de Software en la IBERO
(entrega y exposición el viernes 13 de noviembre de 2026) y la pieza central de su portafolio de
ingeniería de datos en LinkedIn. Lo que lo distingue de un tablero ECOBICI cualquiera es la
cadena completa y explicable: decisiones registradas en una bitácora (D-001…D-010), recuperación
demostrada de Kafka, calidad de datos medida, un modelo comparado honestamente contra un baseline,
y todo corriendo gratis en una laptop con Windows 11.

### Users

| Persona | What they come to do | Frequency |
|---|---|---|
| Isacc (autor) | Construir, entender y explicar cada pieza; grabar el video y escribir los posts | diario (8–13 nov) |
| Profesor Cardeña (IBERO) | Evaluar retos cubiertos, arquitectura, calidad y la exposición | una vez (13 nov) |
| Reclutador / red de LinkedIn | Ver el mapa en video, leer el README y juzgar el criterio técnico | una vez por post |

### Goals — v1 scope

1. El recolector captura `station_status` cada 60 s y `station_information` diario, con heartbeat
   que distingue "sin cambios" de "caído", y nunca muere por errores del feed.
2. Kafka (KRaft) transporta los snapshots con commit después de escribir; una caída del consumidor
   de 5 minutos no pierde ningún snapshot.
3. El lake tiene bronze (crudo, append-only), silver (validado, cuarentena, deduplicado) y gold
   (agregados horarios y features) en Parquet, replicado en SeaweedFS con mínimo privilegio.
4. Un pipeline horario idempotente (Airflow 3, con fallback por script) agrega clima de Open-Meteo y
   corre checks de calidad que fallan de forma visible.
5. Un modelo LightGBM predice bicis en t+60 por estación, comparado contra baseline y regresión
   lineal con validación temporal, registrado en MLflow.
6. Una API FastAPI sirve estaciones, historia, predicciones y anomalías a un mapa 3D React +
   deck.gl con tema verde claro/oscuro.
7. La documentación (README, documento técnico, manual, guion de 90 s, presentación de 10
   diapositivas) permite clonar, correr y exponer el proyecto.

### Non-Goals — explicitly out of scope for v1

| Not building | Why not now | Revisit when |
|---|---|---|
| Hospedar la API o el mapa en la nube | Cuesta dinero o tiempo de configuración; la demo es local + video | Después de la entrega, si un reclutador pide un enlace vivo |
| Autenticación de usuarios | No hay datos privados ni usuarios; todo es público y local | Si el mapa se publica con funciones de escritura |
| WebSockets en la API | El feed cambia cada ~60 s; el polling basta (stretch opcional, se corta 3.º) | Si se publica en vivo y el tráfico de polling importa |
| Amazon MSK y MWAA | Servicios administrados caros; el plan los prohíbe | Nunca para este proyecto escolar |
| LLM de pago para el RAG | Costo; Ollama local cubre el reto | Si el modelo local no cabe en RAM (se avisa antes) |
| Re-entrenamiento en tiempo real del modelo | 5 días de datos no lo justifican; se entrena a mano | Con ≥ 4 semanas de datos |
| App móvil | El mapa web es responsivo | Nunca en v1 |
| Otras ciudades / otros sistemas GBFS | Un solo feed basta para demostrar la cadena | Si el código se generaliza después de la entrega |
| Subir viajes crudos con PII a cualquier lado | `Genero_Usuario`, `Edad_Usuario`, `Bici` son datos personales | Nunca |

**The builder must not implement anything in this table**, even if it seems like a small addition
while working on an adjacent step. If a step appears to require a non-goal, that is a blueprint
defect — stop and report it rather than expanding scope.

### Success metrics

| Metric | Target | How measured |
|---|---|---|
| `cobertura_heartbeat` | ≥ 95 % de minutos con heartbeat `guardado`/`sin_cambios` del 9 al 12 nov | check `huecos_heartbeat` de `scripts/run_hourly.py` + notebook EDA |
| `tasa_regla_capacidad` | Reportada (no objetivo) en cada manifiesto silver | `_manifest.json` de `bronze_to_silver` |
| Snapshots perdidos en la demo Kafka | 0 | `scripts/demo_recovery.sh` (`faltantes == 0`) |
| `mejora_vs_baseline_pct` | > 0 (o explicación honesta en `docs/modelo.md`) | salida JSON de `radar.ml.train` |
| Gate global §20.1 | Verde el 12 nov 23:59 | comandos de §20.1 |
| Posts de LinkedIn | 6 borradores escritos al cerrar cada etapa | `docs/bitacora.md` |

---

## 2. Tech Stack

**Runtime track: Python 3.12 + TypeScript.** This table names *choices*, not versions. Every version
pin lives in Section 11 and nowhere else.

Pins come from the `stack-researcher` report of 2026-11-08 (the authority). The runtime-track files
are only a fallback; no pin in this blueprint comes from them.

| Layer | Choice | Why this, over what |
|---|---|---|
| Language / runtime | Python 3.12 (pipeline, ML, API) | El track sugiere 3.14, pero Airflow 3.4, LightGBM y las imágenes oficiales de Airflow/Python están probadas en 3.12; un solo Python en todo el repo y en los contenedores. |
| Lenguaje del mapa | TypeScript 5.9 + React 19 | TS 7 (nativo) es un major nuevo sin compatibilidad comprobada con Vite/deck.gl; 6.0 salió hoy. 5.9 es la última línea madura. |
| Framework API | FastAPI + Pydantic + uvicorn | Validación por tipos, Swagger gratis en `/docs`, `Depends` para DI. Flask no valida ni documenta solo. |
| Streaming | Kafka 4 en modo KRaft + confluent-kafka | Es el reto 30; KRaft quita ZooKeeper (un contenedor menos). confluent-kafka (librdkafka) sobre kafka-python: mantenido y con idempotencia real del productor. |
| Formato y motor analítico | Parquet + DuckDB, polars para transformaciones, pandas solo para ML/benchmark | DuckDB consulta Parquet con SQL sin servidor (retos 13 y 17); polars es rápido y estricto con tipos; pandas lo exigen scikit-learn y el benchmark. |
| Almacenamiento de objetos | SeaweedFS (S3 local) — D-009 | MinIO archivado el 2026-04-25; Garage requiere más configuración; RustFS demasiado nuevo. |
| Orquestación | Airflow 3 standalone + LocalExecutor + Postgres (perfil `orquestacion`) | Es el reto 28. Standalone es la opción más ligera; Postgres en vez de SQLite permite LocalExecutor. Fallback: `scripts/run_hourly.py`. |
| ML | scikit-learn + LightGBM + MLflow (tracking + registry) | LightGBM es el estándar para tabulares; MLflow es el reto 33. XGBoost daría resultados similares con más configuración. |
| Database | NOT APPLICABLE como base transaccional — el lake en Parquet es el almacén; Postgres existe solo como metadatos de Airflow y SQLite solo para MLflow | Un warehouse/DB relacional no aporta a un pipeline de solo-anexar; DuckDB cubre las consultas. |
| ORM / data access | Patrón Repository sobre Parquet (polars/DuckDB) | Sin base relacional, un ORM no tiene qué mapear. |
| Auth | NOT APPLICABLE — sin cuentas de usuario | Todo es público y local. |
| Background work | Recolector como contenedor de larga vida + DAGs de Airflow | Un loop con `restart: unless-stopped` es más simple y robusto que un cron para 60 s. |
| Payments | NOT APPLICABLE — sin dinero | — |
| File storage | Disco local `data/` (fuente) + SeaweedFS (réplica S3) + AWS S3 (solo gold, opcional) | El disco es lo más rápido; S3 demuestra el reto 38 y el 6. |
| Email / notifications | Logs WARNING estructurados + alerta de presupuesto AWS por correo | No hay usuarios a quienes notificar; la alerta de costo sí es obligatoria. |
| Hosting | Local (Docker Desktop, Windows 11) + GitHub (código y CI) | D-003: gratis; la demo es video. |
| Mapa | deck.gl `ColumnLayer` sobre MapLibre con OpenFreeMap (D-010) | Sin token ni tarjeta; deck.gl dibuja miles de columnas 3D en GPU. |
| Clima | Open-Meteo (sin API key) | Gratis y con horas pasadas en el mismo endpoint. |
| RAG (opcional) | Ollama (`llama3.2:3b` + `nomic-embed-text`) + chromadb | 100 % local y gratis; cabe en 31.5 GB de RAM. |
| IaC | Terraform + provider AWS (S3, IAM, Budgets) | Es el reto de Terraform; nunca se aplica sin aprobación. |
| Package manager | uv (Python) y npm (frontend) | uv reemplaza pip/venv/pip-tools y fija todo en `uv.lock`; npm viene con Node 24 y basta para un solo paquete frontend. |

### Compatibility check

Checked against `knowledge/stack-compatibility.md` — no known-bad combinations. Su fila "Data /
analytics" prescribe exactamente esta división (orquestación Python + tablero en el track web por
separado, sin forzar un runtime en ambos). Notas que este blueprint resuelve explícitamente:

- `confluent-kafka` publica wheels `manylinux_2_28`: se usa `python:3.12-slim` (glibc), **no Alpine**.
- LightGBM necesita `libgomp1` en `python:3.12-slim`: `docker/api.Dockerfile` lo instala.
- Airflow nunca entra al entorno uv del proyecto (sus constraints chocarían con `pandas==3.1.1`):
  su imagen crea un venv aislado `/opt/radar/.venv` con el extra `lake`.
- `react-map-gl` 8 se usa vía `react-map-gl/maplibre`; `@deck.gl/react` 9.4 pide `@deck.gl/widgets`
  ~9.4 como peer, por eso se instala.
- La shape `data-pipeline-analytics` declara que The Architect no tiene shape de ML: el
  entrenamiento y el servicio del modelo se diseñan aquí con un alcance acotado (un modelo
  tabular, sin serving distribuido) porque el usuario lo pidió explícitamente.

---

## 3. Directory Structure

Origen de cada archivo: **[W]** = emitido en `workspace/` y copiado por el Bootstrap; **[B]** =
creado por un comando del Bootstrap; **[N]** = lo escribe el paso N de §9; **[gen]** = lo genera
una herramienta (nunca se escribe a mano).

```
radar-ecobici-cdmx/
├── CLAUDE.md                      [W] instrucciones del agente (Sección 0 del plan, extendida)
├── AGENTS.md                      [W] puente para agentes que no leen CLAUDE.md
├── README.md                      [1] (editado en 13 y 18)
├── LICENSE                        [1] MIT, Angel Isacc Cabrera Leal 2026
├── .gitignore                     [W] existe ANTES del primer commit
├── .gitattributes                 [W] LF en todo el repo (los .sh corren en Linux)
├── .dockerignore                  [W] excluye data/, blueprints/, .env, frontend/
├── .env.example                   [W] versionado; .env [B] se copia de aquí y está ignorado
├── .python-version                [W] 3.12
├── pyproject.toml                 [W] dependencias fijadas + ruff/mypy/pytest
├── uv.lock                        [gen] `uv sync` en el Bootstrap; versionado
├── docker-compose.yml             [W] servicios y perfiles (collector, streaming, lake, orquestacion, ml, api)
├── .claude/
│   ├── settings.json              [W] permisos: allow/ask/deny
│   ├── rules/*.md                 [W] encabezados, python-pipeline, pruebas, frontend, infra-costos
│   └── skills/{cerrar-etapa,nuevo-modulo-python}/SKILL.md   [W]
├── .github/workflows/ci.yml       [13] lint + tipos + pruebas + build de imágenes
├── blueprints/radar-ecobici-cdmx/ este bundle (versionado; excluido de cada herramienta)
├── src/radar/                     paquete Python (layout src, instalado editable por uv)
│   ├── __init__.py, */__init__.py [W] encabezados de paquete
│   ├── config.py                  [2] Settings (pydantic-settings, lee .env)
│   ├── logging.py                 [W] structlog JSON a stderr
│   ├── collector/
│   │   ├── retry.py               [2] RetryPolicy (Strategy)
│   │   ├── gbfs_client.py         [2] descubrimiento + fetch
│   │   ├── hashing.py             [2] sha256 canónico, ids desconocidos
│   │   ├── sinks.py               [3] SnapshotSink + LocalFileSink + make_sink (editado en 4)
│   │   ├── heartbeat.py           [3] JSONL por día
│   │   └── runner.py              [3] loop de 60 s (`python -m radar.collector.runner`)
│   ├── streaming/
│   │   ├── messages.py            [4] diseño de mensajes
│   │   ├── producer.py            [4] KafkaSink + creación de topics
│   │   └── consumer.py            [5] grupo bronze-writer
│   ├── lake/
│   │   ├── store.py               [6] LakeStore (Local/S3) + make_lake_store
│   │   ├── job.py                 [7] TransformJob (Template Method)
│   │   ├── quality.py             [7] cadena de checks (Chain of Responsibility)
│   │   ├── bronze_to_silver.py    [7]
│   │   ├── silver_to_gold.py      [8] (+ reporte SQL)
│   │   ├── trips.py               [8] viajes: probe, descarga, silver, demanda, benchmark
│   │   └── weather.py             [9] Open-Meteo
│   ├── ml/
│   │   ├── features.py            [10]
│   │   ├── models.py              [11] Predictor (Strategy)
│   │   ├── evaluate.py            [11]
│   │   └── train.py               [11]
│   ├── api/
│   │   ├── schemas.py             [12]
│   │   ├── repository.py          [12] (editado en 15)
│   │   ├── deps.py                [12] DI + ModelHolder
│   │   └── main.py                [12]
│   ├── anomalies/detectors.py     [15]
│   └── rag/{index.py,ask.py}      [17]
├── dags/
│   ├── README.md                  [W]
│   ├── horario.py                 [9]
│   └── diario.py                  [9]
├── docker/
│   ├── README.md                  [W]
│   ├── collector.Dockerfile       [3]
│   ├── airflow.Dockerfile         [9]
│   ├── api.Dockerfile             [13]
│   └── seaweedfs/
│       ├── s3.json.example        [W] formato de identidades
│       └── s3.json                [6, ignorado] generado por `scripts/lake_setup.py render`
├── sql/{README.md [W], analitica.sql [8]}
├── notebooks/{README.md [W], 01_eda.ipynb [10]}
├── scripts/
│   ├── README.md                  [W]
│   ├── demo_recovery.sh           [5]
│   ├── lake_setup.py              [6]
│   ├── sync_lake.py               [6]
│   ├── run_hourly.py              [9] pipeline horario + fallback de Airflow
│   └── sync_gold_to_s3.sh         [16]
├── infra/
│   ├── README.md                  [W]
│   └── terraform/{main.tf [16], variables.tf [16], .terraform.lock.hcl [gen 16], terraform.tfvars [ignorado]}
├── frontend/
│   ├── README.md, package.json, tsconfig.json, vite.config.ts, index.html   [W]
│   ├── package-lock.json          [gen 14] `npm --prefix frontend install`; versionado
│   ├── scripts/smoke.mjs          [W] smoke de `vite preview` en un solo proceso
│   └── src/
│       ├── main.tsx               [W] monta <App />
│       ├── styles/theme.css       [W] tokens de diseño
│       ├── lib/scales.ts          [W] paletas, umbrales, mapas base, contraste
│       ├── lib/scales.test.ts     [W] contraste WCAG + umbrales
│       ├── App.tsx                [14] contenedor
│       ├── hooks.ts               [14] useStations, useStationHistory, useTheme, useReducedMotion
│       └── components/{StationMap.tsx [14, editado 15], Panel.tsx [14]}
├── tests/
│   ├── README.md, conftest.py     [W]
│   ├── fixtures/{gbfs,station_status,station_information}.json   [W] respuestas grabadas
│   ├── unit/test_encabezados.py   [W]
│   ├── unit/test_recolector_nucleo.py [2] · test_runner.py [3] · test_messages.py [4]
│   ├── unit/test_consumer.py [5] · test_store.py [6] · test_quality.py [7] · test_bronze_to_silver.py [7]
│   ├── unit/test_gold_trips.py [8] · test_features.py [10] · test_models.py [11]
│   ├── unit/test_detectors.py [15] · test_rag.py [17]
│   └── integration/test_api.py    [12]
├── docs/
│   ├── README.md                  [W]
│   ├── bitacora.md                [1] copiada de la bitácora del usuario + D-009/D-010
│   ├── arquitectura.md [1] · metrics.md [1] · kafka.md [5] · data-lake.md [8] · modelo.md [10, 11]
│   ├── backend.md [13] · runbook.md [13] · frontend.md [14] · anomalias.md [15] · nube.md [16] · rag.md [17]
│   ├── documento-tecnico.md · manual-usuario.md · guion-video.md · presentacion.md   [18]
│   └── reportes/                  [gen 8, 11] benchmark, SQL, comparación de modelos, EDA en HTML
├── data/                          [B, ignorado] lake local: bronze/ silver/ gold/ state/ _verify/
├── models/                        [B, ignorado] models/champion.joblib (respaldo del modelo)
└── chroma/                        [gen 17, ignorado] índice vectorial del RAG
```

**Boundary rules**
- Las capas solo importan "hacia abajo": `collector` → (`config`, `logging`); `streaming` →
  `collector`; `lake` → (`config`, `logging`, `collector.hashing`); `ml` → `lake`; `api` → (`ml`,
  `lake`, `anomalies`). Nada importa `api`. `scripts/` y `dags/` solo orquestan código de `radar.*`.
- `src/radar/config.py` es el único lugar que lee variables de entorno.
- Solo `LocalFileSink` y `BronzeWriter` escriben en `data/bronze/`, y nunca sobrescriben ni borran.
- Convención de imports: ver la *Resolution convention matrix* de §19.6 (imports absolutos
  `radar.*` en Python; imports relativos sin extensión en TypeScript).

---

## 4. Data Model

No hay base de datos relacional: el modelo de datos son **datasets del lake** (archivos JSON.gz,
JSONL y Parquet) con rutas y esquemas fijos. Todas las rutas son relativas a `DATA_DIR` (`data/`
en el host, `/app/data` en los contenedores del recolector y la API, `/opt/radar/data` en Airflow).
Todo tiempo dentro del lake es **UTC**; `dt`/`hour` salen del `last_updated` del feed.

### Entities

**bronze/station_status** — snapshot crudo del feed, tal cual llegó; append-only.

| Field | Type | Constraints | Notes |
|---|---|---|---|
| ruta | `bronze/station_status/dt=YYYY-MM-DD/hour=HH/station_status_<last_updated>.json.gz` | única por `last_updated` | el nombre es la llave de idempotencia (Kafka re-entrega ⇒ mismo archivo) |
| contenido | bytes gzip | nunca se modifica | cuerpo HTTP original; `data.stations[]` con `station_id` (string), `num_bikes_available`, `num_bikes_disabled`, `num_docks_available`, `num_docks_disabled`, `is_installed`, `is_renting`, `is_returning`, `last_reported` (epoch s), `eightd_has_available_keys`, `is_charging` |

**bronze/station_information** — versión cruda solo cuando cambia el hash.

| Field | Type | Constraints | Notes |
|---|---|---|---|
| ruta | `bronze/station_information/dt=YYYY-MM-DD/station_information_<sha8>.json.gz` | única por hash | `sha8` = primeros 8 hex de `stations_hash` |
| contenido | bytes gzip | nunca se modifica | `station_id`, `external_id`, `name`, `short_name` (p. ej. `"710"`), `lat`, `lon`, `rental_methods`, `capacity`, `electric_bike_surcharge_waiver`, `is_charging`, `eightd_has_key_dispenser`, `has_kiosk` |

**bronze/heartbeat** — una línea JSON por corrida del recolector (`dt=YYYY-MM-DD.jsonl`).

| Field | Type | Constraints | Notes |
|---|---|---|---|
| `ts_utc` | string ISO-8601 | not null | inicio de la corrida |
| `feed` | string | `station_status` \| `station_information` | |
| `result` | string | `sin_cambios` \| `guardado` \| `error` | distingue "no cambió" de "no corrí" |
| `http_status` | int | nullable | null si no hubo respuesta |
| `n_stations` | int | nullable | |
| `duration_ms` | int | not null | |
| `error` | string | nullable | `feed_congelado` o el mensaje de la excepción |
| `last_updated` | int | nullable | `last_updated` del feed |
| `sink` | string | `local` \| `kafka` | permite comparar antes/después de la migración |

**state/collector.json** — estado del recolector (no es dato crudo): `last_status_last_updated`,
`last_status_change_utc`, `info_hash`, `info_refreshed_utc`.

**bronze/trips** — CSV mensual de viajes, **solo local** (PII): `data/bronze/trips/<archivo de la URL>`.

**silver/station_status** — una fila por estación por snapshot validado
(`silver/station_status/dt=…/hour=…/part-0.parquet`).

| Field | Type | Constraints | Notes |
|---|---|---|---|
| `station_id` | string | not null, en `station_information` | |
| `snapshot_last_updated` | timestamp[s, UTC] | not null | del feed |
| `last_reported` | timestamp[s, UTC] | not null | (`station_id`, `last_reported`) único |
| `num_bikes_available` | int16 | ≥ 0 | |
| `num_bikes_disabled` | int16 | ≥ 0 | |
| `num_docks_available` | int16 | ≥ 0 | |
| `num_docks_disabled` | int16 | ≥ 0 | |
| `capacity` | int16 | > 0 | de `station_information`; la suma de los cuatro anteriores es igual |
| `is_installed`, `is_renting`, `is_returning` | bool | not null | |
| `is_charging` | bool | nullable | |

**silver/quarantine** — mismas columnas que bronze aplanado (como string si el tipo falló) +
`motivo` (`esquema` \| `estacion_desconocida` \| `regla_capacidad`); misma partición.

**silver/station_information** — `silver/station_information/station_information.parquet`:
`station_id`, `short_name`, `short_name_norm`, `name`, `lat`, `lon`, `capacity`, `info_hash`.

**silver/trips** — `silver/trips/month=YYYY-MM/part-0.parquet`: `estacion_retiro`,
`estacion_retiro_norm`, `retiro_utc`, `estacion_arribo`, `estacion_arribo_norm`, `arribo_utc`
(lista blanca; nunca `Genero_Usuario`, `Edad_Usuario`, `Bici`).

**silver/weather** — `silver/weather/weather.parquet`: `ts_utc` (único), `temperature_2m`,
`precipitation`, `relative_humidity_2m`, `wind_speed_10m`.

**gold/station_hour** — `gold/station_hour/dt=…/part-0.parquet`: `station_id`, `hora_utc`,
`bikes_mean`, `bikes_min`, `bikes_max`, `docks_mean`, `n_obs`, `pct_vacia`, `pct_llena`.

**gold/features_base** — `gold/features_base/dt=…/part-0.parquet`: `station_id`, `ts_utc`
(rejilla de 5 min), `bikes_available`, `docks_available`, `capacity`, `is_renting`.

**gold/trip_demand** — `gold/trip_demand/trip_demand.parquet`: `short_name_norm`, `dow`,
`hour_local`, `salidas_prom`, `llegadas_prom`, `tasa_match` (constante del archivo).

**_manifest.json** (en cada partición silver/gold) — llaves ordenadas, sin marcas de tiempo:
silver = `hora`, `archivos_bronze`, `filas_leidas`, `filas_validas`, `filas_cuarentena`,
`filas_duplicadas`, `tasa_regla_capacidad`, `sha256_validas`; gold = `dia`,
`filas_station_hour`, `filas_features_base`, `sha256_station_hour`.

### Relationships

- `station_information (1) —(1..N)→ silver/station_status` por `station_id` (validado por
  `KnownStationCheck`; sin cascada: el lake es inmutable por capa).
- `silver/station_information.short_name_norm (1) —(0..N)→ silver/trips.estacion_*_norm`
  (emparejamiento parcial; códigos compuestos como `266-267` quedan sin pareja y se reportan).
- `silver/station_status (N) —→ gold/station_hour (1 por estación-hora)` y `gold/features_base`.
- Borrado: no existe en bronze. Silver/gold se **reemplazan** por partición completa al reprocesar.

### Indexes

| Table | Index | Why |
|---|---|---|
| bronze/station_status | nombre de archivo `station_status_<last_updated>` | idempotencia de escritura (local y Kafka) |
| silver/* y gold/* | partición `dt=`/`hour=` | reprocesar una hora toca una carpeta; DuckDB poda particiones |
| silver/station_status | orden físico por (`station_id`, `last_reported`) | `sha256_validas` determinista y lecturas por estación |

### Schema

Esquemas de Parquet que los jobs declaran como constantes de módulo (`SILVER_STATUS_SCHEMA` y
`SILVER_STATION_INFO_SCHEMA` en `bronze_to_silver.py`, `GOLD_STATION_HOUR_SCHEMA` y
`GOLD_FEATURES_BASE_SCHEMA` en `silver_to_gold.py`, `SILVER_TRIPS_SCHEMA` y
`GOLD_TRIP_DEMAND_SCHEMA` en `trips.py`, `SILVER_WEATHER_SCHEMA` en `weather.py`) y escriben con `pyarrow`:

```python
import pyarrow as pa

SILVER_STATUS_SCHEMA = pa.schema([
    ("station_id", pa.string()),
    ("snapshot_last_updated", pa.timestamp("s", tz="UTC")),
    ("last_reported", pa.timestamp("s", tz="UTC")),
    ("num_bikes_available", pa.int16()),
    ("num_bikes_disabled", pa.int16()),
    ("num_docks_available", pa.int16()),
    ("num_docks_disabled", pa.int16()),
    ("capacity", pa.int16()),
    ("is_installed", pa.bool_()),
    ("is_renting", pa.bool_()),
    ("is_returning", pa.bool_()),
    ("is_charging", pa.bool_()),
])

SILVER_STATION_INFO_SCHEMA = pa.schema([
    ("station_id", pa.string()), ("short_name", pa.string()), ("short_name_norm", pa.string()),
    ("name", pa.string()), ("lat", pa.float64()), ("lon", pa.float64()),
    ("capacity", pa.int16()), ("info_hash", pa.string()),
])

SILVER_TRIPS_SCHEMA = pa.schema([
    ("estacion_retiro", pa.string()), ("estacion_retiro_norm", pa.string()),
    ("retiro_utc", pa.timestamp("s", tz="UTC")),
    ("estacion_arribo", pa.string()), ("estacion_arribo_norm", pa.string()),
    ("arribo_utc", pa.timestamp("s", tz="UTC")),
])

SILVER_WEATHER_SCHEMA = pa.schema([
    ("ts_utc", pa.timestamp("s", tz="UTC")), ("temperature_2m", pa.float64()),
    ("precipitation", pa.float64()), ("relative_humidity_2m", pa.float64()),
    ("wind_speed_10m", pa.float64()),
])

GOLD_STATION_HOUR_SCHEMA = pa.schema([
    ("station_id", pa.string()), ("hora_utc", pa.timestamp("s", tz="UTC")),
    ("bikes_mean", pa.float64()), ("bikes_min", pa.int16()), ("bikes_max", pa.int16()),
    ("docks_mean", pa.float64()), ("n_obs", pa.int32()),
    ("pct_vacia", pa.float64()), ("pct_llena", pa.float64()),
])

GOLD_FEATURES_BASE_SCHEMA = pa.schema([
    ("station_id", pa.string()), ("ts_utc", pa.timestamp("s", tz="UTC")),
    ("bikes_available", pa.int16()), ("docks_available", pa.int16()),
    ("capacity", pa.int16()), ("is_renting", pa.bool_()),
])

GOLD_TRIP_DEMAND_SCHEMA = pa.schema([
    ("short_name_norm", pa.string()), ("dow", pa.int8()), ("hour_local", pa.int8()),
    ("salidas_prom", pa.float64()), ("llegadas_prom", pa.float64()), ("tasa_match", pa.float64()),
])
```

### Migrations

NOT APPLICABLE como migraciones de base de datos — no hay base relacional propia (Postgres es de
Airflow y MLflow usa SQLite; ambos migran solos al arrancar). Regla de evolución del lake:
**bronze nunca cambia**; si un esquema silver/gold cambia, se actualiza la constante del módulo y
se reprocesa con `bronze_to_silver --backfill` (o `--hour` por hora), que reemplaza particiones
completas. Un campo requerido que desaparece del feed hace fallar `SchemaCheck` de forma visible
(cuarentena con `motivo="esquema"`), nunca un null silencioso.

### Seed data

Sin seed: los datos reales llegan del feed desde el paso 3. Las pruebas usan las respuestas
grabadas `tests/fixtures/gbfs.json`, `tests/fixtures/station_status.json` y
`tests/fixtures/station_information.json` (emitidas en `workspace/`): 3 estaciones conocidas, la
`"3"` viola la regla de capacidad, la `"999"` es desconocida, `last_updated = 1794160800`
(2026-11-08T18:00:00Z).

---

## 5. API Design

Tres contratos: la API HTTP (FastAPI), los mensajes de Kafka y la interfaz de los CLI.

### Conventions

- Base path: `/` (API local de un solo consumidor; sin versionado de ruta en v1).
- Response envelope: **sin envoltura** — cada ruta devuelve directamente su modelo Pydantic (lista
  u objeto). Error: siempre `{"detail": {"codigo": "<codigo>", "mensaje": "<texto en español>"}}`,
  salvo 422 que conserva el formato de validación de FastAPI (`{"detail": [ ... ]}`).
- Error codes: `estacion_no_encontrada` → 404; `modelo_no_disponible` → 503;
  `datos_no_disponibles` → 503; validación de parámetros → 422 (FastAPI).
- Validation: Pydantic v2; esquemas en `src/radar/api/schemas.py`; parámetros con `Query(ge=1, le=24)`.
- Pagination: ninguna — `/stations` devuelve las ~677 estaciones (≈ 150 KB), que el mapa necesita completas.
- Idempotency: todas las rutas son `GET` puras.
- Rate limits: ninguno (API local de un solo cliente; ver §14).
- CORS: `allow_origins` = `API_CORS_ORIGINS` (`http://localhost:5173`), `allow_methods=["GET"]`.

### Routes

| Method | Path | Description | Auth | Rate limit |
|---|---|---|---|---|
| GET | `/health` | Estado de la API, del modelo y frescura de datos | public | ninguno |
| GET | `/stations` | Estado actual de cada estación (última fila silver) + `anomalia` | public | ninguno |
| GET | `/stations/{station_id}/history?hours=6` | Serie de bicis/docks de las últimas `hours` (1–24) | public | ninguno |
| GET | `/predict/{station_id}` | Predicción t+60 de una estación | public | ninguno |
| GET | `/predictions` | Predicción t+60 de todas las estaciones (lo que colorea el mapa) | public | ninguno |
| GET | `/anomalies` | Anomalías actuales (lista vacía hasta el paso 15) | public | ninguno |
| GET | `/docs`, `/openapi.json` | Swagger generado por FastAPI | public | ninguno |

### Critical endpoints — full detail

**Modelos Pydantic (contrato exacto de `schemas.py`):**

```python
from datetime import datetime
from typing import Literal
from pydantic import BaseModel

PredictionClass = Literal["se_vacia", "estable", "se_llena"]

class HealthOut(BaseModel):
    status: Literal["ok"]
    modelo_cargado: bool
    fuente_modelo: Literal["mlflow", "archivo", "ninguno"]
    segundos_desde_ultimo_dato: float | None

class StationOut(BaseModel):
    station_id: str
    nombre: str
    lat: float
    lon: float
    capacidad: int
    bicis_disponibles: int
    docks_disponibles: int
    ultimo_reporte_utc: datetime
    anomalia: bool = False

class HistoryPointOut(BaseModel):
    ts_utc: datetime
    bicis_disponibles: int
    docks_disponibles: int

class PredictionOut(BaseModel):
    station_id: str
    bicis_actuales: int
    prediccion_t60: float
    delta: float               # prediccion_t60 - bicis_actuales
    clase: PredictionClass     # delta <= -3 se_vacia · delta >= 3 se_llena · resto estable
    generado_utc: datetime
    modelo_version: str        # "mlflow:<version>" o "archivo"

class AnomalyOut(BaseModel):
    station_id: str
    tipo: Literal["congelada", "zscore", "isolation_forest"]
    puntaje: float
    detalle: str
    detectado_utc: datetime

class ErrorDetail(BaseModel):
    codigo: str
    mensaje: str
```

**`GET /health`** — siempre 200 si el proceso vive. `modelo_cargado=false` con
`fuente_modelo="ninguno"` cuando ni MLflow ni `models/champion.joblib` están disponibles;
`segundos_desde_ultimo_dato` = ahora − `snapshot_last_updated` más reciente en silver (null si no
hay silver). Sin efectos secundarios.

**`GET /stations`** — 200 con `list[StationOut]`; 503 `datos_no_disponibles` si no existe
`silver/station_information/station_information.parquet` o ninguna partición silver.
`anomalia=true` exactamente para los `station_id` que `/anomalies` devuelve.

**`GET /stations/{station_id}/history`** — `hours` entero 1–24 (default 6; fuera de rango → 422);
404 `estacion_no_encontrada` si el id no existe; 200 con puntos ordenados por `ts_utc` ascendente.

**`GET /predict/{station_id}`** — 404 `estacion_no_encontrada` si el id no existe; 503
`modelo_no_disponible` sin modelo; 503 `datos_no_disponibles` si el marco de inferencia no tiene
fila para esa estación; 200 con `PredictionOut` (predicción recortada a `[0, capacidad]`). El
modelo se carga **una vez** en el lifespan; ninguna petición lo recarga.

**`GET /predictions`** — igual que la anterior para todas las estaciones con fila de inferencia;
503 `modelo_no_disponible` sin modelo.

**`GET /anomalies`** — 200 con `list[AnomalyOut]` (vacía hasta el paso 15; luego los tres
detectores, caché 60 s).

### Contrato de mensajes Kafka

| Topic | Particiones | Key | Value | Headers | Consumidores |
|---|---|---|---|---|---|
| `station_status` | 3 | `station_id` (UTF-8) | JSON de una estación + `feed_last_updated` (int) + `snapshot_id` (`station_status_<last_updated>`) | — | futuros consumidores por estación (orden garantizado por key) |
| `station_status_raw` | 1 | `station_status` | snapshot crudo en gzip (`mtime=0`) | `last_updated`, `content-encoding: gzip` | grupo `bronze-writer` |

Productor: `acks=all`, `enable.idempotence=true`. Consumidor: `enable.auto.commit=false`,
`auto.offset.reset=earliest`, commit síncrono **después** del fsync del archivo ⇒ at-least-once +
escritura idempotente por nombre = efectivamente una vez. Retención 7 días, RF 1 (un broker).

### Contrato de los CLI

Todos con `argparse`: salida `0` = éxito, `2` = error de uso (argparse), `3` = check fallido
(calidad, idempotencia, política, conteo mínimo). `--json` imprime un objeto JSON en stdout; los
logs van a stderr. Los `Verify` de §9 dependen de esta separación.

---

## 6. Frontend Architecture

### Routes

| Route | Page | Data source | Auth |
|---|---|---|---|
| `/` | Mapa 3D + panel | `GET /stations`, `GET /predictions` cada 60 s; `GET /stations/{id}/history?hours=6` al seleccionar | public |

### Rendering strategy

SPA cliente con Vite (`vite build` → `frontend/dist/`), sin SSR: el mapa es WebGL y solo existe en
el navegador. `index.html` fija `data-theme` con un script en línea antes del primer pintado (sin
destello de tema). No hay caché de datos más allá del último resultado en memoria; el polling cada
60 s es la política de refresco (el feed cambia ~cada 60 s).

### Component hierarchy

```
main.tsx            [W]  StrictMode + estilos globales
└── App.tsx         contenedor: useStations, useTheme, useReducedMotion, estación seleccionada
    ├── <a.skip-link href="#panel">Saltar al panel</a>
    ├── StationMap.tsx   presentacional: <DeckGL> + <Map mapStyle=BASEMAPS[theme]> (react-map-gl/maplibre)
    │                    ColumnLayer (altura=bicis, color=clase de predicción o rampa)
    │                    ScatterplotLayer de anomalías (paso 15), tooltip, FlyToInterpolator
    └── Panel.tsx        presentacional: <aside id="panel">
                         botón de tema · última actualización (aria-live) · leyenda (texto+ícono)
                         lista filtrable de estaciones (<button>) · detalle + recharts LineChart (6 h)
                         estados carga/vacío/error · atribución OpenFreeMap
```

### State management

- Estado del servidor: hooks propios (`useStations`, `useStationHistory`) con `fetch` + `setInterval`
  de 60000 ms y `AbortController`; sin librería de caché (un solo recurso, una sola vista).
- Estado de cliente: estación seleccionada y filtro en `App`; tema en `useTheme` (persistido en
  `localStorage["radar-tema"]`).
- Deliberadamente NO global: no hay Redux/Zustand/Context; dos componentes no lo justifican.

### Loading, empty, and error states

| Superficie | Cargando | Vacío | Error |
|---|---|---|---|
| Mapa | columnas ausentes + texto "Cargando estaciones…" en el panel | "Sin datos de estaciones todavía: ¿corre el recolector y el pipeline horario?" | "No se pudo contactar la API en <url>. Reintentando en 60 s." (sin borrar los datos previos) |
| Lista de estaciones | "Cargando…" | "Ninguna estación coincide con «<filtro>»" | igual que el mapa |
| Historia (recharts) | "Cargando últimas 6 h…" | "Sin historia para esta estación" | "No se pudo cargar la historia" |
| Predicción | clase "sin predicción" con ícono "—" | — | si `/predictions` da 503, columnas con color de rampa y aviso "Modelo no disponible" |

---

## 7. Design System

Fuente: decisiones del usuario (paleta verde) verificadas con `frontend/src/lib/scales.test.ts`.
Archivos: `frontend/src/styles/theme.css` (variables CSS) y `frontend/src/lib/scales.ts`
(valores en TS para deck.gl), ambos emitidos en `workspace/`.

### Colors

| Token | Light | Dark | Usage |
|---|---|---|---|
| `--primary` | `#15803D` | `#22C55E` | botones, enlaces, estado activo |
| `--primary-strong` | `#166534` | `#16A34A` | hover/pressed |
| `--primary-fg` | `#FFFFFF` | `#07120D` | texto sobre primario |
| `--bg` (background) | `#F2FAF5` | `#07120D` | página |
| `--surface` | `#FFFFFF` | `#0E1F17` | panel, tooltip |
| `--surface-2` | `#E7F4EC` | `#14291F` | filas, tarjetas internas |
| `--border` | `#C9E4D3` | `#1F3D2E` | divisores (decorativos) |
| `--text` (fg) | `#0D2A1C` | `#E3F5EA` | texto |
| `--text-muted` (fg-muted) | `#4A6B5A` | `#8DB8A1` | texto secundario |
| `--accent` | `#65A30D` | `#A3E635` | detalles decorativos |
| `--focus` | `#16A34A` | `#4ADE80` | anillo de foco |
| `--destructive` | `#B91C1C` | `#F87171` | texto de error |
| `--success` | `#15803D` | `#22C55E` | igual que `--primary` |
| `--anomaly` | `#EF4444` | `#EF4444` | marcador de anomalía en el mapa (UI, no texto) |

Columnas (bicis disponibles, igual en ambos temas): `#E5F5E0` (0) → `#A1D99B` (≤ 1/3) →
`#41AB5D` (≤ 2/3) → `#006D2C` (llena). Predicción (delta = predicción − actual): se vacía
(≤ −3) `#F97316` ▼, estable `#84CC16` ●, se llena (≥ +3) `#06B6D4` ▲. Mapa base: oscuro
`https://tiles.openfreemap.org/styles/dark`, claro `https://tiles.openfreemap.org/styles/positron`.

**Contrast:** todos los pares texto/fondo cumplen WCAG 2.2 AA y lo prueba
`npm --prefix frontend run test`. Los tres más riesgosos (medidos al escribir este blueprint):
`--focus` sobre `--bg` claro **3.10:1** (≥ 3:1 de UI), `--primary` sobre `--bg` claro
**4.72:1**, `--text-muted` sobre `--surface-2` claro **5.24:1**.

### Typography

| Role | Family | Size / line-height | Weight | Tracking |
|---|---|---|---|---|
| Display (título del panel) | `Inter, system-ui, -apple-system, "Segoe UI", Roboto, sans-serif` | 24px / 1.25 | 700 | -0.01em |
| Heading (secciones del panel) | misma pila | 18px / 1.3 | 600 | 0 |
| Body | misma pila | 15px / 1.5 | 400 | 0 |
| Números (bicis, horas) | misma pila con `font-variant-numeric: tabular-nums` | 15px / 1.5 | 600 | 0 |
| Mono (ids de estación) | `ui-monospace, "Cascadia Mono", Consolas, monospace` | 13px / 1.4 | 400 | 0 |

**Font loading:** sin fuentes descargadas — solo la pila del sistema por CSS (si el equipo tiene
Inter, se usa; si no, Segoe UI en Windows). Cero peticiones de fuentes, cero `font-display`.

### Spacing, radius, elevation

- Spacing scale: base 4px — 4, 8, 12, 16, 24, 32 (`--space-1` … `--space-6`).
- Radius: 6px botones/inputs (`--radius-sm`), 10px tarjetas (`--radius-md`), 16px panel (`--radius-lg`).
- Shadows: panel `0 8px 24px rgba(0, 0, 0, 0.28)`; todo lo demás plano con bordes.
- Max content width: panel `360px` (`--panel-width`) · Breakpoints: `< 768px` panel como hoja
  inferior (45 % de alto); `≥ 768px` panel lateral derecho.

### Motion

- Interacciones de UI: 150 ms (`--duration-fast`), `cubic-bezier(0.22, 1, 0.36, 1)`.
- Cámara del mapa: `FlyToInterpolator` 1200 ms al seleccionar una estación; vista inicial con
  `pitch` 50° y `bearing` −20°.
- `prefers-reduced-motion: reduce`: transiciones CSS a 0.01 ms (en `theme.css`) y
  `transitionDuration` 0 en la cámara (`useReducedMotion`).

### Component style

Instrumento de datos sobrio: el mapa oscuro manda y el panel es una superficie plana con bordes
finos, sin degradados ni sombras salvo la del panel. Verde = sistema ECOBICI; el color de las
columnas siempre significa algo (cantidad o tendencia) y siempre va acompañado de texto o ícono.
Un componente nuevo pertenece si usa solo tokens, tiene foco visible y no agrega color decorativo.

---

## 8. Authentication & Authorization

NOT APPLICABLE — el proyecto no tiene cuentas de usuario: todos los datos son públicos (feed GBFS
abierto), la API y el mapa corren solo en `localhost`, y la autenticación es un non-goal de §1.
Las únicas credenciales son de infraestructura (identidades S3 de SeaweedFS, perfil de AWS,
token de GitHub), tratadas en §14.

---

## 9. BUILD ORDER

**Esta es la sección que todo el blueprint existe para producir.** Un builder que sigue §9 al pie de
la letra y se detiene cuando cada gate pasa a verde entrega el proyecto.

### The rules of a step

1. **Una sesión por paso.** Máximo **5 archivos** y **6 criterios de aceptación** por paso (los
   mismos números que `templates/tasks-schema.md` y el validador). Los archivos `[W]` de §3 ya
   existen antes del paso 1 y no cuentan.
2. **Cada paso trae los cuatro campos** `Do`, `Done when`, `Verify`, `Checkpoint`, y se ejecutan
   en ese orden. El `Checkpoint` es un bloque de shell literal: `git add -A && git commit -m
   "<tipo>(<id>): <descripción>"` (convencional en español, con el id de la tarea) y luego
   `git tag step-NN-<slug>`.
3. **"Done when" es observable y decidible por una máquina**, en forma EARS
   (**WHEN** `<disparador>` **THE SYSTEM SHALL** `<respuesta observable>`).
4. **"Se ve bien" está prohibido**, igual que *funciona*, *está implementado*, *renderiza bien*.
5. **`Verify` es shell literal** (Git Bash), copiable, con el resultado esperado en un comentario.
6. **Un paso no está hecho hasta que pasan sus `Verify`** *y* los de los pasos previos siguen pasando.
7. **Checkpoint en cada paso** (`git tag step-NN-<slug>`). Si un paso se rompe:
   `git reset --hard <tag del paso anterior>` y reintenta; nunca depures hacia adelante. El
   rollback **pide confirmación al usuario** (`Bash(git reset --hard step-:*)` está en `ask`
   en `.claude/settings.json`) porque descarta trabajo sin commit.
8. **Nunca te adelantes.** Si un paso está bloqueado, detente y reporta. Excepción documentada:
   el time-box de 3 h del paso 9 y la tabla de recortes de §20.2, que solo el usuario activa.
9. **Ningún paso rompe retroactivamente el gate de un paso anterior.** La configuración se valida
   por paso (§10 "Required by step"): `Settings` no exige secretos; cada módulo que usa uno lo
   valida al usarlo. Antes de escribir el paso N+1 se releyeron los `Verify` previos.
10. **Ningún número derivado sin contarlo.** Los `Verify` afirman propiedades (`exit 0`, manifiestos
    idénticos, "todas las secciones presentes"); los únicos conteos son invariantes del contenido
    del propio blueprint: 4 mensajes de estación del fixture de 4 estaciones (paso 4), 5 comandos
    del README (paso 18) y 10 diapositivas (paso 18).
11. **Todo `Verify` sale 0 cuando el paso es correcto.** Los caminos de error documentados se
    envuelven: `…; test $? -eq 3`. Código 2 = uso (argparse), 3 = check fallido: nunca se confunden.
12. **Cada check es posible en su medio:** pruebas sin red para la lógica; comandos en vivo solo
    para propiedades del sistema corriendo (contenedores, feed, broker).
13. **El primer paso que produce algo ejecutable lo corre:** el recolector corre en el paso 3, el
    productor en el 4, el consumidor en el 5, la app FastAPI completa en el 12 (TestClient con
    lifespan sobre datos reales) y su contenedor en el 13, el build del mapa con smoke en el 14.
14. **Un `Verify` no depende de su propio `Checkpoint`.** Ningún `Verify` revisa árbol limpio,
    archivos rastreados ni tags del mismo paso; esas aserciones viven después del commit en el
    bloque `Checkpoint` o en §20.1.
15. **No hay salidas byte-exactas escritas antes de su productor** (ver §19.6): las comparaciones
    son corrida-contra-corrida (manifiestos) o propiedades.
16. **Un gate falla por la razón correcta.** Ningún gate acepta "cualquier salida distinta de 0";
    `git check-ignore` se usa con una sola ruta por invocación y se compara con su código exacto.

### One step, one unit — the counting rule

> **Un paso de §9 = una tarea de `tasks.json` = un bloque de tarea en un epic.**

18 pasos ⇒ 18 tareas ⇒ 18 bloques. La regla de §9 de la plantilla (5–9 pasos por epic) permite 2 o
3 epics para 18 pasos; se usan **3 de 6 pasos** cada uno, cortados en fronteras de capa:
`01-ingesta-streaming` (pasos 1–6), `02-lake-modelo-api` (7–12), `03-producto-entrega` (13–18).
Las 10 "Etapas" del plan siguen visibles como etiqueta de cada paso.

**Calendario:** dom 8 → pasos 1–3 (¡el recolector se enciende HOY!); lun 9 → 4–8; mar 10 → 9–11;
mié 11 → 12–14; jue 12 → 15–18; vie 13 → ensayo y exposición.

### Step map

| # | Paso | Tarea | Epic | Depende de | Toca | Gate | Día |
|---|---|---|---|---|---|---|---|
| 1 | Fundar el repo: licencia, README, arquitectura y bitácora | `E1-T1` | `01-ingesta-streaming` | — (Bootstrap §10) | 5 archivos | `git check-ignore -q .env` | dom 8 |
| 2 | Núcleo del recolector: settings, reintentos y hash | `E1-T2` | `01-ingesta-streaming` | 1 | 5 archivos | `uv run pytest tests/unit/test_recolector_nucleo.py tests/unit/test_…` | dom 8 |
| 3 | Recolector en Docker: sinks, heartbeat y loop de 60 s | `E1-T3` | `01-ingesta-streaming` | 2 | 5 archivos | `uv run pytest tests/unit/test_runner.py tests/unit/test_encabezados…` | dom 8 |
| 4 | Kafka KRaft, topics y productor KafkaSink | `E1-T4` | `01-ingesta-streaming` | 3 | 4 archivos | `uv run pytest tests/unit/test_messages.py tests/unit/test_encabezad…` | lun 9 |
| 5 | Consumidor bronze-writer, demo de recuperación y migración | `E1-T5` | `01-ingesta-streaming` | 4 | 4 archivos | `uv run pytest tests/unit/test_consumer.py tests/unit/test_encabezad…` | lun 9 |
| 6 | SeaweedFS: buckets, mínimo privilegio y LakeStore | `E1-T6` | `01-ingesta-streaming` | 3 | 4 archivos | `uv run pytest tests/unit/test_store.py tests/unit/test_encabezados.py` | lun 9 |
| 7 | Bronze a silver con cadena de calidad y cuarentena | `E2-T1` | `02-lake-modelo-api` | 6 | 5 archivos | `uv run pytest tests/unit/test_quality.py tests/unit/test_bronze_to_…` | lun 9 |
| 8 | Gold horario, viajes históricos, SQL analítico y benchmark | `E2-T2` | `02-lake-modelo-api` | 7 | 5 archivos | `uv run pytest tests/unit/test_gold_trips.py tests/unit/test_encabez…` | lun 9 |
| 9 | Pipeline horario con clima, checks de calidad y Airflow | `E2-T3` | `02-lake-modelo-api` | 8 | 5 archivos | `uv run pytest tests/unit/test_encabezados.py` | mar 10 |
| 10 | Notebook EDA y features sin fuga de datos | `E2-T4` | `02-lake-modelo-api` | 9 | 4 archivos | `uv run pytest tests/unit/test_features.py tests/unit/test_encabezad…` | mar 10 |
| 11 | Entrenar baseline, lineal y LightGBM con MLflow | `E2-T5` | `02-lake-modelo-api` | 10 | 5 archivos | `uv run pytest tests/unit/test_models.py tests/unit/test_encabezados…` | mar 10 |
| 12 | API FastAPI con repositorio, DI y modelo cargado una vez | `E2-T6` | `02-lake-modelo-api` | 11 | 5 archivos | `uv run pytest tests/integration/test_api.py tests/unit/test_encabez…` | mié 11 |
| 13 | CI en GitHub Actions, imagen de la API y runbook | `E3-T1` | `03-producto-entrega` | 12 | 5 archivos | `uv run python -c "import sys; t = open('.github/workflows/ci.yml', …` | mié 11 |
| 14 | Mapa 3D con deck.gl, tema claro/oscuro y panel lateral | `E3-T2` | `03-producto-entrega` | 12 | 5 archivos | `npm --prefix frontend install` | mié 11 |
| 15 | Detección de anomalías conectada a la API y al mapa | `E3-T3` | `03-producto-entrega` | 14 | 5 archivos | `uv run pytest tests/unit/test_detectors.py tests/unit/test_encabeza…` | jue 12 |
| 16 | Terraform para S3, IAM mínimo y presupuesto de 5 USD | `E3-T4` | `03-producto-entrega` | 8 | 4 archivos | `terraform -chdir=infra/terraform fmt -check -recursive` | jue 12 |
| 17 | Asistente RAG local con Ollama y fuentes citadas | `E3-T5` | `03-producto-entrega` | 11 | 4 archivos | `uv run pytest tests/unit/test_rag.py tests/unit/test_encabezados.py` | jue 12 |
| 18 | Documentación final, guion de video y presentación | `E3-T6` | `03-producto-entrega` | 13, 14 | 5 archivos | `uv run python -c "import sys; t = open('README.md', encoding='utf-8…` | jue 12 |

#### Step 1 — Fundar el repo: licencia, README, arquitectura y bitácora

> Etapa 0 — Repo y arquitectura · tarea `E1-T1` · epic `01-ingesta-streaming` · prioridad `p0` · depende de: Bootstrap de §10 · checkpoint `step-01-fundacion`

**Meta:** El repo existe con `.gitignore` correcto desde el primer commit, licencia MIT, README inicial, diagrama Mermaid, métricas definidas y la bitácora del usuario dentro de `docs/`.

**Archivos:** `docs/bitacora.md`, `LICENSE`, `README.md`, `docs/arquitectura.md`, `docs/metrics.md`

**Do**
Precondición: el Bootstrap de §10 ya corrió (existen `.git/`, `uv.lock`, `.env`, `.gitignore` y el
primer commit `chore: estructura inicial desde el blueprint`). Si no, córrelo primero.

1. **Bitácora.** Copia la bitácora del usuario sin modificarla y luego agrega la fase nueva al final:
   ```bash
   [ -e docs/bitacora.md ] || cp "../ECO_BIKES/Bitácora — Radar ECOBICI CDMX.md" docs/bitacora.md
   ```
   Si el archivo origen no existe, detente y pregunta al usuario por la ruta (máximo 3 preguntas).
   Agrega al final de `docs/bitacora.md` exactamente esta sección (formato de la bitácora):
   ```markdown
   ---
   ## Fase 2 — Arquitectura y construcción
   **Fechas:** 8–13 nov 2026 · **Estado:** ⏳ En proceso

   ### Decisiones

   **D-009 · SeaweedFS reemplaza a MinIO como S3 local**
   - *Alternativas:* MinIO (repo archivado el 2026-04-25, sin imágenes comunitarias nuevas); Garage (requiere configurar layout y llaves con su CLI); RustFS (versión 1.0 con dos semanas de vida).
   - *Por qué:* SeaweedFS corre en un solo contenedor (`weed server -s3`), tiene una API S3 madura, identidades con acciones por bucket (mínimo privilegio) y desarrollo muy activo.
   - *Consecuencias:* las identidades viven en `docker/seaweedfs/s3.json` (generado e ignorado en git); el código habla S3 vía boto3, así que cambiar a AWS S3 solo cambia el endpoint.

   **D-010 · Mapa base OpenFreeMap**
   - *Alternativas:* CARTO basemaps; Mapbox; MapTiler.
   - *Por qué:* OpenFreeMap no pide token ni tarjeta (estilos `dark` y `positron` responden 200 sin llave); CARTO se descartó porque exige API key.
   - *Consecuencias:* atribución obligatoria "OpenFreeMap © OpenMapTiles Data from OpenStreetMap"; sus términos de uso no se leyeron aún (pendiente antes de publicar el video).
   ```
2. **`LICENSE`** — texto MIT estándar; primera línea `MIT License`, tercera línea
   `Copyright (c) 2026 Angel Isacc Cabrera Leal`.
3. **`README.md`** — título `# Radar ECOBICI CDMX` y las secciones, en este orden y con estos
   encabezados exactos: `## Problema`, `## Objetivo`, `## Arquitectura` (enlaza
   `docs/arquitectura.md`), `## Stack` (tabla del §2 sin versiones), `## Estado por etapas`
   (checklist de las Etapas 0–10 del plan, con la 0 marcada), `## Cómo correrlo` (enlaza al
   Bootstrap) y `## Licencia`.
4. **`docs/arquitectura.md`** — explicación breve por capa + este diagrama (es el que se ve en GitHub):
   ````markdown
   ```mermaid
   flowchart LR
     GBFS[(Feed GBFS ECOBICI)] -->|cada 60 s| COL[Recolector Python]
     COL -->|SnapshotSink: KafkaSink| K[(Kafka KRaft\nstation_status · station_status_raw)]
     K -->|grupo bronze-writer| BW[Consumidor]
     BW --> B[(bronze JSON.gz + heartbeat)]
     B --> S[(silver Parquet + cuarentena)]
     S --> G[(gold agregados + features)]
     B & S & G -. sync .-> SW[(SeaweedFS S3)]
     G -. gold .-> AWS[(AWS S3)]
     AF[Airflow horario/diario] --> S
     AF --> G
     OM[Open-Meteo] --> AF
     G --> ML[Entrenamiento + MLflow]
     ML -->|radar-bikes-t60@champion| API[FastAPI]
     S --> API
     API --> MAP[Mapa 3D React + deck.gl]
     DOCS[docs/ + reportes] --> RAG[RAG local Ollama]
   ```
   ````
5. **`docs/metrics.md`** — tabla con definición, grano, fuente y umbral de: `cobertura_heartbeat`
   (% de minutos con heartbeat `guardado` o `sin_cambios` por hora), `tasa_regla_capacidad`
   (fracción de filas que cumplen la regla de capacidad), `filas_cuarentena`, `lag_consumidor`
   (mensajes pendientes del grupo `bronze-writer`), `mae_t60` (MAE de bicis a 60 min),
   `mejora_vs_baseline_pct` (`100 × (mae_baseline − mae_modelo) / mae_baseline`) y `latencia_api_p95`.

**Done when**
- [ ] WHEN `git check-ignore -q .env` runs THE SYSTEM SHALL exit 0, and `git check-ignore -q .env.example` SHALL exit 1 (`.env` ignored, `.env.example` not ignored).
- [ ] WHEN `git log --all --name-only --format= -- .env` runs THE SYSTEM SHALL print nothing, proving `.env` was never committed.
- [ ] WHEN `docs/bitacora.md` is read THE SYSTEM SHALL contain the decision ids `D-008`, `D-009` and `D-010`.
- [ ] WHEN `LICENSE` is read THE SYSTEM SHALL have `MIT License` as its first line and contain `Copyright (c) 2026 Angel Isacc Cabrera Leal`.
- [ ] WHEN `docs/arquitectura.md` and `README.md` are read THE SYSTEM SHALL find a line that is exactly three backticks followed by `mermaid` in the former and the lines `## Problema`, `## Objetivo`, `## Arquitectura`, `## Stack` and `## Estado por etapas` in the latter.
- [ ] WHEN `docs/metrics.md` is read THE SYSTEM SHALL define `cobertura_heartbeat`, `tasa_regla_capacidad`, `mae_t60` and `mejora_vs_baseline_pct`.

**Verify**
```bash
git check-ignore -q .env  # expect: exit 0
git check-ignore -q .env.example; test $? -eq 1  # expect: código 1 → la línea sale 0
test -z "$(git log --all --name-only --format= -- .env)"  # expect: exit 0
grep -q 'D-008' docs/bitacora.md && grep -q 'D-009' docs/bitacora.md && grep -q 'D-010' docs/bitacora.md  # expect: exit 0
head -n 1 LICENSE | grep -qx 'MIT License' && grep -qF 'Copyright (c) 2026 Angel Isacc Cabrera Leal' LICENSE  # expect: exit 0
grep -qx '```mermaid' docs/arquitectura.md  # expect: exit 0
uv run python -c "import sys; t = open('README.md', encoding='utf-8').read().splitlines(); sys.exit(0 if all(h in t for h in ['## Problema', '## Objetivo', '## Arquitectura', '## Stack', '## Estado por etapas']) else 1)"  # expect: exit 0
uv run python -c "import sys; t = open('docs/metrics.md', encoding='utf-8').read(); sys.exit(0 if all(m in t for m in ['cobertura_heartbeat', 'tasa_regla_capacidad', 'mae_t60', 'mejora_vs_baseline_pct']) else 1)"  # expect: exit 0
```

**Checkpoint**
```bash
git add -A && git commit -m "docs(E1-T1): fundación del repo, licencia MIT, arquitectura y bitácora"
git tag step-01-fundacion
test -z "$(git status --porcelain)"  # expect: árbol limpio: el commit tomó todo
git ls-files --error-unmatch .gitignore  # expect: exit 0: versionado desde el commit del Bootstrap
git ls-files --error-unmatch .env.example  # expect: exit 0
git ls-files --error-unmatch docs/bitacora.md  # expect: exit 0
# --- PUERTA HUMANA: publicar el repo (acción hacia afuera) ---
# Anuncia: "Voy a crear github.com/Isacc-Cabrera/radar-ecobici-cdmx (público) y hacer push de main y tags".
# Ejecuta SOLO después del "sí" explícito del usuario. Si dice que no, omite estas dos líneas: ningún paso posterior las requiere.
GH_TOKEN="$(gh auth token -u IsaccCabrera09)" gh repo create Isacc-Cabrera/radar-ecobici-cdmx --public --source . --remote origin --push
git -c credential.helper= -c 'credential.helper=!f() { echo username=x-access-token; echo "password=$(gh auth token -u IsaccCabrera09)"; }; f' push --tags
# rollback si el paso 2 se rompe: git reset --hard step-01-fundacion
```

**Preguntas de checkpoint:** ¿Qué hace `git add .` si `.gitignore` no existe? · ¿Para qué sirve `.env.example`? · **Retos:** 7 (GitHub) · Post #1 se publica el lunes 9, después de encender el recolector (paso 3).

#### Step 2 — Núcleo del recolector: settings, reintentos y hash

> Etapa 1a — Núcleo del recolector · tarea `E1-T2` · epic `01-ingesta-streaming` · prioridad `p0` · depende de: paso 1 (`E1-T1`) · checkpoint `step-02-recolector-nucleo`

**Meta:** Existe la lógica pura del recolector (configuración, descubrimiento GBFS, cliente con política de reintentos y hash de estaciones), probada sin red.

**Archivos:** `src/radar/config.py`, `src/radar/collector/retry.py`, `src/radar/collector/gbfs_client.py`, `src/radar/collector/hashing.py`, `tests/unit/test_recolector_nucleo.py`

**Do**
- **`src/radar/config.py`** — `class Settings(BaseSettings)` con un campo por variable de la tabla
  de §10 (nombre en minúsculas: `gbfs_url`, `gbfs_lang`, `retry_base_seconds`, …; `data_dir: Path`;
  secretos como `str | None = None`). `model_config = SettingsConfigDict(env_file=".env",
  env_file_encoding="utf-8", extra="ignore", env_ignore_empty=True)` — `extra="ignore"` es
  obligatorio porque `.env` también trae variables de Compose y AWS. `get_settings()` con
  `functools.lru_cache`. Ningún secreto es obligatorio aquí: cada paso que lo usa lo valida
  (§9 regla 9: la validación se degrada por paso).
- **`src/radar/collector/retry.py`** — patrón **Strategy**. `@dataclass(frozen=True) RetryDecision(retry: bool,
  delay_s: float, reason: str)`; `class RetryPolicy(Protocol)` con `decide(attempt, status,
  retry_after, error) -> RetryDecision`; `ExponentialJitterPolicy(base_s, cap_s, max_attempts=6,
  rng=random.Random())`: 4xx ≠ 429 → no reintenta; 429 → espera `min(cap_s, Retry-After)` (acepta
  segundos o fecha HTTP); 5xx, `httpx.TimeoutException`, `httpx.TransportError` → espera
  `rng.uniform(0, min(cap_s, base_s * 2**attempt))` (full jitter); agotado `max_attempts` → no reintenta.
- **`src/radar/collector/gbfs_client.py`** — `GbfsClient(settings, http: httpx.Client, policy:
  RetryPolicy, sleep: Callable[[float], None] = time.sleep)`. `discover() -> dict[str, str]` lee
  `GBFS_URL`, toma `data[GBFS_LANG].feeds` y devuelve `{name: url}`; si falta el idioma o los feeds
  `station_status`/`station_information` lanza `FeedSchemaError`. `fetch(name) -> FetchResult(payload,
  raw: bytes, http_status, duration_ms)` aplica la política; 4xx → `FeedClientError`; reintentos
  agotados → `FeedUnavailableError`. Encabezado `User-Agent: radar-ecobici-cdmx/0.1
  (+https://github.com/Isacc-Cabrera/radar-ecobici-cdmx)`.
- **`src/radar/collector/hashing.py`** — `canonical_stations(payload) -> bytes` (lista
  `data.stations` ordenada por `station_id`, `json.dumps(sort_keys=True, separators=(",", ":"),
  ensure_ascii=False)`, SIN `last_updated` ni `ttl`), `stations_hash(payload) -> str` (sha256 hex),
  `find_unknown_station_ids(status_payload, known_ids: set[str]) -> set[str]`.
- **`tests/unit/test_recolector_nucleo.py`** — `respx.mock(assert_all_mocked=True)`, fixtures de
  `tests/fixtures/`, `sleep` falso que registra esperas y `random.Random` con semilla.

**Done when**
- [ ] WHEN `uv run pytest tests/unit/test_recolector_nucleo.py` runs THE SYSTEM SHALL exit 0 with 0 failed while every HTTP call is served by respx mocks (no network).
- [ ] WHEN discovery reads `tests/fixtures/gbfs.json` with `GBFS_LANG=es` THE SYSTEM SHALL return the `station_status` and `station_information` URLs listed under `data.es.feeds`, and SHALL raise `FeedSchemaError` for a language absent from `data`.
- [ ] WHEN the mocked feed answers 503, 503 and then 200 THE SYSTEM SHALL return the 200 payload after exactly 2 waits, each between 0 and `min(300, RETRY_BASE_SECONDS * 2**attempt)` seconds.
- [ ] WHEN the mocked feed answers 429 with `Retry-After: 7` THE SYSTEM SHALL wait exactly 7 seconds before the next request.
- [ ] WHEN the mocked feed answers 404 THE SYSTEM SHALL make exactly 1 request and raise `FeedClientError` without waiting.
- [ ] WHEN two `station_information` payloads differ only in `last_updated` THE SYSTEM SHALL return the same `stations_hash`, a different one when a station's `capacity` changes, and `find_unknown_station_ids` SHALL return `{"999"}` for `tests/fixtures/station_status.json`.

**Verify**
```bash
uv run pytest tests/unit/test_recolector_nucleo.py tests/unit/test_encabezados.py  # expect: exit 0
uv run ruff check . && uv run ruff format --check . && uv run mypy  # expect: exit 0
```

**Checkpoint**
```bash
git add -A && git commit -m "feat(E1-T2): núcleo del recolector GBFS con reintentos y hash"
git tag step-02-recolector-nucleo
# rollback si el paso 3 se rompe: git reset --hard step-02-recolector-nucleo
```

**Preguntas de checkpoint:** ¿Por qué no se reintenta un 404 pero sí un 503? · ¿Por qué el hash excluye `last_updated`? · **Retos:** 16 (API pública), 18 (automatización)

#### Step 3 — Recolector en Docker: sinks, heartbeat y loop de 60 s

> Etapa 1b — Recolector en Docker · tarea `E1-T3` · epic `01-ingesta-streaming` · prioridad `p0` · depende de: paso 2 (`E1-T2`) · checkpoint `step-03-recolector-docker`

**Meta:** El recolector corre en un contenedor no root con `restart: unless-stopped`, escribe bronze y heartbeat cada 60 s, y queda encendido hasta el viernes.

**Archivos:** `src/radar/collector/sinks.py`, `src/radar/collector/heartbeat.py`, `src/radar/collector/runner.py`, `docker/collector.Dockerfile`, `tests/unit/test_runner.py`

**Do**
- **`sinks.py`** — patrón **Ports & Adapters** + **Factory**. Puerto `class SnapshotSink(Protocol)`:
  `write_status(raw: bytes, payload: dict) -> str` y `write_information(raw: bytes, payload: dict,
  sha8: str) -> str`, ambos devuelven `"guardado"` o `"ya_existia"`. Adaptador `LocalFileSink(data_dir)`:
  gzip del cuerpo crudo tal cual llegó, escritura atómica (`*.tmp` → `flush` → `os.fsync` →
  `os.replace`), y si el destino ya existe no lo toca. Rutas exactas del §4. `make_sink(settings)`:
  `"local"` → `LocalFileSink`; cualquier otro valor → `ValueError("SINK desconocido: …")` (el paso 4
  agrega `"kafka"`).
- **`heartbeat.py`** — `@dataclass Heartbeat(ts_utc, feed, result, http_status, n_stations,
  duration_ms, error, last_updated, sink)` y `HeartbeatWriter(data_dir).append(hb)` que agrega una
  línea JSON a `data/bronze/heartbeat/dt=<fecha de ts_utc>.jsonl` (append + flush).
- **`runner.py`** — `CollectorRunner(settings, client, sink, heartbeat, clock, sleep)`:
  `tick_status()`: si `last_updated` es igual al anterior → heartbeat `sin_cambios` sin escribir;
  si es nuevo → `sink.write_status` → `guardado`; si `last_updated` lleva congelado más de
  `STALE_ALERT_SECONDS` → `log.warning("feed_congelado")` + heartbeat `error` con
  `error="feed_congelado"`; ids desconocidos (`find_unknown_station_ids`) → log + `refresh_information(force=True)`.
  `refresh_information()`: al arrancar, cada `INFO_REFRESH_HOURS` y a demanda; escribe solo si
  `stations_hash` cambió. Estado en `data/state/collector.json`. Toda excepción de un tick se
  captura → heartbeat `error` con el mensaje; el loop nunca termina. Agenda con `time.monotonic()`
  cada `STATUS_INTERVAL_SECONDS`. `main()`: `configure_logging`, arma `httpx.Client(timeout=…)`,
  `make_sink`; con `RUN_ONCE=true` hace un refresh + un tick y sale 0 si el resultado fue
  `guardado` o `sin_cambios`, 1 si fue `error`. Se ejecuta con `python -m radar.collector.runner`.
- **`docker/collector.Dockerfile`** — exactamente:
  ```dockerfile
  # Autor: Isacc Cabrera
  # Descripción: Imagen del recolector, productor Kafka y consumidor bronze-writer (misma imagen).
  # Patrón: N/A
  # Fecha de creación: 2026-11-08
  # Cambios:
  #     - 2026-11-08 · Isacc Cabrera · Creación (paso 3).
  FROM python:3.12-slim
  ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never \
      UV_PROJECT_ENVIRONMENT=/app/.venv
  RUN pip install --no-cache-dir uv==0.13.7 && useradd --create-home --uid 10001 radar
  WORKDIR /app
  COPY pyproject.toml uv.lock .python-version ./
  RUN uv sync --frozen --no-dev --no-install-project --extra streaming --extra lake
  COPY src ./src
  RUN uv sync --frozen --no-dev --no-editable --extra streaming --extra lake \
      && mkdir -p /app/data && chown -R radar:radar /app
  ENV PATH="/app/.venv/bin:$PATH"
  USER radar
  CMD ["python", "-m", "radar.collector.runner"]
  ```
- **`tests/unit/test_runner.py`** — cliente falso que devuelve los fixtures, `settings` en `tmp_path`.
- Enciende y deja encendido: `docker compose up -d collector`. Desactiva la suspensión de la laptop.
  Revisión manual a los 5 min (no es gate): ver §20.1.

**Done when**
- [ ] WHEN `uv run pytest tests/unit/test_runner.py` runs THE SYSTEM SHALL exit 0 with 0 failed.
- [ ] WHEN a tick receives a `last_updated` already seen THE SYSTEM SHALL append a heartbeat line with `result="sin_cambios"` and write no new bronze file.
- [ ] WHEN `last_updated` stays frozen longer than `STALE_ALERT_SECONDS` THE SYSTEM SHALL log a WARNING and append a heartbeat with `result="error"` and `error="feed_congelado"`.
- [ ] WHEN the client raises any exception during a tick THE SYSTEM SHALL append a heartbeat with `result="error"` and keep the loop running.
- [ ] WHEN `docker compose run --rm -e RUN_ONCE=true collector` runs against the live feed THE SYSTEM SHALL exit 0 and leave at least one `station_status_*.json.gz` under `data/bronze/station_status/dt=*/hour=*/` and at least one `data/bronze/heartbeat/dt=*.jsonl` file.
- [ ] WHEN `docker compose up -d collector` runs THE SYSTEM SHALL report the `collector` container running with restart policy `unless-stopped` and user `radar` (not root).

**Verify**
```bash
uv run pytest tests/unit/test_runner.py tests/unit/test_encabezados.py  # expect: exit 0
uv run ruff check . && uv run ruff format --check . && uv run mypy  # expect: exit 0
docker compose build collector  # expect: exit 0
docker compose run --rm -e RUN_ONCE=true collector  # expect: exit 0
uv run python -c "import glob, sys; s = glob.glob('data/bronze/station_status/dt=*/hour=*/station_status_*.json.gz'); h = glob.glob('data/bronze/heartbeat/dt=*.jsonl'); sys.exit(0 if s and h else 1)"  # expect: exit 0
docker compose up -d collector  # expect: exit 0
test "$(docker inspect -f '{{.State.Running}} {{.HostConfig.RestartPolicy.Name}} {{.Config.User}}' "$(docker compose ps -q collector)")" = 'true unless-stopped radar'  # expect: exit 0
```

**Checkpoint**
```bash
git add -A && git commit -m "feat(E1-T3): recolector en Docker con bronze, heartbeat y loop de 60 s"
git tag step-03-recolector-docker
# rollback si el paso 4 se rompe: git reset --hard step-03-recolector-docker
```

**Preguntas de checkpoint:** ¿Qué se pierde si borro el contenedor sin volumen? · ¿Cómo distingo "sin cambios" de "recolector caído"? · **Retos:** 16 (API pública), 31 (Docker), Implementación de contenedores, 18 (automatización) · 📣 Post #1 (publicar lunes 9): "Empecé a construir una plataforma de datos en tiempo real con ECOBICI CDMX" — imagen: el diagrama de `docs/arquitectura.md`; aprendizaje: 10 s vs 60 s (5.9 M vs ~1 M filas/día).

#### Step 4 — Kafka KRaft, topics y productor KafkaSink

> Etapa 2a — Kafka KRaft y productor · tarea `E1-T4` · epic `01-ingesta-streaming` · prioridad `p0` · depende de: paso 3 (`E1-T3`) · checkpoint `step-04-kafka-productor`

**Meta:** Kafka 4 en modo KRaft corre en el perfil `streaming`, existen los topics `station_status` (3 particiones) y `station_status_raw` (1), y el recolector puede publicar con `SINK=kafka`.

**Archivos:** `src/radar/streaming/messages.py`, `src/radar/streaming/producer.py`, `src/radar/collector/sinks.py`, `tests/unit/test_messages.py`

**Do**
- **Diseño de mensajes (justificado en §5):** dos topics.
  - `station_status` — 3 particiones, key = `station_id`, un mensaje JSON por estación con todos
    sus campos + `feed_last_updated` + `snapshot_id` (`station_status_<last_updated>`). La key
    manda cada estación siempre a la misma partición ⇒ orden por estación para consumidores
    posteriores (anomalías).
  - `station_status_raw` — 1 partición, key = `station_status`, valor = snapshot crudo en gzip,
    headers `last_updated` y `content-encoding: gzip`. Lo consume el grupo `bronze-writer` (paso 5)
    para escribir bronze con el mismo nombre idempotente.
- **`messages.py`** — `@dataclass KafkaMessage(key: bytes, value: bytes, headers: list[tuple[str, bytes]])`,
  `snapshot_id(last_updated)`, `station_messages(payload) -> list[KafkaMessage]`,
  `raw_message(raw, last_updated) -> KafkaMessage` (`gzip.compress(raw, mtime=0)` para bytes deterministas).
- **`producer.py`** — adaptador `KafkaSink` del puerto `SnapshotSink`: `write_status` produce el
  raw y luego las N estaciones, `flush(30)` y falla (`KafkaDeliveryError`) si algún delivery
  report trae error; `write_information` delega en `LocalFileSink` (station_information se guarda
  directo: cambia una vez al día y no necesita buffer). Config del productor: `bootstrap.servers=KAFKA_BOOTSTRAP`,
  `acks=all`, `enable.idempotence=true`, `linger.ms=50`. CLI `python -m radar.streaming.producer
  --ensure-topics --check`: crea los topics si faltan (RF 1, `retention.ms` 7 días; ignora
  `TOPIC_ALREADY_EXISTS`), los describe, imprime JSON y sale 0 si las particiones coinciden, 3 si no.
- **`sinks.py` (editar)** — `make_sink` agrega la rama `"kafka"` con import diferido de
  `radar.streaming.producer.KafkaSink`. Agrega la línea a `Cambios`.
- Levanta Kafka: `docker compose --profile streaming up -d --wait kafka` (el servicio y su
  healthcheck ya están en `docker-compose.yml`). El recolector local del paso 3 sigue encendido.

**Done when**
- [ ] WHEN `uv run pytest tests/unit/test_messages.py` runs THE SYSTEM SHALL exit 0 with 0 failed.
- [ ] WHEN `tests/fixtures/station_status.json` is converted THE SYSTEM SHALL produce 4 station messages keyed by their `station_id`, each value carrying `feed_last_updated` and `snapshot_id` `station_status_1794160800`, plus 1 raw message keyed `station_status` whose value gunzips to the original bytes.
- [ ] WHEN `docker compose --profile streaming up -d --wait kafka` runs THE SYSTEM SHALL exit 0 with the broker healthy, and `docker compose --profile streaming config --services` SHALL list `kafka` and no service containing `zookeeper`.
- [ ] WHEN `uv run python -m radar.streaming.producer --ensure-topics --check` runs twice THE SYSTEM SHALL exit 0 both times with `station_status` at 3 partitions and `station_status_raw` at 1 partition.
- [ ] WHEN `docker compose --profile streaming run --rm -e RUN_ONCE=true collector-kafka` runs against the live feed THE SYSTEM SHALL exit 0 only after every produced message is acknowledged by the broker.

**Verify**
```bash
uv run pytest tests/unit/test_messages.py tests/unit/test_encabezados.py  # expect: exit 0
uv run ruff check . && uv run ruff format --check . && uv run mypy  # expect: exit 0
docker compose --profile streaming up -d --wait kafka  # expect: exit 0
docker compose --profile streaming config --services | uv run python -c "import sys; s = sys.stdin.read().split(); sys.exit(0 if 'kafka' in s and not any('zookeeper' in x for x in s) else 1)"  # expect: exit 0
uv run python -m radar.streaming.producer --ensure-topics --check  # expect: exit 0
uv run python -m radar.streaming.producer --ensure-topics --check  # expect: exit 0
docker compose --profile streaming build collector-kafka  # expect: exit 0
docker compose --profile streaming run --rm -e RUN_ONCE=true collector-kafka  # expect: exit 0
```

**Checkpoint**
```bash
git add -A && git commit -m "feat(E1-T4): Kafka KRaft, topics y productor KafkaSink"
git tag step-04-kafka-productor
# rollback si el paso 5 se rompe: git reset --hard step-04-kafka-productor
```

**Preguntas de checkpoint:** ¿Por qué la key `station_id` garantiza el orden por estación? · ¿Por qué dos topics y no uno? · **Retos:** 30 (Kafka)

#### Step 5 — Consumidor bronze-writer, demo de recuperación y migración

> Etapa 2b — Consumidor, demo de recuperación y migración · tarea `E1-T5` · epic `01-ingesta-streaming` · prioridad `p0` · depende de: paso 4 (`E1-T4`) · checkpoint `step-05-kafka-consumidor`

**Meta:** El grupo `bronze-writer` escribe bronze desde Kafka con commit manual después del fsync, la demo de recuperación pasa, y la captura migró a Kafka sin huecos.

**Archivos:** `src/radar/streaming/consumer.py`, `scripts/demo_recovery.sh`, `tests/unit/test_consumer.py`, `docs/kafka.md`

**Do**
- **`consumer.py`** — `BronzeWriter(consumer, sink: LocalFileSink)` con `process(msg)`: lee el
  header `last_updated`, descomprime, `sink.write_status(raw, payload)` (atómico + fsync; si ya
  existe, no escribe) y SOLO después `consumer.commit(message=msg, asynchronous=False)`. Si la
  escritura lanza, no hay commit y la excepción se registra (el mensaje se reintenta al
  reiniciar). Config: `group.id=KAFKA_GROUP_ID`, `enable.auto.commit=false`,
  `auto.offset.reset=earliest`, suscrito a `KAFKA_TOPIC_RAW`. CLI `python -m radar.streaming.consumer
  [--exit-when-idle S] [--min-messages N]`: sin flags corre para siempre; con `--exit-when-idle`
  termina tras S segundos sin mensajes e imprime `{"consumidos": n, "escritos": w, "ya_existian": e}`;
  sale 3 si `consumidos < N`.
- **`scripts/demo_recovery.sh`** (`#!/usr/bin/env bash`, encabezado, `set -euo pipefail`) —
  `PAUSE=${DEMO_PAUSE_SECONDS:-300}`; levanta `kafka collector-kafka bronze-writer` con `--wait`;
  guarda `inicio` (UTC); `docker compose --profile streaming stop bronze-writer`; `sleep "$PAUSE"`;
  `docker compose --profile streaming start bronze-writer`; espera hasta 180 s y valida con
  `uv run python - <<'PY' … PY` que cada heartbeat con `sink="kafka"`, `result="guardado"` y
  `ts_utc >= inicio` (hasta 90 s antes del fin) tenga su `station_status_<last_updated>.json.gz`;
  imprime `esperados`, `encontrados`, `faltantes` y sale 0 solo si `faltantes == 0` y `esperados >= 1`.
- **Migración sin huecos (hazla en este orden y anota las horas en `docs/kafka.md`):**
  1. `docker compose --profile streaming up -d --build --wait kafka collector-kafka bronze-writer`
     (el recolector local del paso 3 sigue corriendo: ambos escriben el mismo nombre de archivo,
     los duplicados se colapsan por idempotencia).
  2. Espera ≥ 3 min y confirma heartbeats `sink="kafka"` `guardado` con su archivo en bronze.
  3. `docker compose stop collector` — el recolector viejo queda detenido (no se borra).
- **`docs/kafka.md`** — secciones `## Diseño de mensajes`, `## Semántica de entrega`,
  `## Demo de recuperación` (salida real del script) y `## Migración sin huecos` (horas reales).

**Done when**
- [ ] WHEN `uv run pytest tests/unit/test_consumer.py` runs THE SYSTEM SHALL exit 0 with 0 failed.
- [ ] WHEN the same raw message is processed twice by `BronzeWriter.process` THE SYSTEM SHALL leave exactly one bronze file and commit the message offset both times.
- [ ] WHEN writing the bronze file raises an exception THE SYSTEM SHALL NOT commit that offset.
- [ ] WHEN `docker compose --profile streaming run --rm bronze-writer python -m radar.streaming.consumer --exit-when-idle 20 --min-messages 1` runs with the `bronze-writer` service stopped THE SYSTEM SHALL exit 0 after consuming at least 1 message.
- [ ] WHEN `DEMO_PAUSE_SECONDS=120 bash scripts/demo_recovery.sh` runs THE SYSTEM SHALL stop the consumer, keep producing, restart it, and exit 0 only if every heartbeat with `sink="kafka"` and `result="guardado"` in the window has its `station_status_<last_updated>.json.gz` in bronze.
- [ ] WHEN `docker compose --profile streaming ps --services --status running` runs after the migration THE SYSTEM SHALL list `kafka`, `collector-kafka` and `bronze-writer` and SHALL NOT list `collector`.

**Verify**
```bash
uv run pytest tests/unit/test_consumer.py tests/unit/test_encabezados.py  # expect: exit 0
uv run ruff check . && uv run ruff format --check . && uv run mypy  # expect: exit 0
docker compose --profile streaming build bronze-writer  # expect: exit 0
docker compose --profile streaming stop bronze-writer  # expect: exit 0
docker compose --profile streaming run --rm bronze-writer python -m radar.streaming.consumer --exit-when-idle 20 --min-messages 1  # expect: exit 0
DEMO_PAUSE_SECONDS=120 bash scripts/demo_recovery.sh  # expect: exit 0
docker compose --profile streaming ps --services --status running | uv run python -c "import sys; s = set(sys.stdin.read().split()); sys.exit(0 if {'kafka', 'collector-kafka', 'bronze-writer'} <= s and 'collector' not in s else 1)"  # expect: exit 0
uv run python -c "import sys; t = open('docs/kafka.md', encoding='utf-8').read(); sys.exit(0 if all(h in t for h in ['## Diseño de mensajes', '## Semántica de entrega', '## Demo de recuperación', '## Migración sin huecos']) else 1)"  # expect: exit 0
```

**Checkpoint**
```bash
git add -A && git commit -m "feat(E1-T5): consumidor bronze-writer, demo de recuperación y migración a Kafka"
git tag step-05-kafka-consumidor
# rollback si el paso 6 se rompe: git reset --hard step-05-kafka-consumidor
```

**Preguntas de checkpoint:** ¿Por qué no escribir directo a la base? · ¿Qué pasa si agrego un segundo consumidor al mismo grupo? ¿Y uno a otro grupo? · ¿Por qué el commit va después de escribir? · **Retos:** 30 (Kafka) · 📣 Post #2 (mar 10): "Apagué mi consumidor 5 minutos a propósito y no perdí ni un dato" — imagen: productor → particiones → consumidor con offsets; aprendizaje: qué es un offset y por qué el commit va después de escribir. Corre la demo con `DEMO_PAUSE_SECONDS=300` para el post.

#### Step 6 — SeaweedFS: buckets, mínimo privilegio y LakeStore

> Etapa 3a — SeaweedFS y LakeStore · tarea `E1-T6` · epic `01-ingesta-streaming` · prioridad `p0` · depende de: paso 3 (`E1-T3`) · checkpoint `step-06-lake-seaweedfs`

**Meta:** SeaweedFS corre en el perfil `lake` con los buckets `radar-bronze`, `radar-silver`, `radar-gold`, una identidad de app con mínimo privilegio probado, y bronze se sincroniza de forma idempotente.

**Archivos:** `scripts/lake_setup.py`, `src/radar/lake/store.py`, `scripts/sync_lake.py`, `tests/unit/test_store.py`

**Do**
- **`scripts/lake_setup.py`** — subcomandos (argparse):
  - `render`: si alguna de `S3_ADMIN_ACCESS_KEY`, `S3_ADMIN_SECRET_KEY`, `S3_APP_ACCESS_KEY`,
    `S3_APP_SECRET_KEY` está vacía en `.env`, la rellena con `secrets.token_hex(16)` (reescribe solo
    esas líneas); luego escribe `docker/seaweedfs/s3.json` con el formato de
    `docker/seaweedfs/s3.json.example`: identidad `admin` (`Admin`, `Read`, `Write`, `List`,
    `Tagging`) e identidad `radar-app` (`Read:`, `Write:`, `List:` sobre cada bucket). Si
    `docker/seaweedfs/s3.json` es un directorio (Docker lo crea si montaste antes de renderizar),
    sale 3 con el mensaje "borra el directorio docker/seaweedfs/s3.json y vuelve a correr render".
  - `buckets`: con la identidad admin crea los tres buckets si faltan (idempotente).
  - `check-policy`: con la identidad app hace put/get/list en cada bucket, borra su objeto de
    prueba, y espera `AccessDenied` en `create_bucket` y `delete_bucket`; imprime JSON y sale 0
    solo si todo coincide, 3 si no.
- **`src/radar/lake/store.py`** — **Ports & Adapters** + **Factory**. Puerto `LakeStore(Protocol)`:
  `put_file(local: Path, key: str) -> str` (`"subido"`/`"omitido"` si ya existe con el mismo
  tamaño), `exists(key)`, `list_keys(prefix)`. Adaptadores `LocalLakeStore(root)` y
  `S3LakeStore(bucket, client)` (boto3 con `endpoint_url=S3_ENDPOINT_URL`, credenciales de app,
  `signature_version="s3v4"`, path-style). `make_lake_store(settings, layer, backend)`.
- **`scripts/sync_lake.py`** — `--layer bronze|silver|gold [--check-idempotent] [--json]`: recorre
  `data/<layer>/` y sube con `S3LakeStore` al bucket de la capa. Nunca sube nada bajo
  `bronze/trips/` (PII) — la exclusión está en el código, no en un flag. Con
  `--check-idempotent` hace dos pasadas y sale 3 si la segunda vuelve a subir alguna llave que la
  primera ya vio. JSON: `{"subidos", "omitidos", "claves_trips"}` por pasada.
- **`tests/unit/test_store.py`** — `LocalLakeStore` en `tmp_path` + la fábrica.
- Orden: `uv run python scripts/lake_setup.py render` → `docker compose --profile lake up -d --wait seaweedfs`
  → `buckets` → `check-policy` → `sync_lake.py --layer bronze`.

**Done when**
- [ ] WHEN `uv run pytest tests/unit/test_store.py` runs THE SYSTEM SHALL exit 0 with 0 failed.
- [ ] WHEN `make_lake_store` receives backend `local` THE SYSTEM SHALL return a `LocalLakeStore` and for `s3` an `S3LakeStore`, and a second `LocalLakeStore.put_file` with the same key and size SHALL return `omitido`.
- [ ] WHEN `uv run python scripts/lake_setup.py render` runs and then `docker compose --profile lake up -d --wait seaweedfs` runs THE SYSTEM SHALL fill any blank `S3_*` key in `.env`, write `docker/seaweedfs/s3.json` with the identities `admin` and `radar-app`, and report the service healthy.
- [ ] WHEN `uv run python scripts/lake_setup.py buckets` runs twice THE SYSTEM SHALL exit 0 both times with the buckets `radar-bronze`, `radar-silver` and `radar-gold` present.
- [ ] WHEN `uv run python scripts/lake_setup.py check-policy` runs THE SYSTEM SHALL exit 0 only if the `radar-app` identity can put, get and list objects in the three buckets and receives `AccessDenied` on `CreateBucket` and on `DeleteBucket`.
- [ ] WHEN `uv run python scripts/sync_lake.py --layer bronze --check-idempotent --json` runs THE SYSTEM SHALL exit 0 only if the second pass re-uploads none of the keys the first pass saw and no uploaded key contains `trips/`.

**Verify**
```bash
uv run pytest tests/unit/test_store.py tests/unit/test_encabezados.py  # expect: exit 0
uv run ruff check . && uv run ruff format --check . && uv run mypy  # expect: exit 0
uv run python scripts/lake_setup.py render  # expect: exit 0
docker compose --profile lake up -d --wait seaweedfs  # expect: exit 0
uv run python scripts/lake_setup.py buckets  # expect: exit 0
uv run python scripts/lake_setup.py buckets  # expect: exit 0
uv run python scripts/lake_setup.py check-policy  # expect: exit 0
uv run python scripts/sync_lake.py --layer bronze --check-idempotent --json  # expect: exit 0
git check-ignore -q docker/seaweedfs/s3.json  # expect: exit 0
```

**Checkpoint**
```bash
git add -A && git commit -m "feat(E1-T6): SeaweedFS con buckets por capa, mínimo privilegio y LakeStore"
git tag step-06-lake-seaweedfs
# rollback si el paso 7 se rompe: git reset --hard step-06-lake-seaweedfs
```

**Preguntas de checkpoint:** ¿Qué permisos mínimos necesita la app y por qué no usa la identidad admin? · ¿Qué ganamos con el puerto `LakeStore` si mañana cambiamos a AWS S3? · **Retos:** 38 (Data Lake), Infraestructura en 3 capas (parcial)

#### Step 7 — Bronze a silver con cadena de calidad y cuarentena

> Etapa 3b — bronze → silver · tarea `E2-T1` · epic `02-lake-modelo-api` · prioridad `p0` · depende de: paso 6 (`E1-T6`) · checkpoint `step-07-silver`

**Meta:** Cada hora de bronze se convierte en Parquet silver validado, con cuarentena para las filas que fallan, deduplicación y un manifiesto idéntico entre corridas.

**Archivos:** `src/radar/lake/job.py`, `src/radar/lake/quality.py`, `src/radar/lake/bronze_to_silver.py`, `tests/unit/test_quality.py`, `tests/unit/test_bronze_to_silver.py`

**Do**
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

**Done when**
- [ ] WHEN `uv run pytest tests/unit/test_quality.py tests/unit/test_bronze_to_silver.py` runs THE SYSTEM SHALL exit 0 with 0 failed.
- [ ] WHEN the fixture snapshot `tests/fixtures/station_status.json` is transformed THE SYSTEM SHALL quarantine station `"3"` with `motivo="regla_capacidad"` (5+0+6+0 is not 12) and station `"999"` with `motivo="estacion_desconocida"`, and keep stations `"1"` and `"2"` in silver.
- [ ] WHEN two rows share `(station_id, last_reported)` THE SYSTEM SHALL keep exactly one in silver and count the other in `filas_duplicadas` without quarantining it.
- [ ] WHEN a row lacks a required field or carries a non-integer count THE SYSTEM SHALL quarantine it with `motivo="esquema"` before any other check runs.
- [ ] WHEN `uv run python -m radar.lake.bronze_to_silver --hour auto --check-idempotent --json` runs THE SYSTEM SHALL process the latest hour that ended at least 10 minutes ago twice and exit 0 only if both manifests are identical, `filas_validas` is above 0 and `tasa_regla_capacidad` lies between 0 and 1.
- [ ] WHEN `uv run python -m radar.lake.bronze_to_silver --backfill --json` runs THE SYSTEM SHALL process every complete hour that has bronze files and no `_manifest.json`, and exit 0.

**Verify**
```bash
uv run pytest tests/unit/test_quality.py tests/unit/test_bronze_to_silver.py tests/unit/test_encabezados.py  # expect: exit 0
uv run ruff check . && uv run ruff format --check . && uv run mypy  # expect: exit 0
uv run python -m radar.lake.bronze_to_silver --hour auto --check-idempotent --json  # expect: exit 0
uv run python -m radar.lake.bronze_to_silver --backfill --json  # expect: exit 0
uv run python scripts/sync_lake.py --layer silver --check-idempotent --json  # expect: exit 0
```

**Checkpoint**
```bash
git add -A && git commit -m "feat(E2-T1): bronze a silver con cadena de calidad, cuarentena y deduplicación"
git tag step-07-silver
# rollback si el paso 8 se rompe: git reset --hard step-07-silver
```

**Preguntas de checkpoint:** ¿Qué va en bronze que no va en silver? · ¿Por qué cuarentena en lugar de borrar? · ¿Por qué particionar por fecha y hora? · **Retos:** 38 (Data Lake), 9 (limpieza)

#### Step 8 — Gold horario, viajes históricos, SQL analítico y benchmark

> Etapa 3c — gold, viajes, SQL y benchmark · tarea `E2-T2` · epic `02-lake-modelo-api` · prioridad `p0` · depende de: paso 7 (`E2-T1`) · checkpoint `step-08-gold-viajes`

**Meta:** Existen las tablas gold (`station_hour`, `features_base`, `trip_demand`), los viajes de octubre sin PII en silver, el reporte SQL y la comparativa DuckDB vs pandas.

**Archivos:** `src/radar/lake/silver_to_gold.py`, `src/radar/lake/trips.py`, `tests/unit/test_gold_trips.py`, `sql/analitica.sql`, `docs/data-lake.md`

**Do**
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

**Done when**
- [ ] WHEN `uv run pytest tests/unit/test_gold_trips.py` runs THE SYSTEM SHALL exit 0 with 0 failed.
- [ ] WHEN the station code `"085"` is normalized THE SYSTEM SHALL return `"85"`, keep `"266-267"` unchanged, and count `"266-267"` as unmatched against `station_information.short_name`.
- [ ] WHEN `uv run python -m radar.lake.trips --download --to-silver --demand --json` runs THE SYSTEM SHALL keep the raw CSV only under `data/bronze/trips/` and write `data/silver/trips/month=*/part-0.parquet` without the columns `Genero_Usuario`, `Edad_Usuario` and `Bici`.
- [ ] WHEN `uv run python -m radar.lake.silver_to_gold --day auto --check-idempotent --json` runs THE SYSTEM SHALL build `gold/station_hour` and `gold/features_base` for that UTC day twice and exit 0 only if both manifests are identical and `filas_station_hour` is above 0.
- [ ] WHEN `uv run python -m radar.lake.trips --benchmark` runs THE SYSTEM SHALL write `docs/reportes/benchmark_duckdb_pandas.md` with wall-clock seconds and peak memory for DuckDB and for pandas.
- [ ] WHEN `uv run python -m radar.lake.silver_to_gold --sql-report sql/analitica.sql --out docs/reportes/analitica.md` runs THE SYSTEM SHALL execute every query in the file, which contains a JOIN, a HAVING clause, an `IN (SELECT` subquery and an `OVER (` window, and exit 0.

**Verify**
```bash
uv run pytest tests/unit/test_gold_trips.py tests/unit/test_encabezados.py  # expect: exit 0
uv run ruff check . && uv run ruff format --check . && uv run mypy  # expect: exit 0
uv run python -m radar.lake.trips --probe  # expect: exit 0
uv run python -m radar.lake.bronze_to_silver --backfill --json  # expect: exit 0
uv run python -m radar.lake.trips --download --to-silver --demand --json  # expect: exit 0
uv run python -c "import glob, sys, pyarrow.parquet as pq; f = glob.glob('data/silver/trips/month=*/part-0.parquet'); sys.exit(0 if f and not set(pq.read_schema(f[0]).names) & {'Genero_Usuario', 'Edad_Usuario', 'Bici'} else 1)"  # expect: exit 0
uv run python -m radar.lake.silver_to_gold --day auto --check-idempotent --json  # expect: exit 0
uv run python -m radar.lake.trips --benchmark && test -s docs/reportes/benchmark_duckdb_pandas.md  # expect: exit 0
uv run python -m radar.lake.silver_to_gold --sql-report sql/analitica.sql --out docs/reportes/analitica.md  # expect: exit 0
uv run python -c "import sys; t = open('sql/analitica.sql', encoding='utf-8').read().upper(); sys.exit(0 if all(k in t for k in ['JOIN', 'HAVING', 'IN (SELECT', 'OVER (']) else 1)"  # expect: exit 0
uv run python -c "import sys; t = open('docs/data-lake.md', encoding='utf-8').read().splitlines(); sys.exit(0 if all(h in t for h in ['## Bronze', '## Silver', '## Gold', '## Seguridad']) else 1)"  # expect: exit 0
uv run python scripts/sync_lake.py --layer gold --check-idempotent --json  # expect: exit 0
```

**Checkpoint**
```bash
git add -A && git commit -m "feat(E2-T2): gold horario, viajes sin PII, SQL analítico y benchmark"
git tag step-08-gold-viajes
# rollback si el paso 9 se rompe: git reset --hard step-08-gold-viajes
```

**Preguntas de checkpoint:** ¿Por qué Parquet y no CSV? · ¿Qué mostró el benchmark DuckDB vs pandas y por qué? · **Retos:** 38, 17 (DuckDB), 13 (SQL), 9 (limpieza), Infraestructura en 3 capas (parcial)

#### Step 9 — Pipeline horario con clima, checks de calidad y Airflow

> Etapa 4 — Orquestación con Airflow (time-box 3 h) · tarea `E2-T3` · epic `02-lake-modelo-api` · prioridad `p1` · depende de: paso 8 (`E2-T2`) · checkpoint `step-09-orquestacion`

**Meta:** Un pipeline horario idempotente (bronze → silver → clima → gold → calidad) corre como script y como DAGs `horario`/`diario` en Airflow 3 standalone.

**Archivos:** `src/radar/lake/weather.py`, `scripts/run_hourly.py`, `dags/horario.py`, `dags/diario.py`, `docker/airflow.Dockerfile`

**Do**
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

**Done when**
- [ ] WHEN `uv run python scripts/run_hourly.py --hour auto --stage todo --json` runs THE SYSTEM SHALL run bronze to silver, Open-Meteo weather for lat 19.4326 / lon -99.1332, silver to gold and the quality checks for the chosen hour, and exit 0.
- [ ] WHEN the hour printed by `run_hourly.py --print-auto-hour` (chosen with the default threshold) is checked with `--stage calidad --max-gap-minutes 0` THE SYSTEM SHALL run the `calidad` stage, exit with code 3 and print the failed check name `huecos_heartbeat` (the deliberately failing quality check).
- [ ] WHEN the weather stage runs twice THE SYSTEM SHALL leave `data/silver/weather/weather.parquet` with at least one row and unique `ts_utc` values.
- [ ] WHEN `docker compose --profile orquestacion up -d --build --wait airflow` runs THE SYSTEM SHALL report Airflow standalone (LocalExecutor, Postgres metadata) healthy on port 8080.
- [ ] WHEN `airflow dags list -o json` runs inside the `airflow` container THE SYSTEM SHALL list the DAG ids `horario` and `diario`.
- [ ] WHEN `airflow dags test horario` runs inside the container with the logical date printed by `run_hourly.py --print-airflow-logical-date` (the `auto` hour plus 1 h) THE SYSTEM SHALL exit 0.

**Verify**
```bash
uv run pytest tests/unit/test_encabezados.py  # expect: exit 0
uv run ruff check . && uv run ruff format --check . && uv run mypy  # expect: exit 0
uv run python scripts/run_hourly.py --hour auto --stage todo --json  # expect: exit 0
uv run python scripts/run_hourly.py --hour auto --stage todo --json  # expect: exit 0
H="$(uv run python scripts/run_hourly.py --print-auto-hour)"; out="$(uv run python scripts/run_hourly.py --hour "$H" --stage calidad --max-gap-minutes 0 2>&1)"; rc=$?; test $rc -eq 3 && echo "$out" | grep -q huecos_heartbeat  # expect: exit 0
uv run python -c "import sys, pyarrow.parquet as pq; t = pq.read_table('data/silver/weather/weather.parquet').column('ts_utc').to_pylist(); sys.exit(0 if t and len(t) == len(set(t)) else 1)"  # expect: exit 0
docker compose --profile orquestacion up -d --build --wait airflow  # expect: exit 0
docker compose --profile orquestacion exec -T airflow airflow dags list -o json | uv run python -c "import json, sys; ids = {d['dag_id'] for d in json.load(sys.stdin)}; sys.exit(0 if {'horario', 'diario'} <= ids else 1)"  # expect: exit 0
uv run python scripts/run_hourly.py --print-airflow-logical-date > data/_verify/hora.txt  # expect: exit 0
MSYS_NO_PATHCONV=1 docker compose --profile orquestacion exec -T airflow bash -c 'airflow dags test horario "$(cat /opt/radar/data/_verify/hora.txt)"'  # expect: exit 0
```

**Checkpoint**
```bash
git add -A && git commit -m "feat(E2-T3): pipeline horario con clima, checks de calidad y DAGs de Airflow"
git tag step-09-orquestacion
# rollback si el paso 10 se rompe: git reset --hard step-09-orquestacion
```

**Preguntas de checkpoint:** Si corro el mismo DAG del mismo día dos veces, ¿qué debe pasar? · ¿Qué es un backfill? · **Retos:** 28 (Airflow), 18 (automatización) · 📣 Post #3 (mié 11): "De datos crudos a datos confiables: bronze, silver y gold" — imagen: una fila real cruda → limpia → agregada; aprendizaje: la regla de capacidad descubierta con la estación 311.

#### Step 10 — Notebook EDA y features sin fuga de datos

> Etapa 5a — EDA y features · tarea `E2-T4` · epic `02-lake-modelo-api` · prioridad `p0` · depende de: paso 9 (`E2-T3`) · checkpoint `step-10-eda-features`

**Meta:** Existe un notebook EDA ejecutable con gráficas Plotly y conclusiones, y un módulo de features (lags, rolling, calendario, clima, demanda) probado contra fuga de datos.

**Archivos:** `notebooks/01_eda.ipynb`, `src/radar/ml/features.py`, `tests/unit/test_features.py`, `docs/modelo.md`

**Do**
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

**Done when**
- [ ] WHEN `uv run pytest tests/unit/test_features.py` runs THE SYSTEM SHALL exit 0 with 0 failed.
- [ ] WHEN input values at times later than t are altered THE SYSTEM SHALL produce identical feature values at t for `lag_15`, `lag_30`, `lag_60`, `roll_mean_30` and `roll_mean_60`.
- [ ] WHEN the training frame is built on the 5-minute grid THE SYSTEM SHALL set `y_t60` to the `bikes_available` value 12 steps later for the same station and drop rows without a target.
- [ ] WHEN calendar features are built THE SYSTEM SHALL mark 2026-12-25 with `is_holiday=1` and 2026-11-10 with `is_holiday=0`, computing `hour_local` and `dow` in `America/Mexico_City`.
- [ ] WHEN `uv run jupyter nbconvert --to html --execute notebooks/01_eda.ipynb --output-dir data/_verify` runs THE SYSTEM SHALL execute every cell on the real lake and exit 0.
- [ ] WHEN `docs/modelo.md` is read THE SYSTEM SHALL contain the sections `## Objetivo`, `## Features` and `## Revisión de fuga de datos`.

**Verify**
```bash
uv run pytest tests/unit/test_features.py tests/unit/test_encabezados.py  # expect: exit 0
uv run ruff check . && uv run ruff format --check . && uv run mypy  # expect: exit 0
uv run jupyter nbconvert --to html --execute notebooks/01_eda.ipynb --output-dir data/_verify  # expect: exit 0
uv run python -c "import sys; t = open('docs/modelo.md', encoding='utf-8').read().splitlines(); sys.exit(0 if all(h in t for h in ['## Objetivo', '## Features', '## Revisión de fuga de datos']) else 1)"  # expect: exit 0
```

**Checkpoint**
```bash
git add -A && git commit -m "feat(E2-T4): notebook EDA y features sin fuga de datos"
git tag step-10-eda-features
# rollback si el paso 11 se rompe: git reset --hard step-10-eda-features
```

**Preguntas de checkpoint:** ¿Dónde podría haber fuga de datos en estas features? · ¿Por qué la demanda de viajes de octubre no es fuga para predecir noviembre? · **Retos:** 45 (series de tiempo), 12 (notebook), 19 (Plotly)

#### Step 11 — Entrenar baseline, lineal y LightGBM con MLflow

> Etapa 5b — Entrenamiento y MLflow · tarea `E2-T5` · epic `02-lake-modelo-api` · prioridad `p0` · depende de: paso 10 (`E2-T4`) · checkpoint `step-11-modelo-mlflow`

**Meta:** Baseline, regresión lineal y ≥3 configuraciones de LightGBM se comparan con TimeSeriesSplit, todo queda en MLflow y el ganador está registrado como `radar-bikes-t60@champion` con un respaldo local.

**Archivos:** `src/radar/ml/models.py`, `src/radar/ml/train.py`, `src/radar/ml/evaluate.py`, `tests/unit/test_models.py`, `docs/modelo.md`

**Do**
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

**Done when**
- [ ] WHEN `uv run pytest tests/unit/test_models.py` runs THE SYSTEM SHALL exit 0 with 0 failed, including a check that every fold's validation timestamps are later than all of its training timestamps.
- [ ] WHEN `docker compose --profile ml up -d --wait mlflow` runs THE SYSTEM SHALL report MLflow healthy at http://localhost:5000.
- [ ] WHEN `uv run python -m radar.ml.train --json` runs THE SYSTEM SHALL exit 0 and print MAE, RMSE and R² for `baseline`, `lineal` and at least 3 `lightgbm_*` configurations.
- [ ] WHEN training finishes THE SYSTEM SHALL register a version of `radar-bikes-t60` with alias `champion` in MLflow and write the fallback file `models/champion.joblib`.
- [ ] WHEN training finishes THE SYSTEM SHALL write `docs/reportes/comparacion_modelos.md` and `docs/reportes/real_vs_pred.html`, and `docs/modelo.md` SHALL contain `## Comparación de modelos`.
- [ ] WHEN `uv run jupyter nbconvert --to html --execute notebooks/01_eda.ipynb --output-dir docs/reportes` runs THE SYSTEM SHALL exit 0 and leave `docs/reportes/01_eda.html`.

**Verify**
```bash
uv run pytest tests/unit/test_models.py tests/unit/test_encabezados.py  # expect: exit 0
uv run ruff check . && uv run ruff format --check . && uv run mypy  # expect: exit 0
docker compose --profile ml up -d --wait mlflow  # expect: exit 0
uv run python -m radar.ml.train --json > data/_verify/train.json  # expect: exit 0
uv run python -c "import json, sys; m = json.load(open('data/_verify/train.json', encoding='utf-8'))['modelos']; ok = {'baseline', 'lineal'} <= set(m) and sum(k.startswith('lightgbm_') for k in m) >= 3 and all({'mae', 'rmse', 'r2'} <= set(v) for v in m.values()); sys.exit(0 if ok else 1)"  # expect: exit 0
uv run python -c "import sys, mlflow; mlflow.set_tracking_uri('http://localhost:5000'); v = mlflow.MlflowClient().get_model_version_by_alias('radar-bikes-t60', 'champion'); sys.exit(0 if v.version else 1)"  # expect: exit 0
test -s models/champion.joblib && test -s docs/reportes/comparacion_modelos.md && test -s docs/reportes/real_vs_pred.html  # expect: exit 0
grep -qx '## Comparación de modelos' docs/modelo.md  # expect: exit 0
uv run jupyter nbconvert --to html --execute notebooks/01_eda.ipynb --output-dir docs/reportes && test -s docs/reportes/01_eda.html  # expect: exit 0
```

**Checkpoint**
```bash
git add -A && git commit -m "feat(E2-T5): baseline, lineal y LightGBM con TimeSeriesSplit y MLflow"
git tag step-11-modelo-mlflow
# rollback si el paso 12 se rompe: git reset --hard step-11-modelo-mlflow
```

**Preguntas de checkpoint:** ¿Cuánto superé al baseline (en %)? · ¿Dónde podría haber fuga de datos? · ¿Qué indica la diferencia entre MAE de entrenamiento y de validación? · **Retos:** 45, 22 (regresión, 3 modelos), 33 (MLflow), 12 (notebook), 37 (MLOps, parcial) · 📣 Post #4 (jue 12): "Mi modelo predice cuántas bicis habrá en una estación dentro de 1 hora" — imagen: `docs/reportes/real_vs_pred.html` (captura); aprendizaje: por qué no se valida al azar y cuánto superó al baseline (con el número).

#### Step 12 — API FastAPI con repositorio, DI y modelo cargado una vez

> Etapa 6a — API FastAPI · tarea `E2-T6` · epic `02-lake-modelo-api` · prioridad `p0` · depende de: paso 11 (`E2-T5`) · checkpoint `step-12-api`

**Meta:** La API sirve `/health`, `/stations`, `/stations/{station_id}/history`, `/predict/{station_id}`, `/predictions` y `/anomalies` (stub) con esquemas Pydantic, el modelo cargado una vez en el lifespan y CORS para el mapa.

**Archivos:** `src/radar/api/schemas.py`, `src/radar/api/repository.py`, `src/radar/api/deps.py`, `src/radar/api/main.py`, `tests/integration/test_api.py`

**Do**
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

**Done when**
- [ ] WHEN `uv run pytest tests/integration/test_api.py` runs THE SYSTEM SHALL exit 0 with 0 failed.
- [ ] WHEN `GET /health` is requested THE SYSTEM SHALL return 200 with the fields `status`, `modelo_cargado`, `fuente_modelo` and `segundos_desde_ultimo_dato`.
- [ ] WHEN `GET /predict/{station_id}` is requested for an unknown id THE SYSTEM SHALL return 404 with `detail.codigo` equal to `estacion_no_encontrada`.
- [ ] WHEN `GET /stations/{station_id}/history?hours=0` is requested THE SYSTEM SHALL return 422.
- [ ] WHEN several prediction requests are served THE SYSTEM SHALL have loaded the model exactly once, during the lifespan.
- [ ] WHEN a CORS preflight arrives from `http://localhost:5173` THE SYSTEM SHALL answer with `access-control-allow-origin: http://localhost:5173`, and `GET /docs` SHALL return 200 on the real app with real lake data and a non-empty `/stations` list.

**Verify**
```bash
uv run pytest tests/integration/test_api.py tests/unit/test_encabezados.py  # expect: exit 0
uv run ruff check . && uv run ruff format --check . && uv run mypy  # expect: exit 0
uv run python -c "import sys; from fastapi.testclient import TestClient; from radar.api.main import app; c = TestClient(app); c.__enter__(); h = c.get('/health'); s = c.get('/stations'); d = c.get('/docs'); c.__exit__(None, None, None); sys.exit(0 if h.status_code == 200 and s.status_code == 200 and len(s.json()) > 0 and d.status_code == 200 else 1)"  # expect: exit 0
```

**Checkpoint**
```bash
git add -A && git commit -m "feat(E2-T6): API FastAPI con repositorio, DI y modelo cargado en el lifespan"
git tag step-12-api
# rollback si el paso 13 se rompe: git reset --hard step-12-api
```

**Preguntas de checkpoint:** ¿Cómo evito recargar el modelo en cada petición? · ¿Qué devuelve la API si pido una estación que no existe y por qué ese código? · **Retos:** 47 (API de inferencia), Framework de backend, Webservice, Pruebas unitarias*

#### Step 13 — CI en GitHub Actions, imagen de la API y runbook

> Etapa 6b — CI, contenedor de la API y runbook · tarea `E3-T1` · epic `03-producto-entrega` · prioridad `p0` · depende de: paso 12 (`E2-T6`) · checkpoint `step-13-ci`

**Meta:** Cada push/PR corre lint, tipos, pruebas y build de imágenes en GitHub Actions; la API corre en su contenedor; README tiene el badge; existen el manual de backend y el runbook.

**Archivos:** `.github/workflows/ci.yml`, `docker/api.Dockerfile`, `docs/backend.md`, `docs/runbook.md`, `README.md`

**Do**
- **`.github/workflows/ci.yml`** (encabezado `#`) — `on: [push, pull_request]`. Job `python`
  (`ubuntu-latest`): `actions/checkout@v7`; `actions/setup-python@v7` con `python-version: "3.12"`;
  `astral-sh/setup-uv@v10` con `version: "0.13.7"`; `uv sync --frozen --all-extras --all-groups`;
  `uv run ruff check .`; `uv run ruff format --check .`; `uv run mypy`; `uv run pytest --cov=radar`.
  Job `docker`: `docker/setup-buildx-action@v4`; dos pasos `docker/build-push-action@v7` con
  `context: .`, `file: docker/collector.Dockerfile` / `docker/api.Dockerfile` y `push: false`.
  Sin secretos. Las pruebas no usan red ni `data/`.
- **`docker/api.Dockerfile`** — exactamente:
  ```dockerfile
  # Autor: Isacc Cabrera
  # Descripción: Imagen de la API FastAPI con el modelo (LightGBM necesita libgomp1).
  # Patrón: N/A
  # Fecha de creación: 2026-11-11
  # Cambios:
  #     - 2026-11-11 · Isacc Cabrera · Creación (paso 13).
  FROM python:3.12-slim
  ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never \
      UV_PROJECT_ENVIRONMENT=/app/.venv
  RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 \
      && rm -rf /var/lib/apt/lists/* \
      && pip install --no-cache-dir uv==0.13.7 && useradd --create-home --uid 10001 radar
  WORKDIR /app
  COPY pyproject.toml uv.lock .python-version ./
  RUN uv sync --frozen --no-dev --no-install-project --extra api --extra lake --extra ml
  COPY src ./src
  RUN uv sync --frozen --no-dev --no-editable --extra api --extra lake --extra ml \
      && chown -R radar:radar /app
  ENV PATH="/app/.venv/bin:$PATH"
  USER radar
  EXPOSE 8000
  CMD ["uvicorn", "radar.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
  ```
- **`docs/backend.md`** — manual de implementación de FastAPI: arquitectura (schemas → repository
  → deps → main), cómo se carga el modelo, cómo correrla, Swagger en `/docs`, y ejemplos `curl`
  de `/health`, `/stations`, `/predict/{station_id}` y `/anomalies` con respuestas reales.
- **`docs/runbook.md`** — `## El recolector se detuvo`, `## Kafka no arranca`, `## Huecos en el
  heartbeat`, `## SeaweedFS no responde`, `## La API responde sin modelo`, cada una con comandos
  concretos (`docker compose ps`, `docker compose logs --tail 50 <servicio>`, …).
- **`README.md` (editar)** — badge
  `![CI](https://github.com/Isacc-Cabrera/radar-ecobici-cdmx/actions/workflows/ci.yml/badge.svg)`
  debajo del título y enlace a `docs/backend.md`.

**Done when**
- [ ] WHEN `.github/workflows/ci.yml` is read THE SYSTEM SHALL trigger on `push` and `pull_request` and contain `astral-sh/setup-uv@v10`, `0.13.7`, `uv sync --frozen --all-extras --all-groups`, `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy`, `uv run pytest`, `docker/collector.Dockerfile`, `docker/api.Dockerfile` and `push: false`.
- [ ] WHEN the CI commands run locally THE SYSTEM SHALL exit 0 for the sync, lint, format, type, test and both image builds.
- [ ] WHEN `docker compose --profile api up -d --build --wait api` runs THE SYSTEM SHALL report `api` healthy and `curl -sf localhost:8000/health` SHALL exit 0.
- [ ] WHEN `curl -s -o /dev/null -w '%{http_code}' localhost:8000/docs` runs THE SYSTEM SHALL print `200`.
- [ ] WHEN `README.md` is read THE SYSTEM SHALL contain the badge URL `https://github.com/Isacc-Cabrera/radar-ecobici-cdmx/actions/workflows/ci.yml/badge.svg`.
- [ ] WHEN `docs/backend.md` and `docs/runbook.md` are read THE SYSTEM SHALL find curl examples for `/health`, `/stations`, `/predict/` and `/anomalies` in the former and `## El recolector se detuvo` and `## Kafka no arranca` in the latter.

**Verify**
```bash
uv run python -c "import sys; t = open('.github/workflows/ci.yml', encoding='utf-8').read(); k = ['push', 'pull_request', 'astral-sh/setup-uv@v10', '0.13.7', 'uv sync --frozen --all-extras --all-groups', 'uv run ruff check .', 'uv run ruff format --check .', 'uv run mypy', 'uv run pytest', 'docker/collector.Dockerfile', 'docker/api.Dockerfile', 'push: false']; sys.exit(0 if all(x in t for x in k) else 1)"  # expect: exit 0
uv sync --frozen --all-extras --all-groups  # expect: exit 0
uv run ruff check . && uv run ruff format --check . && uv run mypy  # expect: exit 0
uv run pytest  # expect: exit 0
docker build -f docker/collector.Dockerfile -t radar-ecobici/collector:local .  # expect: exit 0
docker build -f docker/api.Dockerfile -t radar-ecobici/api:local .  # expect: exit 0
docker compose --profile api up -d --build --wait api  # expect: exit 0
curl -sf localhost:8000/health  # expect: exit 0
test "$(curl -s -o /dev/null -w '%{http_code}' localhost:8000/docs)" = 200  # expect: exit 0
grep -qF 'https://github.com/Isacc-Cabrera/radar-ecobici-cdmx/actions/workflows/ci.yml/badge.svg' README.md  # expect: exit 0
uv run python -c "import sys; b = open('docs/backend.md', encoding='utf-8').read(); r = open('docs/runbook.md', encoding='utf-8').read().splitlines(); ok = all(('curl' in b and p in b) for p in ['/health', '/stations', '/predict/', '/anomalies']) and '## El recolector se detuvo' in r and '## Kafka no arranca' in r; sys.exit(0 if ok else 1)"  # expect: exit 0
```

**Checkpoint**
```bash
git add -A && git commit -m "ci(E3-T1): GitHub Actions, imagen de la API, manual de backend y runbook"
git tag step-13-ci
# rollback si el paso 14 se rompe: git reset --hard step-13-ci
```

**Preguntas de checkpoint:** ¿Qué corre en CI y por qué? · ¿Por qué las pruebas de CI no necesitan datos reales ni red? · **Retos:** CI/CD, Pruebas unitarias*

#### Step 14 — Mapa 3D con deck.gl, tema claro/oscuro y panel lateral

> Etapa 7 — Mapa 3D · tarea `E3-T2` · epic `03-producto-entrega` · prioridad `p0` · depende de: paso 12 (`E2-T6`) · checkpoint `step-14-mapa-3d`

**Meta:** El mapa 3D muestra una columna por estación (altura = bicis, color = predicción), con tooltip, leyenda, panel con historia, cámara inclinada animada, polling de 60 s y tema verde claro/oscuro persistido.

**Archivos:** `frontend/src/App.tsx`, `frontend/src/components/StationMap.tsx`, `frontend/src/components/Panel.tsx`, `frontend/src/hooks.ts`, `docs/frontend.md`

**Do**
`frontend/package.json`, `tsconfig.json`, `vite.config.ts`, `index.html`, `src/main.tsx`,
`src/styles/theme.css`, `src/lib/scales.ts`, `src/lib/scales.test.ts` y `scripts/smoke.mjs` ya
están en el repo (los copió el Bootstrap). Primero: `npm --prefix frontend install` (genera
`frontend/package-lock.json`, que se versiona).

- **`src/hooks.ts`** — **Custom Hooks**: `useStations(apiUrl)` hace polling cada `60000` ms a
  `/stations` y `/predictions` (une por `station_id`), expone `{stations, loading, error,
  updatedAt}`; `useStationHistory(apiUrl, stationId)` lee `/stations/{id}/history?hours=6`;
  `useTheme()` arranca con `resolveTheme(localStorage.getItem(THEME_STORAGE_KEY),
  matchMedia("(prefers-color-scheme: dark)").matches)`, escribe `data-theme` en `<html>` y
  persiste en `localStorage`; `useReducedMotion()` lee `(prefers-reduced-motion: reduce)`. Tipos
  `Station`, `HistoryPoint` iguales a §5. `API_URL = import.meta.env.VITE_API_URL ?? "http://localhost:8000"`.
- **`src/App.tsx`** — **Container**: `export default function App()`; usa los hooks, guarda la
  estación seleccionada y renderiza enlace "Saltar al panel", `<StationMap>` y `<Panel>`.
- **`src/components/StationMap.tsx`** — **Presentational**: `<DeckGL>` con `<Map>` de
  `react-map-gl/maplibre` como hijo (`mapStyle={BASEMAPS[theme]}`), `ColumnLayer` (`diskResolution`
  12, `radius` 40, `extruded`, `elevationScale` 40, `getElevation = bicis`, `getFillColor` =
  `PREDICTION_COLORS[clase]` si hay predicción, si no `bikeColor`), `pickable` + `getTooltip`
  (nombre, bicis, docks, predicción, clase). Vista inicial CDMX `longitude -99.1677, latitude
  19.4205, zoom 12.5, pitch 50, bearing -20`; al seleccionar una estación vuela a ella con
  `FlyToInterpolator` y `transitionDuration` 1200 ms (0 si `useReducedMotion()`).
- **`src/components/Panel.tsx`** — `<aside>` con: título, botón de tema (`aria-pressed`, texto
  "Tema oscuro"/"Tema claro", 24×24 px mínimo), hora de actualización con `aria-live="polite"`,
  leyenda (rampa de bicis + tres clases con `PREDICTION_LABELS` ícono + texto), lista de
  estaciones como `<button>` navegables con teclado (filtrable por nombre), detalle de la
  seleccionada con gráfica `recharts` (`LineChart` de bicis últimas 6 h), estados de carga, vacío
  y error en texto, y la atribución `BASEMAP_ATTRIBUTION`. Responsivo: panel lateral de
  `var(--panel-width)` en ≥ 768 px, hoja inferior en < 768 px.
- **`docs/frontend.md`** — manual del framework: `## Componentes y estado`, `## Cómo dibuja
  deck.gl en WebGL`, `## Polling vs WebSocket`, `## Tema y accesibilidad`, `## Capturas y GIF`
  (cómo grabar 15–20 s con la cámara girando: ScreenToGif o la Xbox Game Bar en Windows, y dónde
  guardar `docs/img/mapa.gif`).
- WebSockets: fuera de alcance (§1). Ningún componente escribe un hex literal.

**Done when**
- [ ] WHEN `npm --prefix frontend install` runs THE SYSTEM SHALL exit 0 and write `frontend/package-lock.json`.
- [ ] WHEN `npm --prefix frontend run typecheck` runs THE SYSTEM SHALL exit 0 under strict TypeScript.
- [ ] WHEN `npm --prefix frontend run test` runs THE SYSTEM SHALL exit 0, asserting contrast of at least 4.5:1 for every text/background token pair in both themes and the -3/+3 prediction thresholds.
- [ ] WHEN `npm --prefix frontend run build` and then `npm --prefix frontend run smoke` run THE SYSTEM SHALL exit 0 with the preview server answering `/` with 200 and the `#root` container.
- [ ] WHEN the four new source files are searched THE SYSTEM SHALL find `ColumnLayer` and `FlyToInterpolator` in `StationMap.tsx`, `THEME_STORAGE_KEY`, `prefers-color-scheme`, `prefers-reduced-motion` and `60000` in `hooks.ts`, and no `#RRGGBB` color literal in any of them.
- [ ] WHEN `docs/frontend.md` is read THE SYSTEM SHALL contain `## Componentes y estado`, `## Cómo dibuja deck.gl en WebGL`, `## Polling vs WebSocket` and `## Capturas y GIF`.

**Verify**
```bash
npm --prefix frontend install  # expect: exit 0
npm --prefix frontend run typecheck  # expect: exit 0
npm --prefix frontend run test  # expect: exit 0
npm --prefix frontend run build  # expect: exit 0
npm --prefix frontend run smoke  # expect: exit 0
uv run python -c "import re, sys; r = lambda p: open('frontend/src/' + p, encoding='utf-8').read(); m, h = r('components/StationMap.tsx'), r('hooks.ts'); files = [r('App.tsx'), m, r('components/Panel.tsx'), h]; ok = 'ColumnLayer' in m and 'FlyToInterpolator' in m and all(k in h for k in ['THEME_STORAGE_KEY', 'prefers-color-scheme', 'prefers-reduced-motion', '60000']) and not any(re.search(r'#[0-9A-Fa-f]{6}\b', f) for f in files); sys.exit(0 if ok else 1)"  # expect: exit 0
uv run python -c "import sys; t = open('docs/frontend.md', encoding='utf-8').read().splitlines(); sys.exit(0 if all(h in t for h in ['## Componentes y estado', '## Cómo dibuja deck.gl en WebGL', '## Polling vs WebSocket', '## Capturas y GIF']) else 1)"  # expect: exit 0
uv run pytest tests/unit/test_encabezados.py  # expect: exit 0
```

**Checkpoint**
```bash
git add -A && git commit -m "feat(E3-T2): mapa 3D con deck.gl, tema claro/oscuro y panel lateral"
git tag step-14-mapa-3d
git ls-files --error-unmatch frontend/package-lock.json  # expect: exit 0: lockfile versionado
# rollback si el paso 15 se rompe: git reset --hard step-14-mapa-3d
```

**Preguntas de checkpoint:** ¿Por qué columnas 3D y no solo colores? · ¿Qué ganaría con WebSockets? · **Retos:** Framework de frontend, Mapas/geolocalización (parcial) · 📣 Post #5 (sáb 14): video/GIF de 15–20 s del mapa 3D girando sobre la CDMX en hora pico; aprendizaje: cómo un dato de una API pública termina en una columna 3D, en 4 pasos.

#### Step 15 — Detección de anomalías conectada a la API y al mapa

> Etapa 8a — Anomalías · tarea `E3-T3` · epic `03-producto-entrega` · prioridad `p1` · depende de: paso 14 (`E3-T2`) · checkpoint `step-15-anomalias`

**Meta:** Tres detectores (congelada, z-score, Isolation Forest) marcan estaciones anómalas, `/anomalies` los sirve, `/stations` los refleja y el mapa los dibuja; FP/FN documentados con ejemplos reales.

**Archivos:** `src/radar/anomalies/detectors.py`, `tests/unit/test_detectors.py`, `src/radar/api/repository.py`, `frontend/src/components/StationMap.tsx`, `docs/anomalias.md`

**Do**
- **`detectors.py`** — **Strategy**: `class AnomalyDetector(Protocol)` con `name` y
  `detect(recent: pd.DataFrame, history: pd.DataFrame, now_utc) -> list[Anomaly]`.
  `FrozenStationDetector` (`last_reported` sin cambio ≥ `ANOMALY_FROZEN_MINUTES`, `tipo="congelada"`),
  `ZScoreDetector` (bicis actuales vs media/desviación de `gold/station_hour` para esa estación y
  hora local; `|z| ≥ ANOMALY_ZSCORE` ⇒ `tipo="zscore"`; omite estaciones con < 3 observaciones
  históricas en esa hora o desviación 0), `IsolationForestDetector` (sklearn, `contamination=0.02`,
  `random_state=42`, features: % de bicis, hora local, cambio en 60 min; `tipo="isolation_forest"`).
  CLI `python -m radar.anomalies.detectors --json`: corre los tres sobre el lake y imprime
  conteos por detector y la intersección entre ellos.
- **`repository.py` (editar)** — `anomalies()` corre los detectores (caché 60 s) y `stations()`
  marca `anomalia=True` en las estaciones presentes en esa lista. Línea nueva en `Cambios`.
- **`StationMap.tsx` (editar)** — `ScatterplotLayer` (de `@deck.gl/layers`) sobre las estaciones con
  `anomalia`, color `ANOMALY_COLOR`, y "⚠ Anomalía" en el tooltip (texto, no solo color).
- **`docs/anomalias.md`** — método de cada detector, `## Falsos positivos` y `## Falsos negativos`
  con al menos dos ejemplos reales cada uno (estación, hora, por qué), y la comparación z-score vs
  Isolation Forest con la intersección del CLI.

**Done when**
- [ ] WHEN `uv run pytest tests/unit/test_detectors.py` runs THE SYSTEM SHALL exit 0 with 0 failed.
- [ ] WHEN a station's `last_reported` stays unchanged for at least `ANOMALY_FROZEN_MINUTES` THE SYSTEM SHALL flag it with `tipo="congelada"`, and SHALL NOT flag a station whose `last_reported` advanced within that window.
- [ ] WHEN a station's bikes deviate from its station-hour mean by an absolute z-score of at least `ANOMALY_ZSCORE` THE SYSTEM SHALL flag it with `tipo="zscore"`, and SHALL skip stations with fewer than 3 historical observations for that hour.
- [ ] WHEN `uv run python -m radar.anomalies.detectors --json` runs on the real lake THE SYSTEM SHALL exit 0 and print counts for `congelada`, `zscore` and `isolation_forest` plus their intersection.
- [ ] WHEN `GET /anomalies` is requested from the real app THE SYSTEM SHALL return 200 with items carrying `station_id`, `tipo`, `puntaje` and `detalle`, and `GET /stations` SHALL set `anomalia=true` for exactly those station ids.
- [ ] WHEN `npm --prefix frontend run typecheck` and `npm --prefix frontend run build` run THE SYSTEM SHALL exit 0 with `StationMap.tsx` using `ANOMALY_COLOR`, and `docs/anomalias.md` SHALL contain `## Falsos positivos` and `## Falsos negativos`.

**Verify**
```bash
uv run pytest tests/unit/test_detectors.py tests/unit/test_encabezados.py  # expect: exit 0
uv run ruff check . && uv run ruff format --check . && uv run mypy  # expect: exit 0
uv run python -m radar.anomalies.detectors --json  # expect: exit 0
uv run python -c "import sys; from fastapi.testclient import TestClient; from radar.api.main import app; c = TestClient(app); c.__enter__(); a = c.get('/anomalies'); s = c.get('/stations'); c.__exit__(None, None, None); ok = a.status_code == 200 and s.status_code == 200 and all({'station_id', 'tipo', 'puntaje', 'detalle'} <= set(x) for x in a.json()) and {x['station_id'] for x in a.json()} == {x['station_id'] for x in s.json() if x['anomalia']}; sys.exit(0 if ok else 1)"  # expect: exit 0
npm --prefix frontend run typecheck  # expect: exit 0
npm --prefix frontend run build  # expect: exit 0
grep -q 'ANOMALY_COLOR' frontend/src/components/StationMap.tsx  # expect: exit 0
grep -qx '## Falsos positivos' docs/anomalias.md && grep -qx '## Falsos negativos' docs/anomalias.md  # expect: exit 0
```

**Checkpoint**
```bash
git add -A && git commit -m "feat(E3-T3): detección de anomalías en API y mapa"
git tag step-15-anomalias
# rollback si el paso 16 se rompe: git reset --hard step-15-anomalias
```

**Preguntas de checkpoint:** ¿Qué es un falso positivo en este contexto y cómo lo reduciría? · ¿Por qué comparar z-score con Isolation Forest? · **Retos:** 44 (anomalías)

#### Step 16 — Terraform para S3, IAM mínimo y presupuesto de 5 USD

> Etapa 8b — Terraform/AWS (⚠️ costos) · tarea `E3-T4` · epic `03-producto-entrega` · prioridad `p2` · depende de: paso 8 (`E2-T2`) · checkpoint `step-16-terraform-aws`

**Meta:** Terraform describe (sin aplicar) un bucket S3 seguro, un usuario IAM limitado a ese bucket y una alerta de presupuesto de 5 USD; el sync de gold y el destroy están documentados con costos.

**Archivos:** `infra/terraform/main.tf`, `infra/terraform/variables.tf`, `scripts/sync_gold_to_s3.sh`, `docs/nube.md`

**Do**
**⚠️ AVISO DE COSTO.** Este paso NO ejecuta `terraform apply`. Antes de cualquier `apply` (fuera
del build, en el checklist de §20.1) presenta al usuario la lista de recursos y su costo estimado
y espera su "sí". Prerrequisitos solo para aplicar: cuenta AWS, AWS CLI v2 configurado con el
perfil `radar-ecobici` (versión no verificada: verificar antes de instalar).

- **`infra/terraform/main.tf`** (encabezado `#`) — `terraform { required_version = "~> 1.17"
  required_providers { aws = { source = "hashicorp/aws", version = "~> 6.72" } } }`, `provider
  "aws" { region = var.region }`. Recursos: `aws_s3_bucket.gold` (`bucket = var.bucket_name`,
  `force_destroy = true` para la demo), `aws_s3_bucket_versioning` (`status = "Enabled"`),
  `aws_s3_bucket_public_access_block` (los cuatro `block_*`/`ignore_*`/`restrict_*` en `true`),
  `aws_s3_bucket_server_side_encryption_configuration` (`sse_algorithm = "AES256"`),
  `aws_iam_user.sync` (`radar-gold-sync`), `aws_iam_user_policy.sync` (`s3:ListBucket` sobre el ARN
  del bucket con condición `s3:prefix = gold/*`; `s3:GetObject` y `s3:PutObject` sobre
  `${bucket_arn}/gold/*`; nada más), `aws_budgets_budget.mensual` (`budget_type = "COST"`,
  `limit_amount = "5"`, `limit_unit = "USD"`, `time_unit = "MONTHLY"`, notificaciones ACTUAL 80 % y
  FORECASTED 100 % a `var.alert_email`). Outputs: nombre y ARN del bucket. **Sin
  `aws_iam_access_key`** (la llave quedaría en el state): la llave se crea a mano tras aplicar.
- **`infra/terraform/variables.tf`** — `region` (default `"us-east-1"`), `bucket_name` (sin
  default), `alert_email` (sin default, `sensitive = true`). Valores reales en
  `infra/terraform/terraform.tfvars` (ignorado).
- **`scripts/sync_gold_to_s3.sh`** (`#!/usr/bin/env bash`, encabezado, `set -euo pipefail`) —
  carga `.env` (`set -a; . ./.env; set +a`), exige `AWS_S3_BUCKET` no vacío y corre
  `aws s3 sync data/gold "s3://$AWS_S3_BUCKET/gold" --profile "$AWS_PROFILE" --exclude "*" --include "*.parquet"`.
  Nunca sincroniza otra carpeta.
- **`docs/nube.md`** — `## Recursos`, `## Costos estimados` (S3 Standard y requests para < 1 GB:
  centavos al mes; Budgets: las dos primeras alertas sin costo — verificar en la página de
  precios de AWS antes de aplicar), `## Cómo aplicar (requiere aprobación)` (`init`, `plan`,
  `apply`, crear la llave con `aws iam create-access-key --user-name radar-gold-sync`, sync),
  `## terraform destroy` (vaciar y destruir al terminar la demo) y `## Mínimo privilegio`.
- Corre `terraform -chdir=infra/terraform init -backend=false -input=false` (descarga el provider
  y crea `.terraform.lock.hcl`, que se versiona).

**Done when**
- [ ] WHEN `terraform -chdir=infra/terraform fmt -check -recursive` runs THE SYSTEM SHALL exit 0.
- [ ] WHEN `terraform -chdir=infra/terraform init -backend=false -input=false` and then `terraform -chdir=infra/terraform validate` run THE SYSTEM SHALL exit 0 with provider `hashicorp/aws` constrained to `~> 6.72`.
- [ ] WHEN `infra/terraform/main.tf` is read THE SYSTEM SHALL declare `aws_s3_bucket_versioning`, `aws_s3_bucket_public_access_block`, `aws_s3_bucket_server_side_encryption_configuration`, `aws_iam_user_policy` and `aws_budgets_budget` with `limit_amount = "5"`, and SHALL NOT declare `aws_iam_access_key`.
- [ ] WHEN `git check-ignore -q infra/terraform/terraform.tfstate` runs THE SYSTEM SHALL exit 0, and `git check-ignore -q infra/terraform/.terraform.lock.hcl` SHALL exit 1.
- [ ] WHEN `bash -n scripts/sync_gold_to_s3.sh` runs THE SYSTEM SHALL exit 0, and the script SHALL reference only the local folder `data/gold`.
- [ ] WHEN `docs/nube.md` is read THE SYSTEM SHALL contain `## Costos estimados`, `## Cómo aplicar (requiere aprobación)` and `## terraform destroy`.

**Verify**
```bash
terraform -chdir=infra/terraform fmt -check -recursive  # expect: exit 0
terraform -chdir=infra/terraform init -backend=false -input=false  # expect: exit 0
terraform -chdir=infra/terraform validate  # expect: exit 0
uv run python -c "import sys; t = open('infra/terraform/main.tf', encoding='utf-8').read(); ok = all(k in t for k in ['aws_s3_bucket_versioning', 'aws_s3_bucket_public_access_block', 'aws_s3_bucket_server_side_encryption_configuration', 'aws_iam_user_policy', 'aws_budgets_budget', 'limit_amount = \"5\"', '~> 6.72']) and 'aws_iam_access_key' not in t; sys.exit(0 if ok else 1)"  # expect: exit 0
git check-ignore -q infra/terraform/terraform.tfstate  # expect: exit 0
git check-ignore -q infra/terraform/.terraform.lock.hcl; test $? -eq 1  # expect: código 1 → la línea sale 0
bash -n scripts/sync_gold_to_s3.sh  # expect: exit 0
uv run python -c "import re, sys; t = open('scripts/sync_gold_to_s3.sh', encoding='utf-8').read(); sys.exit(0 if 'data/gold' in t and not re.search(r'data/(bronze|silver)', t) else 1)"  # expect: exit 0
uv run python -c "import sys; t = open('docs/nube.md', encoding='utf-8').read().splitlines(); sys.exit(0 if all(h in t for h in ['## Costos estimados', '## Cómo aplicar (requiere aprobación)', '## terraform destroy']) else 1)"  # expect: exit 0
uv run pytest tests/unit/test_encabezados.py  # expect: exit 0
```

**Checkpoint**
```bash
git add -A && git commit -m "feat(E3-T4): Terraform para S3, IAM mínimo y alerta de presupuesto (sin aplicar)"
git tag step-16-terraform-aws
git ls-files --error-unmatch infra/terraform/.terraform.lock.hcl  # expect: exit 0: lock del provider versionado
# rollback si el paso 17 se rompe: git reset --hard step-16-terraform-aws
```

**Preguntas de checkpoint:** ¿Por qué el state no se sube a Git? · ¿Qué permisos mínimos necesita mi app? · **Retos:** Terraform, 6 (S3), 37 (MLOps, completo)

#### Step 17 — Asistente RAG local con Ollama y fuentes citadas

> Etapa 9 — Asistente RAG (opcional; primero en cortarse) · tarea `E3-T5` · epic `03-producto-entrega` · prioridad `p2` · depende de: paso 11 (`E2-T5`) · checkpoint `step-17-rag`

**Meta:** Un asistente local (Ollama + chromadb) responde preguntas sobre `docs/` y `docs/reportes/` citando fuentes, y responde "No sé" cuando no hay evidencia.

**Archivos:** `src/radar/rag/index.py`, `src/radar/rag/ask.py`, `tests/unit/test_rag.py`, `docs/rag.md`

**Do**
Prerrequisito: Ollama instalado en Windows (§10). Costo: 0 (todo local). RAM: ~4 GB libres para
`llama3.2:3b` (2.0 GB en disco) + `nomic-embed-text` (274 MB, contexto 2K).
`ollama pull llama3.2:3b` y `ollama pull nomic-embed-text`.

- **`index.py`** — **Ports & Adapters**: puerto `Embedder(Protocol)` con `embed(texts) ->
  list[list[float]]`; adaptador `OllamaEmbedder` (`POST` a `OLLAMA_URL` + `/api/embed` con
  `{"model": OLLAMA_EMBED_MODEL, "input": [...]}`). Trocea `docs/*.md` y `docs/reportes/*.md` en
  fragmentos de ~800 caracteres con 100 de traslape (respetando encabezados), id
  `<ruta>#<n>`, y los guarda en `chromadb.PersistentClient(path="chroma",
  settings=Settings(anonymized_telemetry=False))`, colección `radar-docs` con distancia coseno y
  `embedding_function=None` (los vectores siempre vienen del `Embedder`, así chromadb nunca
  descarga su modelo ONNX por defecto); re-crea la colección en cada indexado. CLI `--json` → `{"fragmentos": n, "archivos": m}`.
- **`ask.py`** — puerto `Llm(Protocol)`; adaptador `OllamaLlm` (`POST /api/chat`, `stream:
  false`, `temperature 0`). `answer(question, k=4)`: si la mejor distancia > `0.5` responde
  exactamente `No sé: no encontré evidencia en la documentación del proyecto.` sin llamar al LLM;
  si no, prompt en español que exige responder solo con el contexto y citar `[fuente: ruta#n]`.
  Devuelve `{"respuesta", "fuentes": [ids], "distancia_min"}`. CLI `python -m radar.rag.ask "<pregunta>" --json`.
- **`tests/unit/test_rag.py`** — `EmbedderFalso` determinista (hash → vector), `chromadb.EphemeralClient`,
  `LlmFalso` que cuenta llamadas.
- **`docs/rag.md`** — `## Arquitectura`, `## Requisitos de RAM`, `## Cómo usarlo`,
  `## Riesgos y alucinaciones`.

**Done when**
- [ ] WHEN `uv run pytest tests/unit/test_rag.py` runs THE SYSTEM SHALL exit 0 with 0 failed using a fake embedder, an in-memory chromadb client and a fake LLM.
- [ ] WHEN the best retrieved fragment's cosine distance exceeds 0.5 THE SYSTEM SHALL answer `No sé: no encontré evidencia en la documentación del proyecto.` and SHALL make zero LLM calls.
- [ ] WHEN evidence exists THE SYSTEM SHALL return an answer plus a `fuentes` list of `ruta#n` ids taken from `docs/`.
- [ ] WHEN `ollama list` runs THE SYSTEM SHALL list `llama3.2:3b` and `nomic-embed-text`.
- [ ] WHEN `uv run python -m radar.rag.index --json` and then `uv run python -m radar.rag.ask "¿Cada cuánto se recolecta station_status y por qué?" --json` run THE SYSTEM SHALL exit 0 and print a non-empty `fuentes` list whose ids all start with `docs/`.
- [ ] WHEN `docs/rag.md` is read THE SYSTEM SHALL contain `## Requisitos de RAM` and `## Riesgos y alucinaciones`.

**Verify**
```bash
uv run pytest tests/unit/test_rag.py tests/unit/test_encabezados.py  # expect: exit 0
uv run ruff check . && uv run ruff format --check . && uv run mypy  # expect: exit 0
ollama list | uv run python -c "import sys; t = sys.stdin.read(); sys.exit(0 if 'llama3.2:3b' in t and 'nomic-embed-text' in t else 1)"  # expect: exit 0
uv run python -m radar.rag.index --json  # expect: exit 0
uv run python -m radar.rag.ask "¿Cada cuánto se recolecta station_status y por qué?" --json | uv run python -c "import json, sys; f = json.load(sys.stdin)['fuentes']; sys.exit(0 if f and all(x.startswith('docs/') for x in f) else 1)"  # expect: exit 0
grep -qx '## Requisitos de RAM' docs/rag.md && grep -qx '## Riesgos y alucinaciones' docs/rag.md  # expect: exit 0
git check-ignore -q chroma/x  # expect: exit 0
```

**Checkpoint**
```bash
git add -A && git commit -m "feat(E3-T5): asistente RAG local con Ollama y fuentes citadas"
git tag step-17-rag
# rollback si el paso 18 se rompe: git reset --hard step-17-rag
```

**Preguntas de checkpoint:** ¿Qué es un embedding y por qué sirve para buscar? · ¿Cómo evita este diseño que el asistente invente respuestas? · **Retos:** 39 (IA generativa)*

#### Step 18 — Documentación final, guion de video y presentación

> Etapa 10 — Documentación, video y presentación · tarea `E3-T6` · epic `03-producto-entrega` · prioridad `p0` · depende de: paso 13 (`E3-T1`), paso 14 (`E3-T2`) · checkpoint `step-18-documentacion`

**Meta:** README final, documento técnico, manual de usuario, guion de 90 s y presentación de 10 diapositivas existen, y el repo se puede clonar y probar siguiendo solo el README.

**Archivos:** `README.md`, `docs/documento-tecnico.md`, `docs/manual-usuario.md`, `docs/guion-video.md`, `docs/presentacion.md`

**Do**
- **`README.md` (reescribir)** — conserva título y badge; secciones exactas: `## Demo` (GIF
  `docs/img/mapa.gif` cuando exista + enlace al video), `## Problema`, `## Arquitectura` (diagrama
  Mermaid de `docs/arquitectura.md`), `## Cómo correrlo en 5 comandos`, `## Resultados del modelo`
  (tabla de `docs/reportes/comparacion_modelos.md`), `## Retos cubiertos` (tabla "Resumen de retos
  por etapa" del plan, marcando lo que se entregó y lo recortado), `## Trabajo futuro` (§20.4),
  `## Licencia` (MIT, Angel Isacc Cabrera Leal). El bloque de 5 comandos es exactamente:
  ```bash
  $ git clone https://github.com/Isacc-Cabrera/radar-ecobici-cdmx.git && cd radar-ecobici-cdmx
  $ cp .env.example .env && uv sync --all-extras --all-groups
  $ docker compose --profile streaming up -d --build --wait kafka collector-kafka bronze-writer
  $ docker compose --profile ml up -d --wait mlflow && uv run python scripts/run_hourly.py --hour auto --stage todo && uv run python -m radar.ml.train --json
  $ docker compose --profile api up -d --build --wait api && npm --prefix frontend install && npm --prefix frontend run dev
  ```
  y aclara que el paso 4 necesita al menos unas horas de datos recolectados.
- **`docs/documento-tecnico.md`** — `## Resumen`, `## Arquitectura`, `## Decisiones` (D-001…D-010
  (+D-011 si el paso 9 usó el respaldo), con enlace a `bitacora.md`), `## Datos y calidad`, `## Modelo`, `## API y mapa`,
  `## Limitaciones`, `## Trabajo futuro`.
- **`docs/manual-usuario.md`** — `## Requisitos`, `## Cómo usar el mapa` (tema, lista, detalle,
  leyenda), `## Cómo leer la predicción`, `## Preguntas frecuentes`; enlaza `bitacora.md` para el
  porqué de las decisiones.
- **`docs/guion-video.md`** — guion de 90 s con marcas `mm:ss` (de `00:00` a `01:30` como máximo):
  gancho, problema, arquitectura, demo del mapa, modelo vs baseline, cierre con el repo.
- **`docs/presentacion.md`** — exactamente 10 encabezados `## Diapositiva N — <título>` (N = 1…10):
  1 Portada, 2 Problema, 3 Arquitectura, 4 Datos, 5 Modelo, 6 Demo, 7 Resultados,
  8 Limitaciones, 9 Trabajo futuro, 10 Cierre y preguntas; cada una con 3–5 viñetas y nota del orador.

**Done when**
- [ ] WHEN `README.md` is read THE SYSTEM SHALL contain the lines `## Demo`, `## Arquitectura`, `## Cómo correrlo en 5 comandos`, `## Resultados del modelo`, `## Retos cubiertos`, `## Trabajo futuro` and `## Licencia`.
- [ ] WHEN the `## Cómo correrlo en 5 comandos` section of `README.md` is read THE SYSTEM SHALL contain exactly 5 lines starting with `$ `.
- [ ] WHEN `docs/presentacion.md` is read THE SYSTEM SHALL contain exactly 10 headings starting with `## Diapositiva `, numbered 1 to 10 in order.
- [ ] WHEN `docs/guion-video.md` is read THE SYSTEM SHALL contain at least one `mm:ss` time mark and no mark later than `01:30`.
- [ ] WHEN `docs/documento-tecnico.md` and `docs/manual-usuario.md` are read THE SYSTEM SHALL find `bitacora.md` linked in both, `## Decisiones` in the former and `## Cómo usar el mapa` in the latter.
- [ ] WHEN `uv run pytest` and `uv run ruff check .` run on the final tree THE SYSTEM SHALL exit 0.

**Verify**
```bash
uv run python -c "import sys; t = open('README.md', encoding='utf-8').read().splitlines(); sys.exit(0 if all(h in t for h in ['## Demo', '## Arquitectura', '## Cómo correrlo en 5 comandos', '## Resultados del modelo', '## Retos cubiertos', '## Trabajo futuro', '## Licencia']) else 1)"  # expect: exit 0
uv run python -c "import sys; t = open('README.md', encoding='utf-8').read().split('## Cómo correrlo en 5 comandos', 1)[1].split('\n## ', 1)[0]; sys.exit(0 if sum(1 for l in t.splitlines() if l.startswith('$ ')) == 5 else 1)"  # expect: exit 0
uv run python -c "import re, sys; h = re.findall(r'^## Diapositiva (\d+)', open('docs/presentacion.md', encoding='utf-8').read(), re.M); sys.exit(0 if [int(x) for x in h] == list(range(1, 11)) else 1)"  # expect: exit 0
uv run python -c "import re, sys; m = [int(a) * 60 + int(b) for a, b in re.findall(r'\b(\d\d):(\d\d)\b', open('docs/guion-video.md', encoding='utf-8').read())]; sys.exit(0 if m and max(m) <= 90 else 1)"  # expect: exit 0
uv run python -c "import sys; d = open('docs/documento-tecnico.md', encoding='utf-8').read(); u = open('docs/manual-usuario.md', encoding='utf-8').read(); ok = 'bitacora.md' in d and 'bitacora.md' in u and '## Decisiones' in d.splitlines() and '## Cómo usar el mapa' in u.splitlines(); sys.exit(0 if ok else 1)"  # expect: exit 0
uv run pytest  # expect: exit 0
uv run ruff check .  # expect: exit 0
```

**Checkpoint**
```bash
git add -A && git commit -m "docs(E3-T6): README final, documento técnico, manual, guion y presentación"
git tag step-18-documentacion
rm -rf data/_verify/clon && git clone --no-local . data/_verify/clon  # expect: copia limpia desde el commit recién hecho
test -f data/_verify/clon/.env.example && test -f data/_verify/clon/uv.lock && test -f data/_verify/clon/frontend/package-lock.json  # expect: exit 0
cd data/_verify/clon && uv sync --frozen --all-extras --all-groups && uv run pytest && cd ../../..  # expect: exit 0: se clona y se prueba siguiendo solo el README
rm -rf data/_verify/clon  # expect: limpieza
# fin del build: §20.1
```

**Preguntas de checkpoint:** ¿Qué tres lecciones (técnica, de diseño, de gestión del tiempo) contarías en el post de lanzamiento? · Si tuvieras una semana más, ¿qué harías primero y por qué? · **Retos:** 36 y 50 (proyecto integral / integrador), 10 (data storytelling), 20 (portafolio) · 📣 Post #6 (lun 16, después de exponer): video demo + link al repo + 3 lecciones aprendidas (técnica, diseño, gestión del tiempo). Etiqueta a la IBERO si aplica.

---

### 9.1 Parity and cutover

Este build es greenfield **salvo por una migración interna**: en el paso 5 la captura pasa del
recolector local (`collector`, `SINK=local`, paso 3) al camino Kafka (`collector-kafka` →
`station_status_raw` → `bronze-writer`). El dato no puede perderse durante el cambio, así que la
migración se trata como un cutover formal.

#### Parity set

| # | Behavior held constant | How parity is proved | Tolerance |
|---|---|---|---|
| 1 | Cada snapshot nuevo del feed termina en `bronze/station_status/.../station_status_<last_updated>.json.gz` | Durante el traslape, el conjunto de `last_updated` con heartbeat `guardado` de `sink="kafka"` tiene su archivo en bronze (`scripts/demo_recovery.sh` hace exactamente este cruce) | exacto: 0 faltantes |
| 2 | El contenido de bronze es el cuerpo HTTP original | El consumidor escribe los bytes descomprimidos del mensaje raw (que son el cuerpo original); misma función `LocalFileSink.write_status` en ambos caminos | exacto |
| 3 | El heartbeat sigue distinguiendo `sin_cambios` / `guardado` / `error` | `collector-kafka` usa el mismo `CollectorRunner`; solo cambia el campo `sink` | exacto |

**Shadow period:** ≥ 3 minutos con ambos recolectores encendidos (paso 5, Do, punto 1–2). Los dos
escriben el mismo nombre de archivo, así que el traslape no duplica nada. La paridad cuenta como
probada con 0 faltantes en la demo de recuperación.

#### Cutover

| Phase | What changes | Who is affected | Reversible by | Verify |
|---|---|---|---|---|
| dark launch | `collector-kafka` + `bronze-writer` arrancan junto al `collector` local | nadie (bronze idempotente) | `docker compose --profile streaming stop collector-kafka bronze-writer` | `docker compose --profile streaming ps --services --status running` |
| full | `docker compose stop collector` | el lake depende solo de Kafka | `docker compose up -d collector` (instantáneo, < 10 s) | `DEMO_PAUSE_SECONDS=120 bash scripts/demo_recovery.sh` |
| decommission | ninguno en v1: el servicio `collector` queda definido y detenido como respaldo | — | — | — |

**The kill switch:** `docker compose up -d collector` vuelve a capturar por el camino local en
menos de 10 s, sin deploy. Úsalo ante cualquier criterio de aborto.

**Re-verificación del paso 3 después del cutover.** Los checks en vivo del paso 3
(`docker compose up -d collector` y la aserción de contenedor corriendo con `unless-stopped`)
quedan **exentos** de re-verificación a partir del paso 5: re-ejecutarlos reencendería el
recolector viejo. La única forma de re-verificar el paso 3 es la corrida de una vuelta
`docker compose run --rm -e RUN_ONCE=true collector` (contenedor efímero que no deja un segundo
recolector corriendo) más `uv run pytest tests/unit/test_runner.py`.

#### Abort criteria

Quién vigila: el autor (Isacc Cabrera), revisando el heartbeat durante la ventana de shadow
(`tail -f data/bronze/heartbeat/dt=<hoy>.jsonl` y `docker compose --profile streaming ps`).

- [ ] WHEN `scripts/demo_recovery.sh` reports `faltantes` > 0 THE SYSTEM SHALL revert with `docker compose up -d collector` and keep both paths until fixed.
- [ ] WHEN the heartbeat shows `result="error"` with `sink="kafka"` for 5 consecutive minutes THE SYSTEM SHALL revert with the kill switch.
- [ ] WHEN `docker compose --profile streaming ps` shows `kafka` not running THE SYSTEM SHALL revert with the kill switch before investigating.

#### Data migration

NOT APPLICABLE — no data moves: ambos caminos escriben el mismo directorio `data/bronze/` con el
mismo esquema de nombres.

#### Decommission

No hay decommission en el build: el servicio `collector` se conserva detenido como kill switch
hasta después de la exposición (13 nov). Borrarlo es una decisión posterior del usuario.

---

## 10. Environment Setup

### Prerequisites

| Tool | Version | Check |
|---|---|---|
| Windows 11 + Git Bash (Git for Windows) | Git 2.x | `git --version` |
| uv | ≥ 0.13 local (CI fija 0.13.7) | `uv --version` |
| Docker Desktop (WSL2) con Compose v2 | Docker 29 | `docker --version && docker compose version` |
| Node.js | 24 (Vite exige ^20.19 o ≥ 22.12) | `node --version` |
| GitHub CLI con la cuenta personal `IsaccCabrera09` en el keyring | cualquiera con `gh auth token -u` | `gh auth token -u IsaccCabrera09 >/dev/null && echo ok` |
| Terraform CLI (solo paso 16) | 1.17.1 | `terraform version` |
| Ollama para Windows (solo paso 17, opcional) | 0.40.15 | `ollama --version` |
| AWS CLI v2 (solo para aplicar el paso 16, fuera del build) | verificar antes de instalar | `aws --version` |

Python 3.12 lo instala el Bootstrap (`uv python install 3.12`). Desactiva la suspensión de la
laptop mientras el recolector corra (Configuración → Energía → "Nunca").

### Accounts to create first

| Servicio | URL | Paso que lo requiere | Costo |
|---|---|---|---|
| GitHub (cuenta personal `Isacc-Cabrera`) | https://github.com | 1 (puerta humana: crear repo y push) · 13 (CI corre al hacer push) | gratis |
| AWS (solo si el usuario aprueba el paso 16 aplicado) | https://aws.amazon.com | fuera del build (checklist §20.1) | ⚠️ puede generar costo; alerta de 5 USD |
| Ninguna otra: GBFS, Open-Meteo y OpenFreeMap no piden cuenta ni llave | — | — | gratis |

### Environment variables

| Variable | Purpose | Where to get it | Required by step | Secret? |
|---|---|---|---|---|
| `GBFS_URL` | Índice GBFS de ECOBICI | `.env.example`: `https://gbfs.mex.lyftbikes.com/gbfs/gbfs.json` | 2 | no |
| `GBFS_LANG` | Idioma de feeds dentro del índice | `.env.example`: `es` | 2 | no |
| `HTTP_TIMEOUT_SECONDS` | Timeout del cliente HTTP | `.env.example`: `10` | 2 | no |
| `RETRY_BASE_SECONDS` | Base del backoff exponencial | `.env.example`: `2` | 2 | no |
| `RETRY_CAP_SECONDS` | Tope del backoff (5 min) | `.env.example`: `300` | 2 | no |
| `LOG_LEVEL` | Nivel de logs | `.env.example`: `INFO` | 2 | no |
| `STATUS_INTERVAL_SECONDS` | Periodo de `station_status` (D-005) | `.env.example`: `60` | 3 | no |
| `INFO_REFRESH_HOURS` | Periodo de `station_information` | `.env.example`: `24` | 3 | no |
| `STALE_ALERT_SECONDS` | Umbral de feed congelado | `.env.example`: `600` | 3 | no |
| `DATA_DIR` | Raíz del lake (`/app/data` en contenedores vía Compose) | `.env.example`: `data` | 3 | no |
| `SINK` | `local` o `kafka` (Compose lo fija por servicio) | `.env.example`: `local` | 3 | no |
| `RUN_ONCE` | Una vuelta y salir (verificación) | `.env.example`: `false` | 3 | no |
| `KAFKA_BOOTSTRAP` | Broker desde el host (`kafka:9092` dentro de Compose) | `.env.example`: `localhost:9094` | 4 | no |
| `KAFKA_TOPIC_STATUS` | Topic por estación | `.env.example`: `station_status` | 4 | no |
| `KAFKA_TOPIC_RAW` | Topic de snapshots crudos | `.env.example`: `station_status_raw` | 4 | no |
| `KAFKA_GROUP_ID` | Grupo del consumidor | `.env.example`: `bronze-writer` | 5 | no |
| `DEMO_PAUSE_SECONDS` | Segundos que la demo de recuperación deja apagado al consumidor | `.env.example`: `300` (documental: `scripts/demo_recovery.sh` lo lee del shell con default 300; el Verify pasa `120`) | 5 | no |
| `S3_ENDPOINT_URL` | SeaweedFS S3 desde el host (`http://seaweedfs:8333` en Airflow) | `.env.example`: `http://localhost:8333` | 6 | no |
| `S3_REGION` | Región nominal para boto3 | `.env.example`: `us-east-1` | 6 | no |
| `S3_ADMIN_ACCESS_KEY` / `S3_ADMIN_SECRET_KEY` | Identidad admin de SeaweedFS | las genera `uv run python scripts/lake_setup.py render` en `.env` | 6 | sí |
| `S3_APP_ACCESS_KEY` / `S3_APP_SECRET_KEY` | Identidad `radar-app` (mínimo privilegio) | las genera `render` en `.env` | 6 | sí |
| `LAKE_BUCKET_BRONZE` / `LAKE_BUCKET_SILVER` / `LAKE_BUCKET_GOLD` | Buckets por capa | `.env.example`: `radar-bronze`, `radar-silver`, `radar-gold` | 6 | no |
| `TRIPS_CSV_URL` | CSV mensual de viajes | `.env.example`: `https://ecobici.cdmx.gob.mx/wp-content/uploads/2026/10/public_data_web_2026-10.csv`, verificada por el hilo principal el 2026-11-08 (HTTP 200, ~96 MB, encabezado confirmado) en https://ecobici.cdmx.gob.mx/datos-abiertos/; si `--probe` falla, el paso 8 toma el CSV más reciente de esa página | 8 | no |
| `LAKE_SYNC_S3` | Sincronizar a SeaweedFS al final del pipeline horario | `.env.example`: `false` | 9 | no |
| `WEATHER_LAT` / `WEATHER_LON` | Coordenadas CDMX para Open-Meteo | `.env.example`: `19.4326` / `-99.1332` | 9 | no |
| `QUALITY_MAX_GAP_MINUTES` | Umbral del check `huecos_heartbeat` | `.env.example`: `10` | 9 | no |
| `QUALITY_MAX_CAPACITY_FAIL_PCT` | Umbral del check `regla_capacidad` | `.env.example`: `5` | 9 | no |
| `POSTGRES_AIRFLOW_PASSWORD` | Contraseña local de la base de metadatos de Airflow | `.env.example`: `airflow-local` (solo local) | 9 | sí (local) |
| `MLFLOW_TRACKING_URI` | Servidor MLflow (`http://mlflow:5000` dentro de Compose) | `.env.example`: `http://localhost:5000` | 11 | no |
| `MODEL_NAME` | Nombre en el Model Registry | `.env.example`: `radar-bikes-t60` | 11 | no |
| `MODEL_FALLBACK_PATH` | Respaldo local del modelo | `.env.example`: `models/champion.joblib` | 11 | no |
| `API_CORS_ORIGINS` | Orígenes permitidos por CORS | `.env.example`: `http://localhost:5173` | 12 | no |
| `VITE_API_URL` | URL de la API para el mapa (opcional; default en código) | `frontend/.env.local` si cambia; default `http://localhost:8000` | 14 | no |
| `ANOMALY_FROZEN_MINUTES` | Minutos para "congelada" | `.env.example`: `90` | 15 | no |
| `ANOMALY_ZSCORE` | Umbral de z-score | `.env.example`: `3` | 15 | no |
| `AWS_PROFILE` | Perfil del AWS CLI para el sync de gold | `.env.example`: `radar-ecobici` | 16 (solo al aplicar) | no |
| `AWS_S3_BUCKET` | Bucket creado por Terraform | output de `terraform apply` (tras aprobación) | 16 (solo al aplicar) | no |
| `OLLAMA_URL` | Servidor local de Ollama | `.env.example`: `http://localhost:11434` | 17 | no |
| `OLLAMA_CHAT_MODEL` / `OLLAMA_EMBED_MODEL` | Modelos locales | `.env.example`: `llama3.2:3b` / `nomic-embed-text` | 17 | no |

`.env.example` está versionado con todas las llaves; los secretos van vacíos. `.env` está
ignorado. **"Required by step" es contrato con §9:** `Settings` (paso 2) declara todo con default
o como opcional (`str | None`); el módulo que consume una variable la valida al usarla
(`lake_setup.py` y `store.py` lanzan un error con nombre si faltan las llaves S3; `train.py` si no
responde MLflow). Ningún paso exige una variable antes del paso de esta tabla.

**Carga de `.env` por herramienta** (listar una variable no la carga):

| Herramienta | Mecanismo |
|---|---|
| Todo `uv run python …` (módulos `radar.*`, `scripts/*.py`, notebook) | `Settings(env_file=".env")` en `src/radar/config.py` (cargador en la configuración de la propia herramienta) |
| MLflow client dentro de `train.py`/`deps.py` | `mlflow.set_tracking_uri(settings.mlflow_tracking_uri)` explícito (MLflow no lee `.env`) |
| Docker Compose | lee `.env` para interpolación y lo pasa con `env_file: [.env]`; `environment:` sobrescribe lo propio de cada contenedor |
| `scripts/sync_gold_to_s3.sh` | `set -a; . ./.env; set +a` en su primera línea de lógica |
| `scripts/demo_recovery.sh` | no lee variables propias salvo `DEMO_PAUSE_SECONDS` (default 300); delega en Compose y `uv run` |
| Terraform | no lee `.env`: variables en `infra/terraform/terraform.tfvars` (ignorado); `validate` no las necesita |
| Vite | lee `frontend/.env*` (no el `.env` raíz); `VITE_API_URL` tiene default en código |

### Files that must be committed

| File | Why it is committed | Ignore-file exception line |
|---|---|---|
| `.env.example` | contrato de configuración (D-007) | `!.env.example` (después de los patrones `.env*`; ya está en `.gitignore`) |
| `.gitignore`, `.gitattributes`, `.dockerignore` | gobiernan qué ven git y Docker | — no coincide con ningún patrón |
| `uv.lock` | resolución exacta de Python | — no coincide con ningún patrón |
| `frontend/package-lock.json` | resolución exacta de npm | — no coincide con ningún patrón |
| `infra/terraform/.terraform.lock.hcl` | fija el binario del provider | — no coincide (`.terraform/` es solo el directorio) |
| `pyproject.toml`, `docker-compose.yml`, `.python-version` y todos los archivos `[W]` de §3 | configuración que los gates necesitan | — no coincide con ningún patrón |
| `.claude/settings.json`, `.claude/rules/*.md`, `.claude/skills/*/SKILL.md`, `CLAUDE.md`, `AGENTS.md` | configuración del agente | — no coincide |
| `docker/seaweedfs/s3.json.example` | formato de identidades sin secretos | — no coincide (`docker/seaweedfs/s3.json` es exacto) |
| `docs/reportes/*` | evidencia para la exposición y corpus del RAG | — no coincide |
| `blueprints/radar-ecobici-cdmx/**` | el diseño del proyecto | — no coincide (el `.gitignore` dice explícitamente que se versiona) |

La línea de excepción `!.env.example` ya está escrita en `workspace/.gitignore`, que el Bootstrap
coloca **antes** del primer commit.

### Bootstrap

Corre desde la raíz del proyecto, en Git Bash. Es no interactivo, idempotente, y una segunda
ejecución consecutiva sale 0 sin cambiar nada que importe.

```bash
set -euo pipefail
# order matters: copia de workspace (.gitignore con sus excepciones, .gitattributes, .dockerignore,
#   .env.example, pyproject.toml, configs) → .env → carpetas locales → repo init + identidad local
#   → Python 3.12 + uv sync (crea uv.lock) → assert .env ignorado → primer commit.
#   Servicios (Kafka, SeaweedFS, Airflow, MLflow, API), topics y buckets los crea cada paso de §9.
WS=blueprints/radar-ecobici-cdmx/workspace
test -d "$WS"   # el bundle debe estar en blueprints/radar-ecobici-cdmx/ de la raíz
# Copia NO destructiva: crea solo los archivos que faltan y nunca sobrescribe uno existente
# (pyproject.toml, docker-compose.yml, frontend/package.json, .gitignore, CLAUDE.md… una vez
# editados por un paso, se conservan). `[ -e ] ||` sale 0 al omitir; no se usa `cp -n` porque
# su código de salida al omitir cambia entre GNU y BSD.
(cd "$WS" && find . -type f) | while IFS= read -r f; do
  [ -e "$f" ] || { mkdir -p "$(dirname "$f")" && cp "$WS/$f" "$f"; }
done
[ -e .env ] || cp .env.example .env
mkdir -p data/_verify models docs/reportes
git rev-parse --git-dir >/dev/null 2>&1 || git init -b main   # idempotente
git config user.name "Isacc Cabrera"                                         # identidad LOCAL del repo
git config user.email "255291757+Isacc-Cabrera@users.noreply.github.com"     # nunca --global
# uv 0.13.x en Windows puede salir 2 ("Missing expected target directory for Python minor version link")
# aunque el intérprete SÍ quedó instalado: se acepta si después `uv python find` lo encuentra.
uv python find 3.12 >/dev/null 2>&1 || uv python install 3.12 || uv python find 3.12 >/dev/null
uv sync --all-extras --all-groups      # crea .venv y uv.lock; Airflow NO entra aquí
git check-ignore -q .env               # .env ignorado antes de cualquier commit
rc=0; git check-ignore -q .env.example || rc=$?; test "$rc" -eq 1   # .env.example NO ignorado (1); 128 = error de uso y falla
git rev-parse --verify -q HEAD >/dev/null || { git add -A && git commit -m "chore: estructura inicial desde el blueprint"; }
```

**Smoke test del hilo principal (2026-11-08):** este bloque corrió dos veces seguidas con exit 0
(con `uv sync` completo: 233 paquetes), el `Verify` del paso 1 pasó, y `ruff check`, `ruff format
--check`, `mypy` y la prueba de encabezados pasaron con el bundle presente; `docker compose config
-q` pasa para todos los perfiles.

Permisos: cada comando del bloque (`cd`, `find . -type f`, el lazo `while IFS= read -r f`, `[ -e`,
`mkdir -p`, `cp`, `git …`, `uv …`, `test`, `rc=`) está en `permissions.allow` (§19.3).

Notas: el primer commit solo ocurre si el repo no tiene ninguno (una segunda ejecución no
commitea trabajo a medias). Los archivos que nunca se sobrescriben son todos los de `workspace/`
una vez que existen — en particular `pyproject.toml`, `docker-compose.yml` y
`frontend/package.json`; `uv.lock` y `package-lock.json` no viven en `workspace/` y nunca se copian.

---

## 11. Dependencies

Fuente externa de datos (no es paquete): `TRIPS_CSV_URL` =
`https://ecobici.cdmx.gob.mx/wp-content/uploads/2026/10/public_data_web_2026-10.csv`, verificada
por el hilo principal el 2026-11-08 (HTTP 200, ~96 MB, encabezado confirmado), publicada en
https://ecobici.cdmx.gob.mx/datos-abiertos/.

Fuente de todas las versiones: informe `stack-researcher` + verificación del hilo principal del
**2026-11-08** (PyPI `https://pypi.org/pypi/<name>/json`, `registry.npmjs.org`, Docker Hub tags
API, GitHub Packages, `checkpoint-api.hashicorp.com`). Ninguna versión viene de memoria ni de los
runtime tracks. Paquetes 0.x fijados con `==`.

### Runtime

| Package | Version | Source (registry URL or track file) | Checked | Installed by | Purpose |
|---|---|---|---|---|---|
| httpx | 0.28.1 | https://pypi.org/pypi/httpx/json | 2026-11-08 | §10 Bootstrap (`uv sync --all-extras --all-groups` sobre `pyproject.toml`) | cliente HTTP del feed, Open-Meteo y Ollama (sin release desde 2024-12; aceptado como estándar) |
| structlog | 26.1.0 | https://pypi.org/pypi/structlog/json | 2026-11-08 | §10 Bootstrap | logs JSON estructurados |
| pydantic | 2.14.3 | https://pypi.org/pypi/pydantic/json | 2026-11-08 | §10 Bootstrap | modelos de la API y validación |
| pydantic-settings | 2.16.2 | https://pypi.org/pypi/pydantic-settings/json | 2026-11-08 | §10 Bootstrap | `Settings` desde `.env` |
| confluent-kafka | 2.16.1 | https://pypi.org/pypi/confluent-kafka/json | 2026-11-08 | §10 Bootstrap (extra `streaming`); imagen: `docker/collector.Dockerfile` (paso 3) | productor/consumidor Kafka (wheels manylinux_2_28: no Alpine) |
| boto3 | 1.43.126 | https://pypi.org/pypi/boto3/json | 2026-11-08 | §10 Bootstrap (extra `lake`) | cliente S3 para SeaweedFS |
| pyarrow | 26.0.1 | https://pypi.org/pypi/pyarrow/json | 2026-11-08 | §10 Bootstrap (extra `lake`) | escritura/lectura Parquet y esquemas |
| duckdb | 2.0.0 | https://pypi.org/pypi/duckdb/json | 2026-11-08 | §10 Bootstrap (extra `lake`) | SQL analítico y benchmark |
| polars | 2.1.0 | https://pypi.org/pypi/polars/json | 2026-11-08 | §10 Bootstrap (extra `lake`) | transformaciones bronze→silver→gold |
| pandas | 3.1.1 | https://pypi.org/pypi/pandas/json | 2026-11-08 | §10 Bootstrap (extra `lake`) | features/ML y benchmark (semántica 3.x: copy-on-write) |
| scikit-learn | 1.9.1 | https://pypi.org/pypi/scikit-learn/json | 2026-11-08 | §10 Bootstrap (extra `ml`) | regresión lineal, TimeSeriesSplit, IsolationForest |
| lightgbm | 4.7.0 | https://pypi.org/pypi/lightgbm/json | 2026-11-08 | §10 Bootstrap (extra `ml`); imagen: `docker/api.Dockerfile` (paso 13, + `libgomp1`) | modelo principal |
| mlflow | 3.18.0 | https://pypi.org/pypi/mlflow/json | 2026-11-08 | §10 Bootstrap (extra `ml`) | tracking y Model Registry |
| plotly | 7.1.0 | https://pypi.org/pypi/plotly/json | 2026-11-08 | §10 Bootstrap (extra `ml`) | gráficas del EDA y real vs predicción |
| holidays | 0.108 | https://pypi.org/pypi/holidays/json | 2026-11-08 | §10 Bootstrap (extra `ml`) | festivos de México |
| jupyterlab | 4.6.6 | https://pypi.org/pypi/jupyterlab/json | 2026-11-08 | §10 Bootstrap (extra `ml`) | notebook EDA (trae `ipykernel`) |
| nbconvert | 7.17.2 | https://pypi.org/pypi/nbconvert/json | 2026-11-08 | §10 Bootstrap (extra `ml`) | ejecutar y exportar el notebook a HTML |
| fastapi | 0.162.2 | https://pypi.org/pypi/fastapi/json | 2026-11-08 | §10 Bootstrap (extra `api`); imagen: `docker/api.Dockerfile` | API |
| uvicorn | 0.54.0 | https://pypi.org/pypi/uvicorn/json | 2026-11-08 | §10 Bootstrap (extra `api`) | servidor ASGI |
| chromadb | 1.5.9 | https://pypi.org/pypi/chromadb/json | 2026-11-08 | §10 Bootstrap (extra `rag`) | base vectorial del RAG |
| uv_build (backend de build) | `>=0.13.7,<0.14.0` (rango, no pin) | https://pypi.org/pypi/uv-build/json — rango comprobado construyendo el paquete con `uv build --wheel` (sin fijar la versión exacta resuelta) | 2026-11-08 | §10 Bootstrap (`[build-system]` de `pyproject.toml`) | instala el paquete `radar` desde `src/` |
| psutil (transitiva) | la que resuelva `uv.lock` | transitiva de `jupyterlab` → `ipykernel` | 2026-11-08 | §10 Bootstrap | memoria pico en el benchmark (paso 8); sin pin propio |
| tzdata (transitiva) | la que resuelva `uv.lock` | transitiva de `pandas` | 2026-11-08 | §10 Bootstrap | base de zonas horarias en Windows (`America/Mexico_City`) |

### Development

| Package | Version | Source (registry URL or track file) | Checked | Installed by | Purpose |
|---|---|---|---|---|---|
| pytest | 9.2.0 | https://pypi.org/pypi/pytest/json | 2026-11-08 | §10 Bootstrap (grupo `dev`) | pruebas |
| pytest-cov | 7.1.0 | https://pypi.org/pypi/pytest-cov/json | 2026-11-08 | §10 Bootstrap (grupo `dev`) | cobertura en CI |
| respx | 0.23.1 | https://pypi.org/pypi/respx/json | 2026-11-08 | §10 Bootstrap (grupo `dev`) | mocks de httpx (pide httpx ≥ 0.25) |
| ruff | 0.17.4 | https://pypi.org/pypi/ruff/json | 2026-11-08 | §10 Bootstrap (grupo `dev`) | lint + formato |
| mypy | 2.4.1 | https://pypi.org/pypi/mypy/json | 2026-11-08 | §10 Bootstrap (grupo `dev`) | tipos |
| react | 19.3.1 | https://registry.npmjs.org/react | 2026-11-08 | paso 14 (`npm --prefix frontend install`) | UI |
| react-dom | 19.3.1 | https://registry.npmjs.org/react-dom | 2026-11-08 | paso 14 | render en el DOM |
| @deck.gl/core | 9.4.1 | https://registry.npmjs.org/@deck.gl/core | 2026-11-08 | paso 14 | motor deck.gl, `FlyToInterpolator` |
| @deck.gl/layers | 9.4.1 | https://registry.npmjs.org/@deck.gl/layers | 2026-11-08 | paso 14 | `ColumnLayer`, `ScatterplotLayer` |
| @deck.gl/react | 9.4.1 | https://registry.npmjs.org/@deck.gl/react | 2026-11-08 | paso 14 | componente `DeckGL` |
| @deck.gl/widgets | 9.4.1 | https://registry.npmjs.org/@deck.gl/widgets | 2026-11-08 | paso 14 | peer obligatorio de `@deck.gl/react` ~9.4 |
| maplibre-gl | 6.17.0 | https://registry.npmjs.org/maplibre-gl | 2026-11-08 | paso 14 | mapa base sin token |
| react-map-gl | 8.1.3 | https://registry.npmjs.org/react-map-gl | 2026-11-08 | paso 14 | `<Map>` vía `react-map-gl/maplibre` |
| recharts | 3.10.1 | https://registry.npmjs.org/recharts | 2026-11-08 | paso 14 | gráfica de las últimas 6 h |
| vite | 8.3.6 | https://registry.npmjs.org/vite | 2026-11-08 | paso 14 | dev server, build y `preview` del smoke |
| @vitejs/plugin-react | 6.2.1 | https://registry.npmjs.org/@vitejs/plugin-react | 2026-11-08 | paso 14 | JSX/refresh (peer vite ^8) |
| typescript | 5.9.3 | https://registry.npmjs.org/typescript | 2026-11-08 | paso 14 | tipos (7.0.2 descartado: major nativo sin compatibilidad comprobada) |
| vitest | 5.1.0 | https://registry.npmjs.org/vitest | 2026-11-08 | paso 14 | pruebas del sistema de diseño (peer vite ^8) |
| @types/react | 19.3.0 | https://registry.npmjs.org/@types/react | 2026-11-08 | paso 14 | tipos |
| @types/react-dom | 19.3.0 | https://registry.npmjs.org/@types/react-dom | 2026-11-08 | paso 14 | tipos |

### Imágenes Docker (el archivo emitido es el instalador)

| Package | Version | Source (registry URL or track file) | Checked | Installed by | Purpose |
|---|---|---|---|---|---|
| python | 3.12-slim | https://hub.docker.com/_/python (tags API) | 2026-11-08 | `docker/collector.Dockerfile` (3), `docker/api.Dockerfile` (13) | base glibc |
| apache/kafka | 4.4.0 | https://hub.docker.com/r/apache/kafka (tags API; `latest` va atrasado) | 2026-11-08 | `docker-compose.yml` [W], arrancado en el paso 4 | broker KRaft |
| chrislusf/seaweedfs | 4.53 | https://hub.docker.com/r/chrislusf/seaweedfs | 2026-11-08 | `docker-compose.yml` [W], paso 6 | S3 local (D-009) |
| apache/airflow | 3.4.0-python3.12 | https://hub.docker.com/r/apache/airflow | 2026-11-08 | `docker/airflow.Dockerfile` (paso 9) | orquestación (receta compose: UNVERIFIED, verificar antes de invertir más de 3 h) |
| postgres | 17-alpine | https://hub.docker.com/_/postgres | 2026-11-08 | `docker-compose.yml` [W], paso 9 | metadatos de Airflow |
| ghcr.io/mlflow/mlflow | v3.18.0 | https://github.com/mlflow/mlflow/pkgs/container/mlflow | 2026-11-08 | `docker-compose.yml` [W], paso 11 | servidor MLflow |
| uv (dentro de imágenes) | 0.13.7 | https://pypi.org/pypi/uv/json | 2026-11-08 | `pip install uv==0.13.7` en los Dockerfiles (3, 9, 13) | sync reproducible desde `uv.lock` |

### CI, IaC y modelos locales

| Package | Version | Source (registry URL or track file) | Checked | Installed by | Purpose |
|---|---|---|---|---|---|
| actions/checkout | v7 | https://github.com/actions/checkout/releases | 2026-11-08 | `.github/workflows/ci.yml` (paso 13) | clonar en CI |
| actions/setup-python | v7 | https://github.com/actions/setup-python/releases | 2026-11-08 | paso 13 | Python 3.12 en CI |
| astral-sh/setup-uv | v10 (uv 0.13.7) | https://github.com/astral-sh/setup-uv/releases | 2026-11-08 | paso 13 | uv fijado en CI |
| actions/setup-node | v7 | https://github.com/actions/setup-node/releases | 2026-11-08 | reservado (CI v1 no compila el frontend; ver §12) | — |
| docker/setup-buildx-action | v4 | https://github.com/docker/setup-buildx-action/releases | 2026-11-08 | paso 13 | builder de imágenes |
| docker/build-push-action | v7 | https://github.com/docker/build-push-action/releases | 2026-11-08 | paso 13 (`push: false`) | build de imágenes sin publicar |
| Terraform CLI | 1.17.1 | https://checkpoint-api.hashicorp.com/v1/check/terraform | 2026-11-08 | prerrequisito (§10), usado en el paso 16 | IaC |
| hashicorp/aws (provider) | `~> 6.72` (6.72.0 verificado) | https://registry.terraform.io/providers/hashicorp/aws | 2026-11-08 | paso 16 (`terraform init -backend=false`) | S3, IAM, Budgets |
| Ollama | 0.40.15 | https://github.com/ollama/ollama/releases | 2026-11-08 | prerrequisito (§10), paso 17 | runtime LLM local |
| llama3.2:3b | 2.0 GB | https://ollama.com/library/llama3.2 | 2026-11-08 | paso 17 (`ollama pull llama3.2:3b`) | redacción de respuestas RAG |
| nomic-embed-text | 274 MB, contexto 2K | https://ollama.com/library/nomic-embed-text | 2026-11-08 | paso 17 (`ollama pull nomic-embed-text`) | embeddings |

### Deliberately not used

| Rejected | Instead | Why |
|---|---|---|
| MinIO | SeaweedFS | repo archivado 2026-04-25 (D-009) |
| ZooKeeper | Kafka KRaft | eliminado desde Kafka 4.0 |
| tenacity | `RetryPolicy` escrito a mano | el patrón Strategy es material de la exposición |
| kafka-python | confluent-kafka | idempotencia del productor y mantenimiento |
| Apache Airflow dentro del entorno uv | imagen `apache/airflow` con venv aislado | sus constraints chocan con `pandas==3.1.1` |
| Mapbox / CARTO | OpenFreeMap | piden token o API key (D-010) |
| `@deck.gl/mapbox` | `DeckGL` con `<Map>` de `react-map-gl/maplibre` como hijo | integración más simple, sin interleaving |
| WebSockets | polling de 60 s | non-goal (§1) |
| TypeScript 7.0.2 / 6.0.3 | 5.9.3 | major nativo nuevo / publicado hoy |
| Alpine para imágenes Python | `python:3.12-slim` | sin wheels musl de confluent-kafka |
| `aws_iam_access_key` en Terraform | llave creada a mano tras aprobar | el secreto quedaría en el state |

---

## 12. Deployment Strategy

### Hosting

Local: Docker Desktop en Windows 11 (31.5 GB RAM, 8 núcleos) corre todos los servicios con
Docker Compose; el frontend corre con `npm --prefix frontend run dev` (http://localhost:5173) o se
sirve con `vite preview`. No hay despliegue público en v1 (non-goal). El código vive en GitHub
(`Isacc-Cabrera/radar-ecobici-cdmx`, público). AWS solo almacena gold si el usuario aprueba el paso 16.

### Environments

| Environment | Branch | URL | Database | Third-party mode |
|---|---|---|---|---|
| Local | `main` | http://localhost:8000 (API), :5173 (mapa), :8080 (Airflow), :5000 (MLflow), :8333 (S3) | lake `data/` + SeaweedFS | feed GBFS real, Open-Meteo real |
| CI | cualquier push/PR | GitHub Actions | ninguna (pruebas con fixtures) | sin red externa en pruebas |
| Production | NOT APPLICABLE — no hay despliegue público (non-goal §1) | — | — | — |

### CI/CD

`.github/workflows/ci.yml` (paso 13), en cada push y pull request:
1. `uv sync --frozen --all-extras --all-groups`
2. `uv run ruff check .` · `uv run ruff format --check .`
3. `uv run mypy`
4. `uv run pytest --cov=radar`
5. build de `docker/collector.Dockerfile` y `docker/api.Dockerfile` con `push: false`

El frontend y Terraform no corren en CI v1 (el gate local §20.1 sí los incluye); agregar un job
con `actions/setup-node@v7` es el primer punto de §20.4.

### Release and rollback

Cada paso es un tag `step-NN-<slug>`; un paso fallido se revierte con `git reset --hard <tag
anterior>`. Los contenedores se reconstruyen con `docker compose ... up -d --build <servicio>`.
Rollback de la migración Kafka: §9.1 (kill switch en < 10 s). Push a GitHub solo con aprobación.

### Domain, DNS, TLS

NOT APPLICABLE — sin dominio ni despliegue público.

---

## 13. Testing Strategy

| Layer | Framework | What it covers | Where | Runs |
|---|---|---|---|---|
| Unit | pytest + respx | reintentos, descubrimiento, hash, runner, mensajes Kafka, consumidor (fakes), LakeStore, cadena de calidad, gold/viajes, features (sin fuga), modelos y folds, detectores, RAG (fakes), encabezados | `tests/unit/` | cada paso + CI |
| Integration | pytest + FastAPI `TestClient` | la API completa con dependencias sustituidas (rutas, validación, CORS, carga única del modelo) | `tests/integration/test_api.py` | paso 12 en adelante + CI |
| Frontend unit | Vitest (entorno node) | contraste WCAG de tokens, umbrales de predicción, rampa, tema inicial | `frontend/src/lib/scales.test.ts` | paso 14 + §20.1 |
| Sistema (en vivo) | comandos `Verify` | contenedores, feed real, broker, SeaweedFS, Airflow, MLflow, smoke de la API y del build | §9 | cada paso (no en CI) |

### Critical flows to cover E2E
1. Feed → recolector → Kafka → bronze sin pérdida tras 2–5 min de consumidor caído (`scripts/demo_recovery.sh`).
2. Bronze → silver → gold idempotente para una hora real (`--check-idempotent` en los pasos 7 y 8).
3. Modelo registrado → API → `/stations` con datos reales (smoke del paso 12 y contenedor del 13).

### Test data
Las pruebas usan solo `tests/fixtures/*.json` (emitidos en `workspace/`) y marcos sintéticos
construidos en cada prueba; la fixture `settings` apunta `data_dir` a `tmp_path`, así que ninguna
prueba lee ni escribe `data/`. No hay servicio real en las pruebas automáticas: Kafka, S3 y Ollama
se sustituyen con fakes; los servicios reales solo aparecen en los `Verify` en vivo, provistos por
`docker-compose.yml` (§19.6).

### What is deliberately not tested
- Renderizado WebGL del mapa (no observable en entorno node): se cubre con el build, el smoke y
  una revisión manual con captura/GIF (§20.1).
- E2E en navegador con Playwright: fuera de alcance por tiempo; la skill `playwright-cli` queda
  como opción en §18.
- DAGs de Airflow en pytest (Airflow no está en el entorno uv): se prueban dentro del contenedor
  con `airflow dags list` y `airflow dags test`.

---

## 14. Security & Secrets

| Concern | Control | Implemented in |
|---|---|---|
| Secret storage | `.env` ignorado; secretos S3 generados por `lake_setup.py render`; `terraform.tfvars` ignorado; token de GitHub solo en el keyring de `gh` | `.gitignore` [W], paso 6, paso 16 |
| Secret rotation | Llaves S3: vaciar en `.env` y repetir `render` + reiniciar SeaweedFS; llave AWS: `aws iam delete-access-key` tras la demo | `docs/nube.md`, `docs/runbook.md` |
| Input validation | Pydantic en la API; `SchemaCheck` en silver; argparse en los CLI | pasos 7 y 12 |
| Output encoding / XSS | React escapa por defecto; nunca `dangerouslySetInnerHTML` | paso 14 |
| SQL injection | DuckDB solo ejecuta `sql/analitica.sql` versionado; sin SQL construido con entrada de usuario | paso 8 |
| AuthN / AuthZ | NOT APPLICABLE: sin cuentas; API solo en localhost | — |
| CSRF | NOT APPLICABLE: solo rutas GET sin cookies | — |
| Rate limiting / abuse | Sin límites (API local); el recolector respeta `Retry-After` y consulta cada 60 s (no cada 10 s) | paso 2 |
| Webhook verification | NOT APPLICABLE: no hay webhooks | — |
| Dependency audit | Versiones fijadas y `uv.lock`/`package-lock.json` versionados; revisión manual con `npm --prefix frontend audit` antes del post de lanzamiento | §20.1 |
| Security headers | NOT APPLICABLE: sin despliegue público | — |
| PII handling | Viajes crudos (`Genero_Usuario`, `Edad_Usuario`, `Bici`) solo en `data/bronze/trips/` local; silver con lista blanca; `sync_lake.py` excluye `bronze/trips/` en código; nunca a AWS | pasos 6 y 8 |
| Logging hygiene | Nunca se loguean llaves ni el contenido de `.env`; `lake_setup.py` imprime solo nombres de identidad | pasos 2 y 6 |
| Least privilege | Identidad `radar-app` en SeaweedFS sin crear/borrar buckets (probado); IAM de AWS limitado a `gold/*` de un bucket | pasos 6 y 16 |
| Identidad de GitHub | Identidad git LOCAL del repo; token personal inyectado por comando (`gh auth token -u IsaccCabrera09`), nunca guardado en archivos; prohibidos `gh auth switch` y `git config --global` | §10, `.claude/settings.json` |

**Hard rules**
- Ningún secreto se commitea, se imprime en un log ni llega al bundle del navegador.
- Nada sale de la máquina (push, `terraform apply`, LLM de pago) sin el "sí" del usuario.
- Datos regulados: el CSV de viajes trae datos personales (género, edad, id de bici); se tratan como
  PII bajo la ley mexicana de protección de datos en posesión de particulares — minimización por
  lista blanca y retención solo local.

---

## 15. Accessibility

**Target: WCAG 2.2 Level AA.**

### Baseline requirements
| Requirement | Rule |
|---|---|
| Semantic HTML | `<main>` con el mapa, `<aside id="panel">` con un `h1`, lista de estaciones como `<ul>` de `<button>` |
| Keyboard | botón de tema, filtro, lista y detalle operables con teclado; enlace "Saltar al panel" |
| Focus visible | `:focus-visible` con 3px `--focus` (≥ 3:1 medido) |
| Contrast | todos los pares texto/fondo ≥ 4.5:1 en ambos temas (prueba automática) |
| Forms | el filtro de estaciones tiene `<label>` visible |
| Images | el canvas del mapa lleva `aria-label` descriptivo; la información también está en la lista |
| Motion | `prefers-reduced-motion: reduce` desactiva el vuelo de cámara y las transiciones |
| Zoom / reflow | a < 768 px el panel pasa a hoja inferior; sin scroll horizontal |
| Live regions | "Actualizado hace N s" con `aria-live="polite"` |

### WCAG 2.2 additions — the ones most often missed
| SC | Requirement |
|---|---|
| 2.4.11 Focus Not Obscured (Min) | el panel nunca tapa el elemento enfocado de la lista (scroll al foco) |
| 2.5.7 Dragging Movements | además de arrastrar el mapa, seleccionar desde la lista vuela a la estación |
| 2.5.8 Target Size (Min) | botones ≥ 24×24 px |
| 3.3.7 Redundant Entry | NOT APPLICABLE — no hay formularios de varios pasos |
| 3.3.8 Accessible Authentication (Min) | NOT APPLICABLE — no hay autenticación |

### Verification
```bash
npm --prefix frontend run test   # expect: exit 0 — contraste AA de todos los pares de tokens en ambos temas
```
Pases manuales antes de la exposición: recorrido solo con teclado, un pase con Narrador de Windows
sobre el panel y zoom al 200 % a 375 px de ancho.

---

## 16. Observability & Cost

### Instrumentation
| Signal | Tool | What it captures | Who looks at it |
|---|---|---|---|
| Errors | structlog nivel ERROR/WARNING a stderr → `docker compose logs` | excepciones de cada tick, `feed_congelado`, fallas de entrega Kafka | Isacc |
| Logs | structlog JSON (`ts`, `level`, `logger`, `event`, campos) | todo el pipeline | Isacc |
| Metrics | heartbeat JSONL + manifiestos `_manifest.json` + MLflow | las métricas de abajo | Isacc y el notebook EDA |
| Uptime | `docker compose ps` + `GET /health` + check `huecos_heartbeat` | recolector vivo y datos frescos | Isacc, cada mañana |

### The metrics that matter for this project
| Metric | Target | Alert at |
|---|---|---|
| `cobertura_heartbeat` por hora | ≥ 95 % | hueco > `QUALITY_MAX_GAP_MINUTES` (10 min) → check `huecos_heartbeat` sale 3 |
| Feed congelado | 0 eventos | `last_updated` sin cambio > 600 s → WARNING + heartbeat `error` |
| `tasa_regla_capacidad` | reportada | `100 × (1 − tasa)` > 5 % → check `regla_capacidad` sale 3 |
| `lag_consumidor` del grupo `bronze-writer` | ≤ 2 mensajes | creciendo 5 min seguidos → kill switch §9.1 |
| `mae_t60` / `mejora_vs_baseline_pct` | mejora > 0 | mejora ≤ 0 → explicación obligatoria en `docs/modelo.md` |
| `segundos_desde_ultimo_dato` en `/health` | < 180 s | > 600 s → revisar `docs/runbook.md` |

### Health check
`GET /health` (API): 200 si el proceso vive, con `modelo_cargado`, `fuente_modelo` y
`segundos_desde_ultimo_dato` (frescura real de silver). Lo usa el healthcheck del servicio `api` en
Compose. El recolector se monitorea por heartbeat (no tiene puerto).

### Cost model
| Service | Free tier | Cost at expected v1 scale | Cost at 10× | Cliff to watch |
|---|---|---|---|---|
| Todo local (Docker, Kafka, SeaweedFS, Airflow, MLflow, Ollama) | gratis | 0 USD | 0 USD | disco: ~1 M filas/día ⇒ unos cientos de MB/día en bronze gzip |
| GBFS / Open-Meteo / OpenFreeMap | gratis, sin llave | 0 USD | 0 USD | términos de OpenFreeMap no leídos (pendiente) |
| GitHub + Actions (repo público) | gratis | 0 USD | 0 USD | — |
| AWS S3 (solo gold, < 1 GB, si se aprueba) | 12 meses de free tier en cuentas nuevas | centavos de USD/mes | < 1 USD/mes | requests masivos de sync; alerta de presupuesto en 5 USD |
| AWS Budgets | primeras alertas sin costo (verificar en precios de AWS) | 0 USD | 0 USD | — |

**Estimated monthly cost at launch: 0 USD** (AWS sin aplicar). El único rubro posible es S3 si el
usuario aprueba el paso 16; la palanca es `terraform destroy` después de la demo.

---

## 17. Model Routing

El proyecto llama a un LLM en tiempo de ejecución solo en el paso 17 (opcional, primero en
recortarse), y es **local** (Ollama): no hay modelos de Anthropic ni de pago, por lo que no aplica
la tabla de precios de la skill `claude-api`.

### Routing table
| Task in this product | Model tier | Why this tier | Fallback |
|---|---|---|---|
| Redactar la respuesta del RAG a partir de fragmentos | `llama3.2:3b` local (Ollama) | 2.0 GB, cabe con holgura en 31.5 GB de RAM y responde en segundos en CPU | responder "No sé…" (sin LLM) |
| Embeddings de `docs/` y reportes | `nomic-embed-text` local (274 MB, contexto 2K) | gratis, buen recall para español técnico corto | ninguno: sin embeddings no se indexa |
| Cualquier tarea con LLM de pago | prohibida sin aprobación | costo (regla de trabajo 6) | — |

### Prompt and context strategy
Prompt de sistema en español dentro de `src/radar/rag/ask.py` (constante versionada): responder
solo con el contexto, citar `[fuente: ruta#n]`, decir "No sé" si el contexto no alcanza. Contexto =
4 fragmentos de ~800 caracteres (cabe en el contexto del modelo). Sin caché.

### Cost controls
Costo 0 (local). Límite de tokens de salida `num_predict: 400`; `temperature: 0`.

### Failure handling
Timeout de 60 s por llamada a Ollama con `RetryPolicy`; si Ollama no responde, el CLI sale 3 con
el mensaje "¿está corriendo `ollama serve`?". Umbral de distancia 0.5 ⇒ "No sé" sin llamar al LLM.

### Evaluation
`tests/unit/test_rag.py` (fakes deterministas) + la pregunta fija del `Verify` del paso 17
("¿Cada cuánto se recolecta station_status y por qué?"), que debe citar fuentes de `docs/`.

---

## 18. Skills to Use During Build

Nombres y comandos copiados de `knowledge/skills-registry.md`. Ninguna es dependencia dura: si no
está instalada, el builder sigue con este blueprint, lo anota en una línea y continúa.

| Skill | Build steps | Why | Install |
|---|---|---|---|
| frontend-design | 14, 15 | UI distintiva y cuidada para el panel y el mapa dentro del sistema de §7 | `/plugin marketplace add anthropics/skills` then `/plugin install example-skills@anthropic-agent-skills` |
| ui-ux-pro-max | 14 | revisar jerarquía, espaciado y estados del panel contra los tokens de §7 | `/plugin marketplace add nextlevelbuilder/ui-ux-pro-max-skill` then `/plugin install ui-ux-pro-max@ui-ux-pro-max-skill` |
| emil-design-eng | 14 | duración y curva del vuelo de cámara y de las transiciones del panel | `npx skills@latest add emilkowalski/skills` |
| playwright-cli | 14 (opcional) | capturas del mapa en ambos temas para el README y el post | `npm install -g @playwright/cli@latest` then `playwright-cli install --skills` |
| pdf | 11, 18 (opcional) | revisar el PDF impreso del notebook antes de entregarlo | `/plugin marketplace add anthropics/skills` then `/plugin install document-skills@anthropic-agent-skills` |

Todas se activan solas por intención (sin `/`); se nombran en prosa al usarlas.

---

## 19. Agent Workspace

Bundle mode: todo lo de esta sección son archivos reales en
`blueprints/radar-ecobici-cdmx/workspace/`, y el contenido de abajo es idéntico byte por byte.

```
blueprints/radar-ecobici-cdmx/workspace/
├── CLAUDE.md                    # §19.1
├── AGENTS.md                    # §19.2
├── .claude/settings.json        # §19.3
├── .claude/skills/*/SKILL.md    # §19.4
├── .claude/rules/*.md           # §19.5
└── (configs, fixtures, paquete y frontend base)   # §19.6
```

**La copia es segura de re-ejecutar** y se hace con el bloque de §10 (copia archivo por archivo
solo si falta: `[ -e "$f" ] || { mkdir -p …; cp …; }`, que sale 0 al omitir en cualquier
plataforma; no se usa `cp -Rn`, que sale 1 en BSD/macOS al omitir). Nunca se sobrescriben
`pyproject.toml`, `docker-compose.yml`, `frontend/package.json` ni ningún otro archivo ya presente;
`uv.lock` y `frontend/package-lock.json` no están en `workspace/`.

**Los archivos emitidos pasan los gates del propio blueprint:** Python formateado con
`ruff format` (línea 100, comillas dobles, 4 espacios) y `ruff check` limpio; JSON con 2 espacios
(ningún gate formatea JSON); TS/TSX con 2 espacios y comillas dobles (sin formateador en el gate).
`.claude/commands/` no se emite.

### 19.1 `CLAUDE.md`

```markdown
# Radar ECOBICI CDMX — Instrucciones para Claude

Pipeline de datos en tiempo real del feed GBFS de ECOBICI CDMX + predicción de bicis disponibles
por estación en la próxima hora + API + mapa 3D. Proyecto escolar (IBERO, Ingeniería de Software,
entrega 13-nov-2026) y de portafolio/LinkedIn. Autor: Angel Isacc Cabrera Leal (GitHub:
Isacc-Cabrera). Licencia MIT.

## Comandos (Git Bash, desde la raíz del repo)

| Tarea | Comando |
|---|---|
| Instalar entorno Python | `uv sync --all-extras --all-groups` (Python 3.12 en `.python-version`) |
| Pruebas (todas / una) | `uv run pytest` · `uv run pytest tests/unit/test_runner.py` |
| Lint · formato · tipos | `uv run ruff check .` · `uv run ruff format --check .` · `uv run mypy` |
| Recolector (Docker) | `docker compose up -d collector` · una vuelta: `docker compose run --rm -e RUN_ONCE=true collector` |
| Kafka + productor + consumidor | `docker compose --profile streaming up -d --wait kafka collector-kafka bronze-writer` |
| Lake S3 (SeaweedFS) | `uv run python scripts/lake_setup.py render` y luego `docker compose --profile lake up -d --wait seaweedfs` |
| Pipeline de una hora | `uv run python scripts/run_hourly.py --hour auto --stage todo` |
| Airflow | `docker compose --profile orquestacion up -d --build --wait airflow` → http://localhost:8080 |
| MLflow | `docker compose --profile ml up -d --wait mlflow` → http://localhost:5000 |
| Entrenar | `uv run python -m radar.ml.train --json` |
| API | `docker compose --profile api up -d --build --wait api` → http://localhost:8000/docs |
| Frontend | `npm --prefix frontend install` · `npm --prefix frontend run dev` → http://localhost:5173 |
| Frontend gates | `npm --prefix frontend run typecheck` · `run test` · `run build` · `run smoke` |
| Terraform (sin costo) | `terraform -chdir=infra/terraform fmt -check -recursive` · `init -backend=false` · `validate` |

**Gate de cada paso:** `uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest`
pasa antes de marcar un paso como hecho. Versiones: `pyproject.toml` + `uv.lock`,
`frontend/package.json` + `package-lock.json`. Léelas ahí; nunca adivines una versión.
Orden de construcción: `blueprints/radar-ecobici-cdmx/tasks.json` y sus `epics/`.

**Push a GitHub (cuenta personal, nunca `gh auth switch`, nunca config global):**
`git -c credential.helper= -c 'credential.helper=!f() { echo username=x-access-token; echo "password=$(gh auth token -u IsaccCabrera09)"; }; f' push --follow-tags`
Anúncialo y espera el "sí" del usuario antes de cada push (es una acción hacia afuera).

## Cómo trabajamos

- Tú generas el código; yo lo reviso y lo tengo que poder explicar en la exposición (D-008).
- Antes de cada pieza nueva, explica en 3–6 líneas el concepto que usa, las alternativas
  principales y por qué se eligió esta. Cada epic trae un bloque "Explicación para la exposición".
- Aplica y NOMBRA patrones de diseño donde aporten; el docstring del módulo dice cuál usa.
- Al terminar cada paso: resumen de lo hecho, 2–3 preguntas de checkpoint para mí, y propuesta
  de entrada para `docs/bitacora.md` (formato: qué decidí · alternativas · por qué ·
  consecuencias) + recordatorio del post de LinkedIn si la etapa tiene uno. No marques un paso
  como terminado sin verificarlo (correr, probar, mirar). Skill: `cerrar-etapa`.
- Prioriza herramientas gratuitas. AVÍSAME ANTES de cualquier cosa que pueda generar costos
  (AWS, APIs de LLM de pago, servicios administrados). Nunca uses MSK ni MWAA.
- Commits pequeños en español estilo convencional: `feat(E1-T2): …`, `fix:`, `docs:`, `chore:`.
- Nunca hagas commit de `.env`, `data/`, `terraform.tfstate*`, `mlruns/`, `mlartifacts/`.
- Si tienes dudas, máximo 3 preguntas al usuario, juntas, con tu recomendación.

## Decisiones ya tomadas (respetarlas)

- **D-005:** `station_status` cada 60 s (ttl=10 permite más, pero el modelo predice a 1 h;
  10 s = 5.9 M filas/día vs ~1 M a 60 s). `station_information` al arrancar y una vez al día;
  cambio detectado con sha256 del JSON canónico de la lista de estaciones (ordenada por
  `station_id`, llaves ordenadas, SIN `last_updated`). `station_id` desconocido en status →
  guardar igual, registrar en log y refrescar `station_information`.
- **D-006:** guardar primero, validar después. bronze = respuesta cruda, append-only, nunca se
  modifica ni se borra. silver = validación de `station_id`, regla de capacidad, tipos y
  deduplicación por `(station_id, last_reported)`; las filas que fallan van a cuarentena.
  gold = agregados por hora y features. heartbeat por corrida: `ts_utc, feed, result
  (sin_cambios|guardado|error), http_status, n_stations, duration_ms, error, last_updated, sink`.
- **Fallas:** 4xx → log y no reintentar (429: respetar `Retry-After`); 5xx/timeouts/conexión →
  backoff exponencial con full jitter y tope de 300 s; el proceso nunca muere. `last_updated`
  congelado más de `STALE_ALERT_SECONDS` (600) → WARNING + heartbeat `error` con
  `error="feed_congelado"`.
- **Regla de calidad:** `num_bikes_available + num_bikes_disabled + num_docks_available +
  num_docks_disabled == capacity`, sobre todas las estaciones; se reporta la tasa de cumplimiento.
- **D-007:** licencia MIT. Variables en `.env` (ignorado) + `.env.example` versionado.
- **D-009:** SeaweedFS reemplaza a MinIO (repo de MinIO archivado el 2026-04-25).
- **D-010:** mapa base OpenFreeMap (sin token); CARTO descartado porque exige API key.

## Stack

Python 3.12 · uv · Docker Compose (perfiles) · Kafka 4 KRaft (sin ZooKeeper) · confluent-kafka ·
SeaweedFS (S3 local) · Parquet · DuckDB · polars/pandas · Airflow 3 · scikit-learn · LightGBM ·
MLflow · FastAPI + Pydantic · pytest · GitHub Actions · React 19 + Vite + deck.gl + MapLibre ·
Terraform (S3 + IAM + Budgets) · Open-Meteo · Ollama + chromadb (RAG opcional).

## Arquitectura — cómo se conecta

**Ruta de un dato:** feed GBFS → `radar/collector/runner.py` (cada 60 s) → `SnapshotSink`
(`LocalFileSink` o `KafkaSink`) → topic `station_status_raw` → `radar/streaming/consumer.py`
(grupo `bronze-writer`) → `data/bronze/...json.gz` → `radar/lake/bronze_to_silver.py` →
`data/silver/` → `radar/lake/silver_to_gold.py` → `data/gold/` → `radar/ml/train.py` (MLflow
`radar-bikes-t60@champion`) → `radar/api/main.py` → `frontend/` (deck.gl).

| Capa | Puede importar de | Nunca |
|---|---|---|
| `radar/collector` | `radar.config`, `radar.logging` | `lake`, `ml`, `api` |
| `radar/streaming` | `collector` (puerto `SnapshotSink`, `LocalFileSink`) | `lake`, `ml`, `api` |
| `radar/lake` | `config`, `logging`, `collector.hashing` | `ml`, `api` |
| `radar/ml` | `lake` (lectura) | `api`, `streaming` |
| `radar/api` | `ml`, `lake`, `anomalies` | `collector`, `streaming` |
| `scripts/`, `dags/` | cualquier `radar.*` | contener lógica que no esté en `radar.*` |

| Concepto | Fuente única |
|---|---|
| Configuración / env | `src/radar/config.py` (`Settings`, lee `.env`); nadie más lee `os.environ` |
| Rutas del lake | `Settings.data_dir` + el layout de `blueprint.md` §4 |
| Tokens de diseño | `frontend/src/styles/theme.css` y `frontend/src/lib/scales.ts` |
| Contrato de la API | `src/radar/api/schemas.py` |

## Reglas de código

1. **Encabezado obligatorio** en todo archivo fuente (py, ts/tsx, mjs, sql, Dockerfile, yaml,
   tf, sh): Autor, Descripción, Patrón, Fecha de creación, Cambios. Al editar, agrega una línea a
   `Cambios`. Formato exacto: `.claude/rules/encabezados.md`. `tests/unit/test_encabezados.py` lo exige.
2. Docstrings estilo Google, en español, en toda función y clase pública.
3. Imports absolutos `from radar.<módulo> import …`; nunca relativos, nunca `sys.path`.
4. Todo CLI llama `configure_logging(settings.log_level)` antes de loguear; logs a stderr,
   salida `--json` a stdout. Códigos de salida: 0 ok, 2 uso (argparse), 3 check fallido.
5. Toda transformación es idempotente: reprocesar la misma hora reemplaza su partición.
6. Ninguna prueba toca la red: respx para HTTP, fakes para Kafka/S3/Ollama.
7. Sin dependencias nuevas sin justificarlas en el commit; Airflow nunca entra al entorno uv.

## Sistema de diseño (detalle en `.claude/rules/frontend.md`)

Verde, temas oscuro/claro (default = `prefers-color-scheme`, persistido en `localStorage`
`radar-tema`). Primario oscuro `#22C55E` / claro `#15803D`; fondo `#07120D` / `#F2FAF5`; texto
`#E3F5EA` / `#0D2A1C`. Columnas: `#E5F5E0 → #A1D99B → #41AB5D → #006D2C`. Predicción: se vacía
`#F97316`, estable `#84CC16`, se llena `#06B6D4`; anomalía `#EF4444`. Fuente system UI,
números `tabular-nums`. WCAG 2.2 AA.

## Reglas diferidas (lee el archivo antes de editar esa área)

| Archivo | Aplica a |
|---|---|
| `.claude/rules/encabezados.md` | todo archivo fuente |
| `.claude/rules/python-pipeline.md` | `src/radar/**`, `scripts/**`, `dags/**` |
| `.claude/rules/pruebas.md` | `tests/**` |
| `.claude/rules/frontend.md` | `frontend/**` |
| `.claude/rules/infra-costos.md` | `infra/**`, `docker/**`, `docker-compose.yml`, `.github/**` |

## No negociable

1. Bronze es append-only: ningún código borra o reescribe un archivo bronze.
2. Los viajes crudos (con `Genero_Usuario`, `Edad_Usuario`, `Bici`) nunca salen de `data/bronze/trips/`
   local: ni a silver, ni a SeaweedFS, ni a AWS.
3. Nunca `terraform apply`, nunca un LLM de pago y nunca un push sin el "sí" explícito del usuario.
4. Nunca commit de secretos, `.env`, `data/`, `*.tfstate*`, `mlruns/`, `mlartifacts/`.
5. Nunca `gh auth switch` ni `git config --global`: identidad local del repo, siempre.
6. Nunca marques un paso como hecho con un gate fallando; nunca edites un comando Verify.
```

### 19.2 `AGENTS.md`

```markdown
# Radar ECOBICI CDMX — instrucciones para agentes

Pipeline en tiempo real del feed GBFS de ECOBICI CDMX (recolector → Kafka → lake bronze/silver/gold)
+ predicción de bicis disponibles a 60 min + API FastAPI + mapa 3D. Proyecto escolar y de
portafolio de Angel Isacc Cabrera Leal (MIT). Idioma del proyecto: español (es-MX).

## Comandos (Git Bash, desde la raíz)

| Tarea | Comando |
|---|---|
| Entorno | `uv sync --all-extras --all-groups` |
| Gate Python | `uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest` |
| Recolector | `docker compose up -d collector` |
| Streaming | `docker compose --profile streaming up -d --wait kafka collector-kafka bronze-writer` |
| API | `docker compose --profile api up -d --build --wait api` |
| Frontend | `npm --prefix frontend run typecheck && npm --prefix frontend run test && npm --prefix frontend run build` |
| Terraform | `terraform -chdir=infra/terraform validate` (nunca `apply` sin aprobación) |

## No negociable

1. Bronze es append-only: ningún código borra o reescribe un archivo bronze.
2. Los viajes crudos con PII nunca salen de `data/bronze/trips/` local.
3. Nunca `terraform apply`, LLM de pago ni `git push` sin el "sí" explícito del usuario.
4. Nunca commit de secretos, `.env`, `data/`, `*.tfstate*`, `mlruns/`, `mlartifacts/`.
5. Nunca `gh auth switch` ni `git config --global`.
6. Todo archivo fuente lleva el encabezado obligatorio (Autor, Descripción, Patrón, Fecha, Cambios).

Arquitectura, límites entre capas, decisiones D-005…D-010 y sistema de diseño: `CLAUDE.md` en este
directorio (fuente de verdad). Orden de construcción: `blueprints/radar-ecobici-cdmx/tasks.json`.
```

### 19.3 `.claude/settings.json`

Cada prefijo de los `Verify` de §9 y del gate §20.1 está en `allow` (`uv run`, `uv sync`,
`docker compose`, `docker build`, `docker inspect`, `git check-ignore`, `git log`, `git ls-files`,
`git clone --no-local`, `npm --prefix frontend`, `terraform -chdir=infra/terraform fmt|init -backend=false|validate`,
`curl -sf localhost`, `curl -s -o /dev/null -w`, `DEMO_PAUSE_SECONDS=120 bash scripts/demo_recovery.sh`,
`bash -n`, `ollama list`, `grep`, `test`, `head`, `echo`, `H=`, `out=`, `rc=`, `MSYS_NO_PATHCONV=1 docker compose`,
`cd`, `cp`, `sleep`, `[ -e`, `find . -type f`, `while IFS= read -r f`). Push, `gh`,
`terraform apply/destroy`, `aws`, el sync a AWS y el rollback `git reset --hard step-…` están en `ask`.

```json
{
  "permissions": {
    "allow": [
      "Bash(uv run:*)",
      "Bash(uv sync:*)",
      "Bash(uv lock:*)",
      "Bash(uv python install:*)",
      "Bash(uv python find:*)",
      "Bash(uv --version)",
      "Bash(docker compose:*)",
      "Bash(MSYS_NO_PATHCONV=1 docker compose:*)",
      "Bash(docker build:*)",
      "Bash(docker inspect:*)",
      "Bash(docker ps:*)",
      "Bash(docker logs:*)",
      "Bash(docker --version)",
      "Bash(git status:*)",
      "Bash(git log:*)",
      "Bash(git diff:*)",
      "Bash(git show:*)",
      "Bash(git check-ignore:*)",
      "Bash(git ls-files:*)",
      "Bash(git tag:*)",
      "Bash(git add:*)",
      "Bash(git commit:*)",
      "Bash(git rev-parse:*)",
      "Bash(git init:*)",
      "Bash(git clone --no-local:*)",
      "Bash(git config user.name:*)",
      "Bash(git config user.email:*)",
      "Bash(npm --prefix frontend:*)",
      "Bash(node --version)",
      "Bash(terraform version)",
      "Bash(terraform -chdir=infra/terraform fmt:*)",
      "Bash(terraform -chdir=infra/terraform init -backend=false:*)",
      "Bash(terraform -chdir=infra/terraform validate:*)",
      "Bash(terraform -chdir=infra/terraform plan:*)",
      "Bash(curl -sf localhost:*)",
      "Bash(curl -s -o /dev/null -w:*)",
      "Bash(DEMO_PAUSE_SECONDS=120 bash scripts/demo_recovery.sh)",
      "Bash(bash scripts/demo_recovery.sh)",
      "Bash(bash -n:*)",
      "Bash(ollama list)",
      "Bash(ollama pull llama3.2:3b)",
      "Bash(ollama pull nomic-embed-text)",
      "Bash(grep:*)",
      "Bash(head:*)",
      "Bash(test:*)",
      "Bash(mkdir -p:*)",
      "Bash(rm -rf data/_verify:*)",
      "Bash(ls:*)",
      "Bash(cat:*)",
      "Bash(find . -type f:*)",
      "Bash(while IFS= read -r f:*)",
      "Bash(cp:*)",
      "Bash(cd:*)",
      "Bash(sleep:*)",
      "Bash([ -e:*)",
      "Bash(echo:*)",
      "Bash(H=:*)",
      "Bash(out=:*)",
      "Bash(rc=:*)"
    ],
    "ask": [
      "Bash(git push:*)",
      "Bash(git -c:*)",
      "Bash(gh:*)",
      "Bash(GH_TOKEN=:*)",
      "Bash(terraform apply:*)",
      "Bash(terraform destroy:*)",
      "Bash(terraform -chdir=infra/terraform apply:*)",
      "Bash(terraform -chdir=infra/terraform destroy:*)",
      "Bash(aws:*)",
      "Bash(bash scripts/sync_gold_to_s3.sh:*)",
      "Bash(docker compose down -v:*)",
      "Bash(git reset --hard step-:*)"
    ],
    "deny": [
      "Read(./.env)",
      "Read(./docker/seaweedfs/s3.json)",
      "Read(./infra/terraform/terraform.tfvars)",
      "Bash(git push --force:*)",
      "Bash(git push -f:*)",
      "Bash(gh auth switch:*)",
      "Bash(git config --global:*)",
      "Bash(rm -rf data/bronze:*)",
      "Bash(rm -rf data/silver:*)",
      "Bash(rm -rf data/gold:*)",
      "Bash(rm -rf data)",
      "Bash(rm -rf data/)"
    ]
  }
}
```

### 19.4 Project skills — `.claude/skills/<name>/SKILL.md`

| Skill | Triggers on | What it automates |
|---|---|---|
| `cerrar-etapa` | "terminé el paso", "cierra la etapa", "checkpoint", "siguiente paso" | verify + gate + done en `tasks.json` + commit/tag + resumen, preguntas, bitácora, LinkedIn, push con aprobación |
| `nuevo-modulo-python` | "crea el módulo", "nuevo script", "nueva prueba" | encabezado, patrón nombrado, Settings/logging, CLI 0/2/3, prueba sin red |

````markdown
---
name: cerrar-etapa
description: Cierra un paso o etapa de Radar ECOBICI - corre el gate, hace commit + tag del checkpoint, y entrega resumen, preguntas de checkpoint, entrada de bitácora y recordatorio de LinkedIn. Úsala cuando digas "terminé el paso", "cierra la etapa", "checkpoint" o "siguiente paso".
---

# Cerrar etapa

## Cuándo usarla
- Al terminar los comandos `Verify` de un paso de `blueprints/radar-ecobici-cdmx/tasks.json`.
- Cuando el usuario dice "cierra la etapa", "checkpoint", "ya quedó", "siguiente paso".

## Pasos
1. Corre todos los comandos `verify` del task en `tasks.json`, en orden, desde la raíz. Si uno
   falla, no sigas: arréglalo (nunca edites el comando).
2. Corre el gate: `uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest`.
3. Marca el task `done` en `blueprints/radar-ecobici-cdmx/tasks.json` (así el commit del
   checkpoint incluye el estado).
4. Ejecuta el bloque `Checkpoint` del epic tal cual (commit convencional en español con el id
   del task y `git tag step-NN-<slug>`), incluidas sus aserciones posteriores al commit.
5. Entrega al usuario, en este orden:
   - Resumen (5–8 viñetas) de lo construido y de los patrones de diseño usados.
   - Las 2–3 preguntas de checkpoint del epic ("Preguntas de checkpoint").
   - Propuesta de entrada para `docs/bitacora.md`: **qué decidí · alternativas · por qué ·
     consecuencias**. No la pegues tú: el usuario la revisa y la pega.
   - Recordatorio del post de LinkedIn si el epic lo indica para esta etapa.
6. Pregunta si hacer push (acción hacia afuera). Solo con un "sí" explícito corre:
   `git -c credential.helper= -c 'credential.helper=!f() { echo username=x-access-token; echo "password=$(gh auth token -u IsaccCabrera09)"; }; f' push --follow-tags`

## Verify
```bash
git tag -l 'step-*'          # expect: incluye el tag del paso recién cerrado
test -z "$(git status --porcelain)"   # expect: exit 0, árbol limpio después del commit
```

## No hagas
- No marques `done` con un gate rojo ni saltes un comando `verify`.
- No hagas push, `gh auth switch` ni `git config --global`.
- No escribas en `docs/bitacora.md` sin que el usuario apruebe el texto.
````

````markdown
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
````

### 19.5 `.claude/rules/*.md`

| File | `paths` globs | Covers |
|---|---|---|
| `.claude/rules/encabezados.md` | `src/**`, `tests/**`, `scripts/**`, `dags/**`, `sql/**`, `docker/**`, `infra/**`, `frontend/src/**`, `frontend/scripts/**`, `frontend/vite.config.ts`, `docker-compose.yml`, `.github/**` | encabezado obligatorio y docstrings |
| `.claude/rules/python-pipeline.md` | `src/radar/**`, `scripts/**`, `dags/**` | Settings, CLI, rutas del lake, idempotencia, patrones |
| `.claude/rules/pruebas.md` | `tests/**` | pruebas sin red, fixtures, 0 skipped |
| `.claude/rules/frontend.md` | `frontend/**` | tokens, colores, tema, cámara, accesibilidad |
| `.claude/rules/infra-costos.md` | `infra/**`, `docker/**`, `docker-compose.yml`, `.github/**` | imágenes fijadas, Dockerfiles, CI, costos |

````markdown
---
description: Encabezado obligatorio de autor en todo archivo fuente
paths:
  - "src/**"
  - "tests/**"
  - "scripts/**"
  - "dags/**"
  - "sql/**"
  - "docker/**"
  - "infra/**"
  - "frontend/src/**"
  - "frontend/scripts/**"
  - "frontend/vite.config.ts"
  - "docker-compose.yml"
  - ".github/**"
---

# Encabezado obligatorio

Todo archivo `.py`, `.ts`, `.tsx`, `.mjs`, `.sql`, `*.Dockerfile`, `.yml`/`.yaml`, `.tf` y `.sh`
empieza con este encabezado. `tests/unit/test_encabezados.py` revisa las primeras 25 líneas y
falla si falta `Autor: Isacc Cabrera`, `Descripción:`, `Patrón:`, `Fecha de creación:` o `Cambios:`.

Python (docstring de módulo; líneas de 100 caracteres como máximo):

```python
"""
Autor: Isacc Cabrera
Descripción: <qué hace este módulo y por qué existe>
Patrón: <patrón de diseño aplicado o "N/A">
Fecha de creación: <YYYY-MM-DD>
Cambios:
    - <YYYY-MM-DD> · Isacc Cabrera · <qué se hizo>
"""
```

TypeScript / TSX / MJS:

```ts
/**
 * Autor: Isacc Cabrera
 * Descripción: <qué hace>
 * Patrón: <patrón o "N/A">
 * Fecha de creación: <YYYY-MM-DD>
 * Cambios:
 *     - <YYYY-MM-DD> · Isacc Cabrera · <qué se hizo>
 */
```

SQL: mismas cinco líneas con prefijo `-- `. Dockerfile, YAML, Terraform y shell: prefijo `# `
(en `.sh` el encabezado va DESPUÉS de la línea `#!/usr/bin/env bash`).

- Al editar un archivo existente, agrega una línea nueva al final de `Cambios`; nunca borres las previas.
- `Patrón:` nombra el patrón real (Ports & Adapters, Factory, Strategy, Chain of Responsibility,
  Repository, Dependency Injection, Template Method, Singleton, Container/Presentational,
  Custom Hook) o `N/A`. No inventes un patrón para llenar el campo.
- Toda función y clase pública lleva docstring estilo Google en español (`Args:`, `Returns:`,
  `Raises:`).
- Los JSON y Markdown no llevan encabezado (JSON no admite comentarios).
````

```markdown
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
```

```markdown
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
```

```markdown
---
description: Convenciones del mapa 3D (React + Vite + deck.gl + MapLibre)
paths:
  - "frontend/**"
---

# Frontend

- Contenedor/Presentacional: `App.tsx` (contenedor) usa los hooks `useStations` y `useTheme` de
  `src/hooks.ts`; `StationMap.tsx` y `Panel.tsx` solo reciben props.
- Colores, umbrales, mapas base y atribución SOLO desde `src/lib/scales.ts` y variables CSS de
  `src/styles/theme.css`. Ningún componente escribe un hex literal.
- Tokens (oscuro / claro): bg `#07120D`/`#F2FAF5`, surface `#0E1F17`/`#FFFFFF`, surface-2
  `#14291F`/`#E7F4EC`, border `#1F3D2E`/`#C9E4D3`, text `#E3F5EA`/`#0D2A1C`, text-muted
  `#8DB8A1`/`#4A6B5A`, primary `#22C55E`/`#15803D`, primary-strong `#16A34A`/`#166534`, accent
  `#A3E635`/`#65A30D`, focus `#4ADE80`/`#16A34A`, destructive `#F87171`/`#B91C1C`.
- Columnas: altura = bicis disponibles (`elevationScale` 40), color por rampa
  `#E5F5E0 → #A1D99B → #41AB5D → #006D2C`. Predicción (delta = pred − actual): ≤ −3 se vacía
  `#F97316` ▼, estable `#84CC16` ●, ≥ +3 se llena `#06B6D4` ▲. Anomalía `#EF4444` ⚠.
- Tema: default `prefers-color-scheme`, persistido en `localStorage["radar-tema"]`; el mapa base
  cambia con el tema (OpenFreeMap `dark` / `positron`). Atribución visible siempre:
  "OpenFreeMap © OpenMapTiles Data from OpenStreetMap".
- Cámara: `pitch` 50°, transición con `FlyToInterpolator` de 1200 ms; con
  `prefers-reduced-motion: reduce` la duración es 0.
- Polling cada 60 s a `VITE_API_URL` (default `http://localhost:8000`); sin WebSockets (no-goal).
- Accesibilidad: el botón de tema y la lista de estaciones son operables con teclado; foco visible
  con `--focus`; la leyenda usa texto + ícono, nunca solo color; objetivos de 24×24 px mínimo;
  `aria-live="polite"` en la hora de última actualización.
- Imports relativos sin extensión (`./components/Panel`); `moduleResolution: "bundler"`.
- Estados de carga, vacío y error visibles en el panel (texto, no solo spinner).
```

```markdown
---
description: Docker, CI y Terraform — imágenes fijadas y control de costos
paths:
  - "infra/**"
  - "docker/**"
  - "docker-compose.yml"
  - ".github/**"
---

# Infraestructura y costos

- Imágenes fijadas, nunca `latest`: `python:3.12-slim`, `apache/kafka:4.4.0`,
  `chrislusf/seaweedfs:4.53`, `apache/airflow:3.4.0-python3.12`, `postgres:17-alpine`,
  `ghcr.io/mlflow/mlflow:v3.18.0`. uv dentro de imágenes: `pip install --no-cache-dir uv==0.13.7`.
- Dockerfiles: usuario no root (`radar`), primero `uv sync --frozen --no-dev --no-install-project`
  (capa cacheable) y luego `COPY src` + `uv sync --frozen --no-dev`. La API necesita `libgomp1`
  (LightGBM).
- Compose: nombra siempre el servicio al levantarlo; el recolector viejo (`collector`) queda
  detenido después de la migración del paso 5. Nunca `docker compose down -v` (borra volúmenes).
- `docker/seaweedfs/s3.json` se genera con `scripts/lake_setup.py render` ANTES de levantar
  SeaweedFS (si no existe, Docker crea un directorio con ese nombre); está en `.gitignore`.
- CI (GitHub Actions): `actions/checkout@v7`, `astral-sh/setup-uv@v10` con `version: "0.13.7"`,
  `actions/setup-python@v7`, `docker/setup-buildx-action@v4`, `docker/build-push-action@v7` con
  `push: false`. CI no usa secretos ni red hacia AWS.
- Terraform: `required_version = "~> 1.17"`, `hashicorp/aws ~> 6.72`, state local ignorado.
  **Antes de cualquier `apply`: lista los recursos, su costo estimado y espera el "sí" del
  usuario.** Después de la demo: `terraform destroy` (instrucciones en `docs/nube.md`).
- Nunca MSK ni MWAA; nunca crear llaves de acceso con Terraform (quedarían en el state).
```

### 19.6 Verify-critical config and local infrastructure

Todos estos archivos están en `workspace/` con su contenido completo (abajo) y los coloca el
Bootstrap antes del paso 1.

| File | Path in the project | Which `Verify` commands need it | Resolution/env handling it carries | Bundle-path exclusion |
|---|---|---|---|---|
| Manifiesto Python + config de ruff/mypy/pytest | `pyproject.toml` | todos los `uv run …` (pasos 1–18) | `module-name = "radar"`, `module-root = "src"` (uv_build); pytest `pythonpath = ["src"]`; mypy `mypy_path = "src"` | ruff `extend-exclude = ["blueprints", …]`; mypy `exclude = ["^blueprints/", …]`; pytest `norecursedirs = ["blueprints", …]` |
| Versión de Python | `.python-version` | todos los `uv` | `3.12` | n/a — uv no recorre el árbol |
| Ignore de git | `.gitignore` | 1, 6, 16 (`git check-ignore`) y §20.1 | `!.env.example` | no ignora `blueprints/` a propósito (se versiona) |
| Finales de línea | `.gitattributes` | 3, 5, 9, 13 (scripts y Dockerfiles en Linux) | `eol=lf` | n/a |
| Contexto de Docker | `.dockerignore` | 3, 4, 5, 9, 13 (`docker build`/`compose build`) | excluye `.env`, `data/` | `blueprints/` |
| Servicios locales | `docker-compose.yml` | 3–9, 11, 13, 15 | `env_file: [.env]` + `environment` por servicio; healthchecks para `--wait`; imágenes fijadas | n/a — Compose no recorre el árbol (contexto controlado por `.dockerignore`) |
| Formato de identidades S3 | `docker/seaweedfs/s3.json.example` | 6 (`lake_setup.py render` lo usa como plantilla) | sin secretos | n/a |
| Fixtures de pytest | `tests/conftest.py` | todos los `uv run pytest` | fixture `settings` con `_env_file=None` (las pruebas no leen el `.env` real) | `norecursedirs` de `pyproject.toml` |
| Prueba de encabezados | `tests/unit/test_encabezados.py` | todos los pasos que corren `uv run pytest tests/unit/test_encabezados.py` | recorre solo carpetas fuente desde la raíz | nunca entra a `blueprints/` (lista explícita de carpetas) |
| Respuestas grabadas | `tests/fixtures/{gbfs,station_status,station_information}.json` | 2, 3, 4, 7 | — | n/a |
| Paquete y logging | `src/radar/__init__.py`, `src/radar/*/__init__.py`, `src/radar/logging.py` | 2+ (`import radar…`, `uv run mypy`) | logs a stderr (stdout limpio para `--json`) | n/a |
| Manifiesto npm | `frontend/package.json` | 14, 15, §20.1 | versiones exactas; scripts `typecheck`, `test`, `build`, `smoke` | n/a — npm corre con `--prefix frontend` |
| Config TS | `frontend/tsconfig.json` | 14, 15 (`typecheck`) | `moduleResolution: "bundler"`, `types: ["vite/client"]` | `include: ["src"]` (nunca ve `blueprints/`) |
| Config Vite/Vitest | `frontend/vite.config.ts` | 14, 15 (`test`, `build`, `smoke`) | `server.port 5173`, `preview.port 4173`, Vitest `environment: "node"` | raíz `frontend/`; `include: ["src/**/*.test.ts"]` |
| HTML de entrada | `frontend/index.html` | 14 (`build`, `smoke` busca `id="root"`) | tema antes del primer pintado | n/a |
| Entrada React y tokens | `frontend/src/main.tsx`, `frontend/src/styles/theme.css`, `frontend/src/lib/scales.ts` | 14 | importa `./App` (lo escribe el paso 14) | n/a |
| Prueba de tokens | `frontend/src/lib/scales.test.ts` | 14 (`npm --prefix frontend run test`) | — | n/a |
| Smoke del build | `frontend/scripts/smoke.mjs` | 14, §20.1 (`npm --prefix frontend run smoke`) | resuelve `vite` desde `frontend/node_modules` (npm corre el script con cwd `frontend/`) | n/a |

Servicios que los gates necesitan (cuatro partes): (1) definidos en `docker-compose.yml` con tags
fijados y healthcheck; (2) variables `KAFKA_BOOTSTRAP`, `S3_ENDPOINT_URL`, `MLFLOW_TRACKING_URI`
con su valor local en §10 y `.env.example`; (3) comandos `up`/`stop` en cada paso y en la tabla de
comandos de `CLAUDE.md` (bajar: `docker compose --profile <p> stop <servicio>`; nunca `down -v`);
(4) `docker compose` en `permissions.allow`.

#### Resolution convention matrix

**La convención, una sola vez:** Python importa siempre en forma absoluta `from radar.<módulo>
import <nombre>` (paquete `radar` en `src/radar`, instalado por uv); TypeScript importa con rutas
relativas sin extensión (`./components/Panel`) y paquetes por nombre.

| Context | Command that exercises it | Convention as it appears there | Config + literal setting that makes it work |
|---|---|---|---|
| Código de aplicación (contenedores) | `docker compose run --rm -e RUN_ONCE=true collector` / `python -m radar.collector.runner` | `from radar.collector.retry import …` | `docker/collector.Dockerfile` — `uv sync --frozen --no-dev --no-editable --extra streaming --extra lake` instala `radar` en `/app/.venv` |
| Pruebas | `uv run pytest` | `from radar.config import Settings` | `pyproject.toml` — `[tool.pytest.ini_options] pythonpath = ["src"]` + instalación editable de `uv sync` |
| Scripts sueltos | `uv run python scripts/lake_setup.py render` | `from radar.lake.store import make_lake_store` | `pyproject.toml` — `[build-system] uv_build` + `[tool.uv.build-backend] module-name = "radar"`: `uv sync` instala el paquete editable en `.venv` |
| Tipos | `uv run mypy` | absolutos | `pyproject.toml` — `[tool.mypy] files = ["src/radar"]`, `mypy_path = "src"`, `explicit_package_bases = true` |
| Airflow (tareas) | `airflow dags test horario …` → `/opt/radar/.venv/bin/python /opt/radar/scripts/run_hourly.py` | absolutos | `docker/airflow.Dockerfile` — `uv sync --frozen --no-dev --no-editable --extra lake` en `/opt/radar` |
| API (contenedor) | `docker compose --profile api up -d --build --wait api` | absolutos | `docker/api.Dockerfile` — `uv sync … --extra api --extra lake --extra ml` |
| Notebook | `uv run jupyter nbconvert --execute notebooks/01_eda.ipynb` | `from radar.ml.features import …` | kernel del `.venv` (instalación editable); rutas de datos relativas a la raíz (`Path.cwd().parent`) |
| Frontend (app y build) | `npm --prefix frontend run build` | `import App from "./App"` | `frontend/tsconfig.json` — `"moduleResolution": "bundler"`; Vite resuelve sin extensión |
| Frontend (pruebas) | `npm --prefix frontend run test` | `from "./scales"` | `frontend/vite.config.ts` — Vitest usa el resolvedor de Vite |
| Frontend (smoke) | `npm --prefix frontend run smoke` | `import { preview } from "vite"` | paquete por nombre desde `frontend/node_modules`; ESM por `"type": "module"` |

#### Cross-artifact value reconciliation

| Shared value | Single source — the file that decides it | Literal value | Every other place it appears | Compared |
|---|---|---|---|---|
| Paquete Python | `pyproject.toml` — `module-name` | `radar` | `src/radar/`, todos los `-m radar.…` de §9, Dockerfiles (CMD), `dags/*.py` (vía scripts), `.claude/rules/python-pipeline.md` | yes |
| Imagen del recolector | `docker-compose.yml` — `services.collector.image` | `radar-ecobici/collector:local` | `collector-kafka.image`, `bronze-writer.image`, paso 13 `docker build -t`, §20.1 | yes |
| Imagen de la API | `docker-compose.yml` — `services.api.image` | `radar-ecobici/api:local` | paso 13 `docker build -t`, §20.1 | yes |
| Puerto de la API | `docker/api.Dockerfile` — CMD `--port` | `8000` | compose `ports "8000:8000"` y healthcheck, `VITE_API_URL` default en `hooks.ts`, curls de §9 (13) y §20.1, `docs/backend.md` | yes |
| Puerto del mapa | `frontend/vite.config.ts` — `server.port` | `5173` | `API_CORS_ORIGINS` en `.env.example`, criterio 6 del paso 12, `CLAUDE.md` | yes |
| Puerto del preview | `frontend/vite.config.ts` — `preview.port` | `4173` | `frontend/scripts/smoke.mjs` | yes |
| Kafka desde el host | `docker-compose.yml` — `KAFKA_ADVERTISED_LISTENERS` `EXTERNAL://localhost:9094` | `localhost:9094` | `.env.example` `KAFKA_BOOTSTRAP`, compose `ports "9094:9094"` | yes |
| Kafka dentro de Compose | `docker-compose.yml` — `PLAINTEXT://kafka:9092` | `kafka:9092` | `collector-kafka.environment` y `bronze-writer.environment` `KAFKA_BOOTSTRAP`, healthcheck `localhost:9092` (dentro del contenedor) | yes |
| Topics y grupo | `.env.example` | `station_status`, `station_status_raw`, `bronze-writer` | `Settings` defaults (paso 2), §5, `docs/kafka.md` | yes |
| Endpoint S3 | `docker-compose.yml` — `seaweedfs` `-s3.port=8333` | `8333` | `.env.example` `S3_ENDPOINT_URL=http://localhost:8333`, compose `airflow` `http://seaweedfs:8333`, `ports "8333:8333"` | yes |
| Buckets | `.env.example` `LAKE_BUCKET_*` | `radar-bronze`, `radar-silver`, `radar-gold` | `docker/seaweedfs/s3.json.example` (acciones `Read:radar-bronze`…), `lake_setup.py`, criterio 4 del paso 6 | yes |
| Config de SeaweedFS | `docker-compose.yml` — bind `./docker/seaweedfs/s3.json` | `docker/seaweedfs/s3.json` | `.gitignore` (línea exacta), `lake_setup.py render`, verify del paso 6, `.claude/settings.json` deny | yes |
| MLflow | `docker-compose.yml` — `--port=5000` | `5000` | `.env.example` `MLFLOW_TRACKING_URI=http://localhost:5000`, compose `api` `http://mlflow:5000`, verify del paso 11 | yes |
| Nombre del modelo | `.env.example` `MODEL_NAME` | `radar-bikes-t60` | `train.py`/`deps.py` (vía Settings), verify del paso 11 (`get_model_version_by_alias('radar-bikes-t60', 'champion')`), §5 | yes |
| Respaldo del modelo | `.env.example` `MODEL_FALLBACK_PATH` | `models/champion.joblib` | compose `api` `/app/models/champion.joblib` + bind `./models:/app/models`, verify del paso 11, `.gitignore` `models/` | yes |
| Lake en contenedores | `docker-compose.yml` — `DATA_DIR` | `/app/data` (collector, api), `/opt/radar/data` (airflow) | binds `./data:/app/data`, `./data:/opt/radar/data`, verify del paso 9 (`/opt/radar/data/_verify/hora.txt`) | yes |
| Python | `.python-version` | `3.12` | `pyproject.toml` `requires-python = ">=3.12,<3.13"`, `python:3.12-slim`, `apache/airflow:3.4.0-python3.12`, CI `python-version: "3.12"`, Bootstrap `uv python install 3.12` | yes |
| uv en imágenes y CI | §11 | `0.13.7` | los tres Dockerfiles (`uv==0.13.7`), CI `version: "0.13.7"`, verify del paso 13 | yes |
| Llave de tema | `frontend/src/lib/scales.ts` — `THEME_STORAGE_KEY` | `radar-tema` | `frontend/index.html` (script en línea), `CLAUDE.md`, `.claude/rules/frontend.md` | yes |
| Ruta del bundle | layout de The Architect | `blueprints/` | ruff `extend-exclude`, mypy `exclude`, pytest `norecursedirs`, `.dockerignore`, Bootstrap `WS=blueprints/radar-ecobici-cdmx/workspace` | yes |
| Repo de GitHub | decisión del usuario | `Isacc-Cabrera/radar-ecobici-cdmx` | checkpoint del paso 1, badge del paso 13, README del paso 18, User-Agent del paso 2 | yes |

Contratos entre artefactos y el paso más temprano que los ejercita: paquete `radar` ↔ imágenes
(paso 3, `docker compose run … collector`); Kafka host/compose (paso 4, `--ensure-topics` desde el
host y `run collector-kafka` desde Compose); buckets ↔ identidades (paso 6, `check-policy`);
MLflow puerto ↔ verify (paso 11); API puerto ↔ healthcheck ↔ curl (paso 13); preview ↔ smoke
(paso 14).

#### Byte-exact artifact reconciliation

NOT APPLICABLE — this blueprint authors no byte-exact expected output. Las comparaciones exactas
son corrida-contra-corrida (manifiestos con `--check-idempotent`, sincronizaciones dobles) o
propiedades; los fixtures de `tests/fixtures/` son **entradas**, no salidas esperadas, y la única
cadena literal que una prueba compara (`No sé: no encontré evidencia en la documentación del
proyecto.`, paso 17) la produce el propio código del proyecto, no el runtime.

#### Contenido de los archivos emitidos

**`.dockerignore`**

```gitignore
# Contexto de build de Docker: solo pyproject.toml, uv.lock, .python-version, src/ y scripts/ importan.
# Excluir data/ es obligatorio: el lake local pesa GB y contiene datos crudos de viajes (PII).
blueprints/
data/
models/
mlruns/
mlartifacts/
chroma/
.venv/
.git/
.env
.env.*
frontend/
notebooks/
infra/
docs/
tests/
**/__pycache__/
.ruff_cache/
.mypy_cache/
.pytest_cache/
```

**`.env.example`**

```bash
# Radar ECOBICI CDMX — variables de entorno (D-007: este archivo SÍ se versiona; .env NO).
# Copia: `cp .env.example .env` (el Bootstrap lo hace). Los valores vacíos son secretos que se
# generan o se obtienen en el paso indicado; nunca escribas un secreto real en este archivo.
# Sin espacios alrededor de "=" y sin comillas: lo leen pydantic-settings, Docker Compose y bash.

# --- Recolector (pasos 2-3) ---
GBFS_URL=https://gbfs.mex.lyftbikes.com/gbfs/gbfs.json
GBFS_LANG=es
HTTP_TIMEOUT_SECONDS=10
RETRY_BASE_SECONDS=2
RETRY_CAP_SECONDS=300
STATUS_INTERVAL_SECONDS=60
INFO_REFRESH_HOURS=24
STALE_ALERT_SECONDS=600
DATA_DIR=data
SINK=local
RUN_ONCE=false
LOG_LEVEL=INFO

# --- Kafka (pasos 4-5) ---
KAFKA_BOOTSTRAP=localhost:9094
KAFKA_TOPIC_STATUS=station_status
KAFKA_TOPIC_RAW=station_status_raw
KAFKA_GROUP_ID=bronze-writer
# Documental: scripts/demo_recovery.sh lo lee del shell (default 300); el Verify del paso 5 usa 120.
DEMO_PAUSE_SECONDS=300

# --- Data lake S3 local con SeaweedFS (paso 6) ---
S3_ENDPOINT_URL=http://localhost:8333
S3_REGION=us-east-1
S3_ADMIN_ACCESS_KEY=
S3_ADMIN_SECRET_KEY=
S3_APP_ACCESS_KEY=
S3_APP_SECRET_KEY=
LAKE_BUCKET_BRONZE=radar-bronze
LAKE_BUCKET_SILVER=radar-silver
LAKE_BUCKET_GOLD=radar-gold
LAKE_SYNC_S3=false

# --- Viajes históricos (paso 8) — la URL cambia de formato cada mes: verifícala con --probe ---
TRIPS_CSV_URL=https://ecobici.cdmx.gob.mx/wp-content/uploads/2026/10/public_data_web_2026-10.csv

# --- Orquestación y calidad (paso 9) ---
WEATHER_LAT=19.4326
WEATHER_LON=-99.1332
QUALITY_MAX_GAP_MINUTES=10
QUALITY_MAX_CAPACITY_FAIL_PCT=5
POSTGRES_AIRFLOW_PASSWORD=airflow-local

# --- Modelo y MLflow (paso 11) ---
MLFLOW_TRACKING_URI=http://localhost:5000
MODEL_NAME=radar-bikes-t60
MODEL_FALLBACK_PATH=models/champion.joblib

# --- API (paso 12) ---
API_CORS_ORIGINS=http://localhost:5173

# --- Anomalías (paso 15) ---
ANOMALY_FROZEN_MINUTES=90
ANOMALY_ZSCORE=3

# --- AWS (paso 16; SOLO tras aprobación explícita por costo) ---
AWS_PROFILE=radar-ecobici
AWS_S3_BUCKET=

# --- RAG local con Ollama (paso 17, opcional) ---
OLLAMA_URL=http://localhost:11434
OLLAMA_CHAT_MODEL=llama3.2:3b
OLLAMA_EMBED_MODEL=nomic-embed-text
```

**`.gitattributes`**

```gitignore
# Finales de línea LF en todo el repo: los scripts .sh y los Dockerfile se ejecutan en Linux
# (contenedores) aunque el repo viva en Windows; un CRLF rompe `bash` con "$'\r': command not found".
* text=auto eol=lf
*.sh text eol=lf
*.Dockerfile text eol=lf
*.parquet binary
*.gz binary
*.png binary
*.gif binary
*.joblib binary
```

**`.gitignore`**

```gitignore
# Radar ECOBICI CDMX — .gitignore
# Debe existir ANTES del primer `git add` (lo coloca el Bootstrap del blueprint, §10).
# Una ruta ya rastreada por git nunca vuelve a ser ignorada: por eso el orden importa.

# --- Secretos y configuración local ---
.env
.env.local
.env.*.local
!.env.example
docker/seaweedfs/s3.json
infra/terraform/terraform.tfvars

# --- Datos (D-006: bronze/silver/gold viven en data/, nunca en git) ---
data/
*.parquet
models/
chroma/

# --- MLflow ---
mlruns/
mlartifacts/

# --- Terraform (el state local NUNCA se sube) ---
*.tfstate
*.tfstate.*
.terraform/
crash.log

# --- Python ---
__pycache__/
*.py[cod]
*.egg-info/
.venv/
build/
.ruff_cache/
.mypy_cache/
.pytest_cache/
.coverage
coverage.xml
htmlcov/
.ipynb_checkpoints/

# --- Node / frontend ---
node_modules/
dist/
frontend/.vite/

# --- Sistema operativo / editores ---
.DS_Store
Thumbs.db
.idea/
.vscode/

# NOTA: blueprints/ SÍ se versiona (es el diseño del proyecto).
```

**`.python-version`**

```
3.12
```

**`dags/README.md`**

```markdown
# dags/

DAGs de Airflow 3 (perfil `orquestacion`). Se montan en el contenedor en `/opt/airflow/dags`.

- `horario.py` — cada hora: bronze → silver → clima → gold → checks de calidad de la hora anterior.
- `diario.py` — cada día: refresco de `station_information` por hash.

Las tareas no contienen lógica: llaman a `scripts/run_hourly.py`, que también es el *fallback*
si Airflow no funciona en el time-box de 3 h (paso 9).
```

**`docker-compose.yml`**

```yaml
# Autor: Isacc Cabrera
# Descripción: Servicios locales de Radar ECOBICI CDMX con perfiles por etapa. Sin perfil arranca
#   solo el recolector (paso 3). Perfiles: streaming (Kafka KRaft + productor + bronze-writer),
#   lake (SeaweedFS S3), orquestacion (Airflow + Postgres), ml (MLflow), api (FastAPI).
#   Regla: nombra siempre el servicio (`docker compose --profile X up -d <servicio>`) para no
#   reencender por accidente el recolector viejo después de la migración del paso 5.
# Patrón: N/A (infraestructura declarativa)
# Fecha de creación: 2026-11-08
# Cambios:
#     - 2026-11-08 · Isacc Cabrera · Creación desde el blueprint con imágenes fijadas.

name: radar-ecobici

services:
  collector:
    build:
      context: .
      dockerfile: docker/collector.Dockerfile
    image: radar-ecobici/collector:local
    command: ["python", "-m", "radar.collector.runner"]
    env_file: [.env]
    environment:
      DATA_DIR: /app/data
      SINK: local
    volumes:
      - ./data:/app/data
    restart: unless-stopped

  kafka:
    profiles: [streaming]
    image: apache/kafka:4.4.0
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:9094
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,EXTERNAL://localhost:9094
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
    ports:
      - "9094:9094"
    healthcheck:
      test:
        - CMD-SHELL
        - /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null 2>&1
      interval: 10s
      timeout: 10s
      retries: 12
      start_period: 20s
    restart: unless-stopped

  collector-kafka:
    profiles: [streaming]
    build:
      context: .
      dockerfile: docker/collector.Dockerfile
    image: radar-ecobici/collector:local
    command: ["python", "-m", "radar.collector.runner"]
    env_file: [.env]
    environment:
      DATA_DIR: /app/data
      SINK: kafka
      KAFKA_BOOTSTRAP: kafka:9092
    volumes:
      - ./data:/app/data
    depends_on:
      kafka:
        condition: service_healthy
    restart: unless-stopped

  bronze-writer:
    profiles: [streaming]
    build:
      context: .
      dockerfile: docker/collector.Dockerfile
    image: radar-ecobici/collector:local
    command: ["python", "-m", "radar.streaming.consumer"]
    env_file: [.env]
    environment:
      DATA_DIR: /app/data
      KAFKA_BOOTSTRAP: kafka:9092
    volumes:
      - ./data:/app/data
    depends_on:
      kafka:
        condition: service_healthy
    restart: unless-stopped

  seaweedfs:
    profiles: [lake, orquestacion]
    image: chrislusf/seaweedfs:4.53
    command:
      - server
      - -dir=/data
      - -ip.bind=0.0.0.0
      - -s3
      - -s3.port=8333
      - -s3.config=/etc/seaweedfs/s3.json
      - -master.volumeSizeLimitMB=1024
    ports:
      - "8333:8333"
      - "9333:9333"
    volumes:
      - seaweed-data:/data
      - ./docker/seaweedfs/s3.json:/etc/seaweedfs/s3.json:ro
    healthcheck:
      test: ["CMD-SHELL", "wget -q -O /dev/null http://127.0.0.1:9333/cluster/status || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 12
      start_period: 15s
    restart: unless-stopped

  postgres-airflow:
    profiles: [orquestacion]
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: ${POSTGRES_AIRFLOW_PASSWORD:-airflow-local}
      POSTGRES_DB: airflow
    volumes:
      - airflow-db:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U airflow -d airflow"]
      interval: 10s
      timeout: 5s
      retries: 10
    restart: unless-stopped

  airflow:
    profiles: [orquestacion]
    build:
      context: .
      dockerfile: docker/airflow.Dockerfile
    image: radar-ecobici/airflow:local
    command: ["standalone"]
    env_file: [.env]
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:${POSTGRES_AIRFLOW_PASSWORD:-airflow-local}@postgres-airflow:5432/airflow
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS: "true"
      DATA_DIR: /opt/radar/data
      S3_ENDPOINT_URL: http://seaweedfs:8333
    volumes:
      - ./dags:/opt/airflow/dags
      - ./data:/opt/radar/data
    ports:
      - "8080:8080"
    depends_on:
      postgres-airflow:
        condition: service_healthy
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://localhost:8080/api/v2/monitor/health || exit 1"]
      interval: 30s
      timeout: 10s
      retries: 10
      start_period: 120s
    restart: unless-stopped

  mlflow:
    profiles: [ml, api]
    image: ghcr.io/mlflow/mlflow:v3.18.0
    command:
      - mlflow
      - server
      - --host=0.0.0.0
      - --port=5000
      - --backend-store-uri=sqlite:////mlflow/mlflow.db
      - --artifacts-destination=/mlflow/artifacts
      - --serve-artifacts
    environment:
      MLFLOW_SERVER_ALLOWED_HOSTS: "localhost,localhost:5000,127.0.0.1,127.0.0.1:5000,mlflow,mlflow:5000"
    ports:
      - "5000:5000"
    volumes:
      - mlflow-data:/mlflow
    healthcheck:
      test:
        - CMD
        - python
        - -c
        - import urllib.request; urllib.request.urlopen('http://localhost:5000/health', timeout=5)
      interval: 10s
      timeout: 10s
      retries: 12
      start_period: 20s
    restart: unless-stopped

  api:
    profiles: [api]
    build:
      context: .
      dockerfile: docker/api.Dockerfile
    image: radar-ecobici/api:local
    env_file: [.env]
    environment:
      DATA_DIR: /app/data
      MLFLOW_TRACKING_URI: http://mlflow:5000
      MODEL_FALLBACK_PATH: /app/models/champion.joblib
    volumes:
      - ./data:/app/data:ro
      - ./models:/app/models:ro
    ports:
      - "8000:8000"
    healthcheck:
      test:
        - CMD
        - python
        - -c
        - import urllib.request; urllib.request.urlopen('http://localhost:8000/health', timeout=5)
      interval: 10s
      timeout: 10s
      retries: 12
      start_period: 30s
    restart: unless-stopped

volumes:
  seaweed-data:
  airflow-db:
  mlflow-data:
```

**`docker/README.md`**

```markdown
# docker/

Dockerfiles por servicio (contexto de build = raíz del repo; ver `.dockerignore`).

- `collector.Dockerfile` — recolector, productor Kafka y consumidor `bronze-writer` (misma imagen).
- `airflow.Dockerfile` — Airflow 3 con un venv aislado del proyecto en `/opt/radar/.venv`.
- `api.Dockerfile` — API FastAPI con el modelo.
- `seaweedfs/s3.json.example` — formato de identidades S3; el real (`s3.json`) se genera y se ignora.
```

**`docker/seaweedfs/s3.json.example`**

```json
{
  "identities": [
    {
      "name": "admin",
      "credentials": [
        { "accessKey": "EJEMPLO_ADMIN_ACCESS_KEY", "secretKey": "EJEMPLO_ADMIN_SECRET_KEY" }
      ],
      "actions": ["Admin", "Read", "Write", "List", "Tagging"]
    },
    {
      "name": "radar-app",
      "credentials": [
        { "accessKey": "EJEMPLO_APP_ACCESS_KEY", "secretKey": "EJEMPLO_APP_SECRET_KEY" }
      ],
      "actions": [
        "Read:radar-bronze",
        "Write:radar-bronze",
        "List:radar-bronze",
        "Read:radar-silver",
        "Write:radar-silver",
        "List:radar-silver",
        "Read:radar-gold",
        "Write:radar-gold",
        "List:radar-gold"
      ]
    }
  ]
}
```

**`docs/README.md`**

```markdown
# docs/

Documentación del proyecto (también es el corpus del asistente RAG del paso 17).

- `bitacora.md` — decisiones (qué decidí · alternativas · por qué · consecuencias).
- `arquitectura.md` — diagrama Mermaid de punta a punta.
- `metrics.md` — definición de cada métrica del proyecto.
- `kafka.md`, `data-lake.md`, `modelo.md`, `backend.md`, `frontend.md`, `anomalias.md`,
  `nube.md`, `rag.md`, `runbook.md` — un manual por etapa.
- `documento-tecnico.md`, `manual-usuario.md`, `guion-video.md`, `presentacion.md` — entrega final.
- `reportes/` — salidas generadas (benchmark, consultas SQL, comparación de modelos, EDA).
```

**`frontend/README.md`**

````markdown
# frontend/

Mapa 3D con React + Vite + deck.gl (`ColumnLayer`) sobre MapLibre y mapas base de OpenFreeMap
(sin token ni tarjeta).

```bash
npm --prefix frontend install
npm --prefix frontend run dev        # http://localhost:5173 (la API debe estar en :8000)
npm --prefix frontend run test
npm --prefix frontend run build && npm --prefix frontend run smoke
```

Manual completo: `docs/frontend.md`.
````

**`frontend/index.html`**

```html
<!doctype html>
<html lang="es-MX">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <meta name="description" content="Mapa 3D en vivo de las estaciones de ECOBICI CDMX con predicción a 60 minutos." />
    <title>Radar ECOBICI CDMX</title>
    <script>
      // Fija el tema antes del primer pintado (evita el destello): localStorage > prefers-color-scheme.
      (function () {
        var tema = null;
        try { tema = localStorage.getItem("radar-tema"); } catch (e) { tema = null; }
        if (tema !== "dark" && tema !== "light") {
          tema = window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
        }
        document.documentElement.setAttribute("data-theme", tema);
      })();
    </script>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>
```

**`frontend/package.json`**

```json
{
  "name": "radar-ecobici-mapa",
  "private": true,
  "version": "0.1.0",
  "description": "Mapa 3D de Radar ECOBICI CDMX: columnas por estación y predicción t+60 min.",
  "author": "Angel Isacc Cabrera Leal",
  "license": "MIT",
  "type": "module",
  "engines": {
    "node": ">=22.12"
  },
  "scripts": {
    "dev": "vite",
    "typecheck": "tsc --noEmit -p tsconfig.json",
    "test": "vitest run",
    "build": "vite build",
    "preview": "vite preview",
    "smoke": "node scripts/smoke.mjs"
  },
  "dependencies": {
    "@deck.gl/core": "9.4.1",
    "@deck.gl/layers": "9.4.1",
    "@deck.gl/react": "9.4.1",
    "@deck.gl/widgets": "9.4.1",
    "maplibre-gl": "6.17.0",
    "react": "19.3.1",
    "react-dom": "19.3.1",
    "react-map-gl": "8.1.3",
    "recharts": "3.10.1"
  },
  "devDependencies": {
    "@types/react": "19.3.0",
    "@types/react-dom": "19.3.0",
    "@vitejs/plugin-react": "6.2.1",
    "typescript": "5.9.3",
    "vite": "8.3.6",
    "vitest": "5.1.0"
  }
}
```

**`frontend/scripts/smoke.mjs`**

```js
/**
 * Autor: Isacc Cabrera
 * Descripción: Prueba de humo del build: levanta `vite preview` con la API de Vite en el mismo
 *   proceso, pide "/" y sale con 0 solo si responde 200 con el contenedor #root. Un solo proceso
 *   evita servidores huérfanos en Windows. Requiere haber corrido `npm run build` antes.
 * Patrón: N/A
 * Fecha de creación: 2026-11-08
 * Cambios:
 *     - 2026-11-08 · Isacc Cabrera · Creación desde el blueprint.
 */
import { preview } from "vite";

const server = await preview({ preview: { port: 4173, strictPort: false } });
let ok = false;
try {
  const url = server.resolvedUrls?.local[0] ?? "http://localhost:4173/";
  const response = await fetch(url);
  const html = await response.text();
  const hasRoot = html.includes('id="root"');
  ok = response.status === 200 && hasRoot;
  console.log(JSON.stringify({ url, status: response.status, root: hasRoot }));
} finally {
  await server.close();
}
process.exit(ok ? 0 : 1);
```

**`frontend/src/lib/scales.test.ts`**

```ts
/**
 * Autor: Isacc Cabrera
 * Descripción: Pruebas del sistema de diseño: contraste WCAG AA de cada par texto/fondo en ambos
 *   temas, umbrales de la clase de predicción, tramos de la rampa y resolución del tema inicial.
 * Patrón: N/A (pruebas unitarias)
 * Fecha de creación: 2026-11-08
 * Cambios:
 *     - 2026-11-08 · Isacc Cabrera · Creación desde el blueprint (§7 y §15).
 */
import { describe, expect, it } from "vitest";
import {
  BIKE_RAMP,
  THEME_TOKENS,
  type Theme,
  bikeColor,
  contrastRatio,
  hexToRgb,
  predictionClass,
  resolveTheme,
} from "./scales";

const THEMES: Theme[] = ["dark", "light"];

const TEXT_PAIRS: Array<[string, string]> = [
  ["text", "bg"],
  ["text", "surface"],
  ["text", "surface2"],
  ["textMuted", "bg"],
  ["textMuted", "surface"],
  ["textMuted", "surface2"],
  ["primary", "bg"],
  ["primaryFg", "primary"],
  ["destructive", "bg"],
];

describe("contraste WCAG 2.2 AA", () => {
  for (const theme of THEMES) {
    const tokens = THEME_TOKENS[theme];
    for (const [fg, bg] of TEXT_PAIRS) {
      it(`${theme}: ${fg} sobre ${bg} >= 4.5:1`, () => {
        expect(contrastRatio(tokens[fg] ?? "", tokens[bg] ?? "")).toBeGreaterThanOrEqual(4.5);
      });
    }
    it(`${theme}: focus sobre bg >= 3:1`, () => {
      expect(contrastRatio(tokens.focus ?? "", tokens.bg ?? "")).toBeGreaterThanOrEqual(3);
    });
  }
});

describe("predictionClass", () => {
  it("-3 o menos se vacía", () => {
    expect(predictionClass(-3)).toBe("se_vacia");
  });
  it("entre -3 y +3 es estable", () => {
    expect(predictionClass(-2.9)).toBe("estable");
    expect(predictionClass(2.9)).toBe("estable");
  });
  it("+3 o más se llena", () => {
    expect(predictionClass(3)).toBe("se_llena");
  });
  it("sin predicción devuelve null", () => {
    expect(predictionClass(null)).toBeNull();
  });
});

describe("bikeColor", () => {
  it("estación vacía usa el primer tramo", () => {
    expect(bikeColor(0, 10)).toEqual(hexToRgb(BIKE_RAMP[0] ?? ""));
  });
  it("estación llena usa el último tramo", () => {
    expect(bikeColor(10, 10)).toEqual(hexToRgb(BIKE_RAMP[3] ?? ""));
  });
  it("capacidad cero no divide entre cero", () => {
    expect(bikeColor(0, 0)).toEqual(hexToRgb(BIKE_RAMP[0] ?? ""));
  });
});

describe("resolveTheme", () => {
  it("lo guardado gana sobre el sistema", () => {
    expect(resolveTheme("light", true)).toBe("light");
  });
  it("sin valor guardado sigue prefers-color-scheme", () => {
    expect(resolveTheme(null, true)).toBe("dark");
    expect(resolveTheme(null, false)).toBe("light");
  });
  it("un valor inválido se ignora", () => {
    expect(resolveTheme("sepia", false)).toBe("light");
  });
});
```

**`frontend/src/lib/scales.ts`**

```ts
/**
 * Autor: Isacc Cabrera
 * Descripción: Sistema de diseño en código: paletas por tema, rampa de bicis disponibles, clases
 *   de predicción (se vacía / estable / se llena), mapas base de OpenFreeMap y utilidades de
 *   contraste WCAG. Lógica pura, sin React, para que Vitest la pruebe en entorno node.
 * Patrón: N/A (design tokens + funciones puras)
 * Fecha de creación: 2026-11-08
 * Cambios:
 *     - 2026-11-08 · Isacc Cabrera · Creación desde el blueprint (§7).
 */

export type Theme = "dark" | "light";
export type PredictionClass = "se_vacia" | "estable" | "se_llena";
export type Rgb = [number, number, number];

export const THEME_STORAGE_KEY = "radar-tema";

export const THEME_TOKENS: Record<Theme, Record<string, string>> = {
  dark: {
    bg: "#07120D",
    surface: "#0E1F17",
    surface2: "#14291F",
    border: "#1F3D2E",
    text: "#E3F5EA",
    textMuted: "#8DB8A1",
    primary: "#22C55E",
    primaryStrong: "#16A34A",
    primaryFg: "#07120D",
    accent: "#A3E635",
    focus: "#4ADE80",
    destructive: "#F87171",
  },
  light: {
    bg: "#F2FAF5",
    surface: "#FFFFFF",
    surface2: "#E7F4EC",
    border: "#C9E4D3",
    text: "#0D2A1C",
    textMuted: "#4A6B5A",
    primary: "#15803D",
    primaryStrong: "#166534",
    primaryFg: "#FFFFFF",
    accent: "#65A30D",
    focus: "#16A34A",
    destructive: "#B91C1C",
  },
};

/** Rampa secuencial verde para bicis disponibles: vacía → llena (igual en ambos temas). */
export const BIKE_RAMP: readonly string[] = ["#E5F5E0", "#A1D99B", "#41AB5D", "#006D2C"];

/** Paleta divergente de la predicción t+60 (delta = predicción − actual). */
export const PREDICTION_COLORS: Record<PredictionClass, string> = {
  se_vacia: "#F97316",
  estable: "#84CC16",
  se_llena: "#06B6D4",
};

/** Etiquetas e íconos: la leyenda nunca depende solo del color (WCAG 1.4.1). */
export const PREDICTION_LABELS: Record<PredictionClass, { label: string; icon: string }> = {
  se_vacia: { label: "Se vacía", icon: "▼" },
  estable: { label: "Estable", icon: "●" },
  se_llena: { label: "Se llena", icon: "▲" },
};

export const ANOMALY_COLOR = "#EF4444";
export const PREDICTION_THRESHOLD = 3;

export const BASEMAPS: Record<Theme, string> = {
  dark: "https://tiles.openfreemap.org/styles/dark",
  light: "https://tiles.openfreemap.org/styles/positron",
};

export const BASEMAP_ATTRIBUTION = "OpenFreeMap © OpenMapTiles Data from OpenStreetMap";

/** Convierte "#RRGGBB" en [r, g, b]. */
export function hexToRgb(hex: string): Rgb {
  const clean = hex.replace("#", "");
  return [
    parseInt(clean.slice(0, 2), 16),
    parseInt(clean.slice(2, 4), 16),
    parseInt(clean.slice(4, 6), 16),
  ];
}

/** Color de la columna según la proporción bicis/capacidad (cuatro tramos de BIKE_RAMP). */
export function bikeColor(bikes: number, capacity: number): Rgb {
  const ratio = capacity > 0 ? bikes / capacity : 0;
  let index = 3;
  if (bikes <= 0) {
    index = 0;
  } else if (ratio <= 1 / 3) {
    index = 1;
  } else if (ratio <= 2 / 3) {
    index = 2;
  }
  return hexToRgb(BIKE_RAMP[index] ?? "#E5F5E0");
}

/** Clasifica el delta predicho: ≤ −3 se vacía, ≥ +3 se llena, el resto estable. */
export function predictionClass(delta: number | null): PredictionClass | null {
  if (delta === null || Number.isNaN(delta)) {
    return null;
  }
  if (delta <= -PREDICTION_THRESHOLD) {
    return "se_vacia";
  }
  if (delta >= PREDICTION_THRESHOLD) {
    return "se_llena";
  }
  return "estable";
}

/** Tema inicial: lo guardado en localStorage gana; si no, prefers-color-scheme. */
export function resolveTheme(stored: string | null, prefersDark: boolean): Theme {
  if (stored === "dark" || stored === "light") {
    return stored;
  }
  return prefersDark ? "dark" : "light";
}

function channel(value: number): number {
  const c = value / 255;
  return c <= 0.04045 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4;
}

/** Luminancia relativa WCAG 2.x de un color hex. */
export function relativeLuminance(hex: string): number {
  const [r, g, b] = hexToRgb(hex);
  return 0.2126 * channel(r) + 0.7152 * channel(g) + 0.0722 * channel(b);
}

/** Razón de contraste WCAG entre dos colores hex (1 a 21). */
export function contrastRatio(a: string, b: string): number {
  const la = relativeLuminance(a);
  const lb = relativeLuminance(b);
  const [hi, lo] = la >= lb ? [la, lb] : [lb, la];
  return (hi + 0.05) / (lo + 0.05);
}
```

**`frontend/src/main.tsx`**

```tsx
/**
 * Autor: Isacc Cabrera
 * Descripción: Punto de entrada de React: monta <App /> en #root e importa los estilos globales
 *   (tokens del sistema de diseño y la hoja de MapLibre).
 * Patrón: N/A
 * Fecha de creación: 2026-11-08
 * Cambios:
 *     - 2026-11-08 · Isacc Cabrera · Creación desde el blueprint.
 */
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "maplibre-gl/dist/maplibre-gl.css";
import "./styles/theme.css";
import App from "./App";

const rootElement = document.getElementById("root");
if (rootElement === null) {
  throw new Error("No existe el elemento #root en index.html");
}

createRoot(rootElement).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
```

**`frontend/src/styles/theme.css`**

```css
/**
 * Autor: Isacc Cabrera
 * Descripción: Tokens del sistema de diseño (paleta verde, tema oscuro y claro). Los componentes
 *   usan solo var(--token); ningún componente escribe un hex literal.
 * Patrón: N/A (design tokens)
 * Fecha de creación: 2026-11-08
 * Cambios:
 *     - 2026-11-08 · Isacc Cabrera · Creación desde el blueprint (§7).
 */
:root[data-theme="dark"] {
  --bg: #07120d;
  --surface: #0e1f17;
  --surface-2: #14291f;
  --border: #1f3d2e;
  --text: #e3f5ea;
  --text-muted: #8db8a1;
  --primary: #22c55e;
  --primary-strong: #16a34a;
  --primary-fg: #07120d;
  --accent: #a3e635;
  --focus: #4ade80;
  --destructive: #f87171;
  --anomaly: #ef4444;
  color-scheme: dark;
}

:root[data-theme="light"] {
  --bg: #f2faf5;
  --surface: #ffffff;
  --surface-2: #e7f4ec;
  --border: #c9e4d3;
  --text: #0d2a1c;
  --text-muted: #4a6b5a;
  --primary: #15803d;
  --primary-strong: #166534;
  --primary-fg: #ffffff;
  --accent: #65a30d;
  --focus: #16a34a;
  --destructive: #b91c1c;
  --anomaly: #ef4444;
  color-scheme: light;
}

:root {
  --font-sans: Inter, system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
  --space-1: 4px;
  --space-2: 8px;
  --space-3: 12px;
  --space-4: 16px;
  --space-5: 24px;
  --space-6: 32px;
  --radius-sm: 6px;
  --radius-md: 10px;
  --radius-lg: 16px;
  --shadow-panel: 0 8px 24px rgba(0, 0, 0, 0.28);
  --duration-fast: 150ms;
  --duration-base: 250ms;
  --ease-out: cubic-bezier(0.22, 1, 0.36, 1);
  --panel-width: 360px;
}

*,
*::before,
*::after {
  box-sizing: border-box;
}

html,
body,
#root {
  height: 100%;
  margin: 0;
}

body {
  background: var(--bg);
  color: var(--text);
  font-family: var(--font-sans);
  font-size: 15px;
  line-height: 1.5;
  -webkit-font-smoothing: antialiased;
}

.num {
  font-variant-numeric: tabular-nums;
}

:focus-visible {
  outline: 3px solid var(--focus);
  outline-offset: 2px;
}

.skip-link {
  position: absolute;
  left: var(--space-2);
  top: -48px;
  background: var(--primary);
  color: var(--primary-fg);
  padding: var(--space-2) var(--space-3);
  border-radius: var(--radius-sm);
  z-index: 10;
}

.skip-link:focus {
  top: var(--space-2);
}

@media (prefers-reduced-motion: reduce) {
  *,
  *::before,
  *::after {
    animation-duration: 0.01ms !important;
    transition-duration: 0.01ms !important;
  }
}
```

**`frontend/tsconfig.json`**

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "lib": ["ES2022", "DOM", "DOM.Iterable"],
    "module": "ESNext",
    "moduleResolution": "bundler",
    "jsx": "react-jsx",
    "strict": true,
    "noEmit": true,
    "skipLibCheck": true,
    "isolatedModules": true,
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "noFallthroughCasesInSwitch": true,
    "types": ["vite/client"]
  },
  "include": ["src"],
  "exclude": ["node_modules", "dist"]
}
```

**`frontend/vite.config.ts`**

```ts
/**
 * Autor: Isacc Cabrera
 * Descripción: Configuración de Vite (dev server en el puerto 5173, que la API permite por CORS)
 *   y de Vitest (entorno node: las pruebas cubren lógica pura de escalas y tema).
 * Patrón: N/A
 * Fecha de creación: 2026-11-08
 * Cambios:
 *     - 2026-11-08 · Isacc Cabrera · Creación desde el blueprint.
 */
import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [react()],
  server: { port: 5173, strictPort: true },
  preview: { port: 4173 },
  test: {
    environment: "node",
    include: ["src/**/*.test.ts"],
    exclude: ["node_modules", "dist"],
  },
});
```

**`infra/README.md`**

```markdown
# infra/

Infraestructura como código para AWS con Terraform (paso 16). **Genera costos**: no se ejecuta
`terraform apply` sin aprobación explícita. El state es local y está ignorado en git.

- `terraform/` — bucket S3 (versionado, cifrado, sin acceso público), usuario IAM de mínimo
  privilegio y alerta de presupuesto de 5 USD.
```

**`notebooks/README.md`**

```markdown
# notebooks/

- `01_eda.ipynb` — análisis exploratorio con Plotly (mapa y series de tiempo) y conclusiones
  (paso 10). Se exporta a HTML en `docs/reportes/` con `nbconvert` (paso 11); el PDF se obtiene
  imprimiendo ese HTML desde el navegador.
```

**`pyproject.toml`**

```toml
# Autor: Isacc Cabrera
# Descripción: Manifiesto del proyecto Python (dependencias fijadas, extras por capa y configuración
#   de ruff, mypy y pytest). Fuente única de versiones Python; uv.lock es su resolución exacta.
# Patrón: N/A
# Fecha de creación: 2026-11-08
# Cambios:
#     - 2026-11-08 · Isacc Cabrera · Creación desde el blueprint (versiones verificadas en PyPI).

[project]
name = "radar-ecobici-cdmx"
version = "0.1.0"
description = "Pipeline en tiempo real del feed GBFS de ECOBICI CDMX, predicción t+60 min, API y mapa 3D."
requires-python = ">=3.12,<3.13"
license = "MIT"
authors = [{ name = "Angel Isacc Cabrera Leal" }]
dependencies = [
    "httpx==0.28.1",
    "structlog==26.1.0",
    "pydantic==2.14.3",
    "pydantic-settings==2.16.2",
]

[project.optional-dependencies]
streaming = ["confluent-kafka==2.16.1"]
lake = [
    "boto3==1.43.126",
    "pyarrow==26.0.1",
    "duckdb==2.0.0",
    "polars==2.1.0",
    "pandas==3.1.1",
]
ml = [
    "scikit-learn==1.9.1",
    "lightgbm==4.7.0",
    "mlflow==3.18.0",
    "plotly==7.1.0",
    "holidays==0.108",
    "jupyterlab==4.6.6",
    "nbconvert==7.17.2",
]
api = ["fastapi==0.162.2", "uvicorn==0.54.0"]
rag = ["chromadb==1.5.9"]

[dependency-groups]
dev = [
    "pytest==9.2.0",
    "pytest-cov==7.1.0",
    "respx==0.23.1",
    "ruff==0.17.4",
    "mypy==2.4.1",
]

[build-system]
requires = ["uv_build>=0.13.7,<0.14.0"]
build-backend = "uv_build"

[tool.uv]
required-version = ">=0.13.0"

[tool.uv.build-backend]
module-name = "radar"
module-root = "src"

[tool.ruff]
line-length = 100
target-version = "py312"
extend-exclude = ["blueprints", "frontend", "notebooks", "data"]

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "SIM"]

[tool.ruff.lint.isort]
known-first-party = ["radar"]

[tool.mypy]
python_version = "3.12"
files = ["src/radar"]
mypy_path = "src"
explicit_package_bases = true
disallow_untyped_defs = true
ignore_missing_imports = true
warn_unused_ignores = true
exclude = ["^blueprints/", "^frontend/", "^data/"]

[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["src"]
addopts = "-ra --strict-markers"
norecursedirs = ["blueprints", "frontend", "node_modules", ".venv", "data", "notebooks"]
```

**`scripts/README.md`**

```markdown
# scripts/

Scripts de operación que se corren con `uv run python scripts/<archivo>.py` (o `bash` para `.sh`).

| Script | Para qué | Paso |
|---|---|---|
| `demo_recovery.sh` | Demo de recuperación del consumidor Kafka | 5 |
| `lake_setup.py` | Identidades S3, buckets y prueba de mínimo privilegio en SeaweedFS | 6 |
| `sync_lake.py` | Sube bronze/silver/gold locales a SeaweedFS (nunca viajes crudos) | 6 |
| `run_hourly.py` | Pipeline horario + checks de calidad (fallback de Airflow) | 9 |
| `sync_gold_to_s3.sh` | Sube gold a AWS S3 (solo tras aprobar costos) | 16 |
```

**`sql/README.md`**

```markdown
# sql/

Consultas analíticas en DuckDB sobre las capas silver y gold (Parquet).

- `analitica.sql` — JOIN, GROUP BY + HAVING, subconsulta y funciones de ventana (paso 8).
  Se ejecuta con `uv run python -m radar.lake.silver_to_gold --sql-report sql/analitica.sql`.
```

**`src/README.md`**

```markdown
# src/

Código Python del paquete `radar` (layout `src/`, instalado en modo editable por `uv sync`).

| Subpaquete | Qué hace | Paso |
|---|---|---|
| `radar/collector/` | Recolector GBFS: cliente, reintentos (Strategy), hash, sinks (Ports & Adapters), heartbeat | 2-3 |
| `radar/streaming/` | Kafka: mensajes, productor `KafkaSink`, consumidor `bronze-writer` | 4-5 |
| `radar/lake/` | Data lake: `LakeStore`, bronze → silver (cadena de calidad), silver → gold, viajes, clima | 6-9 |
| `radar/ml/` | Features, estrategias `Predictor`, entrenamiento con MLflow | 10-11 |
| `radar/api/` | FastAPI: esquemas, repositorio, inyección de dependencias | 12 |
| `radar/anomalies/` | Detectores de anomalías | 15 |
| `radar/rag/` | Asistente RAG local con Ollama | 17 |

Regla de importación: las capas solo importan "hacia abajo" (ver `CLAUDE.md`).
```

**`src/radar/__init__.py`**

```python
"""
Autor: Isacc Cabrera
Descripción: Paquete raíz de Radar ECOBICI CDMX (recolección GBFS, streaming, data lake, modelo,
    API, anomalías y RAG). Cada subpaquete es una capa con dependencias en una sola dirección.
Patrón: N/A
Fecha de creación: 2026-11-08
Cambios:
    - 2026-11-08 · Isacc Cabrera · Creación del paquete desde el blueprint.
"""

__version__ = "0.1.0"
```

**`src/radar/anomalies/__init__.py`**

```python
"""
Autor: Isacc Cabrera
Descripción: Subpaquete de detección de anomalías por estación (congeladas,
    z-score por estación-hora e Isolation Forest).
Patrón: Strategy (detectores intercambiables)
Fecha de creación: 2026-11-08
Cambios:
    - 2026-11-08 · Isacc Cabrera · Creación del subpaquete desde el blueprint.
"""
```

**`src/radar/api/__init__.py`**

```python
"""
Autor: Isacc Cabrera
Descripción: Subpaquete de la API FastAPI: esquemas Pydantic, repositorio sobre
    silver/gold, inyección de dependencias y aplicación.
Patrón: Repository + Dependency Injection
Fecha de creación: 2026-11-08
Cambios:
    - 2026-11-08 · Isacc Cabrera · Creación del subpaquete desde el blueprint.
"""
```

**`src/radar/collector/__init__.py`**

```python
"""
Autor: Isacc Cabrera
Descripción: Subpaquete del recolector GBFS: cliente HTTP, reintentos,
    hash de station_information, sinks y heartbeat.
Patrón: Ports & Adapters (el puerto SnapshotSink vive en sinks.py)
Fecha de creación: 2026-11-08
Cambios:
    - 2026-11-08 · Isacc Cabrera · Creación del subpaquete desde el blueprint.
"""
```

**`src/radar/lake/__init__.py`**

```python
"""
Autor: Isacc Cabrera
Descripción: Subpaquete del data lake medallion: almacenamiento (LakeStore),
    bronze a silver, silver a gold, viajes y clima.
Patrón: Template Method (TransformJob) y Chain of Responsibility (calidad)
Fecha de creación: 2026-11-08
Cambios:
    - 2026-11-08 · Isacc Cabrera · Creación del subpaquete desde el blueprint.
"""
```

**`src/radar/logging.py`**

```python
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
```

**`src/radar/ml/__init__.py`**

```python
"""
Autor: Isacc Cabrera
Descripción: Subpaquete de machine learning: features, estrategias de modelo,
    entrenamiento con validación temporal y evaluación.
Patrón: Strategy (Predictor: Baseline, Lineal, LightGBM)
Fecha de creación: 2026-11-08
Cambios:
    - 2026-11-08 · Isacc Cabrera · Creación del subpaquete desde el blueprint.
"""
```

**`src/radar/rag/__init__.py`**

```python
"""
Autor: Isacc Cabrera
Descripción: Subpaquete del asistente RAG local (Ollama + chromadb) sobre docs/
    y docs/reportes/, con fuentes citadas.
Patrón: Ports & Adapters (Embedder y LLM como puertos)
Fecha de creación: 2026-11-08
Cambios:
    - 2026-11-08 · Isacc Cabrera · Creación del subpaquete desde el blueprint.
"""
```

**`src/radar/streaming/__init__.py`**

```python
"""
Autor: Isacc Cabrera
Descripción: Subpaquete de streaming con Kafka: diseño de mensajes, productor
    (adaptador KafkaSink) y consumidor bronze-writer.
Patrón: Ports & Adapters (KafkaSink adapta el puerto SnapshotSink)
Fecha de creación: 2026-11-08
Cambios:
    - 2026-11-08 · Isacc Cabrera · Creación del subpaquete desde el blueprint.
"""
```

**`tests/README.md`**

```markdown
# tests/

`uv run pytest` — todas las pruebas corren sin red (respuestas GBFS grabadas en `fixtures/`).

- `unit/` — lógica pura: reintentos, hash, sinks, calidad, features, modelos, detectores.
- `integration/` — la API completa con `TestClient` y dependencias sustituidas.
- `fixtures/` — JSON grabados del feed GBFS (`gbfs.json`, `station_status.json`,
  `station_information.json`).
- `unit/test_encabezados.py` — exige el encabezado obligatorio en todo archivo fuente.
```

**`tests/conftest.py`**

```python
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
```

**`tests/fixtures/gbfs.json`**

```json
{
  "last_updated": 1794160800,
  "ttl": 10,
  "version": "2.3",
  "data": {
    "en": {
      "feeds": [
        { "name": "system_information", "url": "https://gbfs.example.test/gbfs/en/system_information.json" },
        { "name": "station_information", "url": "https://gbfs.example.test/gbfs/en/station_information.json" },
        { "name": "station_status", "url": "https://gbfs.example.test/gbfs/en/station_status.json" }
      ]
    },
    "es": {
      "feeds": [
        { "name": "system_information", "url": "https://gbfs.example.test/gbfs/es/system_information.json" },
        { "name": "station_information", "url": "https://gbfs.example.test/gbfs/es/station_information.json" },
        { "name": "station_status", "url": "https://gbfs.example.test/gbfs/es/station_status.json" }
      ]
    }
  }
}
```

**`tests/fixtures/station_information.json`**

```json
{
  "last_updated": 1794160800,
  "ttl": 10,
  "version": "2.3",
  "data": {
    "stations": [
      {
        "station_id": "1",
        "external_id": "ext-0001",
        "name": "710 Reforma - Insurgentes",
        "short_name": "710",
        "lat": 19.4326,
        "lon": -99.1617,
        "rental_methods": ["KEY", "CREDITCARD"],
        "capacity": 19,
        "electric_bike_surcharge_waiver": false,
        "is_charging": false,
        "eightd_has_key_dispenser": false,
        "has_kiosk": true
      },
      {
        "station_id": "2",
        "external_id": "ext-0085",
        "name": "085 Durango - Salamanca",
        "short_name": "085",
        "lat": 19.4192,
        "lon": -99.1661,
        "rental_methods": ["KEY", "CREDITCARD"],
        "capacity": 15,
        "electric_bike_surcharge_waiver": false,
        "is_charging": false,
        "eightd_has_key_dispenser": false,
        "has_kiosk": true
      },
      {
        "station_id": "3",
        "external_id": "ext-0311",
        "name": "311 Amsterdam - Michoacán",
        "short_name": "311",
        "lat": 19.4115,
        "lon": -99.1702,
        "rental_methods": ["KEY", "CREDITCARD"],
        "capacity": 12,
        "electric_bike_surcharge_waiver": false,
        "is_charging": false,
        "eightd_has_key_dispenser": false,
        "has_kiosk": true
      }
    ]
  }
}
```

**`tests/fixtures/station_status.json`**

```json
{
  "last_updated": 1794160800,
  "ttl": 10,
  "version": "2.3",
  "data": {
    "stations": [
      {
        "station_id": "1",
        "num_bikes_available": 7,
        "num_bikes_disabled": 1,
        "num_docks_available": 10,
        "num_docks_disabled": 1,
        "is_installed": 1,
        "is_renting": 1,
        "is_returning": 1,
        "last_reported": 1794160790,
        "eightd_has_available_keys": false,
        "is_charging": false
      },
      {
        "station_id": "2",
        "num_bikes_available": 0,
        "num_bikes_disabled": 0,
        "num_docks_available": 15,
        "num_docks_disabled": 0,
        "is_installed": 1,
        "is_renting": 1,
        "is_returning": 1,
        "last_reported": 1794160700,
        "eightd_has_available_keys": false,
        "is_charging": false
      },
      {
        "station_id": "3",
        "num_bikes_available": 5,
        "num_bikes_disabled": 0,
        "num_docks_available": 6,
        "num_docks_disabled": 0,
        "is_installed": 1,
        "is_renting": 1,
        "is_returning": 1,
        "last_reported": 1794160750,
        "eightd_has_available_keys": false,
        "is_charging": false
      },
      {
        "station_id": "999",
        "num_bikes_available": 3,
        "num_bikes_disabled": 0,
        "num_docks_available": 5,
        "num_docks_disabled": 0,
        "is_installed": 1,
        "is_renting": 1,
        "is_returning": 1,
        "last_reported": 1794160780,
        "eightd_has_available_keys": false,
        "is_charging": false
      }
    ]
  }
}
```

**`tests/unit/test_encabezados.py`**

```python
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
```

---

## 20. Acceptance Gate, Risks & Decision Log

### 20.1 Global acceptance gate

El proyecto está **terminado** cuando todo esto sale 0 desde la raíz, con el bundle presente:

```bash
uv sync --frozen --all-extras --all-groups              # expect: exit 0
uv run ruff check .                                     # expect: exit 0, sin hallazgos
uv run ruff format --check .                            # expect: exit 0
uv run mypy                                             # expect: exit 0
uv run pytest                                           # expect: exit 0, 0 failed, 0 skipped
docker build -f docker/collector.Dockerfile -t radar-ecobici/collector:local .   # expect: exit 0
docker build -f docker/api.Dockerfile -t radar-ecobici/api:local .               # expect: exit 0
docker compose --profile api up -d --build --wait api   # expect: exit 0, api healthy
curl -sf localhost:8000/health                          # expect: exit 0 — corre el artefacto construido
npm --prefix frontend ci                                # expect: exit 0
npm --prefix frontend run typecheck                     # expect: exit 0
npm --prefix frontend run test                          # expect: exit 0 (incluye contraste AA)
npm --prefix frontend run build                         # expect: exit 0
npm --prefix frontend run smoke                         # expect: exit 0 — sirve el build y responde 200
terraform -chdir=infra/terraform fmt -check -recursive  # expect: exit 0 (si el paso 16 no se recortó)
terraform -chdir=infra/terraform init -backend=false -input=false   # expect: exit 0
terraform -chdir=infra/terraform validate               # expect: exit 0
```

Cada expectativa es una propiedad, ningún conteo. Ninguna línea acepta "cualquier salida distinta
de 0".

Manual gates (una vez, antes de la exposición):

- [ ] Cada paso de §9 tiene su tag: `git tag -l 'step-*'` lista los 18 (o los no recortados).
      El repositorio lo crea el Bootstrap de §10.
- [ ] Archivos versionados, una ruta por invocación: `git ls-files --error-unmatch .env.example`,
      `… uv.lock`, `… frontend/package-lock.json`, `… .gitignore`, `… .claude/settings.json`,
      `… infra/terraform/.terraform.lock.hcl` — cada uno sale 0.
- [ ] Ninguno ignorado, una ruta por invocación: `git check-ignore -q .env.example; test $? -eq 1`
      y lo mismo para `uv.lock` y `frontend/package-lock.json` (1 = no ignorado; 128 = error de uso y falla).
- [ ] `.env` nunca entró al historial: `test -z "$(git log --all --name-only --format= -- .env)"`.
- [ ] El ignore estaba antes del primer commit: `git log --diff-filter=A --format=%s -- .gitignore`
      muestra solo `chore: estructura inicial desde el blueprint`.
- [x] Bootstrap ejecutado dos veces seguidas con exit 0 (`uv sync` completo, 233 paquetes),
      `Verify` del paso 1, `ruff`/`format`/`mypy`/prueba de encabezados con el bundle presente y
      `docker compose config -q` en todos los perfiles: confirmado por el hilo principal el 2026-11-08.
- [ ] Bootstrap re-ejecutado sobre el árbol ya construido sale 0 y `uv run mypy` sigue
      encontrando sus dependencias (la copia guardada no revirtió `pyproject.toml`).
- [ ] Toda fila de *Cross-artifact value reconciliation* dice `Compared: yes`, y los gates de lint
      se corrieron desde la raíz con `blueprints/` presente.
- [ ] Recolector: tras `docker compose up -d collector` (o el camino Kafka) y 5 minutos, hay ≥ 5
      archivos nuevos en bronze y ≥ 5 líneas de heartbeat nuevas; con el Wi-Fi apagado 2 min el
      contenedor sigue vivo y el heartbeat registra `error`.
- [ ] §9.1: paridad probada (demo con 0 faltantes) y kill switch probado una vez.
- [ ] Todo non-goal de §1 sigue sin construirse.
- [ ] Las variables de §10 están en `.env` local y ningún secreto está en el repo.
- [ ] Pase solo con teclado, un pase con Narrador y zoom al 200 % sobre el panel (§15).
- [ ] Un WARNING `feed_congelado` provocado a propósito (bajando `STALE_ALERT_SECONDS` a 30 en una
      corrida `RUN_ONCE`) aparece en los logs (§16).
- [ ] Rollback ensayado una vez: `git reset --hard <tag>` en una copia y gate verde.
- [ ] Clonar y correr: el checkpoint del paso 18 pasó (`git clone --no-local . data/_verify/clon`
      + `uv sync --frozen …` + `uv run pytest` en la copia).

**Post-build launch checklist** (requiere a otra parte o una aprobación; no son gates del build):

- [ ] Primer run de GitHub Actions en verde en `Isacc-Cabrera/radar-ecobici-cdmx` (requiere push aprobado).
- [ ] ⚠️ `terraform apply` solo con el "sí" explícito del usuario tras mostrarle recursos y costo;
      captura de `plan/apply`, del bucket con gold y de la alerta de presupuesto; `terraform destroy` después de la demo.
- [ ] Leer los términos de uso de OpenFreeMap antes de publicar el video.
- [ ] El profesor confirma qué retos cuentan (marcados con * en el plan).
- [ ] Video de 90 s grabado, presentación ensayada, posts de LinkedIn #1–#6 publicados en sus fechas.

### 20.2 Risk register

| Risk | Likelihood | Impact | Early signal | Mitigation |
|---|---|---|---|---|
| La laptop se suspende o se cae el Wi-Fi ⇒ huecos de datos | H | H | check `huecos_heartbeat` sale 3; heartbeat con `error` | recolector primero (paso 3, domingo), `restart: unless-stopped`, suspensión desactivada; los huecos se reportan como limitación (Isacc) |
| Solo ~5 días de datos ⇒ el modelo apenas supera al baseline | H | M | `mejora_vs_baseline_pct` ≤ 0 en el paso 11 | features de demanda de viajes históricos, comparación honesta obligatoria en `docs/modelo.md` |
| Huecos durante la migración a Kafka | M | H | demo con `faltantes` > 0 | traslape + nombres idempotentes + kill switch (§9.1) |
| Airflow pesado/complejo en Windows + Docker | M | M | el paso 9 pasa de 3 h | time-box; fallback `scripts/run_hourly.py`; recorte #4 decidido por el usuario (D-011) |
| Deriva del esquema del feed o cambio de endpoint de Lyft | L | H | cuarentena `motivo="esquema"` crece; `FeedSchemaError` en logs | descubrimiento vía `gbfs.json`; `SchemaCheck` falla ruidosamente |
| Costo en AWS | L | M | alerta de presupuesto (80 % de 5 USD) | no se aplica sin aprobación; `terraform destroy` tras la demo |
| Fecha límite (13 nov) | H | H | un paso del calendario se atrasa > 2 h | orden de recorte: RAG (17) → Terraform/AWS (16) → WebSockets (ya fuera) → Airflow (fallback) → Anomalías (15); núcleo intocable: recolector, Kafka, lake, modelo vs baseline, API, mapa 3D, README + diagrama + video |
| Instalación pesada o wheels faltantes del conjunto `chromadb` + `mlflow` + `lightgbm` en Windows | L | M | `uv sync --all-extras --all-groups` falla en el Bootstrap | la resolución ya se comprobó (`uv lock`: 233 paquetes, sin conflictos, 2026-11-08); si una wheel falla, se reporta y el extra `rag` se instala aparte |

### 20.3 Decision log

| # | Decision | Rejected alternative | Why | Would reverse if |
|---|---|---|---|---|
| 1 | Python 3.12 en todo el backend | 3.14 (default del track) | Airflow 3.4, LightGBM e imágenes probadas en 3.12 | todas las imágenes y wheels publican 3.14 estable |
| 2 | Kafka 4 KRaft + dos topics (por estación y crudo) | un solo topic de snapshots | orden por estación + bronze idéntico al feed | un solo consumidor por diseño |
| 3 | Commit después del fsync + nombre idempotente | exactly-once transaccional | el destino es disco, no otro topic | el destino pasa a ser Kafka |
| 4 | SeaweedFS (D-009) | MinIO, Garage, RustFS | MinIO archivado; Garage más configuración; RustFS inmaduro | MinIO vuelve a publicar imágenes comunitarias |
| 5 | Lake en Parquet + DuckDB, sin base relacional | Postgres como warehouse | append-only analítico; DuckDB lee Parquet sin servidor | se necesitan escrituras concurrentes transaccionales |
| 6 | Airflow standalone + LocalExecutor + Postgres | CeleryExecutor; Dagster; cron | lo más ligero que cubre el reto 28 | Airflow no corre en 3 h (→ fallback, D-011) |
| 7 | LightGBM + baseline + lineal con TimeSeriesSplit | XGBoost; redes recurrentes | estándar tabular, rápido en CPU, explicable | ≥ 4 semanas de datos y patrones de largo plazo |
| 8 | MLflow con alias `champion` + respaldo joblib | solo archivo local | reto 33 y API resiliente si MLflow está apagado | — |
| 9 | FastAPI sin envoltura de respuesta | envoltura `{ok, data}` | Swagger y Pydantic tipan directo; un solo cliente | múltiples clientes externos |
| 10 | React + deck.gl ColumnLayer + OpenFreeMap (D-010) | Leaflet 2D; Mapbox | 3D en GPU, sin token | OpenFreeMap restringe el uso |
| 11 | Polling de 60 s | WebSockets | el feed cambia cada ~60 s | se publica en vivo con muchos clientes |
| 12 | TypeScript 5.9.3 | 7.0.2 / 6.0.3 | madurez de la cadena Vite/deck.gl | deck.gl y Vite declaran soporte de TS 7 |
| 13 | `RetryPolicy` a mano (Strategy) | tenacity | material didáctico de la exposición | el recolector crece a muchos clientes HTTP |
| 14 | 3 epics de 6 pasos | 8–10 epics temáticos (sugerencia del brief) | la regla de conteo de la plantilla permite 2–3 epics para 18 pasos; las Etapas siguen como etiqueta por paso | la plantilla cambia su regla |
| 15 | Ollama local para RAG | API de LLM de pago | costo 0 y regla de trabajo 6 | el modelo local no cabe en RAM |

### 20.4 What to build next

1. Job de CI para el frontend (`actions/setup-node@v7`, typecheck + test + build) — cuando el repo
   reciba PRs de otras personas.
2. Despliegue público del mapa y la API (non-goal) — si un reclutador pide un enlace vivo.
3. WebSockets en la API — si se publica en vivo y el polling pesa.
4. Re-entrenamiento programado del modelo en Airflow — con ≥ 4 semanas de datos.
5. Generalizar a otros sistemas GBFS — después de la entrega, si el código se reutiliza.

---

*End of blueprint. Build order is §9. Stop when §20.1 is green.*
