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
