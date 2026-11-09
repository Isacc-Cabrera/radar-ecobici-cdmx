# Epic 01: Ingesta y streaming

> Después de este epic existe el repo público con su arquitectura y bitácora, el recolector GBFS
> corre 24/7 en Docker, Kafka (KRaft) transporta los snapshots a bronze sin pérdidas y SeaweedFS
> guarda las capas con mínimo privilegio.

| | |
|---|---|
| **Epic id** | `01-ingesta-streaming` |
| **Tasks** | `E1-T1` … `E1-T6` (pasos 1–6 de `blueprint.md` §9) |
| **Depends on** | nothing — start here (después del Bootstrap de `blueprint.md` §10) |
| **Unlocks** | `02-lake-modelo-api` |
| **Parallel with** | nada: `02` lee el bronze que este epic produce |

You do not need any other file to complete this epic. Everything below is repeated here on purpose.

---

## Stack

Python 3.12 (uv) · Docker Compose con perfiles · Kafka 4.4 KRaft + confluent-kafka · SeaweedFS
4.53 (S3) · boto3 · polars/pyarrow · pytest + respx · structlog. Shell: **Git Bash** en Windows 11,
siempre desde la raíz `C:\Users\angel\Proyectos_Linkedin\ECO_BIKES\radar-ecobici-cdmx`. Python
fijado en `.python-version`; versiones en `pyproject.toml` + `uv.lock` — léelas, nunca adivines una.
Idioma de código, comentarios y docs: español (identificadores en inglés).

| Task | Command |
|---|---|
| Entorno | `uv sync --all-extras --all-groups` |
| Lint · formato · tipos | `uv run ruff check . && uv run ruff format --check . && uv run mypy` |
| Pruebas (una) | `uv run pytest tests/unit/test_runner.py` |
| Recolector | `docker compose up -d collector` · `docker compose run --rm -e RUN_ONCE=true collector` |
| Kafka y streaming | `docker compose --profile streaming up -d --wait kafka collector-kafka bronze-writer` · bajar: `docker compose --profile streaming stop <servicio>` |
| SeaweedFS | `uv run python scripts/lake_setup.py render` → `docker compose --profile lake up -d --wait seaweedfs` |
| Logs | `docker compose logs --tail 50 <servicio>` |

**Gate:** `uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest`
pasa antes de marcar cualquier task como hecho.

Los servicios (Kafka, SeaweedFS) ya están definidos en `docker-compose.yml`, que el Bootstrap copió
a la raíz: tú no lo escribes, y nunca sustituyes un servicio que un criterio nombra por un falso.
Nombra siempre el servicio al levantarlo; nunca `docker compose down -v`.

## Directory subtree

```
LICENSE, README.md                      # NUEVOS (E1-T1)
docs/{bitacora.md, arquitectura.md, metrics.md}   # NUEVOS (E1-T1); kafka.md (E1-T5)
src/radar/
  config.py                             # NUEVO (E1-T2) — Settings
  logging.py, __init__.py, */__init__.py   # existen (workspace), solo lectura
  collector/
    retry.py, gbfs_client.py, hashing.py   # NUEVOS (E1-T2)
    sinks.py, heartbeat.py, runner.py      # NUEVOS (E1-T3); sinks.py se edita en E1-T4
  streaming/
    messages.py, producer.py            # NUEVOS (E1-T4)
    consumer.py                         # NUEVO (E1-T5)
  lake/store.py                         # NUEVO (E1-T6)
scripts/{demo_recovery.sh (E1-T5), lake_setup.py, sync_lake.py (E1-T6)}
docker/collector.Dockerfile             # NUEVO (E1-T3)
docker-compose.yml, docker/seaweedfs/s3.json.example, .gitignore, .env.example   # existen (workspace), solo lectura
tests/unit/{test_recolector_nucleo, test_runner, test_messages, test_consumer, test_store}.py   # NUEVOS
tests/conftest.py, tests/fixtures/*.json, tests/unit/test_encabezados.py   # existen (workspace)
```

Fuera de este subárbol no se toca nada. Si un task parece requerir otro archivo, detente y reporta.

## Data model touched here

| Entity | Fields this epic adds or reads | Notes |
|---|---|---|
| `bronze/station_status/dt=YYYY-MM-DD/hour=HH/station_status_<last_updated>.json.gz` | cuerpo HTTP crudo en gzip | `dt`/`hour` = `last_updated` en UTC; el nombre es la llave de idempotencia; nunca se sobrescribe ni se borra |
| `bronze/station_information/dt=YYYY-MM-DD/station_information_<sha8>.json.gz` | cuerpo crudo | solo cuando cambia `stations_hash` |
| `bronze/heartbeat/dt=YYYY-MM-DD.jsonl` | `ts_utc, feed, result (sin_cambios\|guardado\|error), http_status, n_stations, duration_ms, error, last_updated, sink` | `error="feed_congelado"` si `last_updated` lleva > `STALE_ALERT_SECONDS` sin cambiar |
| `state/collector.json` | `last_status_last_updated, last_status_change_utc, info_hash, info_refreshed_utc` | estado, no dato crudo |
| Kafka `station_status` (3 particiones) | key `station_id`; JSON de la estación + `feed_last_updated` + `snapshot_id` | orden por estación |
| Kafka `station_status_raw` (1 partición) | key `station_status`; gzip crudo; headers `last_updated`, `content-encoding` | lo consume el grupo `bronze-writer` |
| Buckets `radar-bronze`, `radar-silver`, `radar-gold` | réplica S3 de `data/<capa>/` | nunca `bronze/trips/` |

Campos del feed (verificados 2026-11-08, 677 estaciones). `station_status`: `station_id` (string),
`num_bikes_available`, `num_bikes_disabled`, `num_docks_available`, `num_docks_disabled`,
`is_installed`, `is_renting`, `is_returning`, `last_reported` (epoch s), `eightd_has_available_keys`,
`is_charging`. `station_information`: `station_id`, `external_id`, `name`, `short_name`, `lat`,
`lon`, `rental_methods`, `capacity`, `electric_bike_surcharge_waiver`, `is_charging`,
`eightd_has_key_dispenser`, `has_kiosk`. Índice: `https://gbfs.mex.lyftbikes.com/gbfs/gbfs.json`
(ttl 10; feeds en `data.<lang>.feeds[]` con `name`/`url`; idiomas `en`, `es`).

## Contracts

**Consumed** — already exists, do not rebuild:

| From | Interface | Guarantee |
|---|---|---|
| workspace | `radar.logging.configure_logging(level)`, `get_logger(name)` | logs JSON a stderr; llamar `configure_logging` primero en todo CLI |
| workspace | fixtures `load_fixture(name)` y `settings` (en `tests/conftest.py`) | sin `.env` real; `data_dir` en `tmp_path` |
| workspace | `docker-compose.yml` servicios `collector`, `kafka`, `collector-kafka`, `bronze-writer`, `seaweedfs` | imágenes fijadas, healthchecks para `--wait` |

**Produced** — later epics depend on exactly these signatures:

| Export | Signature | Used by |
|---|---|---|
| `src/radar/config.py` → `Settings`, `get_settings` | `Settings(_env_file=…, **overrides)`; `get_settings() -> Settings` | todos |
| `src/radar/collector/retry.py` → `ExponentialJitterPolicy` | `decide(attempt, status, retry_after, error) -> RetryDecision` | `02` (clima), `03` (RAG) |
| `src/radar/collector/gbfs_client.py` → `GbfsClient` | `discover() -> dict[str, str]`; `fetch(name) -> FetchResult` | `02` (`run_hourly.py --info`) |
| `src/radar/collector/hashing.py` → `stations_hash`, `find_unknown_station_ids` | `(payload) -> str`; `(status_payload, known_ids) -> set[str]` | `02` |
| `src/radar/collector/sinks.py` → `LocalFileSink`, `make_sink` | `write_status(raw, payload) -> "guardado" \| "ya_existia"` | `02` |
| `src/radar/lake/store.py` → `LakeStore`, `make_lake_store` | `put_file(local, key) -> "subido" \| "omitido"`; `make_lake_store(settings, layer, backend)` | `02` |
| `scripts/sync_lake.py` | `--layer bronze\|silver\|gold [--check-idempotent] [--json]` | `02` |

## Conventions that bite in this area

- `Settings` necesita `extra="ignore"` y `env_ignore_empty=True`: `.env` trae variables de Compose y
  AWS, y los secretos vacíos deben valer `None`, no `""`.
- Escritura bronze atómica: `*.tmp` → `flush` → `os.fsync` → `os.replace`. Si el destino existe, no se toca.
- `gzip.compress(raw, mtime=0)` para que el mensaje raw sea determinista.
- En Git Bash, un argumento que empiece con `/` puede ser convertido a ruta de Windows: usa
  `MSYS_NO_PATHCONV=1` delante de `docker compose … exec` con rutas absolutas del contenedor.
- El recolector viejo (`collector`) queda **detenido** tras E1-T5; no lo vuelvas a levantar salvo como kill switch.
- Encabezado obligatorio en cada archivo nuevo (`.claude/rules/encabezados.md`); en `.sh` va después del shebang.

Full project rules: `CLAUDE.md`. Area rules: `.claude/rules/python-pipeline.md`,
`.claude/rules/pruebas.md`, `.claude/rules/infra-costos.md`, `.claude/rules/encabezados.md`. Todos
están en la raíz: el Bootstrap los copió desde `workspace/` antes del task uno.

---

## Tasks

Listed in the same order as `tasks.json`. That order is the build order — work top to bottom and do
not re-rank by priority or by what looks quick.

### `E1-T1` — Fundar el repo: licencia, README, arquitectura y bitácora

**Depends on:** nothing (corre después del Bootstrap de §10) · **Priority:** p0 — metadata para recortes, no un orden de ejecución · **Etapa 0 — Repo y arquitectura** · día sugerido: dom 8

**Meta:** El repo existe con `.gitignore` correcto desde el primer commit, licencia MIT, README inicial, diagrama Mermaid, métricas definidas y la bitácora del usuario dentro de `docs/`.

**Files**
- `docs/bitacora.md`
- `LICENSE`
- `README.md`
- `docs/arquitectura.md`
- `docs/metrics.md`

**Implementación**
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

**Explicación para la exposición** (dásela al usuario en 3–6 líneas ANTES de escribir el código)
- **Por qué `.gitignore` antes del primer `git add`:** `git add -A` toma todo lo que hay en disco;
  si `.env` existe sin estar ignorado, entra al historial y queda ahí aunque luego lo ignores (git
  nunca deja de rastrear un archivo ya rastreado). Borrarlo después no lo saca del historial.
- **Por qué esta estructura:** capas = carpetas (`collector`, `streaming`, `lake`, `ml`, `api`) con
  dependencias en un solo sentido; `src/` evita importar código sin instalar; `scripts/` y `dags/`
  no tienen lógica propia. Alternativa: monolito plano — más rápido al inicio, imposible de explicar
  en la exposición.
- **`.env.example`:** contrato de configuración sin secretos; cualquiera que clone sabe qué variables existen.

**Acceptance**

Copiado de este task en `tasks.json` (mismo texto; aquí con énfasis markdown). Cada uno lo decide un comando de abajo, en esta máquina, durante el build.

1. **WHEN** `git check-ignore -q .env` runs **THE SYSTEM SHALL** exit 0, and `git check-ignore -q .env.example` SHALL exit 1 (`.env` ignored, `.env.example` not ignored).
2. **WHEN** `git log --all --name-only --format= -- .env` runs **THE SYSTEM SHALL** print nothing, proving `.env` was never committed.
3. **WHEN** `docs/bitacora.md` is read **THE SYSTEM SHALL** contain the decision ids `D-008`, `D-009` and `D-010`.
4. **WHEN** `LICENSE` is read **THE SYSTEM SHALL** have `MIT License` as its first line and contain `Copyright (c) 2026 Angel Isacc Cabrera Leal`.
5. **WHEN** `docs/arquitectura.md` and `README.md` are read **THE SYSTEM SHALL** find a line that is exactly three backticks followed by `mermaid` in the former and the lines `## Problema`, `## Objetivo`, `## Arquitectura`, `## Stack` and `## Estado por etapas` in the latter.
6. **WHEN** `docs/metrics.md` is read **THE SYSTEM SHALL** define `cobertura_heartbeat`, `tasa_regla_capacidad`, `mae_t60` and `mejora_vs_baseline_pct`.

**Verify** — cada comando, en orden, desde la raíz del proyecto. Cada uno sale 0 cuando el task es correcto; el último en salir 0 es lo que lo vuelve `done`.

```bash
git check-ignore -q .env
git check-ignore -q .env.example; test $? -eq 1
test -z "$(git log --all --name-only --format= -- .env)"
grep -q 'D-008' docs/bitacora.md && grep -q 'D-009' docs/bitacora.md && grep -q 'D-010' docs/bitacora.md
head -n 1 LICENSE | grep -qx 'MIT License' && grep -qF 'Copyright (c) 2026 Angel Isacc Cabrera Leal' LICENSE
grep -qx '```mermaid' docs/arquitectura.md
uv run python -c "import sys; t = open('README.md', encoding='utf-8').read().splitlines(); sys.exit(0 if all(h in t for h in ['## Problema', '## Objetivo', '## Arquitectura', '## Stack', '## Estado por etapas']) else 1)"
uv run python -c "import sys; t = open('docs/metrics.md', encoding='utf-8').read(); sys.exit(0 if all(m in t for m in ['cobertura_heartbeat', 'tasa_regla_capacidad', 'mae_t60', 'mejora_vs_baseline_pct']) else 1)"
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
```

Corre ambos (commit y tag) después de que el último comando `Verify` salga 0, antes de empezar el siguiente task. Las líneas posteriores al tag son aserciones que solo son decidibles después del commit. Nunca inventes el tag: es el campo `checkpoint` de `tasks.json`.

**Cierre de etapa** (skill `cerrar-etapa`): resumen + estas preguntas de checkpoint para el usuario + propuesta de entrada de bitácora (qué decidí · alternativas · por qué · consecuencias).

- ¿Qué hace `git add .` si `.gitignore` no existe?
- ¿Para qué sirve `.env.example`?

Retos que cubre: 7 (GitHub). Post #1 se publica el lunes 9, después de encender el recolector (paso 3).

### `E1-T2` — Núcleo del recolector: settings, reintentos y hash

**Depends on:** `E1-T1` · **Priority:** p0 — metadata para recortes, no un orden de ejecución · **Etapa 1a — Núcleo del recolector** · día sugerido: dom 8

**Meta:** Existe la lógica pura del recolector (configuración, descubrimiento GBFS, cliente con política de reintentos y hash de estaciones), probada sin red.

**Files**
- `src/radar/config.py`
- `src/radar/collector/retry.py`
- `src/radar/collector/gbfs_client.py`
- `src/radar/collector/hashing.py`
- `tests/unit/test_recolector_nucleo.py`

**Implementación**
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

**Explicación para la exposición** (dásela al usuario en 3–6 líneas ANTES de escribir el código)
- **Strategy (`RetryPolicy`):** la decisión "¿reintento y cuánto espero?" es intercambiable sin
  tocar el cliente; en pruebas se inyecta una política y un `sleep` falsos. Alternativa: `tenacity`
  — funciona, pero esconde el patrón que se quiere explicar.
- **Full jitter:** esperar `uniform(0, min(tope, base·2^n))` evita que muchos clientes reintenten
  sincronizados ("thundering herd"); el tope de 300 s cumple D-005/fallas.
- **4xx vs 5xx:** un 4xx es error nuestro (reintentar no lo arregla), salvo 429 que pide bajar el
  ritmo con `Retry-After`; un 5xx o timeout es del servidor y puede pasar solo.
- **Hash canónico:** ordenar estaciones y llaves hace que el mismo contenido dé el mismo sha256;
  sin `last_updated` porque cambia aunque el contenido sea igual (bitácora Fase 1).

**Acceptance**

Copiado de este task en `tasks.json` (mismo texto; aquí con énfasis markdown). Cada uno lo decide un comando de abajo, en esta máquina, durante el build.

1. **WHEN** `uv run pytest tests/unit/test_recolector_nucleo.py` runs **THE SYSTEM SHALL** exit 0 with 0 failed while every HTTP call is served by respx mocks (no network).
2. **WHEN** discovery reads `tests/fixtures/gbfs.json` with `GBFS_LANG=es` **THE SYSTEM SHALL** return the `station_status` and `station_information` URLs listed under `data.es.feeds`, and SHALL raise `FeedSchemaError` for a language absent from `data`.
3. **WHEN** the mocked feed answers 503, 503 and then 200 **THE SYSTEM SHALL** return the 200 payload after exactly 2 waits, each between 0 and `min(300, RETRY_BASE_SECONDS * 2**attempt)` seconds.
4. **WHEN** the mocked feed answers 429 with `Retry-After: 7` **THE SYSTEM SHALL** wait exactly 7 seconds before the next request.
5. **WHEN** the mocked feed answers 404 **THE SYSTEM SHALL** make exactly 1 request and raise `FeedClientError` without waiting.
6. **WHEN** two `station_information` payloads differ only in `last_updated` **THE SYSTEM SHALL** return the same `stations_hash`, a different one when a station's `capacity` changes, and `find_unknown_station_ids` SHALL return `{"999"}` for `tests/fixtures/station_status.json`.

**Verify** — cada comando, en orden, desde la raíz del proyecto. Cada uno sale 0 cuando el task es correcto; el último en salir 0 es lo que lo vuelve `done`.

```bash
uv run pytest tests/unit/test_recolector_nucleo.py tests/unit/test_encabezados.py
uv run ruff check . && uv run ruff format --check . && uv run mypy
```

**Checkpoint**

```bash
git add -A && git commit -m "feat(E1-T2): núcleo del recolector GBFS con reintentos y hash"
git tag step-02-recolector-nucleo
```

Corre ambos (commit y tag) después de que el último comando `Verify` salga 0, antes de empezar el siguiente task. Las líneas posteriores al tag son aserciones que solo son decidibles después del commit. Nunca inventes el tag: es el campo `checkpoint` de `tasks.json`.

**Cierre de etapa** (skill `cerrar-etapa`): resumen + estas preguntas de checkpoint para el usuario + propuesta de entrada de bitácora (qué decidí · alternativas · por qué · consecuencias).

- ¿Por qué no se reintenta un 404 pero sí un 503?
- ¿Por qué el hash excluye `last_updated`?

Retos que cubre: 16 (API pública), 18 (automatización).

### `E1-T3` — Recolector en Docker: sinks, heartbeat y loop de 60 s

**Depends on:** `E1-T2` · **Priority:** p0 — metadata para recortes, no un orden de ejecución · **Etapa 1b — Recolector en Docker** · día sugerido: dom 8

**Meta:** El recolector corre en un contenedor no root con `restart: unless-stopped`, escribe bronze y heartbeat cada 60 s, y queda encendido hasta el viernes.

**Files**
- `src/radar/collector/sinks.py`
- `src/radar/collector/heartbeat.py`
- `src/radar/collector/runner.py`
- `docker/collector.Dockerfile`
- `tests/unit/test_runner.py`

**Implementación**
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

**Explicación para la exposición** (dásela al usuario en 3–6 líneas ANTES de escribir el código)
- **Imagen vs contenedor:** la imagen es la plantilla inmutable (capas); el contenedor es una
  instancia en ejecución con su capa escribible. Borrar el contenedor borra esa capa.
- **Capas y caché:** copiar `pyproject.toml`/`uv.lock` y sincronizar ANTES de copiar `src/` hace
  que cambiar código no reinstale dependencias.
- **Volumen `./data:/app/data`:** los datos viven en el host; sin volumen se pierden con el contenedor.
- **`restart: unless-stopped`:** Docker lo revive tras un crash o reinicio, salvo que tú lo detengas.
- **Bronze crudo (D-006):** la foto de un minuto no se puede volver a pedir; validar después
  permite corregir reglas sin perder datos. Ports & Adapters: el runner no sabe si escribe a disco o a Kafka.

**Acceptance**

Copiado de este task en `tasks.json` (mismo texto; aquí con énfasis markdown). Cada uno lo decide un comando de abajo, en esta máquina, durante el build.

1. **WHEN** `uv run pytest tests/unit/test_runner.py` runs **THE SYSTEM SHALL** exit 0 with 0 failed.
2. **WHEN** a tick receives a `last_updated` already seen **THE SYSTEM SHALL** append a heartbeat line with `result="sin_cambios"` and write no new bronze file.
3. **WHEN** `last_updated` stays frozen longer than `STALE_ALERT_SECONDS` **THE SYSTEM SHALL** log a WARNING and append a heartbeat with `result="error"` and `error="feed_congelado"`.
4. **WHEN** the client raises any exception during a tick **THE SYSTEM SHALL** append a heartbeat with `result="error"` and keep the loop running.
5. **WHEN** `docker compose run --rm -e RUN_ONCE=true collector` runs against the live feed **THE SYSTEM SHALL** exit 0 and leave at least one `station_status_*.json.gz` under `data/bronze/station_status/dt=*/hour=*/` and at least one `data/bronze/heartbeat/dt=*.jsonl` file.
6. **WHEN** `docker compose up -d collector` runs **THE SYSTEM SHALL** report the `collector` container running with restart policy `unless-stopped` and user `radar` (not root).

**Verify** — cada comando, en orden, desde la raíz del proyecto. Cada uno sale 0 cuando el task es correcto; el último en salir 0 es lo que lo vuelve `done`.

```bash
uv run pytest tests/unit/test_runner.py tests/unit/test_encabezados.py
uv run ruff check . && uv run ruff format --check . && uv run mypy
docker compose build collector
docker compose run --rm -e RUN_ONCE=true collector
uv run python -c "import glob, sys; s = glob.glob('data/bronze/station_status/dt=*/hour=*/station_status_*.json.gz'); h = glob.glob('data/bronze/heartbeat/dt=*.jsonl'); sys.exit(0 if s and h else 1)"
docker compose up -d collector
test "$(docker inspect -f '{{.State.Running}} {{.HostConfig.RestartPolicy.Name}} {{.Config.User}}' "$(docker compose ps -q collector)")" = 'true unless-stopped radar'
```

**Checkpoint**

```bash
git add -A && git commit -m "feat(E1-T3): recolector en Docker con bronze, heartbeat y loop de 60 s"
git tag step-03-recolector-docker
```

Corre ambos (commit y tag) después de que el último comando `Verify` salga 0, antes de empezar el siguiente task. Las líneas posteriores al tag son aserciones que solo son decidibles después del commit. Nunca inventes el tag: es el campo `checkpoint` de `tasks.json`.

**Cierre de etapa** (skill `cerrar-etapa`): resumen + estas preguntas de checkpoint para el usuario + propuesta de entrada de bitácora (qué decidí · alternativas · por qué · consecuencias).

- ¿Qué se pierde si borro el contenedor sin volumen?
- ¿Cómo distingo "sin cambios" de "recolector caído"?

Retos que cubre: 16 (API pública), 31 (Docker), Implementación de contenedores, 18 (automatización). 📣 Post #1 (publicar lunes 9): "Empecé a construir una plataforma de datos en tiempo real con ECOBICI CDMX" — imagen: el diagrama de `docs/arquitectura.md`; aprendizaje: 10 s vs 60 s (5.9 M vs ~1 M filas/día).

### `E1-T4` — Kafka KRaft, topics y productor KafkaSink

**Depends on:** `E1-T3` · **Priority:** p0 — metadata para recortes, no un orden de ejecución · **Etapa 2a — Kafka KRaft y productor** · día sugerido: lun 9

**Meta:** Kafka 4 en modo KRaft corre en el perfil `streaming`, existen los topics `station_status` (3 particiones) y `station_status_raw` (1), y el recolector puede publicar con `SINK=kafka`.

**Files**
- `src/radar/streaming/messages.py`
- `src/radar/streaming/producer.py`
- `src/radar/collector/sinks.py`
- `tests/unit/test_messages.py`

**Implementación**
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

**Explicación para la exposición** (dásela al usuario en 3–6 líneas ANTES de escribir el código)
- **Broker / topic / partición / offset:** el broker guarda logs append-only; un topic se divide
  en particiones; cada mensaje tiene un offset creciente dentro de su partición.
- **Por qué la key garantiza orden por estación:** misma key ⇒ mismo hash ⇒ misma partición, y
  dentro de una partición el orden es total.
- **KRaft:** Kafka 4 ya no usa ZooKeeper; los metadatos viven en un quórum de controllers dentro de Kafka.
- **Dos topics:** uno por estación (procesamiento por estación) y uno crudo (bronze idéntico al feed).
  Alternativa: un solo topic de snapshot — bronze fácil, pero obliga a todos los consumidores a desarmarlo.

**Acceptance**

Copiado de este task en `tasks.json` (mismo texto; aquí con énfasis markdown). Cada uno lo decide un comando de abajo, en esta máquina, durante el build.

1. **WHEN** `uv run pytest tests/unit/test_messages.py` runs **THE SYSTEM SHALL** exit 0 with 0 failed.
2. **WHEN** `tests/fixtures/station_status.json` is converted **THE SYSTEM SHALL** produce 4 station messages keyed by their `station_id`, each value carrying `feed_last_updated` and `snapshot_id` `station_status_1794160800`, plus 1 raw message keyed `station_status` whose value gunzips to the original bytes.
3. **WHEN** `docker compose --profile streaming up -d --wait kafka` runs **THE SYSTEM SHALL** exit 0 with the broker healthy, and `docker compose --profile streaming config --services` SHALL list `kafka` and no service containing `zookeeper`.
4. **WHEN** `uv run python -m radar.streaming.producer --ensure-topics --check` runs twice **THE SYSTEM SHALL** exit 0 both times with `station_status` at 3 partitions and `station_status_raw` at 1 partition.
5. **WHEN** `docker compose --profile streaming run --rm -e RUN_ONCE=true collector-kafka` runs against the live feed **THE SYSTEM SHALL** exit 0 only after every produced message is acknowledged by the broker.

**Verify** — cada comando, en orden, desde la raíz del proyecto. Cada uno sale 0 cuando el task es correcto; el último en salir 0 es lo que lo vuelve `done`.

```bash
uv run pytest tests/unit/test_messages.py tests/unit/test_encabezados.py
uv run ruff check . && uv run ruff format --check . && uv run mypy
docker compose --profile streaming up -d --wait kafka
docker compose --profile streaming config --services | uv run python -c "import sys; s = sys.stdin.read().split(); sys.exit(0 if 'kafka' in s and not any('zookeeper' in x for x in s) else 1)"
uv run python -m radar.streaming.producer --ensure-topics --check
uv run python -m radar.streaming.producer --ensure-topics --check
docker compose --profile streaming build collector-kafka
docker compose --profile streaming run --rm -e RUN_ONCE=true collector-kafka
```

**Checkpoint**

```bash
git add -A && git commit -m "feat(E1-T4): Kafka KRaft, topics y productor KafkaSink"
git tag step-04-kafka-productor
```

Corre ambos (commit y tag) después de que el último comando `Verify` salga 0, antes de empezar el siguiente task. Las líneas posteriores al tag son aserciones que solo son decidibles después del commit. Nunca inventes el tag: es el campo `checkpoint` de `tasks.json`.

**Cierre de etapa** (skill `cerrar-etapa`): resumen + estas preguntas de checkpoint para el usuario + propuesta de entrada de bitácora (qué decidí · alternativas · por qué · consecuencias).

- ¿Por qué la key `station_id` garantiza el orden por estación?
- ¿Por qué dos topics y no uno?

Retos que cubre: 30 (Kafka).

### `E1-T5` — Consumidor bronze-writer, demo de recuperación y migración

**Depends on:** `E1-T4` · **Priority:** p0 — metadata para recortes, no un orden de ejecución · **Etapa 2b — Consumidor, demo de recuperación y migración** · día sugerido: lun 9

**Meta:** El grupo `bronze-writer` escribe bronze desde Kafka con commit manual después del fsync, la demo de recuperación pasa, y la captura migró a Kafka sin huecos.

**Files**
- `src/radar/streaming/consumer.py`
- `scripts/demo_recovery.sh`
- `tests/unit/test_consumer.py`
- `docs/kafka.md`

**Implementación**
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

**Explicación para la exposición** (dásela al usuario en 3–6 líneas ANTES de escribir el código)
- **Consumer group:** los consumidores de un mismo grupo se reparten las particiones; otro grupo
  recibe una copia completa e independiente.
- **At-least-once vs exactly-once:** con commit después de escribir, un fallo entre escribir y
  hacer commit re-entrega el mensaje (al menos una vez). Como el nombre del archivo es
  `station_status_<last_updated>`, re-escribirlo no duplica nada ⇒ efectivamente una vez.
  Exactly-once real (transacciones Kafka) no aplica: el destino es un disco, no otro topic.
- **Por qué no escribir directo a la base:** Kafka desacopla al productor del destino y guarda el
  backlog mientras el consumidor está caído; eso es lo que la demo prueba.

**Acceptance**

Copiado de este task en `tasks.json` (mismo texto; aquí con énfasis markdown). Cada uno lo decide un comando de abajo, en esta máquina, durante el build.

1. **WHEN** `uv run pytest tests/unit/test_consumer.py` runs **THE SYSTEM SHALL** exit 0 with 0 failed.
2. **WHEN** the same raw message is processed twice by `BronzeWriter.process` **THE SYSTEM SHALL** leave exactly one bronze file and commit the message offset both times.
3. **WHEN** writing the bronze file raises an exception **THE SYSTEM SHALL** NOT commit that offset.
4. **WHEN** `docker compose --profile streaming run --rm bronze-writer python -m radar.streaming.consumer --exit-when-idle 20 --min-messages 1` runs with the `bronze-writer` service stopped **THE SYSTEM SHALL** exit 0 after consuming at least 1 message.
5. **WHEN** `DEMO_PAUSE_SECONDS=120 bash scripts/demo_recovery.sh` runs **THE SYSTEM SHALL** stop the consumer, keep producing, restart it, and exit 0 only if every heartbeat with `sink="kafka"` and `result="guardado"` in the window has its `station_status_<last_updated>.json.gz` in bronze.
6. **WHEN** `docker compose --profile streaming ps --services --status running` runs after the migration **THE SYSTEM SHALL** list `kafka`, `collector-kafka` and `bronze-writer` and SHALL NOT list `collector`.

**Verify** — cada comando, en orden, desde la raíz del proyecto. Cada uno sale 0 cuando el task es correcto; el último en salir 0 es lo que lo vuelve `done`.

```bash
uv run pytest tests/unit/test_consumer.py tests/unit/test_encabezados.py
uv run ruff check . && uv run ruff format --check . && uv run mypy
docker compose --profile streaming build bronze-writer
docker compose --profile streaming stop bronze-writer
docker compose --profile streaming run --rm bronze-writer python -m radar.streaming.consumer --exit-when-idle 20 --min-messages 1
DEMO_PAUSE_SECONDS=120 bash scripts/demo_recovery.sh
docker compose --profile streaming ps --services --status running | uv run python -c "import sys; s = set(sys.stdin.read().split()); sys.exit(0 if {'kafka', 'collector-kafka', 'bronze-writer'} <= s and 'collector' not in s else 1)"
uv run python -c "import sys; t = open('docs/kafka.md', encoding='utf-8').read(); sys.exit(0 if all(h in t for h in ['## Diseño de mensajes', '## Semántica de entrega', '## Demo de recuperación', '## Migración sin huecos']) else 1)"
```

**Checkpoint**

```bash
git add -A && git commit -m "feat(E1-T5): consumidor bronze-writer, demo de recuperación y migración a Kafka"
git tag step-05-kafka-consumidor
```

Corre ambos (commit y tag) después de que el último comando `Verify` salga 0, antes de empezar el siguiente task. Las líneas posteriores al tag son aserciones que solo son decidibles después del commit. Nunca inventes el tag: es el campo `checkpoint` de `tasks.json`.

**Cierre de etapa** (skill `cerrar-etapa`): resumen + estas preguntas de checkpoint para el usuario + propuesta de entrada de bitácora (qué decidí · alternativas · por qué · consecuencias).

- ¿Por qué no escribir directo a la base?
- ¿Qué pasa si agrego un segundo consumidor al mismo grupo? ¿Y uno a otro grupo?
- ¿Por qué el commit va después de escribir?

Retos que cubre: 30 (Kafka). 📣 Post #2 (mar 10): "Apagué mi consumidor 5 minutos a propósito y no perdí ni un dato" — imagen: productor → particiones → consumidor con offsets; aprendizaje: qué es un offset y por qué el commit va después de escribir. Corre la demo con `DEMO_PAUSE_SECONDS=300` para el post.

### `E1-T6` — SeaweedFS: buckets, mínimo privilegio y LakeStore

**Depends on:** `E1-T3` · **Priority:** p0 — metadata para recortes, no un orden de ejecución · **Etapa 3a — SeaweedFS y LakeStore** · día sugerido: lun 9

**Meta:** SeaweedFS corre en el perfil `lake` con los buckets `radar-bronze`, `radar-silver`, `radar-gold`, una identidad de app con mínimo privilegio probado, y bronze se sincroniza de forma idempotente.

**Files**
- `scripts/lake_setup.py`
- `src/radar/lake/store.py`
- `scripts/sync_lake.py`
- `tests/unit/test_store.py`

**Implementación**
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

**Explicación para la exposición** (dásela al usuario en 3–6 líneas ANTES de escribir el código)
- **Lake vs warehouse:** el lake guarda archivos baratos en cualquier formato (aquí Parquet) y el
  esquema se aplica al leer; un warehouse impone esquema al escribir.
- **API S3 y por qué SeaweedFS (D-009):** el código habla S3 con boto3; mañana el endpoint puede
  ser AWS sin tocar lógica (eso es lo que compra el puerto `LakeStore`).
- **Mínimo privilegio:** la app solo puede leer/escribir/listar sus tres buckets; crear o borrar
  buckets es de admin. Se prueba, no se asume (`check-policy`).

**Acceptance**

Copiado de este task en `tasks.json` (mismo texto; aquí con énfasis markdown). Cada uno lo decide un comando de abajo, en esta máquina, durante el build.

1. **WHEN** `uv run pytest tests/unit/test_store.py` runs **THE SYSTEM SHALL** exit 0 with 0 failed.
2. **WHEN** `make_lake_store` receives backend `local` **THE SYSTEM SHALL** return a `LocalLakeStore` and for `s3` an `S3LakeStore`, and a second `LocalLakeStore.put_file` with the same key and size SHALL return `omitido`.
3. **WHEN** `uv run python scripts/lake_setup.py render` runs and then `docker compose --profile lake up -d --wait seaweedfs` runs **THE SYSTEM SHALL** fill any blank `S3_*` key in `.env`, write `docker/seaweedfs/s3.json` with the identities `admin` and `radar-app`, and report the service healthy.
4. **WHEN** `uv run python scripts/lake_setup.py buckets` runs twice **THE SYSTEM SHALL** exit 0 both times with the buckets `radar-bronze`, `radar-silver` and `radar-gold` present.
5. **WHEN** `uv run python scripts/lake_setup.py check-policy` runs **THE SYSTEM SHALL** exit 0 only if the `radar-app` identity can put, get and list objects in the three buckets and receives `AccessDenied` on `CreateBucket` and on `DeleteBucket`.
6. **WHEN** `uv run python scripts/sync_lake.py --layer bronze --check-idempotent --json` runs **THE SYSTEM SHALL** exit 0 only if the second pass re-uploads none of the keys the first pass saw and no uploaded key contains `trips/`.

**Verify** — cada comando, en orden, desde la raíz del proyecto. Cada uno sale 0 cuando el task es correcto; el último en salir 0 es lo que lo vuelve `done`.

```bash
uv run pytest tests/unit/test_store.py tests/unit/test_encabezados.py
uv run ruff check . && uv run ruff format --check . && uv run mypy
uv run python scripts/lake_setup.py render
docker compose --profile lake up -d --wait seaweedfs
uv run python scripts/lake_setup.py buckets
uv run python scripts/lake_setup.py buckets
uv run python scripts/lake_setup.py check-policy
uv run python scripts/sync_lake.py --layer bronze --check-idempotent --json
git check-ignore -q docker/seaweedfs/s3.json
```

**Checkpoint**

```bash
git add -A && git commit -m "feat(E1-T6): SeaweedFS con buckets por capa, mínimo privilegio y LakeStore"
git tag step-06-lake-seaweedfs
```

Corre ambos (commit y tag) después de que el último comando `Verify` salga 0, antes de empezar el siguiente task. Las líneas posteriores al tag son aserciones que solo son decidibles después del commit. Nunca inventes el tag: es el campo `checkpoint` de `tasks.json`.

**Cierre de etapa** (skill `cerrar-etapa`): resumen + estas preguntas de checkpoint para el usuario + propuesta de entrada de bitácora (qué decidí · alternativas · por qué · consecuencias).

- ¿Qué permisos mínimos necesita la app y por qué no usa la identidad admin?
- ¿Qué ganamos con el puerto `LakeStore` si mañana cambiamos a AWS S3?

Retos que cubre: 38 (Data Lake), Infraestructura en 3 capas (parcial).


---

## Epic acceptance

The epic is done when every task is `done` **and**:

1. **WHEN** `docker compose --profile streaming ps --services --status running` runs **THE SYSTEM SHALL** list `kafka`, `collector-kafka`, `bronze-writer` and `seaweedfs` and not `collector`.
2. **WHEN** the consumer is stopped for 120 s and restarted **THE SYSTEM SHALL** lose no snapshot (`scripts/demo_recovery.sh` exits 0).

```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest
docker compose --profile streaming --profile lake ps --services --status running | uv run python -c "import sys; s = set(sys.stdin.read().split()); sys.exit(0 if {'kafka', 'collector-kafka', 'bronze-writer', 'seaweedfs'} <= s and 'collector' not in s else 1)"
DEMO_PAUSE_SECONDS=120 bash scripts/demo_recovery.sh
```

## Pitfalls

- **Encender tarde el recolector** — cada hora sin recolectar es una hora menos para el modelo.
  E1-T3 se cierra el domingo y el contenedor no se apaga hasta el viernes.
- **`docker/seaweedfs/s3.json` como directorio** — si levantas SeaweedFS antes de `render`, Docker
  crea un directorio con ese nombre; bórralo y repite `render`.
- **Commit antes de escribir** — el commit de offset va después del `fsync`; al revés pierdes datos
  en un crash.
- **`docker compose up -d` sin nombre de servicio** — reenciende el `collector` viejo (no tiene perfil).
- **Push sin permiso** — crear el repo y hacer push es una acción hacia afuera: anúnciala y espera el "sí".

## Before moving on

- [ ] Every task in this epic is `done` in `tasks.json` — no task left `in_progress`.
- [ ] Every `verify` command of every task in this epic passed, not just the first one.
- [ ] No `verify` command was edited, and none was skipped because a file it names did not exist.
- [ ] **Every task in this epic has its `checkpoint` tag in version control** — `git tag -l 'step-0*'` lists `step-01-fundacion` … `step-06-lake-seaweedfs`.
- [ ] Gate command passes clean, run from the project root.
- [ ] Every "Produced" contract above exists with the stated signature.
- [ ] No file outside the subtree was modified.
- [ ] `.env.example` ya trae todas las variables de este epic (no se agregó ninguna nueva).
- [ ] One commit per task, each with its task id in the conventional scope, each followed by its checkpoint tag.
