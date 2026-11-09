# Epic 03: Producto y entrega

> Después de este epic el proyecto tiene CI en GitHub Actions, la API en su contenedor, el mapa 3D
> con tema verde claro/oscuro, detección de anomalías, la infraestructura AWS descrita en Terraform
> (sin aplicar), el asistente RAG opcional y toda la documentación de entrega.

| | |
|---|---|
| **Epic id** | `03-producto-entrega` |
| **Tasks** | `E3-T1` … `E3-T6` (pasos 13–18 de `blueprint.md` §9) |
| **Depends on** | `02-lake-modelo-api` |
| **Unlocks** | nada — es el último epic |
| **Parallel with** | dentro del epic: `E3-T4` (Terraform) y `E3-T5` (RAG) no comparten archivos con `E3-T2`/`E3-T3` |

You do not need any other file to complete this epic. Everything below is repeated here on purpose.

---

## Stack

Python 3.12 (uv) · FastAPI · GitHub Actions · Docker · React 19 + Vite 8 + TypeScript 5.9 ·
deck.gl 9.4 (`ColumnLayer`) + MapLibre 6 vía `react-map-gl/maplibre` + OpenFreeMap · recharts ·
Vitest · Terraform 1.17 + provider AWS `~> 6.72` · Ollama + chromadb. Shell: **Git Bash** desde la
raíz. Versiones: `pyproject.toml` + `uv.lock`, `frontend/package.json` + `frontend/package-lock.json`.
Idioma: español (identificadores en inglés).

| Task | Command |
|---|---|
| Gate Python | `uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest` |
| API en contenedor | `docker compose --profile api up -d --build --wait api` · `curl -sf localhost:8000/health` |
| Frontend | `npm --prefix frontend install` · `run dev` (http://localhost:5173) · `run typecheck` · `run test` · `run build` · `run smoke` |
| Terraform (sin costo) | `terraform -chdir=infra/terraform fmt -check -recursive` · `init -backend=false -input=false` · `validate` |
| RAG | `ollama pull llama3.2:3b` · `ollama pull nomic-embed-text` · `uv run python -m radar.rag.index --json` |

**Gate:** el gate Python de arriba, y para tasks del frontend también
`npm --prefix frontend run typecheck && npm --prefix frontend run test`.

`frontend/package.json`, `tsconfig.json`, `vite.config.ts`, `index.html`, `src/main.tsx`,
`src/styles/theme.css`, `src/lib/scales.ts`, `src/lib/scales.test.ts` y `scripts/smoke.mjs` ya
existen (los copió el Bootstrap): no los reescribas. `main.tsx` importa `./App` con export default.

## Directory subtree

```
.github/workflows/ci.yml        # NUEVO (E3-T1)
docker/api.Dockerfile           # NUEVO (E3-T1)
README.md                       # existe; se edita en E3-T1 (badge) y se reescribe en E3-T6
frontend/src/
  App.tsx, hooks.ts             # NUEVOS (E3-T2)
  components/StationMap.tsx     # NUEVO (E3-T2), editado en E3-T3
  components/Panel.tsx          # NUEVO (E3-T2)
  main.tsx, styles/theme.css, lib/scales.ts   # existen (workspace), solo lectura
src/radar/anomalies/detectors.py   # NUEVO (E3-T3)
src/radar/api/repository.py     # existe (E2-T6), se edita en E3-T3
src/radar/rag/{index.py, ask.py}   # NUEVOS (E3-T5)
infra/terraform/{main.tf, variables.tf}   # NUEVOS (E3-T4)
scripts/sync_gold_to_s3.sh      # NUEVO (E3-T4)
tests/unit/{test_detectors, test_rag}.py   # NUEVOS
docs/{backend.md, runbook.md (E3-T1), frontend.md (E3-T2), anomalias.md (E3-T3), nube.md (E3-T4), rag.md (E3-T5)}
docs/{documento-tecnico.md, manual-usuario.md, guion-video.md, presentacion.md}   # NUEVOS (E3-T6)
```

Fuera de este subárbol no se toca nada. Si un task parece requerir otro archivo, detente y reporta.

## Data model touched here

| Entity | Fields this epic adds or reads | Notes |
|---|---|---|
| API `StationOut` | `station_id, nombre, lat, lon, capacidad, bicis_disponibles, docks_disponibles, ultimo_reporte_utc, anomalia` | `anomalia` pasa a ser real en E3-T3 |
| API `PredictionOut` | `station_id, bicis_actuales, prediccion_t60, delta, clase, generado_utc, modelo_version` | `clase`: `delta <= -3` `se_vacia`, `>= 3` `se_llena`, si no `estable` |
| API `HistoryPointOut` | `ts_utc, bicis_disponibles, docks_disponibles` | `GET /stations/{id}/history?hours=1..24` |
| API `AnomalyOut` | `station_id, tipo (congelada \| zscore \| isolation_forest), puntaje, detalle, detectado_utc` | |
| `gold/station_hour` | lectura: media y desviación por estación-hora | base del z-score |
| `chroma/` | colección `radar-docs`, ids `<ruta>#<n>` | ignorado en git |
| S3 de AWS (solo si se aprueba) | `s3://<bucket>/gold/**.parquet` | nunca bronze ni silver |

## Contracts

**Consumed** — already exists, do not rebuild:

| From | Interface | Guarantee |
|---|---|---|
| `02-lake-modelo-api` | `radar.api.main.app` y rutas de §5 | `GET` puras; errores `{"detail": {"codigo", "mensaje"}}` |
| `02-lake-modelo-api` | `StationRepository.anomalies()` | devuelve `[]` hasta E3-T3 |
| `02-lake-modelo-api` | `models/champion.joblib`, MLflow `radar-bikes-t60@champion` | la API carga uno u otro en el lifespan |
| workspace | `frontend/src/lib/scales.ts`: `THEME_TOKENS`, `BIKE_RAMP`, `PREDICTION_COLORS`, `PREDICTION_LABELS`, `ANOMALY_COLOR`, `BASEMAPS`, `BASEMAP_ATTRIBUTION`, `THEME_STORAGE_KEY`, `bikeColor`, `predictionClass`, `resolveTheme` | únicos colores y umbrales permitidos |

**Produced** — later work depends on exactly these signatures:

| Export | Signature | Used by |
|---|---|---|
| `frontend/src/App.tsx` → default `App` | `() => JSX.Element` | `frontend/src/main.tsx` |
| `frontend/src/hooks.ts` → `useStations`, `useStationHistory`, `useTheme`, `useReducedMotion` | `useStations(apiUrl) -> {stations, loading, error, updatedAt}` | `App.tsx` |
| `src/radar/anomalies/detectors.py` → `AnomalyDetector` | `detect(recent, history, now_utc) -> list[Anomaly]` | `repository.py` |

## Conventions that bite in this area

- Ningún hex literal en componentes: todo color viene de `scales.ts` o de `var(--token)`.
- Tema: default `prefers-color-scheme`, persistido en `localStorage["radar-tema"]` vía
  `THEME_STORAGE_KEY`; el `index.html` ya lo fija antes del primer pintado.
- `prefers-reduced-motion: reduce` ⇒ `transitionDuration` 0 en la cámara.
- Leyenda con texto + ícono (`▼ Se vacía`, `● Estable`, `▲ Se llena`, `⚠ Anomalía`), nunca solo color.
- Atribución visible: "OpenFreeMap © OpenMapTiles Data from OpenStreetMap".
- ⚠️ Nunca `terraform apply`, nunca push, nunca un LLM de pago sin el "sí" del usuario.

Full project rules: `CLAUDE.md`. Area rules: `.claude/rules/frontend.md`,
`.claude/rules/infra-costos.md`, `.claude/rules/python-pipeline.md`, `.claude/rules/encabezados.md`.

---

## Tasks

Listed in the same order as `tasks.json`. That order is the build order — work top to bottom and do
not re-rank by priority or by what looks quick. Orden de recorte si falta tiempo (solo el usuario lo
decide): `E3-T5` (RAG) → `E3-T4` (Terraform) → `E3-T3` (anomalías). `E3-T6` no depende de ninguno de los tres.

### `E3-T1` — CI en GitHub Actions, imagen de la API y runbook

**Depends on:** `E2-T6` · **Priority:** p0 — metadata para recortes, no un orden de ejecución · **Etapa 6b — CI, contenedor de la API y runbook** · día sugerido: mié 11

**Meta:** Cada push/PR corre lint, tipos, pruebas y build de imágenes en GitHub Actions; la API corre en su contenedor; README tiene el badge; existen el manual de backend y el runbook.

**Files**
- `.github/workflows/ci.yml`
- `docker/api.Dockerfile`
- `docs/backend.md`
- `docs/runbook.md`
- `README.md`

**Implementación**
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

**Explicación para la exposición** (dásela al usuario en 3–6 líneas ANTES de escribir el código)
- **Qué pasa en CI al abrir un PR:** GitHub levanta una máquina limpia, instala exactamente lo del
  `uv.lock`, corre lint/tipos/pruebas y construye las imágenes; si algo falla, el PR queda en rojo
  antes de mezclarse.
- **`push: false`:** se prueba que las imágenes construyen sin publicarlas (sin registro ni costo).

**Acceptance**

Copiado de este task en `tasks.json` (mismo texto; aquí con énfasis markdown). Cada uno lo decide un comando de abajo, en esta máquina, durante el build.

1. **WHEN** `.github/workflows/ci.yml` is read **THE SYSTEM SHALL** trigger on `push` and `pull_request` and contain `astral-sh/setup-uv@v10`, `0.13.7`, `uv sync --frozen --all-extras --all-groups`, `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy`, `uv run pytest`, `docker/collector.Dockerfile`, `docker/api.Dockerfile` and `push: false`.
2. **WHEN** the CI commands run locally **THE SYSTEM SHALL** exit 0 for the sync, lint, format, type, test and both image builds.
3. **WHEN** `docker compose --profile api up -d --build --wait api` runs **THE SYSTEM SHALL** report `api` healthy and `curl -sf localhost:8000/health` SHALL exit 0.
4. **WHEN** `curl -s -o /dev/null -w '%{http_code}' localhost:8000/docs` runs **THE SYSTEM SHALL** print `200`.
5. **WHEN** `README.md` is read **THE SYSTEM SHALL** contain the badge URL `https://github.com/Isacc-Cabrera/radar-ecobici-cdmx/actions/workflows/ci.yml/badge.svg`.
6. **WHEN** `docs/backend.md` and `docs/runbook.md` are read **THE SYSTEM SHALL** find curl examples for `/health`, `/stations`, `/predict/` and `/anomalies` in the former and `## El recolector se detuvo` and `## Kafka no arranca` in the latter.

**Verify** — cada comando, en orden, desde la raíz del proyecto. Cada uno sale 0 cuando el task es correcto; el último en salir 0 es lo que lo vuelve `done`.

```bash
uv run python -c "import sys; t = open('.github/workflows/ci.yml', encoding='utf-8').read(); k = ['push', 'pull_request', 'astral-sh/setup-uv@v10', '0.13.7', 'uv sync --frozen --all-extras --all-groups', 'uv run ruff check .', 'uv run ruff format --check .', 'uv run mypy', 'uv run pytest', 'docker/collector.Dockerfile', 'docker/api.Dockerfile', 'push: false']; sys.exit(0 if all(x in t for x in k) else 1)"
uv sync --frozen --all-extras --all-groups
uv run ruff check . && uv run ruff format --check . && uv run mypy
uv run pytest
docker build -f docker/collector.Dockerfile -t radar-ecobici/collector:local .
docker build -f docker/api.Dockerfile -t radar-ecobici/api:local .
docker compose --profile api up -d --build --wait api
curl -sf localhost:8000/health
test "$(curl -s -o /dev/null -w '%{http_code}' localhost:8000/docs)" = 200
grep -qF 'https://github.com/Isacc-Cabrera/radar-ecobici-cdmx/actions/workflows/ci.yml/badge.svg' README.md
uv run python -c "import sys; b = open('docs/backend.md', encoding='utf-8').read(); r = open('docs/runbook.md', encoding='utf-8').read().splitlines(); ok = all(('curl' in b and p in b) for p in ['/health', '/stations', '/predict/', '/anomalies']) and '## El recolector se detuvo' in r and '## Kafka no arranca' in r; sys.exit(0 if ok else 1)"
```

**Checkpoint**

```bash
git add -A && git commit -m "ci(E3-T1): GitHub Actions, imagen de la API, manual de backend y runbook"
git tag step-13-ci
```

Corre ambos (commit y tag) después de que el último comando `Verify` salga 0, antes de empezar el siguiente task. Las líneas posteriores al tag son aserciones que solo son decidibles después del commit. Nunca inventes el tag: es el campo `checkpoint` de `tasks.json`.

**Cierre de etapa** (skill `cerrar-etapa`): resumen + estas preguntas de checkpoint para el usuario + propuesta de entrada de bitácora (qué decidí · alternativas · por qué · consecuencias).

- ¿Qué corre en CI y por qué?
- ¿Por qué las pruebas de CI no necesitan datos reales ni red?

Retos que cubre: CI/CD, Pruebas unitarias*.

### `E3-T2` — Mapa 3D con deck.gl, tema claro/oscuro y panel lateral

**Depends on:** `E2-T6` · **Priority:** p0 — metadata para recortes, no un orden de ejecución · **Etapa 7 — Mapa 3D** · día sugerido: mié 11

**Meta:** El mapa 3D muestra una columna por estación (altura = bicis, color = predicción), con tooltip, leyenda, panel con historia, cámara inclinada animada, polling de 60 s y tema verde claro/oscuro persistido.

**Files**
- `frontend/src/App.tsx`
- `frontend/src/components/StationMap.tsx`
- `frontend/src/components/Panel.tsx`
- `frontend/src/hooks.ts`
- `docs/frontend.md`

**Implementación**
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

**Explicación para la exposición** (dásela al usuario en 3–6 líneas ANTES de escribir el código)
- **Componentes y estado:** el contenedor (`App`) tiene el estado y los efectos; los componentes
  presentacionales solo pintan props ⇒ fáciles de razonar y de explicar.
- **deck.gl en WebGL:** cada estación es una instancia de un cilindro en la GPU; deck.gl manda
  arreglos de posiciones/alturas/colores y la GPU dibuja miles de columnas en un solo draw call.
- **Polling vs WebSocket:** el feed cambia cada ~60 s; pedir cada 60 s es simple y suficiente.
  WebSocket ahorra peticiones vacías pero agrega conexión persistente y reconexión (no-goal v1).
- **Columnas 3D vs solo colores:** la altura codifica una cantidad (bicis) y el color otra
  (tendencia), dos variables en un solo vistazo.

**Acceptance**

Copiado de este task en `tasks.json` (mismo texto; aquí con énfasis markdown). Cada uno lo decide un comando de abajo, en esta máquina, durante el build.

1. **WHEN** `npm --prefix frontend install` runs **THE SYSTEM SHALL** exit 0 and write `frontend/package-lock.json`.
2. **WHEN** `npm --prefix frontend run typecheck` runs **THE SYSTEM SHALL** exit 0 under strict TypeScript.
3. **WHEN** `npm --prefix frontend run test` runs **THE SYSTEM SHALL** exit 0, asserting contrast of at least 4.5:1 for every text/background token pair in both themes and the -3/+3 prediction thresholds.
4. **WHEN** `npm --prefix frontend run build` and then `npm --prefix frontend run smoke` run **THE SYSTEM SHALL** exit 0 with the preview server answering `/` with 200 and the `#root` container.
5. **WHEN** the four new source files are searched **THE SYSTEM SHALL** find `ColumnLayer` and `FlyToInterpolator` in `StationMap.tsx`, `THEME_STORAGE_KEY`, `prefers-color-scheme`, `prefers-reduced-motion` and `60000` in `hooks.ts`, and no `#RRGGBB` color literal in any of them.
6. **WHEN** `docs/frontend.md` is read **THE SYSTEM SHALL** contain `## Componentes y estado`, `## Cómo dibuja deck.gl en WebGL`, `## Polling vs WebSocket` and `## Capturas y GIF`.

**Verify** — cada comando, en orden, desde la raíz del proyecto. Cada uno sale 0 cuando el task es correcto; el último en salir 0 es lo que lo vuelve `done`.

```bash
npm --prefix frontend install
npm --prefix frontend run typecheck
npm --prefix frontend run test
npm --prefix frontend run build
npm --prefix frontend run smoke
uv run python -c "import re, sys; r = lambda p: open('frontend/src/' + p, encoding='utf-8').read(); m, h = r('components/StationMap.tsx'), r('hooks.ts'); files = [r('App.tsx'), m, r('components/Panel.tsx'), h]; ok = 'ColumnLayer' in m and 'FlyToInterpolator' in m and all(k in h for k in ['THEME_STORAGE_KEY', 'prefers-color-scheme', 'prefers-reduced-motion', '60000']) and not any(re.search(r'#[0-9A-Fa-f]{6}\b', f) for f in files); sys.exit(0 if ok else 1)"
uv run python -c "import sys; t = open('docs/frontend.md', encoding='utf-8').read().splitlines(); sys.exit(0 if all(h in t for h in ['## Componentes y estado', '## Cómo dibuja deck.gl en WebGL', '## Polling vs WebSocket', '## Capturas y GIF']) else 1)"
uv run pytest tests/unit/test_encabezados.py
```

**Checkpoint**

```bash
git add -A && git commit -m "feat(E3-T2): mapa 3D con deck.gl, tema claro/oscuro y panel lateral"
git tag step-14-mapa-3d
git ls-files --error-unmatch frontend/package-lock.json  # expect: exit 0: lockfile versionado
```

Corre ambos (commit y tag) después de que el último comando `Verify` salga 0, antes de empezar el siguiente task. Las líneas posteriores al tag son aserciones que solo son decidibles después del commit. Nunca inventes el tag: es el campo `checkpoint` de `tasks.json`.

**Cierre de etapa** (skill `cerrar-etapa`): resumen + estas preguntas de checkpoint para el usuario + propuesta de entrada de bitácora (qué decidí · alternativas · por qué · consecuencias).

- ¿Por qué columnas 3D y no solo colores?
- ¿Qué ganaría con WebSockets?

Retos que cubre: Framework de frontend, Mapas/geolocalización (parcial). 📣 Post #5 (sáb 14): video/GIF de 15–20 s del mapa 3D girando sobre la CDMX en hora pico; aprendizaje: cómo un dato de una API pública termina en una columna 3D, en 4 pasos.

### `E3-T3` — Detección de anomalías conectada a la API y al mapa

**Depends on:** `E3-T2` · **Priority:** p1 — metadata para recortes, no un orden de ejecución · **Etapa 8a — Anomalías** · día sugerido: jue 12

**Meta:** Tres detectores (congelada, z-score, Isolation Forest) marcan estaciones anómalas, `/anomalies` los sirve, `/stations` los refleja y el mapa los dibuja; FP/FN documentados con ejemplos reales.

**Files**
- `src/radar/anomalies/detectors.py`
- `tests/unit/test_detectors.py`
- `src/radar/api/repository.py`
- `frontend/src/components/StationMap.tsx`
- `docs/anomalias.md`

**Implementación**
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

**Explicación para la exposición** (dásela al usuario en 3–6 líneas ANTES de escribir el código)
- **z-score:** cuántas desviaciones estándar se aleja el valor de lo normal para esa estación a esa hora.
- **Isolation Forest:** aísla puntos con cortes aleatorios; los raros se aíslan en pocos cortes.
  No necesita etiquetas, pero tampoco explica el "por qué".
- **FP/FN:** una estación vacía a las 8 am en Reforma es normal (FP si se marca); una estación
  rota que reporta valores plausibles es un FN.

**Acceptance**

Copiado de este task en `tasks.json` (mismo texto; aquí con énfasis markdown). Cada uno lo decide un comando de abajo, en esta máquina, durante el build.

1. **WHEN** `uv run pytest tests/unit/test_detectors.py` runs **THE SYSTEM SHALL** exit 0 with 0 failed.
2. **WHEN** a station's `last_reported` stays unchanged for at least `ANOMALY_FROZEN_MINUTES` **THE SYSTEM SHALL** flag it with `tipo="congelada"`, and SHALL NOT flag a station whose `last_reported` advanced within that window.
3. **WHEN** a station's bikes deviate from its station-hour mean by an absolute z-score of at least `ANOMALY_ZSCORE` **THE SYSTEM SHALL** flag it with `tipo="zscore"`, and SHALL skip stations with fewer than 3 historical observations for that hour.
4. **WHEN** `uv run python -m radar.anomalies.detectors --json` runs on the real lake **THE SYSTEM SHALL** exit 0 and print counts for `congelada`, `zscore` and `isolation_forest` plus their intersection.
5. **WHEN** `GET /anomalies` is requested from the real app **THE SYSTEM SHALL** return 200 with items carrying `station_id`, `tipo`, `puntaje` and `detalle`, and `GET /stations` SHALL set `anomalia=true` for exactly those station ids.
6. **WHEN** `npm --prefix frontend run typecheck` and `npm --prefix frontend run build` run **THE SYSTEM SHALL** exit 0 with `StationMap.tsx` using `ANOMALY_COLOR`, and `docs/anomalias.md` SHALL contain `## Falsos positivos` and `## Falsos negativos`.

**Verify** — cada comando, en orden, desde la raíz del proyecto. Cada uno sale 0 cuando el task es correcto; el último en salir 0 es lo que lo vuelve `done`.

```bash
uv run pytest tests/unit/test_detectors.py tests/unit/test_encabezados.py
uv run ruff check . && uv run ruff format --check . && uv run mypy
uv run python -m radar.anomalies.detectors --json
uv run python -c "import sys; from fastapi.testclient import TestClient; from radar.api.main import app; c = TestClient(app); c.__enter__(); a = c.get('/anomalies'); s = c.get('/stations'); c.__exit__(None, None, None); ok = a.status_code == 200 and s.status_code == 200 and all({'station_id', 'tipo', 'puntaje', 'detalle'} <= set(x) for x in a.json()) and {x['station_id'] for x in a.json()} == {x['station_id'] for x in s.json() if x['anomalia']}; sys.exit(0 if ok else 1)"
npm --prefix frontend run typecheck
npm --prefix frontend run build
grep -q 'ANOMALY_COLOR' frontend/src/components/StationMap.tsx
grep -qx '## Falsos positivos' docs/anomalias.md && grep -qx '## Falsos negativos' docs/anomalias.md
```

**Checkpoint**

```bash
git add -A && git commit -m "feat(E3-T3): detección de anomalías en API y mapa"
git tag step-15-anomalias
```

Corre ambos (commit y tag) después de que el último comando `Verify` salga 0, antes de empezar el siguiente task. Las líneas posteriores al tag son aserciones que solo son decidibles después del commit. Nunca inventes el tag: es el campo `checkpoint` de `tasks.json`.

**Cierre de etapa** (skill `cerrar-etapa`): resumen + estas preguntas de checkpoint para el usuario + propuesta de entrada de bitácora (qué decidí · alternativas · por qué · consecuencias).

- ¿Qué es un falso positivo en este contexto y cómo lo reduciría?
- ¿Por qué comparar z-score con Isolation Forest?

Retos que cubre: 44 (anomalías).

### `E3-T4` — Terraform para S3, IAM mínimo y presupuesto de 5 USD

**Depends on:** `E2-T2` · **Priority:** p2 — metadata para recortes, no un orden de ejecución · **Etapa 8b — Terraform/AWS (⚠️ costos)** · día sugerido: jue 12

**Meta:** Terraform describe (sin aplicar) un bucket S3 seguro, un usuario IAM limitado a ese bucket y una alerta de presupuesto de 5 USD; el sync de gold y el destroy están documentados con costos.

**Files**
- `infra/terraform/main.tf`
- `infra/terraform/variables.tf`
- `scripts/sync_gold_to_s3.sh`
- `docs/nube.md`

**Implementación**
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

**Explicación para la exposición** (dásela al usuario en 3–6 líneas ANTES de escribir el código)
- **State:** el archivo donde Terraform recuerda qué creó y con qué IDs; puede contener datos
  sensibles, por eso es local e ignorado en git.
- **plan/apply/destroy:** plan muestra el diff sin tocar nada; apply lo ejecuta; destroy borra todo lo del state.
- **Mínimo privilegio:** el usuario de sync solo puede listar y escribir `gold/` de un bucket; si
  la llave se filtra, el daño se limita a eso.

**Acceptance**

Copiado de este task en `tasks.json` (mismo texto; aquí con énfasis markdown). Cada uno lo decide un comando de abajo, en esta máquina, durante el build.

1. **WHEN** `terraform -chdir=infra/terraform fmt -check -recursive` runs **THE SYSTEM SHALL** exit 0.
2. **WHEN** `terraform -chdir=infra/terraform init -backend=false -input=false` and then `terraform -chdir=infra/terraform validate` run **THE SYSTEM SHALL** exit 0 with provider `hashicorp/aws` constrained to `~> 6.72`.
3. **WHEN** `infra/terraform/main.tf` is read **THE SYSTEM SHALL** declare `aws_s3_bucket_versioning`, `aws_s3_bucket_public_access_block`, `aws_s3_bucket_server_side_encryption_configuration`, `aws_iam_user_policy` and `aws_budgets_budget` with `limit_amount = "5"`, and SHALL NOT declare `aws_iam_access_key`.
4. **WHEN** `git check-ignore -q infra/terraform/terraform.tfstate` runs **THE SYSTEM SHALL** exit 0, and `git check-ignore -q infra/terraform/.terraform.lock.hcl` SHALL exit 1.
5. **WHEN** `bash -n scripts/sync_gold_to_s3.sh` runs **THE SYSTEM SHALL** exit 0, and the script SHALL reference only the local folder `data/gold`.
6. **WHEN** `docs/nube.md` is read **THE SYSTEM SHALL** contain `## Costos estimados`, `## Cómo aplicar (requiere aprobación)` and `## terraform destroy`.

**Verify** — cada comando, en orden, desde la raíz del proyecto. Cada uno sale 0 cuando el task es correcto; el último en salir 0 es lo que lo vuelve `done`.

```bash
terraform -chdir=infra/terraform fmt -check -recursive
terraform -chdir=infra/terraform init -backend=false -input=false
terraform -chdir=infra/terraform validate
uv run python -c "import sys; t = open('infra/terraform/main.tf', encoding='utf-8').read(); ok = all(k in t for k in ['aws_s3_bucket_versioning', 'aws_s3_bucket_public_access_block', 'aws_s3_bucket_server_side_encryption_configuration', 'aws_iam_user_policy', 'aws_budgets_budget', 'limit_amount = \"5\"', '~> 6.72']) and 'aws_iam_access_key' not in t; sys.exit(0 if ok else 1)"
git check-ignore -q infra/terraform/terraform.tfstate
git check-ignore -q infra/terraform/.terraform.lock.hcl; test $? -eq 1
bash -n scripts/sync_gold_to_s3.sh
uv run python -c "import re, sys; t = open('scripts/sync_gold_to_s3.sh', encoding='utf-8').read(); sys.exit(0 if 'data/gold' in t and not re.search(r'data/(bronze|silver)', t) else 1)"
uv run python -c "import sys; t = open('docs/nube.md', encoding='utf-8').read().splitlines(); sys.exit(0 if all(h in t for h in ['## Costos estimados', '## Cómo aplicar (requiere aprobación)', '## terraform destroy']) else 1)"
uv run pytest tests/unit/test_encabezados.py
```

**Checkpoint**

```bash
git add -A && git commit -m "feat(E3-T4): Terraform para S3, IAM mínimo y alerta de presupuesto (sin aplicar)"
git tag step-16-terraform-aws
git ls-files --error-unmatch infra/terraform/.terraform.lock.hcl  # expect: exit 0: lock del provider versionado
```

Corre ambos (commit y tag) después de que el último comando `Verify` salga 0, antes de empezar el siguiente task. Las líneas posteriores al tag son aserciones que solo son decidibles después del commit. Nunca inventes el tag: es el campo `checkpoint` de `tasks.json`.

**Cierre de etapa** (skill `cerrar-etapa`): resumen + estas preguntas de checkpoint para el usuario + propuesta de entrada de bitácora (qué decidí · alternativas · por qué · consecuencias).

- ¿Por qué el state no se sube a Git?
- ¿Qué permisos mínimos necesita mi app?

Retos que cubre: Terraform, 6 (S3), 37 (MLOps, completo).

### `E3-T5` — Asistente RAG local con Ollama y fuentes citadas

**Depends on:** `E2-T5` · **Priority:** p2 — metadata para recortes, no un orden de ejecución · **Etapa 9 — Asistente RAG (opcional; primero en cortarse)** · día sugerido: jue 12

**Meta:** Un asistente local (Ollama + chromadb) responde preguntas sobre `docs/` y `docs/reportes/` citando fuentes, y responde "No sé" cuando no hay evidencia.

**Files**
- `src/radar/rag/index.py`
- `src/radar/rag/ask.py`
- `tests/unit/test_rag.py`
- `docs/rag.md`

**Implementación**
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

**Explicación para la exposición** (dásela al usuario en 3–6 líneas ANTES de escribir el código)
- **Embeddings:** vectores donde textos con significado parecido quedan cerca.
- **Retriever vs LLM:** el retriever busca los fragmentos relevantes; el LLM redacta usando solo
  esos fragmentos. Sin evidencia, no se llama al LLM ⇒ "No sé".
- **Alucinaciones:** el LLM puede inventar; mitigación: contexto obligatorio, citas y umbral de distancia.

**Acceptance**

Copiado de este task en `tasks.json` (mismo texto; aquí con énfasis markdown). Cada uno lo decide un comando de abajo, en esta máquina, durante el build.

1. **WHEN** `uv run pytest tests/unit/test_rag.py` runs **THE SYSTEM SHALL** exit 0 with 0 failed using a fake embedder, an in-memory chromadb client and a fake LLM.
2. **WHEN** the best retrieved fragment's cosine distance exceeds 0.5 **THE SYSTEM SHALL** answer `No sé: no encontré evidencia en la documentación del proyecto.` and SHALL make zero LLM calls.
3. **WHEN** evidence exists **THE SYSTEM SHALL** return an answer plus a `fuentes` list of `ruta#n` ids taken from `docs/`.
4. **WHEN** `ollama list` runs **THE SYSTEM SHALL** list `llama3.2:3b` and `nomic-embed-text`.
5. **WHEN** `uv run python -m radar.rag.index --json` and then `uv run python -m radar.rag.ask "¿Cada cuánto se recolecta station_status y por qué?" --json` run **THE SYSTEM SHALL** exit 0 and print a non-empty `fuentes` list whose ids all start with `docs/`.
6. **WHEN** `docs/rag.md` is read **THE SYSTEM SHALL** contain `## Requisitos de RAM` and `## Riesgos y alucinaciones`.

**Verify** — cada comando, en orden, desde la raíz del proyecto. Cada uno sale 0 cuando el task es correcto; el último en salir 0 es lo que lo vuelve `done`.

```bash
uv run pytest tests/unit/test_rag.py tests/unit/test_encabezados.py
uv run ruff check . && uv run ruff format --check . && uv run mypy
ollama list | uv run python -c "import sys; t = sys.stdin.read(); sys.exit(0 if 'llama3.2:3b' in t and 'nomic-embed-text' in t else 1)"
uv run python -m radar.rag.index --json
uv run python -m radar.rag.ask "¿Cada cuánto se recolecta station_status y por qué?" --json | uv run python -c "import json, sys; f = json.load(sys.stdin)['fuentes']; sys.exit(0 if f and all(x.startswith('docs/') for x in f) else 1)"
grep -qx '## Requisitos de RAM' docs/rag.md && grep -qx '## Riesgos y alucinaciones' docs/rag.md
git check-ignore -q chroma/x
```

**Checkpoint**

```bash
git add -A && git commit -m "feat(E3-T5): asistente RAG local con Ollama y fuentes citadas"
git tag step-17-rag
```

Corre ambos (commit y tag) después de que el último comando `Verify` salga 0, antes de empezar el siguiente task. Las líneas posteriores al tag son aserciones que solo son decidibles después del commit. Nunca inventes el tag: es el campo `checkpoint` de `tasks.json`.

**Cierre de etapa** (skill `cerrar-etapa`): resumen + estas preguntas de checkpoint para el usuario + propuesta de entrada de bitácora (qué decidí · alternativas · por qué · consecuencias).

- ¿Qué es un embedding y por qué sirve para buscar?
- ¿Cómo evita este diseño que el asistente invente respuestas?

Retos que cubre: 39 (IA generativa)*.

### `E3-T6` — Documentación final, guion de video y presentación

**Depends on:** `E3-T1`, `E3-T2` · **Priority:** p0 — metadata para recortes, no un orden de ejecución · **Etapa 10 — Documentación, video y presentación** · día sugerido: jue 12

**Meta:** README final, documento técnico, manual de usuario, guion de 90 s y presentación de 10 diapositivas existen, y el repo se puede clonar y probar siguiendo solo el README.

**Files**
- `README.md`
- `docs/documento-tecnico.md`
- `docs/manual-usuario.md`
- `docs/guion-video.md`
- `docs/presentacion.md`

**Implementación**
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

**Explicación para la exposición** (dásela al usuario en 3–6 líneas ANTES de escribir el código)
- **Data storytelling:** problema → evidencia → decisión → resultado; cada diapositiva responde
  una pregunta del profesor.
- **Clonar y correr:** si el README no basta para levantar el proyecto en una máquina limpia, no
  está documentado; por eso el checkpoint clona el repo y corre las pruebas en la copia.

**Acceptance**

Copiado de este task en `tasks.json` (mismo texto; aquí con énfasis markdown). Cada uno lo decide un comando de abajo, en esta máquina, durante el build.

1. **WHEN** `README.md` is read **THE SYSTEM SHALL** contain the lines `## Demo`, `## Arquitectura`, `## Cómo correrlo en 5 comandos`, `## Resultados del modelo`, `## Retos cubiertos`, `## Trabajo futuro` and `## Licencia`.
2. **WHEN** the `## Cómo correrlo en 5 comandos` section of `README.md` is read **THE SYSTEM SHALL** contain exactly 5 lines starting with `$ `.
3. **WHEN** `docs/presentacion.md` is read **THE SYSTEM SHALL** contain exactly 10 headings starting with `## Diapositiva `, numbered 1 to 10 in order.
4. **WHEN** `docs/guion-video.md` is read **THE SYSTEM SHALL** contain at least one `mm:ss` time mark and no mark later than `01:30`.
5. **WHEN** `docs/documento-tecnico.md` and `docs/manual-usuario.md` are read **THE SYSTEM SHALL** find `bitacora.md` linked in both, `## Decisiones` in the former and `## Cómo usar el mapa` in the latter.
6. **WHEN** `uv run pytest` and `uv run ruff check .` run on the final tree **THE SYSTEM SHALL** exit 0.

**Verify** — cada comando, en orden, desde la raíz del proyecto. Cada uno sale 0 cuando el task es correcto; el último en salir 0 es lo que lo vuelve `done`.

```bash
uv run python -c "import sys; t = open('README.md', encoding='utf-8').read().splitlines(); sys.exit(0 if all(h in t for h in ['## Demo', '## Arquitectura', '## Cómo correrlo en 5 comandos', '## Resultados del modelo', '## Retos cubiertos', '## Trabajo futuro', '## Licencia']) else 1)"
uv run python -c "import sys; t = open('README.md', encoding='utf-8').read().split('## Cómo correrlo en 5 comandos', 1)[1].split('\n## ', 1)[0]; sys.exit(0 if sum(1 for l in t.splitlines() if l.startswith('$ ')) == 5 else 1)"
uv run python -c "import re, sys; h = re.findall(r'^## Diapositiva (\d+)', open('docs/presentacion.md', encoding='utf-8').read(), re.M); sys.exit(0 if [int(x) for x in h] == list(range(1, 11)) else 1)"
uv run python -c "import re, sys; m = [int(a) * 60 + int(b) for a, b in re.findall(r'\b(\d\d):(\d\d)\b', open('docs/guion-video.md', encoding='utf-8').read())]; sys.exit(0 if m and max(m) <= 90 else 1)"
uv run python -c "import sys; d = open('docs/documento-tecnico.md', encoding='utf-8').read(); u = open('docs/manual-usuario.md', encoding='utf-8').read(); ok = 'bitacora.md' in d and 'bitacora.md' in u and '## Decisiones' in d.splitlines() and '## Cómo usar el mapa' in u.splitlines(); sys.exit(0 if ok else 1)"
uv run pytest
uv run ruff check .
```

**Checkpoint**

```bash
git add -A && git commit -m "docs(E3-T6): README final, documento técnico, manual, guion y presentación"
git tag step-18-documentacion
rm -rf data/_verify/clon && git clone --no-local . data/_verify/clon  # expect: copia limpia desde el commit recién hecho
test -f data/_verify/clon/.env.example && test -f data/_verify/clon/uv.lock && test -f data/_verify/clon/frontend/package-lock.json  # expect: exit 0
cd data/_verify/clon && uv sync --frozen --all-extras --all-groups && uv run pytest && cd ../../..  # expect: exit 0: se clona y se prueba siguiendo solo el README
rm -rf data/_verify/clon  # expect: limpieza
```

Corre ambos (commit y tag) después de que el último comando `Verify` salga 0, antes de empezar el siguiente task. Las líneas posteriores al tag son aserciones que solo son decidibles después del commit. Nunca inventes el tag: es el campo `checkpoint` de `tasks.json`.

**Cierre de etapa** (skill `cerrar-etapa`): resumen + estas preguntas de checkpoint para el usuario + propuesta de entrada de bitácora (qué decidí · alternativas · por qué · consecuencias).

- ¿Qué tres lecciones (técnica, de diseño, de gestión del tiempo) contarías en el post de lanzamiento?
- Si tuvieras una semana más, ¿qué harías primero y por qué?

Retos que cubre: 36 y 50 (proyecto integral / integrador), 10 (data storytelling), 20 (portafolio). 📣 Post #6 (lun 16, después de exponer): video demo + link al repo + 3 lecciones aprendidas (técnica, diseño, gestión del tiempo). Etiqueta a la IBERO si aplica.


---

## Epic acceptance

The epic is done when every task is `done` **and**:

1. **WHEN** the global gate runs from the project root with the bundle present **THE SYSTEM SHALL** exit 0 for the Python gate, both image builds, the API container health check and the four frontend scripts.
2. **WHEN** the API container is up **THE SYSTEM SHALL** answer `/health` and `/docs` with 200.

```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest
docker compose --profile api up -d --build --wait api && curl -sf localhost:8000/health
npm --prefix frontend run typecheck && npm --prefix frontend run test && npm --prefix frontend run build && npm --prefix frontend run smoke
```

## Pitfalls

- **El mapa sin API** — `useStations` debe mostrar el error en texto y reintentar en 60 s; nunca
  dejar la pantalla en blanco.
- **Colores sueltos** — un hex en un componente rompe el criterio 5 de `E3-T2`.
- **Llaves en el state** — no uses `aws_iam_access_key`; la llave se crea a mano tras aprobar.
- **RAG que inventa** — el umbral 0.5 y el "No sé" son obligatorios; no los relajes para que "conteste algo".
- **README que no corre** — el checkpoint de `E3-T6` clona el repo y corre las pruebas en la copia.

## Before moving on

- [ ] Every task in this epic is `done` in `tasks.json` — no task left `in_progress` (o marcado como recortado por decisión del usuario, registrada en la bitácora).
- [ ] Every `verify` command of every task in this epic passed, not just the first one.
- [ ] No `verify` command was edited, and none was skipped because a file it names did not exist.
- [ ] **Every task in this epic has its `checkpoint` tag in version control** — `step-13-ci` … `step-18-documentacion` en `git tag -l 'step-*'`.
- [ ] Gate command passes clean, run from the project root.
- [ ] Every "Produced" contract above exists with the stated signature.
- [ ] No file outside the subtree was modified.
- [ ] `.env.example` ya trae todas las variables de este epic (no se agregó ninguna nueva).
- [ ] One commit per task, each with its task id in the conventional scope, each followed by its checkpoint tag.
