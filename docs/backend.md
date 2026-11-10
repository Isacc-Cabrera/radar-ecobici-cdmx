# Manual del backend (API FastAPI)

La API sirve el estado de las estaciones, su historia, las predicciones a 60 minutos y las
anomalías. La consume el mapa 3D (`frontend/`, http://localhost:5173). Swagger interactivo en
**http://localhost:8000/docs** (y el esquema en `/openapi.json`).

## Arquitectura

```mermaid
flowchart LR
    req[Petición HTTP] --> main[main.py<br/>rutas + validación]
    main -->|Depends| deps[deps.py<br/>ModelHolder · get_repository]
    main --> schemas[schemas.py<br/>contrato Pydantic]
    deps --> repo[repository.py<br/>StationRepository]
    repo --> lake[(data/silver · gold<br/>Parquet)]
    deps --> mlflow[(MLflow<br/>radar-bikes-t60@champion)]
    deps -.respaldo.-> joblib[(models/champion.joblib)]
```

| Archivo | Responsabilidad | Patrón |
|---|---|---|
| `src/radar/api/schemas.py` | Modelos Pydantic de cada respuesta (el contrato de `blueprint.md` §5) | — |
| `src/radar/api/repository.py` | Lee el lake: estado actual, historia y marco de inferencia, con caché de 30 s | Repository |
| `src/radar/api/deps.py` | `ModelHolder` (carga el modelo) y proveedores `get_*` para `Depends` | Dependency Injection + Singleton |
| `src/radar/api/main.py` | `create_app`: lifespan, CORS, rutas y errores | Dependency Injection |

- **Las rutas no saben que los datos son Parquet.** Todo pasa por `StationRepository`; cambiar el
  almacenamiento (por ejemplo, a una base de datos) no toca `main.py`.
- **"Ahora" es el último dato en silver, no el reloj.** Si el recolector se detuvo, la API sigue
  respondiendo con lo último que vio y `/health` dice qué tan viejo es (`segundos_desde_ultimo_dato`).
- **La rejilla de inferencia se ancla en el último dato** (now, now − 5 min, …), así
  `bicis_actuales` de `/predict` coincide con `bicis_disponibles` de `/stations` y los lags de
  15/30/60 min caen exactamente en puntos de la rejilla.

## Cómo se carga el modelo

Deserializar el modelo cuesta cientos de milisegundos; hacerlo en cada petición multiplicaría la
latencia. Por eso se carga **una sola vez** en el `lifespan` de FastAPI y se guarda en
`app.state.model_holder`; cada ruta lo recibe con `Depends(get_model_holder)`.

`ModelHolder.load(settings)` intenta, en orden:

1. **MLflow:** una sonda a `GET {MLFLOW_TRACKING_URI}/health` con 2 s de límite (para no quedarse
   esperando los reintentos internos de MLflow si el servidor está caído) y luego
   `mlflow.pyfunc.load_model("models:/radar-bikes-t60@champion")` → `fuente_modelo="mlflow"`,
   `modelo_version="mlflow:<versión>"`.
2. **Respaldo local:** `joblib.load(MODEL_FALLBACK_PATH)` (`models/champion.joblib`, lo escribe
   `radar.ml.train`) → `fuente_modelo="archivo"`.
3. **Sin modelo:** la API arranca igual; `/health` lo reporta y `/predict` responde 503
   `modelo_no_disponible`. El resto de las rutas sigue funcionando.

## Cómo correrla

| Modo | Comando | Notas |
|---|---|---|
| Contenedor (recomendado) | `docker compose --profile api up -d --build --wait mlflow api` | `api` no depende de `mlflow`: si no está arriba usa el respaldo joblib; monta `data/` y `models/` en solo lectura |
| Local | `uv run uvicorn radar.api.main:app --port 8000 --reload` | lee `.env` (`DATA_DIR`, `MLFLOW_TRACKING_URI`, `MODEL_FALLBACK_PATH`) |
| Pruebas | `uv run pytest tests/integration/test_api.py` | `TestClient` con repositorio y modelo falsos, sin red |

Variables que usa (`.env`, leídas por `radar.config.Settings`): `DATA_DIR`,
`MLFLOW_TRACKING_URI`, `MODEL_NAME`, `MODEL_FALLBACK_PATH`, `API_CORS_ORIGINS`, `LOG_LEVEL`.
CORS: solo `GET` desde `API_CORS_ORIGINS` (`http://localhost:5173`).

## Errores

Siempre `{"detail": {"codigo": "...", "mensaje": "..."}}`, salvo 422, que conserva el formato de
validación de FastAPI.

| Código HTTP | `codigo` | Cuándo |
|---|---|---|
| 404 | `estacion_no_encontrada` | el `station_id` no existe en `station_information` |
| 422 | (formato FastAPI) | parámetro inválido, p. ej. `hours` fuera de 1–24 |
| 503 | `modelo_no_disponible` | no se cargó ningún modelo |
| 503 | `datos_no_disponibles` | no hay silver, o la estación no tiene datos recientes |

## Ejemplos con respuestas reales

Capturados del contenedor el 10 de noviembre de 2026 (datos hasta las 15:50 UTC, modelo
`mlflow:1`). Las listas largas se recortan.

### `GET /health`

```bash
curl -s localhost:8000/health
```

```json
{"status":"ok","modelo_cargado":true,"fuente_modelo":"mlflow","segundos_desde_ultimo_dato":2371.6}
```

### `GET /stations`

```bash
curl -s localhost:8000/stations
```

```json
[
  {
    "station_id": "1",
    "nombre": "CE-710 Molino del Rey - Glorieta de la Lealtad",
    "lat": 19.416795,
    "lon": -99.192508,
    "capacidad": 39,
    "bicis_disponibles": 16,
    "docks_disponibles": 23,
    "ultimo_reporte_utc": "2026-11-10T15:50:33Z",
    "anomalia": false
  },
  "... 677 estaciones en total"
]
```

### `GET /stations/{station_id}/history?hours=1`

```bash
curl -s "localhost:8000/stations/313/history?hours=1"
```

```json
[
  {"ts_utc": "2026-11-10T15:00:25Z", "bicis_disponibles": 0, "docks_disponibles": 75},
  {"ts_utc": "2026-11-10T15:08:25Z", "bicis_disponibles": 1, "docks_disponibles": 74},
  "... 10 puntos (solo los cambios de estado)"
]
```

```bash
curl -s "localhost:8000/stations/313/history?hours=0"
```

```json
{"detail":[{"type":"greater_than_equal","loc":["query","hours"],"msg":"Input should be greater than or equal to 1","input":"0","ctx":{"ge":1}}]}
```

### `GET /predict/{station_id}`

```bash
curl -s localhost:8000/predict/313
```

```json
{"station_id":"313","bicis_actuales":0,"prediccion_t60":1.98,"delta":1.98,"clase":"estable","generado_utc":"2026-11-10T16:38:52.676919Z","modelo_version":"mlflow:1"}
```

`clase`: `se_vacia` si `delta <= -3`, `se_llena` si `delta >= 3`, si no `estable`. La predicción
se recorta a `[0, capacidad]`.

```bash
curl -s localhost:8000/predict/no-existe
```

```json
{"detail":{"codigo":"estacion_no_encontrada","mensaje":"no existe la estación no-existe"}}
```

### `GET /predictions`

```bash
curl -s localhost:8000/predictions
```

Una `PredictionOut` por estación con datos recientes; en esa captura, 677 predicciones: 604
`estable`, 47 `se_vacia` y 26 `se_llena`. Tarda ~1 s la primera vez (arma el marco de
inferencia) y ~30 ms mientras dura la caché de 30 s.

### `GET /anomalies`

```bash
curl -s localhost:8000/anomalies
```

```json
[]
```

Vacía hasta el paso 15, cuando se conectan los detectores de anomalías.
