# Arquitectura — Radar ECOBICI CDMX

> Autor: Isacc Cabrera · Creado: 2026-11-09 · Se mantiene actualizado al cerrar cada etapa.

## Diagrama

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

## Capas

| Capa | Qué hace | Decisión clave |
|---|---|---|
| **Ingesta** | El recolector descubre las URLs desde `gbfs.json`, pide `station_status` cada 60 s y `station_information` una vez al día. | D-005: 60 s y no 10 s (≈1 M vs 5.9 M filas/día); el modelo predice a 1 h. |
| **Streaming** | El recolector publica a Kafka: un mensaje por estación (key = `station_id`, 3 particiones) y el snapshot crudo para bronze. | Desacopla captura de escritura; el commit del offset va **después** de escribir (at-least-once + escritura idempotente). |
| **Bronze** | Respuesta cruda tal cual llega, comprimida, particionada por `dt=`/`hour=`, solo se agrega. Incluye el heartbeat de cada ejecución. | D-006: guardar primero, validar después; el heartbeat distingue "sin cambios" de "recolector caído". |
| **Silver** | JSON aplanado a Parquet con tipos, `station_id` validado, regla de capacidad y deduplicación. Lo que falla va a **cuarentena**, no se borra. | Cadena de checks (Chain of Responsibility). |
| **Gold** | Agregados por estación y hora + tabla de features (lags, rolling means, clima, demanda histórica). | Grano único por métrica (`docs/metrics.md`). |
| **Orquestación** | Airflow corre cada hora bronze→silver→gold + clima + checks; un DAG diario refresca `station_information`. | Tareas idempotentes; respaldo con script programado. |
| **Modelo** | Baseline vs regresión lineal vs LightGBM con validación temporal; registro en MLflow. | Nunca se valida al azar en series de tiempo. |
| **Servicio** | FastAPI sirve estaciones, predicciones y anomalías; el modelo se carga una sola vez al arrancar. | Repository + inyección de dependencias. |
| **Mapa** | Columnas 3D por estación: altura = bicis disponibles, color = predicción; modo claro y oscuro en verdes. | Mapa base OpenFreeMap sin token (D-010). |
| **Nube (opcional)** | Terraform crea un bucket S3, un usuario IAM con permisos mínimos y una alerta de presupuesto de 5 USD. | Solo gold sube a AWS; `terraform destroy` al terminar la demo. |

## Patrones de diseño

| Patrón | Dónde | Para qué |
|---|---|---|
| Ports & Adapters (hexagonal) | `SnapshotSink` (local, Kafka, S3) y `LakeStore` (local, S3) | Cambiar el destino sin tocar la lógica del recolector ni del lake. |
| Factory | `make_sink(settings)`, `make_store(settings)` | Elegir el adaptador según `.env`. |
| Strategy | `RetryPolicy` (backoff + jitter + `Retry-After`) y `Predictor` (baseline, lineal, LightGBM) | Intercambiar algoritmos con la misma interfaz. |
| Chain of Responsibility | Checks de calidad de silver | Cada check decide y pasa al siguiente; los rechazos van a cuarentena. |
| Template Method | Jobs de transformación (extract → validate → load) | Mismo esqueleto idempotente por partición. |
| Repository | Lectura de gold/silver desde la API | La API no sabe si los datos vienen de Parquet o S3. |
| Inyección de dependencias + Singleton | `Depends` de FastAPI y modelo cargado en `lifespan` | Probar con dobles y no recargar el modelo en cada petición. |
| Container/Presentational + hooks | Mapa en React (`useStations`, `useTheme`) | Separar datos de presentación. |

## Estructura del repositorio

```text
src/radar/      paquete Python: collector, streaming, lake, ml, api, anomalies, rag
dags/           DAGs de Airflow
docker/         Dockerfiles y configuración de SeaweedFS
frontend/       mapa 3D (React + Vite + deck.gl)
infra/          Terraform (AWS)
notebooks/      EDA
scripts/        utilidades y demos
sql/            consultas analíticas
tests/          pruebas unitarias e integración
docs/           documentación, bitácora y reportes
data/           lake local (ignorado en git)
```

**Por qué esta estructura:** un solo paquete instalable (`src/radar`) con un subpaquete por etapa
del plan, para que cada etapa agregue código sin reorganizar lo anterior y las pruebas importen el
paquete igual que los contenedores. El layout `src/` evita importar por accidente el código sin
instalar. Lo que no es Python (DAGs, frontend, infraestructura) vive en carpetas propias porque
tiene su propio ciclo de vida y herramientas.
