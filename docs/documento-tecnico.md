# Documento técnico — Radar ECOBICI CDMX

**Autor:** Angel Isacc Cabrera Leal · Proyecto integrador de Ingeniería de Software (IBERO) ·
Entrega: 13 de noviembre de 2026 · Cifras medidas el 10 de noviembre de 2026.

## Resumen

ECOBICI publica cada minuto cuántas bicis y muelles libres tiene cada una de sus 677 estaciones,
pero solo dice cómo está cada estación **ahora** y no guarda historia. Radar ECOBICI recolecta ese
feed sin perder datos, lo convierte en un data lake confiable (bronze → silver → gold), predice
cuántas bicis habrá en cada estación **dentro de 60 minutos** y lo muestra en una API y un mapa 3D.

| Resultado | Valor |
|---|---|
| Estaciones monitoreadas | 677, cada 60 s |
| Viajes históricos integrados (octubre de 2026) | 1,599,384, sin datos personales en silver |
| Cumplimiento de la regla de capacidad | 99.70 %–99.78 % |
| Modelo ganador | `lightgbm_a`: MAE 2.848 contra 3.018 del baseline (**5.6 % mejor**) |
| Anomalías detectadas en una corrida | 101 estaciones (31 congeladas, 70 por Isolation Forest) |
| Pruebas | 155 de Python y 34 del frontend, con CI en GitHub Actions |
| Costo de operación | 0: todo corre local; AWS está descrito en Terraform y sin aplicar |

## Arquitectura

```mermaid
flowchart LR
  GBFS[(Feed GBFS ECOBICI)] -->|cada 60 s| COL[Recolector Python]
  COL -->|KafkaSink| K[(Kafka KRaft)]
  K -->|grupo bronze-writer| BW[Consumidor]
  BW --> B[(bronze JSON.gz + heartbeat)]
  B --> S[(silver Parquet + cuarentena)]
  S --> G[(gold agregados + features)]
  B & S & G -. sync .-> SW[(SeaweedFS S3)]
  AF[Airflow horario/diario] --> S
  AF --> G
  OM[Open-Meteo] --> AF
  G --> ML[Entrenamiento + MLflow]
  ML -->|radar-bikes-t60@champion| API[FastAPI]
  S --> API
  API --> MAP[Mapa 3D React + deck.gl]
  DOCS[docs/] --> RAG[RAG local Ollama]
```

| Capa | Tecnología | Patrón | Detalle |
|---|---|---|---|
| Ingesta | Python, httpx | Ports & Adapters (`SnapshotSink`), Strategy (`RetryPolicy`) | [arquitectura.md](arquitectura.md) |
| Streaming | Kafka 4 KRaft | at-least-once + escritura idempotente | [kafka.md](kafka.md) |
| Lake | Parquet, polars, DuckDB, SeaweedFS | Template Method, Chain of Responsibility | [data-lake.md](data-lake.md) |
| Orquestación | Airflow 3 standalone | DAGs sin lógica | `dags/`, `scripts/run_hourly.py` |
| Modelo | scikit-learn, LightGBM, MLflow | Strategy (`Predictor`) | [modelo.md](modelo.md) |
| API | FastAPI, Pydantic | Repository, Dependency Injection, Singleton | [backend.md](backend.md) |
| Mapa | React, deck.gl, MapLibre | Container/Presentational, Custom Hooks | [frontend.md](frontend.md) |
| Anomalías | scikit-learn | Strategy (`AnomalyDetector`) | [anomalias.md](anomalias.md) |
| Nube | Terraform (sin aplicar) | mínimo privilegio | [nube.md](nube.md) |
| Asistente | Ollama, chromadb | Ports & Adapters | [rag.md](rag.md) |
| Operación | GitHub Actions, Docker Compose | — | [runbook.md](runbook.md) |

## Decisiones

El qué, las alternativas, el porqué y las consecuencias de cada decisión están en la
[bitácora](bitacora.md).

| ID | Decisión | Por qué, en una línea |
|---|---|---|
| D-001 | Proyecto: Radar ECOBICI CDMX | datos públicos, reales y en vivo, con un problema que se entiende |
| D-002 | Proyecto integral en vez de retos sueltos | una sola historia de punta a punta cubre más retos |
| D-003 | Herramientas gratuitas + alojamiento híbrido | todo local; la nube es opcional y acotada |
| D-004 | Forma de trabajo: modo tutor | reemplazada por D-008 |
| D-005 | `station_status` cada 60 s; `station_information` al día | el modelo predice a 1 h: 10 s daría 5.9 M de filas al día contra ~1 M |
| D-006 | Guardar primero, validar después | si la validación falla, bronze permite reprocesar |
| D-007 | Licencia MIT | portafolio público |
| D-008 | Claude genera el código; el autor lo revisa y lo explica | el tiempo se invierte en entender y decidir |
| D-009 | SeaweedFS en lugar de MinIO | el repositorio de MinIO se archivó |
| D-010 | Mapa base OpenFreeMap | CARTO exige API key |

No hizo falta **D-011** (el respaldo si Airflow no funcionaba): Airflow pasó muy dentro del
time-box de 3 horas.

## Datos y calidad

- **Fuentes:** feed GBFS de ECOBICI (cada 60 s), CSV mensual de viajes de datos abiertos y clima
  horario de Open-Meteo.
- **Bronze** guarda la respuesta cruda y es append-only. **Silver** valida con una cadena de
  chequeos (esquema → estación conocida → regla de capacidad → deduplicación); lo que falla va a
  cuarentena con su motivo. **Gold** construye una rejilla de 5 minutos con el último valor conocido
  (as-of hacia atrás, sin mirar al futuro) y agregados por hora.
- **Regla de capacidad:** `bicis + bicis dañadas + muelles + muelles dañados == capacidad`. Se
  cumple entre el 99.70 % y el 99.78 %. Las violaciones se concentran en pocas estaciones, siempre
  por ±1; la CE-028 Toledo–Tokio es la peor.
- **Deduplicación:** ~78 % de las filas leídas son minutos sin cambios y se descartan.
- **Idempotencia:** cada transformación reemplaza su partición completa y deja un `_manifest.json`
  sin marcas de tiempo: dos corridas dan el mismo manifiesto.
- **Privacidad:** el CSV de viajes trae género, edad e identificador de bici. Se queda solo en
  `data/bronze/trips/` local; silver usa una **lista blanca** de 6 columnas.
- **Benchmark:** sobre 1.6 M de viajes, DuckDB tarda 0.47 s y pandas 0.59 s en Parquet; sobre CSV,
  0.56 s contra 9.8 s. Convertir a Parquet una sola vez es lo que más ayuda.
- **Hallazgo:** las horas en que el recolector corrió solo unos minutos quedan "congeladas" en la
  rejilla (0–4 % de puntos con cambio, contra ~45 % normal). El entrenamiento las excluye.

## Modelo

**Objetivo:** bicis disponibles por estación a t+60 min. Validación temporal con 4 folds
expandibles y **purga** del horizonte (47,390 filas, 677 estaciones, 8 horas con cobertura
completa).

| Modelo | MAE | RMSE | R² | MAE entrenamiento | Brecha de sobreajuste |
|---|---|---|---|---|---|
| baseline (persistencia) | 3.018 | 4.950 | 0.510 | 2.982 | 1.2 % |
| lineal | 3.154 | 4.839 | 0.560 | 2.671 | 15.3 % |
| **lightgbm_a** | **2.848** | **4.562** | **0.597** | 2.037 | 28.5 % |
| lightgbm_b | 2.931 | 4.662 | 0.582 | 1.767 | 39.7 % |
| lightgbm_c | 2.990 | 4.693 | 0.578 | 1.379 | 53.9 % |

- En la primera versión **el baseline ganaba**. Lo que cambió el resultado fue que LightGBM
  aprendiera el **cambio** respecto a las bicis actuales, en lugar del nivel.
- El ganador supera al baseline en 3 de 4 folds; de madrugada gana la persistencia.
- Las configuraciones más complejas memorizan: sube la brecha y empeora la validación.
- Registrado en MLflow como `radar-bikes-t60@champion`, con respaldo en `models/champion.joblib`.
- La revisión de fuga de datos, feature por feature, está en [modelo.md](modelo.md).

## API y mapa

- **API (FastAPI):** `/health`, `/stations`, `/stations/{id}/history`, `/predict/{id}`,
  `/predictions`, `/anomalies` y Swagger en `/docs`. El modelo se carga **una sola vez** al
  arrancar (MLflow → respaldo joblib → sin modelo y 503). `/predictions` tarda ~1 s en frío y ~30 ms
  con caché. Corre en su contenedor.
- **Mapa:** una columna 3D por estación (altura = bicis, color = tendencia a 1 h), panel con
  buscador, historia de 6 horas, anillos de anomalía, tema claro/oscuro y polling cada 60 s. Cumple
  contraste WCAG AA, se opera con teclado y respeta el movimiento reducido.
- **Anomalías:** tres detectores (congelada, z-score, Isolation Forest) con falsos positivos y
  negativos documentados con estaciones reales en [anomalias.md](anomalias.md).

## Limitaciones

- **Poca historia.** El modelo se entrenó con 8 horas útiles de una tarde-noche: no ha visto una
  mañana completa ni un fin de semana. El 5.6 % es honesto, pero preliminar.
- **Sin rebalanceo.** Los camiones que redistribuyen bicis no aparecen en ningún dato.
- **Corre en una laptop.** Si se suspende, se pierden esas horas; el pipeline las detecta y excluye.
- **Z-score sin datos todavía.** Necesita 3 días por hora local; hoy no marca ninguna estación.
- **Isolation Forest con falsos positivos.** Marca la hora pico porque nunca vio esas horas.
- **Frescura.** La API sirve lo último procesado por el pipeline horario, no el minuto actual.
- **AWS sin aplicar.** La infraestructura está validada, pero nunca se desplegó.
- **El CI no cubre el frontend.** Sus pruebas corren solo en local.

## Trabajo futuro

1. Job de CI para el frontend (typecheck, pruebas y build).
2. Despliegue público del mapa y la API.
3. WebSockets en la API, si se publica en vivo y el polling pesa.
4. Re-entrenamiento programado en Airflow, con al menos 4 semanas de datos.
5. Generalizar a otros sistemas GBFS.
6. Detector "fuera de servicio" a partir de `is_renting` y calibrar el umbral del asistente RAG.
