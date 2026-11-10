# Radar ECOBICI CDMX

![CI](https://github.com/Isacc-Cabrera/radar-ecobici-cdmx/actions/workflows/ci.yml/badge.svg)

> Pipeline de datos en tiempo real sobre el feed público GBFS de ECOBICI CDMX, predicción de
> bicis disponibles por estación en la próxima hora, API y mapa 3D de la ciudad.

**Autor:** Angel Isacc Cabrera Leal ([@Isacc-Cabrera](https://github.com/Isacc-Cabrera)) ·
Proyecto integrador de Ingeniería de Software (IBERO) · Entrega: 13 de noviembre de 2026.

## Demo

Una columna 3D por estación: la **altura** son las bicis disponibles ahora y el **color** es la
tendencia a una hora (▼ se vacía, ● estable, ▲ se llena). Los anillos rojos (⚠) son anomalías.

*Pendiente:* el GIF (`docs/img/mapa.gif`) y el enlace al video se agregan al grabarlos; el guion
está en [docs/guion-video.md](docs/guion-video.md) y la forma de grabar, en
[docs/frontend.md](docs/frontend.md#capturas-y-gif).

## Problema

Quien usa ECOBICI llega a una estación y no hay bicis, o llega a devolverla y no hay anclajes.
El feed oficial solo dice cómo está cada estación **ahora**; no dice cómo va a estar en una hora,
ni guarda historia para analizar qué estaciones se vacían, a qué hora y por qué. Además, los datos
crudos traen inconsistencias (por ejemplo, estaciones donde bicis + anclajes no suman la capacidad)
que nadie valida.

## Arquitectura

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

| Documento | Contenido |
|---|---|
| [Documento técnico](docs/documento-tecnico.md) | visión completa, decisiones y limitaciones |
| [Manual de usuario](docs/manual-usuario.md) | cómo usar el mapa y leer la predicción |
| [Arquitectura](docs/arquitectura.md) · [Bitácora](docs/bitacora.md) | capas, patrones y el porqué de cada decisión |
| [Data lake](docs/data-lake.md) · [Kafka](docs/kafka.md) · [Métricas](docs/metrics.md) | datos, calidad y streaming |
| [Modelo](docs/modelo.md) · [Anomalías](docs/anomalias.md) | features, fuga de datos, FP y FN |
| [Backend](docs/backend.md) · [Frontend](docs/frontend.md) · [Runbook](docs/runbook.md) | API, mapa y qué hacer ante fallas |
| [Nube](docs/nube.md) · [RAG](docs/rag.md) | Terraform (sin aplicar) y asistente local |
| [Guion](docs/guion-video.md) · [Presentación](docs/presentacion.md) | video de 90 s y 10 diapositivas |

## Cómo correrlo en 5 comandos

Requisitos: Git Bash, [uv](https://docs.astral.sh/uv/), Docker Desktop y Node 22.12 o superior.

```bash
$ git clone https://github.com/Isacc-Cabrera/radar-ecobici-cdmx.git && cd radar-ecobici-cdmx
$ cp .env.example .env && uv sync --all-extras --all-groups
$ docker compose --profile streaming up -d --build --wait kafka collector-kafka bronze-writer
$ docker compose --profile ml up -d --wait mlflow && uv run python scripts/run_hourly.py --hour auto --stage todo && uv run python -m radar.ml.train --json
$ docker compose --profile api up -d --build --wait api && npm --prefix frontend install && npm --prefix frontend run dev
```

Abre http://localhost:5173 (Swagger de la API: http://localhost:8000/docs).

> **El paso 4 necesita al menos unas horas de datos recolectados.** Después del paso 3, deja el
> recolector corriendo: el pipeline solo procesa horas completas (`--hour auto` elige la más
> reciente sin huecos) y el entrenamiento necesita varias horas seguidas. Las pruebas no necesitan
> datos: `uv run pytest`.

## Resultados del modelo

Predicción de bicis por estación a 60 minutos, con validación temporal purgada (4 folds, 47,390
filas, 677 estaciones, 8 horas con cobertura completa). Reporte en
[docs/reportes/comparacion_modelos.md](docs/reportes/comparacion_modelos.md).

| Modelo | MAE | RMSE | R² | MAE entrenamiento | Brecha de sobreajuste |
|---|---|---|---|---|---|
| baseline (persistencia) | 3.018 | 4.950 | 0.510 | 2.982 | 1.2 % |
| baseline_semana | no disponible | | | | menos de 7 días de historia |
| lineal | 3.154 | 4.839 | 0.560 | 2.671 | 15.3 % |
| **lightgbm_a (ganador)** | **2.848** | **4.562** | **0.597** | 2.037 | 28.5 % |
| lightgbm_b | 2.931 | 4.662 | 0.582 | 1.767 | 39.7 % |
| lightgbm_c | 2.990 | 4.693 | 0.578 | 1.379 | 53.9 % |

**El ganador mejora el MAE del baseline en 5.6 %** y lo supera en 3 de 4 folds. Es un resultado
preliminar: el modelo se entrenó con una sola tarde-noche de datos (ver
[limitaciones](docs/modelo.md#limitaciones)).

## Retos cubiertos

| Etapa | Tema | Retos | Estado |
|---|---|---|---|
| 0 | Repo y arquitectura | 7 | ✅ Entregado |
| 1 | Recolector en Docker | 16, 31, Contenedores, 18 | ✅ Entregado |
| 2 | Streaming con Kafka | 30 | ✅ Entregado |
| 3 | Data lake y SQL analítico | 38, 17, 13, 9, 3 capas (parcial) | ✅ Entregado |
| 4 | Orquestación con Airflow | 28 | ✅ Entregado |
| 5 | Modelo y MLflow | 45, 22, 33, 12, 19 | ✅ Entregado |
| 6 | API, pruebas y CI | 47, Backend, Webservice, CI/CD, Pruebas unitarias* | ✅ Entregado |
| 7 | Mapa 3D | Frontend, Mapas* | ✅ Entregado |
| 7 | Tiempo real en el navegador | WebSockets* | ✂️ Recortado: se usa polling cada 60 s |
| 8 | Anomalías y nube | 44, Terraform, 6, 37 | ✅ Entregado; AWS descrito y validado, sin aplicar |
| 9 | Asistente RAG | 39* | ✅ Entregado |
| 10 | Documentación y presentación | 36, 50, 10, 20 | ✅ Entregado; video y GIF pendientes de grabar |

\* Parcial u opcional.

## Trabajo futuro

1. Job de CI para el frontend (typecheck, pruebas y build).
2. Despliegue público del mapa y la API.
3. WebSockets en la API, si se publica en vivo y el polling pesa.
4. Re-entrenamiento programado del modelo en Airflow, con al menos 4 semanas de datos.
5. Generalizar a otros sistemas GBFS.

## Licencia

[MIT](LICENSE) © 2026 Angel Isacc Cabrera Leal.
