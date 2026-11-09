# Radar ECOBICI CDMX

> Pipeline de datos en tiempo real sobre el feed público GBFS de ECOBICI CDMX, predicción de
> bicis disponibles por estación en la próxima hora, API y mapa 3D de la ciudad.

**Autor:** Angel Isacc Cabrera Leal ([@Isacc-Cabrera](https://github.com/Isacc-Cabrera)) ·
Proyecto integrador de Ingeniería de Software (IBERO) · Entrega: 13 de noviembre de 2026.

## Problema

Quien usa ECOBICI llega a una estación y no hay bicis, o llega a devolverla y no hay anclajes.
El feed oficial solo dice cómo está cada estación **ahora**; no dice cómo va a estar en una hora,
ni guarda historia para analizar qué estaciones se vacían, a qué hora y por qué. Además, los datos
crudos traen inconsistencias (por ejemplo, estaciones donde bicis + anclajes no suman la capacidad)
que nadie valida.

## Objetivo

1. Recolectar el estado de las ~680 estaciones cada 60 s sin perder datos (D-005, D-006).
2. Convertirlo en un data lake confiable en tres capas: **bronze** (crudo), **silver** (validado)
   y **gold** (agregados y features).
3. Predecir las bicis disponibles por estación **60 minutos después** y superar a un baseline
   ingenuo.
4. Servir estaciones, predicciones y anomalías por una API y mostrarlas en un mapa 3D.

## Arquitectura

Diagrama y explicación por capa en [docs/arquitectura.md](docs/arquitectura.md).

```text
GBFS → recolector → Kafka → bronze → silver → gold → modelo (MLflow) → API → mapa 3D
                                        ↑ Airflow cada hora (+ clima Open-Meteo)
```

## Stack

| Capa | Elección |
|---|---|
| Lenguaje | Python 3.12 (pipeline, ML, API) · TypeScript + React (mapa) |
| Ingesta | Recolector propio con `httpx`, reintentos con backoff exponencial y jitter |
| Streaming | Apache Kafka en modo KRaft + `confluent-kafka` |
| Data lake | Parquet + DuckDB + polars; S3 local con SeaweedFS |
| Orquestación | Apache Airflow 3 (respaldo: script programado) |
| Machine learning | scikit-learn, LightGBM, MLflow |
| API | FastAPI + Pydantic |
| Mapa | React + Vite + deck.gl + MapLibre (mapa base OpenFreeMap, sin token) |
| Infraestructura | Docker Compose por perfiles · Terraform (S3 + IAM + presupuesto en AWS) |
| Calidad | pytest, ruff, mypy, GitHub Actions |
| Clima | Open-Meteo (sin API key) |

## Estado por etapas

- [x] Etapa 0 — Repo, licencia y arquitectura
- [ ] Etapa 1 — Recolector en Docker (bronze + heartbeat)
- [ ] Etapa 2 — Streaming con Kafka
- [ ] Etapa 3 — Data lake bronze/silver/gold y SQL analítico
- [ ] Etapa 4 — Orquestación con Airflow
- [ ] Etapa 5 — Modelo predictivo + MLflow
- [ ] Etapa 6 — API, pruebas y CI
- [ ] Etapa 7 — Mapa 3D
- [ ] Etapa 8 — Anomalías + Terraform/AWS
- [ ] Etapa 9 — Asistente RAG (opcional)
- [ ] Etapa 10 — Documentación, video y presentación

## Cómo correrlo

Requisitos: Git Bash, [uv](https://docs.astral.sh/uv/), Docker Desktop y Node 24. La preparación
completa del entorno (Python 3.12, dependencias, `.env`) está en el bloque **Bootstrap** de
[blueprints/radar-ecobici-cdmx/blueprint.md](blueprints/radar-ecobici-cdmx/blueprint.md) (§10).
Este README se completa con los 5 comandos de arranque en la Etapa 10.

```bash
cp .env.example .env
uv sync --all-extras --all-groups
uv run pytest
```

## Licencia

[MIT](LICENSE) © 2026 Angel Isacc Cabrera Leal.
