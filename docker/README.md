# docker/

Dockerfiles por servicio (contexto de build = raíz del repo; ver `.dockerignore`).

- `collector.Dockerfile` — recolector, productor Kafka y consumidor `bronze-writer` (misma imagen).
- `airflow.Dockerfile` — Airflow 3 con un venv aislado del proyecto en `/opt/radar/.venv`.
- `api.Dockerfile` — API FastAPI con el modelo.
- `seaweedfs/s3.json.example` — formato de identidades S3; el real (`s3.json`) se genera y se ignora.
