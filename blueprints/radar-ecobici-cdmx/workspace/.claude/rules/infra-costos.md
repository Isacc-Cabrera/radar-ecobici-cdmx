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
