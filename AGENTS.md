# Radar ECOBICI CDMX — instrucciones para agentes

Pipeline en tiempo real del feed GBFS de ECOBICI CDMX (recolector → Kafka → lake bronze/silver/gold)
+ predicción de bicis disponibles a 60 min + API FastAPI + mapa 3D. Proyecto escolar y de
portafolio de Angel Isacc Cabrera Leal (MIT). Idioma del proyecto: español (es-MX).

## Comandos (Git Bash, desde la raíz)

| Tarea | Comando |
|---|---|
| Entorno | `uv sync --all-extras --all-groups` |
| Gate Python | `uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest` |
| Recolector | `docker compose up -d collector` |
| Streaming | `docker compose --profile streaming up -d --wait kafka collector-kafka bronze-writer` |
| API | `docker compose --profile api up -d --build --wait api` |
| Frontend | `npm --prefix frontend run typecheck && npm --prefix frontend run test && npm --prefix frontend run build` |
| Terraform | `terraform -chdir=infra/terraform validate` (nunca `apply` sin aprobación) |

## No negociable

1. Bronze es append-only: ningún código borra o reescribe un archivo bronze.
2. Los viajes crudos con PII nunca salen de `data/bronze/trips/` local.
3. Nunca `terraform apply`, LLM de pago ni `git push` sin el "sí" explícito del usuario.
4. Nunca commit de secretos, `.env`, `data/`, `*.tfstate*`, `mlruns/`, `mlartifacts/`.
5. Nunca `gh auth switch` ni `git config --global`.
6. Todo archivo fuente lleva el encabezado obligatorio (Autor, Descripción, Patrón, Fecha, Cambios).

Arquitectura, límites entre capas, decisiones D-005…D-010 y sistema de diseño: `CLAUDE.md` en este
directorio (fuente de verdad). Orden de construcción: `blueprints/radar-ecobici-cdmx/tasks.json`.
