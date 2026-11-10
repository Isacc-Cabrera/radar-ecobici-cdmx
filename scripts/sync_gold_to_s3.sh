#!/usr/bin/env bash
# Autor: Isacc Cabrera
# Descripción: Sube los Parquet de la capa gold local al bucket S3 de AWS (gold/). Solo gold:
#   nunca bronze (viajes crudos con datos personales) ni silver. Requiere haber aplicado
#   infra/terraform con aprobación, AWS CLI v2 con el perfil AWS_PROFILE y AWS_S3_BUCKET en .env.
#   aws s3 sync solo sube lo que cambió (idempotente). Genera un costo mínimo (ver docs/nube.md).
# Patrón: N/A
# Fecha de creación: 2026-11-12
# Cambios:
#     - 2026-11-12 · Isacc Cabrera · Creación (paso 16).
set -euo pipefail

if [[ ! -f .env ]]; then
  echo "No existe .env: corre este script desde la raíz del repo." >&2
  exit 2
fi
set -a
# shellcheck disable=SC1091
. ./.env
set +a

if [[ -z "${AWS_S3_BUCKET:-}" ]]; then
  echo "AWS_S3_BUCKET está vacío en .env: aplica Terraform primero (docs/nube.md)." >&2
  exit 2
fi
if [[ ! -d data/gold ]]; then
  echo "No existe data/gold: corre el pipeline antes de sincronizar." >&2
  exit 2
fi

aws s3 sync data/gold "s3://${AWS_S3_BUCKET}/gold" \
  --profile "${AWS_PROFILE}" \
  --exclude "*" --include "*.parquet"
