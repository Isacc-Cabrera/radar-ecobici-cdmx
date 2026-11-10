# Autor: Isacc Cabrera
# Descripción: Infraestructura AWS opcional de Radar ECOBICI, descrita pero NO aplicada: un bucket
#   S3 privado, versionado y cifrado para la capa gold; un usuario IAM que solo puede listar y
#   escribir gold/ en ese bucket; y un presupuesto mensual de 5 USD con alertas por correo.
#   No declara la llave de acceso del usuario: quedaría en texto plano en el state; se crea a
#   mano después de aplicar (docs/nube.md). Nunca terraform apply sin la aprobación explícita del usuario.
# Patrón: N/A (infraestructura declarativa)
# Fecha de creación: 2026-11-10
# Cambios:
#     - 2026-11-10 · Isacc Cabrera · Creación (paso 16).

terraform {
  required_version = "~> 1.17"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.72"
    }
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Proyecto = "radar-ecobici-cdmx"
      Entorno  = "demo"
    }
  }
}

# --- Bucket de gold ------------------------------------------------------------------------------

resource "aws_s3_bucket" "gold" {
  bucket        = var.bucket_name
  force_destroy = true # demo: destroy borra el bucket aunque tenga objetos
}

resource "aws_s3_bucket_versioning" "gold" {
  bucket = aws_s3_bucket.gold.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "gold" {
  bucket = aws_s3_bucket.gold.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "gold" {
  bucket = aws_s3_bucket.gold.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# --- Usuario de sincronización con mínimo privilegio ---------------------------------------------

resource "aws_iam_user" "sync" {
  name = "radar-gold-sync"
}

resource "aws_iam_user_policy" "sync" {
  name = "radar-gold-sync-solo-gold"
  user = aws_iam_user.sync.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListarSoloGold"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = aws_s3_bucket.gold.arn
        Condition = {
          StringLike = {
            "s3:prefix" = ["gold/*"]
          }
        }
      },
      {
        Sid      = "LeerEscribirGold"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = "${aws_s3_bucket.gold.arn}/gold/*"
      }
    ]
  })
}

# --- Presupuesto mensual de 5 USD ----------------------------------------------------------------

resource "aws_budgets_budget" "mensual" {
  name         = "radar-ecobici-mensual"
  budget_type  = "COST"
  limit_amount = "5"
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.alert_email]
  }
}

# --- Salidas ---------------------------------------------------------------------------------------

output "bucket_name" {
  description = "Nombre del bucket de gold (va en AWS_S3_BUCKET de .env)."
  value       = aws_s3_bucket.gold.bucket
}

output "bucket_arn" {
  description = "ARN del bucket de gold."
  value       = aws_s3_bucket.gold.arn
}
