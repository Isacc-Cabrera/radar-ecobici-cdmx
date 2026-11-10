# Autor: Isacc Cabrera
# Descripción: Variables de la infraestructura AWS de Radar ECOBICI. Los valores reales van en
#   infra/terraform/terraform.tfvars, que está ignorado por git (el correo es dato personal).
# Patrón: N/A (infraestructura declarativa)
# Fecha de creación: 2026-11-12
# Cambios:
#     - 2026-11-12 · Isacc Cabrera · Creación (paso 16).

variable "region" {
  description = "Región de AWS donde vive el bucket de gold."
  type        = string
  default     = "us-east-1"
}

variable "bucket_name" {
  description = "Nombre global y único del bucket S3 para gold (p. ej. radar-ecobici-gold-<sufijo>)."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$", var.bucket_name))
    error_message = "bucket_name debe tener 3-63 caracteres: minúsculas, números, puntos o guiones."
  }
}

variable "alert_email" {
  description = "Correo que recibe las alertas del presupuesto mensual de 5 USD."
  type        = string
  sensitive   = true
}
