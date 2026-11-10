# Nube (AWS) — opcional y con aprobación

> ⚠️ **Nada de esto está aplicado.** `infra/terraform/` describe la infraestructura y se valida
> sin costo (`fmt`, `init -backend=false`, `validate`). Ningún comando de este proyecto corre
> `terraform apply` sin el "sí" explícito del usuario. Nunca se usan servicios administrados
> caros (MSK, MWAA).

El proyecto funciona completo en local (SeaweedFS hace de S3). La nube es solo para mostrar que
la capa **gold** puede vivir en S3 con seguridad y con el gasto acotado.

## Recursos

| Recurso Terraform | Qué es | Configuración |
|---|---|---|
| `aws_s3_bucket.gold` | bucket para la capa gold | `bucket = var.bucket_name`, `force_destroy = true` (demo) |
| `aws_s3_bucket_versioning.gold` | versiones de cada objeto | `Enabled`: un sync equivocado se puede revertir |
| `aws_s3_bucket_public_access_block.gold` | bloqueo de acceso público | los cuatro `block_*` / `ignore_*` / `restrict_*` en `true` |
| `aws_s3_bucket_server_side_encryption_configuration.gold` | cifrado en reposo | `AES256` (SSE-S3, sin costo extra) |
| `aws_iam_user.sync` | usuario `radar-gold-sync` | solo para `scripts/sync_gold_to_s3.sh` |
| `aws_iam_user_policy.sync` | su política (ver Mínimo privilegio) | listar `gold/*`, leer y escribir `gold/*`; nada más |
| `aws_budgets_budget.mensual` | presupuesto de **5 USD/mes** | alertas por correo: gasto real > 80 % y pronóstico > 100 % |

Variables (`infra/terraform/variables.tf`): `region` (`us-east-1`), `bucket_name` y `alert_email`
(sensible). Los valores reales van en `infra/terraform/terraform.tfvars`, ignorado por git:

```hcl
bucket_name = "radar-ecobici-gold-<sufijo-unico>"
alert_email = "<tu-correo>"
```

**Lo que NO está en Terraform, a propósito:** `aws_iam_access_key`. Si Terraform creara la llave,
el secreto quedaría en texto plano en `terraform.tfstate`. La llave se crea a mano después de
aplicar (paso 4 de "Cómo aplicar").

**State:** es el archivo donde Terraform recuerda qué creó y con qué IDs. Es local
(`infra/terraform/terraform.tfstate`) e ignorado por git, porque puede contener datos sensibles.
En cambio `.terraform.lock.hcl` **sí** se versiona: fija la versión exacta del provider de AWS
para que `init` instale lo mismo en cualquier máquina.

## Costos estimados

Estimación para el tamaño real de la demo: `data/gold` pesa **≈ 0.4 MB** (5 archivos Parquet) y
crece unos pocos MB por semana. Precios de lista de `us-east-1` (noviembre de 2026);
**verificar en https://aws.amazon.com/s3/pricing/ y https://aws.amazon.com/aws-cost-management/aws-budgets/pricing/ antes de aplicar.**

| Concepto | Precio de lista | Uso estimado de la demo | Costo/mes |
|---|---|---|---|
| S3 Standard, almacenamiento | ≈ 0.023 USD por GB-mes | < 1 GB (con versiones) | < 0.03 USD |
| S3 PUT/LIST | ≈ 0.005 USD por 1,000 | unos cientos por sync | < 0.01 USD |
| S3 GET | ≈ 0.0004 USD por 1,000 | lecturas ocasionales | ≈ 0 |
| Transferencia de entrada | gratis | sync desde la laptop | 0 |
| IAM (usuario y política) | gratis | — | 0 |
| AWS Budgets | los presupuestos de monitoreo con alertas por correo no tienen costo (los "action-enabled" a partir del tercero sí) | 1 presupuesto con 2 alertas | 0 |
| **Total** | | | **centavos al mes** |

El presupuesto de 5 USD es la red de seguridad: si algo se configura mal (por ejemplo, subir
bronze por error o un bucle de sync), el correo llega antes de que el gasto crezca.

## Cómo aplicar (requiere aprobación)

> Antes del paso 2, se presenta la lista de recursos de arriba con su costo y se espera el "sí"
> del usuario. Requisitos: cuenta de AWS, AWS CLI v2 con el perfil `radar-ecobici`
> (`aws configure --profile radar-ecobici`) y Terraform `~> 1.17`.

1. Preparar y revisar (sin costo):
   ```bash
   terraform -chdir=infra/terraform init
   terraform -chdir=infra/terraform plan -out=plan.tfplan
   ```
   El plan debe mostrar **7 recursos por crear** y ninguno por cambiar o destruir.
2. Aplicar (crea recursos; costo de centavos):
   ```bash
   AWS_PROFILE=radar-ecobici terraform -chdir=infra/terraform apply plan.tfplan
   ```
3. Copiar el nombre del bucket a `.env`:
   ```bash
   terraform -chdir=infra/terraform output -raw bucket_name   # → AWS_S3_BUCKET=...
   ```
4. Crear la llave del usuario de sync **a mano** (no queda en el state) y guardarla en un perfil
   propio de AWS CLI, nunca en git:
   ```bash
   aws iam create-access-key --user-name radar-gold-sync --profile radar-ecobici
   aws configure --profile radar-gold-sync    # pegar la llave; luego AWS_PROFILE=radar-gold-sync en .env
   ```
5. Confirmar el correo de AWS Budgets (llega un mensaje de suscripción a `alert_email`).
6. Sincronizar gold:
   ```bash
   bash scripts/sync_gold_to_s3.sh
   ```
   Solo sube `data/gold/**/*.parquet` al prefijo `gold/` y solo lo que cambió.

## terraform destroy

Al terminar la demo, borrar todo para que el costo vuelva a cero:

1. Borrar la llave de acceso creada a mano (Terraform no la conoce y no podría borrar el usuario):
   ```bash
   aws iam list-access-keys --user-name radar-gold-sync --profile radar-ecobici
   aws iam delete-access-key --user-name radar-gold-sync --access-key-id <AKIA...> --profile radar-ecobici
   ```
2. Destruir (con `force_destroy = true` el bucket se vacía solo, incluidas las versiones):
   ```bash
   AWS_PROFILE=radar-ecobici terraform -chdir=infra/terraform destroy
   ```
3. Revisar en la consola de AWS → Billing que no quede nada con costo, y vaciar
   `AWS_S3_BUCKET` en `.env`.

## Mínimo privilegio

El usuario `radar-gold-sync` solo puede hacer tres cosas, todas dentro de `gold/` de un bucket:

| Acción | Recurso | Para qué |
|---|---|---|
| `s3:ListBucket` | el bucket, con condición `s3:prefix = gold/*` | que `aws s3 sync` sepa qué ya existe |
| `s3:GetObject` | `arn:aws:s3:::<bucket>/gold/*` | comparar y descargar gold |
| `s3:PutObject` | `arn:aws:s3:::<bucket>/gold/*` | subir los Parquet |

No puede borrar objetos, leer otros prefijos, tocar otros buckets, cambiar políticas ni crear
recursos. Si la llave se filtra, el daño máximo es escribir Parquet en `gold/` de ese bucket, y el
versionado permite recuperar cualquier versión anterior. La app local nunca necesita permisos de
AWS: lee el lake desde `data/` y SeaweedFS.
