# infra/

Infraestructura como código para AWS con Terraform (paso 16). **Genera costos**: no se ejecuta
`terraform apply` sin aprobación explícita. El state es local y está ignorado en git.

- `terraform/` — bucket S3 (versionado, cifrado, sin acceso público), usuario IAM de mínimo
  privilegio y alerta de presupuesto de 5 USD.
