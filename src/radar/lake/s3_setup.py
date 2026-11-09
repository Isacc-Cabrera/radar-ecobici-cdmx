"""
Autor: Isacc Cabrera
Descripción: Lógica de preparación de SeaweedFS que usa scripts/lake_setup.py: rellena las
    llaves S3 vacías de .env con secretos aleatorios, arma las identidades de s3.json (admin con
    todo; radar-app solo Read/Write/List sobre sus tres buckets), crea los buckets con la
    identidad admin y comprueba el mínimo privilegio con la identidad app. La prueba de
    DeleteBucket se hace con un objeto centinela dentro del bucket: si la política estuviera mal,
    S3 respondería BucketNotEmpty en vez de borrar, así que la prueba nunca arriesga datos.
Patrón: Dependency Injection (clientes S3 y generador de secretos inyectados)
Fecha de creación: 2026-11-09
Cambios:
    - 2026-11-09 · Isacc Cabrera · Creación (paso 6).
"""

import secrets
import uuid
from collections.abc import Callable
from typing import Any

from radar.config import Settings

SECRET_KEYS = (
    "S3_ADMIN_ACCESS_KEY",
    "S3_ADMIN_SECRET_KEY",
    "S3_APP_ACCESS_KEY",
    "S3_APP_SECRET_KEY",
)


def fill_blank_keys(
    env_text: str, token: Callable[[], str] = lambda: secrets.token_hex(16)
) -> tuple[str, list[str]]:
    """Rellena las llaves S3 vacías de un ``.env`` (solo esas líneas).

    Args:
        env_text: Contenido actual de ``.env``.
        token: Generador de secretos (inyectable en pruebas).

    Returns:
        ``(texto nuevo, llaves rellenadas)``. Las líneas con valor no se tocan; si una llave no
        aparece, se agrega al final.
    """
    lines = env_text.splitlines()
    filled: list[str] = []
    seen: set[str] = set()
    for i, line in enumerate(lines):
        name, sep, value = line.partition("=")
        name = name.strip()
        if sep and name in SECRET_KEYS:
            seen.add(name)
            if not value.strip():
                lines[i] = f"{name}={token()}"
                filled.append(name)
    for name in SECRET_KEYS:
        if name not in seen:
            lines.append(f"{name}={token()}")
            filled.append(name)
    return "\n".join(lines) + "\n", filled


def s3_config(settings: Settings) -> dict[str, Any]:
    """Identidades de SeaweedFS (formato de ``docker/seaweedfs/s3.json.example``).

    Args:
        settings: Configuración con las llaves ya rellenadas y los nombres de bucket.

    Returns:
        Diccionario listo para ``json.dump``.

    Raises:
        ValueError: Si falta alguna llave.
    """
    creds = (
        settings.s3_admin_access_key,
        settings.s3_admin_secret_key,
        settings.s3_app_access_key,
        settings.s3_app_secret_key,
    )
    if not all(creds):
        raise ValueError("faltan llaves S3 en .env; corre `lake_setup.py render`")
    buckets = (settings.lake_bucket_bronze, settings.lake_bucket_silver, settings.lake_bucket_gold)
    return {
        "identities": [
            {
                "name": "admin",
                "credentials": [{"accessKey": creds[0], "secretKey": creds[1]}],
                "actions": ["Admin", "Read", "Write", "List", "Tagging"],
            },
            {
                "name": "radar-app",
                "credentials": [{"accessKey": creds[2], "secretKey": creds[3]}],
                "actions": [f"{a}:{b}" for b in buckets for a in ("Read", "Write", "List")],
            },
        ]
    }


def lake_buckets(settings: Settings) -> list[str]:
    """Los tres buckets del lake, en orden bronze/silver/gold."""
    return [settings.lake_bucket_bronze, settings.lake_bucket_silver, settings.lake_bucket_gold]


def ensure_buckets(admin: Any, buckets: list[str]) -> dict[str, str]:
    """Crea los buckets que falten (idempotente).

    Args:
        admin: Cliente S3 con la identidad admin.
        buckets: Buckets deseados.

    Returns:
        ``{bucket: "creado" | "ya_existia"}``.
    """
    existing = {b["Name"] for b in admin.list_buckets().get("Buckets", [])}
    result: dict[str, str] = {}
    for bucket in buckets:
        if bucket in existing:
            result[bucket] = "ya_existia"
        else:
            admin.create_bucket(Bucket=bucket)
            result[bucket] = "creado"
    return result


def _error_code(call: Callable[[], Any]) -> str:
    """Ejecuta ``call`` y regresa ``"ok"`` o el código de error S3."""
    from botocore.exceptions import ClientError

    try:
        call()
    except ClientError as exc:
        return str(exc.response.get("Error", {}).get("Code", "desconocido"))
    return "ok"


def _check_bucket(app: Any, bucket: str) -> dict[str, str]:
    """put → get → list → delete_bucket (con el objeto adentro) → delete_object en un bucket."""
    key = f"_check_policy/{uuid.uuid4().hex}.txt"
    body = b"radar check-policy"
    got: dict[str, bytes] = {}
    listed: dict[str, list[str]] = {}

    def _get() -> None:
        got["body"] = app.get_object(Bucket=bucket, Key=key)["Body"].read()

    def _list() -> None:
        page = app.list_objects_v2(Bucket=bucket, Prefix="_check_policy/")
        listed["keys"] = [o["Key"] for o in page.get("Contents", [])]

    res = {"put": _error_code(lambda: app.put_object(Bucket=bucket, Key=key, Body=body))}
    res["get"] = _error_code(_get)
    if res["get"] == "ok" and got.get("body") != body:
        res["get"] = "contenido_distinto"
    res["list"] = _error_code(_list)
    if res["list"] == "ok" and key not in listed.get("keys", []):
        res["list"] = "objeto_no_listado"
    # el objeto sigue adentro: si la política estuviera mal, S3 diría BucketNotEmpty, no borra
    res["delete_bucket"] = _error_code(lambda: app.delete_bucket(Bucket=bucket))
    res["delete_object"] = _error_code(lambda: app.delete_object(Bucket=bucket, Key=key))
    return res


def check_policy(app: Any, buckets: list[str], admin: Any | None = None) -> dict[str, Any]:
    """Prueba el mínimo privilegio de la identidad app.

    Por bucket: put → get (mismo contenido) → list → intento de ``delete_bucket`` con el objeto
    todavía adentro (debe ser ``AccessDenied``) → borra el objeto. Al final intenta
    ``create_bucket`` (debe ser ``AccessDenied``); si por error lo logra, ``admin`` lo limpia.

    Args:
        app: Cliente S3 con la identidad app.
        buckets: Buckets a probar.
        admin: Cliente admin para limpiar un bucket creado por error (opcional).

    Returns:
        ``{"buckets": {...}, "create_bucket": código, "ok": bool}``.
    """
    report: dict[str, Any] = {"buckets": {}}
    ok = True
    for bucket in buckets:
        res = _check_bucket(app, bucket)
        report["buckets"][bucket] = res
        ok = ok and all(res[a] == "ok" for a in ("put", "get", "list", "delete_object"))
        ok = ok and res["delete_bucket"] == "AccessDenied"
    probe = f"radar-check-policy-{uuid.uuid4().hex[:8]}"
    report["create_bucket"] = _error_code(lambda: app.create_bucket(Bucket=probe))
    if report["create_bucket"] == "ok" and admin is not None:
        admin.delete_bucket(Bucket=probe)
    ok = ok and report["create_bucket"] == "AccessDenied"
    report["ok"] = ok
    return report
