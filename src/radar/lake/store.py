"""
Autor: Isacc Cabrera
Descripción: Almacenamiento del data lake. El puerto LakeStore define cómo se guarda un archivo
    bajo una llave ("dt=.../hour=.../x.json.gz"); hay dos adaptadores: LocalLakeStore (disco) y
    S3LakeStore (cualquier API S3: SeaweedFS hoy, AWS mañana, solo cambia el endpoint). put_file
    es idempotente: si la llave ya existe con el mismo tamaño regresa "omitido" y no sube nada.
    También arma la lista de archivos de una capa para sincronizar, excluyendo en código los
    viajes crudos (bronze/trips, PII) y el heartbeat del día en curso (archivo que sigue
    creciendo), y la función que sincroniza esa lista.
Patrón: Ports & Adapters (LakeStore con LocalLakeStore y S3LakeStore) + Factory
    (make_lake_store, make_s3_client) + Dependency Injection (cliente S3 inyectado)
Fecha de creación: 2026-11-09
Cambios:
    - 2026-11-09 · Isacc Cabrera · Creación (paso 6).
"""

import os
import shutil
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol

from radar.config import Settings

PutResult = Literal["subido", "omitido"]
Layer = Literal["bronze", "silver", "gold"]
Backend = Literal["local", "s3"]
Identity = Literal["admin", "app"]

LAYERS: tuple[Layer, ...] = ("bronze", "silver", "gold")
TRIPS_PREFIX = "trips/"


class LakeStore(Protocol):
    """Puerto: dónde y cómo se guardan los archivos de una capa del lake."""

    def put_file(self, local: Path, key: str) -> PutResult:
        """Guarda ``local`` bajo ``key``.

        Args:
            local: Archivo de origen.
            key: Llave destino con ``/`` como separador.

        Returns:
            ``"subido"`` si se escribió; ``"omitido"`` si ya existía con el mismo tamaño.
        """
        ...

    def exists(self, key: str) -> bool:
        """Indica si ``key`` ya está guardada."""
        ...

    def list_keys(self, prefix: str = "") -> list[str]:
        """Llaves guardadas que empiezan con ``prefix``, ordenadas."""
        ...


class LocalLakeStore:
    """Adaptador de disco: la llave es una ruta relativa a ``root``.

    Args:
        root: Directorio raíz de la capa.
    """

    def __init__(self, root: Path) -> None:
        self.root = root

    def _path(self, key: str) -> Path:
        return self.root.joinpath(*key.split("/"))

    def put_file(self, local: Path, key: str) -> PutResult:
        """Copia atómica (temporal + fsync + replace). Ver ``LakeStore``."""
        dest = self._path(key)
        if dest.exists() and dest.stat().st_size == local.stat().st_size:
            return "omitido"
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(dest.name + ".tmp")
        with open(local, "rb") as src, open(tmp, "wb") as out:
            shutil.copyfileobj(src, out)
            out.flush()
            os.fsync(out.fileno())
        os.replace(tmp, dest)
        return "subido"

    def exists(self, key: str) -> bool:
        """Ver ``LakeStore``."""
        return self._path(key).is_file()

    def list_keys(self, prefix: str = "") -> list[str]:
        """Ver ``LakeStore``."""
        if not self.root.exists():
            return []
        keys = (p.relative_to(self.root).as_posix() for p in self.root.rglob("*") if p.is_file())
        return sorted(k for k in keys if k.startswith(prefix) and not k.endswith(".tmp"))


class S3LakeStore:
    """Adaptador S3 (SeaweedFS o AWS): un bucket por capa.

    Args:
        bucket: Bucket de la capa.
        client: Cliente ``boto3`` de S3 (inyectado; en pruebas, un falso).
    """

    def __init__(self, bucket: str, client: Any) -> None:
        self.bucket = bucket
        self._client = client

    def _size(self, key: str) -> int | None:
        from botocore.exceptions import ClientError

        try:
            head = self._client.head_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
                return None
            raise
        return int(head["ContentLength"])

    def put_file(self, local: Path, key: str) -> PutResult:
        """Sube con ``upload_file`` salvo que ya exista con el mismo tamaño. Ver ``LakeStore``."""
        if self._size(key) == local.stat().st_size:
            return "omitido"
        self._client.upload_file(str(local), self.bucket, key)
        return "subido"

    def exists(self, key: str) -> bool:
        """Ver ``LakeStore``."""
        return self._size(key) is not None

    def list_keys(self, prefix: str = "") -> list[str]:
        """Ver ``LakeStore``."""
        paginator = self._client.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        return sorted(keys)


def bucket_for(settings: Settings, layer: Layer) -> str:
    """Bucket configurado para una capa.

    Args:
        settings: Configuración con ``lake_bucket_*``.
        layer: ``bronze``, ``silver`` o ``gold``.

    Returns:
        El nombre del bucket.
    """
    return {
        "bronze": settings.lake_bucket_bronze,
        "silver": settings.lake_bucket_silver,
        "gold": settings.lake_bucket_gold,
    }[layer]


def make_s3_client(settings: Settings, identity: Identity = "app") -> Any:
    """Fabrica un cliente S3 de boto3 para SeaweedFS/AWS.

    Args:
        settings: Endpoint, región y credenciales.
        identity: ``"app"`` (mínimo privilegio, la de todo el pipeline) o ``"admin"``
            (solo ``lake_setup.py buckets``).

    Returns:
        Cliente ``boto3`` con firma s3v4 y direccionamiento path-style.
    """
    import boto3
    from botocore.config import Config

    if identity == "admin":
        key, secret = settings.s3_admin_access_key, settings.s3_admin_secret_key
    else:
        key, secret = settings.s3_app_access_key, settings.s3_app_secret_key
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        region_name=settings.s3_region,
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def make_lake_store(
    settings: Settings, layer: Layer, backend: Backend, client: Any = None
) -> LakeStore:
    """Fábrica del adaptador ``LakeStore`` según el backend.

    Args:
        settings: Configuración.
        layer: Capa del lake.
        backend: ``"local"`` (``data/<capa>``) o ``"s3"`` (bucket de la capa).
        client: Cliente S3 a usar; si es None se crea uno con la identidad app.

    Returns:
        ``LocalLakeStore`` o ``S3LakeStore``.

    Raises:
        ValueError: Si el backend no existe.
    """
    if backend == "local":
        return LocalLakeStore(settings.data_dir / layer)
    if backend == "s3":
        return S3LakeStore(bucket_for(settings, layer), client or make_s3_client(settings))
    raise ValueError(f"backend desconocido: {backend}")


def layer_files(settings: Settings, layer: Layer, now: datetime | None = None) -> list[str]:
    """Llaves de ``data/<capa>`` que se deben sincronizar.

    Excluye, en código y sin flag para desactivarlo: todo ``bronze/trips/`` (viajes crudos con
    datos personales, nunca salen de la máquina), los temporales ``*.tmp`` y el heartbeat del día
    UTC en curso (sigue creciendo; se sube cuando el día cierra).

    Args:
        settings: Configuración con ``data_dir``.
        layer: Capa a recorrer.
        now: Hora actual (inyectable en pruebas).

    Returns:
        Llaves relativas a la capa, ordenadas.
    """
    today = (now or datetime.now(UTC)).strftime("%Y-%m-%d")
    open_heartbeat = f"heartbeat/dt={today}.jsonl"
    keys = LocalLakeStore(settings.data_dir / layer).list_keys()
    return [
        k
        for k in keys
        if not (layer == "bronze" and k.startswith(TRIPS_PREFIX))
        and not (layer == "bronze" and k == open_heartbeat)
    ]


@dataclass
class SyncResult:
    """Resumen de una pasada de sincronización.

    Attributes:
        subidos: Llaves que se subieron en esta pasada.
        omitidos: Cuántas llaves ya estaban con el mismo tamaño.
    """

    subidos: list[str] = field(default_factory=list)
    omitidos: int = 0

    def resumen(self) -> dict[str, int]:
        """Conteos para la salida JSON (``claves_trips`` debe ser siempre 0)."""
        return {
            "subidos": len(self.subidos),
            "omitidos": self.omitidos,
            "claves_trips": sum(TRIPS_PREFIX in k for k in self.subidos),
        }


def sync_files(store: LakeStore, root: Path, keys: Iterable[str]) -> SyncResult:
    """Sube cada llave de ``root`` al ``store`` (idempotente).

    Args:
        store: Destino.
        root: Directorio local de la capa.
        keys: Llaves relativas a ``root``.

    Returns:
        Qué se subió y cuánto se omitió.
    """
    result = SyncResult()
    for key in keys:
        if store.put_file(root.joinpath(*key.split("/")), key) == "subido":
            result.subidos.append(key)
        else:
            result.omitidos += 1
    return result
