"""
Autor: Isacc Cabrera
Descripción: Plantilla de toda transformación del lake (bronze→silver, silver→gold). run() fija el
    orden extraer → validar → cargar; las subclases solo implementan extract() y validate(). La
    carga es idempotente y atómica: escribe cada tabla en "<partición>.tmp/", intercambia esa
    carpeta por la final y deja _manifest.json con llaves ordenadas y SIN marcas de tiempo, así
    que reprocesar la misma partición da el mismo manifiesto. check_idempotent() lo comprueba.
Patrón: Template Method (TransformJob.run)
Fecha de creación: 2026-11-09
Cambios:
    - 2026-11-09 · Isacc Cabrera · Creación (paso 7).
"""

import hashlib
import json
import os
import shutil
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

MANIFEST = "_manifest.json"
PART_FILE = "part-0.parquet"


@dataclass
class JobOutput:
    """Lo que produce ``validate``: tablas por partición y el manifiesto.

    Attributes:
        tables: ``{carpeta de partición: tabla}``; cada una se escribe como ``part-0.parquet``.
        manifest: Manifiesto (sin marcas de tiempo); se guarda en ``manifest_dir``.
        manifest_dir: Carpeta (una de ``tables``) donde va ``_manifest.json``.
        sha_of: Carpeta cuya tabla se resume con sha256 en ``manifest[sha_key]``.
        sha_key: Nombre de la llave del sha256 en el manifiesto.
    """

    tables: dict[Path, pa.Table]
    manifest: dict[str, Any]
    manifest_dir: Path
    sha_of: Path | None = None
    sha_key: str = "sha256"
    extra_files: dict[Path, pa.Table] = field(default_factory=dict)


def write_parquet(table: pa.Table, path: Path) -> None:
    """Escribe Parquet con opciones fijas (mismo contenido → mismos bytes).

    Args:
        table: Tabla a escribir.
        path: Archivo destino.
    """
    pq.write_table(table, path, compression="zstd", write_statistics=True)


def sha256_file(path: Path) -> str:
    """sha256 hexadecimal de un archivo."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def replace_dir(tmp: Path, final: Path) -> None:
    """Intercambia ``tmp`` por ``final`` (en Windows ``os.replace`` no pisa carpetas llenas).

    Args:
        tmp: Carpeta nueva ya escrita.
        final: Carpeta destino; si existe se mueve a ``.old`` y se elimina al final.
    """
    old = final.with_name(final.name + ".old")
    if old.exists():
        shutil.rmtree(old)
    if final.exists():
        os.replace(final, old)
    os.replace(tmp, final)
    if old.exists():
        shutil.rmtree(old)


class TransformJob(ABC):
    """Plantilla extraer → validar → cargar para una partición."""

    @abstractmethod
    def extract(self, partition: str) -> Any:
        """Lee la entrada de la partición (por ejemplo, los archivos bronze de una hora)."""

    @abstractmethod
    def validate(self, partition: str, data: Any) -> JobOutput:
        """Limpia y valida ``data``; regresa tablas y manifiesto."""

    def load(self, output: JobOutput) -> dict[str, Any]:
        """Escribe cada tabla de forma atómica y el manifiesto; regresa el manifiesto.

        Args:
            output: Resultado de ``validate``.

        Returns:
            El manifiesto final (con el sha256 si se pidió).
        """
        manifest = dict(output.manifest)
        for final, table in output.tables.items():
            tmp = final.with_name(final.name + ".tmp")
            if tmp.exists():
                shutil.rmtree(tmp)
            tmp.mkdir(parents=True)
            write_parquet(table, tmp / PART_FILE)
            if final == output.sha_of:
                manifest[output.sha_key] = sha256_file(tmp / PART_FILE)
            if final == output.manifest_dir:
                (tmp / MANIFEST).write_text(
                    json.dumps(manifest, sort_keys=True, indent=2) + "\n", encoding="utf-8"
                )
            replace_dir(tmp, final)
        for path, table in output.extra_files.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp_file = path.with_name(path.name + ".tmp")
            write_parquet(table, tmp_file)
            os.replace(tmp_file, path)
        return manifest

    def run(self, partition: str) -> dict[str, Any]:
        """Template Method: extract → validate → load. No se sobreescribe.

        Args:
            partition: Identificador de la partición (por ejemplo ``2026-11-09T20``).

        Returns:
            El manifiesto escrito.
        """
        data = self.extract(partition)
        output = self.validate(partition, data)
        return self.load(output)

    def check_idempotent(self, partition: str) -> tuple[bool, dict[str, Any], dict[str, Any]]:
        """Corre ``run`` dos veces y compara los manifiestos.

        Args:
            partition: Partición a procesar.

        Returns:
            ``(iguales, manifiesto 1, manifiesto 2)``.
        """
        first = self.run(partition)
        second = self.run(partition)
        return first == second, first, second
