"""
Autor: Isacc Cabrera
Descripción: Indexador del asistente RAG. Trocea docs/*.md y docs/reportes/*.md en fragmentos
    de ~800 caracteres con 100 de traslape, respetando los encabezados Markdown (cada fragmento
    lleva el título de su sección para no perder contexto), calcula sus embeddings con el puerto
    Embedder (adaptador OllamaEmbedder: POST /api/embed de Ollama, local y gratis) y los guarda
    en chromadb (carpeta chroma/, ignorada por git), colección "radar-docs" con distancia coseno.
    embedding_function=None: chromadb nunca descarga su modelo ONNX por defecto; los vectores
    siempre vienen del Embedder. Cada indexado re-crea la colección (idempotente).
    nomic-embed-text espera los prefijos "search_document: " (al indexar) y "search_query: "
    (al preguntar): los pone el adaptador, el puerto no cambia.
    CLI: python -m radar.rag.index [--json] → {"fragmentos": n, "archivos": m}.
Patrón: Ports & Adapters (Embedder es el puerto; OllamaEmbedder, el adaptador)
Fecha de creación: 2026-11-10
Cambios:
    - 2026-11-10 · Isacc Cabrera · Creación (paso 17).
"""

import argparse
import contextlib
import json
import re
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import chromadb
import httpx
from chromadb.config import Settings as ChromaSettings

from radar.config import Settings, get_settings
from radar.logging import configure_logging, get_logger

log = get_logger(__name__)

COLLECTION = "radar-docs"
CHROMA_PATH = Path("chroma")
CHUNK_CHARS = 800
OVERLAP_CHARS = 100
BATCH = 32
DOC_GLOBS = ("docs/*.md", "docs/reportes/*.md")
_HEADING = re.compile(r"^(#{1,6})\s+(.*)$")


class Embedder(Protocol):
    """Puerto: convierte textos en vectores."""

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        """Un vector por texto, en el mismo orden."""
        ...


class OllamaEmbedder:
    """Adaptador de ``Embedder`` sobre ``POST {OLLAMA_URL}/api/embed``.

    Args:
        url: ``OLLAMA_URL``.
        model: ``OLLAMA_EMBED_MODEL`` (``nomic-embed-text``).
        prefix: ``"search_document: "`` al indexar, ``"search_query: "`` al preguntar.
        client: Cliente httpx (inyectable).
    """

    def __init__(
        self, url: str, model: str, prefix: str = "", client: httpx.Client | None = None
    ) -> None:
        self.url = url.rstrip("/")
        self.model = model
        self.prefix = prefix
        self.client = client or httpx.Client(timeout=120.0)

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        """Vectores de Ollama, en lotes de ``BATCH`` textos.

        Raises:
            httpx.HTTPError: Si Ollama no responde o el modelo no está descargado.
        """
        vectors: list[list[float]] = []
        for start in range(0, len(texts), BATCH):
            batch = [self.prefix + t for t in texts[start : start + BATCH]]
            response = self.client.post(
                f"{self.url}/api/embed", json={"model": self.model, "input": batch}
            )
            response.raise_for_status()
            vectors.extend(response.json()["embeddings"])
        return vectors


@dataclass(frozen=True)
class Chunk:
    """Un fragmento indexable.

    Attributes:
        id: ``<ruta>#<n>`` (ruta relativa a la raíz del repo, con ``/``).
        text: Texto del fragmento, precedido por el título de su sección.
        source: Ruta del archivo.
    """

    id: str
    text: str
    source: str


def sections(markdown: str) -> list[tuple[str, str]]:
    """Divide un Markdown por encabezados.

    Returns:
        ``[(título de la sección, cuerpo)]``; el texto antes del primer encabezado va con
        título vacío.
    """
    out: list[tuple[str, list[str]]] = [("", [])]
    for line in markdown.splitlines():
        match = _HEADING.match(line)
        if match:
            out.append((match.group(2).strip(), []))
        else:
            out[-1][1].append(line)
    return [(title, "\n".join(body).strip()) for title, body in out if "\n".join(body).strip()]


def split_text(text: str, size: int = CHUNK_CHARS, overlap: int = OVERLAP_CHARS) -> list[str]:
    """Corta ``text`` en piezas de hasta ``size`` caracteres con ``overlap`` de traslape.

    Prefiere cortar en un salto de párrafo o de línea, o en un espacio, para no partir palabras.
    """
    text = text.strip()
    if len(text) <= size:
        return [text] if text else []
    pieces = []
    start = 0
    while start < len(text):
        end = min(start + size, len(text))
        if end < len(text):
            window = text[start:end]
            cut = max(window.rfind("\n\n"), window.rfind("\n"), window.rfind(" "))
            if cut > size // 2:
                end = start + cut
        pieces.append(text[start:end].strip())
        if end >= len(text):
            break
        start = max(end - overlap, start + 1)
    return [p for p in pieces if p]


def chunk_file(path: Path, root: Path) -> list[Chunk]:
    """Fragmentos de un archivo Markdown con ids ``<ruta>#<n>`` (n desde 0)."""
    rel = path.relative_to(root).as_posix()
    chunks: list[Chunk] = []
    for title, body in sections(path.read_text(encoding="utf-8")):
        header = f"{path.stem} › {title}\n" if title else f"{path.stem}\n"
        for piece in split_text(body):
            chunks.append(Chunk(id=f"{rel}#{len(chunks)}", text=header + piece, source=rel))
    return chunks


def doc_files(root: Path) -> list[Path]:
    """Archivos Markdown de ``docs/`` y ``docs/reportes/``, en orden."""
    return sorted({p for pattern in DOC_GLOBS for p in root.glob(pattern) if p.is_file()})


def chroma_client(path: Path = CHROMA_PATH) -> Any:
    """Cliente persistente de chromadb en ``path``, sin telemetría."""
    return chromadb.PersistentClient(
        path=str(path), settings=ChromaSettings(anonymized_telemetry=False)
    )


def build_index(root: Path, embedder: Embedder, client: Any) -> dict[str, int]:
    """Re-crea la colección ``radar-docs`` con todos los fragmentos de la documentación.

    Args:
        root: Raíz del repo (contiene ``docs/``).
        embedder: Puerto de embeddings.
        client: Cliente de chromadb (persistente o en memoria).

    Returns:
        ``{"fragmentos": n, "archivos": m}``.
    """
    files = doc_files(root)
    chunks = [c for f in files for c in chunk_file(f, root)]
    with contextlib.suppress(Exception):  # la primera vez no existe todavía
        client.delete_collection(COLLECTION)
    collection = client.create_collection(
        COLLECTION, metadata={"hnsw:space": "cosine"}, embedding_function=None
    )
    if chunks:
        collection.add(
            ids=[c.id for c in chunks],
            documents=[c.text for c in chunks],
            embeddings=embedder.embed([c.text for c in chunks]),
            metadatas=[{"fuente": c.source} for c in chunks],
        )
    summary = {"fragmentos": len(chunks), "archivos": len(files)}
    log.info("rag_indexado", **summary)
    return summary


def document_embedder(settings: Settings) -> OllamaEmbedder:
    """Adaptador para indexar (prefijo ``search_document: ``)."""
    return OllamaEmbedder(settings.ollama_url, settings.ollama_embed_model, "search_document: ")


def main(argv: list[str] | None = None) -> int:
    """CLI ``python -m radar.rag.index [--json]``.

    Returns:
        0 ok; 2 uso; 3 si Ollama no responde.
    """
    parser = argparse.ArgumentParser(description="Indexa docs/ en chromadb para el asistente.")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    settings = get_settings()
    configure_logging(settings.log_level)
    try:
        summary = build_index(Path.cwd(), document_embedder(settings), chroma_client())
    except httpx.HTTPError as exc:
        log.error("ollama_no_disponible", url=settings.ollama_url, error=str(exc))
        return 3
    if args.json:
        print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())
