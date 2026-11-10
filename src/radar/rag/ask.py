"""
Autor: Isacc Cabrera
Descripción: Asistente RAG que responde preguntas sobre la documentación del proyecto citando
    fuentes. answer(): busca los k fragmentos más cercanos en chromadb; si el mejor está a una
    distancia coseno mayor que 0.5 no hay evidencia y responde "No sé…" SIN llamar al LLM; si
    hay evidencia, arma un prompt en español que exige responder solo con ese contexto y citar
    [fuente: ruta#n], y llama al puerto Llm (adaptador OllamaLlm: POST /api/chat, stream false,
    temperatura 0, modelo local OLLAMA_CHAT_MODEL). Devuelve la respuesta, las fuentes (ids
    ruta#n de docs/) y la distancia mínima. Segunda barrera: si el LLM contesta "No sé", la
    respuesta se normaliza y no se devuelven fuentes (el umbral de 0.5 no detiene todas las
    preguntas fuera de tema: con nomic-embed-text una ajena queda alrededor de 0.37).
    CLI: python -m radar.rag.ask "<pregunta>" [--json] [-k 4].
Patrón: Ports & Adapters (Llm y Embedder son puertos; Ollama, los adaptadores)
Fecha de creación: 2026-11-10
Cambios:
    - 2026-11-10 · Isacc Cabrera · Creación (paso 17).
"""

import argparse
import json
import sys
from typing import Any, Protocol

import httpx

from radar.config import Settings, get_settings
from radar.logging import configure_logging, get_logger
from radar.rag.index import COLLECTION, Embedder, OllamaEmbedder, chroma_client

log = get_logger(__name__)

MAX_DISTANCE = 0.5
NO_SE = "No sé: no encontré evidencia en la documentación del proyecto."
SYSTEM_PROMPT = (
    "Eres el asistente del proyecto Radar ECOBICI CDMX. Responde en español, en 2 a 5 "
    "oraciones, usando ÚNICAMENTE el contexto que te doy. Después de cada afirmación cita su "
    "fuente con el formato [fuente: ruta#n] usando los ids del contexto. Si el contexto no "
    "contiene la respuesta, responde exactamente: " + NO_SE
)


class Llm(Protocol):
    """Puerto: genera una respuesta a partir de un prompt de sistema y uno de usuario."""

    def chat(self, system: str, user: str) -> str:
        """Texto de la respuesta."""
        ...


class OllamaLlm:
    """Adaptador de ``Llm`` sobre ``POST {OLLAMA_URL}/api/chat`` (local, sin costo).

    Args:
        url: ``OLLAMA_URL``.
        model: ``OLLAMA_CHAT_MODEL`` (``llama3.2:3b``).
        client: Cliente httpx (inyectable).
    """

    def __init__(self, url: str, model: str, client: httpx.Client | None = None) -> None:
        self.url = url.rstrip("/")
        self.model = model
        self.client = client or httpx.Client(timeout=300.0)

    def chat(self, system: str, user: str) -> str:
        """Respuesta completa (``stream: false``, temperatura 0 para que sea reproducible)."""
        response = self.client.post(
            f"{self.url}/api/chat",
            json={
                "model": self.model,
                "stream": False,
                "options": {"temperature": 0},
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
            },
        )
        response.raise_for_status()
        content: str = response.json()["message"]["content"]
        return content.strip()


def build_prompt(question: str, ids: list[str], documents: list[str]) -> str:
    """Prompt de usuario: el contexto numerado por id y la pregunta."""
    context = "\n\n".join(f"[{i}]\n{doc}" for i, doc in zip(ids, documents, strict=True))
    return f"Contexto:\n{context}\n\nPregunta: {question}"


def answer(question: str, embedder: Embedder, llm: Llm, client: Any, k: int = 4) -> dict[str, Any]:
    """Responde con evidencia de la documentación o dice "No sé".

    Args:
        question: Pregunta del usuario.
        embedder: Puerto de embeddings (con prefijo de consulta).
        llm: Puerto del modelo de lenguaje.
        client: Cliente de chromadb con la colección ``radar-docs`` ya indexada.
        k: Fragmentos a recuperar.

    Returns:
        ``{"respuesta", "fuentes": [ruta#n], "distancia_min"}``. Sin evidencia, ``fuentes`` es
        ``[]`` y el LLM no se llama.
    """
    collection = client.get_collection(COLLECTION, embedding_function=None)
    result = collection.query(
        query_embeddings=embedder.embed([question]),
        n_results=k,
        include=["documents", "distances"],
    )
    ids: list[str] = result["ids"][0]
    distances: list[float] = result["distances"][0]
    documents: list[str] = result["documents"][0]
    best = round(min(distances), 4) if distances else None
    if best is None or best > MAX_DISTANCE:
        log.info("rag_sin_evidencia", distancia_min=best)
        return {"respuesta": NO_SE, "fuentes": [], "distancia_min": best}
    keep = [i for i, d in enumerate(distances) if d <= MAX_DISTANCE]
    kept_ids = [ids[i] for i in keep]
    text = llm.chat(SYSTEM_PROMPT, build_prompt(question, kept_ids, [documents[i] for i in keep]))
    if text.startswith("No sé"):  # segunda barrera: el LLM no encontró la respuesta en el contexto
        return {"respuesta": NO_SE, "fuentes": [], "distancia_min": best}
    return {"respuesta": text, "fuentes": kept_ids, "distancia_min": best}


def query_embedder(settings: Settings) -> OllamaEmbedder:
    """Adaptador para preguntar (prefijo ``search_query: ``)."""
    return OllamaEmbedder(settings.ollama_url, settings.ollama_embed_model, "search_query: ")


def main(argv: list[str] | None = None) -> int:
    """CLI ``python -m radar.rag.ask "<pregunta>" [--json] [-k 4]``.

    Returns:
        0 ok; 2 uso; 3 si Ollama no responde o el índice no existe.
    """
    parser = argparse.ArgumentParser(description="Pregunta a la documentación del proyecto.")
    parser.add_argument("pregunta")
    parser.add_argument("-k", type=int, default=4)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    settings = get_settings()
    configure_logging(settings.log_level)
    for stream in (sys.stdout, sys.stderr):  # Windows redirige con cp1252: forzar UTF-8
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    llm = OllamaLlm(settings.ollama_url, settings.ollama_chat_model)
    try:
        out = answer(args.pregunta, query_embedder(settings), llm, chroma_client(), args.k)
    except httpx.HTTPError as exc:
        log.error("ollama_no_disponible", url=settings.ollama_url, error=str(exc))
        return 3
    except Exception as exc:  # colección inexistente: falta indexar
        log.error("indice_no_disponible", error=str(exc), sugerencia="python -m radar.rag.index")
        return 3
    if args.json:
        print(json.dumps(out, ensure_ascii=False))
    else:
        print(out["respuesta"])
        for source in out["fuentes"]:
            print(f"  · {source}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
