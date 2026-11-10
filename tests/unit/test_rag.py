"""
Autor: Isacc Cabrera
Descripción: Pruebas del paso 17 sin red ni Ollama: EmbedderFalso determinista (bolsa de
    palabras con hash → vector), chromadb en memoria (EphemeralClient) y LlmFalso que cuenta
    llamadas. Cubre el troceo por encabezados con traslape, el índice con ids ruta#n de docs/
    (re-indexar no duplica), "No sé" sin llamar al LLM cuando la mejor distancia supera 0.5, la
    respuesta con fuentes cuando hay evidencia, la segunda barrera cuando el LLM dice "No sé" y
    los adaptadores de Ollama con respx.
Patrón: N/A (pruebas)
Fecha de creación: 2026-11-12
Cambios:
    - 2026-11-12 · Isacc Cabrera · Creación (paso 17).
"""

import hashlib
import json
import math
import re
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import chromadb
import httpx
import pytest
import respx
from chromadb.config import Settings as ChromaSettings

from radar.rag.ask import MAX_DISTANCE, NO_SE, OllamaLlm, answer
from radar.rag.index import (
    CHUNK_CHARS,
    OVERLAP_CHARS,
    OllamaEmbedder,
    build_index,
    chunk_file,
    doc_files,
    sections,
    split_text,
)

DIM = 256
OLLAMA = "http://ollama.test"


class EmbedderFalso:
    """Vector = conteo de palabras en cubetas por hash, normalizado (determinista)."""

    def __init__(self) -> None:
        self.calls = 0

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        self.calls += 1
        vectors = []
        for text in texts:
            v = [0.0] * DIM
            for word in re.findall(r"\w+", text.lower()):
                v[int(hashlib.md5(word.encode()).hexdigest(), 16) % DIM] += 1.0
            norm = math.sqrt(sum(x * x for x in v)) or 1.0
            vectors.append([x / norm for x in v])
        return vectors


class LlmFalso:
    """Devuelve una respuesta fija y guarda los prompts que recibió."""

    def __init__(self, reply: str = "Cada 60 segundos [fuente: docs/recolector.md#0].") -> None:
        self.reply = reply
        self.prompts: list[str] = []

    def chat(self, system: str, user: str) -> str:
        self.prompts.append(user)
        return self.reply


@pytest.fixture
def client() -> Any:
    return chromadb.EphemeralClient(settings=ChromaSettings(anonymized_telemetry=False))


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    docs = tmp_path / "docs"
    (docs / "reportes").mkdir(parents=True)
    (docs / "recolector.md").write_text(
        "# Recolector\n\n## Frecuencia\n\nEl recolector consulta station_status cada 60 segundos "
        "porque el modelo predice la próxima hora.\n\n## Fallas\n\nAnte errores 5xx reintenta con "
        "backoff exponencial y full jitter.\n",
        encoding="utf-8",
    )
    (docs / "reportes" / "modelo.md").write_text(
        "# Modelo\n\nLightGBM supera al baseline de persistencia en el MAE.\n", encoding="utf-8"
    )
    (tmp_path / "README.md").write_text("# No se indexa\n\nfuera de docs\n", encoding="utf-8")
    return tmp_path


# --- troceo ------------------------------------------------------------------------------------
def test_secciones_por_encabezado() -> None:
    md = "intro\n# Uno\ntexto uno\n## Dos\ntexto dos\n## Vacía\n"
    assert sections(md) == [("", "intro"), ("Uno", "texto uno"), ("Dos", "texto dos")]


def test_troceo_respeta_tamano_y_traslape() -> None:
    words = " ".join(f"palabra{i}" for i in range(400))
    pieces = split_text(words)
    assert len(pieces) > 1
    assert all(len(p) <= CHUNK_CHARS for p in pieces)
    first_tail = pieces[0][-OVERLAP_CHARS // 2 :]
    assert first_tail.split()[-1] in pieces[1]  # el traslape repite el final del anterior
    assert split_text("corto") == ["corto"] and split_text("  ") == []


def test_fragmentos_llevan_titulo_e_id_ruta_n(repo: Path) -> None:
    chunks = chunk_file(repo / "docs" / "recolector.md", repo)
    assert [c.id for c in chunks] == ["docs/recolector.md#0", "docs/recolector.md#1"]
    assert chunks[0].text.startswith("recolector › Frecuencia\n")
    assert [p.relative_to(repo).as_posix() for p in doc_files(repo)] == [
        "docs/recolector.md",
        "docs/reportes/modelo.md",
    ]


# --- índice --------------------------------------------------------------------------------------
def test_indice_guarda_fragmentos_y_reindexar_no_duplica(repo: Path, client: Any) -> None:
    embedder = EmbedderFalso()
    assert build_index(repo, embedder, client) == {"fragmentos": 3, "archivos": 2}
    assert build_index(repo, embedder, client) == {"fragmentos": 3, "archivos": 2}
    collection = client.get_collection("radar-docs")
    assert collection.count() == 3
    assert all(i.startswith("docs/") for i in collection.get()["ids"])
    assert collection.metadata["hnsw:space"] == "cosine"


# --- respuestas ----------------------------------------------------------------------------------
def test_con_evidencia_responde_con_fuentes_de_docs(repo: Path, client: Any) -> None:
    build_index(repo, EmbedderFalso(), client)
    llm = LlmFalso()
    out = answer(
        "¿Cada cuánto consulta el recolector station_status?", EmbedderFalso(), llm, client
    )
    assert out["fuentes"] and all(f.startswith("docs/") for f in out["fuentes"])
    assert out["fuentes"][0] == "docs/recolector.md#0"
    assert out["distancia_min"] <= MAX_DISTANCE
    assert out["respuesta"] == llm.reply
    assert len(llm.prompts) == 1 and "[docs/recolector.md#0]" in llm.prompts[0]


def test_sin_evidencia_dice_no_se_sin_llamar_al_llm(repo: Path, client: Any) -> None:
    build_index(repo, EmbedderFalso(), client)
    llm = LlmFalso()
    out = answer("receta mole poblano chocolate", EmbedderFalso(), llm, client)
    assert out["distancia_min"] > MAX_DISTANCE
    assert out == {"respuesta": NO_SE, "fuentes": [], "distancia_min": out["distancia_min"]}
    assert llm.prompts == []  # cero llamadas al LLM


def test_si_el_llm_dice_no_se_no_devuelve_fuentes(repo: Path, client: Any) -> None:
    build_index(repo, EmbedderFalso(), client)
    llm = LlmFalso(reply=NO_SE)
    out = answer(
        "¿Cada cuánto consulta el recolector station_status?", EmbedderFalso(), llm, client
    )
    assert out["respuesta"] == NO_SE and out["fuentes"] == []
    assert len(llm.prompts) == 1


# --- adaptadores de Ollama (sin red: respx) ------------------------------------------------------
def test_ollama_embedder_usa_prefijo_y_lotes() -> None:
    with respx.mock(assert_all_mocked=True) as mock, httpx.Client() as http:
        route = mock.post(f"{OLLAMA}/api/embed").mock(
            side_effect=lambda req: httpx.Response(
                200, json={"embeddings": [[0.1, 0.2]] * len(json.loads(req.content)["input"])}
            )
        )
        embedder = OllamaEmbedder(OLLAMA, "nomic-embed-text", "search_query: ", http)
        vectors = embedder.embed([f"t{i}" for i in range(40)])
    assert len(vectors) == 40
    assert route.call_count == 2  # lotes de 32
    body = json.loads(route.calls[0].request.content)
    assert body["model"] == "nomic-embed-text"
    assert body["input"][0] == "search_query: t0"


def test_ollama_llm_pide_respuesta_completa_y_determinista() -> None:
    with respx.mock(assert_all_mocked=True) as mock, httpx.Client() as http:
        route = mock.post(f"{OLLAMA}/api/chat").mock(
            return_value=httpx.Response(200, json={"message": {"content": "  Hola  "}})
        )
        assert OllamaLlm(OLLAMA, "llama3.2:3b", http).chat("sistema", "usuario") == "Hola"
    body = json.loads(route.calls.last.request.content)
    assert body["stream"] is False and body["options"]["temperature"] == 0
    assert body["model"] == "llama3.2:3b"
    assert [m["role"] for m in body["messages"]] == ["system", "user"]
