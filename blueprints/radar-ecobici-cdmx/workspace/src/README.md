# src/

Código Python del paquete `radar` (layout `src/`, instalado en modo editable por `uv sync`).

| Subpaquete | Qué hace | Paso |
|---|---|---|
| `radar/collector/` | Recolector GBFS: cliente, reintentos (Strategy), hash, sinks (Ports & Adapters), heartbeat | 2-3 |
| `radar/streaming/` | Kafka: mensajes, productor `KafkaSink`, consumidor `bronze-writer` | 4-5 |
| `radar/lake/` | Data lake: `LakeStore`, bronze → silver (cadena de calidad), silver → gold, viajes, clima | 6-9 |
| `radar/ml/` | Features, estrategias `Predictor`, entrenamiento con MLflow | 10-11 |
| `radar/api/` | FastAPI: esquemas, repositorio, inyección de dependencias | 12 |
| `radar/anomalies/` | Detectores de anomalías | 15 |
| `radar/rag/` | Asistente RAG local con Ollama | 17 |

Regla de importación: las capas solo importan "hacia abajo" (ver `CLAUDE.md`).
