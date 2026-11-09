#!/usr/bin/env bash
# Autor: Isacc Cabrera
# Descripción: Demo de recuperación de Kafka. Apaga el consumidor bronze-writer a propósito
#   mientras el productor sigue publicando, lo vuelve a encender y comprueba que cada snapshot
#   publicado en la ventana (heartbeat sink=kafka, result=guardado) terminó en bronze: los
#   mensajes esperaron en el topic y el consumidor retomó desde su último offset confirmado.
#   Uso: DEMO_PAUSE_SECONDS=120 bash scripts/demo_recovery.sh  (default 300 s).
# Patrón: N/A
# Fecha de creación: 2026-11-09
# Cambios:
#     - 2026-11-09 · Isacc Cabrera · Creación (paso 5).
#     - 2026-11-09 · Isacc Cabrera · Ventana inicio → reanudación (sin recorte de 90 s, que
#       dejaba 30 s con pausa de 120 s y a veces 0 snapshots esperados).
set -euo pipefail

PAUSE=${DEMO_PAUSE_SECONDS:-300}
WAIT_MAX=180
cd "$(dirname "$0")/.."

docker compose --profile streaming up -d --wait kafka collector-kafka bronze-writer
inicio=$(date -u +%Y-%m-%dT%H:%M:%S)
echo "inicio=${inicio}Z · deteniendo bronze-writer por ${PAUSE} s"
docker compose --profile streaming stop bronze-writer
sleep "$PAUSE"
echo "reanudando bronze-writer"
docker compose --profile streaming start bronze-writer
fin=$(date -u +%Y-%m-%dT%H:%M:%S)

check() {
  INICIO="$inicio" FIN="$fin" uv run python - <<'PY'
import glob
import json
import os
import sys
from datetime import datetime

inicio = datetime.fromisoformat(os.environ["INICIO"] + "+00:00")
# ventana = todo lo publicado mientras el consumidor estuvo apagado (inicio → reanudación)
limite = datetime.fromisoformat(os.environ["FIN"] + "+00:00")
esperados, faltantes = [], []
for path in sorted(glob.glob("data/bronze/heartbeat/dt=*.jsonl")):
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            hb = json.loads(line)
            ts = datetime.fromisoformat(hb["ts_utc"])
            if (
                hb["feed"] == "station_status"
                and hb["sink"] == "kafka"
                and hb["result"] == "guardado"
                and inicio <= ts <= limite
            ):
                esperados.append(hb["last_updated"])
for lu in esperados:
    if not glob.glob(f"data/bronze/station_status/dt=*/hour=*/station_status_{lu}.json.gz"):
        faltantes.append(lu)
resumen = {"esperados": len(esperados), "encontrados": len(esperados) - len(faltantes),
           "faltantes": len(faltantes)}
print(json.dumps(resumen))
sys.exit(0 if resumen["faltantes"] == 0 and resumen["esperados"] >= 1 else 1)
PY
}

deadline=$((SECONDS + WAIT_MAX))
until check; do
  if ((SECONDS >= deadline)); then
    echo "FALLA: faltan snapshots después de ${WAIT_MAX} s" >&2
    exit 1
  fi
  sleep 10
done
echo "OK: el consumidor recuperó todo lo publicado mientras estuvo apagado"
