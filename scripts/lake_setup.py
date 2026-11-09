"""
Autor: Isacc Cabrera
Descripción: Prepara SeaweedFS. Subcomandos: `render` (rellena llaves S3 vacías en .env y escribe
    docker/seaweedfs/s3.json, ignorado en git), `buckets` (crea radar-bronze/silver/gold con la
    identidad admin, idempotente) y `check-policy` (prueba que la identidad app pueda put/get/list
    y reciba AccessDenied en CreateBucket y DeleteBucket). La lógica vive en radar.lake.s3_setup.
    Salida: JSON a stdout; 0 ok, 2 uso, 3 check fallido.
Patrón: N/A (CLI delgado sobre radar.lake.s3_setup)
Fecha de creación: 2026-11-09
Cambios:
    - 2026-11-09 · Isacc Cabrera · Creación (paso 6).
"""

import argparse
import json
import shutil
import sys
from pathlib import Path

from radar.config import Settings
from radar.lake.s3_setup import (
    check_policy,
    ensure_buckets,
    fill_blank_keys,
    lake_buckets,
    s3_config,
)
from radar.lake.store import make_s3_client
from radar.logging import configure_logging, get_logger

log = get_logger(__name__)

ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = ROOT / ".env"
S3_JSON = ROOT / "docker" / "seaweedfs" / "s3.json"


def render() -> int:
    """Rellena .env y escribe s3.json."""
    if S3_JSON.is_dir():
        print(
            "borra el directorio docker/seaweedfs/s3.json y vuelve a correr render", file=sys.stderr
        )
        return 3
    if not ENV_PATH.exists():
        shutil.copyfile(ROOT / ".env.example", ENV_PATH)
    text, filled = fill_blank_keys(ENV_PATH.read_text(encoding="utf-8"))
    if filled:
        ENV_PATH.write_text(text, encoding="utf-8", newline="\n")
    settings = Settings(_env_file=ENV_PATH)  # type: ignore[call-arg]
    S3_JSON.write_text(
        json.dumps(s3_config(settings), indent=2) + "\n", encoding="utf-8", newline="\n"
    )
    print(json.dumps({"env_rellenadas": filled, "s3_json": "docker/seaweedfs/s3.json", "ok": True}))
    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI ``lake_setup.py {render,buckets,check-policy}``."""
    parser = argparse.ArgumentParser(description="Prepara SeaweedFS para Radar ECOBICI.")
    parser.add_argument("comando", choices=["render", "buckets", "check-policy"])
    args = parser.parse_args(argv)
    settings = Settings(_env_file=ENV_PATH)  # type: ignore[call-arg]
    configure_logging(settings.log_level)
    if args.comando == "render":
        return render()
    buckets = lake_buckets(settings)
    admin = make_s3_client(settings, "admin")
    if args.comando == "buckets":
        result = ensure_buckets(admin, buckets)
        present = {b["Name"] for b in admin.list_buckets().get("Buckets", [])}
        ok = all(b in present for b in buckets)
        print(json.dumps({"buckets": result, "ok": ok}))
        return 0 if ok else 3
    report = check_policy(make_s3_client(settings, "app"), buckets, admin=admin)
    print(json.dumps(report))
    if not report["ok"]:
        log.error("minimo_privilegio_fallido", reporte=report)
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
