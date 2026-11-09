"""
Autor: Isacc Cabrera
Descripción: Sincroniza una capa local (data/<capa>) con su bucket en SeaweedFS usando la
    identidad app. Idempotente: lo que ya está con el mismo tamaño se omite. Nunca sube viajes
    crudos (bronze/trips): la exclusión está en radar.lake.store.layer_files, no en un flag.
    Con --check-idempotent hace dos pasadas y sale 3 si la segunda vuelve a subir una llave que
    la primera ya había visto, o si alguna llave subida contiene "trips/".
Patrón: N/A (CLI delgado sobre radar.lake.store)
Fecha de creación: 2026-11-09
Cambios:
    - 2026-11-09 · Isacc Cabrera · Creación (paso 6).
"""

import argparse
import json
import sys
from typing import cast

from radar.config import get_settings
from radar.lake.store import LAYERS, Layer, layer_files, make_lake_store, sync_files
from radar.logging import configure_logging, get_logger

log = get_logger(__name__)


def main(argv: list[str] | None = None) -> int:
    """CLI ``sync_lake.py --layer bronze|silver|gold [--check-idempotent] [--json]``."""
    parser = argparse.ArgumentParser(description="Sube una capa del lake a SeaweedFS.")
    parser.add_argument("--layer", required=True, choices=LAYERS)
    parser.add_argument("--check-idempotent", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    settings = get_settings()
    configure_logging(settings.log_level)
    layer = cast(Layer, args.layer)
    store = make_lake_store(settings, layer, "s3")
    root = settings.data_dir / layer

    keys1 = layer_files(settings, layer)
    first = sync_files(store, root, keys1)
    passes = [first.resumen()]
    # en bronze "trips/" son viajes crudos (PII); silver/trips ya viene anonimizado (paso 8)
    pii = layer == "bronze"
    ok = not (pii and passes[0]["claves_trips"])
    resubidas: list[str] = []
    if args.check_idempotent:
        second = sync_files(store, root, layer_files(settings, layer))
        passes.append(second.resumen())
        seen = set(keys1)
        resubidas = [k for k in second.subidos if k in seen]
        ok = ok and not resubidas and not (pii and passes[1]["claves_trips"])
    out = {
        "capa": layer,
        "bucket": getattr(store, "bucket", None),
        "pasadas": passes,
        "resubidas": resubidas,
        "ok": ok,
    }
    log.info("lake_sincronizado", **{k: v for k, v in out.items() if k != "pasadas"})
    if args.json:
        print(json.dumps(out))
    return 0 if ok else 3


if __name__ == "__main__":
    sys.exit(main())
