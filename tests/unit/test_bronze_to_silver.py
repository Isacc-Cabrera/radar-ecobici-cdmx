"""
Autor: Isacc Cabrera
Descripción: Pruebas del job bronze → silver sobre un bronze armado en tmp_path con las
    respuestas grabadas: particiones y esquema Parquet, cuarentena, manifiesto idéntico entre
    corridas, station_information vigente por hora, hora "auto", backfill y normalización.
Patrón: N/A (pruebas)
Fecha de creación: 2026-11-09
Cambios:
    - 2026-11-09 · Isacc Cabrera · Creación (paso 7).
"""

import copy
import gzip
import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from radar.config import Settings
from radar.lake import bronze_to_silver as b2s
from radar.lake.bronze_to_silver import (
    SILVER_STATION_INFO_SCHEMA,
    SILVER_STATUS_SCHEMA,
    BronzeToSilverJob,
    auto_hour,
    normalize_short_name,
)

Loader = Callable[[str], Any]
HOUR = "2026-11-08T18"  # last_updated 1794160800 = 2026-11-08T18:00:00Z


def _put(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(gzip.compress(json.dumps(payload).encode("utf-8"), mtime=0))


def _bronze(settings: Settings, load_fixture: Loader, extra: int = 0) -> dict[str, Any]:
    status = load_fixture("station_status.json")
    root = settings.data_dir / "bronze"
    for i in range(extra + 1):
        snap = copy.deepcopy(status)
        snap["last_updated"] += 60 * i
        lu = snap["last_updated"]
        _put(root / f"station_status/dt=2026-11-08/hour=18/station_status_{lu}.json.gz", snap)
    info = load_fixture("station_information.json")
    _put(root / "station_information/dt=2026-11-08/station_information_aaaa0000.json.gz", info)
    return status


def test_hora_del_fixture_escribe_silver_cuarentena_y_manifiesto(
    settings: Settings, load_fixture: Loader
) -> None:
    _bronze(settings, load_fixture)
    manifest = BronzeToSilverJob(settings).run(HOUR)
    silver = settings.data_dir / "silver/station_status/dt=2026-11-08/hour=18"
    stored = pq.read_schema(silver / "part-0.parquet")
    assert stored.names == SILVER_STATUS_SCHEMA.names  # Parquet guarda los timestamp en ms
    table = pq.read_table(silver / "part-0.parquet", schema=SILVER_STATUS_SCHEMA)
    assert table.column("last_reported").to_pylist()[0] == datetime(
        2026, 11, 8, 17, 59, 50, tzinfo=UTC
    )
    assert table.column("station_id").to_pylist() == ["1", "2"]
    assert table.column("capacity").to_pylist() == [19, 15]
    quarantine = pq.read_table(
        settings.data_dir / "silver/quarantine/dt=2026-11-08/hour=18/part-0.parquet"
    ).to_pydict()
    assert dict(zip(quarantine["station_id"], quarantine["motivo"], strict=True)) == {
        "3": "regla_capacidad",
        "999": "estacion_desconocida",
    }
    assert json.loads((silver / "_manifest.json").read_text(encoding="utf-8")) == manifest
    assert {k: v for k, v in manifest.items() if k != "sha256_validas"} == {
        "hora": HOUR,
        "archivos_bronze": 1,
        "filas_leidas": 4,
        "filas_validas": 2,
        "filas_cuarentena": 2,
        "filas_duplicadas": 0,
        "tasa_regla_capacidad": round(2 / 3, 6),
    }
    assert len(manifest["sha256_validas"]) == 64


def test_reprocesar_da_el_mismo_manifiesto_y_no_deja_temporales(
    settings: Settings, load_fixture: Loader
) -> None:
    _bronze(settings, load_fixture, extra=2)
    same, first, second = BronzeToSilverJob(settings).check_idempotent(HOUR)
    assert same and first == second
    assert first["filas_duplicadas"] == 4  # 2 válidas x 2 snapshots repetidos
    leftovers = [
        p for p in (settings.data_dir / "silver").rglob("*") if p.name.endswith((".tmp", ".old"))
    ]
    assert leftovers == []


def test_station_information_silver_normaliza_y_lleva_hash(
    settings: Settings, load_fixture: Loader
) -> None:
    _bronze(settings, load_fixture)
    BronzeToSilverJob(settings).run(HOUR)
    path = settings.data_dir / "silver/station_information/station_information.parquet"
    table = pq.read_table(path)
    assert table.schema.equals(SILVER_STATION_INFO_SCHEMA)
    assert table.column("short_name_norm").to_pylist() == ["710", "85", "311"]
    assert len(set(table.column("info_hash").to_pylist())) == 1


def test_usa_la_station_information_vigente_en_esa_hora(
    settings: Settings, load_fixture: Loader
) -> None:
    _bronze(settings, load_fixture)
    newer = load_fixture("station_information.json")
    newer["last_updated"] = 1794200000  # después de la hora 18
    for s in newer["data"]["stations"]:
        s["capacity"] = 99
    _put(
        settings.data_dir
        / "bronze/station_information/dt=2026-11-09/station_information_bbbb.json.gz",
        newer,
    )
    manifest = BronzeToSilverJob(settings).run(HOUR)
    assert manifest["filas_validas"] == 2  # con capacidad 99 nada cumpliría la regla


def test_auto_hour_es_la_ultima_hora_cerrada_hace_10_minutos() -> None:
    assert auto_hour(datetime(2026, 11, 9, 21, 15, tzinfo=UTC)) == datetime(
        2026, 11, 9, 20, tzinfo=UTC
    )
    assert auto_hour(datetime(2026, 11, 9, 21, 9, tzinfo=UTC)) == datetime(
        2026, 11, 9, 19, tzinfo=UTC
    )


def test_backfill_procesa_solo_horas_cerradas_sin_manifiesto(
    settings: Settings, load_fixture: Loader, monkeypatch: pytest.MonkeyPatch, capsys: Any
) -> None:
    _bronze(settings, load_fixture)
    monkeypatch.setattr(b2s, "get_settings", lambda: settings)
    assert b2s.main(["--backfill", "--json"]) == 0
    first = json.loads(capsys.readouterr().out)
    assert first["horas"] == [HOUR]
    assert b2s.main(["--backfill", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["horas"] == []


def test_check_idempotent_sale_3_si_no_hay_filas_validas(
    settings: Settings, load_fixture: Loader, monkeypatch: pytest.MonkeyPatch, capsys: Any
) -> None:
    _bronze(settings, load_fixture)
    monkeypatch.setattr(b2s, "get_settings", lambda: settings)
    assert b2s.main(["--hour", HOUR, "--check-idempotent", "--json"]) == 0
    capsys.readouterr()
    assert b2s.main(["--hour", "2026-11-08T05", "--check-idempotent", "--json"]) == 3


def test_normalize_short_name() -> None:
    assert normalize_short_name("085") == "85"
    assert normalize_short_name("266-267") == "266-267"
    assert normalize_short_name(None) is None
