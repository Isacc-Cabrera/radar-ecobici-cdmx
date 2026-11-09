"""
Autor: Isacc Cabrera
Descripción: Pruebas de la cadena de calidad de silver: motivos de cuarentena con el snapshot
    grabado (estación "3" rompe la regla de capacidad, "999" no existe), esquema antes que
    cualquier otro check, duplicados contados pero no en cuarentena, y orden de la cadena.
Patrón: N/A (pruebas)
Fecha de creación: 2026-11-09
Cambios:
    - 2026-11-09 · Isacc Cabrera · Creación (paso 7).
"""

import copy
from collections.abc import Callable
from typing import Any

import polars as pl

from radar.lake.quality import (
    CapacityRuleCheck,
    DedupCheck,
    KnownStationCheck,
    SchemaCheck,
    build_chain,
    flatten_snapshot,
    raw_frame,
    to_text,
)

Loader = Callable[[str], Any]


def _capacities(load_fixture: Loader) -> dict[str, int]:
    info = load_fixture("station_information.json")
    return {s["station_id"]: s["capacity"] for s in info["data"]["stations"]}


def _run(payloads: list[dict[str, Any]], caps: dict[str, int]) -> tuple[Any, ...]:
    rows = [r for p in payloads for r in flatten_snapshot(p)]
    first, capacity, dedup = build_chain(caps)
    valid, quarantine = first.handle(raw_frame(rows))
    return valid, quarantine, capacity, dedup


def _motivos(quarantine: pl.DataFrame) -> dict[str, str]:
    return dict(
        zip(quarantine["station_id"].to_list(), quarantine["motivo"].to_list(), strict=True)
    )


def test_fixture_manda_3_y_999_a_cuarentena_y_deja_1_y_2(load_fixture: Loader) -> None:
    valid, quarantine, capacity, _ = _run(
        [load_fixture("station_status.json")], _capacities(load_fixture)
    )
    assert sorted(valid["station_id"].to_list()) == ["1", "2"]
    assert _motivos(quarantine) == {"3": "regla_capacidad", "999": "estacion_desconocida"}
    assert (capacity.evaluadas, capacity.cumplen) == (3, 2)
    assert capacity.tasa == round(2 / 3, 6)


def test_duplicado_se_cuenta_y_no_va_a_cuarentena(load_fixture: Loader) -> None:
    snap = load_fixture("station_status.json")
    later = copy.deepcopy(snap)
    later["last_updated"] += 60  # mismo last_reported por estación: minuto sin cambios
    valid, quarantine, _, dedup = _run([snap, later], _capacities(load_fixture))
    assert sorted(valid["station_id"].to_list()) == ["1", "2"]
    assert dedup.duplicadas == 2
    assert "duplicado" not in quarantine["motivo"].to_list()
    assert valid["snapshot_last_updated"].to_list() == [snap["last_updated"]] * 2


def test_campo_faltante_o_conteo_no_entero_es_esquema_antes_que_todo(
    load_fixture: Loader,
) -> None:
    snap = load_fixture("station_status.json")
    stations = snap["data"]["stations"]
    del stations[0]["num_docks_available"]  # "1": falta un campo
    stations[2]["num_bikes_available"] = 5.5  # "3": rompería capacidad, pero esquema va primero
    stations[3]["num_bikes_disabled"] = "dos"  # "999": desconocida, pero esquema va primero
    valid, quarantine, capacity, _ = _run([snap], _capacities(load_fixture))
    assert valid["station_id"].to_list() == ["2"]
    assert _motivos(quarantine) == {"1": "esquema", "3": "esquema", "999": "esquema"}
    assert capacity.evaluadas == 1


def test_negativos_y_banderas_raras_son_esquema(load_fixture: Loader) -> None:
    snap = load_fixture("station_status.json")
    snap["data"]["stations"][0]["num_bikes_available"] = -1
    snap["data"]["stations"][1]["is_renting"] = "quizas"
    _, quarantine, _, _ = _run([snap], _capacities(load_fixture))
    assert _motivos(quarantine)["1"] == "esquema"
    assert _motivos(quarantine)["2"] == "esquema"


def test_schema_check_tipa_columnas(load_fixture: Loader) -> None:
    df = raw_frame(flatten_snapshot(load_fixture("station_status.json")))
    good, bad = SchemaCheck().check(df)
    assert bad.height == 0
    assert good.schema["num_bikes_available"] == pl.Int64
    assert good.schema["is_renting"] == pl.Boolean
    assert good.schema["is_charging"] == pl.Boolean


def test_is_charging_ausente_queda_nulo(load_fixture: Loader) -> None:
    snap = load_fixture("station_status.json")
    del snap["data"]["stations"][0]["is_charging"]
    good, bad = SchemaCheck().check(raw_frame(flatten_snapshot(snap)))
    assert bad.height == 0
    assert good.filter(pl.col("station_id") == "1")["is_charging"].to_list() == [None]


def test_cadena_en_orden_fijo() -> None:
    first, _, _ = build_chain({"1": 10})
    order = []
    node: Any = first
    while node is not None:
        order.append(type(node))
        node = node._next
    assert order == [SchemaCheck, KnownStationCheck, CapacityRuleCheck, DedupCheck]


def test_to_text_conserva_el_tipo_original() -> None:
    assert to_text(True) == "true"
    assert to_text(7) == "7"
    assert to_text(7.0) == "7.0"
    assert to_text(None) is None
