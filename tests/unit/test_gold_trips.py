"""
Autor: Isacc Cabrera
Descripción: Pruebas del paso 8: normalización y emparejamiento de códigos ("085" → "85";
    "266-267" se conserva y no empareja), viajes a silver con lista blanca (sin PII, aunque el CSV
    traiga columnas nuevas) y hora local → UTC, demanda promedio por fechas del mes, gold con
    rejilla de 5 min y agregados por hora, manifiesto idéntico entre corridas, reporte SQL,
    probe/descarga sin red (respx) y el benchmark en proceso con tablas pequeñas.
Patrón: N/A (pruebas)
Fecha de creación: 2026-11-09
Cambios:
    - 2026-11-09 · Isacc Cabrera · Creación (paso 8).
"""

import copy
import gzip
import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import polars as pl
import pyarrow.parquet as pq
import pytest
import respx

from radar.config import Settings
from radar.lake.bronze_to_silver import BronzeToSilverJob, normalize_short_name
from radar.lake.silver_to_gold import (
    FEATURES_BASE_SCHEMA,
    STATION_HOUR_SCHEMA,
    SilverToGoldJob,
    run_sql_report,
    split_queries,
)
from radar.lake.trips import (
    EXPECTED_HEADER,
    PII_COLUMNS,
    SILVER_TRIPS_SCHEMA,
    TripsToSilverJob,
    bench_engine,
    build_demand,
    download,
    latest_csv_url,
    match_stats,
    parse_trips,
    probe,
    write_demand,
)

Loader = Callable[[str], Any]
DAY = "2026-11-08"
T0 = 1794160800  # 2026-11-08T18:00:00Z (snapshot del fixture)
CSV_URL = "https://ejemplo.test/datos/public_data_web_2026-10.csv"


# --- ayudas ----------------------------------------------------------------------------------
def _put(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(gzip.compress(json.dumps(payload).encode("utf-8"), mtime=0))


def _silver_con_cambio(settings: Settings, load_fixture: Loader) -> None:
    """Bronze de la hora 18 con 2 snapshots: a las 18:20 la estación "1" pasa de 7 a 3 bicis."""
    root = settings.data_dir / "bronze"
    snap = load_fixture("station_status.json")
    later = copy.deepcopy(snap)
    later["last_updated"] = T0 + 20 * 60
    st1 = later["data"]["stations"][0]
    st1.update(num_bikes_available=3, num_docks_available=14, last_reported=T0 + 20 * 60 - 5)
    for payload in (snap, later):
        lu = payload["last_updated"]
        _put(root / f"station_status/dt={DAY}/hour=18/station_status_{lu}.json.gz", payload)
    info = load_fixture("station_information.json")
    _put(root / f"station_information/dt={DAY}/station_information_aaaa0000.json.gz", info)
    BronzeToSilverJob(settings).run(f"{DAY}T18")


def _csv(rows: list[str], extra_header: str = "") -> str:
    return "\n".join([EXPECTED_HEADER + extra_header, *rows]) + "\n"


# --- normalización y emparejamiento ----------------------------------------------------------
def test_085_se_normaliza_y_266_267_se_conserva_y_no_empareja(load_fixture: Loader) -> None:
    assert normalize_short_name("085") == "85"
    assert normalize_short_name(" 266-267 ") == "266-267"
    info = load_fixture("station_information.json")["data"]["stations"]
    known = {normalize_short_name(s["short_name"]) for s in info}
    tasa, unmatched = match_stats(pl.Series(["85", "266-267", "85", "710"]), known)
    assert unmatched == ["266-267"]
    assert tasa == 0.75


# --- viajes a silver -------------------------------------------------------------------------
def test_to_silver_solo_lista_blanca_utc_y_latin1(settings: Settings) -> None:
    text = _csv(
        [
            "M,22.0,4027652,085,01/10/2026,08:00:00,266-267,01/10/2026,08:30:15,Pérez",
            "F,30.0,1111111,710,fecha-rota,08:00:00,085,01/10/2026,09:00:00,López",
        ],
        extra_header=",Apellido_Usuario",  # columna nueva con datos personales
    )
    trips_dir = settings.data_dir / "bronze" / "trips"
    trips_dir.mkdir(parents=True)
    (trips_dir / "public_data_web_2026-10.csv").write_bytes(text.encode("latin-1"))

    manifest = TripsToSilverJob(settings).run("public_data_web_2026-10.csv")

    part = settings.data_dir / "silver/trips/month=2026-10/part-0.parquet"
    table = pq.read_table(part, schema=SILVER_TRIPS_SCHEMA)
    assert pq.read_schema(part).names == SILVER_TRIPS_SCHEMA.names
    assert not set(pq.read_schema(part).names) & {*PII_COLUMNS, "Apellido_Usuario"}
    row = table.to_pylist()[0]
    assert row["estacion_retiro"] == "085" and row["estacion_retiro_norm"] == "85"
    assert row["estacion_arribo_norm"] == "266-267"
    assert row["retiro_utc"] == datetime(2026, 10, 1, 14, 0, tzinfo=UTC)  # CDMX = UTC-6
    assert manifest["codificacion"] == "latin-1"
    assert (manifest["filas_leidas"], manifest["filas_validas"]) == (2, 1)
    assert manifest["filas_descartadas"] == 1


def test_csv_sin_una_columna_de_la_lista_blanca_falla() -> None:
    with pytest.raises(ValueError, match="lista blanca"):
        parse_trips("Ciclo_Estacion_Retiro,Fecha_Retiro\n085,01/10/2026\n")


def test_demanda_promedia_entre_las_fechas_del_mes() -> None:
    trips, _ = parse_trips(
        _csv(
            [
                "M,1,1,085,01/10/2026,08:10:00,710,01/10/2026,08:40:00",  # jueves
                "M,1,1,085,08/10/2026,08:20:00,710,08/10/2026,08:50:00",  # jueves
                "M,1,1,085,30/09/2026,23:50:00,710,01/10/2026,00:10:00",  # sale en septiembre
            ]
        )
    )
    demand, summary = build_demand(trips, {"85", "710"}, ["2026-10"])
    jueves_8 = demand.filter(
        (pl.col("short_name_norm") == "85") & (pl.col("dow") == 3) & (pl.col("hour_local") == 8)
    )
    assert jueves_8["salidas_prom"].to_list() == [0.4]  # 2 viajes / 5 jueves de oct-2026
    assert summary["salidas_fuera_de_mes"] == 1
    assert summary["tasa_match"] == 1.0


def test_write_demand_escribe_gold(settings: Settings, load_fixture: Loader) -> None:
    _silver_con_cambio(settings, load_fixture)
    trips_dir = settings.data_dir / "bronze" / "trips"
    trips_dir.mkdir(parents=True)
    (trips_dir / "public_data_web_2026-10.csv").write_text(
        _csv(["M,1,1,085,01/10/2026,08:10:00,266-267,01/10/2026,08:40:00"]), encoding="utf-8"
    )
    TripsToSilverJob(settings).run("public_data_web_2026-10.csv")
    summary = write_demand(settings)
    assert summary["ejemplos_sin_emparejar"] == ["266-267"]
    assert (settings.data_dir / "gold/trip_demand/trip_demand.parquet").exists()


# --- gold ------------------------------------------------------------------------------------
def test_gold_rejilla_de_5_min_y_agregados_por_hora(
    settings: Settings, load_fixture: Loader
) -> None:
    _silver_con_cambio(settings, load_fixture)
    manifest = SilverToGoldJob(settings).run(DAY)
    gold = settings.data_dir / "gold"
    base = pl.from_arrow(
        pq.read_table(gold / f"features_base/dt={DAY}/part-0.parquet", schema=FEATURES_BASE_SCHEMA)
    )
    assert isinstance(base, pl.DataFrame)
    st1 = base.filter(pl.col("station_id") == "1")["bikes_available"].to_list()
    assert st1 == [7] * 4 + [3] * 8  # 18:00-18:15 = 7; desde 18:20 = 3
    hour = pq.read_table(
        gold / f"station_hour/dt={DAY}/part-0.parquet", schema=STATION_HOUR_SCHEMA
    ).to_pylist()
    by_id = {r["station_id"]: r for r in hour}
    assert by_id["1"]["bikes_mean"] == round(52 / 12, 4)
    assert (by_id["1"]["bikes_min"], by_id["1"]["bikes_max"], by_id["1"]["n_obs"]) == (3, 7, 2)
    assert by_id["2"]["pct_vacia"] == 1.0 and by_id["2"]["n_obs"] == 1
    assert manifest["filas_station_hour"] == 2 and manifest["filas_features_base"] == 24


def test_gold_reprocesar_da_el_mismo_manifiesto(settings: Settings, load_fixture: Loader) -> None:
    _silver_con_cambio(settings, load_fixture)
    same, first, second = SilverToGoldJob(settings).check_idempotent(DAY)
    assert same and first == second
    assert set(first) == {
        "dia",
        "horas_silver",
        "filas_station_hour",
        "filas_features_base",
        "sha256_station_hour",
    }


def test_gold_de_un_dia_sin_silver_queda_vacio(settings: Settings) -> None:
    manifest = SilverToGoldJob(settings).run("2026-01-01")
    assert manifest["filas_station_hour"] == 0


# --- SQL -------------------------------------------------------------------------------------
def test_split_queries_ignora_encabezado_y_separa_por_nombre() -> None:
    text = "-- Autor: x\n\n-- name: uno\nSELECT 1;\n\n-- name: dos\nSELECT 2 AS b;\n"
    assert split_queries(text) == [("uno", "SELECT 1"), ("dos", "SELECT 2 AS b")]
    with pytest.raises(ValueError):
        split_queries("SELECT 1;")


def test_reporte_sql_ejecuta_join_y_ventana(settings: Settings, load_fixture: Loader) -> None:
    _silver_con_cambio(settings, load_fixture)
    SilverToGoldJob(settings).run(DAY)
    sql = (
        "-- name: con_join\n"
        "SELECT i.short_name, g.n_obs FROM gold_station_hour g "
        "JOIN station_information i ON i.station_id = g.station_id ORDER BY 1;\n"
        "-- name: ventana\n"
        "SELECT station_id, RANK() OVER (ORDER BY n_obs DESC) AS lugar FROM gold_station_hour;\n"
    )
    markdown, summary = run_sql_report(settings.data_dir, sql)
    assert summary == [{"nombre": "con_join", "filas": 2}, {"nombre": "ventana", "filas": 2}]
    assert "## con_join" in markdown and "| 085 | 1 |" in markdown


# --- probe y descarga (sin red) --------------------------------------------------------------
def test_probe_pide_rango_y_revisa_encabezado() -> None:
    with respx.mock(assert_all_mocked=True) as mock, httpx.Client() as client:
        body = ("﻿" + EXPECTED_HEADER + "\nM,1").encode()  # con BOM, como el real
        route = mock.get(CSV_URL).mock(return_value=httpx.Response(206, content=body))
        assert probe(CSV_URL, client) == (True, EXPECTED_HEADER)
        assert route.calls.last.request.headers["Range"] == "bytes=0-2047"
        route.mock(return_value=httpx.Response(206, content=b"otra,cosa\n1,2"))
        assert probe(CSV_URL, client)[0] is False


def test_latest_csv_url_toma_el_mes_mas_reciente() -> None:
    html = (
        '<a href="/wp-content/uploads/2026/09/datos_2026-09.csv">sep</a>'
        '<a href="https://ecobici.cdmx.gob.mx/wp-content/uploads/2026/10/public_data_web_2026-10.csv">'
        "oct</a><a href='/otro.pdf'>x</a>"
    )
    assert latest_csv_url(html) == (
        "https://ecobici.cdmx.gob.mx/wp-content/uploads/2026/10/public_data_web_2026-10.csv"
    )
    assert latest_csv_url("<p>sin enlaces</p>") is None


def test_download_omite_si_mismo_tamano_y_no_reescribe(tmp_path: Path) -> None:
    body = _csv(["M,1,1,085,01/10/2026,08:10:00,710,01/10/2026,08:40:00"]).encode()
    dest = tmp_path / "bronze" / "trips"
    with respx.mock(assert_all_mocked=True) as mock, httpx.Client() as client:
        mock.get(CSV_URL).mock(return_value=httpx.Response(200, content=body))
        assert download(CSV_URL, dest, client)[0] == "descargado"
        assert download(CSV_URL, dest, client)[0] == "omitido"
        (dest / "public_data_web_2026-10.csv").write_bytes(b"distinto")
        with pytest.raises(FileExistsError):
            download(CSV_URL, dest, client)
    assert not list(dest.glob("*.tmp"))


# --- benchmark -------------------------------------------------------------------------------
def test_bench_engine_duckdb_y_pandas_dan_lo_mismo(tmp_path: Path) -> None:
    text = _csv(
        [
            "M,1,1,085,01/10/2026,08:10:00,710,01/10/2026,08:40:00",
            "M,1,1,085,01/10/2026,08:20:00,710,01/10/2026,08:30:00",
            "M,1,1,710,02/10/2026,19:00:00,085,02/10/2026,19:05:00",
        ]
    )
    csv = tmp_path / "viajes_2026-10.csv"
    csv.write_text(text, encoding="utf-8")
    trips, _ = parse_trips(text)
    parquet = tmp_path / "part-0.parquet"
    pq.write_table(trips.to_arrow().cast(SILVER_TRIPS_SCHEMA), parquet)
    results = [
        bench_engine(engine, source, src, "utf-8-sig")
        for source, src in (("parquet", parquet.as_posix()), ("csv", csv.as_posix()))
        for engine in ("duckdb", "pandas")
    ]
    assert {(r["grupos"], r["viajes"], r["duracion_total"]) for r in results} == {(2, 3, 45.0)}
    assert all(r["pico_mb"] >= r["base_mb"] > 0 for r in results)
