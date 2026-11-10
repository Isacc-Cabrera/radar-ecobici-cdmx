"""
Autor: Isacc Cabrera
Descripción: Pruebas del paso 10 sobre un marco sintético: alterar valores posteriores a t no
    cambia ninguna feature en t (sin fuga), y_t60 es el valor 12 pasos después en la misma
    estación y no salta huecos de la rejilla, calendario en hora de CDMX con festivos de México,
    clima as-of hacia atrás, demanda por estación y marco de inferencia sin target.
Patrón: N/A (pruebas)
Fecha de creación: 2026-11-10
Cambios:
    - 2026-11-10 · Isacc Cabrera · Creación (paso 10).
"""

from datetime import UTC, datetime

import numpy as np
import pandas as pd
import pytest

from radar.ml.features import (
    FEATURES,
    LAGS,
    ROLLS,
    TARGET,
    add_calendar,
    build_inference_frame,
    build_training_frame,
    demand_by_station,
)

START = pd.Timestamp("2026-11-10T12:00:00Z")  # 06:00 en CDMX, martes
STEPS = 48  # 4 horas de rejilla


def _grid(stations: tuple[str, ...] = ("1", "2"), steps: int = STEPS) -> pd.DataFrame:
    rows = []
    for k, sid in enumerate(stations):
        for i in range(steps):
            rows.append(
                {
                    "station_id": sid,
                    "ts_utc": START + i * pd.Timedelta(minutes=5),
                    "bikes_available": (i * 7 + k * 3) % 20,
                    "docks_available": 0,
                    "capacity": 20,
                    "is_renting": True,
                }
            )
    return pd.DataFrame(rows)


def _weather() -> pd.DataFrame:
    hours = pd.date_range(START - pd.Timedelta(hours=1), periods=8, freq="h", tz="UTC")
    return pd.DataFrame(
        {
            "ts_utc": hours,
            "temperature_2m": np.arange(8, dtype=float) + 10,
            "precipitation": np.zeros(8),
            "relative_humidity_2m": np.zeros(8),
            "wind_speed_10m": np.zeros(8),
        }
    )


def _row(df: pd.DataFrame, sid: str, ts: pd.Timestamp) -> pd.Series:
    match = df[(df["station_id"] == sid) & (df["ts_utc"] == ts)]
    assert len(match) == 1
    return match.iloc[0]


@pytest.mark.parametrize("cut", [12, 20, 30])
def test_alterar_el_futuro_no_cambia_las_features_en_t(cut: int) -> None:
    base = _grid()
    t = START + cut * pd.Timedelta(minutes=5)
    altered = base.copy()
    futuro = altered["ts_utc"] > t
    altered.loc[futuro, "bikes_available"] = 999
    a = build_training_frame(base, _weather(), None)
    b = build_training_frame(altered, _weather(), None)
    for sid in ("1", "2"):
        ra, rb = _row(a, sid, t), _row(b, sid, t)
        for col in FEATURES:
            assert ra[col] == rb[col] or (pd.isna(ra[col]) and pd.isna(rb[col])), col
        assert ra[TARGET] != rb[TARGET]  # el target sí es futuro: debe cambiar


def test_lags_y_ventanas_tienen_los_valores_del_pasado() -> None:
    base = _grid()
    df = build_training_frame(base, None, None)
    t = START + 24 * pd.Timedelta(minutes=5)
    row = _row(df, "1", t)
    serie = base[base["station_id"] == "1"].set_index("ts_utc")["bikes_available"]
    assert row["lag_15"] == serie[t - pd.Timedelta(minutes=15)]
    assert row["lag_30"] == serie[t - pd.Timedelta(minutes=30)]
    assert row["lag_60"] == serie[t - pd.Timedelta(minutes=60)]
    assert row["roll_mean_30"] == pytest.approx(serie[t - pd.Timedelta(minutes=25) : t].mean())
    assert row["roll_mean_60"] == pytest.approx(serie[t - pd.Timedelta(minutes=55) : t].mean())
    first = _row(df, "1", START)
    assert all(pd.isna(first[c]) for c in LAGS)  # sin pasado, NaN (no se inventa)
    assert first["roll_mean_30"] == serie[START]
    assert set(ROLLS) <= set(df.columns)


def test_y_t60_es_12_pasos_despues_y_descarta_filas_sin_target() -> None:
    base = _grid()
    df = build_training_frame(base, None, None)
    for sid in ("1", "2"):
        serie = base[base["station_id"] == sid]["bikes_available"].to_numpy()
        got = df[df["station_id"] == sid][TARGET].to_numpy()
        np.testing.assert_array_equal(got, serie[12:])
    assert len(df) == 2 * (STEPS - 12)
    assert df[TARGET].notna().all()


def test_y_t60_no_salta_un_hueco_de_la_rejilla() -> None:
    base = _grid(("1",))
    hueco = (base["ts_utc"] >= START + pd.Timedelta(hours=1)) & (
        base["ts_utc"] < START + pd.Timedelta(hours=2)
    )
    df = build_training_frame(base[~hueco], None, None)
    # con shift(-12) por filas, las 12:00 tomarían el valor de las 14:00; por tiempo no hay target
    assert START not in set(df["ts_utc"])
    assert (df["ts_utc"] + pd.Timedelta(minutes=60)).isin(set(base[~hueco]["ts_utc"])).all()


def test_calendario_en_hora_de_cdmx_y_festivos() -> None:
    df = pd.DataFrame(
        {
            "ts_utc": pd.to_datetime(
                ["2026-12-25T18:00:00Z", "2026-11-10T05:30:00Z", "2026-11-15T03:00:00Z"],
                utc=True,
            )
        }
    )
    cal = add_calendar(df)
    assert cal["is_holiday"].tolist() == [1, 0, 0]
    assert cal["hour_local"].tolist() == [12, 23, 21]  # UTC-6: 05:30Z del 10 = 23:30 del 9
    assert cal["dow"].tolist() == [4, 0, 5]  # viernes, lunes 9, sábado 14
    assert cal["is_weekend"].tolist() == [0, 0, 1]


def test_clima_usa_la_ultima_hora_anterior_o_igual() -> None:
    df = build_training_frame(_grid(), _weather(), None)
    w = _weather().set_index("ts_utc")["temperature_2m"]
    for minutes in (0, 55, 60, 65):
        t = START + pd.Timedelta(minutes=minutes)
        expected = w[t.floor("h")]
        assert _row(df, "1", t)["temperature_2m"] == expected


def test_demanda_por_estacion_dia_y_hora_local() -> None:
    info = pd.DataFrame({"station_id": ["1", "2"], "short_name_norm": ["85", "266-267"]})
    demand = pd.DataFrame(
        {
            "short_name_norm": ["85", "999"],
            "dow": [1, 1],
            "hour_local": [6, 6],
            "salidas_prom": [4.0, 9.0],
            "llegadas_prom": [1.5, 9.0],
            "tasa_match": [1.0, 1.0],
        }
    )
    df = build_training_frame(_grid(), None, demand_by_station(demand, info))
    a_las_6 = _row(df, "1", START)  # martes 06:00 local
    assert (a_las_6["demanda_salidas"], a_las_6["demanda_llegadas"]) == (4.0, 1.5)
    assert _row(df, "2", START)["demanda_salidas"] == 0.0  # sin viajes en esa hora
    assert _row(df, "1", START + pd.Timedelta(hours=1))["demanda_salidas"] == 0.0


def test_inferencia_da_la_ultima_fila_por_estacion_sin_target() -> None:
    base = _grid()
    now = datetime(2026, 11, 10, 14, 2, tzinfo=UTC)
    inf = build_inference_frame(base, _weather(), None, now)
    assert list(inf.columns) == ["station_id", "ts_utc", *FEATURES]
    assert inf["station_id"].tolist() == ["1", "2"]
    assert (inf["ts_utc"] == pd.Timestamp("2026-11-10T14:00:00Z")).all()
    train = build_training_frame(base, _weather(), None)
    t = pd.Timestamp("2026-11-10T14:00:00Z")
    for col in FEATURES:
        assert inf.iloc[0][col] == _row(train, "1", t)[col], col
