"""
Autor: Isacc Cabrera
Descripción: Pruebas del paso 11 sin red ni MLflow: los folds temporales validan estrictamente
    después de entrenar y purgan las filas cuyo target cae en validación; métricas y brecha de
    sobreajuste; cada estrategia recorta a [0, capacity]; LastWeekBaseline queda no disponible
    con menos de 7 días; LightGBM aprende el cambio; el adaptador pyfunc delega en el
    Predictor; cobertura de heartbeat y tabla de comparación.
Patrón: N/A (pruebas)
Fecha de creación: 2026-11-10
Cambios:
    - 2026-11-10 · Isacc Cabrera · Creación (paso 11).
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from radar.ml.evaluate import HORIZON, metrics, overfit_gap, time_series_folds
from radar.ml.features import FEATURES, TARGET, build_training_frame
from radar.ml.models import (
    LIGHTGBM_CONFIGS,
    LastWeekBaseline,
    LightGBMPredictor,
    LinearPredictor,
    PersistenceBaseline,
    RadarPyfuncModel,
    clip_to_capacity,
    make_predictors,
)
from radar.ml.train import comparison_markdown, covered_hours

START = pd.Timestamp("2026-11-09T21:00:00Z")


def _frame(hours: int = 6, stations: int = 3, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rows = []
    for s in range(stations):
        level = 10.0
        for i in range(hours * 12):
            level = float(np.clip(level + rng.integers(-2, 3), 0, 20))
            rows.append(
                {
                    "station_id": str(s),
                    "ts_utc": START + i * pd.Timedelta(minutes=5),
                    "bikes_available": level,
                    "docks_available": 20 - level,
                    "capacity": 20,
                    "is_renting": True,
                }
            )
    return build_training_frame(pd.DataFrame(rows), None, None)


# --- folds temporales -----------------------------------------------------------------------
def test_cada_fold_valida_despues_de_todo_su_entrenamiento() -> None:
    df = _frame(hours=11)  # 10 h con target: bloques de 2 h, el primero sobrevive a la purga
    folds = list(time_series_folds(df, n_splits=4))
    assert len(folds) == 4
    previous_train = 0
    for fold in folds:
        train_ts = df["ts_utc"].iloc[fold.train_idx]
        valid_ts = df["ts_utc"].iloc[fold.valid_idx]
        assert len(train_ts) and len(valid_ts)
        assert valid_ts.min() > train_ts.max()  # estrictamente después
        # purga: ningún target de entrenamiento (ts + 60) cae dentro de la validación
        assert (train_ts + HORIZON).max() < valid_ts.min()
        assert not set(fold.train_idx) & set(fold.valid_idx)
        assert len(fold.train_idx) >= previous_train  # folds expandibles
        previous_train = len(fold.train_idx)


def test_fold_sin_entrenamiento_tras_la_purga_se_omite() -> None:
    df = _frame(hours=6)  # bloques de 1 h: todo el primer bloque tiene su target en el segundo
    folds = list(time_series_folds(df, n_splits=4))
    assert [f.k for f in folds] == [1, 2, 3]
    assert all(len(f.train_idx) for f in folds)


def test_folds_con_pocos_instantes_fallan_claro() -> None:
    df = _frame(hours=2).head(3)
    with pytest.raises(ValueError, match="instantes"):
        list(time_series_folds(df, n_splits=4))


# --- métricas ----------------------------------------------------------------------------------
def test_metricas_y_brecha_de_sobreajuste() -> None:
    m = metrics(np.array([1.0, 2.0, 3.0, 4.0]), np.array([1.0, 2.0, 3.0, 6.0]))
    assert m == {"mae": 0.5, "rmse": 1.0, "r2": 0.2}
    assert overfit_gap(1.0, 2.0) == 50.0
    assert overfit_gap(2.0, 2.0) == 0.0
    assert overfit_gap(0.0, 0.0) == 0.0


# --- estrategias -------------------------------------------------------------------------------
def test_recorte_a_cero_y_capacidad() -> None:
    X = pd.DataFrame({"capacity": [10, 10, 10]})
    np.testing.assert_array_equal(clip_to_capacity(np.array([-3.0, 5.0, 12.0]), X), [0, 5, 10])


def test_persistencia_predice_las_bicis_actuales() -> None:
    df = _frame()
    pred = PersistenceBaseline().predict(df)
    np.testing.assert_array_equal(pred, df["bikes_available"].to_numpy())


def test_semana_anterior_no_disponible_con_menos_de_7_dias() -> None:
    df = _frame()
    model = LastWeekBaseline()
    model.fit(df, df[TARGET])
    assert model.available is False
    with pytest.raises(RuntimeError):
        model.predict(df)


def test_semana_anterior_busca_el_mismo_momento_7_dias_antes() -> None:
    hist = pd.DataFrame(
        {
            "station_id": ["1", "1"],
            "ts_utc": [START, START + pd.Timedelta(days=8)],
            "bikes_available": [7.0, 3.0],
            "capacity": [20, 20],
        }
    )
    model = LastWeekBaseline()
    model.fit(hist, hist["bikes_available"])
    assert model.available is True
    # objetivo = t + 60; hace 7 días de (START + 7d − 60 min + 60 min) es START → 7 bicis
    query = pd.DataFrame(
        {
            "station_id": ["1", "1"],
            "ts_utc": [START + pd.Timedelta(days=7, minutes=-60), START + pd.Timedelta(days=9)],
            "bikes_available": [1.0, 4.0],
            "capacity": [20, 20],
        }
    )
    np.testing.assert_array_equal(model.predict(query), [7.0, 4.0])  # sin historia: persistencia


@pytest.mark.parametrize(
    "model", [LinearPredictor(), LightGBMPredictor("lightgbm_a", LIGHTGBM_CONFIGS["lightgbm_a"])]
)
def test_modelos_aceptan_lags_vacios_y_recortan(model: LinearPredictor | LightGBMPredictor) -> None:
    df = _frame()
    assert df[FEATURES].isna().any().any()  # el inicio de cada estación no tiene lags
    model.fit(df, df[TARGET])
    pred = model.predict(df)
    assert pred.shape == (len(df),)
    assert (pred >= 0).all() and (pred <= df["capacity"].to_numpy()).all()


def test_lightgbm_aprende_el_cambio() -> None:
    df = _frame(hours=8)
    df = df[df["bikes_available"] <= 15].copy()
    y = df["bikes_available"] + 2  # dentro de una hora siempre hay 2 bicis más
    model = LightGBMPredictor("lightgbm_a", LIGHTGBM_CONFIGS["lightgbm_a"])
    model.fit(df, y)
    np.testing.assert_allclose(model.predict(df), y.to_numpy(), atol=0.05)
    assert model.params()["target"] == "cambio"


def test_estrategias_a_comparar() -> None:
    names = [p.name for p in make_predictors()]
    assert names[:3] == ["baseline", "baseline_semana", "lineal"]
    assert sum(n.startswith("lightgbm_") for n in names) == 3


def test_adaptador_pyfunc_delega_en_el_predictor() -> None:
    df = _frame()
    wrapped = RadarPyfuncModel(PersistenceBaseline())
    np.testing.assert_array_equal(wrapped.predict(None, df), df["bikes_available"].to_numpy())


# --- entrenamiento -----------------------------------------------------------------------------
def test_cobertura_excluye_horas_con_huecos(tmp_path: Path) -> None:
    hb = tmp_path / "bronze" / "heartbeat"
    hb.mkdir(parents=True)
    lines = []
    for m in range(60):  # 21 h completa
        ts = START + pd.Timedelta(minutes=m, seconds=3)
        lines.append({"ts_utc": ts.isoformat(), "feed": "station_status", "result": "guardado"})
    for m in range(4):  # 22 h solo 4 minutos
        ts = START + pd.Timedelta(hours=1, minutes=m)
        lines.append({"ts_utc": ts.isoformat(), "feed": "station_status", "result": "guardado"})
    (hb / "dt=2026-11-09.jsonl").write_text("\n".join(json.dumps(r) for r in lines), "utf-8")
    assert covered_hours(tmp_path, 10.0) == {START}


def test_tabla_de_comparacion_marca_ganador_y_no_disponibles() -> None:
    fold = {"fold": 0, "mae": 3.0, "valida_desde": "2026-11-09T23:00:00+00:00"}
    entry = {
        "disponible": True,
        "mae": 3.0,
        "rmse": 4.0,
        "r2": 0.5,
        "mae_train": 2.9,
        "brecha_sobreajuste_pct": 3.3,
        "folds": [fold],
    }
    results = {
        "baseline": entry,
        "baseline_semana": {"disponible": False},
        "lightgbm_a": {**entry, "mae": 2.8, "folds": [{**fold, "mae": 2.8}]},
    }
    data = {
        "filas": 10,
        "estaciones": 2,
        "horas_usadas": 1,
        "horas_gold": 2,
        "desde": "a",
        "hasta": "b",
        "horas_excluidas": ["2026-11-10T04"],
    }
    md = comparison_markdown(results, "lightgbm_a", 6.67, data)
    assert "| lightgbm_a **(ganador)** | 2.800 |" in md
    assert "| baseline_semana | no disponible |" in md
    assert "**6.7 %**" in md and "2026-11-10T04" in md
