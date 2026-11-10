"""
Autor: Isacc Cabrera
Descripción: Pruebas del paso 15 sobre datos sintéticos: el detector de congeladas marca solo
    las estaciones cuyo last_reported no avanzó en la ventana; el z-score marca |z| ≥ umbral,
    omite estaciones con menos de 3 días de historia en esa hora o desviación 0 y no usa la
    última hora como referencia; Isolation Forest es reproducible, marca un estado imposible
    para la historia y no corre sin historia suficiente; run_all/summarize juntan e intersecan;
    y el CLI corre sobre un lake mínimo.
Patrón: N/A (pruebas)
Fecha de creación: 2026-11-10
Cambios:
    - 2026-11-10 · Isacc Cabrera · Creación (paso 15).
"""

import json
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from radar.anomalies import detectors as det
from radar.anomalies.detectors import (
    Anomaly,
    FrozenStationDetector,
    IsolationForestDetector,
    ZScoreDetector,
    make_detectors,
    run_all,
    state_at,
    summarize,
)
from radar.config import Settings

NOW = pd.Timestamp("2026-11-13T15:00:00Z")  # 09:00 en CDMX


def _recent(rows: list[tuple[str, pd.Timestamp, pd.Timestamp, int]], cap: int = 20) -> pd.DataFrame:
    """Silver reciente: (station_id, snapshot, last_reported, bicis)."""
    return pd.DataFrame(
        {
            "station_id": [r[0] for r in rows],
            "snapshot_last_updated": [r[1] for r in rows],
            "last_reported": [r[2] for r in rows],
            "num_bikes_available": [r[3] for r in rows],
            "capacity": [cap] * len(rows),
        }
    )


def _history(stations: dict[str, list[float]], days: int, hour_utc: int = 15) -> pd.DataFrame:
    """Rejilla de 5 min en la hora ``hour_utc`` de ``days`` días anteriores a NOW.

    ``stations[sid][d]`` = bicis constantes en esa hora del día ``d``.
    """
    rows = []
    for sid, per_day in stations.items():
        for d in range(days):
            start = (NOW - pd.Timedelta(days=d + 1)).replace(hour=hour_utc)
            for k in range(12):
                rows.append(
                    {
                        "station_id": sid,
                        "ts_utc": start + pd.Timedelta(minutes=5 * k),
                        "bikes_available": per_day[d],
                        "capacity": 20,
                    }
                )
    return pd.DataFrame(rows)


# --- congelada -----------------------------------------------------------------------------------
def test_congelada_marca_solo_sin_reporte_en_la_ventana() -> None:
    m = pd.Timedelta(minutes=1)
    recent = _recent(
        [
            ("vieja", NOW - 100 * m, NOW - 95 * m, 5),  # last_reported hace 95 min
            ("vieja", NOW - 1 * m, NOW - 95 * m, 5),  # el snapshot avanza, el reporte no
            ("viva", NOW - 100 * m, NOW - 100 * m, 5),
            ("viva", NOW - 1 * m, NOW - 2 * m, 6),  # avanzó dentro de la ventana
            ("justa", NOW - 1 * m, NOW - 90 * m, 7),  # exactamente en el umbral
        ]
    )
    found = FrozenStationDetector(90).detect(recent, pd.DataFrame(), NOW.to_pydatetime())
    assert [a.station_id for a in found] == ["justa", "vieja"]
    assert all(a.tipo == "congelada" for a in found)
    assert {a.station_id: a.puntaje for a in found} == {"justa": 90.0, "vieja": 95.0}


# --- z-score -------------------------------------------------------------------------------------
def _now_rows(bikes: dict[str, int]) -> pd.DataFrame:
    snap = NOW - pd.Timedelta(minutes=1)
    return _recent([(sid, snap, snap, b) for sid, b in bikes.items()])


def test_zscore_marca_lo_lejano_y_omite_poca_historia() -> None:
    history = pd.concat(
        [
            _history({"a": [10, 11, 9, 10]}, days=4),  # media 10, desv ≈ 0.82
            _history({"b": [10, 11]}, days=2),  # solo 2 días: se omite
            _history({"c": [10, 11, 9, 10]}, days=4),
        ]
    )
    recent = _now_rows({"a": 18, "b": 30, "c": 10})
    found = ZScoreDetector(3.0).detect(recent, history, NOW.to_pydatetime())
    assert [a.station_id for a in found] == ["a"]
    assert found[0].tipo == "zscore"
    assert found[0].puntaje == pytest.approx(
        abs((18 - 10) / np.std([10, 11, 9, 10], ddof=1)), rel=1e-3
    )
    assert "4 días" in found[0].detalle


def test_zscore_omite_desviacion_cero_y_otras_horas() -> None:
    history = pd.concat(
        [
            _history({"plana": [10, 10, 10]}, days=3),  # desviación 0
            _history({"otra_hora": [1, 2, 3]}, days=3, hour_utc=20),  # otra hora local
        ]
    )
    recent = _now_rows({"plana": 19, "otra_hora": 20})
    assert ZScoreDetector(3.0).detect(recent, history, NOW.to_pydatetime()) == []


def test_zscore_no_usa_la_ultima_hora_como_referencia() -> None:
    hoy = _history({"a": [18]}, days=1)
    hoy["ts_utc"] = hoy["ts_utc"] + pd.Timedelta(days=1) - pd.Timedelta(minutes=30)  # hace 30 min
    history = pd.concat([_history({"a": [10, 11, 9]}, days=3), hoy])
    ref = ZScoreDetector().reference(history, NOW)
    assert ref.loc["a", "n"] == 3  # el tramo de hace 30 min no cuenta


# --- Isolation Forest ----------------------------------------------------------------------------
def _normal_history(n_stations: int = 30, days: int = 3) -> pd.DataFrame:
    rng = np.random.default_rng(0)
    rows = []
    for s in range(n_stations):
        for d in range(days):
            start = (NOW - pd.Timedelta(days=d + 1)).replace(hour=13)
            level = 10.0
            for k in range(48):  # 4 h de rejilla con cambios pequeños
                level = float(np.clip(level + rng.integers(-1, 2), 6, 14))
                rows.append(
                    {
                        "station_id": str(s),
                        "ts_utc": start + pd.Timedelta(minutes=5 * k),
                        "bikes_available": level,
                        "capacity": 20,
                    }
                )
    return pd.DataFrame(rows)


def test_isolation_forest_marca_un_estado_imposible_y_es_reproducible() -> None:
    history = _normal_history()
    m = pd.Timedelta(minutes=1)
    recent = _recent(
        [("normal", NOW - 61 * m, NOW - 61 * m, 10), ("normal", NOW - m, NOW - m, 11)]
        + [("raro", NOW - 61 * m, NOW - 61 * m, 0), ("raro", NOW - m, NOW - m, 20)]
    )
    now = (NOW.replace(hour=15)).to_pydatetime()
    first = IsolationForestDetector().detect(recent, history, now)
    second = IsolationForestDetector().detect(recent, history, now)
    assert [a.station_id for a in first] == ["raro"]  # +20 bicis en 60 min y 100 % llena
    assert first == second
    assert first[0].tipo == "isolation_forest" and "+20" in first[0].detalle


def test_isolation_forest_sin_historia_no_marca_nada() -> None:
    recent = _now_rows({"a": 5})
    assert (
        IsolationForestDetector().detect(
            recent,
            pd.DataFrame(columns=["station_id", "ts_utc", "bikes_available", "capacity"]),
            NOW.to_pydatetime(),
        )
        == []
    )


# --- utilidades y CLI ----------------------------------------------------------------------------
def test_state_at_toma_el_ultimo_valor_hasta_ese_instante() -> None:
    m = pd.Timedelta(minutes=1)
    recent = _recent([("a", NOW - 70 * m, NOW - 70 * m, 3), ("a", NOW - 5 * m, NOW - 5 * m, 9)])
    assert state_at(recent, NOW - 60 * m)["bikes"].tolist() == [3.0]
    assert state_at(recent, NOW)["bikes"].tolist() == [9.0]


def test_summarize_cuenta_e_interseca() -> None:
    t = NOW.to_pydatetime()
    found = [
        Anomaly("1", "congelada", 120.0, "x", t),
        Anomaly("1", "isolation_forest", 0.7, "x", t),
        Anomaly("2", "zscore", 4.0, "x", t),
    ]
    out = summarize(found)
    assert out["conteos"] == {"congelada": 1, "zscore": 1, "isolation_forest": 1}
    assert out["interseccion"]["congelada&isolation_forest"] == ["1"]  # type: ignore[index]
    assert out["interseccion"]["todos"] == []  # type: ignore[index]
    assert out["estaciones_unicas"] == 2


def test_make_detectors_usa_los_umbrales_de_settings(settings: Settings) -> None:
    s = settings.model_copy(update={"anomaly_frozen_minutes": 45.0, "anomaly_zscore": 2.5})
    frozen, zscore, forest = make_detectors(s)
    assert (frozen.minutes, zscore.threshold) == (45.0, 2.5)  # type: ignore[attr-defined]
    assert forest.contamination == 0.02 and forest.random_state == 42  # type: ignore[attr-defined]
    assert run_all([frozen], _recent([]), pd.DataFrame(), NOW.to_pydatetime()) == []


def test_cli_imprime_conteos_e_interseccion(
    settings: Settings, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    part = settings.data_dir / "silver/station_status/dt=2026-11-13/hour=14"
    part.mkdir(parents=True)
    m = pd.Timedelta(minutes=1)
    recent = _recent([("1", NOW - 2 * m, NOW - 200 * m, 4), ("2", NOW - 2 * m, NOW - 3 * m, 4)])
    recent.to_parquet(part / "part-0.parquet")
    monkeypatch.setattr(det, "get_settings", lambda: settings)
    assert det.main(["--json"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["conteos"] == {"congelada": 1, "zscore": 0, "isolation_forest": 0}
    assert set(out["interseccion"]) == {
        "congelada&zscore",
        "congelada&isolation_forest",
        "zscore&isolation_forest",
        "todos",
    }


def test_cli_sin_silver_sale_3(settings: Settings, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(det, "get_settings", lambda: settings)
    assert det.main(["--json"]) == 3


def test_anomaly_conserva_el_instante(tmp_path: Path) -> None:
    a = Anomaly("1", "zscore", 3.2, "x", datetime(2026, 11, 13, 15) + timedelta(minutes=1))
    assert a.detectado_utc.minute == 1
