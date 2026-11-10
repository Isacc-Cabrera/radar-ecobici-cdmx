"""
Autor: Isacc Cabrera
Descripción: Evaluación temporal de los modelos. time_series_folds parte el marco por TIEMPO
    (no por filas, nunca al azar): los instantes únicos se dividen en n_splits + 1 bloques y el
    fold k entrena con los bloques 0..k y valida con el k+1. Además PURGA del entrenamiento las
    filas cuyo target (ts + 60 min) cae dentro del periodo de validación: sin esa purga, la
    última hora de entrenamiento "conocería" valores que se usan para validar. Calcula MAE,
    RMSE y R², la brecha de sobreajuste y la gráfica real vs predicho (Plotly HTML).
Patrón: N/A (funciones puras)
Fecha de creación: 2026-11-10
Cambios:
    - 2026-11-10 · Isacc Cabrera · Creación (paso 11).
"""

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

HORIZON = pd.Timedelta(minutes=60)


@dataclass(frozen=True)
class Fold:
    """Un fold temporal: índices de entrenamiento y validación del marco original.

    Attributes:
        k: Número de fold (0 = el que menos historia tiene).
        train_idx: Posiciones de entrenamiento (ya purgadas).
        valid_idx: Posiciones de validación.
        valid_start: Primer instante de validación.
    """

    k: int
    train_idx: np.ndarray
    valid_idx: np.ndarray
    valid_start: pd.Timestamp


def time_series_folds(
    df: pd.DataFrame, n_splits: int = 4, horizon: pd.Timedelta = HORIZON
) -> Iterator[Fold]:
    """Folds expandibles sobre el tiempo, con purga del horizonte del target.

    Args:
        df: Marco con ``ts_utc``.
        n_splits: Número de folds.
        horizon: Distancia entre una fila y su target (60 min).

    Yields:
        Folds en orden temporal; cada validación es estrictamente posterior a su entrenamiento.
        Un fold cuyo entrenamiento queda vacío después de la purga se omite.

    Raises:
        ValueError: Si hay menos instantes únicos que bloques.
    """
    # datetime64[ns] sin zona (valores UTC): numpy compara y suma sin objetos Timestamp
    ts = pd.to_datetime(df["ts_utc"], utc=True).dt.tz_convert(None).to_numpy("datetime64[ns]")
    unique = np.unique(ts)
    if len(unique) < n_splits + 1:
        raise ValueError(f"se necesitan ≥ {n_splits + 1} instantes distintos, hay {len(unique)}")
    blocks = np.array_split(unique, n_splits + 1)
    for k in range(n_splits):
        valid_start = blocks[k + 1][0]
        valid_end = blocks[k + 1][-1]
        # purga: el target de la fila (ts + horizonte) debe caer antes de la validación
        train_mask = ts + horizon.to_timedelta64() < valid_start
        valid_mask = (ts >= valid_start) & (ts <= valid_end)
        if not train_mask.any():  # el primer bloque puede quedar vacío tras la purga
            continue
        yield Fold(
            k=k,
            train_idx=np.flatnonzero(train_mask),
            valid_idx=np.flatnonzero(valid_mask),
            valid_start=pd.Timestamp(valid_start).tz_localize("UTC"),
        )


def metrics(y: np.ndarray | pd.Series, yhat: np.ndarray) -> dict[str, float]:
    """MAE, RMSE y R² redondeados a 4 decimales.

    Args:
        y: Valores reales.
        yhat: Predicciones.

    Returns:
        ``{"mae", "rmse", "r2"}``.
    """
    y_arr = np.asarray(y, dtype=float)
    return {
        "mae": round(float(mean_absolute_error(y_arr, yhat)), 4),
        "rmse": round(float(np.sqrt(mean_squared_error(y_arr, yhat))), 4),
        "r2": round(float(r2_score(y_arr, yhat)), 4),
    }


def overfit_gap(train_mae: float, valid_mae: float) -> float:
    """Brecha de sobreajuste: cuánto peor es validar que entrenar, en % del MAE de validación.

    0 % = generaliza igual que memoriza; cerca de 100 % = memoriza (MAE de entrenamiento ≈ 0).

    Args:
        train_mae: MAE sobre los datos de entrenamiento.
        valid_mae: MAE sobre validación.

    Returns:
        ``100 · (valid − train) / valid`` (0 si ``valid_mae`` es 0).
    """
    if valid_mae == 0:
        return 0.0
    return round(100 * (valid_mae - train_mae) / valid_mae, 2)


def plot_real_vs_pred(
    frame: pd.DataFrame, path: Path, title: str, stations: list[str], tz: str
) -> Path:
    """Gráfica Plotly real vs predicho (en el instante objetivo t + 60) para unas estaciones.

    Args:
        frame: ``station_id``, ``etiqueta``, ``ts_utc``, ``y_t60``, ``pred``, ``baseline``.
        path: Archivo HTML de salida.
        title: Título.
        stations: ``station_id`` a graficar.
        tz: Zona horaria para el eje x.

    Returns:
        La ruta escrita.
    """
    fig = go.Figure()
    for sid in stations:
        g = frame[frame["station_id"] == sid].sort_values("ts_utc")
        if g.empty:
            continue
        x = (pd.to_datetime(g["ts_utc"], utc=True) + HORIZON).dt.tz_convert(tz)
        label = str(g["etiqueta"].iloc[0])
        fig.add_scatter(x=x, y=g["y_t60"], mode="lines", name=f"{label} · real")
        fig.add_scatter(
            x=x, y=g["pred"], mode="lines", line={"dash": "dash"}, name=f"{label} · modelo"
        )
        fig.add_scatter(
            x=x,
            y=g["baseline"],
            mode="lines",
            line={"dash": "dot"},
            opacity=0.5,
            name=f"{label} · baseline",
        )
    fig.update_layout(
        title=title,
        xaxis_title="Hora objetivo (CDMX)",
        yaxis_title="Bicis disponibles",
        legend={"orientation": "h", "y": -0.2},
        height=620,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(path, include_plotlyjs="cdn")
    return path
