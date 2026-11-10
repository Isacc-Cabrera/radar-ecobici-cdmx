"""
Autor: Isacc Cabrera
Descripción: Estrategias de predicción de bicis a t+60 min con una interfaz común (Predictor):
    PersistenceBaseline (lo mismo que ahora), LastWeekBaseline (mismo momento 7 días antes;
    no disponible si no hay una semana de historia), LinearPredictor (imputación por mediana +
    estandarización + regresión lineal) y LightGBMPredictor con 3 configuraciones fijas de
    simple a compleja que aprenden el cambio respecto a las bicis actuales. Todas recortan la
    predicción a [0, capacity]. RadarPyfuncModel envuelve cualquier Predictor para
    registrarlo en MLflow como modelo pyfunc.
Patrón: Strategy (Predictor y sus implementaciones intercambiables)
Fecha de creación: 2026-11-10
Cambios:
    - 2026-11-10 · Isacc Cabrera · Creación (paso 11).
"""

from typing import Any, ClassVar, Protocol, runtime_checkable

import lightgbm as lgb
import numpy as np
import pandas as pd
from mlflow.pyfunc import PythonModel
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from radar.ml.features import FEATURES

LIGHTGBM_CONFIGS: dict[str, dict[str, Any]] = {
    "lightgbm_a": {"num_leaves": 15, "learning_rate": 0.05, "n_estimators": 300},
    "lightgbm_b": {
        "num_leaves": 31,
        "learning_rate": 0.05,
        "n_estimators": 500,
        "min_child_samples": 50,
    },
    "lightgbm_c": {
        "num_leaves": 63,
        "learning_rate": 0.03,
        "n_estimators": 800,
        "subsample": 0.8,
        "subsample_freq": 1,
        "colsample_bytree": 0.8,
    },
}
LAST_WEEK = pd.Timedelta(days=7)
SEED = 42


@runtime_checkable
class Predictor(Protocol):
    """Interfaz de toda estrategia de predicción.

    Attributes:
        name: Nombre corto (``baseline``, ``lineal``, ``lightgbm_a``…).
        available: False si la estrategia no puede evaluarse con los datos actuales.
    """

    name: str
    available: bool

    def fit(self, X: pd.DataFrame, y: pd.Series) -> None:
        """Entrena con las features ``X`` y el target ``y``."""
        ...

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """Predice bicis a t+60 para cada fila de ``X``."""
        ...

    def params(self) -> dict[str, Any]:
        """Hiperparámetros para registrar en MLflow."""
        ...


def clip_to_capacity(pred: np.ndarray, X: pd.DataFrame) -> np.ndarray:
    """Recorta a ``[0, capacity]``: una estación no puede tener bicis negativas ni de más.

    Args:
        pred: Predicciones crudas.
        X: Features (usa la columna ``capacity``).

    Returns:
        Predicciones recortadas como float.
    """
    upper = X["capacity"].to_numpy(dtype=float)
    return np.clip(np.asarray(pred, dtype=float), 0.0, upper)


class PersistenceBaseline:
    """Predice que dentro de 60 min habrá lo mismo que ahora (``bikes_available``)."""

    name = "baseline"
    available = True

    def fit(self, X: pd.DataFrame, y: pd.Series) -> None:
        """No aprende nada."""

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """Devuelve ``bikes_available`` recortado a la capacidad."""
        return clip_to_capacity(X["bikes_available"].to_numpy(dtype=float), X)

    def params(self) -> dict[str, Any]:
        """Sin hiperparámetros."""
        return {"estrategia": "persistencia"}


class LastWeekBaseline:
    """Predice el valor de la misma estación 7 días antes del instante objetivo (t+60 − 7 d).

    Necesita ``station_id`` y ``ts_utc`` en ``X`` y una semana de historia. Si la historia vista
    en ``fit`` cubre menos de 7 días, ``available`` queda en False y no se evalúa.
    """

    name = "baseline_semana"

    def __init__(self) -> None:
        self.available = False
        self._history: pd.Series | None = None

    def fit(self, X: pd.DataFrame, y: pd.Series) -> None:
        """Guarda la historia ``(station_id, ts) → bicis`` y decide si hay una semana."""
        ts = pd.to_datetime(X["ts_utc"], utc=True)
        self.available = bool(len(ts)) and (ts.max() - ts.min()) >= LAST_WEEK
        self._history = pd.Series(
            X["bikes_available"].to_numpy(dtype=float),
            index=pd.MultiIndex.from_arrays([X["station_id"].astype(str), ts]),
        )

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """Busca el valor de hace una semana; si falta, usa la persistencia.

        Raises:
            RuntimeError: Si no hay una semana de historia (``available`` es False).
        """
        if not self.available or self._history is None:
            raise RuntimeError("baseline_semana no disponible: menos de 7 días de historia")
        target_ts = pd.to_datetime(X["ts_utc"], utc=True) + pd.Timedelta(minutes=60) - LAST_WEEK
        keys = pd.MultiIndex.from_arrays([X["station_id"].astype(str), target_ts])
        values = self._history.reindex(keys).to_numpy()
        fallback = X["bikes_available"].to_numpy(dtype=float)
        return clip_to_capacity(np.where(np.isnan(values), fallback, values), X)

    def params(self) -> dict[str, Any]:
        """Sin hiperparámetros."""
        return {"estrategia": "misma_hora_semana_anterior"}


class LinearPredictor:
    """Regresión lineal sobre ``FEATURES``.

    La mediana imputa los lags faltantes (al inicio de cada tramo no hay pasado) porque
    ``LinearRegression`` no acepta NaN; luego se estandariza.
    """

    name = "lineal"
    available = True

    def __init__(self) -> None:
        self.model = Pipeline(
            [
                ("imputar", SimpleImputer(strategy="median", keep_empty_features=True)),
                ("escalar", StandardScaler()),
                ("lineal", LinearRegression()),
            ]
        )

    def fit(self, X: pd.DataFrame, y: pd.Series) -> None:
        """Entrena el pipeline."""
        self.model.fit(X[FEATURES], y)

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """Predice y recorta a la capacidad."""
        return clip_to_capacity(self.model.predict(X[FEATURES]), X)

    def params(self) -> dict[str, Any]:
        """Pasos del pipeline."""
        return {"pipeline": "SimpleImputer(median)>StandardScaler>LinearRegression"}


class LightGBMPredictor:
    """Gradient boosting con LightGBM; maneja los NaN de los lags sin imputar.

    Aprende el CAMBIO (``y_t60 − bikes_available``) y no el nivel: el estado actual explica casi
    todo a 60 min, así que "no cambia" es el punto de partida natural y los árboles solo
    corrigen. Con el nivel, los árboles parten cada rango de bicis por separado y no vencían a
    la persistencia (MAE 3.03 vs 3.02); con el cambio, sí (paso 11, ver docs/modelo.md).
    El objetivo L1 optimiza directamente el MAE.

    Args:
        name: Nombre de la configuración (``lightgbm_a``, ``_b`` o ``_c``).
        params: Hiperparámetros de ``LGBMRegressor``.
    """

    available = True

    def __init__(self, name: str, params: dict[str, Any]) -> None:
        self.name = name
        self._params = dict(params)
        self.model = lgb.LGBMRegressor(
            objective="regression_l1", random_state=SEED, n_jobs=-1, verbose=-1, **params
        )

    def fit(self, X: pd.DataFrame, y: pd.Series) -> None:
        """Entrena el regresor sobre el cambio respecto a las bicis actuales."""
        delta = y.to_numpy(dtype=float) - X["bikes_available"].to_numpy(dtype=float)
        self.model.fit(X[FEATURES], delta)

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """Bicis actuales + cambio previsto, recortado a la capacidad."""
        delta = np.asarray(self.model.predict(X[FEATURES]), dtype=float)
        return clip_to_capacity(X["bikes_available"].to_numpy(dtype=float) + delta, X)

    def params(self) -> dict[str, Any]:
        """Hiperparámetros de la configuración, el objetivo L1 y el target (cambio)."""
        return {"objective": "regression_l1", "target": "cambio", **self._params}


def make_predictors() -> list[Predictor]:
    """Todas las estrategias a comparar, en orden de complejidad.

    Returns:
        Persistencia, semana anterior, lineal y las 3 configuraciones de LightGBM.
    """
    lgbms: list[Predictor] = [LightGBMPredictor(n, p) for n, p in LIGHTGBM_CONFIGS.items()]
    return [PersistenceBaseline(), LastWeekBaseline(), LinearPredictor(), *lgbms]


class RadarPyfuncModel(PythonModel):
    """Adaptador de un ``Predictor`` al formato pyfunc de MLflow.

    Args:
        predictor: Estrategia ya entrenada.
    """

    FEATURES: ClassVar[list[str]] = FEATURES

    def __init__(self, predictor: Predictor) -> None:
        self.predictor = predictor

    def predict(self, context: Any, model_input: pd.DataFrame, params: Any = None) -> np.ndarray:
        """Predice con el ``Predictor`` envuelto.

        Args:
            context: Contexto de MLflow (no se usa).
            model_input: Marco con ``FEATURES`` (y ``station_id``/``ts_utc`` si el
                predictor los necesita).
            params: Parámetros de inferencia (no se usan).

        Returns:
            Bicis previstas a t+60, recortadas a ``[0, capacity]``.
        """
        return self.predictor.predict(model_input)
