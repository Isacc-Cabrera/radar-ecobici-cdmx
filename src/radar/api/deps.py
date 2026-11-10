"""
Autor: Isacc Cabrera
Descripción: Dependencias de la API. ModelHolder carga el modelo UNA vez en el lifespan:
    primero MLflow (models:/<MODEL_NAME>@champion, con una sonda a /health de 2 s para no
    quedarse esperando los reintentos internos de MLflow si el servidor está caído), luego el
    respaldo joblib (MODEL_FALLBACK_PATH) y, si ninguno existe, sigue sin modelo
    (fuente="ninguno": /predict responde 503). Los proveedores get_* leen app.state y FastAPI
    los inyecta en cada ruta con Depends; las pruebas los reemplazan con dependency_overrides.
Patrón: Dependency Injection (Depends) + Singleton (ModelHolder creado una vez en el lifespan)
Fecha de creación: 2026-11-10
Cambios:
    - 2026-11-10 · Isacc Cabrera · Creación (paso 12).
"""

from typing import Any, Literal

import httpx
import joblib
import numpy as np
import pandas as pd
from fastapi import Request

from radar.api.repository import StationRepository
from radar.config import Settings
from radar.logging import get_logger
from radar.ml.features import FEATURES

log = get_logger(__name__)

Fuente = Literal["mlflow", "archivo", "ninguno"]
PROBE_TIMEOUT_S = 2.0


class ModelHolder:
    """Guarda el modelo cargado y de dónde salió.

    Attributes:
        model: Modelo pyfunc de MLflow o ``Predictor`` de joblib (None sin modelo).
        fuente: ``mlflow``, ``archivo`` o ``ninguno``.
        version: ``mlflow:<versión>``, ``archivo`` o ``ninguno``.
        loads: Cuántas veces se llamó ``load`` (debe ser 1 por proceso).
    """

    def __init__(self) -> None:
        self.model: Any = None
        self.fuente: Fuente = "ninguno"
        self.version = "ninguno"
        self.loads = 0

    @property
    def cargado(self) -> bool:
        """True si hay un modelo listo para predecir."""
        return self.model is not None

    def load(self, settings: Settings) -> "ModelHolder":
        """Carga el campeón de MLflow o el respaldo local.

        Args:
            settings: Configuración (``mlflow_tracking_uri``, ``model_name``,
                ``model_fallback_path``).

        Returns:
            El mismo holder, ya cargado (o sin modelo).
        """
        self.loads += 1
        if self._load_mlflow(settings) or self._load_file(settings):
            log.info("modelo_cargado", fuente=self.fuente, version=self.version)
        else:
            log.warning("modelo_no_disponible", uri=settings.mlflow_tracking_uri)
        return self

    def _load_mlflow(self, settings: Settings) -> bool:
        uri = settings.mlflow_tracking_uri.rstrip("/")
        try:
            httpx.get(f"{uri}/health", timeout=PROBE_TIMEOUT_S).raise_for_status()
            import mlflow  # import diferido: cargar MLflow solo si el servidor responde

            mlflow.set_tracking_uri(uri)  # MLflow no lee .env
            name = settings.model_name
            version = mlflow.MlflowClient().get_model_version_by_alias(name, "champion")
            self.model = mlflow.pyfunc.load_model(f"models:/{name}@champion")
        except Exception as exc:  # servidor caído, sin registro o artefacto ilegible
            log.warning("mlflow_no_disponible", error=str(exc)[:200])
            return False
        self.fuente = "mlflow"
        self.version = f"mlflow:{version.version}"
        return True

    def _load_file(self, settings: Settings) -> bool:
        path = settings.model_fallback_path
        if not path.exists():
            return False
        try:
            self.model = joblib.load(path)
        except Exception as exc:
            log.warning("respaldo_ilegible", archivo=str(path), error=str(exc)[:200])
            return False
        self.fuente = "archivo"
        self.version = "archivo"
        return True

    def predict(self, frame: pd.DataFrame) -> np.ndarray:
        """Predice bicis a t+60 para cada fila del marco de inferencia.

        Args:
            frame: Salida de ``build_inference_frame`` (``FEATURES`` + ``station_id``).

        Returns:
            Predicciones como arreglo float.

        Raises:
            RuntimeError: Si no hay modelo cargado.
        """
        if self.model is None:
            raise RuntimeError("sin modelo cargado")
        X = (
            frame[["station_id", "ts_utc", *FEATURES]]
            if self.fuente == "archivo"
            else frame[FEATURES]
        )
        return np.asarray(self.model.predict(X), dtype=float)


def get_settings_dep(request: Request) -> Settings:
    """Settings del proceso (guardados en ``app.state`` por el lifespan)."""
    settings: Settings = request.app.state.settings
    return settings


def get_repository(request: Request) -> StationRepository:
    """Repositorio único del proceso."""
    repo: StationRepository = request.app.state.repository
    return repo


def get_model_holder(request: Request) -> ModelHolder:
    """Holder del modelo cargado en el lifespan."""
    holder: ModelHolder = request.app.state.model_holder
    return holder
