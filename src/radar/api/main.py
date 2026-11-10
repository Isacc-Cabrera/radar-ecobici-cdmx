"""
Autor: Isacc Cabrera
Descripción: API FastAPI de Radar ECOBICI (blueprint §5). El lifespan configura el logging,
    crea el repositorio y carga el modelo UNA sola vez (ModelHolder en app.state); cada ruta
    recibe sus dependencias con Depends. Rutas: /health, /stations,
    /stations/{station_id}/history, /predict/{station_id}, /predictions, /anomalies y /docs.
    Errores: {"detail": {"codigo", "mensaje"}} con 404 estacion_no_encontrada, 503
    modelo_no_disponible y 503 datos_no_disponibles; 422 lo genera FastAPI. CORS solo GET desde
    API_CORS_ORIGINS (el mapa en http://localhost:5173). create_app recibe fábricas inyectables
    para que las pruebas usen un repositorio y un modelo falsos.
    Ejecutar: uv run uvicorn radar.api.main:app --port 8000
Patrón: Dependency Injection (Depends + fábricas) + Singleton (modelo en el lifespan)
Fecha de creación: 2026-11-10
Cambios:
    - 2026-11-10 · Isacc Cabrera · Creación (paso 12).
"""

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Annotated, Any

import pandas as pd
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from radar.api.deps import ModelHolder, get_model_holder, get_repository
from radar.api.repository import DataUnavailableError, StationRepository
from radar.api.schemas import (
    AnomalyOut,
    ErrorDetail,
    HealthOut,
    HistoryPointOut,
    PredictionClass,
    PredictionOut,
    StationOut,
)
from radar.config import Settings, get_settings
from radar.logging import configure_logging, get_logger

log = get_logger(__name__)

CLASS_THRESHOLD = 3.0
ERRORS: dict[int | str, dict[str, Any]] = {
    404: {"model": ErrorDetail, "description": "estacion_no_encontrada"},
    503: {"model": ErrorDetail, "description": "modelo_no_disponible o datos_no_disponibles"},
}

Repo = Annotated[StationRepository, Depends(get_repository)]
Holder = Annotated[ModelHolder, Depends(get_model_holder)]


def error(status: int, codigo: str, mensaje: str) -> HTTPException:
    """Error con el formato del contrato: ``{"detail": {"codigo", "mensaje"}}``."""
    return HTTPException(status_code=status, detail={"codigo": codigo, "mensaje": mensaje})


def classify(delta: float) -> PredictionClass:
    """``se_vacia`` si pierde ≥ 3 bicis, ``se_llena`` si gana ≥ 3, si no ``estable``."""
    if delta <= -CLASS_THRESHOLD:
        return "se_vacia"
    if delta >= CLASS_THRESHOLD:
        return "se_llena"
    return "estable"


def build_predictions(frame: pd.DataFrame, holder: ModelHolder) -> list[PredictionOut]:
    """Convierte el marco de inferencia en predicciones del contrato.

    Args:
        frame: Salida de ``StationRepository.inference_frame``.
        holder: Modelo cargado.

    Returns:
        Una ``PredictionOut`` por fila.
    """
    preds = holder.predict(frame)
    generated = datetime.now(UTC)
    out = []
    for row, pred in zip(frame.itertuples(index=False), preds, strict=True):
        current = int(row.bikes_available)
        value = round(float(min(max(pred, 0.0), float(row.capacity))), 2)
        delta = round(value - current, 2)
        out.append(
            PredictionOut(
                station_id=str(row.station_id),
                bicis_actuales=current,
                prediccion_t60=value,
                delta=delta,
                clase=classify(delta),
                generado_utc=generated,
                modelo_version=holder.version,
            )
        )
    return out


def create_app(
    settings_factory: Callable[[], Settings] = get_settings,
    repository_factory: Callable[[Settings], StationRepository] | None = None,
    holder_factory: Callable[[], ModelHolder] = ModelHolder,
) -> FastAPI:
    """Fabrica la aplicación con sus dependencias.

    Args:
        settings_factory: Devuelve los Settings.
        repository_factory: Crea el repositorio a partir de Settings (por omisión, sobre
            ``settings.data_dir``).
        holder_factory: Crea el ``ModelHolder`` (las pruebas pasan uno falso).

    Returns:
        La app FastAPI lista para uvicorn o ``TestClient``.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        settings = settings_factory()
        configure_logging(settings.log_level)
        make_repo = repository_factory or (lambda s: StationRepository(s.data_dir))
        app.state.settings = settings
        app.state.repository = make_repo(settings)
        app.state.model_holder = holder_factory().load(settings)  # UNA vez por proceso
        log.info("api_lista", fuente_modelo=app.state.model_holder.fuente)
        yield

    settings = settings_factory()
    app = FastAPI(
        title="Radar ECOBICI CDMX",
        description="Estado de las estaciones de ECOBICI y predicción de bicis a 60 minutos.",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[o.strip() for o in settings.api_cors_origins.split(",") if o.strip()],
        allow_methods=["GET"],
        allow_headers=["*"],
    )

    @app.get("/health", response_model=HealthOut, tags=["estado"])
    def health(repo: Repo, holder: Holder) -> HealthOut:
        """Estado de la API, del modelo y antigüedad del último dato (siempre 200)."""
        return HealthOut(
            status="ok",
            modelo_cargado=holder.cargado,
            fuente_modelo=holder.fuente,
            segundos_desde_ultimo_dato=repo.last_data_age_seconds(),
        )

    @app.get("/stations", response_model=list[StationOut], responses=ERRORS, tags=["estaciones"])
    def stations(repo: Repo) -> list[StationOut]:
        """Estado actual de cada estación."""
        try:
            return repo.stations()
        except DataUnavailableError as exc:
            raise error(503, "datos_no_disponibles", str(exc)) from exc

    @app.get(
        "/stations/{station_id}/history",
        response_model=list[HistoryPointOut],
        responses=ERRORS,
        tags=["estaciones"],
    )
    def history(
        station_id: str, repo: Repo, hours: Annotated[int, Query(ge=1, le=24)] = 6
    ) -> list[HistoryPointOut]:
        """Bicis y muelles de la estación en las últimas ``hours`` horas (1–24)."""
        try:
            if not repo.exists(station_id):
                raise error(404, "estacion_no_encontrada", f"no existe la estación {station_id}")
            return repo.history(station_id, hours)
        except DataUnavailableError as exc:
            raise error(503, "datos_no_disponibles", str(exc)) from exc

    @app.get(
        "/predict/{station_id}", response_model=PredictionOut, responses=ERRORS, tags=["modelo"]
    )
    def predict(station_id: str, repo: Repo, holder: Holder) -> PredictionOut:
        """Predicción t+60 de una estación."""
        try:
            if not repo.exists(station_id):
                raise error(404, "estacion_no_encontrada", f"no existe la estación {station_id}")
            if not holder.cargado:
                raise error(503, "modelo_no_disponible", "no hay modelo cargado")
            frame = repo.inference_frame()
        except DataUnavailableError as exc:
            raise error(503, "datos_no_disponibles", str(exc)) from exc
        row = frame[frame["station_id"] == station_id] if not frame.empty else frame
        if row.empty:
            raise error(503, "datos_no_disponibles", f"sin datos recientes de {station_id}")
        return build_predictions(row, holder)[0]

    @app.get("/predictions", response_model=list[PredictionOut], responses=ERRORS, tags=["modelo"])
    def predictions(repo: Repo, holder: Holder) -> list[PredictionOut]:
        """Predicción t+60 de todas las estaciones con datos recientes (colorea el mapa)."""
        if not holder.cargado:
            raise error(503, "modelo_no_disponible", "no hay modelo cargado")
        try:
            frame = repo.inference_frame()
        except DataUnavailableError as exc:
            raise error(503, "datos_no_disponibles", str(exc)) from exc
        return build_predictions(frame, holder) if not frame.empty else []

    @app.get("/anomalies", response_model=list[AnomalyOut], tags=["estaciones"])
    def anomalies(repo: Repo) -> list[AnomalyOut]:
        """Anomalías actuales (vacía hasta el paso 15)."""
        return repo.anomalies()

    return app


app = create_app()
