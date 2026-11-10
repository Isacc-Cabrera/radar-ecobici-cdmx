"""
Autor: Isacc Cabrera
Descripción: Contrato de la API (blueprint §5): modelos Pydantic v2 de cada respuesta. Las rutas
    devuelven estos modelos directamente, sin envoltura; los errores usan ErrorDetail dentro de
    {"detail": {...}} (salvo 422, que conserva el formato de validación de FastAPI).
Patrón: N/A (esquemas de datos)
Fecha de creación: 2026-11-10
Cambios:
    - 2026-11-10 · Isacc Cabrera · Creación (paso 12).
"""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel

PredictionClass = Literal["se_vacia", "estable", "se_llena"]


class HealthOut(BaseModel):
    """Estado del proceso, del modelo y frescura de los datos."""

    status: Literal["ok"]
    modelo_cargado: bool
    fuente_modelo: Literal["mlflow", "archivo", "ninguno"]
    segundos_desde_ultimo_dato: float | None


class StationOut(BaseModel):
    """Estado actual de una estación (última fila silver)."""

    station_id: str
    nombre: str
    lat: float
    lon: float
    capacidad: int
    bicis_disponibles: int
    docks_disponibles: int
    ultimo_reporte_utc: datetime
    anomalia: bool = False


class HistoryPointOut(BaseModel):
    """Un punto de la historia de una estación."""

    ts_utc: datetime
    bicis_disponibles: int
    docks_disponibles: int


class PredictionOut(BaseModel):
    """Predicción t+60 de una estación."""

    station_id: str
    bicis_actuales: int
    prediccion_t60: float
    delta: float  # prediccion_t60 - bicis_actuales
    clase: PredictionClass  # delta <= -3 se_vacia · delta >= 3 se_llena · resto estable
    generado_utc: datetime
    modelo_version: str  # "mlflow:<version>" o "archivo"


class AnomalyOut(BaseModel):
    """Anomalía detectada en una estación (paso 15)."""

    station_id: str
    tipo: Literal["congelada", "zscore", "isolation_forest"]
    puntaje: float
    detalle: str
    detectado_utc: datetime


class ErrorDetail(BaseModel):
    """Cuerpo de ``detail`` en los errores 404 y 503."""

    codigo: str
    mensaje: str
