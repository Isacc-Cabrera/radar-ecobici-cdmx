"""
Autor: Isacc Cabrera
Descripción: Configuración única del proyecto. Lee variables de entorno y del archivo .env con
    pydantic-settings, las valida por tipo y las expone como un objeto Settings. Es la única
    pieza del código que conoce os.environ; el resto recibe Settings por parámetro.
Patrón: Singleton (get_settings cacheado) + Dependency Injection (Settings se inyecta)
Fecha de creación: 2026-11-09
Cambios:
    - 2026-11-09 · Isacc Cabrera · Creación con todas las variables de §10 del blueprint.
"""

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Variables de configuración de Radar ECOBICI con valores por omisión seguros.

    Ningún secreto es obligatorio aquí: cada módulo que necesita uno lo valida al usarlo, así
    el recolector arranca aunque todavía no existan las llaves de S3 o de AWS.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",  # .env también trae variables de Compose y AWS que no son de Python
        env_ignore_empty=True,  # "S3_APP_SECRET_KEY=" vacío cuenta como no definido
    )

    # --- Recolector (pasos 2-3) ---
    gbfs_url: str = "https://gbfs.mex.lyftbikes.com/gbfs/gbfs.json"
    gbfs_lang: str = "es"
    http_timeout_seconds: float = 10.0
    retry_base_seconds: float = 2.0
    retry_cap_seconds: float = 300.0
    log_level: str = "INFO"
    status_interval_seconds: float = 60.0
    info_refresh_hours: float = 24.0
    stale_alert_seconds: float = 600.0
    data_dir: Path = Path("data")
    sink: str = "local"
    run_once: bool = False

    # --- Kafka (pasos 4-5) ---
    kafka_bootstrap: str = "localhost:9094"
    kafka_topic_status: str = "station_status"
    kafka_topic_raw: str = "station_status_raw"
    kafka_group_id: str = "bronze-writer"

    # --- Data lake S3 local (paso 6) ---
    s3_endpoint_url: str = "http://localhost:8333"
    s3_region: str = "us-east-1"
    s3_admin_access_key: str | None = None
    s3_admin_secret_key: str | None = None
    s3_app_access_key: str | None = None
    s3_app_secret_key: str | None = None
    lake_bucket_bronze: str = "radar-bronze"
    lake_bucket_silver: str = "radar-silver"
    lake_bucket_gold: str = "radar-gold"
    lake_sync_s3: bool = False

    # --- Viajes históricos (paso 8) ---
    trips_csv_url: str | None = None

    # --- Orquestación y calidad (paso 9) ---
    weather_lat: float = 19.4326
    weather_lon: float = -99.1332
    quality_max_gap_minutes: float = 10.0
    quality_max_capacity_fail_pct: float = 5.0

    # --- Modelo (paso 11) ---
    mlflow_tracking_uri: str = "http://localhost:5000"
    model_name: str = "radar-bikes-t60"
    model_fallback_path: Path = Path("models/champion.joblib")

    # --- API (paso 12) ---
    api_cors_origins: str = "http://localhost:5173"

    # --- Anomalías (paso 15) ---
    anomaly_frozen_minutes: float = 90.0
    anomaly_zscore: float = 3.0

    # --- AWS (paso 16) ---
    aws_profile: str | None = None
    aws_s3_bucket: str | None = None

    # --- RAG local (paso 17) ---
    ollama_url: str = "http://localhost:11434"
    ollama_chat_model: str = "llama3.2:3b"
    ollama_embed_model: str = "nomic-embed-text"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Devuelve la instancia única de Settings del proceso.

    Returns:
        Settings leídos del entorno y de ``.env`` la primera vez; la misma instancia después.
    """
    return Settings()
