"""
Autor: Isacc Cabrera
Descripción: Pruebas de integración de la API (paso 12) con TestClient, sin red: la app completa
    (rutas + validación + inyección de dependencias) con un repositorio falso de 3 estaciones y
    un ModelHolder falso que cuenta sus cargas (debe cargarse UNA vez, en el lifespan). Cubre
    /health, /stations, historia con 422 fuera de rango, 404 estacion_no_encontrada, 503 sin
    modelo, clases de predicción, CORS para el mapa y /docs. También prueba el
    StationRepository real sobre un silver mínimo en tmp_path y el ModelHolder real (sin
    MLflow: usa el respaldo joblib o queda sin modelo).
Patrón: N/A (pruebas)
Fecha de creación: 2026-11-10
Cambios:
    - 2026-11-10 · Isacc Cabrera · Creación (paso 12).
"""

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import joblib
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import respx
from fastapi.testclient import TestClient

from radar.api.deps import ModelHolder, get_repository
from radar.api.main import classify, create_app
from radar.api.repository import DataUnavailableError, StationRepository
from radar.api.schemas import HistoryPointOut, StationOut
from radar.config import Settings
from radar.lake.bronze_to_silver import SILVER_STATUS_SCHEMA
from radar.ml.features import FEATURES
from radar.ml.models import PersistenceBaseline

NOW = datetime(2026, 11, 10, 14, 0, tzinfo=UTC)
CORS_ORIGIN = "http://localhost:5173"


# --- dobles de prueba ---------------------------------------------------------------------------
class FakeRepository:
    """Tres estaciones; la "3" no tiene datos recientes para inferencia."""

    def __init__(self) -> None:
        self.ids = ["1", "2", "3"]

    def stations(self) -> list[StationOut]:
        return [
            StationOut(
                station_id=sid,
                nombre=f"CE-{sid}",
                lat=19.4,
                lon=-99.1,
                capacidad=20,
                bicis_disponibles=b,
                docks_disponibles=20 - b,
                ultimo_reporte_utc=NOW,
            )
            for sid, b in zip(self.ids, (10, 2, 7), strict=True)
        ]

    def exists(self, station_id: str) -> bool:
        return station_id in self.ids

    def history(self, station_id: str, hours: int) -> list[HistoryPointOut]:
        return [
            HistoryPointOut(
                ts_utc=NOW - timedelta(minutes=m), bicis_disponibles=5, docks_disponibles=15
            )
            for m in (30, 0)
        ]

    def inference_frame(self) -> pd.DataFrame:
        rows = []
        for sid, bikes in (("1", 10.0), ("2", 2.0)):
            row: dict[str, Any] = dict.fromkeys(FEATURES, 0.0)
            row.update(station_id=sid, ts_utc=NOW, bikes_available=bikes, capacity=20.0)
            rows.append(row)
        return pd.DataFrame(rows)

    def last_data_age_seconds(self) -> float | None:
        return 42.0

    def anomalies(self) -> list[Any]:
        return []


class FakeHolder(ModelHolder):
    """Predice bicis + delta fijo por estación y cuenta cuántas veces se cargó."""

    loads_total = 0

    def __init__(self, with_model: bool = True) -> None:
        super().__init__()
        self._with_model = with_model

    def load(self, settings: Settings) -> "FakeHolder":
        FakeHolder.loads_total += 1
        self.loads += 1
        if self._with_model:
            self.model = object()
            self.fuente, self.version = "mlflow", "mlflow:7"
        return self

    def predict(self, frame: pd.DataFrame) -> np.ndarray:
        delta = frame["station_id"].map({"1": -5.0, "2": 25.0}).to_numpy()
        return frame["bikes_available"].to_numpy() + delta


@pytest.fixture
def make_client(settings: Settings) -> Iterator[Any]:
    clients: list[TestClient] = []

    def factory(with_model: bool = True) -> TestClient:
        FakeHolder.loads_total = 0
        app = create_app(
            settings_factory=lambda: settings,
            repository_factory=lambda s: FakeRepository(),  # type: ignore[arg-type,return-value]
            holder_factory=lambda: FakeHolder(with_model),
        )
        client = TestClient(app)
        client.__enter__()  # corre el lifespan
        clients.append(client)
        return client

    yield factory
    for c in clients:
        c.__exit__(None, None, None)


# --- rutas ---------------------------------------------------------------------------------------
def test_health_trae_los_campos_del_contrato(make_client: Any) -> None:
    body = make_client().get("/health").json()
    assert body == {
        "status": "ok",
        "modelo_cargado": True,
        "fuente_modelo": "mlflow",
        "segundos_desde_ultimo_dato": 42.0,
    }


def test_stations_devuelve_todas_sin_anomalias(make_client: Any) -> None:
    r = make_client().get("/stations")
    assert r.status_code == 200
    assert [s["station_id"] for s in r.json()] == ["1", "2", "3"]
    assert all(s["anomalia"] is False for s in r.json())


def test_historia_valida_horas_y_estacion(make_client: Any) -> None:
    client = make_client()
    assert client.get("/stations/1/history?hours=0").status_code == 422
    assert client.get("/stations/1/history?hours=25").status_code == 422
    ok = client.get("/stations/1/history")  # default 6
    assert ok.status_code == 200 and len(ok.json()) == 2
    missing = client.get("/stations/999/history?hours=2")
    assert missing.status_code == 404
    assert missing.json()["detail"]["codigo"] == "estacion_no_encontrada"


def test_predict_de_estacion_desconocida_da_404(make_client: Any) -> None:
    r = make_client().get("/predict/no-existe")
    assert r.status_code == 404
    assert r.json() == {
        "detail": {"codigo": "estacion_no_encontrada", "mensaje": "no existe la estación no-existe"}
    }


def test_predicciones_con_clase_recorte_y_version(make_client: Any) -> None:
    client = make_client()
    one = client.get("/predict/1").json()
    assert (one["bicis_actuales"], one["prediccion_t60"], one["delta"]) == (10, 5.0, -5.0)
    assert one["clase"] == "se_vacia" and one["modelo_version"] == "mlflow:7"
    full = {p["station_id"]: p for p in client.get("/predictions").json()}
    assert set(full) == {"1", "2"}  # la "3" no tiene datos recientes
    assert full["2"]["prediccion_t60"] == 20.0  # 2 + 25 recortado a la capacidad
    assert full["2"]["clase"] == "se_llena"
    sin_datos = client.get("/predict/3")
    assert sin_datos.status_code == 503
    assert sin_datos.json()["detail"]["codigo"] == "datos_no_disponibles"


def test_el_modelo_se_carga_una_sola_vez(make_client: Any) -> None:
    client = make_client()
    for _ in range(5):
        assert client.get("/predict/1").status_code == 200
    client.get("/predictions")
    assert client.app.state.model_holder.loads == 1
    assert FakeHolder.loads_total == 1


def test_sin_modelo_las_predicciones_dan_503(make_client: Any) -> None:
    client = make_client(with_model=False)
    assert client.get("/health").json()["modelo_cargado"] is False
    for path in ("/predict/1", "/predictions"):
        r = client.get(path)
        assert r.status_code == 503
        assert r.json()["detail"]["codigo"] == "modelo_no_disponible"
    assert client.get("/stations").status_code == 200  # el resto de la API sigue viva


def test_cors_para_el_mapa_y_docs(make_client: Any) -> None:
    client = make_client()
    pre = client.options(
        "/stations",
        headers={"Origin": CORS_ORIGIN, "Access-Control-Request-Method": "GET"},
    )
    assert pre.status_code == 200
    assert pre.headers["access-control-allow-origin"] == CORS_ORIGIN
    otro = client.options(
        "/stations",
        headers={"Origin": "http://evil.test", "Access-Control-Request-Method": "GET"},
    )
    assert "access-control-allow-origin" not in otro.headers
    assert client.get("/docs").status_code == 200
    assert client.get("/anomalies").json() == []


def test_dependency_override_reemplaza_el_repositorio(make_client: Any) -> None:
    client = make_client()

    class Vacio(FakeRepository):
        def stations(self) -> list[StationOut]:
            raise DataUnavailableError("no hay particiones silver")

    client.app.dependency_overrides[get_repository] = lambda: Vacio()
    r = client.get("/stations")
    assert r.status_code == 503
    assert r.json()["detail"]["codigo"] == "datos_no_disponibles"


@pytest.mark.parametrize(
    ("delta", "clase"),
    [
        (-3.0, "se_vacia"),
        (-2.99, "estable"),
        (0.0, "estable"),
        (2.99, "estable"),
        (3.0, "se_llena"),
    ],
)
def test_umbral_de_clase(delta: float, clase: str) -> None:
    assert classify(delta) == clase


# --- repositorio real sobre un silver mínimo ------------------------------------------------------
def _silver(settings: Settings) -> None:
    rows = []
    for hour in (12, 13):
        start = NOW.replace(hour=hour)
        for m in range(0, 60, 10):
            ts = start + timedelta(minutes=m, seconds=7)
            for sid, cap in (("1", 20), ("2", 10)):
                bikes = (m // 10 + hour) % (cap // 2)
                rows.append(
                    {
                        "station_id": sid,
                        "snapshot_last_updated": ts,
                        "last_reported": ts,
                        "num_bikes_available": bikes,
                        "num_bikes_disabled": 0,
                        "num_docks_available": cap - bikes,
                        "num_docks_disabled": 0,
                        "capacity": cap,
                        "is_installed": True,
                        "is_renting": True,
                        "is_returning": True,
                        "is_charging": False,
                    }
                )
    df = pd.DataFrame(rows)
    for hour in (12, 13):
        part = settings.data_dir / f"silver/station_status/dt=2026-11-10/hour={hour}"
        part.mkdir(parents=True)
        sub = df[pd.to_datetime(df["snapshot_last_updated"]).dt.hour == hour]
        pq.write_table(
            pa.Table.from_pandas(sub, schema=SILVER_STATUS_SCHEMA, preserve_index=False),
            part / "part-0.parquet",
        )
    info = pd.DataFrame(
        {
            "station_id": ["1", "2"],
            "short_name": ["001", "002"],
            "short_name_norm": ["1", "2"],
            "name": ["Uno", "Dos"],
            "lat": [19.4, 19.5],
            "lon": [-99.1, -99.2],
            "capacity": [20, 10],
            "info_hash": ["x", "x"],
        }
    )
    path = settings.data_dir / "silver/station_information/station_information.parquet"
    path.parent.mkdir(parents=True)
    info.to_parquet(path)


def test_repositorio_real_estado_historia_e_inferencia(settings: Settings) -> None:
    _silver(settings)
    repo = StationRepository(settings.data_dir)
    stations = {s.station_id: s for s in repo.stations()}
    last = NOW.replace(hour=13) + timedelta(minutes=50, seconds=7)
    assert stations["1"].ultimo_reporte_utc == last
    assert repo.exists("2") and not repo.exists("9")
    hist = repo.history("1", hours=1)
    assert [p.ts_utc for p in hist] == sorted(p.ts_utc for p in hist)
    assert hist[0].ts_utc >= last - timedelta(hours=1)
    frame = repo.inference_frame()
    assert list(frame["station_id"]) == ["1", "2"]
    assert (pd.to_datetime(frame["ts_utc"], utc=True) == pd.Timestamp(last)).all()
    # la rejilla anclada en el último dato: bicis actuales = /stations
    for row in frame.itertuples():
        assert row.bikes_available == stations[row.station_id].bicis_disponibles
    assert frame["lag_60"].notna().all()  # 2 h de silver cubren el lag de 60 min
    age = repo.last_data_age_seconds(now=last + timedelta(seconds=30))
    assert age == 30.0


def test_repositorio_sin_silver_reporta_datos_no_disponibles(settings: Settings) -> None:
    repo = StationRepository(settings.data_dir)
    with pytest.raises(DataUnavailableError):
        repo.stations()
    assert repo.last_data_age_seconds() is None


def test_repositorio_cachea_30_segundos(settings: Settings) -> None:
    _silver(settings)
    clock = [0.0]
    repo = StationRepository(settings.data_dir, clock=lambda: clock[0])
    first = repo.inference_frame()
    (settings.data_dir / "silver/station_status/dt=2026-11-10/hour=13/part-0.parquet").unlink()
    assert repo.inference_frame() is first  # sigue en caché
    clock[0] = 31.0
    assert repo.inference_frame() is not first  # expiró: vuelve a leer


# --- ModelHolder real sin MLflow ------------------------------------------------------------------
def test_holder_usa_el_respaldo_si_mlflow_no_responde(settings: Settings, tmp_path: Path) -> None:
    fallback = tmp_path / "champion.joblib"
    joblib.dump(PersistenceBaseline(), fallback)
    s = settings.model_copy(
        update={"mlflow_tracking_uri": "http://mlflow.test", "model_fallback_path": fallback}
    )
    with respx.mock(assert_all_mocked=True) as mock:
        mock.get("http://mlflow.test/health").mock(side_effect=httpx.ConnectError("caído"))
        holder = ModelHolder().load(s)
    assert (holder.cargado, holder.fuente, holder.version, holder.loads) == (
        True,
        "archivo",
        "archivo",
        1,
    )
    frame = FakeRepository().inference_frame()
    np.testing.assert_array_equal(holder.predict(frame), [10.0, 2.0])


def test_holder_sin_mlflow_ni_archivo_queda_sin_modelo(settings: Settings, tmp_path: Path) -> None:
    s = settings.model_copy(
        update={
            "mlflow_tracking_uri": "http://mlflow.test",
            "model_fallback_path": tmp_path / "no.joblib",
        }
    )
    with respx.mock(assert_all_mocked=True) as mock:
        mock.get("http://mlflow.test/health").mock(return_value=httpx.Response(503))
        holder = ModelHolder().load(s)
    assert (holder.cargado, holder.fuente) == (False, "ninguno")
    with pytest.raises(RuntimeError):
        holder.predict(FakeRepository().inference_frame())
