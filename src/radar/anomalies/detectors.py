"""
Autor: Isacc Cabrera
Descripción: Detectores de anomalías por estación con una interfaz común (AnomalyDetector).
    Todos reciben `recent` (silver de las últimas horas: station_id, snapshot_last_updated,
    last_reported, num_bikes_available, capacity), `history` (gold/features_base: rejilla de
    5 min con bikes_available y capacity) y `now_utc` (el último dato, no el reloj).
    - FrozenStationDetector: last_reported sin avanzar ≥ ANOMALY_FROZEN_MINUTES ("congelada").
    - ZScoreDetector: bicis actuales contra la media y desviación de las medias horarias de la
      estación en esa hora local (lo mismo que gold/station_hour.bikes_mean); |z| ≥
      ANOMALY_ZSCORE ("zscore"). Omite estaciones con < 3 observaciones o desviación 0.
    - IsolationForestDetector: se entrena con la historia (% de bicis, hora local, cambio en 60
      min) y marca los estados actuales que el bosque aísla ("isolation_forest").
    La historia excluye la última hora para no comparar el presente consigo mismo.
    CLI: python -m radar.anomalies.detectors --json (conteos por detector e intersecciones).
Patrón: Strategy (detectores intercambiables con la misma interfaz)
Fecha de creación: 2026-11-10
Cambios:
    - 2026-11-10 · Isacc Cabrera · Creación (paso 15).
"""

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from itertools import combinations
from pathlib import Path
from typing import Literal, Protocol

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from sklearn.ensemble import IsolationForest

from radar.config import Settings, get_settings
from radar.logging import configure_logging, get_logger

log = get_logger(__name__)

TZ_LOCAL = "America/Mexico_City"
Tipo = Literal["congelada", "zscore", "isolation_forest"]
MIN_OBS = 3
CHANGE_WINDOW = pd.Timedelta(minutes=60)
HISTORY_GAP = pd.Timedelta(hours=1)  # la historia termina 1 h antes de "ahora"
RECENT_HOURS = 2


@dataclass(frozen=True)
class Anomaly:
    """Una anomalía detectada (mismos campos que ``AnomalyOut`` de la API).

    Attributes:
        station_id: Estación.
        tipo: Detector que la marcó.
        puntaje: Magnitud (minutos congelada, |z| o rareza del bosque).
        detalle: Explicación en español para el usuario.
        detectado_utc: Instante del dato evaluado.
    """

    station_id: str
    tipo: Tipo
    puntaje: float
    detalle: str
    detectado_utc: datetime


class AnomalyDetector(Protocol):
    """Interfaz común de los detectores."""

    name: Tipo

    def detect(
        self, recent: pd.DataFrame, history: pd.DataFrame, now_utc: datetime
    ) -> list[Anomaly]:
        """Devuelve las anomalías vigentes en ``now_utc``."""
        ...


# --------------------------------------------------------------------------- utilidades
def _utc(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True)


def state_at(recent: pd.DataFrame, when: pd.Timestamp) -> pd.DataFrame:
    """Último estado conocido de cada estación en ``when`` (as-of hacia atrás).

    Args:
        recent: Silver reciente.
        when: Instante.

    Returns:
        Una fila por estación con ``station_id``, ``bikes``, ``capacity`` y ``last_reported``.
    """
    df = recent.assign(snapshot=_utc(recent["snapshot_last_updated"]))
    df = df[df["snapshot"] <= when].sort_values("snapshot", kind="stable")
    last = df.groupby("station_id", sort=True).tail(1)
    return pd.DataFrame(
        {
            "station_id": last["station_id"].astype(str).to_numpy(),
            "bikes": last["num_bikes_available"].astype(float).to_numpy(),
            "capacity": last["capacity"].astype(float).to_numpy(),
            "last_reported": _utc(last["last_reported"]).to_numpy(),
        }
    )


def _history_before(history: pd.DataFrame, now: pd.Timestamp) -> pd.DataFrame:
    h = history.assign(ts=_utc(history["ts_utc"]), station_id=history["station_id"].astype(str))
    return h[h["ts"] < now - HISTORY_GAP]


def _features(
    bikes: pd.Series, capacity: pd.Series, change: pd.Series, ts: pd.Series
) -> pd.DataFrame:
    """Features del bosque: % de bicis, hora local y cambio en 60 min."""
    cap = capacity.where(capacity > 0)
    return pd.DataFrame(
        {
            "pct_bicis": (bikes / cap).fillna(0.0).clip(0.0, 1.0).to_numpy(),
            "hora_local": ts.dt.tz_convert(TZ_LOCAL).dt.hour.to_numpy(),
            "cambio_60": change.fillna(0.0).to_numpy(),
        }
    )


# --------------------------------------------------------------------------- detectores
class FrozenStationDetector:
    """Estación cuyo ``last_reported`` no avanza en ``minutes`` minutos.

    Args:
        minutes: Umbral (``ANOMALY_FROZEN_MINUTES``, 90 por omisión).
    """

    name: Tipo = "congelada"

    def __init__(self, minutes: float = 90.0) -> None:
        self.minutes = minutes

    def detect(
        self, recent: pd.DataFrame, history: pd.DataFrame, now_utc: datetime
    ) -> list[Anomaly]:
        """Marca las estaciones sin reporte nuevo en la ventana."""
        if recent.empty:
            return []
        now = pd.Timestamp(now_utc)
        last = recent.assign(lr=_utc(recent["last_reported"])).groupby("station_id")["lr"].max()
        stale = ((now - last).dt.total_seconds() / 60).round(1)
        out = []
        for sid, minutes in stale[stale >= self.minutes].sort_index().items():
            out.append(
                Anomaly(
                    station_id=str(sid),
                    tipo=self.name,
                    puntaje=float(minutes),
                    detalle=f"sin reporte nuevo desde hace {minutes:.0f} min "
                    f"(umbral {self.minutes:.0f})",
                    detectado_utc=now.to_pydatetime(),
                )
            )
        return out


class ZScoreDetector:
    """Bicis actuales lejos de lo normal para esa estación a esa hora local.

    La referencia son las medias horarias de la estación (una por día en esa hora local,
    igual que ``gold/station_hour.bikes_mean``).

    Args:
        threshold: ``ANOMALY_ZSCORE`` (3 por omisión).
        min_obs: Observaciones históricas mínimas en esa hora (3).
    """

    name: Tipo = "zscore"

    def __init__(self, threshold: float = 3.0, min_obs: int = MIN_OBS) -> None:
        self.threshold = threshold
        self.min_obs = min_obs

    def reference(self, history: pd.DataFrame, now: pd.Timestamp) -> pd.DataFrame:
        """Media, desviación y número de observaciones por estación para la hora local de ``now``.

        Returns:
            Índice ``station_id``; columnas ``media``, ``desv`` y ``n``.
        """
        h = _history_before(history, now)
        if h.empty:
            return pd.DataFrame(columns=["media", "desv", "n"])
        local_hour = now.tz_convert(TZ_LOCAL).hour
        h = h[h["ts"].dt.tz_convert(TZ_LOCAL).dt.hour == local_hour]
        hourly = h.groupby(["station_id", h["ts"].dt.floor("h")])["bikes_available"].mean()
        stats = hourly.groupby(level="station_id").agg(["mean", "std", "count"])
        return stats.rename(columns={"mean": "media", "std": "desv", "count": "n"})

    def detect(
        self, recent: pd.DataFrame, history: pd.DataFrame, now_utc: datetime
    ) -> list[Anomaly]:
        """Marca |z| ≥ umbral; omite estaciones con pocas observaciones o desviación 0."""
        if recent.empty:
            return []
        now = pd.Timestamp(now_utc)
        ref = self.reference(history, now)
        ref = ref[(ref["n"] >= self.min_obs) & (ref["desv"] > 0)]
        current = state_at(recent, now).set_index("station_id")
        joined = current.join(ref, how="inner")
        joined["z"] = (joined["bikes"] - joined["media"]) / joined["desv"]
        flagged = joined[joined["z"].abs() >= self.threshold].sort_index()
        hour = now.tz_convert(TZ_LOCAL).hour
        return [
            Anomaly(
                station_id=str(sid),
                tipo=self.name,
                puntaje=round(float(abs(r["z"])), 2),
                detalle=f"{r['bikes']:.0f} bicis contra {r['media']:.1f} ± {r['desv']:.1f} "
                f"habituales a las {hour} h (z = {r['z']:+.1f}, {int(r['n'])} días)",
                detectado_utc=now.to_pydatetime(),
            )
            for sid, r in flagged.iterrows()
        ]


class IsolationForestDetector:
    """Isolation Forest sobre (% de bicis, hora local, cambio en 60 min).

    Se entrena con la historia de la rejilla y evalúa el estado actual de cada estación; marca
    las que el bosque predice como atípicas (``-1``).

    Args:
        contamination: Fracción esperada de anomalías en el entrenamiento (0.02).
        random_state: Semilla (42).
    """

    name: Tipo = "isolation_forest"

    def __init__(self, contamination: float = 0.02, random_state: int = 42) -> None:
        self.contamination = contamination
        self.random_state = random_state

    @staticmethod
    def training_frame(history: pd.DataFrame, now: pd.Timestamp) -> pd.DataFrame:
        """Features de cada punto de la historia (cambio = bicis − bicis 60 min antes)."""
        h = _history_before(history, now)
        if h.empty:
            return pd.DataFrame(columns=["pct_bicis", "hora_local", "cambio_60"])
        h = h.sort_values(["station_id", "ts"])
        past = h[["station_id", "ts", "bikes_available"]].assign(ts=h["ts"] + CHANGE_WINDOW)
        merged = h.merge(past, on=["station_id", "ts"], how="left", suffixes=("", "_antes"))
        change = merged["bikes_available"] - merged["bikes_available_antes"]
        feats = _features(
            merged["bikes_available"].astype(float),
            merged["capacity"].astype(float),
            change,
            merged["ts"],
        )
        return feats[change.notna().to_numpy()]

    def detect(
        self, recent: pd.DataFrame, history: pd.DataFrame, now_utc: datetime
    ) -> list[Anomaly]:
        """Marca los estados actuales que el bosque aísla."""
        if recent.empty:
            return []
        now = pd.Timestamp(now_utc)
        train = self.training_frame(history, now)
        if len(train) < 100:
            log.info("isolation_forest_sin_historia", filas=len(train))
            return []
        current = state_at(recent, now)
        before = state_at(recent, now - CHANGE_WINDOW).set_index("station_id")["bikes"]
        change = current["bikes"] - current["station_id"].map(before)
        feats = _features(
            current["bikes"],
            current["capacity"],
            change,
            pd.Series([now] * len(current)),
        )
        model = IsolationForest(contamination=self.contamination, random_state=self.random_state)
        model.fit(train.to_numpy())
        labels = model.predict(feats.to_numpy())
        scores = -model.score_samples(feats.to_numpy())  # mayor = más raro
        out = []
        for i in np.flatnonzero(labels == -1):
            f = feats.iloc[i]
            out.append(
                Anomaly(
                    station_id=str(current["station_id"].iloc[i]),
                    tipo=self.name,
                    puntaje=round(float(scores[i]), 3),
                    detalle=f"combinación poco común: {f['pct_bicis']:.0%} de bicis y cambio de "
                    f"{f['cambio_60']:+.0f} en 60 min a las {int(f['hora_local'])} h",
                    detectado_utc=now.to_pydatetime(),
                )
            )
        return sorted(out, key=lambda a: a.station_id)


def make_detectors(settings: Settings) -> list[AnomalyDetector]:
    """Los tres detectores con los umbrales de ``Settings``."""
    return [
        FrozenStationDetector(settings.anomaly_frozen_minutes),
        ZScoreDetector(settings.anomaly_zscore),
        IsolationForestDetector(),
    ]


def run_all(
    detectors: list[AnomalyDetector],
    recent: pd.DataFrame,
    history: pd.DataFrame,
    now_utc: datetime,
) -> list[Anomaly]:
    """Corre todos los detectores y junta sus anomalías (una estación puede salir varias veces)."""
    found: list[Anomaly] = []
    for detector in detectors:
        found.extend(detector.detect(recent, history, now_utc))
    return found


# --------------------------------------------------------------------------- lectura del lake
def load_lake(data_dir: Path, hours: int = RECENT_HOURS) -> tuple[pd.DataFrame, pd.DataFrame]:
    """``(recent, history)``: silver de las últimas ``hours`` particiones y gold/features_base.

    Raises:
        FileNotFoundError: Si no hay silver.
    """
    parts = sorted(
        p
        for p in (data_dir / "silver" / "station_status").glob("dt=*/hour=*/part-0.parquet")
        if p.parent.name[5:].isdigit()
    )
    if not parts:
        raise FileNotFoundError("no hay particiones silver")
    cols = [
        "station_id",
        "snapshot_last_updated",
        "last_reported",
        "num_bikes_available",
        "capacity",
    ]
    recent = pd.concat([pq.read_table(p, columns=cols).to_pandas() for p in parts[-hours:]])
    base = data_dir / "gold" / "features_base"
    history = (
        pd.read_parquet(base, columns=["station_id", "ts_utc", "bikes_available", "capacity"])
        if base.exists()
        else pd.DataFrame(columns=["station_id", "ts_utc", "bikes_available", "capacity"])
    )
    return recent, history


def summarize(anomalies: list[Anomaly]) -> dict[str, object]:
    """Conteos por detector, intersecciones y ejemplos."""
    by_type: dict[str, set[str]] = {t: set() for t in ("congelada", "zscore", "isolation_forest")}
    for a in anomalies:
        by_type[a.tipo].add(a.station_id)
    inter = {f"{a}&{b}": sorted(by_type[a] & by_type[b]) for a, b in combinations(by_type, 2)}
    inter["todos"] = sorted(set.intersection(*by_type.values()))
    return {
        "conteos": {t: len(ids) for t, ids in by_type.items()},
        "estaciones_unicas": len(set().union(*by_type.values())),
        "interseccion": inter,
        "ejemplos": [
            {**asdict(a), "detectado_utc": a.detectado_utc.isoformat()}
            for a in sorted(anomalies, key=lambda a: -a.puntaje)[:15]
        ],
    }


def main(argv: list[str] | None = None) -> int:
    """CLI ``python -m radar.anomalies.detectors [--json]``.

    Returns:
        0 ok; 2 uso; 3 sin datos silver.
    """
    parser = argparse.ArgumentParser(description="Corre los tres detectores sobre el lake.")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    settings = get_settings()
    configure_logging(settings.log_level)
    try:
        recent, history = load_lake(settings.data_dir)
    except FileNotFoundError as exc:
        log.error("anomalias_sin_datos", error=str(exc))
        return 3
    now = _utc(recent["snapshot_last_updated"]).max().to_pydatetime()
    anomalies = run_all(make_detectors(settings), recent, history, now)
    out = {"ahora_utc": now.isoformat(), **summarize(anomalies)}
    log.info("anomalias_detectadas", **out["conteos"])
    if args.json:
        print(json.dumps(out, ensure_ascii=False, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
