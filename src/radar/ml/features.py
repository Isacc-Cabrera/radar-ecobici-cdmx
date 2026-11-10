"""
Autor: Isacc Cabrera
Descripción: Features del modelo t+60 por estación, sin fuga de datos. Parte de la rejilla de
    5 min de gold/features_base y agrega, para cada (station_id, ts_utc):
    - lags por TIEMPO (valor exacto en ts − 15/30/60 min, NaN si ese punto no existe) y
      medias móviles con ventana (t − 30/60 min, t] — solo pasado;
    - calendario en hora local America/Mexico_City (hora, día, fin de semana, festivo MX);
    - clima de la última hora ≤ t (as-of hacia atrás);
    - demanda histórica de viajes (octubre: otro periodo) por estación, día y hora local.
    El target y_t60 es bikes_available en ts + 60 min (12 pasos de la rejilla) de la misma
    estación; se busca por tiempo y no con shift(-12) porque la rejilla tiene huecos (horas
    sin recolector) y 12 filas después podrían ser horas después. Filas sin target se descartan.
    build_inference_frame devuelve la última fila por estación con las mismas columnas.
Patrón: N/A (funciones puras de transformación)
Fecha de creación: 2026-11-10
Cambios:
    - 2026-11-10 · Isacc Cabrera · Creación (paso 10).
"""

from datetime import date, datetime

import holidays
import numpy as np
import pandas as pd

TZ_LOCAL = "America/Mexico_City"
STEP = pd.Timedelta(minutes=5)
HORIZON = pd.Timedelta(minutes=60)
LAGS = {"lag_15": 15, "lag_30": 30, "lag_60": 60}
ROLLS = {"roll_mean_30": "30min", "roll_mean_60": "60min"}
WEATHER_COLS = ["temperature_2m", "precipitation"]
TARGET = "y_t60"

FEATURES: list[str] = [
    "bikes_available",
    "capacity",
    *LAGS,
    *ROLLS,
    "hour_local",
    "dow",
    "is_weekend",
    "is_holiday",
    *WEATHER_COLS,
    "demanda_salidas",
    "demanda_llegadas",
]


def _base(base: pd.DataFrame) -> pd.DataFrame:
    """Columnas mínimas de la rejilla, ``ts_utc`` en UTC (ns) y orden estable."""
    df = base[["station_id", "ts_utc", "bikes_available", "capacity"]].copy()
    df["station_id"] = df["station_id"].astype(str)
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True).astype("datetime64[ns, UTC]")
    df["bikes_available"] = df["bikes_available"].astype("float64")
    df["capacity"] = df["capacity"].astype("float64")
    return df.sort_values(["station_id", "ts_utc"], kind="stable").reset_index(drop=True)


def _value_at(df: pd.DataFrame, offset: pd.Timedelta) -> pd.Series:
    """``bikes_available`` de la misma estación en ``ts_utc + offset`` (NaN si no existe).

    Args:
        df: Rejilla con ``station_id``, ``ts_utc`` y ``bikes_available``.
        offset: Desplazamiento (negativo = pasado, positivo = futuro).

    Returns:
        Serie alineada con ``df``.
    """
    lookup = df[["station_id", "ts_utc", "bikes_available"]].copy()
    lookup["ts_utc"] = lookup["ts_utc"] - offset  # el valor de ts+offset queda "en" ts
    merged = df[["station_id", "ts_utc"]].merge(lookup, on=["station_id", "ts_utc"], how="left")
    return merged["bikes_available"]


def add_lags_and_rolls(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega ``lag_15/30/60`` y ``roll_mean_30/60`` (solo pasado).

    Args:
        df: Rejilla ordenada por ``station_id``, ``ts_utc``.

    Returns:
        Copia con las columnas nuevas.
    """
    out = df.copy()
    for name, minutes in LAGS.items():
        out[name] = _value_at(out, -pd.Timedelta(minutes=minutes)).to_numpy()
    indexed = out.set_index("ts_utc")
    grouped = indexed.groupby("station_id", sort=False)["bikes_available"]
    for name, window in ROLLS.items():
        # closed="right": ventana (t - w, t]; nunca incluye puntos posteriores a t
        rolled = grouped.rolling(window, closed="right").mean()
        out[name] = rolled.to_numpy()  # mismo orden: out ya está ordenado por estación y ts
    return out


def is_holiday(day: date) -> int:
    """1 si ``day`` es festivo oficial en México, 0 si no."""
    return int(day in holidays.country_holidays("MX", years=day.year))


def add_calendar(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega ``hour_local``, ``dow`` (0 = lunes), ``is_weekend`` e ``is_holiday`` (CDMX).

    Args:
        df: Marco con ``ts_utc`` en UTC.

    Returns:
        Copia con las columnas de calendario.
    """
    out = df.copy()
    local = out["ts_utc"].dt.tz_convert(TZ_LOCAL)
    out["hour_local"] = local.dt.hour.astype("int64")
    out["dow"] = local.dt.day_of_week.astype("int64")
    out["is_weekend"] = (out["dow"] >= 5).astype("int64")
    days = local.dt.date
    cache = {d: is_holiday(d) for d in days.unique()}
    out["is_holiday"] = days.map(cache).astype("int64")
    return out


def add_weather(df: pd.DataFrame, weather: pd.DataFrame | None) -> pd.DataFrame:
    """Agrega el clima de la última hora ≤ ``ts_utc`` (as-of hacia atrás).

    Args:
        df: Marco con ``ts_utc``.
        weather: ``silver/weather`` (None o vacío ⇒ NaN).

    Returns:
        Copia con ``temperature_2m`` y ``precipitation``.
    """
    out = df.copy()
    if weather is None or weather.empty:
        for col in WEATHER_COLS:
            out[col] = np.nan
        return out
    w = weather[["ts_utc", *WEATHER_COLS]].copy()
    w["ts_utc"] = pd.to_datetime(w["ts_utc"], utc=True).astype("datetime64[ns, UTC]")
    w = w.sort_values("ts_utc")
    order = out["ts_utc"].argsort(kind="stable")
    joined = pd.merge_asof(
        out.iloc[order][["ts_utc"]], w, on="ts_utc", direction="backward", allow_exact_matches=True
    )
    for col in WEATHER_COLS:
        values = np.empty(len(out))
        values[order] = joined[col].to_numpy()
        out[col] = values
    return out


def demand_by_station(demand: pd.DataFrame, station_info: pd.DataFrame) -> pd.DataFrame:
    """Traduce ``gold/trip_demand`` (por ``short_name_norm``) a ``station_id``.

    Args:
        demand: ``short_name_norm``, ``dow``, ``hour_local``, ``salidas_prom``, ``llegadas_prom``.
        station_info: ``silver/station_information`` (``station_id``, ``short_name_norm``).

    Returns:
        ``station_id``, ``dow``, ``hour_local``, ``salidas_prom``, ``llegadas_prom``.
    """
    ids = station_info[["station_id", "short_name_norm"]].astype({"station_id": str})
    cols = ["short_name_norm", "dow", "hour_local", "salidas_prom", "llegadas_prom"]
    merged = ids.merge(demand[cols], on="short_name_norm", how="inner")
    return merged.drop(columns="short_name_norm")


def add_demand(df: pd.DataFrame, demand: pd.DataFrame | None) -> pd.DataFrame:
    """Agrega ``demanda_salidas`` y ``demanda_llegadas`` por estación, día y hora local.

    Una combinación sin viajes en el mes histórico vale 0.

    Args:
        df: Marco con ``station_id``, ``dow`` y ``hour_local``.
        demand: Salida de ``demand_by_station`` (None ⇒ 0).

    Returns:
        Copia con las dos columnas de demanda.
    """
    out = df.copy()
    if demand is None or demand.empty:
        out["demanda_salidas"] = 0.0
        out["demanda_llegadas"] = 0.0
        return out
    d = demand.rename(
        columns={"salidas_prom": "demanda_salidas", "llegadas_prom": "demanda_llegadas"}
    ).astype({"station_id": str, "dow": "int64", "hour_local": "int64"})
    keys = ["station_id", "dow", "hour_local"]
    merged = out[keys].merge(d[[*keys, "demanda_salidas", "demanda_llegadas"]], on=keys, how="left")
    out["demanda_salidas"] = merged["demanda_salidas"].fillna(0.0).to_numpy()
    out["demanda_llegadas"] = merged["demanda_llegadas"].fillna(0.0).to_numpy()
    return out


def _features(
    base: pd.DataFrame, weather: pd.DataFrame | None, demand: pd.DataFrame | None
) -> pd.DataFrame:
    df = add_lags_and_rolls(_base(base))
    df = add_calendar(df)
    df = add_weather(df, weather)
    return add_demand(df, demand)


def build_training_frame(
    base: pd.DataFrame, weather: pd.DataFrame | None, demand: pd.DataFrame | None
) -> pd.DataFrame:
    """Marco de entrenamiento: ``station_id``, ``ts_utc``, ``FEATURES`` y ``y_t60``.

    Args:
        base: ``gold/features_base`` (rejilla de 5 min).
        weather: ``silver/weather`` o None.
        demand: Salida de ``demand_by_station`` o None.

    Returns:
        Una fila por punto de la rejilla que tiene target (``ts + 60 min`` existe).
    """
    df = _features(base, weather, demand)
    df[TARGET] = _value_at(df, HORIZON).to_numpy()
    df = df.dropna(subset=[TARGET])
    return df[["station_id", "ts_utc", *FEATURES, TARGET]].reset_index(drop=True)


def build_inference_frame(
    base_recent: pd.DataFrame,
    weather: pd.DataFrame | None,
    demand: pd.DataFrame | None,
    now_utc: datetime,
) -> pd.DataFrame:
    """Marco de inferencia: la última fila (``ts_utc ≤ now_utc``) de cada estación.

    Args:
        base_recent: Rejilla reciente (al menos la última hora, para lags y ventanas).
        weather: ``silver/weather`` o None.
        demand: Salida de ``demand_by_station`` o None.
        now_utc: Instante de la predicción; nada posterior se usa.

    Returns:
        ``station_id``, ``ts_utc`` y ``FEATURES`` (sin target), una fila por estación.
    """
    now = pd.Timestamp(now_utc).tz_convert("UTC")
    recent = base_recent[pd.to_datetime(base_recent["ts_utc"], utc=True) <= now]
    if recent.empty:
        return pd.DataFrame(columns=["station_id", "ts_utc", *FEATURES])
    df = _features(recent, weather, demand)
    last = df.groupby("station_id", sort=True).tail(1)
    return last[["station_id", "ts_utc", *FEATURES]].reset_index(drop=True)
