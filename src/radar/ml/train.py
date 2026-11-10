"""
Autor: Isacc Cabrera
Descripción: Entrenamiento y comparación de modelos t+60 con MLflow. Lee gold/features_base,
    deja fuera las horas con cobertura incompleta del recolector (hueco de heartbeat mayor que
    QUALITY_MAX_GAP_MINUTES: la rejilla repetiría un valor viejo toda la hora), arma el marco con
    radar.ml.features, evalúa cada estrategia de radar.ml.models con folds temporales purgados,
    registra un run por estrategia en el experimento "radar-ecobici" (params, MAE/RMSE/R² por
    fold y promedio), elige el menor MAE medio, lo reentrena con todos los datos, lo registra
    como MODEL_NAME con alias "champion", guarda el respaldo joblib en MODEL_FALLBACK_PATH y
    escribe docs/reportes/comparacion_modelos.md y real_vs_pred.html.
    CLI: python -m radar.ml.train [--json] [--folds N] [--sin-mlflow]. La salida --json es lo
    único que va a stdout (los avisos de MLflow se desvían a stderr).
Patrón: Strategy (recorre los Predictor sin conocer su implementación)
Fecha de creación: 2026-11-10
Cambios:
    - 2026-11-10 · Isacc Cabrera · Creación (paso 11).
"""

import argparse
import contextlib
import json
import sys
from datetime import UTC, datetime, timedelta
from importlib.metadata import version
from pathlib import Path
from typing import Any

import joblib
import mlflow
import numpy as np
import pandas as pd
from mlflow.models import infer_signature

from radar.config import Settings, get_settings
from radar.logging import configure_logging, get_logger
from radar.ml.evaluate import metrics, overfit_gap, plot_real_vs_pred, time_series_folds
from radar.ml.features import FEATURES, TARGET, TZ_LOCAL, build_training_frame, demand_by_station
from radar.ml.models import Predictor, RadarPyfuncModel, make_predictors

log = get_logger(__name__)

EXPERIMENT = "radar-ecobici"
REPORT_MD = Path("docs/reportes/comparacion_modelos.md")
REPORT_HTML = Path("docs/reportes/real_vs_pred.html")
OK_RESULTS = ("guardado", "sin_cambios")
PLOT_STATIONS = 4


# --------------------------------------------------------------------------- datos
def covered_hours(data_dir: Path, max_gap_minutes: float) -> set[pd.Timestamp]:
    """Horas UTC en las que el recolector corrió sin huecos mayores a ``max_gap_minutes``.

    Misma regla que ``scripts/run_hourly.py`` (hueco más largo entre corridas exitosas de
    ``station_status`` dentro de la hora, contando los bordes).

    Args:
        data_dir: Raíz del lake.
        max_gap_minutes: Hueco máximo permitido.

    Returns:
        Inicios de hora (UTC) con cobertura completa.
    """
    times: dict[pd.Timestamp, list[datetime]] = {}
    for path in sorted((data_dir / "bronze" / "heartbeat").glob("dt=*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if row.get("feed") != "station_status" or row.get("result") not in OK_RESULTS:
                continue
            ts = datetime.fromisoformat(row["ts_utc"])
            hour = ts.replace(minute=0, second=0, microsecond=0)
            times.setdefault(pd.Timestamp(hour), []).append(ts)
    good = set()
    for hour, values in times.items():
        start = hour.to_pydatetime()
        points = [start, *sorted(values), start + timedelta(hours=1)]
        gap = max((b - a).total_seconds() / 60 for a, b in zip(points, points[1:], strict=False))
        if gap <= max_gap_minutes:
            good.add(hour)
    return good


def load_frame(settings: Settings) -> tuple[pd.DataFrame, dict[str, Any], pd.DataFrame]:
    """Marco de entrenamiento con solo horas de cobertura completa.

    Args:
        settings: Configuración.

    Returns:
        ``(marco, resumen de datos, station_information)``.

    Raises:
        FileNotFoundError: Si falta gold/features_base o station_information.
    """
    root = settings.data_dir
    base = pd.read_parquet(root / "gold" / "features_base")
    base["station_id"] = base["station_id"].astype(str)
    info = pd.read_parquet(root / "silver" / "station_information" / "station_information.parquet")
    info["station_id"] = info["station_id"].astype(str)
    weather_path = root / "silver" / "weather" / "weather.parquet"
    weather = pd.read_parquet(weather_path) if weather_path.exists() else None
    demand_path = root / "gold" / "trip_demand" / "trip_demand.parquet"
    demand = demand_by_station(pd.read_parquet(demand_path), info) if demand_path.exists() else None

    good = covered_hours(root, settings.quality_max_gap_minutes)
    hours = pd.to_datetime(base["ts_utc"], utc=True).dt.floor("h")
    keep = hours.isin(good)
    frame = build_training_frame(base[keep], weather, demand)
    resumen = {
        "horas_gold": int(hours.nunique()),
        "horas_usadas": int(hours[keep].nunique()),
        "horas_excluidas": sorted(h.strftime("%Y-%m-%dT%H") for h in set(hours) - good),
        "filas": len(frame),
        "estaciones": int(frame["station_id"].nunique()),
        "desde": str(frame["ts_utc"].min()),
        "hasta": str(frame["ts_utc"].max()),
    }
    return frame, resumen, info


# --------------------------------------------------------------------------- evaluación
def _fresh(name: str) -> Predictor:
    return next(p for p in make_predictors() if p.name == name)


def evaluate_all(frame: pd.DataFrame, n_splits: int) -> dict[str, dict[str, Any]]:
    """Evalúa todas las estrategias en los mismos folds temporales.

    Args:
        frame: Marco de ``build_training_frame``.
        n_splits: Número de folds.

    Returns:
        ``{nombre: {"disponible", "folds": [...], "mae", "rmse", "r2", "mae_train",
        "brecha_sobreajuste_pct", "params"}}``.
    """
    folds = list(time_series_folds(frame, n_splits))
    y = frame[TARGET]
    results: dict[str, dict[str, Any]] = {}
    for proto in make_predictors():
        per_fold = []
        available = True
        for fold in folds:
            model = _fresh(proto.name)
            train, valid = frame.iloc[fold.train_idx], frame.iloc[fold.valid_idx]
            model.fit(train, y.iloc[fold.train_idx])
            if not model.available:
                available = False
                break
            m = metrics(y.iloc[fold.valid_idx], model.predict(valid))
            m_train = metrics(y.iloc[fold.train_idx], model.predict(train))
            per_fold.append(
                {
                    **m,
                    "mae_train": m_train["mae"],
                    "fold": fold.k,
                    "filas_train": len(fold.train_idx),
                    "filas_valid": len(fold.valid_idx),
                    "valida_desde": fold.valid_start.isoformat(),
                }
            )
        entry: dict[str, Any] = {"disponible": available, "params": proto.params()}
        if available:
            for key in ("mae", "rmse", "r2", "mae_train"):
                entry[key] = round(float(np.mean([f[key] for f in per_fold])), 4)
            entry["brecha_sobreajuste_pct"] = overfit_gap(entry["mae_train"], entry["mae"])
            entry["folds"] = per_fold
        results[proto.name] = entry
        log.info("modelo_evaluado", modelo=proto.name, disponible=available, mae=entry.get("mae"))
    return results


# --------------------------------------------------------------------------- MLflow
def _pip_requirements() -> list[str]:
    pkgs = ("mlflow", "lightgbm", "scikit-learn", "pandas", "numpy", "holidays", "joblib")
    return [f"{p}=={version(p)}" for p in pkgs]


def log_to_mlflow(
    settings: Settings,
    results: dict[str, dict[str, Any]],
    winner: Predictor,
    frame: pd.DataFrame,
    data_summary: dict[str, Any],
) -> dict[str, Any]:
    """Un run por estrategia y el campeón registrado con alias ``champion``.

    Args:
        settings: Configuración (``mlflow_tracking_uri``, ``model_name``).
        results: Salida de ``evaluate_all``.
        winner: Estrategia ganadora ya reentrenada con todos los datos.
        frame: Marco completo (para la firma del modelo).
        data_summary: Resumen de datos para registrar como parámetros.

    Returns:
        ``{"modelo", "version", "alias", "run_id"}``.
    """
    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)  # MLflow no lee .env
    mlflow.set_experiment(EXPERIMENT)
    for name, entry in results.items():
        with mlflow.start_run(run_name=name):
            mlflow.set_tag("disponible", str(entry["disponible"]).lower())
            mlflow.log_params({"modelo": name, **entry["params"]})
            datos = {k: v for k, v in data_summary.items() if k != "horas_excluidas"}
            mlflow.log_params({f"datos_{k}": v for k, v in datos.items()})
            if not entry["disponible"]:
                continue
            for f in entry["folds"]:
                for key in ("mae", "rmse", "r2", "mae_train"):
                    mlflow.log_metric(f"fold_{key}", f[key], step=f["fold"])
            mlflow.log_metrics(
                {k: entry[k] for k in ("mae", "rmse", "r2", "mae_train", "brecha_sobreajuste_pct")}
            )

    sample = frame.head(200)
    signature = infer_signature(sample[FEATURES], winner.predict(sample))
    with mlflow.start_run(run_name=f"{winner.name}_campeon") as run:
        mlflow.set_tag("campeon", "true")
        mlflow.log_params(
            {"modelo": winner.name, "filas_entrenamiento": len(frame), **winner.params()}
        )
        info = mlflow.pyfunc.log_model(
            name="model",
            python_model=RadarPyfuncModel(winner),
            signature=signature,
            pip_requirements=_pip_requirements(),
            registered_model_name=settings.model_name,
        )
    client = mlflow.MlflowClient()
    version_ = str(info.registered_model_version)
    client.set_registered_model_alias(settings.model_name, "champion", version_)
    return {
        "modelo": settings.model_name,
        "version": version_,
        "alias": "champion",
        "run_id": run.info.run_id,
    }


# --------------------------------------------------------------------------- reportes
def comparison_markdown(
    results: dict[str, dict[str, Any]], winner: str, improvement: float, data: dict[str, Any]
) -> str:
    """Tabla Markdown de la comparación (para ``docs/reportes`` y ``docs/modelo.md``).

    Args:
        results: Salida de ``evaluate_all``.
        winner: Nombre del ganador.
        improvement: Mejora del ganador sobre la persistencia, en %.
        data: Resumen de datos.

    Returns:
        Documento Markdown.
    """
    excluded = ", ".join(data["horas_excluidas"]) or "ninguna"
    lines = [
        "# Comparación de modelos t+60",
        "",
        f"Generado por `python -m radar.ml.train`. Datos: {data['filas']:,} filas, "
        f"{data['estaciones']} estaciones, {data['horas_usadas']} horas con cobertura completa "
        f"de {data['horas_gold']} en gold ({data['desde']} → {data['hasta']}, UTC). "
        f"Horas excluidas por huecos del recolector: {excluded}.",
        "",
        "Validación temporal con folds expandibles y purga del horizonte (la fila cuyo target cae "
        "en el periodo de validación no se usa para entrenar). Métricas = promedio de los folds.",
        "",
        "| Modelo | MAE | RMSE | R² | MAE entrenamiento | Brecha de sobreajuste |",
        "|---|---|---|---|---|---|",
    ]
    for name, e in results.items():
        if not e["disponible"]:
            lines.append(f"| {name} | no disponible | | | | menos de 7 días de historia |")
            continue
        mark = " **(ganador)**" if name == winner else ""
        lines.append(
            f"| {name}{mark} | {e['mae']:.3f} | {e['rmse']:.3f} | {e['r2']:.3f} | "
            f"{e['mae_train']:.3f} | {e['brecha_sobreajuste_pct']:.1f} % |"
        )
    lines += [
        "",
        f"**Ganador:** `{winner}` · mejora sobre el baseline de persistencia: "
        f"**{improvement:.1f} %** de MAE.",
        "",
        "## MAE por fold",
        "",
        "| Modelo | "
        + " | ".join(
            f"fold {f['fold']} (valida desde {f['valida_desde'][:16]})"
            for f in results["baseline"]["folds"]
        )
        + " |",
        "|---|" + "---|" * len(results["baseline"]["folds"]),
    ]
    for name, e in results.items():
        if e["disponible"]:
            lines.append(f"| {name} | " + " | ".join(f"{f['mae']:.3f}" for f in e["folds"]) + " |")
    return "\n".join(lines) + "\n"


def real_vs_pred(
    frame: pd.DataFrame, info: pd.DataFrame, winner_name: str, n_splits: int, path: Path
) -> Path:
    """Gráfica del último fold: real vs ganador vs baseline en las estaciones más variables.

    Args:
        frame: Marco completo.
        info: ``station_information`` (para las etiquetas).
        winner_name: Estrategia ganadora.
        n_splits: Número de folds (se usa el último).
        path: Archivo HTML.

    Returns:
        La ruta escrita.
    """
    fold = list(time_series_folds(frame, n_splits))[-1]
    model = _fresh(winner_name)
    model.fit(frame.iloc[fold.train_idx], frame[TARGET].iloc[fold.train_idx])
    valid = frame.iloc[fold.valid_idx].copy()
    valid["pred"] = model.predict(valid)
    valid["baseline"] = valid["bikes_available"]
    labels = info.set_index("station_id")
    valid["etiqueta"] = valid["station_id"].map(
        lambda s: (
            f"{labels.at[s, 'short_name']} {str(labels.at[s, 'name'])[:22]}"
            if s in labels.index
            else s
        )
    )
    top = valid.groupby("station_id")[TARGET].std().sort_values(ascending=False)
    stations = top.head(PLOT_STATIONS).index.tolist()
    title = (
        f"Real vs predicho a 60 min — {winner_name} (último fold de validación, "
        f"desde {fold.valid_start.tz_convert(TZ_LOCAL):%d/%m %H:%M} CDMX)"
    )
    return plot_real_vs_pred(valid, path, title, stations, TZ_LOCAL)


# --------------------------------------------------------------------------- CLI
def main(argv: list[str] | None = None) -> int:
    """CLI ``python -m radar.ml.train``.

    Args:
        argv: Argumentos (None = ``sys.argv``).

    Returns:
        0 ok; 2 uso; 3 sin datos suficientes o MLflow inaccesible.
    """
    parser = argparse.ArgumentParser(description="Entrena y compara los modelos t+60.")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--folds", type=int, default=4)
    parser.add_argument("--sin-mlflow", action="store_true", help="no registra en MLflow")
    args = parser.parse_args(argv)
    settings = get_settings()
    configure_logging(settings.log_level)
    for stream in (sys.stdout, sys.stderr):  # Windows redirige con cp1252: forzar UTF-8
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")

    started = datetime.now(UTC)
    try:
        frame, data_summary, info = load_frame(settings)
        results = evaluate_all(frame, args.folds)
    except (FileNotFoundError, ValueError) as exc:
        log.error("entrenamiento_sin_datos", error=str(exc))
        return 3
    ranked = sorted((e["mae"], n) for n, e in results.items() if e["disponible"])
    winner_name = ranked[0][1]
    base_mae = results["baseline"]["mae"]
    improvement = round(100 * (base_mae - results[winner_name]["mae"]) / base_mae, 2)

    winner = _fresh(winner_name)
    winner.fit(frame, frame[TARGET])
    fallback = settings.model_fallback_path
    fallback.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(winner, fallback)

    REPORT_MD.parent.mkdir(parents=True, exist_ok=True)
    REPORT_MD.write_text(
        comparison_markdown(results, winner_name, improvement, data_summary), encoding="utf-8"
    )
    real_vs_pred(frame, info, winner_name, args.folds, REPORT_HTML)

    registry: dict[str, Any] | None = None
    if not args.sin_mlflow:
        try:
            # MLflow imprime avisos (con emoji) en stdout: van a stderr para no ensuciar el JSON
            with contextlib.redirect_stdout(sys.stderr):
                registry = log_to_mlflow(settings, results, winner, frame, data_summary)
        except Exception as exc:  # MLflow caído o inaccesible: el respaldo joblib ya existe
            log.error("mlflow_fallo", error=str(exc), uri=settings.mlflow_tracking_uri)
            return 3

    out = {
        "modelos": {
            n: {k: e[k] for k in ("mae", "rmse", "r2", "mae_train", "brecha_sobreajuste_pct")}
            for n, e in results.items()
            if e["disponible"]
        },
        "no_disponibles": [n for n, e in results.items() if not e["disponible"]],
        "ganador": winner_name,
        "mejora_vs_baseline_pct": improvement,
        "datos": data_summary,
        "registro": registry,
        "respaldo": str(fallback),
        "segundos": round((datetime.now(UTC) - started).total_seconds(), 1),
    }
    log.info("entrenamiento_terminado", ganador=winner_name, mejora_pct=improvement)
    if args.json:
        print(json.dumps(out, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
