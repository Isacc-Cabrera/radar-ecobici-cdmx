"""
Autor: Isacc Cabrera
Descripción: DAG "diario" de Airflow 3. Todos los días a las 06:15 UTC (00:15 en CDMX) refresca
    station_information: la descarga y la guarda en bronze solo si su sha256 canónico cambió
    (D-005). No tiene lógica: llama scripts/run_hourly.py --info con el venv del proyecto.
Patrón: N/A (orquestación declarativa; la lógica vive en scripts/run_hourly.py)
Fecha de creación: 2026-11-10
Cambios:
    - 2026-11-10 · Isacc Cabrera · Creación (paso 9).
"""

from datetime import UTC, datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="diario",
    description="Refresco de station_information por hash",
    schedule="15 6 * * *",
    start_date=datetime(2026, 11, 8, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["radar", "estaciones"],
):
    BashOperator(
        task_id="station_information",
        bash_command="/opt/radar/.venv/bin/python /opt/radar/scripts/run_hourly.py --info --json",
    )
