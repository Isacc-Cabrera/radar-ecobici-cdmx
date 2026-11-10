"""
Autor: Isacc Cabrera
Descripción: DAG "horario" de Airflow 3. Cada hora corre bronze_a_silver >> clima >>
    silver_a_gold >> calidad para la hora ANTERIOR a logical_date: en Airflow 3 un cron usa
    CronTriggerTimetable (intervalo de longitud cero), así que la corrida de las 14:00 procesa
    la hora 13. El DAG no tiene lógica: cada tarea llama scripts/run_hourly.py con el venv
    aislado del proyecto. Como cada etapa reemplaza su partición, reintentar o hacer backfill
    de una hora es seguro (idempotente).
Patrón: N/A (orquestación declarativa; la lógica vive en scripts/run_hourly.py)
Fecha de creación: 2026-11-10
Cambios:
    - 2026-11-10 · Isacc Cabrera · Creación (paso 9).
"""

from datetime import UTC, datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

PYTHON = "/opt/radar/.venv/bin/python"
SCRIPT = "/opt/radar/scripts/run_hourly.py"
HOUR = "{{ (logical_date - macros.timedelta(hours=1)).strftime('%Y-%m-%dT%H') }}"


def etapa(task_id: str, stage: str) -> BashOperator:
    """Tarea que corre una etapa de run_hourly.py para la hora anterior a logical_date.

    Args:
        task_id: Nombre de la tarea en el DAG.
        stage: Etapa de run_hourly.py (silver, clima, gold o calidad).

    Returns:
        El operador listo para encadenar.
    """
    return BashOperator(
        task_id=task_id,
        bash_command=f'{PYTHON} {SCRIPT} --hour "{HOUR}" --stage {stage} --json',
    )


with DAG(
    dag_id="horario",
    description="bronze → silver → clima → gold → calidad de la hora anterior",
    schedule="0 * * * *",
    start_date=datetime(2026, 11, 8, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["radar", "lake"],
):
    (
        etapa("bronze_a_silver", "silver")
        >> etapa("clima", "clima")
        >> etapa("silver_a_gold", "gold")
        >> etapa("calidad", "calidad")
    )
