# dags/

DAGs de Airflow 3 (perfil `orquestacion`). Se montan en el contenedor en `/opt/airflow/dags`.

- `horario.py` — cada hora: bronze → silver → clima → gold → checks de calidad de la hora anterior.
- `diario.py` — cada día: refresco de `station_information` por hash.

Las tareas no contienen lógica: llaman a `scripts/run_hourly.py`, que también es el *fallback*
si Airflow no funciona en el time-box de 3 h (paso 9).
