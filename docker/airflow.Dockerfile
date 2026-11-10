# Autor: Isacc Cabrera
# Descripción: Airflow 3 standalone con un venv aislado del proyecto en /opt/radar/.venv
#   (las dependencias del proyecto no tocan el entorno de Airflow ni sus constraints).
# Patrón: N/A
# Fecha de creación: 2026-11-10
# Cambios:
#     - 2026-11-10 · Isacc Cabrera · Creación (paso 9).
FROM apache/airflow:3.4.0-python3.12
USER root
RUN mkdir -p /opt/radar && chown airflow:root /opt/radar
USER airflow
RUN pip install --no-cache-dir uv==0.13.7
ENV UV_PYTHON_DOWNLOADS=never UV_LINK_MODE=copy UV_PROJECT_ENVIRONMENT=/opt/radar/.venv
WORKDIR /opt/radar
COPY --chown=airflow:root pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev --no-install-project --extra lake
COPY --chown=airflow:root src ./src
COPY --chown=airflow:root scripts ./scripts
RUN uv sync --frozen --no-dev --no-editable --extra lake
WORKDIR /opt/airflow
