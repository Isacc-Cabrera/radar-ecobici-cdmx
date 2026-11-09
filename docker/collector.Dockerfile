# Autor: Isacc Cabrera
# Descripción: Imagen del recolector, productor Kafka y consumidor bronze-writer (misma imagen).
#   Dos capas de uv sync: primero solo dependencias (se cachean mientras no cambie uv.lock) y
#   después el código, para que editar src/ no reinstale todo. Corre como usuario no root.
# Patrón: N/A
# Fecha de creación: 2026-11-09
# Cambios:
#     - 2026-11-09 · Isacc Cabrera · Creación (paso 3).
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app/.venv
RUN pip install --no-cache-dir uv==0.13.7 && useradd --create-home --uid 10001 radar
WORKDIR /app
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev --no-install-project --extra streaming --extra lake
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable --extra streaming --extra lake \
    && mkdir -p /app/data && chown -R radar:radar /app
ENV PATH="/app/.venv/bin:$PATH"
USER radar
CMD ["python", "-m", "radar.collector.runner"]
