# Autor: Isacc Cabrera
# Descripción: Imagen de la API FastAPI con el modelo (LightGBM necesita libgomp1).
# Patrón: N/A
# Fecha de creación: 2026-11-11
# Cambios:
#     - 2026-11-11 · Isacc Cabrera · Creación (paso 13).
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app/.venv
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir uv==0.13.7 && useradd --create-home --uid 10001 radar
WORKDIR /app
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev --no-install-project --extra api --extra lake --extra ml
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable --extra api --extra lake --extra ml \
    && chown -R radar:radar /app
ENV PATH="/app/.venv/bin:$PATH"
USER radar
EXPOSE 8000
CMD ["uvicorn", "radar.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
