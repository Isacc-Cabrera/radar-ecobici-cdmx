# sql/

Consultas analíticas en DuckDB sobre las capas silver y gold (Parquet).

- `analitica.sql` — JOIN, GROUP BY + HAVING, subconsulta y funciones de ventana (paso 8).
  Se ejecuta con `uv run python -m radar.lake.silver_to_gold --sql-report sql/analitica.sql`.
