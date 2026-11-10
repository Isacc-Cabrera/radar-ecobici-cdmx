# Benchmark DuckDB vs pandas

Generado el 2026-11-09 con `uv run python -m radar.lake.trips --benchmark`.

**Agregación:** viajes y duración media por estación de retiro, día de la semana y hora local. Cada motor corre en su propio subproceso (memoria limpia), 3 veces; se reporta la mediana del tiempo de reloj (`time.perf_counter`, lectura + agregación, sin contar los imports) y la memoria pico del proceso (`psutil` `peak_wset` en Windows, `ru_maxrss` en Linux). *Base* = memoria tras importar las librerías, antes de leer datos.

**Máquina:** Windows-11-10.0.26300-SP0 · Python 3.12.15 · 8 CPUs lógicos. **Datos:** `public_data_web_2026-10.csv` (bronze, CSV) y su versión silver en Parquet.

| Fuente | Motor | Segundos (mediana) | Corridas | Base MB | Pico MB | Pico − base MB | Grupos | Viajes |
|---|---|---|---|---|---|---|---|---|
| parquet | duckdb | 0.468 | 0.488, 0.451, 0.468 | 129.6 | 187.8 | 58.2 | 89063 | 1599384 |
| parquet | pandas | 0.590 | 0.590, 0.555, 0.601 | 129.7 | 378.8 | 249.1 | 89063 | 1599384 |
| csv | duckdb | 0.560 | 0.560, 0.542, 0.560 | 130.5 | 295.0 | 164.5 | 89063 | 1599384 |
| csv | pandas | 9.814 | 9.814, 9.577, 9.960 | 129.8 | 485.9 | 356.1 | 89063 | 1599384 |

- **parquet:** mismos resultados en ambos motores: sí; DuckDB 1.3× más rápido y 191 MB menos de memoria de trabajo que pandas.
- **csv:** mismos resultados en ambos motores: sí; DuckDB 17.5× más rápido y 192 MB menos de memoria de trabajo que pandas.
