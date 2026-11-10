# Comparación de modelos t+60

Generado por `python -m radar.ml.train`. Datos: 47,390 filas, 677 estaciones, 8 horas con cobertura completa de 12 en gold (2026-11-09 21:00:00+00:00 → 2026-11-10 02:55:00+00:00, UTC). Horas excluidas por huecos del recolector: 2026-11-09T20, 2026-11-10T04, 2026-11-10T12, 2026-11-10T14.

Validación temporal con folds expandibles y purga del horizonte (la fila cuyo target cae en el periodo de validación no se usa para entrenar). Métricas = promedio de los folds.

| Modelo | MAE | RMSE | R² | MAE entrenamiento | Brecha de sobreajuste |
|---|---|---|---|---|---|
| baseline | 3.018 | 4.950 | 0.510 | 2.982 | 1.2 % |
| baseline_semana | no disponible | | | | menos de 7 días de historia |
| lineal | 3.154 | 4.839 | 0.560 | 2.671 | 15.3 % |
| lightgbm_a **(ganador)** | 2.848 | 4.562 | 0.597 | 2.037 | 28.5 % |
| lightgbm_b | 2.931 | 4.662 | 0.582 | 1.767 | 39.7 % |
| lightgbm_c | 2.990 | 4.693 | 0.578 | 1.379 | 53.9 % |

**Ganador:** `lightgbm_a` · mejora sobre el baseline de persistencia: **5.6 %** de MAE.

## MAE por fold

| Modelo | fold 0 (valida desde 2026-11-09T22:10) | fold 1 (valida desde 2026-11-09T23:25) | fold 2 (valida desde 2026-11-10T00:40) | fold 3 (valida desde 2026-11-10T01:50) |
|---|---|---|---|---|
| baseline | 3.455 | 3.316 | 2.629 | 2.671 |
| lineal | 3.292 | 3.126 | 2.915 | 3.284 |
| lightgbm_a | 3.079 | 2.996 | 2.554 | 2.762 |
| lightgbm_b | 3.123 | 3.155 | 2.650 | 2.798 |
| lightgbm_c | 3.152 | 3.211 | 2.689 | 2.910 |
