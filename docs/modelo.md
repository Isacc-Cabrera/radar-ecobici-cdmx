# Modelo de Radar ECOBICI CDMX

## Objetivo

Predecir, para cada estación, **cuántas bicis habrá disponibles dentro de 60 minutos** (`y_t60`)
a partir de su estado actual y su pasado reciente, el calendario de CDMX, el clima y la demanda
histórica de viajes.

- **Unidad de predicción:** una estación en un punto de la rejilla de 5 minutos de
  `gold/features_base` (`station_id`, `ts_utc`).
- **Target:** `y_t60` = `bikes_available` de la misma estación en `ts_utc + 60 min`, que son 12
  pasos de la rejilla. Se busca **por tiempo**, no con `shift(-12)` por filas: la rejilla tiene
  huecos (horas sin recolector) y 12 filas después podrían ser horas después. Si ese punto no
  existe, la fila se descarta.
- **Línea base a vencer:** la persistencia ("dentro de una hora habrá lo mismo que ahora"), con
  MAE ≈ 3.06 bicis sobre las 65,665 filas del EDA y 3.02 en la validación temporal del
  entrenamiento (ver Comparación de modelos).
- **Código:** `src/radar/ml/features.py`. `build_training_frame(base, weather, demand)` arma el
  marco de entrenamiento y `build_inference_frame(base_recent, weather, demand, now_utc)` devuelve
  la última fila de cada estación. Ambos usan la misma lista `FEATURES`.

## Features

| Feature | Fórmula | Fuente |
|---|---|---|
| `bikes_available` | bicis disponibles en `t` | `gold/features_base` |
| `capacity` | capacidad de la estación en `t` | `gold/features_base` |
| `lag_15`, `lag_30`, `lag_60` | `bikes_available` en exactamente `t − 15/30/60 min` (NaN si ese punto no existe) | `gold/features_base` |
| `roll_mean_30`, `roll_mean_60` | media de `bikes_available` en la ventana `(t − 30 min, t]` / `(t − 60 min, t]` (`rolling(..., closed="right")`) | `gold/features_base` |
| `hour_local` | hora de `t` en `America/Mexico_City` (0–23) | calendario |
| `dow` | día de la semana de `t` en CDMX (0 = lunes) | calendario |
| `is_weekend` | 1 si `dow` ≥ 5 | calendario |
| `is_holiday` | 1 si la fecha local es festivo oficial (`holidays.country_holidays("MX")`) | calendario |
| `temperature_2m`, `precipitation` | valor de la última hora ≤ `t` (as-of hacia atrás) | `silver/weather` (Open-Meteo) |
| `demanda_salidas`, `demanda_llegadas` | salidas/llegadas promedio de esa estación para el día y la hora local de `t`; 0 si no hubo viajes | `gold/trip_demand` (viajes de octubre de 2026), vía `demand_by_station` |

La demanda viene por `short_name` normalizado; `demand_by_station(demand, station_info)` la
traduce a `station_id` con `silver/station_information` antes de pasarla a las funciones de
features.

## Revisión de fuga de datos

**Fuga de datos** es usar, para predecir en `t`, información que en la vida real todavía no
existiría en `t`. Infla las métricas de validación y luego el modelo falla en producción.

| Feature | Por qué no ve el futuro | Cómo se comprueba |
|---|---|---|
| `bikes_available`, `capacity` | es el valor en `t` | — |
| `lag_15/30/60` | buscan un `ts_utc` estrictamente anterior a `t` | `test_alterar_el_futuro_no_cambia_las_features_en_t` |
| `roll_mean_30/60` | ventana cerrada a la derecha, `(t − w, t]`: nunca incluye puntos posteriores a `t` (una media **centrada** sí lo haría) | misma prueba; `test_lags_y_ventanas_tienen_los_valores_del_pasado` |
| calendario | depende solo del reloj en `t` | `test_calendario_en_hora_de_cdmx_y_festivos` |
| clima | as-of hacia **atrás**: la hora `h ≤ t`, nunca `h + 1` | `test_clima_usa_la_ultima_hora_anterior_o_igual` |
| demanda | es un promedio de **octubre**: un periodo completo y anterior a los datos de noviembre que se predicen | `test_demanda_por_estacion_dia_y_hora_local` |
| `y_t60` | es el único valor futuro, y es el target: alterar el futuro **sí** lo cambia (la prueba lo exige) | `test_y_t60_es_12_pasos_despues_y_descarta_filas_sin_target` |

Riesgos que quedan y cómo se manejan:

- **Clima de pronóstico vs observado.** `silver/weather` guarda 7 días pasados más 1 de
  pronóstico y el upsert reemplaza el pronóstico por lo observado. Al **entrenar** se usa lo
  observado; al **predecir en vivo**, la hora actual puede ser todavía un pronóstico. Es una
  pequeña diferencia entre entrenamiento y producción, pero no es fuga: nunca se usa una hora
  posterior a `t`.
- **Demanda de otro periodo.** No es fuga porque octubre terminó antes de noviembre. El riesgo es
  otro: que el patrón cambie (vacaciones, festivos). Se revisa en la evaluación por hora.
- **Deduplicación que cambia la rejilla.** Silver guarda solo los cambios de estado y gold
  rellena cada punto con el último valor conocido. Si un día cambian las reglas de deduplicación,
  la rejilla cambia; por eso las features se calculan siempre desde `gold/features_base`, nunca
  desde silver directamente.
- **Horas "congeladas".** Si el recolector corrió solo unos minutos en una hora, la rejilla
  repite un valor viejo el resto de la hora (en el EDA, las horas 04 y 12 UTC del 10 de noviembre
  tienen 0–4 % de puntos con cambio, contra ~45 % normal). No es fuga, pero enseña al modelo que
  "nada cambia". El entrenamiento (paso 11) debe excluir las horas con cobertura incompleta del
  heartbeat.
- **Validación aleatoria.** Separar filas al azar mezcla minutos vecinos entre entrenamiento y
  prueba. La validación es siempre temporal (`TimeSeriesSplit`): se entrena con el pasado y se
  evalúa con lo posterior.

## Comparación de modelos

`uv run python -m radar.ml.train --json` (10 de noviembre de 2026). Datos: 47,390 filas de 677
estaciones en **8 horas con cobertura completa del recolector** (de 12 en gold); se excluyeron
las horas con huecos de más de 10 minutos en el heartbeat (20 UTC del 9; 04, 12 y 14 UTC del 10).
Validación temporal con 4 folds expandibles y **purga**: una fila cuyo target (`t + 60 min`) cae
en el periodo de validación no se usa para entrenar. Métricas = promedio de los folds. Reporte
completo: `docs/reportes/comparacion_modelos.md`; gráfica: `docs/reportes/real_vs_pred.html`.

| Modelo | MAE | RMSE | R² | MAE entrenamiento | Brecha de sobreajuste |
|---|---|---|---|---|---|
| baseline (persistencia) | 3.018 | 4.950 | 0.510 | 2.982 | 1.2 % |
| baseline_semana | no disponible | | | | menos de 7 días de historia |
| lineal | 3.154 | 4.839 | 0.560 | 2.671 | 15.3 % |
| **lightgbm_a (ganador)** | **2.848** | **4.562** | **0.597** | 2.037 | 28.5 % |
| lightgbm_b | 2.931 | 4.662 | 0.582 | 1.767 | 39.7 % |
| lightgbm_c | 2.990 | 4.693 | 0.578 | 1.379 | 53.9 % |

**El ganador, `lightgbm_a`, mejora el MAE del baseline en 5.6 %** (2.848 contra 3.018 bicis) y
gana en 3 de los 4 folds; en el último (validación desde la 01:50 UTC, de madrugada, cuando casi
nada se mueve) la persistencia es mejor (2.671 contra 2.762). Está registrado en MLflow como
`radar-bikes-t60` con alias `champion`, con respaldo en `models/champion.joblib`.

Lo que hizo la diferencia: **LightGBM aprende el cambio** (`y_t60 − bikes_available`) y no el
nivel. Aprendiendo el nivel, `lightgbm_a` daba MAE 3.033 y no vencía a la persistencia; los
árboles tenían que reconstruir "lo mismo que ahora" por tramos. Aprendiendo el cambio, "no
cambia" es el punto de partida y los árboles solo corrigen. Para la regresión lineal da igual
(`bikes_available` ya es una feature: el modelo es el mismo), y por eso queda detrás: una recta
no captura que una estación vacía no puede perder bicis ni una llena ganarlas.

## Sobreajuste

La brecha es `100 · (MAE validación − MAE entrenamiento) / MAE validación`.

- El baseline casi no tiene brecha (1.2 %): no aprende nada, no puede memorizar.
- En LightGBM la brecha crece con la complejidad: 28.5 % (15 hojas), 39.7 % (31) y 53.9 % (63
  hojas, 800 árboles). Al mismo tiempo **el MAE de validación empeora** (2.848 → 2.931 → 2.990):
  las configuraciones grandes memorizan la tarde del 9 de noviembre en vez de aprender patrones.
- Con 8 horas de datos gana la configuración más simple. Con más días, una más compleja podría
  ganar: es lo primero que hay que volver a medir.

## Limitaciones

- **Pocos datos.** 8 horas útiles de una sola tarde-noche (lunes 9 y madrugada del martes 10): el
  modelo no ha visto una mañana completa, un fin de semana ni la hora pico de las 8 h. `dow`,
  `is_weekend` e `is_holiday` son prácticamente constantes en el entrenamiento y no pueden
  aportar todavía. `baseline_semana` no se puede evaluar (requiere 7 días).
- **Sin rebalanceo.** El camión que redistribuye bicis entre estaciones no aparece en ningún dato;
  esos saltos son imposibles de predecir y elevan el error en las estaciones grandes.
- **Clima observado.** Se entrena con el clima observado y en vivo se usaría el de la hora actual
  (posiblemente todavía pronóstico); con 8 horas y casi sin lluvia, el clima apenas varía.
- **Demanda de otro periodo.** Viene de octubre; si el patrón de noviembre cambia (vacaciones,
  eventos), la feature se desalinea.
- **Horas excluidas.** El modelo solo se evalúa en horas con el recolector completo; en vivo, si
  el recolector se detiene, las features de lag se vuelven viejas y la predicción empeora.

## Exportar el notebook

- **HTML** (lo que se entrega y se versiona):
  `uv run jupyter nbconvert --to html --execute notebooks/01_eda.ipynb --output-dir docs/reportes`
  deja `docs/reportes/01_eda.html` con todas las gráficas (plotly.js desde CDN).
- **PDF:** abrir ese HTML en el navegador e imprimir → "Guardar como PDF". `nbconvert --to pdf`
  requiere LaTeX y `--to webpdf` requiere Chromium; ninguno se instala en el proyecto.
