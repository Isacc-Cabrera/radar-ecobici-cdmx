# Métricas — Radar ECOBICI CDMX

> Autor: Isacc Cabrera · Creado: 2026-11-09 · Una métrica, una definición, una fuente.
> Zona horaria de negocio: `America/Mexico_City`. Todo se guarda en UTC y se convierte una sola vez en gold.

| Métrica | Definición | Grano | Fuente | Umbral / meta |
|---|---|---|---|---|
| `cobertura_heartbeat` | % de minutos de la hora con un heartbeat `guardado` o `sin_cambios` de `station_status` | estación-sistema × hora | `data/bronze/heartbeat/` | ≥ 90 % por hora; menos → el check `huecos_heartbeat` falla |
| `tasa_regla_capacidad` | Fracción de filas donde `num_bikes_available + num_bikes_disabled + num_docks_available + num_docks_disabled = capacity` | hora | silver | ≥ 95 %; menos → el check `regla_capacidad` falla |
| `filas_cuarentena` | Número de filas enviadas a cuarentena, por motivo | hora × motivo | silver/cuarentena | Se reporta; un salto brusco indica cambio en el feed |
| `lag_consumidor` | Mensajes pendientes del grupo `bronze-writer` en `station_status_raw` | instante | Kafka | 0 en operación normal; > 10 por más de 5 min → revisar el consumidor |
| `mae_t60` | Error absoluto medio de bicis disponibles predichas a 60 min | conjunto de validación temporal | MLflow | Menor que el del baseline |
| `mejora_vs_baseline_pct` | `100 × (mae_baseline − mae_modelo) / mae_baseline` | conjunto de validación temporal | MLflow / `docs/modelo.md` | > 0 (si no, se explica por qué) |
| `latencia_api_p95` | Percentil 95 de la latencia de `/predict/{station_id}` | ventana de prueba | pruebas locales de la API | < 200 ms en local |
