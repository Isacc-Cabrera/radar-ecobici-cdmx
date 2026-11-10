# Detección de anomalías

Tres detectores marcan estaciones que se comportan raro. Comparten la misma interfaz
(**Strategy**: `AnomalyDetector.detect(recent, history, now_utc) -> list[Anomaly]`) en
`src/radar/anomalies/detectors.py`, así que la API los corre en fila sin saber cómo funciona cada
uno.

| Dónde se ve | Cómo |
|---|---|
| API | `GET /anomalies` (caché de 60 s) y `anomalia: true` en `GET /stations` para exactamente esas estaciones |
| Mapa | anillo `ANOMALY_COLOR` en la base de la columna, "⚠ Anomalía" en el tooltip, la lista y la leyenda |
| Terminal | `uv run python -m radar.anomalies.detectors --json` (conteos, intersecciones y los 15 casos más fuertes) |

**Entradas:** `recent` = silver de las últimas 2 horas (`last_reported`, bicis, capacidad);
`history` = `gold/features_base` (rejilla de 5 min); `now_utc` = **el último dato en silver**, no
el reloj. Si el recolector se detiene, ninguna estación se marca como congelada por culpa nuestra.
La historia excluye la última hora para no comparar el presente consigo mismo.

Umbrales en `.env`: `ANOMALY_FROZEN_MINUTES=90`, `ANOMALY_ZSCORE=3`.

## Los tres detectores

### Congelada (`tipo="congelada"`)

- **Qué mira:** el `last_reported` más reciente de la estación. Si no avanzó en
  `ANOMALY_FROZEN_MINUTES` (90 min), la estación dejó de reportar aunque el feed siga respondiendo.
- **Puntaje:** minutos sin reporte nuevo.
- **Fortaleza:** regla simple y explicable; encuentra fallas de hardware o de comunicación.

### Z-score (`tipo="zscore"`)

- **Qué mira:** cuántas desviaciones estándar se alejan las bicis actuales de lo normal para
  **esa estación a esa hora local**: `z = (bicis − media) / desviación`. La referencia son las
  medias horarias de la estación, una por día (lo mismo que `gold/station_hour.bikes_mean`).
- **Marca** si `|z| ≥ 3`. **Omite** estaciones con menos de 3 días de historia en esa hora o con
  desviación 0 (no hay "normal" contra qué comparar).
- **Puntaje:** `|z|`. El detalle dice "18 bicis contra 10.0 ± 0.8 habituales a las 9 h".
- **Fortaleza:** explica el porqué con números; respeta que cada estación y cada hora son distintas.

### Isolation Forest (`tipo="isolation_forest"`)

- **Qué mira:** tres features por estado: **% de bicis**, **hora local** y **cambio en 60 min**.
  Se entrena con toda la historia de la rejilla (`contamination=0.02`, `random_state=42`) y evalúa
  el estado actual de cada estación.
- **Cómo decide:** construye árboles con cortes aleatorios; un punto raro queda aislado en pocos
  cortes. No necesita ejemplos etiquetados.
- **Puntaje:** `−score_samples` (más alto = más raro).
- **Debilidad:** no explica el porqué y depende de que la historia cubra la situación actual.

## Resultados reales

Corrida del 10 de noviembre de 2026 con datos hasta las **15:59 UTC (09:59 en CDMX)**; historia
de gold: tarde del 9 y madrugada/mañana del 10 (horas locales 7–8 y 14–22).

| Detector | Estaciones |
|---|---|
| `congelada` | 31 |
| `zscore` | 0 |
| `isolation_forest` | 70 |
| **únicas** | **101** de 677 |
| intersección congelada ∩ isolation_forest | 0 |
| intersección zscore ∩ isolation_forest | 0 |

## Falsos positivos

Un **falso positivo** es una estación marcada que en realidad se comporta normal (o cuya
"anomalía" no le aporta nada al usuario).

1. **CE-016 Reforma–Río Tiber, 09:59 (Isolation Forest, puntaje 0.720).** 97 % llena y +30 bicis
   en 60 min. Es exactamente lo esperado: a las 9 am los viajeros llegan a las oficinas de
   Reforma. El bosque nunca vio las 9 h (su historia solo cubre 7–8 h y 14–22 h), así que cualquier
   estado a esa hora le parece raro. Lo mismo pasa con **CE-021 Reforma–Dublín** (+20, 96 %),
   **CE-022 Reforma–Manchester** (+19, 94 %) y las de Polanco **CE-214 Platón–Horacio** y **CE-221
   Emilio Castelar–Presidente Masaryk** (100 % llenas, +20 y +22). La mayoría de las 70 marcas del
   bosque son este patrón de hora pico.
2. **CE-091 República de Cuba–Allende, 09:59 (congelada, 110 min).** El feed ya la reporta con
   `is_renting=false`: el operador la tiene fuera de servicio. Es técnicamente "congelada", pero
   para quien busca bici no es una noticia. 28 de las 31 congeladas están así (por ejemplo **CE-439
   Amores–Av. Coyoacán**, 1,815 min, y **CE-699 San Gregorio**, 9,457 min ≈ 6.6 días).

**Cómo reducirlos:**
- Isolation Forest: más días de historia (que vea todas las horas) y entrenar por franja horaria;
  o exigir que el z-score coincida antes de mostrar la marca.
- Congelada: separar "fuera de servicio" (`is_renting=false`, ya conocido) de "congelada sin
  aviso" y mostrar solo la segunda como anomalía.

Ejemplos de congeladas que **sí** valen la pena (verdaderos positivos): **CE-143 Córdoba–San Luis
Potosí** (311 min sin reporte con `is_renting=true` y 5 bicis), **CE-687 Francisco Sosa–Panzacola**
(953 min, en renta) y **CE-665 Moras–Mango** (306 min, en renta): el feed dice que funcionan pero
dejaron de reportar.

## Falsos negativos

Un **falso negativo** es una estación con un problema real que ningún detector marca.

1. **CE-094 Lic. Verdad–Moneda, 09:59.** `is_renting=false` (no se puede rentar) con 11 bicis,
   pero sigue reportando cada pocos minutos con valores plausibles. No está congelada, el
   z-score no tiene historia y el bosque no la ve rara. Para un usuario es una estación inútil que
   el mapa muestra como normal. Hay 6 estaciones en esta situación (por ejemplo **CE-160 Alfonso
   Herrera–Gabino Barreda**).
2. **CE-028 Toledo–Tokio.** Viola la regla de capacidad (`bicis + dañadas + muelles + dañados ≠
   capacidad`) 393 veces y es la que más filas manda a cuarentena en silver (ver
   `docs/data-lake.md`), pero sus conteos son verosímiles: ningún detector la marca. Lo mismo
   **CE-050 Cuernavaca–Veracruz** (116 filas en cuarentena).
3. **Todo lo que solo el z-score vería.** Con menos de 3 días por hora local, el z-score omite las
   677 estaciones: una estación que hoy tuviera 30 bicis donde siempre tiene 5 no se marcaría.
   Es un falso negativo por falta de historia, no por diseño: se corrige solo cuando el recolector
   junte 3 días.

**Cómo reducirlos:** usar `is_renting`/`is_returning` del feed como un cuarto detector
("fuera de servicio"), y convertir la tasa de cuarentena por estación en una señal de anomalía de
calidad de datos.

## Z-score vs Isolation Forest

| | Z-score | Isolation Forest |
|---|---|---|
| Idea | distancia a lo normal **de esa estación a esa hora** | qué tan fácil es aislar el punto entre todos los estados vistos |
| Necesita | ≥ 3 días por hora local | historia que cubra la hora y la situación actual |
| Explica el porqué | sí ("18 contra 10 ± 0.8") | no (solo un puntaje) |
| Hoy | 0 marcas (sin historia suficiente) | 70 marcas (muchas FP de hora pico) |
| Intersección | — | 0 estaciones en común |

**Por qué compararlos:** miden cosas distintas. El z-score es local (cada estación contra sí
misma) y explicable; el bosque es global (todas las estaciones juntas) y encuentra combinaciones
raras sin reglas. Cuando los dos coinciden, la marca es mucho más confiable; cuando no, el
desacuerdo dice cuál le falta contexto. Hoy la intersección es vacía porque el z-score todavía no
tiene historia: es el primer número a volver a medir con 3 días o más de datos
(`uv run python -m radar.anomalies.detectors --json`).
