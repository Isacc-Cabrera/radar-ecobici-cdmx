# Reporte SQL analítico

Generado con `uv run python -m radar.lake.silver_to_gold --sql-report sql/analitica.sql --out docs/reportes/analitica.md` (DuckDB sobre el Parquet del lake, horas en UTC salvo que la columna diga `local`).

Vistas disponibles: `silver_status`, `quarantine`, `station_information`, `silver_trips`, `gold_station_hour`, `gold_features_base`, `trip_demand`.

## estaciones_que_mas_se_vacian

```sql
-- JOIN gold ↔ station_information: % del tiempo observado en que la estación estuvo sin bicis.
SELECT i.short_name,
       i.name,
       ROUND(AVG(g.pct_vacia) * 100, 1) AS pct_tiempo_vacia,
       ROUND(AVG(g.bikes_mean), 2)      AS bicis_prom,
       COUNT(*)                         AS horas
FROM gold_station_hour AS g
JOIN station_information AS i ON i.station_id = g.station_id
GROUP BY i.short_name, i.name
ORDER BY pct_tiempo_vacia DESC, bicis_prom ASC, i.short_name
LIMIT 20
```

20 filas; primeras 20:

| short_name | name | pct_tiempo_vacia | bicis_prom | horas |
|---|---|---|---|---|
| 004 | CE-004 Río Nilo - Río Panuco | 100.0 | 0.0 | 6 |
| 033 | CE-033 Londres - Florencia | 100.0 | 0.0 | 6 |
| 096 | CE-096 López-Ayuntamiento | 100.0 | 0.0 | 6 |
| 115 | CE-115 Roma-Dinamarca | 100.0 | 0.0 | 6 |
| 172 | CE-172 General Francisco Murguía-José Martí | 100.0 | 0.0 | 6 |
| 285 | CE-285 12 de Octubre-Minería | 100.0 | 0.0 | 6 |
| 328 | CE-328 Avenida 1-Calle 7 | 100.0 | 0.0 | 6 |
| 370 | CE-370 Tlacoquemécatl-Av. Insurgentes | 100.0 | 0.0 | 6 |
| 402 | CE-402 Uxmal-Municipio Libre | 100.0 | 0.0 | 6 |
| 407 | CE-407  Prolongación Xochicalco-General Emiliano Zapata | 100.0 | 0.0 | 6 |
| 413 | CE-413 Parroquia-Avenida Coyoacán | 100.0 | 0.0 | 6 |
| 439 | CE-439 Amores-Avenida Coyoacán | 100.0 | 0.0 | 6 |
| 445-446 | CE-445-446 Riff-Avenida Río Churubusco | 100.0 | 0.0 | 6 |
| 452 | CE-452 Leibnitz-Av. Ejercito Nacional | 100.0 | 0.0 | 6 |
| 472 | CE-472 Bahía de Santa Bárbara-Bahía de Ballenas | 100.0 | 0.0 | 6 |
| 517 | CE-517 Ignacio Allende - Clavería | 100.0 | 0.0 | 6 |
| 533 | CE-533  Av Plan de San Luis - Eulalia Gúzman | 100.0 | 0.0 | 6 |
| 535 | CE-535 Av. Jardín - Orquídea | 100.0 | 0.0 | 6 |
| 624 | CE-624 Luis Spota - Eje Central Lázaro Cárdenas | 100.0 | 0.0 | 6 |
| 625 | CE-625 Zempoala - Angel Urraza | 100.0 | 0.0 | 6 |

## patron_por_dia_y_hora_local

```sql
-- GROUP BY + HAVING: solo combinaciones día/hora con al menos 30 puntos de la rejilla de 5 min.
SELECT dayname(ts_utc AT TIME ZONE 'America/Mexico_City')   AS dia,
       hour(ts_utc AT TIME ZONE 'America/Mexico_City')      AS hora_local,
       ROUND(AVG(bikes_available), 2)                       AS bicis_prom,
       ROUND(AVG(CASE WHEN bikes_available = 0 THEN 1 ELSE 0 END) * 100, 1) AS pct_vacias,
       ROUND(AVG(CASE WHEN docks_available = 0 THEN 1 ELSE 0 END) * 100, 1) AS pct_llenas,
       COUNT(*)                                             AS puntos
FROM gold_features_base
GROUP BY dia, hora_local
HAVING COUNT(*) >= 30
ORDER BY MIN(ts_utc)
```

6 filas; primeras 20:

| dia | hora_local | bicis_prom | pct_vacias | pct_llenas | puntos |
|---|---|---|---|---|---|
| Monday | 14 | 6.54 | 13.6 | 4.1 | 2027 |
| Monday | 15 | 5.93 | 15.5 | 3.8 | 8124 |
| Monday | 16 | 5.48 | 15.7 | 3.3 | 8124 |
| Monday | 17 | 5.0 | 20.4 | 3.1 | 8124 |
| Monday | 18 | 4.37 | 30.7 | 4.0 | 7447 |
| Monday | 19 | 4.72 | 31.6 | 5.2 | 8124 |

## estaciones_sobre_el_promedio

```sql
-- Subconsulta IN (SELECT …): estaciones cuyo promedio de bicis supera el promedio de la red.
SELECT i.short_name,
       i.name,
       i.capacity,
       ROUND(AVG(g.bikes_mean), 2) AS bicis_prom
FROM gold_station_hour AS g
JOIN station_information AS i ON i.station_id = g.station_id
WHERE g.station_id IN (SELECT station_id
                       FROM gold_station_hour
                       GROUP BY station_id
                       HAVING AVG(bikes_mean) > (SELECT AVG(bikes_mean) FROM gold_station_hour))
GROUP BY i.short_name, i.name, i.capacity
ORDER BY bicis_prom DESC, i.short_name
LIMIT 20
```

20 filas; primeras 20:

| short_name | name | capacity | bicis_prom |
|---|---|---|---|
| 237-238 | CE-237-238 Andrés Bello-George Eliot | 63 | 34.87 |
| 266-267 | CE-266-267 Jesús García - Carlos J. Meneses | 75 | 31.29 |
| 273-274 | CE-273-274 Luis Donaldo Colosio - Av. Jesús García | 70 | 29.67 |
| 271-272 | CE-271-272 Jesús García - Carlos J. Meneses | 74 | 29.04 |
| 211 | CE-211 Isaac Newton-Horacio | 35 | 23.81 |
| 212 | CE-212 Hegel-Horacio | 35 | 22.73 |
| 031 | CE-031 Hamburgo - Insurgentes | 37 | 22.43 |
| 223 | CE-223 Isaac Newton-Presidente Masaryk | 35 | 20.9 |
| 029 | CE-029 Reforma - Bucareli | 35 | 19.97 |
| 448 | CE-448 Dr. Andrade-Arcos De Belén | 31 | 19.58 |
| 268-269 | CE-268-269 Luis Donaldo Colosio - Av. Jesús García | 69 | 18.89 |
| 046 | CE-046 Durango - Salamanca | 39 | 18.34 |
| 548 | CE-548 Dr. Mariano Azuela - Jose Antonio Alzate | 39 | 18.34 |
| 697 | CE-697 Francisco Sosa - Tres Cruces | 35 | 17.86 |
| 036 | CE-036 Puebla - Veracruz | 31 | 17.56 |
| 704 | CE-704 Pacífico - Asia | 23 | 16.94 |
| 043 | CE-043 Revillagigedo - Juárez | 31 | 16.9 |
| 087 | CE-087 Gante - Venustiano Carranza | 25 | 16.15 |
| 032 | CE-032 Londres - Sevilla | 35 | 15.94 |
| 038 | CE-038 Cozumel - Puebla | 35 | 15.88 |

## ranking_de_actividad_por_hora

```sql
-- Ventana RANK() OVER (PARTITION BY hora): las 3 estaciones con más reportes distintos (más
-- movimiento de bicis) en cada hora UTC.
WITH r AS (
    SELECT hora_utc,
           station_id,
           n_obs,
           RANK() OVER (PARTITION BY hora_utc ORDER BY n_obs DESC) AS lugar
    FROM gold_station_hour
)
SELECT r.hora_utc, r.lugar, i.short_name, i.name, r.n_obs
FROM r
JOIN station_information AS i ON i.station_id = r.station_id
WHERE r.lugar <= 3
ORDER BY r.hora_utc DESC, r.lugar, i.short_name
```

20 filas; primeras 20:

| hora_utc | lugar | short_name | name | n_obs |
|---|---|---|---|---|
| 2026-11-10 01:00:00+00:00 | 1 | 064 | CE-064 Sonora - Ámsterdam | 49 |
| 2026-11-10 01:00:00+00:00 | 2 | 271-272 | CE-271-272 Jesús García - Carlos J. Meneses | 46 |
| 2026-11-10 01:00:00+00:00 | 3 | 027 | CE-027 Reforma- Havre | 41 |
| 2026-11-10 01:00:00+00:00 | 3 | 553 | CE-553  Jaime Torres Bodet - Ribera de San Cosme | 41 |
| 2026-11-10 00:00:00+00:00 | 1 | 271-272 | CE-271-272 Jesús García - Carlos J. Meneses | 59 |
| 2026-11-10 00:00:00+00:00 | 2 | 064 | CE-064 Sonora - Ámsterdam | 50 |
| 2026-11-10 00:00:00+00:00 | 3 | 038 | CE-038 Cozumel - Puebla | 49 |
| 2026-11-09 23:00:00+00:00 | 1 | 271-272 | CE-271-272 Jesús García - Carlos J. Meneses | 58 |
| 2026-11-09 23:00:00+00:00 | 2 | 237-238 | CE-237-238 Andrés Bello-George Eliot | 52 |
| 2026-11-09 23:00:00+00:00 | 3 | 038 | CE-038 Cozumel - Puebla | 46 |
| 2026-11-09 22:00:00+00:00 | 1 | 292 | CE-292 Adolfo Prieto-Eje 4 Sur Xola | 60 |
| 2026-11-09 22:00:00+00:00 | 2 | 271-272 | CE-271-272 Jesús García - Carlos J. Meneses | 53 |
| 2026-11-09 22:00:00+00:00 | 3 | 018 | CE-018 Reforma - Río Rhin | 42 |
| 2026-11-09 21:00:00+00:00 | 1 | 292 | CE-292 Adolfo Prieto-Eje 4 Sur Xola | 60 |
| 2026-11-09 21:00:00+00:00 | 2 | 208 | CE-208 Hesiodo-Lamartine | 45 |
| 2026-11-09 21:00:00+00:00 | 3 | 548 | CE-548 Dr. Mariano Azuela - Jose Antonio Alzate | 44 |
| 2026-11-09 20:00:00+00:00 | 1 | 292 | CE-292 Adolfo Prieto-Eje 4 Sur Xola | 22 |
| 2026-11-09 20:00:00+00:00 | 2 | 154 | CE-154 Tuxpan-Tlaxcala | 16 |
| 2026-11-09 20:00:00+00:00 | 3 | 449 | CE-449 Acapulco-Puebla | 14 |
| 2026-11-09 20:00:00+00:00 | 3 | 555 | CE-555 Ribera de San Cosme - Insurgentes Centro | 14 |

## cuarentena_por_motivo

```sql
-- Calidad: cuántas filas fueron a cuarentena y de cuántas estaciones distintas.
SELECT motivo,
       COUNT(*)                   AS filas,
       COUNT(DISTINCT station_id) AS estaciones
FROM quarantine
GROUP BY motivo
ORDER BY filas DESC
```

1 filas; primeras 20:

| motivo | filas | estaciones |
|---|---|---|
| regla_capacidad | 580 | 11 |

## demanda_historica_hora_pico

```sql
-- Viajes históricos (sin datos personales): estaciones con más salidas promedio en día hábil,
-- emparejadas con el feed por short_name normalizado.
SELECT d.short_name_norm,
       i.name,
       d.hour_local,
       ROUND(AVG(d.salidas_prom), 2)  AS salidas_prom,
       ROUND(AVG(d.llegadas_prom), 2) AS llegadas_prom
FROM trip_demand AS d
JOIN station_information AS i ON i.short_name_norm = d.short_name_norm
WHERE d.dow BETWEEN 0 AND 4
GROUP BY d.short_name_norm, i.name, d.hour_local
ORDER BY salidas_prom DESC, d.short_name_norm
LIMIT 20
```

20 filas; primeras 20:

| short_name_norm | name | hour_local | salidas_prom | llegadas_prom |
|---|---|---|---|---|
| 266-267 | CE-266-267 Jesús García - Carlos J. Meneses | 7 | 66.07 | 6.64 |
| 273-274 | CE-273-274 Luis Donaldo Colosio - Av. Jesús García | 7 | 58.55 | 3.95 |
| 271-272 | CE-271-272 Jesús García - Carlos J. Meneses | 8 | 56.05 | 4.93 |
| 273-274 | CE-273-274 Luis Donaldo Colosio - Av. Jesús García | 8 | 48.25 | 3.7 |
| 271-272 | CE-271-272 Jesús García - Carlos J. Meneses | 6 | 45.69 | 5.66 |
| 271-272 | CE-271-272 Jesús García - Carlos J. Meneses | 7 | 42.31 | 4.59 |
| 208 | CE-208 Hesiodo-Lamartine | 8 | 41.81 | 15.42 |
| 237-238 | CE-237-238 Andrés Bello-George Eliot | 17 | 39.63 | 17.84 |
| 266-267 | CE-266-267 Jesús García - Carlos J. Meneses | 8 | 39.13 | 6.97 |
| 271-272 | CE-271-272 Jesús García - Carlos J. Meneses | 9 | 37.45 | 3.65 |
| 548 | CE-548 Dr. Mariano Azuela - Jose Antonio Alzate | 6 | 36.5 | 15.57 |
| 268-269 | CE-268-269 Luis Donaldo Colosio - Av. Jesús García | 7 | 35.68 | 2.35 |
| 273-274 | CE-273-274 Luis Donaldo Colosio - Av. Jesús García | 9 | 34.83 | 3.1 |
| 27 | CE-027 Reforma- Havre | 17 | 32.28 | 22.19 |
| 208 | CE-208 Hesiodo-Lamartine | 9 | 32.25 | 12.55 |
| 38 | CE-038 Cozumel - Puebla | 17 | 31.91 | 25.23 |
| 38 | CE-038 Cozumel - Puebla | 18 | 31.77 | 24.26 |
| 495 | CE-495 Amado Nervo - de los Maestros | 7 | 31.23 | 14.78 |
| 192-193 | CE-192-193 Rubén Darío.Reforma | 17 | 30.85 | 12.26 |
| 176 | CE-176 Av. Jesús García-Luis Donaldo Colosio | 7 | 30.65 | 1.32 |

