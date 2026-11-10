-- Autor: Isacc Cabrera
-- Descripción: Consultas analíticas sobre el lake (DuckDB). Cada consulta empieza con
--     "-- name: <nombre>"; `python -m radar.lake.silver_to_gold --sql-report` las ejecuta sobre
--     las vistas silver_status, quarantine, station_information, silver_trips,
--     gold_station_hour, gold_features_base y trip_demand, y escribe el resultado en Markdown.
--     Hora local = AT TIME ZONE 'America/Mexico_City' (la sesión corre en UTC).
-- Patrón: N/A
-- Fecha de creación: 2026-11-09
-- Cambios:
--     - 2026-11-09 · Isacc Cabrera · Creación (paso 8).

-- name: estaciones_que_mas_se_vacian
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
LIMIT 20;

-- name: patron_por_dia_y_hora_local
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
ORDER BY MIN(ts_utc);

-- name: estaciones_sobre_el_promedio
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
LIMIT 20;

-- name: ranking_de_actividad_por_hora
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
ORDER BY r.hora_utc DESC, r.lugar, i.short_name;

-- name: cuarentena_por_motivo
-- Calidad: cuántas filas fueron a cuarentena y de cuántas estaciones distintas.
SELECT motivo,
       COUNT(*)                   AS filas,
       COUNT(DISTINCT station_id) AS estaciones
FROM quarantine
GROUP BY motivo
ORDER BY filas DESC;

-- name: demanda_historica_hora_pico
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
LIMIT 20;
