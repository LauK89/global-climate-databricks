-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02 · Silver — Limpieza y tipado de IBTrACS
-- MAGIC
-- MAGIC `bronze.noaa_ibtracs_raw` tiene más de 150 columnas (agrega varias agencias
-- MAGIC meteorológicas). En silver nos quedamos solo con las columnas que vamos a usar
-- MAGIC realmente en el análisis, tipadas, más una traducción legible del código de cuenca.

-- COMMAND ----------

USE CATALOG global_climate;

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.noaa_ibtracs_clean
COMMENT 'Ciclones tropicales globales desde 1980, columnas principales tipadas. Fuente: NOAA IBTrACS v04r01.'
AS
SELECT
    SID                                     AS id_tormenta,
    TRY_CAST(SEASON AS INT)                 AS temporada,
    TRY_CAST(NUMBER AS INT)                 AS numero_orden,
    BASIN                                   AS cuenca_cod,
    CASE BASIN
        WHEN 'NA' THEN 'Atlántico Norte'
        WHEN 'WP' THEN 'Pacífico Oeste (tifones)'
        WHEN 'EP' THEN 'Pacífico Este'
        WHEN 'NI' THEN 'Índico Norte'
        WHEN 'SI' THEN 'Índico Sur'
        WHEN 'SP' THEN 'Pacífico Sur'
        WHEN 'SA' THEN 'Atlántico Sur'
        ELSE 'Desconocida'
    END                                     AS cuenca_desc,
    SUBBASIN                                AS subcuenca,
    NAME                                    AS nombre,
    TRY_CAST(ISO_TIME AS TIMESTAMP)         AS fecha_hora,
    NATURE                                  AS naturaleza,
    TRY_CAST(LAT AS DOUBLE)                 AS latitud,
    TRY_CAST(LON AS DOUBLE)                 AS longitud,
    TRY_CAST(WMO_WIND AS DOUBLE)            AS viento_wmo_kt,
    TRY_CAST(WMO_PRES AS DOUBLE)            AS presion_wmo_hpa,
    TRY_CAST(USA_WIND AS DOUBLE)            AS viento_usa_kt,
    TRY_CAST(USA_PRES AS DOUBLE)            AS presion_usa_hpa,
    TRY_CAST(USA_SSHS AS INT)               AS categoria_saffir_simpson,
    TRY_CAST(DIST2LAND AS DOUBLE)           AS distancia_costa_km,
    TRY_CAST(LANDFALL AS DOUBLE)            AS distancia_en_landfall_km,
    current_date AS  ingest_date
FROM bronze.noaa_ibtracs_raw;

-- COMMAND ----------

ALTER TABLE silver.noaa_ibtracs_clean ALTER COLUMN categoria_saffir_simpson COMMENT 'Categoría Saffir-Simpson: -1 (depresión/tormenta tropical) a 5 (huracán catastrófico). Solo aplica a cuencas donde NOAA es la agencia de referencia (NA, EP)';
ALTER TABLE silver.noaa_ibtracs_clean ALTER COLUMN cuenca_desc COMMENT 'Nombre legible de la cuenca oceánica — NA/EP suelen llamarse "huracanes", WP se llama "tifones": mismo fenómeno físico, distinto nombre regional';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Verificación

-- COMMAND ----------

SELECT
    COUNT(*)                                    AS total_registros,
    COUNT(DISTINCT id_tormenta)                 AS total_tormentas,
    COUNT(*) - COUNT(viento_usa_kt)              AS nulos_viento_usa,
    MIN(temporada)                               AS temporada_min,
    MAX(temporada)                               AS temporada_max
FROM silver.noaa_ibtracs_clean;

-- COMMAND ----------

SELECT cuenca_desc, COUNT(DISTINCT id_tormenta) AS num_tormentas
FROM silver.noaa_ibtracs_clean
GROUP BY cuenca_desc
ORDER BY num_tormentas DESC;

-- COMMAND ----------

SELECT * FROM silver.noaa_ibtracs_clean LIMIT 10;