-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02 · Silver — Limpieza y tipado de NOAA OISST
-- MAGIC
-- MAGIC A diferencia de AEMET, aquí los decimales ya vienen con punto (formato internacional),
-- MAGIC así que el tipado es directo. Añadimos columnas de fecha/año/mes para facilitar
-- MAGIC cruces temporales con las demás fuentes más adelante.

-- COMMAND ----------

USE CATALOG global_climate;

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.noaa_oisst_clean
COMMENT 'Temperatura superficial del mar (SST), región Mediterráneo/España, tipada y con fecha desglosada. Fuente: NOAA OISST v2.1 vía ERDDAP.'
AS
SELECT
    TRY_CAST(time AS TIMESTAMP)        AS fecha_hora,
    CAST(TRY_CAST(time AS TIMESTAMP) AS DATE) AS fecha,
    YEAR(TRY_CAST(time AS TIMESTAMP))  AS anio,
    MONTH(TRY_CAST(time AS TIMESTAMP)) AS mes,
    TRY_CAST(latitude AS DOUBLE)       AS latitud,
    TRY_CAST(longitude AS DOUBLE)      AS longitud,
    TRY_CAST(sst AS DOUBLE)            AS temp_superficial_c,
    TRY_CAST(anom AS DOUBLE)           AS anomalia_c,
    TRY_CAST(err AS DOUBLE)            AS margen_error_c,
    ingest_date
FROM bronze.noaa_oisst_raw;

-- COMMAND ----------

ALTER TABLE silver.noaa_oisst_clean ALTER COLUMN temp_superficial_c COMMENT 'Temperatura superficial del mar (°C)';
ALTER TABLE silver.noaa_oisst_clean ALTER COLUMN anomalia_c COMMENT 'Anomalía respecto a la media climatológica de referencia (°C)';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Verificación

-- COMMAND ----------

SELECT
    COUNT(*) AS total_filas,
    COUNT(*) - COUNT(temp_superficial_c) AS nulos_sst,
    MIN(fecha) AS fecha_min,
    MAX(fecha) AS fecha_max,
    ROUND(AVG(temp_superficial_c), 2) AS temp_media_periodo
FROM silver.noaa_oisst_clean;

-- COMMAND ----------

SELECT * FROM silver.noaa_oisst_clean LIMIT 10;