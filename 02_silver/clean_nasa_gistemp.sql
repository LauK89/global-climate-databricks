-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02 · Silver — Limpieza y tipado de NASA GISTEMP
-- MAGIC
-- MAGIC Tipamos las columnas (vienen todas como string desde bronze, con `NULL` ya
-- MAGIC correctamente aplicado donde el original era `***`). Mantenemos el formato "ancho"
-- MAGIC (una columna por mes) aquí; el paso a formato "largo" (una fila por año-mes) lo
-- MAGIC haremos en gold, que es donde de verdad se necesita para cruzar con otras series.

-- COMMAND ----------

USE CATALOG global_climate;

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.nasa_gistemp_clean
COMMENT 'Anomalías de temperatura global Land-Ocean, mensuales y estacionales, tipadas. Formato ancho (una columna por mes). Fuente: NASA GISS GISTEMP v4.'
AS
SELECT
    TRY_CAST(Year AS INT)         AS anio,
    TRY_CAST(Jan AS DOUBLE)        AS anomalia_ene,
    TRY_CAST(Feb AS DOUBLE)        AS anomalia_feb,
    TRY_CAST(Mar AS DOUBLE)        AS anomalia_mar,
    TRY_CAST(Apr AS DOUBLE)        AS anomalia_abr,
    TRY_CAST(May AS DOUBLE)        AS anomalia_may,
    TRY_CAST(Jun AS DOUBLE)        AS anomalia_jun,
    TRY_CAST(Jul AS DOUBLE)        AS anomalia_jul,
    TRY_CAST(Aug AS DOUBLE)        AS anomalia_ago,
    TRY_CAST(Sep AS DOUBLE)        AS anomalia_sep,
    TRY_CAST(Oct AS DOUBLE)        AS anomalia_oct,
    TRY_CAST(Nov AS DOUBLE)        AS anomalia_nov,
    TRY_CAST(Dec AS DOUBLE)        AS anomalia_dic,
    TRY_CAST(`J-D` AS DOUBLE)      AS anomalia_media_anual,
    TRY_CAST(DJF AS DOUBLE)        AS anomalia_invierno,
    TRY_CAST(MAM AS DOUBLE)        AS anomalia_primavera,
    TRY_CAST(JJA AS DOUBLE)        AS anomalia_verano,
    TRY_CAST(SON AS DOUBLE)        AS anomalia_otonio,
    ingest_date
FROM bronze.nasa_gistemp_raw;

-- COMMAND ----------

ALTER TABLE silver.nasa_gistemp_clean ALTER COLUMN anomalia_media_anual COMMENT 'Anomalía media anual (enero-diciembre) respecto al periodo base 1951-1980 (°C)';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Verificación

-- COMMAND ----------

SELECT
    COUNT(*)              AS total_anios,
    MIN(anio)              AS anio_min,
    MAX(anio)              AS anio_max,
    ROUND(AVG(CASE WHEN anio BETWEEN 1880 AND 1900 THEN anomalia_media_anual END), 2) AS media_1880_1900,
    ROUND(AVG(CASE WHEN anio >= 2015 THEN anomalia_media_anual END), 2)               AS media_desde_2015
FROM silver.nasa_gistemp_clean;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Si `media_desde_2015` sale claramente más alto que `media_1880_1900` (debería rondar
-- MAGIC +1.0 a +1.2 frente a valores negativos de finales del s.XIX), confirma que el
-- MAGIC tipado y el signo de las anomalías son correctos.

-- COMMAND ----------

SELECT * FROM silver.nasa_gistemp_clean ORDER BY anio DESC LIMIT 10;