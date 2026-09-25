-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02 · Silver — Limpieza y tipado del episodio DANA
-- MAGIC
-- MAGIC Mismo origen y formato que `aemet_climatologia_raw` (misma API, mismos campos),
-- MAGIC así que reutilizamos exactamente la misma lógica de limpieza: coma → punto,
-- MAGIC `"Ip"` → 0 con flag, horas validadas con TRY_CAST/regex.

-- COMMAND ----------

USE CATALOG global_climate;

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.aemet_dana_clean
COMMENT 'Datos climatológicos diarios durante el episodio DANA de Valencia (28 oct - 5 nov 2024), limpios y tipados. Fuente: AEMET OpenData API.'
AS
SELECT
    CAST(fecha AS DATE)                         AS fecha,
    indicativo,
    nombre,
    provincia,
    TRY_CAST(altitud AS INT)                    AS altitud_m,

    TRY_CAST(REPLACE(tmed, ',', '.') AS DOUBLE) AS temp_media_c,
    TRY_CAST(REPLACE(tmin, ',', '.') AS DOUBLE) AS temp_min_c,
    TRY_CAST(REPLACE(tmax, ',', '.') AS DOUBLE) AS temp_max_c,

    CASE
        WHEN prec = 'Ip' THEN 0.0
        WHEN prec IS NULL THEN NULL
        ELSE TRY_CAST(REPLACE(prec, ',', '.') AS DOUBLE)
    END                                          AS precipitacion_mm,
    (prec = 'Ip')                                AS precipitacion_inapreciable,

    TRY_CAST(dir AS INT)                         AS direccion_viento_cod,
    TRY_CAST(REPLACE(velmedia, ',', '.') AS DOUBLE) AS viento_media_kmh,
    TRY_CAST(REPLACE(racha, ',', '.') AS DOUBLE)    AS viento_racha_kmh,

    TRY_CAST(REPLACE(presMax, ',', '.') AS DOUBLE)  AS presion_max_hpa,
    TRY_CAST(REPLACE(presMin, ',', '.') AS DOUBLE)  AS presion_min_hpa,

    TRY_CAST(hrMedia AS DOUBLE)                     AS humedad_media_pct,
    TRY_CAST(hrMax AS DOUBLE)                        AS humedad_max_pct,
    TRY_CAST(hrMin AS DOUBLE)                        AS humedad_min_pct,

    CASE WHEN horatmin RLIKE '^[0-9]{2}:[0-9]{2}$' THEN horatmin END AS hora_temp_min,
    CASE WHEN horatmax RLIKE '^[0-9]{2}:[0-9]{2}$' THEN horatmax END AS hora_temp_max,
    CASE WHEN horaracha RLIKE '^[0-9]{2}:[0-9]{2}$' THEN horaracha END AS hora_racha_max,
    current_date AS ingest_date

FROM bronze.aemet_dana_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Verificación
-- MAGIC
-- MAGIC Aquí lo más importante no es contar nulos (es un evento corto, pocos registros),
-- MAGIC sino confirmar que la precipitación extrema del episodio se refleja bien —
-- MAGIC Chiva registró un acumulado histórico esos días.

-- COMMAND ----------

SELECT fecha, nombre, precipitacion_mm, temp_min_c, temp_max_c
FROM silver.aemet_dana_clean
ORDER BY precipitacion_mm DESC
LIMIT 10;

-- COMMAND ----------

SELECT * FROM silver.aemet_dana_clean ORDER BY fecha;