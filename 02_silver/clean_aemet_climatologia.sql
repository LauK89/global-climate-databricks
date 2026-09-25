-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02 · Silver — Limpieza y tipado de climatología AEMET
-- MAGIC
-- MAGIC Transforma `bronze.aemet_climatologia_raw` (todo en string, tal cual llega de la API)
-- MAGIC en una tabla silver correctamente tipada.
-- MAGIC
-- MAGIC **Retos de formato que resolvemos aquí:**
-- MAGIC 1. Los decimales vienen con coma española (`"18,4"`) → hay que convertir a punto antes de castear a `DOUBLE`
-- MAGIC 2. La precipitación usa el código `"Ip"` (inapreciable, <0.1mm) → lo tratamos como `0.0` para cálculos estadísticos, pero guardamos un flag aparte para no perder el matiz
-- MAGIC 3. Los campos de hora a veces traen `"Varias"` en vez de una hora real → los limpiamos a `NULL` cuando no siguen el formato `HH:MM`
-- MAGIC
-- MAGIC Usamos `TRY_CAST` en vez de `CAST` en los campos numéricos: si un valor no se puede
-- MAGIC convertir (dato corrupto o inesperado), devuelve `NULL` en vez de romper todo el proceso.

-- COMMAND ----------

USE CATALOG global_climate;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Vistazo rápido a los datos crudos antes de transformar
-- MAGIC
-- MAGIC Útil para confirmar visualmente el formato exacto antes de escribir las reglas de limpieza.

-- COMMAND ----------

SELECT fecha, indicativo, tmed, prec, tmin, horatmin, tmax, horatmax
FROM bronze.aemet_climatologia_raw
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creación de la tabla silver (patrón CTAS)

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.aemet_climatologia_clean
COMMENT 'Climatología diaria de AEMET, limpia y tipada: decimales con punto, "Ip" normalizado a 0 con flag de precipitación inapreciable, horas validadas.'
AS
SELECT
    -- Identificación y localización
    CAST(fecha AS DATE)                        AS fecha,
    indicativo,
    nombre,
    provincia,
    ciudad_objetivo,
    TRY_CAST(altitud AS INT)                   AS altitud_m,

    -- Temperatura: coma -> punto, luego a DOUBLE
    TRY_CAST(REPLACE(tmed, ',', '.') AS DOUBLE) AS temp_media_c,
    TRY_CAST(REPLACE(tmin, ',', '.') AS DOUBLE) AS temp_min_c,
    TRY_CAST(REPLACE(tmax, ',', '.') AS DOUBLE) AS temp_max_c,

    -- Precipitación: "Ip" -> 0.0, resto coma -> punto -> DOUBLE
    CASE
        WHEN prec = 'Ip' THEN 0.0
        WHEN prec IS NULL THEN NULL
        ELSE TRY_CAST(REPLACE(prec, ',', '.') AS DOUBLE)
    END                                          AS precipitacion_mm,
    (prec = 'Ip')                                AS precipitacion_inapreciable,

    -- Viento
    TRY_CAST(dir AS INT)                         AS direccion_viento_cod,
    TRY_CAST(REPLACE(velmedia, ',', '.') AS DOUBLE) AS viento_media_kmh,
    TRY_CAST(REPLACE(racha, ',', '.') AS DOUBLE)    AS viento_racha_kmh,

    -- Presión atmosférica
    TRY_CAST(REPLACE(presMax, ',', '.') AS DOUBLE)  AS presion_max_hpa,
    TRY_CAST(REPLACE(presMin, ',', '.') AS DOUBLE)  AS presion_min_hpa,

    -- Humedad relativa
    TRY_CAST(hrMedia AS DOUBLE)                     AS humedad_media_pct,
    TRY_CAST(hrMax AS DOUBLE)                        AS humedad_max_pct,
    TRY_CAST(hrMin AS DOUBLE)                        AS humedad_min_pct,

    -- Horas de los extremos: solo si tienen formato HH:MM válido, si no NULL (limpia el caso "Varias")
    CASE WHEN horatmin RLIKE '^[0-9]{2}:[0-9]{2}$' THEN horatmin END AS hora_temp_min,
    CASE WHEN horatmax RLIKE '^[0-9]{2}:[0-9]{2}$' THEN horatmax END AS hora_temp_max,
    CASE WHEN horaracha RLIKE '^[0-9]{2}:[0-9]{2}$' THEN horaracha END AS hora_racha_max,
    CASE WHEN horaHrMax RLIKE '^[0-9]{2}:[0-9]{2}$' THEN horaHrMax END AS hora_humedad_max,
    CASE WHEN horaHrMin RLIKE '^[0-9]{2}:[0-9]{2}$' THEN horaHrMin END AS hora_humedad_min, 
    current_date as fecha_proceso_datos

FROM bronze.aemet_climatologia_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Documentación de columnas en la tabla silver
-- MAGIC
-- MAGIC Igual que en bronze, comentamos las columnas — pero ahora con nombres ya en español
-- MAGIC claro, así que los comentarios pueden ser más breves.

-- COMMAND ----------

ALTER TABLE silver.aemet_climatologia_clean ALTER COLUMN temp_media_c COMMENT 'Temperatura media diaria (°C)';
ALTER TABLE silver.aemet_climatologia_clean ALTER COLUMN precipitacion_mm COMMENT 'Precipitación diaria en mm. Inapreciable (<0.1mm) normalizado a 0.0 — ver columna precipitacion_inapreciable para distinguir de un día sin lluvia';
ALTER TABLE silver.aemet_climatologia_clean ALTER COLUMN precipitacion_inapreciable COMMENT 'TRUE si el día tuvo precipitación inapreciable (<0.1mm) según AEMET, en vez de un día completamente seco';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Verificación
-- MAGIC
-- MAGIC Tres comprobaciones clave: que no haya explosión de nulos por el TRY_CAST (dato corrupto
-- MAGIC generalizado), que las horas se hayan limpiado bien, y una muestra final del resultado.

-- COMMAND ----------

-- ¿Cuántos valores quedaron nulos tras el cast? Si algún campo tiene MUCHOS nulos, revisar por qué
SELECT
    COUNT(*)                                              AS total_filas,
    COUNT(*) - COUNT(temp_media_c)                        AS nulos_temp_media,
    COUNT(*) - COUNT(precipitacion_mm)                    AS nulos_precipitacion,
    SUM(CASE WHEN precipitacion_inapreciable THEN 1 ELSE 0 END) AS dias_inapreciable
FROM silver.aemet_climatologia_clean;

-- COMMAND ----------

-- Confirmar que "Varias" y similares se limpiaron a NULL en las horas
SELECT DISTINCT hora_humedad_min
FROM silver.aemet_climatologia_clean
WHERE hora_humedad_min IS NULL
LIMIT 5;

-- COMMAND ----------

SELECT * FROM silver.aemet_climatologia_clean LIMIT 10;

-- COMMAND ----------

SELECT fecha, indicativo, precipitacion_mm, precipitacion_inapreciable
FROM silver.aemet_climatologia_clean
WHERE precipitacion_inapreciable = false and precipitacion_mm > 0
LIMIT 10;