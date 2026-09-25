-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03 · Gold — Agregados para dashboards
-- MAGIC
-- MAGIC Cuatro tablas, cada una pensada para un uso concreto:
-- MAGIC 1. `temp_global_mensual` — serie mensual larga (para gráficos de línea temporal)
-- MAGIC 2. `resumen_anual` — una fila por año cruzando las 4 fuentes (para el dashboard exploratorio)
-- MAGIC 3. `ciclones_por_cuenca_anual` — conteo de tormentas por año y cuenca, ya a nivel de tormenta (no de observación)
-- MAGIC 4. `dana_vs_normal` — comparativa del episodio DANA contra la climatología histórica de octubre (el "número contundente" para storytelling)

-- COMMAND ----------

USE CATALOG global_climate;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Temperatura global — de formato ancho a formato largo
-- MAGIC
-- MAGIC `STACK` convierte las 12 columnas de mes en 12 filas por año — necesario para
-- MAGIC poder dibujar una serie temporal mensual continua en el dashboard.

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.temp_global_mensual
COMMENT 'Anomalía de temperatura global en formato largo (una fila por año-mes), lista para series temporales.'
AS
SELECT anio, mes, anomalia_c
FROM silver.nasa_gistemp_clean
LATERAL VIEW STACK(12,
    'Ene', anomalia_ene, 'Feb', anomalia_feb, 'Mar', anomalia_mar, 'Abr', anomalia_abr,
    'May', anomalia_may, 'Jun', anomalia_jun, 'Jul', anomalia_jul, 'Ago', anomalia_ago,
    'Sep', anomalia_sep, 'Oct', anomalia_oct, 'Nov', anomalia_nov, 'Dic', anomalia_dic
) AS mes_anom(mes, anomalia_c)
WHERE anomalia_c IS NOT NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Ciclones por cuenca y año — agregado a nivel de tormenta
-- MAGIC
-- MAGIC IBTrACS trae una fila por observación (cada 6h), no por tormenta. Primero
-- MAGIC colapsamos a una fila por tormenta (con su categoría máxima alcanzada), y
-- MAGIC luego contamos — si no, "número de tormentas intensas" saldría inflado por
-- MAGIC contar observaciones en vez de eventos.

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.ciclones_por_cuenca_anual
COMMENT 'Número de tormentas (no observaciones) por año y cuenca oceánica, con recuento de las que alcanzaron categoría intensa (Saffir-Simpson >= 3).'
AS
WITH tormenta_resumen AS (
    SELECT
        id_tormenta,
        temporada,
        cuenca_desc,
        MAX(categoria_saffir_simpson) AS categoria_maxima
    FROM silver.noaa_ibtracs_clean
    GROUP BY id_tormenta, temporada, cuenca_desc
)
SELECT
    temporada AS anio,
    cuenca_desc,
    COUNT(*)                                                        AS num_tormentas,
    SUM(CASE WHEN categoria_maxima >= 3 THEN 1 ELSE 0 END)          AS num_tormentas_intensas
FROM tormenta_resumen
GROUP BY temporada, cuenca_desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Resumen anual cruzado — la tabla central del dashboard exploratorio
-- MAGIC
-- MAGIC Una fila por año, con una métrica de cada fuente. Tendrá huecos (NULL) fuera
-- MAGIC del rango de cada fuente — es normal y esperado, no un error: GISTEMP llega a
-- MAGIC 1880, pero OISST y AEMET solo cubren los últimos ~15 años.

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.resumen_anual
COMMENT 'Una fila por año cruzando temperatura global, temperatura del mar en España, actividad ciclónica global y clima de España. Tendrá NULLs fuera del rango de cada fuente.'
AS
WITH temp_global AS (
    SELECT anio, anomalia_media_anual AS anomalia_temp_global_c
    FROM silver.nasa_gistemp_clean
),
sst_espana AS (
    SELECT anio, ROUND(AVG(temp_superficial_c), 2) AS sst_mediterraneo_c
    FROM silver.noaa_oisst_clean
    GROUP BY anio
),
ciclones AS (
    SELECT
        id_tormenta,
        temporada,
        MAX(categoria_saffir_simpson) AS categoria_maxima
    FROM silver.noaa_ibtracs_clean
    GROUP BY id_tormenta, temporada
),
ciclones_anual AS (
    SELECT
        temporada AS anio,
        COUNT(*)                                               AS num_ciclones_global,
        SUM(CASE WHEN categoria_maxima >= 3 THEN 1 ELSE 0 END) AS num_ciclones_intensos_global
    FROM ciclones
    GROUP BY temporada
),
clima_espana AS (
    SELECT
        YEAR(fecha)                          AS anio,
        ROUND(AVG(temp_media_c), 2)          AS temp_media_espana_c,
        ROUND(SUM(precipitacion_mm), 1)      AS precipitacion_total_espana_mm
    FROM silver.aemet_climatologia_clean
    GROUP BY YEAR(fecha)
)
SELECT
    COALESCE(tg.anio, se.anio, ca.anio, ce.anio) AS anio,
    tg.anomalia_temp_global_c,
    se.sst_mediterraneo_c,
    ca.num_ciclones_global,
    ca.num_ciclones_intensos_global,
    ce.temp_media_espana_c,
    ce.precipitacion_total_espana_mm
FROM temp_global tg
FULL OUTER JOIN sst_espana    se ON tg.anio = se.anio
FULL OUTER JOIN ciclones_anual ca ON COALESCE(tg.anio, se.anio) = ca.anio
FULL OUTER JOIN clima_espana   ce ON COALESCE(tg.anio, se.anio, ca.anio) = ce.anio
ORDER BY anio;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. DANA vs. normal histórico — el número contundente para storytelling
-- MAGIC
-- MAGIC Compara la precipitación real de la DANA contra la media histórica de octubre
-- MAGIC en las mismas estaciones, calculando cuántas veces por encima de lo normal estuvo.

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.dana_vs_normal
COMMENT 'Comparativa de la precipitación del episodio DANA (28 oct - 5 nov 2024) contra la media histórica de octubre en las mismas estaciones.'
AS
WITH normal_historico AS (
    SELECT
        indicativo,
        nombre,
        ROUND(AVG(precipitacion_mm), 2) AS precipitacion_media_diaria_oct_mm
    FROM silver.aemet_climatologia_clean
    WHERE MONTH(fecha) = 10
    GROUP BY indicativo, nombre
),
evento_dana AS (
    SELECT
        indicativo,
        nombre,
        ROUND(SUM(precipitacion_mm), 1) AS precipitacion_total_dana_mm,
        ROUND(MAX(precipitacion_mm), 1) AS precipitacion_maxima_un_dia_mm
    FROM silver.aemet_dana_clean
    GROUP BY indicativo, nombre
)
SELECT
    e.nombre,
    e.precipitacion_total_dana_mm,
    e.precipitacion_maxima_un_dia_mm,
    n.precipitacion_media_diaria_oct_mm,
    ROUND(e.precipitacion_maxima_un_dia_mm / NULLIF(n.precipitacion_media_diaria_oct_mm, 0), 1) AS veces_sobre_lo_normal
FROM evento_dana e
JOIN normal_historico n ON e.indicativo = n.indicativo
ORDER BY veces_sobre_lo_normal DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Verificación

-- COMMAND ----------

SELECT * FROM gold.resumen_anual WHERE anio >= 2015 ORDER BY anio;

-- COMMAND ----------

SELECT * FROM gold.dana_vs_normal;

-- COMMAND ----------

SELECT * FROM gold.ciclones_por_cuenca_anual WHERE anio >= 2015 ORDER BY anio, cuenca_desc;
