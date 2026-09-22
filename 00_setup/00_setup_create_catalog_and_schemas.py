# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup — Catálogo, esquemas y volumes
# MAGIC
# MAGIC **Proyecto:** Análisis de cambio climático (temperatura global + fenómenos atmosféricos extremos)
# MAGIC
# MAGIC Este notebook crea la estructura base en Unity Catalog:
# MAGIC - 1 catálogo: `global_climate`
# MAGIC - 4 esquemas: `bronze`, `silver`, `gold`, `ml`
# MAGIC - 3 volumes gestionados dentro de `bronze`, uno por fuente de datos
# MAGIC
# MAGIC Ejecútalo una sola vez al arrancar el proyecto. Es idempotente (usa `IF NOT EXISTS` en todo).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Parámetros
# MAGIC
# MAGIC Usamos un widget para el nombre del catálogo por si en el futuro quieres clonar
# MAGIC esta estructura para otro proyecto sin tocar el código.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "global_climate", "Nombre del catálogo")
catalog_name = dbutils.widgets.get("catalog_name")
print(f"Catálogo a crear/usar: {catalog_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Catálogo

# COMMAND ----------

spark.sql(f"""
CREATE CATALOG IF NOT EXISTS {catalog_name}
COMMENT 'Portfolio: análisis de cambio climático y fenómenos atmosféricos extremos'
""")

spark.sql(f"USE CATALOG {catalog_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Esquemas (capas medallion + ML)
# MAGIC
# MAGIC | Esquema  | Contenido |
# MAGIC |----------|-----------|
# MAGIC | `bronze` | Datos crudos tal cual llegan de las fuentes (NASA, NOAA, AEMET) |
# MAGIC | `silver` | Datos limpios, tipados, deduplicados, con feature engineering |
# MAGIC | `gold`   | Agregados listos para BI / dashboards / el agente |
# MAGIC | `ml`     | Feature tables, experimentos MLflow y modelos registrados |

# COMMAND ----------

schemas = {
    "bronze": "Datos crudos tal cual llegan de las fuentes (NASA, NOAA, AEMET)",
    "silver": "Datos limpios, tipados y con feature engineering",
    "gold": "Agregados listos para BI, dashboards y el agente",
    "ml": "Feature tables, experimentos y modelos registrados",
}

for schema, comment in schemas.items():
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema}
        COMMENT '{comment}'
    """)
    print(f"✔ Esquema listo: {catalog_name}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Volumes en `bronze`
# MAGIC
# MAGIC Un volumen gestionado por fuente de datos. Dentro de cada uno organizaremos
# MAGIC las cargas por fecha de ingesta (`ingest_date=YYYY-MM-DD/`) cuando subamos los ficheros:
# MAGIC
# MAGIC - `nasa_gistemp` → anomalías de temperatura terrestre/oceánica (NASA GISS)
# MAGIC - `noaa_ibtracs` → ciclones tropicales globales (tifones, huracanes)
# MAGIC - `aemet_dana`   → datos de la DANA de Valencia (octubre 2024) vía AEMET OpenData

# COMMAND ----------

volumes = {
    "nasa_gistemp": "Anomalías de temperatura terrestre y oceánica — NASA GISTEMP",
    "noaa_ibtracs": "Ciclones tropicales globales (tifones, huracanes) — NOAA IBTrACS",
    "aemet_dana": "Datos meteorológicos de la DANA de Valencia (oct. 2024) — AEMET OpenData",
}

for volume, comment in volumes.items():
    spark.sql(f"""
        CREATE VOLUME IF NOT EXISTS {catalog_name}.bronze.{volume}
        COMMENT '{comment}'
    """)
    print(f"✔ Volume listo: /Volumes/{catalog_name}/bronze/{volume}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verificación
# MAGIC
# MAGIC Confirmamos que todo se ha creado correctamente antes de pasar a la ingesta.

# COMMAND ----------

print("=== Esquemas en el catálogo ===")
display(spark.sql(f"SHOW SCHEMAS IN {catalog_name}"))

# COMMAND ----------

print("=== Volumes en bronze ===")
display(spark.sql(f"SHOW VOLUMES IN {catalog_name}.bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Siguiente paso
# MAGIC
# MAGIC Con el catálogo, esquemas y volumes creados, el siguiente notebook (`01_bronze/ingest_nasa_gistemp.py`)
# MAGIC se encargará de subir y catalogar el primer dataset (temperatura global NASA GISTEMP)
# MAGIC como tabla Delta en `global_climate.bronze`.