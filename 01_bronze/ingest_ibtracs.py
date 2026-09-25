# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 01 · Bronze — Ingesta NOAA IBTrACS (ciclones tropicales globales)
# MAGIC
# MAGIC Fichero: `ibtracs.ALL.list.v04r01.csv` — ciclones tropicales desde 1980
# MAGIC (era de satélites), todas las cuencas oceánicas en un único fichero.
# MAGIC
# MAGIC **Mismo aviso que en OISST**: IBTrACS trae una fila de unidades justo debajo de la
# MAGIC cabecera. Aquí además el fichero es grande (~144 MB), así que la limpieza se hace
# MAGIC en Spark directamente en vez de colectar todo a memoria del driver.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "global_climate", "Catálogo")
dbutils.widgets.text("file_name", "ibtracs.ALL.list.v04r01.csv", "Nombre del fichero en el Volume")

catalog_name = dbutils.widgets.get("catalog_name")
file_name = dbutils.widgets.get("file_name")
volume_path = f"/Volumes/{catalog_name}/bronze/noaa_ibtracs/ibtracs.ALL.list.v04r01.csv"

print(f"Leyendo: {volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Lectura saltando la fila de unidades
# MAGIC
# MAGIC Leemos con header=True (Spark toma la fila 1 como nombres de columna), y luego
# MAGIC eliminamos la primera fila de datos, que en realidad es la fila de unidades
# MAGIC (se identifica porque la columna `SEASON` no es numérica en esa fila).

# COMMAND ----------

df_raw = spark.read.csv(volume_path, header=True, inferSchema=False)  # todo como string en bronze

print(f"Filas totales (incluyendo fila de unidades): {df_raw.count()}")
print(f"Columnas: {len(df_raw.columns)}")

# COMMAND ----------

from pyspark.sql.functions import col

# La fila de unidades tiene "SEASON" = "Year" (texto) en vez de un año numérico real
fila_unidades = df_raw.filter(col("SEASON") == "Year")
fila_unidades.select("SID", "SEASON", "BASIN", "NAME").show(truncate=False)

df_ibtracs = df_raw.filter(col("SEASON") != "Year")
print(f"Filas de datos reales tras eliminar la fila de unidades: {df_ibtracs.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Escritura como tabla Delta en bronze
# MAGIC
# MAGIC Todo se guarda como string (fiel al espíritu bronze: tal cual llega). El tipado
# MAGIC numérico de viento, presión, lat/lon, etc. se hará en la capa silver.

# COMMAND ----------

from pyspark.sql.functions import current_date

df_ibtracs = df_ibtracs.withColumn("ingest_date", current_date())

tabla = f"{catalog_name}.bronze.noaa_ibtracs_raw"
df_ibtracs.write.mode("overwrite").saveAsTable(tabla)

print(f"Tabla creada: {tabla} ({df_ibtracs.count()} registros, {len(df_ibtracs.columns)} columnas)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Documentación de columnas principales
# MAGIC
# MAGIC IBTrACS trae más de 150 columnas porque agrega los datos de varias agencias
# MAGIC meteorológicas nacionales (USA/NOAA, JTWC, agencias regionales...). Documentamos
# MAGIC las que realmente vas a usar en el análisis; el resto quedan sin comentar pero
# MAGIC siguen disponibles en la tabla.

# COMMAND ----------

comentarios = {
    "SID": "Identificador único de la tormenta en IBTrACS",
    "SEASON": "Año de la temporada del ciclón",
    "NUMBER": "Número de orden del ciclón dentro de su temporada y cuenca",
    "BASIN": "Cuenca oceánica: NA=Atlántico Norte, WP=Pacífico Oeste (tifones), EP=Pacífico Este, NI=Índico Norte, SI=Índico Sur, SP=Pacífico Sur, SA=Atlántico Sur",
    "SUBBASIN": "Subcuenca oceánica específica dentro de la cuenca principal",
    "NAME": "Nombre asignado al ciclón (o 'NOT_NAMED' si no se le dio nombre)",
    "ISO_TIME": "Fecha y hora UTC de la observación (formato ISO)",
    "NATURE": "Naturaleza del sistema: TS=tormenta tropical, ET=extratropical, DS=perturbación, etc.",
    "LAT": "Latitud del centro del ciclón en el momento de la observación",
    "LON": "Longitud del centro del ciclón en el momento de la observación",
    "WMO_WIND": "Velocidad máxima sostenida del viento según la agencia designada por la OMM (nudos)",
    "WMO_PRES": "Presión mínima central según la agencia designada por la OMM (hPa)",
    "USA_WIND": "Velocidad máxima sostenida del viento según NOAA/NHC (nudos)",
    "USA_PRES": "Presión mínima central según NOAA/NHC (hPa)",
    "USA_SSHS": "Categoría en la escala Saffir-Simpson (huracanes): -1 a 5",
    "DIST2LAND": "Distancia a la costa más cercana, en kilómetros",
    "LANDFALL": "Distancia a la costa en el momento de tocar tierra (landfall), en kilómetros",
}

for columna, descripcion in comentarios.items():
    try:
        spark.sql(f"ALTER TABLE {tabla} ALTER COLUMN `{columna}` COMMENT '{descripcion}'")
    except Exception as e:
        print(f"No se pudo comentar {columna}: {e}")

spark.sql(f"""
    COMMENT ON TABLE {tabla} IS
    'Ciclones tropicales globales (huracanes, tifones) desde 1980 (era de satélites), todas las cuencas oceánicas. Fuente: NOAA IBTrACS v04r01. Agrega datos de múltiples agencias meteorológicas nacionales; más de 150 columnas, documentadas aquí las de uso más frecuente.'
""")

print("Comentarios aplicados sobre las columnas principales.")
display(spark.sql(f"DESCRIBE TABLE {tabla}"))