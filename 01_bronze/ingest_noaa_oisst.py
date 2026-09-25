# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 01 · Bronze — Ingesta NOAA OISST (temperatura superficial del mar)
# MAGIC
# MAGIC El CSV viene de un subset de ERDDAP (región Mediterráneo/España, ventana temporal reducida).
# MAGIC **Aviso importante**: ERDDAP escribe la fila de unidades justo debajo de la cabecera
# MAGIC (fila 2 = "UTC", "degrees_north", "degree_C"...), así que la saltamos explícitamente
# MAGIC para que no se cuele como si fuera un registro de datos.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "global_climate", "Catálogo")
dbutils.widgets.text("file_name", "erddap_oisst_spain.csv", "Nombre del fichero en el Volume")

catalog_name = dbutils.widgets.get("catalog_name")
file_name = dbutils.widgets.get("file_name")
volume_path = f"/Volumes/{catalog_name}/bronze/noaa_oisst/{file_name}"

print(f"Leyendo: {volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Lectura saltando la fila de unidades
# MAGIC
# MAGIC Leemos primero como texto plano para eliminar la línea 2 (unidades), y luego
# MAGIC reconstruimos el CSV limpio en memoria para que Spark lo parsee bien tipado.

# COMMAND ----------

try:
    dbutils.fs.ls(volume_path)
except Exception:
    csv_files = [f.name for f in dbutils.fs.ls(f"/Volumes/{catalog_name}/bronze/noaa_oisst/") if f.name.endswith(".csv")]
    if csv_files:
        file_name = csv_files[0]
        volume_path = f"/Volumes/{catalog_name}/bronze/noaa_oisst/{file_name}"
        print(f"Fichero no encontrado, usando: {file_name}")
    else:
        raise FileNotFoundError(f"No se encontró ningún CSV en /Volumes/{catalog_name}/bronze/noaa_oisst/")

df_oisst = spark.read.csv(volume_path, header=True, inferSchema=True)
# La fila 2 del CSV original contiene unidades ("UTC", "degrees_north", ...)
# y se cuela como primer registro de datos — la filtramos
df_oisst = df_oisst.filter(df_oisst["time"] != "UTC")

print(f"Filas leídas: {df_oisst.count()}")
df_oisst.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Escritura como tabla Delta en bronze

# COMMAND ----------

from pyspark.sql.functions import current_date

df_oisst = df_oisst.withColumn("ingest_date", current_date())

tabla = f"{catalog_name}.bronze.noaa_oisst_raw"
df_oisst.write.mode("overwrite").saveAsTable(tabla)

print(f"Tabla creada: {tabla}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Documentación de columnas
# MAGIC
# MAGIC A diferencia de AEMET, ERDDAP no ofrece un endpoint de metadatos por API en este caso,
# MAGIC así que documentamos a mano — son solo 7 campos, bien conocidos y estables.

# COMMAND ----------

comentarios = {
    "time": "Fecha y hora UTC de la medición (resolución diaria, ancla a las 12:00 UTC)",
    "zlev": "Profundidad de la medición en metros (0.0 = superficie del mar)",
    "latitude": "Latitud del punto de rejilla, en grados norte",
    "longitude": "Longitud del punto de rejilla, en grados este",
    "sst": "Temperatura superficial del mar (Sea Surface Temperature), en grados Celsius",
    "anom": "Anomalía de temperatura respecto a la media climatológica de referencia, en grados Celsius",
    "err": "Margen de error estimado de la medición, en grados Celsius",
}

for columna, descripcion in comentarios.items():
    try:
        spark.sql(f"ALTER TABLE {tabla} ALTER COLUMN `{columna}` COMMENT '{descripcion}'")
    except Exception as e:
        print(f"No se pudo comentar {columna}: {e}")

spark.sql(f"""
    COMMENT ON TABLE {tabla} IS
    'Temperatura superficial del mar (SST) de la región Mediterráneo/España, subset de NOAA OISST v2.1 vía ERDDAP. Resolución diaria, rejilla de 0.25 grados.'
""")

print("Comentarios aplicados.")
display(spark.sql(f"DESCRIBE TABLE {tabla}"))