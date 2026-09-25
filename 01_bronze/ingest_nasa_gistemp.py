# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 01 · Bronze — Ingesta NASA GISTEMP (temperatura global tierra + océano)
# MAGIC
# MAGIC Fichero esperado: `GLB.Ts+dSST.csv` — Índice de temperatura Land-Ocean, mensual y
# MAGIC estacional, desde 1880. Anomalías respecto al periodo base 1951-1980, en grados Celsius.
# MAGIC
# MAGIC **Peculiaridades del fichero** que este notebook resuelve:
# MAGIC 1. Hay una fila de metadatos/título antes de la cabecera real
# MAGIC 2. Los valores ausentes vienen como `***`, no vacíos ni `NA`
# MAGIC 3. Debajo de la tabla principal hay una segunda tabla pegada — la descartamos

# COMMAND ----------

dbutils.widgets.text("catalog_name", "global_climate", "Catálogo")
dbutils.widgets.text("file_name", "GLB.Ts+dSST.csv", "Nombre del fichero en el Volume")

catalog_name = dbutils.widgets.get("catalog_name")
file_name = dbutils.widgets.get("file_name")
volume_path = f"/Volumes/{catalog_name}/bronze/nasa_gistemp/{file_name}"

print(f"Leyendo: {volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Localizar la cabecera real y aislar solo la tabla principal

# COMMAND ----------

raw_lines = [row.value for row in spark.read.text(volume_path).collect()]

# Buscamos la línea que empieza por "Year" -> esa es la cabecera real de la tabla
header_idx = next(i for i, line in enumerate(raw_lines) if line.strip().startswith("Year"))
header = raw_lines[header_idx]

# A partir de ahí, cogemos filas de datos hasta la primera línea vacía o que no empiece por un año de 4 dígitos
data_lines = []
for line in raw_lines[header_idx + 1:]:
    primer_campo = line.split(",")[0].strip()
    if not primer_campo.isdigit() or len(primer_campo) != 4:
        break  # fin de la tabla principal (línea vacía o inicio de la segunda tabla)
    data_lines.append(line)

print(f"Cabecera encontrada en la línea {header_idx}: {header}")
print(f"Filas de datos válidas encontradas: {len(data_lines)}")
print(f"Primera fila: {data_lines[0]}")
print(f"Última fila: {data_lines[-1]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Construir el CSV limpio y leerlo con Spark
# MAGIC
# MAGIC `***` se trata como valor nulo (`nullValue`), no como texto.

# COMMAND ----------

from pyspark.sql import Row
import csv

column_names = header.split(",")
parsed_rows = []
for line in data_lines:
    values = next(csv.reader([line]))
    row = {col: (None if val == "***" else val) for col, val in zip(column_names, values)}
    parsed_rows.append(Row(**row))

df_gistemp = spark.createDataFrame(parsed_rows)

print(f"Filas: {df_gistemp.count()} | Columnas: {df_gistemp.columns}")
df_gistemp.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Escritura como tabla Delta en bronze

# COMMAND ----------

from pyspark.sql.functions import current_date

df_gistemp = df_gistemp.withColumn("ingest_date", current_date())

tabla = f"{catalog_name}.bronze.nasa_gistemp_raw"
df_gistemp.write.mode("overwrite").saveAsTable(tabla)

print(f"Tabla creada: {tabla}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Documentación de columnas

# COMMAND ----------

meses = {
    "Jan": "Enero", "Feb": "Febrero", "Mar": "Marzo", "Apr": "Abril",
    "May": "Mayo", "Jun": "Junio", "Jul": "Julio", "Aug": "Agosto",
    "Sep": "Septiembre", "Oct": "Octubre", "Nov": "Noviembre", "Dec": "Diciembre",
}

comentarios = {"Year": "Año de la observación"}
for abbr, nombre in meses.items():
    comentarios[abbr] = f"Anomalía de temperatura global de {nombre}, respecto al periodo base 1951-1980 (°C)"

comentarios.update({
    "J-D": "Media anual (enero-diciembre) de la anomalía de temperatura (°C)",
    "D-N": "Media anual (diciembre-noviembre, año climático) de la anomalía de temperatura (°C)",
    "DJF": "Media estacional de invierno boreal (diciembre-enero-febrero) (°C)",
    "MAM": "Media estacional de primavera boreal (marzo-abril-mayo) (°C)",
    "JJA": "Media estacional de verano boreal (junio-julio-agosto) (°C)",
    "SON": "Media estacional de otoño boreal (septiembre-octubre-noviembre) (°C)",
})

for columna, descripcion in comentarios.items():
    if columna in df_gistemp.columns:
        try:
            spark.sql(f"ALTER TABLE {tabla} ALTER COLUMN `{columna}` COMMENT '{descripcion}'")
        except Exception as e:
            print(f"No se pudo comentar {columna}: {e}")

spark.sql(f"""
    COMMENT ON TABLE {tabla} IS
    'Índice de temperatura global Land-Ocean (tierra + océano combinados), anomalías mensuales y estacionales respecto al periodo base 1951-1980, desde 1880. Fuente: NASA GISS GISTEMP v4.'
""")

print("Comentarios aplicados.")
display(spark.sql(f"DESCRIBE TABLE {tabla}"))