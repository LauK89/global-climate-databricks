# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 01b · Bronze — Documentar campos AEMET en Unity Catalog
# MAGIC
# MAGIC AEMET devuelve, junto a cada respuesta, una URL de `metadatos` con la descripción
# MAGIC oficial de cada campo (id, descripción, unidad). Este notebook la recupera una sola
# MAGIC vez y la aplica como `COMMENT` de columna sobre las tablas bronze de AEMET, para que
# MAGIC cualquiera que abra la tabla en el Catalog Explorer vea qué significa cada campo
# MAGIC sin tener que consultar la documentación externa.

# COMMAND ----------

import requests

dbutils.widgets.text("catalog_name", "global_climate", "Catálogo")
catalog_name = dbutils.widgets.get("catalog_name")

API_BASE = "https://opendata.aemet.es/opendata/api"
API_KEY = dbutils.secrets.get(scope="aemet", key="api_key")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Recuperar los metadatos oficiales
# MAGIC
# MAGIC Basta una llamada con cualquier estación y un rango corto de fechas — el esquema
# MAGIC de campos es el mismo siempre, no hace falta repetirlo por estación.

# COMMAND ----------

import time

# Usamos una estación cualquiera de la tabla de climatología ya cargada, y un rango de 2 días
estacion_ejemplo = spark.sql(
    f"SELECT indicativo FROM {catalog_name}.bronze.aemet_climatologia_raw LIMIT 1"
).collect()[0]["indicativo"]

endpoint = (
    "valores/climatologicos/diarios/datos/"
    "fechaini/2024-01-01T00:00:00UTC/fechafin/2024-01-02T23:59:59UTC/"
    f"estacion/{estacion_ejemplo}"
)

metadatos = None
for intento in range(3):
    resp = requests.get(f"{API_BASE}/{endpoint}", headers={"api_key": API_KEY, "cache-control": "no-cache"})
    payload = resp.json()
    if payload.get("estado") == 200:
        metadatos_resp = requests.get(payload["metadatos"])
        metadatos = metadatos_resp.json()
        break
    time.sleep(5)

if metadatos is None:
    print(f"AEMET devolvió estado {payload.get('estado')}: {payload.get('descripcion')} — usando metadatos de respaldo")
    metadatos = {"campos": [
        {"id": "fecha", "descripcion": "Fecha del dato (AAAA-MM-DD)"},
        {"id": "indicativo", "descripcion": "Indicativo de la estación"},
        {"id": "nombre", "descripcion": "Nombre de la estación"},
        {"id": "provincia", "descripcion": "Provincia de la estación"},
        {"id": "altitud", "descripcion": "Altitud de la estación", "unidad": "m"},
        {"id": "tmed", "descripcion": "Temperatura media diaria", "unidad": "ºC"},
        {"id": "prec", "descripcion": "Precipitación diaria", "unidad": "mm"},
        {"id": "tmin", "descripcion": "Temperatura mínima del día", "unidad": "ºC"},
        {"id": "tmax", "descripcion": "Temperatura máxima del día", "unidad": "ºC"},
        {"id": "dir", "descripcion": "Dirección del viento", "unidad": "decenas de grado"},
        {"id": "velmedia", "descripcion": "Velocidad media del viento", "unidad": "m/s"},
        {"id": "racha", "descripcion": "Racha máxima de viento", "unidad": "m/s"},
        {"id": "horaracha", "descripcion": "Hora y minuto de la racha máxima (UTC)"},
        {"id": "horatmax", "descripcion": "Hora de la temperatura máxima (UTC)"},
        {"id": "horatmin", "descripcion": "Hora de la temperatura mínima (UTC)"},
        {"id": "hrMax", "descripcion": "Humedad relativa máxima", "unidad": "%"},
        {"id": "hrMedia", "descripcion": "Humedad relativa media", "unidad": "%"},
        {"id": "hrMin", "descripcion": "Humedad relativa mínima", "unidad": "%"},
        {"id": "horaHrMax", "descripcion": "Hora de la humedad relativa máxima (UTC)"},
        {"id": "horaHrMin", "descripcion": "Hora de la humedad relativa mínima (UTC)"},
        {"id": "presMax", "descripcion": "Presión máxima al nivel de la estación", "unidad": "hPa"},
        {"id": "presMin", "descripcion": "Presión mínima al nivel de la estación", "unidad": "hPa"},
        {"id": "horaPresMax", "descripcion": "Hora de la presión máxima (UTC)"},
        {"id": "horaPresMin", "descripcion": "Hora de la presión mínima (UTC)"},
        {"id": "pintMax", "descripcion": "Precipitación máxima en 1 minuto", "unidad": "mm"},
        {"id": "horaPIntMax", "descripcion": "Hora de la precipitación máxima en 1 minuto (UTC)"},
        {"id": "sol", "descripcion": "Insolación durante el día", "unidad": "horas"},
    ]}

print(f"Campos documentados por AEMET: {len(metadatos['campos'])}")
for campo in metadatos["campos"]:
    unidad = f" ({campo['unidad']})" if campo.get("unidad") else ""
    print(f"  {campo['id']}: {campo['descripcion']}{unidad}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Aplicar los comentarios sobre la tabla bronze
# MAGIC
# MAGIC Recorremos el diccionario de campos y comentamos cada columna que exista en la tabla.
# MAGIC Si algún campo del diccionario no está en la tabla (o viceversa), simplemente se omite
# MAGIC y se avisa por consola — no interrumpe el proceso.

# COMMAND ----------

tabla = f"{catalog_name}.bronze.aemet_climatologia_raw"
columnas_tabla = {c.name for c in spark.table(tabla).schema}

comentados, omitidos = 0, []

for campo in metadatos["campos"]:
    col_id = campo["id"]
    if col_id not in columnas_tabla:
        omitidos.append(col_id)
        continue

    descripcion = campo["descripcion"].replace("'", "\\'")
    if campo.get("unidad"):
        descripcion += f" (unidad: {campo['unidad']})"

    spark.sql(f"ALTER TABLE {tabla} ALTER COLUMN `{col_id}` COMMENT '{descripcion}'")
    comentados += 1

print(f"Columnas comentadas: {comentados}")
if omitidos:
    print(f"Campos del diccionario no encontrados en la tabla (omitidos): {omitidos}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificación
# MAGIC
# MAGIC `DESCRIBE TABLE` muestra el comentario junto a cada columna.

# COMMAND ----------

display(spark.sql(f"DESCRIBE TABLE {tabla}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. También a nivel de tabla
# MAGIC
# MAGIC De paso, documentamos la tabla en sí (no solo las columnas) para que el propósito
# MAGIC quede claro en el Catalog Explorer de un vistazo.

# COMMAND ----------

spark.sql(f"""
    COMMENT ON TABLE {tabla} IS
    'Datos climatológicos diarios (temperatura, precipitación, viento, presión, humedad) de estaciones representativas de España. Fuente: AEMET OpenData API. Diccionario de campos oficial recuperado vía el endpoint de metadatos de AEMET.'
""")

print(f"Tabla {tabla} documentada correctamente.")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE global_climate.bronze.aemet_climatologia_raw