# Databricks notebook source
from pyspark.sql import functions as F

df_mart = spark.table("energy_platform.gold.mart_energia_clima_diario")

df_dim_fecha = (
    df_mart
    .select("fecha")
    .dropDuplicates(["fecha"])
    .withColumn("fecha_id", F.date_format(F.col("fecha"), "yyyyMMdd").cast("int"))
    .withColumn("anio", F.year("fecha"))
    .withColumn("mes", F.month("fecha"))
    .withColumn("dia", F.dayofmonth("fecha"))
    .withColumn("trimestre", F.quarter("fecha"))
    .withColumn("semana_anio", F.weekofyear("fecha"))
    .withColumn("dia_semana_num", F.dayofweek("fecha"))
    .withColumn(
        "es_fin_de_semana",
        F.col("dia_semana_num").isin(1, 7)
    )
    .select(
        "fecha_id",
        "fecha",
        "anio",
        "mes",
        "dia",
        "trimestre",
        "semana_anio",
        "dia_semana_num",
        "es_fin_de_semana"
    )
    .orderBy("fecha")
)

display(df_dim_fecha)

# COMMAND ----------

df_dim_fecha.write.mode("overwrite").saveAsTable(
    "energy_platform.gold.dim_fecha"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM energy_platform.gold.dim_fecha
# MAGIC ORDER BY fecha;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT fecha, COUNT(*) AS filas
# MAGIC FROM energy_platform.gold.mart_energia_clima_diario
# MAGIC GROUP BY fecha
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT m.fecha
# MAGIC FROM energy_platform.gold.mart_energia_clima_diario m
# MAGIC LEFT JOIN energy_platform.gold.dim_fecha d
# MAGIC   ON m.fecha = d.fecha
# MAGIC WHERE d.fecha IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT_IF(generacion_pura_mwh IS NULL) AS generacion_pura_nulls,
# MAGIC   COUNT_IF(generacion_renovable_mwh IS NULL) AS generacion_renovable_nulls,
# MAGIC   COUNT_IF(temperatura_media_espana_c IS NULL) AS temperatura_media_nulls,
# MAGIC   COUNT_IF(num_estaciones IS NULL) AS num_estaciones_nulls
# MAGIC FROM energy_platform.gold.mart_energia_clima_diario;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   MIN(porcentaje_renovable) AS min_porcentaje_renovable,
# MAGIC   MAX(porcentaje_renovable) AS max_porcentaje_renovable
# MAGIC FROM energy_platform.gold.mart_energia_clima_diario;