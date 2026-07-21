# Databricks notebook source
df_bronze_aemet = spark.table("energy_platform.bronze.aemet_climatologia_diaria")

display(df_bronze_aemet)

# COMMAND ----------

df_bronze_aemet.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

df_bronze_aemet = spark.table("energy_platform.bronze.aemet_climatologia_diaria")

df_dim_estacion_meteorologica = (
    df_bronze_aemet
    .select(
        F.col("indicativo").alias("estacion_id"),
        F.col("nombre").alias("estacion_nombre"),
        F.col("provincia"),
        F.col("altitud").cast("int").alias("altitud_metros")
    )
    .dropDuplicates(["estacion_id"])
)

display(df_dim_estacion_meteorologica)

# COMMAND ----------

df_dim_estacion_meteorologica.count()

# COMMAND ----------

df_dim_estacion_meteorologica.orderBy("provincia", "estacion_nombre").show(50, truncate=False)

# COMMAND ----------

df_dim_estacion_meteorologica.write.mode("overwrite").saveAsTable(
    "energy_platform.silver.dim_estacion_meteorologica"
)

# COMMAND ----------

from pyspark.sql import functions as F

def decimal_coma_to_double(column_name: str):
    normalized = F.regexp_replace(F.col(column_name), ",", ".")

    return (
        F.when(F.col(column_name).isNull(), None)
        .when(F.col(column_name) == "Ip", F.lit(0.0))
        .otherwise(normalized.cast("double"))
    )

# COMMAND ----------

df_fct_climatologia_diaria = (
    df_bronze_aemet
    .select(
        F.to_date(F.col("fecha")).alias("fecha"),
        F.col("indicativo").alias("estacion_id"),
        decimal_coma_to_double("tmed").alias("temperatura_media_c"),
        decimal_coma_to_double("tmin").alias("temperatura_minima_c"),
        decimal_coma_to_double("tmax").alias("temperatura_maxima_c"),
        decimal_coma_to_double("prec").alias("precipitacion_mm"),
        decimal_coma_to_double("velmedia").alias("velocidad_media_viento"),
        decimal_coma_to_double("racha").alias("racha_maxima_viento"),
        F.col("dir").alias("direccion_viento"),
        F.col("hrMedia").cast("int").alias("humedad_media_pct"),
        F.col("hrMin").cast("int").alias("humedad_minima_pct"),
        F.col("hrMax").cast("int").alias("humedad_maxima_pct"),
        decimal_coma_to_double("sol").alias("horas_sol"),
        F.col("source_file"),
        F.col("loaded_at")
    )
    .dropDuplicates(["fecha", "estacion_id"])
)

display(df_fct_climatologia_diaria)

# COMMAND ----------

df_fct_climatologia_diaria.select(
    F.count(F.when(F.col("fecha").isNull(), 1)).alias("fecha_nulls"),
    F.count(F.when(F.col("estacion_id").isNull(), 1)).alias("estacion_id_nulls"),
    F.count(F.when(F.col("temperatura_media_c").isNull(), 1)).alias("temperatura_media_nulls"),
    F.count(F.when(F.col("precipitacion_mm").isNull(), 1)).alias("precipitacion_nulls")
).show()

# COMMAND ----------

df_fct_climatologia_diaria.groupBy("fecha").agg(
    F.count("*").alias("filas"),
    F.count(F.when(F.col("temperatura_media_c").isNull(), 1)).alias("temperatura_media_nulls"),
    F.count(F.when(F.col("precipitacion_mm").isNull(), 1)).alias("precipitacion_nulls")
).orderBy("fecha").show(50)

# COMMAND ----------

df_fct_climatologia_diaria.write.mode("overwrite").saveAsTable(
    "energy_platform.silver.fct_climatologia_diaria"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS rows
# MAGIC FROM energy_platform.silver.fct_climatologia_diaria;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT fecha, COUNT(*) AS rows
# MAGIC FROM energy_platform.silver.fct_climatologia_diaria
# MAGIC GROUP BY fecha
# MAGIC ORDER BY fecha;