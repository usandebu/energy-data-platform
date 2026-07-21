# Databricks notebook source
from pyspark.sql import functions as F

df_generacion = spark.table("energy_platform.silver.fct_generacion_energia_diaria")
df_tecnologia = spark.table("energy_platform.silver.dim_tecnologia_energia")
df_clima = spark.table("energy_platform.silver.fct_climatologia_diaria")

# COMMAND ----------

df_energia_diaria = (
    df_generacion
    .join(df_tecnologia, on="tecnologia_id", how="left")
    .groupBy("fecha")
    .agg(
        F.sum(
            F.when(
                F.col("grupo_energia_id").isin("Renovable", "No-Renovable"),
                F.col("generacion_mwh")
            ).otherwise(0)
        ).alias("generacion_pura_mwh"),

        F.sum(
            F.when(
                F.col("grupo_energia_id") == "Renovable",
                F.col("generacion_mwh")
            ).otherwise(0)
        ).alias("generacion_renovable_mwh"),

        F.sum(
            F.when(
                F.col("grupo_energia_id") == "No-Renovable",
                F.col("generacion_mwh")
            ).otherwise(0)
        ).alias("generacion_no_renovable_mwh"),

        F.sum(
            F.when(
                F.col("grupo_energia_id") == "Almacenamiento",
                F.col("generacion_mwh")
            ).otherwise(0)
        ).alias("saldo_almacenamiento_mwh"),

        F.sum(
            F.when(
                F.col("grupo_energia_id") == "Demanda",
                F.col("generacion_mwh")
            ).otherwise(0)
        ).alias("saldo_intercambios_mwh"),

        F.sum(
            F.when(
                F.col("tecnologia_nombre") == "Eólica",
                F.col("generacion_mwh")
            ).otherwise(0)
        ).alias("generacion_eolica_mwh"),

        F.sum(
            F.when(
                F.col("tecnologia_nombre") == "Solar fotovoltaica",
                F.col("generacion_mwh")
            ).otherwise(0)
        ).alias("generacion_solar_fotovoltaica_mwh")
    )
    .withColumn(
        "porcentaje_renovable",
        F.col("generacion_renovable_mwh") / F.col("generacion_pura_mwh")
    )
)

# COMMAND ----------

df_clima_diario = (
    df_clima
    .groupBy("fecha")
    .agg(
        F.avg("temperatura_media_c").alias("temperatura_media_espana_c"),
        F.avg("temperatura_minima_c").alias("temperatura_minima_media_espana_c"),
        F.avg("temperatura_maxima_c").alias("temperatura_maxima_media_espana_c"),
        F.avg("precipitacion_mm").alias("precipitacion_media_mm"),
        F.avg("velocidad_media_viento").alias("velocidad_media_viento"),
        F.avg("racha_maxima_viento").alias("racha_maxima_media_viento"),
        F.avg("horas_sol").alias("horas_sol_media"),
        F.countDistinct("estacion_id").alias("num_estaciones")
    )
)

# COMMAND ----------

df_mart_energia_clima_diario = (
    df_energia_diaria
    .join(df_clima_diario, on="fecha", how="inner")
    .orderBy("fecha")
)

display(df_mart_energia_clima_diario)

# COMMAND ----------

df_mart_energia_clima_diario.count()

# COMMAND ----------

df_mart_energia_clima_diario.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("energy_platform.gold.mart_energia_clima_diario")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE energy_platform.gold.mart_energia_clima_diario;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM energy_platform.gold.mart_energia_clima_diario
# MAGIC ORDER BY fecha;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   fecha,
# MAGIC   porcentaje_renovable,
# MAGIC   generacion_eolica_mwh,
# MAGIC   velocidad_media_viento,
# MAGIC   generacion_solar_fotovoltaica_mwh,
# MAGIC   horas_sol_media,
# MAGIC   temperatura_media_espana_c
# MAGIC FROM energy_platform.gold.mart_energia_clima_diario
# MAGIC ORDER BY fecha;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   corr(generacion_eolica_mwh, velocidad_media_viento) AS corr_eolica_viento,
# MAGIC   corr(generacion_solar_fotovoltaica_mwh, horas_sol_media) AS corr_solar_sol,
# MAGIC   corr(generacion_total_mwh, temperatura_media_espana_c) AS corr_generacion_temperatura
# MAGIC FROM energy_platform.gold.mart_energia_clima_diario;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   grupo_energia_id,
# MAGIC   grupo_energia_nombre,
# MAGIC   COUNT(*) AS filas
# MAGIC FROM energy_platform.silver.dim_tecnologia_energia
# MAGIC GROUP BY grupo_energia_id, grupo_energia_nombre
# MAGIC ORDER BY grupo_energia_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   grupo_energia_nombre,
# MAGIC   tecnologia_id,
# MAGIC   tecnologia_nombre,
# MAGIC   es_renovable,
# MAGIC   es_almacenamiento
# MAGIC FROM energy_platform.silver.dim_tecnologia_energia
# MAGIC ORDER BY grupo_energia_nombre, tecnologia_nombre;