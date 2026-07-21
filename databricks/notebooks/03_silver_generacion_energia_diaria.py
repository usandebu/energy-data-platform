# Databricks notebook source
df_bronze_ree = spark.table("energy_platform.bronze.ree_balance_electrico")

display(df_bronze_ree)

# COMMAND ----------

from pyspark.sql import functions as F

df_bronze_ree = spark.table("energy_platform.bronze.ree_balance_electrico")

df_dim_tecnologia_energia = (
    df_bronze_ree
    .filter(F.col("is_composite") == False)
    .select(
        F.col("technology_id").alias("tecnologia_id"),
        F.col("technology_title").alias("tecnologia_nombre"),
        F.col("group_id").alias("grupo_energia_id"),
        F.col("group_title").alias("grupo_energia_nombre")
    )
    .dropDuplicates(["tecnologia_id"])
    .withColumn(
        "es_renovable",
        F.col("grupo_energia_id") == F.lit("Renovable")
    )
    .withColumn(
        "es_almacenamiento",
        F.col("grupo_energia_id") == F.lit("Almacenamiento")
    )
)

display(df_dim_tecnologia_energia)

# COMMAND ----------

df_dim_tecnologia_energia.count()

# COMMAND ----------

df_dim_tecnologia_energia.orderBy("grupo_energia_nombre", "tecnologia_nombre").show(50, truncate=False)

# COMMAND ----------

df_dim_tecnologia_energia.write.mode("overwrite").saveAsTable(
    "energy_platform.silver.dim_tecnologia_energia"
)

# COMMAND ----------

from pyspark.sql import functions as F

df_fct_generacion_energia_diaria = (
    df_bronze_ree
    .filter(F.col("is_composite") == False)
    .select(
        F.to_date(F.col("datetime")).alias("fecha"),
        F.col("technology_id").alias("tecnologia_id"),
        F.col("value").cast("double").alias("generacion_mwh"),
        F.col("percentage").cast("double").alias("porcentaje_grupo"),
        F.col("source_file"),
        F.col("loaded_at")
    )
    .dropDuplicates(["fecha", "tecnologia_id"])
)

display(df_fct_generacion_energia_diaria)

# COMMAND ----------

df_fct_generacion_energia_diaria.count()

# COMMAND ----------

df_fct_generacion_energia_diaria.groupBy("fecha").count().orderBy("fecha").show(50)

# COMMAND ----------

df_fct_generacion_energia_diaria.write.mode("overwrite").saveAsTable(
    "energy_platform.silver.fct_generacion_energia_diaria"
)