# Databricks notebook source
aemet_july_path = "s3://energy-data-platform-dev-raw/aemet/climatologia-diaria/year=2026/month=07/day=*/data.json"

df_aemet_raw_july = (
    spark.read
    .option("multiline", "true")
    .json(aemet_july_path)
)

df_aemet_raw_july.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

df_aemet_bronze_july = (
    df_aemet_raw_july
    .select(
        F.col("_metadata.file_path").alias("source_file"),
        F.explode("data").alias("station_day")
    )
    .select(
        "source_file",
        "station_day.*",
        F.current_timestamp().alias("loaded_at")
    )
)

display(df_aemet_bronze_july)

# COMMAND ----------

df_aemet_bronze_july.count()

# COMMAND ----------

df_aemet_bronze_july.groupBy("source_file").count().show(truncate=False)

# COMMAND ----------

df_aemet_bronze_july.select("fecha").distinct().orderBy("fecha").show(50, truncate=False)

# COMMAND ----------

df_aemet_bronze_july.write.mode("overwrite").saveAsTable(
    "energy_platform.bronze.aemet_climatologia_diaria"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS rows
# MAGIC FROM energy_platform.bronze.aemet_climatologia_diaria;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT fecha, COUNT(*) AS rows
# MAGIC FROM energy_platform.bronze.aemet_climatologia_diaria
# MAGIC GROUP BY fecha
# MAGIC ORDER BY fecha;