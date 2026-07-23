# Databricks notebook source
dbutils.widgets.text("raw_bucket", "")
dbutils.widgets.text("sample_year", "2026")
dbutils.widgets.text("sample_month", "07")


def required_widget(name: str) -> str:
    value = dbutils.widgets.get(name).strip()
    if not value:
        raise ValueError(f"{name} is required")
    return value


raw_bucket = required_widget("raw_bucket")
sample_year = required_widget("sample_year")
sample_month = required_widget("sample_month")

aemet_july_path = (
    f"s3://{raw_bucket}/aemet/climatologia-diaria/"
    f"year={sample_year}/month={sample_month}/day=*/data.json"
)

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
