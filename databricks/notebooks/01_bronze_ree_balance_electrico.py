# Databricks notebook source
dbutils.widgets.text("raw_bucket", "")
dbutils.widgets.text("sample_date", "2026-07-01")
dbutils.widgets.text("sample_year", "2026")
dbutils.widgets.text("sample_month", "07")


def required_widget(name: str) -> str:
    value = dbutils.widgets.get(name).strip()
    if not value:
        raise ValueError(f"{name} is required")
    return value


raw_bucket = required_widget("raw_bucket")
sample_date = required_widget("sample_date")
sample_year = required_widget("sample_year")
sample_month = required_widget("sample_month")

sample_year_from_date, sample_month_from_date, sample_day_from_date = sample_date.split("-")

ree_path = (
    f"s3://{raw_bucket}/ree/balance-electrico/"
    f"year={sample_year_from_date}/month={sample_month_from_date}/"
    f"day={sample_day_from_date}/data.json"
)

df_ree_raw = (
    spark.read
    .option("multiline", "true")
    .json(ree_path)
)

display(df_ree_raw)

# COMMAND ----------

df_ree_raw.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

df_ree_bronze = (
    df_ree_raw
    .select(
        F.col("_metadata.file_path").alias("source_file"),
        F.col("data.type").alias("dataset_type"),
        F.col("data.id").alias("dataset_id"),
        F.col("data.attributes.title").alias("dataset_title"),
        F.col("data.attributes.last-update").alias("dataset_last_update"),
        F.explode("included").alias("energy_group")
    )
    .select(
        "source_file",
        "dataset_type",
        "dataset_id",
        "dataset_title",
        "dataset_last_update",
        F.col("energy_group.id").alias("group_id"),
        F.col("energy_group.type").alias("group_type"),
        F.col("energy_group.attributes.title").alias("group_title"),
        F.explode("energy_group.attributes.content").alias("technology")
    )
    .select(
        "source_file",
        "dataset_type",
        "dataset_id",
        "dataset_title",
        "dataset_last_update",
        "group_id",
        "group_type",
        "group_title",
        F.col("technology.id").alias("technology_id"),
        F.col("technology.type").alias("technology_type"),
        F.col("technology.groupId").alias("technology_group_id"),
        F.col("technology.attributes.title").alias("technology_title"),
        F.col("technology.attributes.composite").alias("is_composite"),
        F.col("technology.attributes.total").alias("total"),
        F.col("technology.attributes.total-percentage").alias("total_percentage"),
        F.explode("technology.attributes.values").alias("measurement")
    )
    .select(
        "source_file",
        "dataset_type",
        "dataset_id",
        "dataset_title",
        "dataset_last_update",
        "group_id",
        "group_type",
        "group_title",
        "technology_id",
        "technology_type",
        "technology_group_id",
        "technology_title",
        "is_composite",
        "total",
        "total_percentage",
        F.col("measurement.datetime").alias("datetime"),
        F.col("measurement.value").alias("value"),
        F.col("measurement.percentage").alias("percentage"),
        F.current_timestamp().alias("loaded_at")
    )
)

display(df_ree_bronze)

# COMMAND ----------

df_ree_bronze.count()

# COMMAND ----------

df_ree_bronze.groupBy("is_composite").count().show()

# COMMAND ----------

df_ree_bronze.select("source_file").distinct().show(truncate=False)

# COMMAND ----------

ree_july_path = (
    f"s3://{raw_bucket}/ree/balance-electrico/"
    f"year={sample_year}/month={sample_month}/day=*/data.json"
)

df_ree_raw_july = (
    spark.read
    .option("multiline", "true")
    .json(ree_july_path)
)

# COMMAND ----------

from pyspark.sql import functions as F

df_ree_bronze_july = (
    df_ree_raw_july
    .select(
        F.col("_metadata.file_path").alias("source_file"),
        F.col("data.type").alias("dataset_type"),
        F.col("data.id").alias("dataset_id"),
        F.col("data.attributes.title").alias("dataset_title"),
        F.col("data.attributes.last-update").alias("dataset_last_update"),
        F.explode("included").alias("energy_group")
    )
    .select(
        "source_file",
        "dataset_type",
        "dataset_id",
        "dataset_title",
        "dataset_last_update",
        F.col("energy_group.id").alias("group_id"),
        F.col("energy_group.type").alias("group_type"),
        F.col("energy_group.attributes.title").alias("group_title"),
        F.explode("energy_group.attributes.content").alias("technology")
    )
    .select(
        "source_file",
        "dataset_type",
        "dataset_id",
        "dataset_title",
        "dataset_last_update",
        "group_id",
        "group_type",
        "group_title",
        F.col("technology.id").alias("technology_id"),
        F.col("technology.type").alias("technology_type"),
        F.col("technology.groupId").alias("technology_group_id"),
        F.col("technology.attributes.title").alias("technology_title"),
        F.col("technology.attributes.composite").alias("is_composite"),
        F.col("technology.attributes.total").alias("total"),
        F.col("technology.attributes.total-percentage").alias("total_percentage"),
        F.explode("technology.attributes.values").alias("measurement")
    )
    .select(
        "source_file",
        "dataset_type",
        "dataset_id",
        "dataset_title",
        "dataset_last_update",
        "group_id",
        "group_type",
        "group_title",
        "technology_id",
        "technology_type",
        "technology_group_id",
        "technology_title",
        "is_composite",
        "total",
        "total_percentage",
        F.col("measurement.datetime").alias("datetime"),
        F.col("measurement.value").alias("value"),
        F.col("measurement.percentage").alias("percentage"),
        F.current_timestamp().alias("loaded_at")
    )
)

display(df_ree_bronze_july)

# COMMAND ----------

df_ree_bronze_july.count()

# COMMAND ----------

df_ree_bronze_july.groupBy("source_file").count().show(truncate=False)

# COMMAND ----------

df_ree_bronze_july.write.mode("overwrite").saveAsTable(
    "energy_platform.bronze.ree_balance_electrico"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS rows
# MAGIC FROM energy_platform.bronze.ree_balance_electrico;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT source_file, COUNT(*) AS rows
# MAGIC FROM energy_platform.bronze.ree_balance_electrico
# MAGIC GROUP BY source_file
# MAGIC ORDER BY source_file;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT is_composite, COUNT(*) AS rows
# MAGIC FROM energy_platform.bronze.ree_balance_electrico
# MAGIC GROUP BY is_composite;

# COMMAND ----------
