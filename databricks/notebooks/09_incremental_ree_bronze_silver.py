# Databricks notebook source
from datetime import date, timedelta

from delta.tables import DeltaTable
from pyspark.sql import functions as F


dbutils.widgets.text("raw_bucket", "")
dbutils.widgets.text("process_start_date", "")
dbutils.widgets.text("process_end_date", "")


def required_widget(name: str) -> str:
    value = dbutils.widgets.get(name).strip()
    if not value:
        raise ValueError(f"{name} is required")
    return value


def parse_date(value: str, name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"{name} must use YYYY-MM-DD format") from error


def iter_dates(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def raw_paths_for_dates(raw_bucket: str, start_date: date, end_date: date) -> list[str]:
    return [
        (
            f"s3://{raw_bucket}/ree/balance-electrico/"
            f"year={day:%Y}/month={day:%m}/day={day:%d}/data.json"
        )
        for day in iter_dates(start_date, end_date)
    ]


def merge_delta(
    source_df,
    target_table: str,
    merge_condition: str,
    preserve_matched_columns: set[str] | None = None,
) -> None:
    preserve_matched_columns = preserve_matched_columns or set()

    if spark.catalog.tableExists(target_table):
        update_assignments = {
            column: f"source.{column}"
            for column in source_df.columns
            if column not in preserve_matched_columns
        }
        (
            DeltaTable.forName(spark, target_table)
            .alias("target")
            .merge(source_df.alias("source"), merge_condition)
            .whenMatchedUpdate(set=update_assignments)
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        source_df.write.format("delta").mode("overwrite").saveAsTable(target_table)


raw_bucket = required_widget("raw_bucket")
process_start_date = parse_date(
    required_widget("process_start_date"),
    "process_start_date",
)
process_end_date = parse_date(
    required_widget("process_end_date"),
    "process_end_date",
)

if process_start_date > process_end_date:
    raise ValueError("process_start_date must be less than or equal to process_end_date")

raw_paths = raw_paths_for_dates(raw_bucket, process_start_date, process_end_date)

# COMMAND ----------

df_ree_raw = (
    spark.read
    .option("multiline", "true")
    .json(raw_paths)
)

# COMMAND ----------

df_ree_bronze_incremental = (
    df_ree_raw
    .select(
        F.col("_metadata.file_path").alias("source_file"),
        F.col("data.type").alias("dataset_type"),
        F.col("data.id").alias("dataset_id"),
        F.col("data.attributes.title").alias("dataset_title"),
        F.col("data.attributes.last-update").alias("dataset_last_update"),
        F.explode("included").alias("energy_group"),
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
        F.explode("energy_group.attributes.content").alias("technology"),
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
        F.explode("technology.attributes.values").alias("measurement"),
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
        F.current_timestamp().alias("loaded_at"),
    )
)

# COMMAND ----------

df_ree_bronze_incremental.select(
    F.count(F.when(F.col("source_file").isNull(), 1)).alias("source_file_nulls"),
    F.count(F.when(F.col("technology_id").isNull(), 1)).alias("technology_id_nulls"),
    F.count(F.when(F.col("datetime").isNull(), 1)).alias("datetime_nulls"),
    F.count(F.when(F.col("value").isNull(), 1)).alias("value_nulls"),
).show()

df_ree_bronze_incremental.groupBy("source_file").count().orderBy("source_file").show(
    200,
    truncate=False,
)

# COMMAND ----------

merge_delta(
    source_df=df_ree_bronze_incremental,
    target_table="energy_platform.bronze.ree_balance_electrico",
    merge_condition="""
        target.source_file = source.source_file
        AND target.technology_id = source.technology_id
        AND target.datetime = source.datetime
    """,
    preserve_matched_columns={"loaded_at"},
)

# COMMAND ----------

df_bronze_ree_incremental = (
    spark.table("energy_platform.bronze.ree_balance_electrico")
    .filter(F.to_date(F.col("datetime")).between(process_start_date, process_end_date))
)

df_dim_tecnologia_energia_incremental = (
    df_bronze_ree_incremental
    .filter(F.col("is_composite") == False)
    .select(
        F.col("technology_id").alias("tecnologia_id"),
        F.col("technology_title").alias("tecnologia_nombre"),
        F.col("group_id").alias("grupo_energia_id"),
        F.col("group_title").alias("grupo_energia_nombre"),
    )
    .dropDuplicates(["tecnologia_id"])
    .withColumn("es_renovable", F.col("grupo_energia_id") == F.lit("Renovable"))
    .withColumn(
        "es_almacenamiento",
        F.col("grupo_energia_id") == F.lit("Almacenamiento"),
    )
)

df_fct_generacion_energia_diaria_incremental = (
    df_bronze_ree_incremental
    .filter(F.col("is_composite") == False)
    .select(
        F.to_date(F.col("datetime")).alias("fecha"),
        F.col("technology_id").alias("tecnologia_id"),
        F.col("value").cast("double").alias("generacion_mwh"),
        F.col("percentage").cast("double").alias("porcentaje_grupo"),
        F.col("source_file"),
        F.col("loaded_at"),
    )
    .dropDuplicates(["fecha", "tecnologia_id"])
)

# COMMAND ----------

merge_delta(
    source_df=df_dim_tecnologia_energia_incremental,
    target_table="energy_platform.silver.dim_tecnologia_energia",
    merge_condition="target.tecnologia_id = source.tecnologia_id",
)

merge_delta(
    source_df=df_fct_generacion_energia_diaria_incremental,
    target_table="energy_platform.silver.fct_generacion_energia_diaria",
    merge_condition="""
        target.fecha = source.fecha
        AND target.tecnologia_id = source.tecnologia_id
    """,
    preserve_matched_columns={"loaded_at"},
)

# COMMAND ----------

spark.sql("""
SELECT
  COUNT(*) AS filas,
  MIN(fecha) AS fecha_min,
  MAX(fecha) AS fecha_max
FROM energy_platform.silver.fct_generacion_energia_diaria
""").show()

spark.sql("""
SELECT filas_por_dia, COUNT(*) AS dias
FROM (
  SELECT fecha, COUNT(*) AS filas_por_dia
  FROM energy_platform.silver.fct_generacion_energia_diaria
  GROUP BY fecha
)
GROUP BY filas_por_dia
ORDER BY filas_por_dia
""").show()
