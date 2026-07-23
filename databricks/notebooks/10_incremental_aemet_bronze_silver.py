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
            f"s3://{raw_bucket}/aemet/climatologia-diaria/"
            f"year={day:%Y}/month={day:%m}/day={day:%d}/data.json"
        )
        for day in iter_dates(start_date, end_date)
    ]


def decimal_coma_to_double(column_name: str):
    return F.expr(
        f"""
        case
            when `{column_name}` is null then null
            when `{column_name}` = 'Ip' then 0.0
            else try_cast(replace(`{column_name}`, ',', '.') as double)
        end
        """
    )


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

df_aemet_raw = (
    spark.read
    .option("multiline", "true")
    .json(raw_paths)
)

# COMMAND ----------

df_aemet_bronze_incremental = (
    df_aemet_raw
    .select(
        F.col("_metadata.file_path").alias("source_file"),
        F.explode("data").alias("station_day"),
    )
    .select(
        "source_file",
        "station_day.*",
        F.current_timestamp().alias("loaded_at"),
    )
)

# COMMAND ----------

df_aemet_bronze_incremental.select(
    F.count(F.when(F.col("source_file").isNull(), 1)).alias("source_file_nulls"),
    F.count(F.when(F.col("fecha").isNull(), 1)).alias("fecha_nulls"),
    F.count(F.when(F.col("indicativo").isNull(), 1)).alias("indicativo_nulls"),
).show()

df_aemet_bronze_incremental.groupBy("source_file").count().orderBy(
    "source_file"
).show(200, truncate=False)

# COMMAND ----------

merge_delta(
    source_df=df_aemet_bronze_incremental,
    target_table="energy_platform.bronze.aemet_climatologia_diaria",
    merge_condition="""
        target.fecha = source.fecha
        AND target.indicativo = source.indicativo
    """,
    preserve_matched_columns={"loaded_at"},
)

# COMMAND ----------

df_bronze_aemet_incremental = (
    spark.table("energy_platform.bronze.aemet_climatologia_diaria")
    .filter(F.to_date(F.col("fecha")).between(process_start_date, process_end_date))
)

df_dim_estacion_meteorologica_incremental = (
    df_bronze_aemet_incremental
    .select(
        F.col("indicativo").alias("estacion_id"),
        F.col("nombre").alias("estacion_nombre"),
        F.col("provincia"),
        F.col("altitud").cast("int").alias("altitud_metros"),
    )
    .dropDuplicates(["estacion_id"])
)

df_fct_climatologia_diaria_incremental = (
    df_bronze_aemet_incremental
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
        F.col("loaded_at"),
    )
    .dropDuplicates(["fecha", "estacion_id"])
)

# COMMAND ----------

df_fct_climatologia_diaria_incremental.select(
    F.count(F.when(F.col("fecha").isNull(), 1)).alias("fecha_nulls"),
    F.count(F.when(F.col("estacion_id").isNull(), 1)).alias("estacion_id_nulls"),
    F.count(F.when(F.col("temperatura_media_c").isNull(), 1)).alias(
        "temperatura_media_nulls"
    ),
    F.count(F.when(F.col("precipitacion_mm").isNull(), 1)).alias(
        "precipitacion_nulls"
    ),
).show()

# COMMAND ----------

merge_delta(
    source_df=df_dim_estacion_meteorologica_incremental,
    target_table="energy_platform.silver.dim_estacion_meteorologica",
    merge_condition="target.estacion_id = source.estacion_id",
)

merge_delta(
    source_df=df_fct_climatologia_diaria_incremental,
    target_table="energy_platform.silver.fct_climatologia_diaria",
    merge_condition="""
        target.fecha = source.fecha
        AND target.estacion_id = source.estacion_id
    """,
    preserve_matched_columns={"loaded_at"},
)

# COMMAND ----------

spark.sql("""
SELECT
  COUNT(*) AS filas,
  MIN(fecha) AS fecha_min,
  MAX(fecha) AS fecha_max
FROM energy_platform.silver.fct_climatologia_diaria
""").show()

spark.sql("""
SELECT filas_por_dia, COUNT(*) AS dias
FROM (
  SELECT fecha, COUNT(*) AS filas_por_dia
  FROM energy_platform.silver.fct_climatologia_diaria
  GROUP BY fecha
)
GROUP BY filas_por_dia
ORDER BY filas_por_dia
""").show()
