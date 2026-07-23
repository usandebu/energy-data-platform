# Databricks notebook source
from pyspark.sql import functions as F

dbutils.widgets.text("raw_bucket", "")
dbutils.widgets.text("sample_date", "2026-07-01")


def required_widget(name: str) -> str:
    value = dbutils.widgets.get(name).strip()
    if not value:
        raise ValueError(f"{name} is required")
    return value


raw_bucket = required_widget("raw_bucket")
sample_date = required_widget("sample_date")
sample_year, sample_month, sample_day = sample_date.split("-")

sample_aemet_path = (
    f"s3://{raw_bucket}/aemet/climatologia-diaria/"
    f"year={sample_year}/month={sample_month}/day={sample_day}/data.json"
)

aemet_schema = (
    spark.read
    .option("multiline", "true")
    .json(sample_aemet_path)
    .schema
)

# COMMAND ----------

aemet_raw_path = f"s3://{raw_bucket}/aemet/climatologia-diaria/year=*/month=*/day=*/data.json"

df_aemet_raw = (
    spark.read
    .schema(aemet_schema)
    .option("multiline", "true")
    .json(aemet_raw_path)
)

# COMMAND ----------

df_aemet_bronze = (
    df_aemet_raw
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

# COMMAND ----------

df_aemet_bronze.count()

# COMMAND ----------

df_aemet_bronze.select(
    F.count(F.when(F.col("source_file").isNull(), 1)).alias("source_file_nulls"),
    F.count(F.when(F.col("fecha").isNull(), 1)).alias("fecha_nulls"),
    F.count(F.when(F.col("indicativo").isNull(), 1)).alias("indicativo_nulls")
).show()

# COMMAND ----------

df_aemet_bronze.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("energy_platform.bronze.aemet_climatologia_diaria")

# COMMAND ----------

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

# COMMAND ----------

df_dim_estacion_meteorologica.count()

# COMMAND ----------

df_dim_estacion_meteorologica.orderBy("provincia", "estacion_nombre").show(50, truncate=False)

# COMMAND ----------

df_dim_estacion_meteorologica.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("energy_platform.silver.dim_estacion_meteorologica")

# COMMAND ----------

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

# COMMAND ----------

df_fct_climatologia_diaria.select(
    F.count(F.when(F.col("fecha").isNull(), 1)).alias("fecha_nulls"),
    F.count(F.when(F.col("estacion_id").isNull(), 1)).alias("estacion_id_nulls"),
    F.count(F.when(F.col("temperatura_media_c").isNull(), 1)).alias("temperatura_media_nulls"),
    F.count(F.when(F.col("precipitacion_mm").isNull(), 1)).alias("precipitacion_nulls")
).show()

# COMMAND ----------

total_rows = df_fct_climatologia_diaria.count()

df_fct_climatologia_diaria.select(
    F.lit(total_rows).alias("total_rows"),
    (F.count(F.when(F.col("temperatura_media_c").isNull(), 1)) / F.lit(total_rows)).alias("temperatura_media_null_ratio"),
    (F.count(F.when(F.col("precipitacion_mm").isNull(), 1)) / F.lit(total_rows)).alias("precipitacion_null_ratio")
).show()

# COMMAND ----------

df_fct_climatologia_diaria.groupBy(F.year("fecha").alias("anio")).agg(
    F.count("*").alias("filas"),
    F.count(F.when(F.col("temperatura_media_c").isNull(), 1)).alias("temperatura_media_nulls"),
    F.count(F.when(F.col("precipitacion_mm").isNull(), 1)).alias("precipitacion_nulls")
).orderBy("anio").show(50)

# COMMAND ----------

df_fct_climatologia_diaria.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("energy_platform.silver.fct_climatologia_diaria")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS filas
# MAGIC FROM energy_platform.silver.fct_climatologia_diaria;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(DISTINCT fecha) AS dias,
# MAGIC   MIN(fecha) AS fecha_min,
# MAGIC   MAX(fecha) AS fecha_max
# MAGIC FROM energy_platform.silver.fct_climatologia_diaria;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS filas,
# MAGIC   MIN(fecha) AS fecha_min,
# MAGIC   MAX(fecha) AS fecha_max
# MAGIC FROM energy_platform.gold.mart_energia_clima_diario;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   MIN(porcentaje_renovable) AS min_porcentaje_renovable,
# MAGIC   MAX(porcentaje_renovable) AS max_porcentaje_renovable
# MAGIC FROM energy_platform.gold.mart_energia_clima_diario;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   corr(generacion_eolica_mwh, velocidad_media_viento) AS corr_eolica_viento,
# MAGIC   corr(generacion_solar_fotovoltaica_mwh, horas_sol_media) AS corr_solar_sol,
# MAGIC   corr(generacion_pura_mwh, temperatura_media_espana_c) AS corr_generacion_temperatura
# MAGIC FROM energy_platform.gold.mart_energia_clima_diario;
