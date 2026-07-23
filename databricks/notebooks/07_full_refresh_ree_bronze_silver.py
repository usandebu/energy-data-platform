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

sample_ree_path = (
    f"s3://{raw_bucket}/ree/balance-electrico/"
    f"year={sample_year}/month={sample_month}/day={sample_day}/data.json"
)

ree_schema = (
    spark.read
    .option("multiline", "true")
    .json(sample_ree_path)
    .schema
)

# COMMAND ----------

ree_raw_path = f"s3://{raw_bucket}/ree/balance-electrico/year=*/month=*/day=*/data.json"

df_ree_raw = (
    spark.read
    .schema(ree_schema)
    .option("multiline", "true")
    .json(ree_raw_path)
)

# COMMAND ----------

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

# COMMAND ----------

df_ree_bronze.count()

# COMMAND ----------

df_ree_bronze.groupBy("source_file").count().where("count != 21").show(truncate=False)

# COMMAND ----------

df_ree_bronze.groupBy("source_file").count().groupBy("count").count().orderBy("count").show(50)

# COMMAND ----------

df_ree_bronze.select(
    "group_title",
    "technology_id",
    "technology_title",
    "is_composite"
).dropDuplicates().orderBy(
    "group_title",
    "technology_title"
).show(200, truncate=False)

# COMMAND ----------

df_ree_bronze.select(
    F.count(F.when(F.col("source_file").isNull(), 1)).alias("source_file_nulls"),
    F.count(F.when(F.col("technology_id").isNull(), 1)).alias("technology_id_nulls"),
    F.count(F.when(F.col("datetime").isNull(), 1)).alias("datetime_nulls"),
    F.count(F.when(F.col("value").isNull(), 1)).alias("value_nulls")
).show()

# COMMAND ----------

df_ree_bronze.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("energy_platform.bronze.ree_balance_electrico")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS rows
# MAGIC FROM energy_platform.bronze.ree_balance_electrico;

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

# COMMAND ----------

df_dim_tecnologia_energia.count()

# COMMAND ----------

df_dim_tecnologia_energia.orderBy("grupo_energia_nombre", "tecnologia_nombre").show(50, truncate=False)

# COMMAND ----------

df_dim_tecnologia_energia.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("energy_platform.silver.dim_tecnologia_energia")

# COMMAND ----------

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

# COMMAND ----------

df_fct_generacion_energia_diaria.count()

# COMMAND ----------

df_fct_generacion_energia_diaria.groupBy("fecha").count().groupBy("count").count().orderBy("count").show(50)

# COMMAND ----------

df_fct_generacion_energia_diaria.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("energy_platform.silver.fct_generacion_energia_diaria")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS tecnologias
# MAGIC FROM energy_platform.silver.dim_tecnologia_energia;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   grupo_energia_nombre,
# MAGIC   COUNT(*) AS tecnologias
# MAGIC FROM energy_platform.silver.dim_tecnologia_energia
# MAGIC GROUP BY grupo_energia_nombre
# MAGIC ORDER BY grupo_energia_nombre;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS filas
# MAGIC FROM energy_platform.silver.fct_generacion_energia_diaria;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(DISTINCT fecha) AS dias,
# MAGIC   MIN(fecha) AS fecha_min,
# MAGIC   MAX(fecha) AS fecha_max
# MAGIC FROM energy_platform.silver.fct_generacion_energia_diaria;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT filas_por_dia, COUNT(*) AS dias
# MAGIC FROM (
# MAGIC   SELECT fecha, COUNT(*) AS filas_por_dia
# MAGIC   FROM energy_platform.silver.fct_generacion_energia_diaria
# MAGIC   GROUP BY fecha
# MAGIC )
# MAGIC GROUP BY filas_por_dia
# MAGIC ORDER BY filas_por_dia;
