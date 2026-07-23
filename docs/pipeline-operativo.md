# Pipeline operativo

Este documento resume cómo funciona ahora mismo el flujo diario del proyecto.

La idea principal es sencilla: cada día se descargan solo los JSON que faltan,
Databricks transforma solo una ventana reciente de datos y dbt reconstruye las
tablas analíticas finales.

## Arquitectura actual

```text
APIs de REE y AEMET
        |
        v
Airflow
        |
        v
S3 Raw incremental
        |
        v
Databricks Bronze/Silver incremental
        |
        v
dbt sobre Databricks
        |
        v
Tablas Gold
```

El flujo diario está pensado para ser idempotente. Es decir: si se ejecuta dos
veces para el mismo rango de fechas, no debe duplicar filas ni dejar resultados
distintos sin motivo.

## DAG diario

El DAG principal es:

```text
energy_daily_incremental_pipeline
```

Tiene estas tareas:

```text
ingest_incremental_raw_s3
run_ree_incremental_bronze_silver
run_aemet_incremental_bronze_silver
dbt_build
```

El orden es:

```text
ingest_incremental_raw_s3
  -> run_ree_incremental_bronze_silver
  -> dbt_build

ingest_incremental_raw_s3
  -> run_aemet_incremental_bronze_silver
  -> dbt_build
```

Primero se actualiza la capa raw en S3. Después REE y AEMET se transforman en
paralelo en Databricks. Cuando las dos ramas terminan correctamente, se ejecuta
`dbt build`.

## Retraso de cada fuente

REE y AEMET no publican los datos al mismo ritmo.

La configuración actual es:

```text
REE_LAG_DAYS=1
AEMET_LAG_DAYS=3
```

Esto significa:

- REE se intenta procesar hasta ayer.
- AEMET se intenta procesar hasta hace tres días.
- La capa Gold solo cruza las fechas que existen en ambas fuentes.

Esto evita forzar a AEMET a ir al mismo ritmo que REE cuando todavía no ha
publicado esos datos.

## Ventana incremental de Databricks

Databricks no transforma todo el histórico cada día. En su lugar reprocesa una
ventana reciente:

```text
DATABRICKS_INCREMENTAL_LOOKBACK_DAYS=14
```

Esto permite cubrir pequeños retrasos, reintentos o ejecuciones fallidas sin
volver a transformar años completos de datos.

Como las escrituras en Delta usan `MERGE`, reprocesar esa ventana no duplica
filas.

## Variables necesarias

Las variables de AWS y Databricks son obligatorias. No se usan valores por
defecto para cosas como el bucket, la región o los IDs de Databricks, porque eso
puede ocultar errores de configuración.

Variables necesarias:

```text
PROJECT_HOST_ROOT
RAW_BUCKET
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_REGION
AEMET_API_KEY
DATABRICKS_HOST
DATABRICKS_HTTP_PATH
DATABRICKS_TOKEN
DATABRICKS_BRONZE_SILVER_JOB_ID
DATABRICKS_REE_INCREMENTAL_JOB_ID
DATABRICKS_AEMET_INCREMENTAL_JOB_ID
REE_LAG_DAYS
AEMET_LAG_DAYS
DATABRICKS_INCREMENTAL_LOOKBACK_DAYS
```

`AWS_SESSION_TOKEN` es opcional. Solo hace falta cuando se usan credenciales
temporales de AWS.

## Airflow en local

Inicializar Airflow:

```bash
docker compose --profile airflow run --rm airflow-init
```

Levantar Airflow:

```bash
docker compose --profile airflow up -d
```

Si cambian variables de entorno o credenciales AWS, no basta con reiniciar. Hay
que recrear los contenedores:

```bash
docker compose --profile airflow up -d --force-recreate airflow-scheduler airflow-webserver
```

Comprobar errores de carga de DAGs:

```bash
docker compose --profile airflow exec airflow-scheduler airflow dags list-import-errors
```

Lanzar el DAG diario manualmente:

```bash
docker compose --profile airflow exec airflow-scheduler airflow dags trigger energy_daily_incremental_pipeline
```

## Ingesta raw incremental

La ingesta raw también se puede ejecutar sin Airflow:

```bash
docker compose run --rm app python -m energy_pipeline.ingest.incremental \
  --source all \
  --raw-storage-backend s3 \
  --raw-bucket "$RAW_BUCKET" \
  --ree-lag-days "$REE_LAG_DAYS" \
  --aemet-lag-days "$AEMET_LAG_DAYS"
```

Este comando:

- busca la última fecha disponible de REE y AEMET en S3;
- descarga solo los días que faltan;
- respeta el retraso distinto de cada fuente;
- termina correctamente si no hay nada pendiente.

## Databricks

Hay dos tipos de notebooks.

Notebooks de reconstrucción completa:

```text
07_full_refresh_ree_bronze_silver
08_full_refresh_aemet_bronze_silver
```

Sirven para rehacer Bronze y Silver desde cero cuando sea necesario.

Notebooks incrementales:

```text
09_incremental_ree_bronze_silver
10_incremental_aemet_bronze_silver
```

Son los que usa el DAG diario.

Reciben estos parámetros:

```text
raw_bucket
process_start_date
process_end_date
```

Las escrituras incrementales usan `MERGE` en Delta.

Claves usadas:

```text
REE Bronze: source_file + technology_id + datetime
REE Silver fact: fecha + tecnologia_id
AEMET Bronze: fecha + indicativo
AEMET Silver fact: fecha + estacion_id
```

Cuando una fila ya existe, se actualizan los datos de negocio, pero se conserva
`loaded_at`. Así una reejecución del mismo rango no cambia timestamps sin
necesidad.

## dbt

Ejecutar dbt manualmente:

```bash
docker compose --profile analytics run --rm dbt build
```

El DAG diario ejecuta este mismo comando después de terminar las dos ramas
incrementales de Databricks.

## Validación

Comprobaciones locales:

```bash
uv run ruff check src tests airflow/dags
uv run pytest -q tests
```

Comprobar Docker Compose:

```bash
docker compose config --quiet
```

Comprobar dbt:

```bash
docker compose --profile analytics run --rm dbt build
```
