# Energy Data Platform

Plataforma de datos para analizar la relación entre generación renovable y
condiciones meteorológicas en España.

El proyecto trabaja con datos reales de Red Eléctrica de España (REE) y AEMET,
con una arquitectura pensada para evolucionar desde una ingesta reproducible en
local hacia una plataforma cloud en AWS.

---

## Resumen

- Descarga y conservación de datos energéticos y meteorológicos reales.
- Diseño por capas para separar ingesta, almacenamiento, transformación,
  modelado y orquestación.
- Conservación de datos raw para permitir reprocesamiento y auditoría.
- Enfoque en reproducibilidad, testing, control de errores y despliegue cloud.

---

## Objetivo del Proyecto

Construir una pipeline de datos que permita cruzar generación eléctrica y datos
meteorológicos para responder preguntas como:

> ¿Cómo influyen la temperatura, el viento o la radiación solar en la generación
> renovable en España?

---

## Tecnologías Principales

- Python
- Docker
- Airflow
- dbt
- AWS S3
- Terraform
- Databricks / PySpark
- pytest y ruff

---

## Fuentes de Datos

### REE

Datos públicos de balance eléctrico, demanda y generación.

### AEMET

Datos meteorológicos oficiales mediante AEMET OpenData.

---

## Arquitectura Prevista

```text
APIs REE / AEMET
        |
        v
Airflow
        |
        v
Ingesta Python
        |
        v
Data Lake en AWS S3
        |
        v
Databricks / PySpark
        |
        v
Bronze / Silver Delta
        |
        v
dbt sobre Databricks
        |
        v
Gold analítico
```

---

## Ejecución Local

Crear la configuración local:

```bash
cp .env.example .env
```

Para AEMET, añadir la API key en `.env`:

```env
AEMET_API_KEY=tu_api_key
```

Si se usa Airflow local con DockerOperator, definir también la ruta absoluta del
proyecto en la máquina host:

```env
PROJECT_HOST_ROOT=/ruta/absoluta/a/energy-data-platform
```

Ingesta REE:

```bash
uv run python -m energy_pipeline.ingest.ree --start-date 2026-01-01 --end-date 2026-01-01
```

Ingesta AEMET:

```bash
uv run python -m energy_pipeline.ingest.aemet --start-date 2026-01-01 --end-date 2026-01-01
```

Backfill por rango:

```bash
uv run python -m energy_pipeline.ingest.backfill --source all --start-date 2026-01-01 --end-date 2026-01-07
```

Los datos raw se guardan con particiones compatibles con S3 y Spark, y también
preparadas para herramientas de catálogo/consulta como Glue o Athena si se
quisieran añadir más adelante:

```text
data/raw/ree/balance-electrico/year=2026/month=01/day=01/data.json
data/raw/ree/balance-electrico/year=2026/month=01/day=01/metadata.json
```

Por defecto el backend raw es local. Para escribir en S3, el bucket debe existir
y las credenciales AWS deben estar disponibles en el entorno:

```env
RAW_STORAGE_BACKEND=s3
RAW_BUCKET=nombre-del-bucket-raw
AWS_REGION=eu-west-1
```

La estructura de keys en S3 mantiene el mismo particionado:

```text
s3://nombre-del-bucket-raw/ree/balance-electrico/year=2026/month=01/day=01/data.json
```

Ejemplo de backfill hacia S3:

```bash
uv run python -m energy_pipeline.ingest.backfill \
  --source all \
  --start-date 2026-07-02 \
  --end-date 2026-07-03 \
  --raw-storage-backend s3 \
  --raw-bucket nombre-del-bucket-raw
```

Airflow incluye un DAG manual para backfills raw hacia S3:

```text
energy_raw_backfill_s3
```

Ejemplo de configuración al dispararlo manualmente:

```json
{
  "source": "all",
  "start_date": "2026-07-02",
  "end_date": "2026-07-03",
  "raw_bucket": "nombre-del-bucket-raw"
}
```

El pipeline diario está documentado en:

```text
docs/pipeline-operativo.md
```

Flujo actual:

```text
Airflow
  -> S3 Raw incremental
  -> Databricks Bronze/Silver incremental
  -> dbt Gold/tests
```

La integración continua con GitHub Actions está documentada en:

```text
docs/integracion-continua.md
```

Validación:

```bash
uv run pytest -q
uv run ruff check .
```

Ejecución con Docker:

```bash
docker compose run --rm app python -m energy_pipeline.ingest.ree --start-date 2026-01-01 --end-date 2026-01-01
docker compose run --rm app python -m energy_pipeline.ingest.aemet --start-date 2026-01-01 --end-date 2026-01-01
```

Validación con Docker:

```bash
docker compose run --rm app python -m pytest -q
docker compose run --rm app python -m ruff check .
```

---

## Qué Demuestra

- Diseño de una pipeline de datos con fuentes externas reales.
- Separación clara entre extracción, ingesta, almacenamiento y transformación.
- Trabajo con APIs, configuración segura y conservación de datos raw.
- Buenas prácticas de desarrollo: tests, logging, validaciones y manejo de
  errores.
- Evolución progresiva hacia una arquitectura cloud con herramientas habituales
  en Data Engineering.
