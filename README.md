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
- AWS S3, Glue Data Catalog y Athena
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
        +--> Glue Data Catalog / Athena
        |
        +--> dbt
        |
        +--> Databricks / PySpark
        |
        v
Modelos analíticos y datasets preparados para análisis
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
uv run python -m energy_pipeline.ingest.ree --start-date 2024-01-01 --end-date 2024-01-07
```

Ingesta AEMET:

```bash
uv run python -m energy_pipeline.ingest.aemet --start-date 2024-01-01 --end-date 2024-01-07
```

Validación:

```bash
uv run pytest -q
uv run ruff check .
```

Ejecución con Docker:

```bash
docker compose run --rm app python -m energy_pipeline.ingest.ree --start-date 2024-01-01 --end-date 2024-01-07
docker compose run --rm app python -m energy_pipeline.ingest.aemet --start-date 2024-01-01 --end-date 2024-01-07
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
