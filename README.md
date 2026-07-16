# Energy Data Platform

Plataforma de datos para trabajar con información energética y meteorológica de España.

El proyecto parte de dos fuentes principales: datos de generación eléctrica de REE y datos meteorológicos de AEMET. La idea es construir una pipeline sólida, cercana a un entorno real, que permita extraer, validar, transformar y analizar estos datos de forma reproducible.

## Objetivo

Analizar cómo influyen variables como la temperatura, el viento o las horas de sol en la generación renovable en España.

## Stack previsto

- Python
- Docker
- Airflow
- dbt
- DuckDB
- AWS S3 / Athena / Glue
- Terraform
- PySpark

## Estado

Proyecto en Fase 1: ingesta local.

Ya existe una primera ingesta de REE que descarga el balance eléctrico y guarda
la respuesta original como JSON raw mediante escritura atómica.

## Ejecución local

Puedes preparar la configuración local copiando el ejemplo:

```bash
cp .env.example .env
```

```bash
make ingest START_DATE=2024-01-01 END_DATE=2024-01-07
```

El archivo se guarda por defecto en:

```text
data/raw/ree/balance-electrico/2024-01-01_2024-01-07.json
```

También se puede cambiar la raíz de almacenamiento:

```bash
make ingest START_DATE=2024-01-01 END_DATE=2024-01-07 RAW_ROOT=/tmp/energy-raw
```

Si `RAW_ROOT` está definido en `.env`, la CLI lo usa cuando no se pasa
`RAW_ROOT` por comando.

## Validación

```bash
make test
make lint
```
