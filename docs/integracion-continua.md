# Integración continua

GitHub Actions permite ejecutar comprobaciones automáticas cada vez que se hace
un `push` o se abre una `pull request`.

En este proyecto se usa para validar que el código sigue en buen estado antes
de mezclar cambios en `main`.

## Workflow

El workflow está en:

```text
.github/workflows/ci.yml
```

Se ejecuta en:

```text
push a main
pull request contra main
```

## Qué comprueba

El workflow hace estas validaciones:

```text
ruff check src tests airflow/dags
pytest -q tests
dbt parse
```

Esto cubre tres partes:

- estilo y errores básicos de Python con Ruff;
- tests unitarios con pytest;
- validación de estructura, macros y modelos de dbt con `dbt parse`.

## Por qué no ejecuta Databricks

El CI no ejecuta Jobs reales de Databricks ni hace `dbt build`.

Motivo:

- requeriría credenciales reales;
- podría generar coste;
- dependería de servicios externos;
- haría que el CI fuera más lento y frágil.

Para esta fase, el objetivo del CI es comprobar que el repositorio está bien
formado y que los tests pasan. La ejecución real del pipeline queda en Airflow y
Databricks.

## Perfil dbt para CI

El perfil de dbt usado por GitHub Actions está en:

```text
dbt/ci/profiles.yml
```

No son credenciales reales. Solo permiten que `dbt parse` pueda cargar el
proyecto sin conectarse a Databricks.

## Probarlo en local

Las dos primeras comprobaciones se pueden lanzar así:

```bash
uv run ruff check src tests airflow/dags
uv run pytest -q tests
```

Para probar dbt parse usando Docker:

```bash
docker compose --profile analytics run --rm dbt parse --profiles-dir /workspace/dbt/ci
```

Ese comando necesita que las variables obligatorias de Docker Compose estén
definidas en el entorno, aunque sean valores de prueba cuando solo se quiere
validar el parseo.
