import os
from datetime import datetime

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


APP_IMAGE = "energy-data-platform-app"
DBT_IMAGE = "energy-data-platform-dbt"
DOCKER_URL = "unix://var/run/docker.sock"
NETWORK_MODE = "energy-data-platform_default"


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


PROJECT_HOST_ROOT = required_env("PROJECT_HOST_ROOT")
RAW_BUCKET = required_env("RAW_BUCKET")
AWS_REGION = required_env("AWS_REGION")
BRONZE_SILVER_JOB_ID = int(required_env("DATABRICKS_BRONZE_SILVER_JOB_ID"))

AWS_ENVIRONMENT = {
    key: value
    for key, value in {
        "AWS_ACCESS_KEY_ID": required_env("AWS_ACCESS_KEY_ID"),
        "AWS_SECRET_ACCESS_KEY": required_env("AWS_SECRET_ACCESS_KEY"),
        "AWS_SESSION_TOKEN": os.environ.get("AWS_SESSION_TOKEN"),
        "AWS_REGION": AWS_REGION,
        "AWS_DEFAULT_REGION": AWS_REGION,
        "AEMET_API_KEY": required_env("AEMET_API_KEY"),
    }.items()
    if value
}

DBT_ENVIRONMENT = {
    key: value
    for key, value in {
        "DATABRICKS_HOST": required_env("DATABRICKS_HOST"),
        "DATABRICKS_HTTP_PATH": required_env("DATABRICKS_HTTP_PATH"),
        "DATABRICKS_TOKEN": required_env("DATABRICKS_TOKEN"),
        "DBT_PROFILES_DIR": "/workspace/dbt",
    }.items()
    if value
}

DATA_MOUNT = Mount(
    source=f"{PROJECT_HOST_ROOT}/data",
    target="/app/data",
    type="bind",
)

PROJECT_MOUNT = Mount(
    source=PROJECT_HOST_ROOT,
    target="/workspace",
    type="bind",
)


with DAG(
    dag_id="energy_daily_incremental_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="0 7 * * *",
    catchup=False,
    tags=["energy", "daily", "s3", "databricks", "dbt"],
) as dag:
    ingest_incremental_raw_s3 = DockerOperator(
        task_id="ingest_incremental_raw_s3",
        image=APP_IMAGE,
        api_version="auto",
        auto_remove="success",
        command=(
            "python -m energy_pipeline.ingest.incremental "
            "--source all "
            "--raw-storage-backend s3 "
            f"--raw-bucket {RAW_BUCKET}"
        ),
        docker_url=DOCKER_URL,
        network_mode=NETWORK_MODE,
        mounts=[DATA_MOUNT],
        environment=AWS_ENVIRONMENT,
    )

    run_bronze_silver_job = DatabricksRunNowOperator(
        task_id="run_bronze_silver_job",
        databricks_conn_id="databricks_default",
        job_id=BRONZE_SILVER_JOB_ID,
    )

    dbt_build = DockerOperator(
        task_id="dbt_build",
        image=DBT_IMAGE,
        api_version="auto",
        auto_remove="success",
        command="build",
        docker_url=DOCKER_URL,
        network_mode=NETWORK_MODE,
        mounts=[PROJECT_MOUNT],
        working_dir="/workspace/dbt/energy_platform",
        environment=DBT_ENVIRONMENT,
    )

    ingest_incremental_raw_s3 >> run_bronze_silver_job >> dbt_build
