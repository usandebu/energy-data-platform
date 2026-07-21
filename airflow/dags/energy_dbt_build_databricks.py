import os
from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


DBT_IMAGE = "energy-data-platform-dbt"
DOCKER_URL = "unix://var/run/docker.sock"
NETWORK_MODE = "energy-data-platform_default"
PROJECT_HOST_ROOT = os.environ.get("PROJECT_HOST_ROOT")

DBT_ENVIRONMENT = {
    key: value
    for key, value in {
        "DATABRICKS_HOST": os.environ.get("DATABRICKS_HOST"),
        "DATABRICKS_HTTP_PATH": os.environ.get("DATABRICKS_HTTP_PATH"),
        "DATABRICKS_TOKEN": os.environ.get("DATABRICKS_TOKEN"),
        "DBT_PROFILES_DIR": "/workspace/dbt",
    }.items()
    if value
}

if not PROJECT_HOST_ROOT:
    raise RuntimeError("PROJECT_HOST_ROOT environment variable is required")

PROJECT_MOUNT = Mount(
    source=PROJECT_HOST_ROOT,
    target="/workspace",
    type="bind",
)


with DAG(
    dag_id="energy_dbt_build_databricks",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["energy", "dbt", "databricks", "gold"],
) as dag:
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
