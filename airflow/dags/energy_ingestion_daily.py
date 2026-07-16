import os
from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


APP_IMAGE = "energy-data-platform-app"
DOCKER_URL = "unix://var/run/docker.sock"
NETWORK_MODE = "energy-data-platform_default"
PROJECT_HOST_ROOT = os.environ.get("PROJECT_HOST_ROOT")

if not PROJECT_HOST_ROOT:
    raise RuntimeError("PROJECT_HOST_ROOT environment variable is required")

DATA_MOUNT = Mount(
    source=f"{PROJECT_HOST_ROOT}/data",
    target="/app/data",
    type="bind",
)

ENV_MOUNT = Mount(
    source=f"{PROJECT_HOST_ROOT}/.env",
    target="/app/.env",
    type="bind",
    read_only=True,
)


with DAG(
    dag_id="energy_ingestion_daily",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["energy", "ingestion"],
) as dag:
    ingest_ree = DockerOperator(
        task_id="ingest_ree",
        image=APP_IMAGE,
        api_version="auto",
        auto_remove="success",
        command=(
            "python -m energy_pipeline.ingest.ree "
            "--start-date {{ dag_run.conf.get('date', ds) }} "
            "--end-date {{ dag_run.conf.get('date', ds) }}"
        ),
        docker_url=DOCKER_URL,
        network_mode=NETWORK_MODE,
        mounts=[DATA_MOUNT, ENV_MOUNT],
    )

    ingest_aemet = DockerOperator(
        task_id="ingest_aemet",
        image=APP_IMAGE,
        api_version="auto",
        auto_remove="success",
        command=(
            "python -m energy_pipeline.ingest.aemet "
            "--start-date {{ dag_run.conf.get('date', ds) }} "
            "--end-date {{ dag_run.conf.get('date', ds) }}"
        ),
        docker_url=DOCKER_URL,
        network_mode=NETWORK_MODE,
        mounts=[DATA_MOUNT, ENV_MOUNT],
    )

    ingest_ree >> ingest_aemet
