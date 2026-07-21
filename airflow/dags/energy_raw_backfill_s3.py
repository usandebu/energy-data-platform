import os
from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


APP_IMAGE = "energy-data-platform-app"
DOCKER_URL = "unix://var/run/docker.sock"
NETWORK_MODE = "energy-data-platform_default"
PROJECT_HOST_ROOT = os.environ.get("PROJECT_HOST_ROOT")
DEFAULT_RAW_BUCKET = os.environ.get("RAW_BUCKET", "energy-data-platform-dev-raw")
DEFAULT_AWS_REGION = os.environ.get("AWS_REGION", "eu-south-2")
RAW_BUCKET_ARG = "{{ dag_run.conf.get('raw_bucket', '" + DEFAULT_RAW_BUCKET + "') }}"
AWS_ENVIRONMENT = {
    key: value
    for key, value in {
        "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID"),
        "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY"),
        "AWS_SESSION_TOKEN": os.environ.get("AWS_SESSION_TOKEN"),
        "AWS_REGION": DEFAULT_AWS_REGION,
        "AWS_DEFAULT_REGION": DEFAULT_AWS_REGION,
    }.items()
    if value
}

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
    dag_id="energy_raw_backfill_s3",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["energy", "raw", "s3", "backfill"],
) as dag:
    backfill_raw_s3 = DockerOperator(
        task_id="backfill_raw_s3",
        image=APP_IMAGE,
        api_version="auto",
        auto_remove="success",
        command=(
            "python -m energy_pipeline.ingest.backfill "
            "--source {{ dag_run.conf.get('source', 'all') }} "
            "--start-date {{ dag_run.conf['start_date'] }} "
            "--end-date {{ dag_run.conf['end_date'] }} "
            "--raw-storage-backend s3 "
            f"--raw-bucket {RAW_BUCKET_ARG}"
        ),
        docker_url=DOCKER_URL,
        network_mode=NETWORK_MODE,
        mounts=[DATA_MOUNT, ENV_MOUNT],
        environment=AWS_ENVIRONMENT,
    )
