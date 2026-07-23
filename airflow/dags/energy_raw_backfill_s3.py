import os
from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


APP_IMAGE = "energy-data-platform-app"
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
RAW_BUCKET_ARG = "{{ dag_run.conf.get('raw_bucket', '" + RAW_BUCKET + "') }}"
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

DATA_MOUNT = Mount(
    source=f"{PROJECT_HOST_ROOT}/data",
    target="/app/data",
    type="bind",
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
        mounts=[DATA_MOUNT],
        environment=AWS_ENVIRONMENT,
    )
