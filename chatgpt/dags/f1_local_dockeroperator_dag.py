from __future__ import annotations

import os
from datetime import datetime, timedelta

try:
    from airflow.sdk import dag
except Exception:
    from airflow.decorators import dag

from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

PROJECT_ROOT = os.environ.get("LOCAL_PROJECT_PATH", "").rstrip("/\\")
if not PROJECT_ROOT:
    PROJECT_ROOT = "/LOCAL_PROJECT_PATH_NOT_SET"

COMMON_MOUNTS = [
    Mount(source=f"{PROJECT_ROOT}/include", target="/app/include", type="bind", read_only=False),
    Mount(source=f"{PROJECT_ROOT}/orchestration", target="/app/orchestration", type="bind", read_only=True),
    Mount(source=f"{PROJECT_ROOT}/hitesh_dbt_project", target="/app/hitesh_dbt_project", type="bind", read_only=False),
    Mount(source=f"{PROJECT_ROOT}/logs", target="/app/logs", type="bind", read_only=False),
]

SNOWFLAKE_ENV = {
    "SNOWFLAKE_ACCOUNT": os.environ.get("SNOWFLAKE_ACCOUNT", ""),
    "SNOWFLAKE_USER": os.environ.get("SNOWFLAKE_USER", ""),
    "SNOWFLAKE_PASSWORD": os.environ.get("SNOWFLAKE_PASSWORD", ""),
    "SNOWFLAKE_ROLE": os.environ.get("SNOWFLAKE_ROLE", ""),
    "SNOWFLAKE_DATABASE": os.environ.get("SNOWFLAKE_DATABASE", ""),
    "SNOWFLAKE_WAREHOUSE": os.environ.get("SNOWFLAKE_WAREHOUSE", ""),
    "SNOWFLAKE_SCHEMA": os.environ.get("SNOWFLAKE_SCHEMA", "HITESH"),
    "PYTHONPATH": "/app",
    "DBT_PROFILES_DIR": "/app/hitesh_dbt_project",
}


@dag(
    dag_id="f1_historical_snowflake_dbt_local",
    description="OpenF1 API -> Snowflake raw/staging -> dbt Snowflake marts, executed through DockerOperator",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "local-f1",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["f1", "openf1", "snowflake", "dbt", "dockeroperator"],
)
def f1_historical_snowflake_dbt_local():
    extract_load = DockerOperator(
        task_id="openf1_extract_load_to_snowflake",
        image="f1-python-etl:local",
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="f1_net",
        mount_tmp_dir=False,
        mounts=COMMON_MOUNTS,
        environment=SNOWFLAKE_ENV,
        command=(
            "python /app/orchestration/run_historical_etl.py "
            "--year {{ dag_run.conf.get('year', 2024) }} "
            "--meeting-key {{ dag_run.conf.get('meeting_key', '') }} "
            "--session-key {{ dag_run.conf.get('session_key', '') }} "
            "--limit-meetings {{ dag_run.conf.get('limit_meetings', 1) }} "
            "--limit-sessions {{ dag_run.conf.get('limit_sessions', 1) }} "
            "--endpoints {{ dag_run.conf.get('endpoints', 'drivers,laps,pit,position,intervals,stints,weather') }}"
        ),
    )

    dbt_deps = DockerOperator(
        task_id="dbt_deps",
        image="f1-dbt-snowflake:local",
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="f1_net",
        mount_tmp_dir=False,
        mounts=COMMON_MOUNTS,
        environment=SNOWFLAKE_ENV,
        command="cd /app/hitesh_dbt_project && dbt deps --profiles-dir /app/hitesh_dbt_project",
    )

    dbt_run = DockerOperator(
        task_id="dbt_run_snowflake_models",
        image="f1-dbt-snowflake:local",
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="f1_net",
        mount_tmp_dir=False,
        mounts=COMMON_MOUNTS,
        environment=SNOWFLAKE_ENV,
        command="cd /app/hitesh_dbt_project && dbt run --profiles-dir /app/hitesh_dbt_project",
    )

    dbt_test = DockerOperator(
        task_id="dbt_test_snowflake_models",
        image="f1-dbt-snowflake:local",
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="f1_net",
        mount_tmp_dir=False,
        mounts=COMMON_MOUNTS,
        environment=SNOWFLAKE_ENV,
        command="cd /app/hitesh_dbt_project && dbt test --profiles-dir /app/hitesh_dbt_project",
    )

    extract_load >> dbt_deps >> dbt_run >> dbt_test


f1_historical_snowflake_dbt_local()
