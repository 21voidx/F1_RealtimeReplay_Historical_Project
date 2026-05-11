# f1_data_pipeline_dockeroperator_dockerfile_a.py

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# from include.hitesh.scripts.f1_snowflake_etl_2 import F1DataIngestion
# from include.eczachly.snowflake_queries import get_snowpark_session
from helper.f1_snowflake_etl_2 import F1DataIngestion
from helper.snowflake_queries import get_snowpark_session


# ─── dbt config mengikuti Dockerfile A ────────────────────────────────────────
# Dockerfile A:
# - dbt project berada di /app
# - profiles.yml berada di /root/.dbt
# - dbt packages di-install saat build image lewat dbt deps

DBT_IMAGE = os.getenv("DBT_DOCKER_IMAGE", "f1-dbt-snowflake:1.0")
DBT_TARGET = os.getenv("DBT_TARGET", "dev")

DBT_PROJECT_DIR = "/app"
DBT_PROFILES_DIR = "/root/.dbt"

DBT_BASE = (
    f"--project-dir {DBT_PROJECT_DIR} "
    f"--profiles-dir {DBT_PROFILES_DIR} "
    f"--target {DBT_TARGET}"
)

# Optional untuk development.
# Jika variabel ini diisi, folder dbt_project dari host akan di-mount ke /app.
# Jika tidak diisi, dbt memakai project yang sudah di-COPY ke image saat build Dockerfile A.
#
# Contoh:
# export DBT_PROJECT_HOST=/home/void/f1-pipeline/dbt/dbt_project
DBT_PROJECT_HOST = os.getenv("DBT_PROJECT_HOST", "/home/void/F1_RealtimeReplay_Historical_Project/dbt/dbt_project")

DBT_MOUNTS = []
if DBT_PROJECT_HOST:
    DBT_MOUNTS.append(
        Mount(
            source=DBT_PROJECT_HOST,
            target=DBT_PROJECT_DIR,
            type="bind",
        )
    )


SNOWFLAKE_ENV_KEYS = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_SCHEMA",
]


def get_dbt_environment() -> Dict[str, str]:
    """
    Environment untuk dbt container.

    profiles.yml sebaiknya memakai env_var(), contoh:
    account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
    user: "{{ env_var('SNOWFLAKE_USER') }}"
    password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
    """
    env = {
        "DBT_TARGET": DBT_TARGET,
        "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
    }

    for key in SNOWFLAKE_ENV_KEYS:
        value = os.getenv(key)
        if value:
            env[key] = value

    return env


DOCKER_COMMON = dict(
    image=DBT_IMAGE,
    auto_remove="force",
    mount_tmp_dir=False,
    network_mode=os.getenv("DBT_DOCKER_NETWORK_MODE", "bridge"),
    environment=get_dbt_environment(),
    mounts=DBT_MOUNTS,
)


# ─── Helper functions ─────────────────────────────────────────────────────────

def get_f1_calendar(year: int) -> List[Dict[str, Any]]:
    """Get all race meetings for a given year."""
    try:
        session = get_snowpark_session()
        ingestion = F1DataIngestion(session)

        meetings = ingestion._make_request("meetings", {"year": year})
        if meetings:
            meetings.sort(key=lambda x: x["date_start"])
            return meetings

        return []
    except Exception as e:
        logging.error("Error getting F1 calendar for year %s: %s", year, str(e))
        return []


def get_all_sessions(year: int) -> List[Dict[str, List[Any]]]:
    """Get all session dates for a given year."""
    try:
        session = get_snowpark_session()
        ingestion = F1DataIngestion(session)

        sessions = ingestion._make_request("sessions", {"year": year})
        if sessions:
            sessions.sort(key=lambda x: x["date_start"])
            return [
                {
                    datetime.fromisoformat(
                        item["date_start"].replace("Z", "+00:00")
                    ).strftime("%Y-%m-%d"): [
                        item["meeting_key"],
                        item["session_key"],
                    ]
                }
                for item in sessions
            ]

        return []
    except Exception as e:
        logging.error("Error getting session dates for year %s: %s", year, str(e))
        return []


# Tetap mengikuti pola DAG asli.
# Catatan:
# Kode ini mengambil jadwal session saat DAG di-parse.
# Untuk production, lebih aman memindahkan fetch API ini ke dalam task.
f1_calendar_2024 = get_f1_calendar(2024)
first_race_date = None
if f1_calendar_2024:
    first_race_date = datetime.fromisoformat(
        f1_calendar_2024[0]["date_start"].replace("Z", "+00:00")
    )
    logging.info("First race of 2024 is on %s", first_race_date)

session_dates_2024 = get_all_sessions(2024)
session_dates = sorted(
    list(
        {
            date
            for date_dict in session_dates_2024
            for date in date_dict.keys()
        }
    )
)


@dag(
    dag_id="f1_data_pipeline_backfill_dockeroperator",
    description="F1 Data Ingestion Pipeline - Open Source Airflow + DockerOperator dbt",
    default_args={
        "owner": "Hitesh Kaushik",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "depends_on_past": False,
        "execution_timeout": timedelta(hours=1),
        "email_on_retry": True,
    },
    start_date=datetime(2026, 5, 10),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    max_active_tasks=3,
    tags=["f1", "snowpark", "etl", "racing", "dbt", "dockeroperator"],
)
def f1_data_pipeline_backfill_dockeroperator():
    @task
    def initialize_ingestion(**context):
        execution_date = context["data_interval_start"]
        logging.info("Processing date: %s", execution_date)

        date_str = execution_date.strftime("%Y-%m-%d")
        logging.info("Checking if %s is a session date", date_str)

        if date_str not in session_dates:
            logging.info("Skipping non-session day %s", date_str)
            return None

        matching_sessions = []
        for session_dict in session_dates_2024:
            if date_str in session_dict:
                meeting_key = session_dict[date_str][0]
                session_key = session_dict[date_str][1]
                matching_sessions.append(
                    {
                        "meeting_key": meeting_key,
                        "session_key": session_key,
                        "execution_date": execution_date.isoformat(),
                    }
                )

        if not matching_sessions:
            logging.info("No sessions found for date %s", date_str)
            return None

        logging.info("Found %s sessions for %s", len(matching_sessions), date_str)
        return {
            "execution_date": execution_date.isoformat(),
            "sessions": matching_sessions,
            "status": "initialized",
            "is_race_day": True,
        }

    @task
    def get_meeting_data(init_status: dict):
        if not init_status:
            logging.info("No valid init status, skipping meeting data fetch")
            return None

        session = get_snowpark_session()
        ingestion = F1DataIngestion(session)
        ingestion.execution_date = datetime.fromisoformat(
            init_status["execution_date"]
        ).date()

        meetings = []
        for session_info in init_status["sessions"]:
            params = {"meeting_key": session_info["meeting_key"]}

            try:
                data = ingestion._make_request("meetings", params)
                if data:
                    meeting = data[0]
                    if not ingestion.record_exists(
                        "F1_MEETINGS",
                        meeting["meeting_key"],
                    ):
                        ingestion.create_table("F1_MEETINGS", meeting)
                        ingestion.load_data("F1_MEETINGS", [meeting])
                        logging.info("Loaded new meeting %s", meeting["meeting_key"])

                    meetings.append(meeting)
            except Exception as e:
                logging.error(
                    "Error processing meeting %s: %s",
                    session_info["meeting_key"],
                    str(e),
                )

        return meetings if meetings else None

    @task
    def get_sessions(meetings: list):
        if not meetings:
            logging.info("No meeting data provided, skipping sessions fetch")
            return []

        session = get_snowpark_session()
        ingestion = F1DataIngestion(session)

        all_sessions = []
        for meeting in meetings:
            try:
                sessions = ingestion.get_session_data(meeting["meeting_key"])
                if sessions:
                    ingestion.create_table("F1_SESSIONS", sessions[0])
                    for sess in sessions:
                        if not ingestion.record_exists(
                            "F1_SESSIONS",
                            meeting["meeting_key"],
                            sess["session_key"],
                        ):
                            ingestion.load_data("F1_SESSIONS", [sess])
                            logging.info("Loaded new session %s", sess["session_key"])

                    all_sessions.extend(sessions)
            except Exception as e:
                logging.error(
                    "Error processing sessions for meeting %s: %s",
                    meeting["meeting_key"],
                    str(e),
                )

        return all_sessions

    @task(trigger_rule="none_failed")
    def process_endpoint(
        sessions_list: List[dict],
        init_data: dict,
        endpoint: str,
    ) -> Dict[str, Any]:
        """Process endpoint data for race sessions."""
        if init_data is None:
            return {
                "endpoint": endpoint,
                "processed": False,
                "reason": "no_init_data",
                "results": [],
            }

        results = []
        session = get_snowpark_session()
        ingestion = F1DataIngestion(session)
        execution_date = datetime.fromisoformat(init_data["execution_date"]).date()
        ingestion.execution_date = execution_date

        table_name = f"F1_{endpoint.upper()}"

        for session_info in sessions_list:
            session_key = session_info["session_key"]
            meeting_key = session_info["meeting_key"]

            try:
                if ingestion.record_exists(table_name, meeting_key, session_key):
                    results.append(
                        {
                            "session_key": session_key,
                            "status": "skipped",
                            "reason": "data_exists",
                            "records": 0,
                        }
                    )
                    continue

                data = ingestion.get_session_endpoint_data(session_info, endpoint)

                # Penting:
                # get_session_endpoint_data() untuk car_data sudah melakukan load_data()
                # secara internal dan mengembalikan summary dict, bukan list record mentah.
                if endpoint == "car_data":
                    if isinstance(data, dict):
                        results.append(
                            {
                                "session_key": session_key,
                                "status": data.get("status", "unknown"),
                                "records": data.get("records", 0),
                                "meeting_key": meeting_key,
                                "error_windows": data.get("error_windows"),
                            }
                        )
                    else:
                        results.append(
                            {
                                "session_key": session_key,
                                "status": "error",
                                "reason": "unexpected_car_data_payload",
                                "records": 0,
                                "meeting_key": meeting_key,
                            }
                        )
                    continue

                if not data:
                    results.append(
                        {
                            "session_key": session_key,
                            "status": "skipped",
                            "reason": "no_data",
                            "records": 0,
                        }
                    )
                    continue

                if not ingestion.table_exists(table_name):
                    ingestion.create_table(table_name, data[0])

                ingestion.load_data(table_name, data)

                results.append(
                    {
                        "session_key": session_key,
                        "status": "success",
                        "records": len(data),
                        "meeting_key": meeting_key,
                    }
                )

            except Exception as e:
                results.append(
                    {
                        "session_key": session_key,
                        "status": "error",
                        "reason": str(e),
                        "meeting_key": meeting_key,
                    }
                )

        return {
            "endpoint": endpoint,
            "processed": True,
            "execution_date": execution_date.isoformat(),
            "results": results,
        }

    @task(trigger_rule="none_failed")
    def process_race_data(
        sessions_list: List[dict],
        init_data: dict,
    ) -> Dict[str, Any]:
        """Process race-specific data, such as positions and intervals."""
        if not init_data or not init_data.get("is_race_day"):
            return {
                "processed": False,
                "reason": "not_race_day",
                "results": [],
            }

        results = []
        session = get_snowpark_session()
        ingestion = F1DataIngestion(session)
        execution_date = datetime.fromisoformat(init_data["execution_date"]).date()
        ingestion.execution_date = execution_date

        for session_info in sessions_list:
            session_key = session_info["session_key"]
            meeting_key = session_info["meeting_key"]

            try:
                if not ingestion.record_exists("F1_INTERVALS", meeting_key, session_key):
                    data = ingestion.get_intervals_data(session_key)
                    if data:
                        if not ingestion.table_exists("F1_INTERVALS"):
                            ingestion.create_table("F1_INTERVALS", data[0])

                        ingestion.load_data("F1_INTERVALS", data)
                        results.append(
                            {
                                "endpoint": "intervals",
                                "session_key": session_key,
                                "status": "success",
                                "records": len(data),
                            }
                        )
            except Exception as e:
                results.append(
                    {
                        "endpoint": "intervals",
                        "session_key": session_key,
                        "status": "error",
                        "error": str(e),
                    }
                )

            try:
                if not ingestion.record_exists("F1_POSITION", meeting_key, session_key):
                    data = ingestion.get_position_data(session_key)
                    if data:
                        if not ingestion.table_exists("F1_POSITION"):
                            ingestion.create_table("F1_POSITION", data[0])

                        ingestion.load_data("F1_POSITION", data)
                        results.append(
                            {
                                "endpoint": "position",
                                "session_key": session_key,
                                "status": "success",
                                "records": len(data),
                            }
                        )
            except Exception as e:
                results.append(
                    {
                        "endpoint": "position",
                        "session_key": session_key,
                        "status": "error",
                        "error": str(e),
                    }
                )

        return {
            "processed": True,
            "execution_date": execution_date.isoformat(),
            "results": results,
        }

    @task(trigger_rule="none_failed")
    def process_technical_data(
        sessions_list: List[dict],
        init_data: dict,
    ) -> Dict[str, Any]:
        """Process technical data, such as stints, pit stops, and race control."""
        if not init_data:
            return {
                "processed": False,
                "reason": "no_init_data",
                "results": [],
            }

        results = []
        session = get_snowpark_session()
        ingestion = F1DataIngestion(session)
        execution_date = datetime.fromisoformat(init_data["execution_date"]).date()
        ingestion.execution_date = execution_date

        technical_endpoints = ["stints", "pit", "race_control"]

        for session_info in sessions_list:
            session_key = session_info["session_key"]
            meeting_key = session_info["meeting_key"]

            for endpoint in technical_endpoints:
                table_name = f"F1_{endpoint.upper()}"
                try:
                    if ingestion.record_exists(table_name, meeting_key, session_key):
                        continue

                    if endpoint == "stints":
                        data = ingestion.get_stints_data(session_key)
                    elif endpoint == "pit":
                        data = ingestion.get_pit_data(session_key)
                    else:
                        data = ingestion.get_race_control_data(session_key)

                    if data:
                        if not ingestion.table_exists(table_name):
                            ingestion.create_table(table_name, data[0])

                        ingestion.load_data(table_name, data)

                        results.append(
                            {
                                "endpoint": endpoint,
                                "session_key": session_key,
                                "status": "success",
                                "records": len(data),
                            }
                        )

                except Exception as e:
                    results.append(
                        {
                            "endpoint": endpoint,
                            "session_key": session_key,
                            "status": "error",
                            "error": str(e),
                        }
                    )

        return {
            "processed": True,
            "execution_date": execution_date.isoformat(),
            "results": results,
        }

    @task(trigger_rule="all_done")
    def cleanup_staging():
        """Clean up any remaining staging tables."""
        try:
            session = get_snowpark_session()
            ingestion = F1DataIngestion(session)
            ingestion.cleanup_staging_tables()
            logging.info("Completed staging table cleanup")
        except Exception as e:
            logging.error("Error in staging table cleanup task: %s", str(e))

    pre_dbt_workflow = EmptyOperator(
        task_id="pre_dbt_workflow",
        trigger_rule="all_done",
    )

    # ─── dbt tasks via DockerOperator ─────────────────────────────────────────
    # Menggantikan Cosmos DbtTaskGroup.
    # Path mengikuti Dockerfile A:
    # - --project-dir /app
    # - --profiles-dir /root/.dbt

    dbt_debug = DockerOperator(
        task_id="dbt_debug",
        command=f"dbt debug {DBT_BASE}",
        **DOCKER_COMMON,
    )

    dbt_deps = DockerOperator(
        task_id="dbt_deps",
        command=f"dbt deps {DBT_BASE}",
        **DOCKER_COMMON,
    )

    dbt_build_staging = DockerOperator(
        task_id="dbt_build_staging",
        command=f"dbt build --select staging.stg_f1_* {DBT_BASE}",
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=30),
        **DOCKER_COMMON,
    )

    dbt_build_intermediate = DockerOperator(
        task_id="dbt_build_intermediate",
        command=f"dbt build --select intermediate.int_* {DBT_BASE}",
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=30),
        **DOCKER_COMMON,
    )

    dbt_build_marts = DockerOperator(
        task_id="dbt_build_marts",
        command=f"dbt build --select marts.f1_* {DBT_BASE}",
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=30),
        **DOCKER_COMMON,
    )

    post_dbt_workflow = EmptyOperator(
        task_id="post_dbt_workflow",
        trigger_rule="all_done",
        retries=2,
    )

    # ─── ETL task graph ───────────────────────────────────────────────────────

    init_data = initialize_ingestion()
    meeting_data = get_meeting_data(init_data)
    sessions_data = get_sessions(meeting_data)

    endpoints = ["drivers", "car_data", "laps", "team_radio", "weather"]
    endpoint_tasks = []

    for endpoint_name in endpoints:
        current_task = process_endpoint.override(
            task_id=f"process_{endpoint_name}"
        )(
            sessions_data,
            init_data,
            endpoint_name,
        )
        endpoint_tasks.append(current_task)

    race_data = process_race_data(sessions_data, init_data)
    technical_data = process_technical_data(sessions_data, init_data)
    cleanup = cleanup_staging()

    init_data >> meeting_data >> sessions_data

    for endpoint_task in endpoint_tasks:
        sessions_data >> endpoint_task >> cleanup

    sessions_data >> race_data >> cleanup
    sessions_data >> technical_data >> cleanup

    cleanup >> pre_dbt_workflow
    pre_dbt_workflow >> dbt_debug >> dbt_deps
    dbt_deps >> dbt_build_staging >> dbt_build_intermediate >> dbt_build_marts
    dbt_build_marts >> post_dbt_workflow


dag = f1_data_pipeline_backfill_dockeroperator()
