# f1_data_pipeline_cosmos_revised.py

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)

from include.eczachly.snowflake_queries import get_snowpark_session
from include.hitesh.scripts.f1_snowflake_etl_2 import F1DataIngestion


AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow"))
PATH_TO_DBT_PROJECT = AIRFLOW_HOME / "dbt" / "dbt_project"
PATH_TO_DBT_PROFILES = AIRFLOW_HOME / "dbt" / "dbt_profiles" / "profiles.yml"
PATH_TO_DBT_EXECUTABLE = Path("/opt/dbt_venv/bin/dbt")

# Cosmos config:
# - ProjectConfig = where the dbt project lives
# - ProfileConfig = how dbt connects to Snowflake
# - ExecutionConfig = where/how Cosmos runs dbt at runtime
project_config = ProjectConfig(
    dbt_project_path=str(PATH_TO_DBT_PROJECT),
)

profile_config = ProfileConfig(
    profile_name="formula_one",
    target_name="dev",
    profiles_yml_filepath=str(PATH_TO_DBT_PROFILES),
)

execution_config = ExecutionConfig(
    dbt_executable_path=str(PATH_TO_DBT_EXECUTABLE),
)

render_config = RenderConfig(
    dbt_executable_path=str(PATH_TO_DBT_EXECUTABLE),
    select=[
        "staging.stg_f1_*",
        "intermediate.int_*",
        "marts.f1_*",
    ],
)


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
    except Exception as exc:
        logging.exception("Error getting F1 calendar for year %s: %s", year, exc)
        return []


def get_all_sessions(year: int) -> List[Dict[str, Any]]:
    """Get all F1 session metadata for a given year."""
    try:
        session = get_snowpark_session()
        ingestion = F1DataIngestion(session)

        sessions = ingestion._make_request("sessions", {"year": year})
        if sessions:
            sessions.sort(key=lambda x: x["date_start"])
            return sessions
        return []
    except Exception as exc:
        logging.exception("Error getting session dates for year %s: %s", year, exc)
        return []


@dag(
    dag_id="f1_data_pipeline_backfill",
    description="F1 Data Ingestion Pipeline - Session Days Only",
    default_args={
        "owner": "Hitesh Kaushik",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "depends_on_past": False,
        "execution_timeout": timedelta(hours=1),
        "email_on_retry": True,
    },
    start_date=datetime(2024, 12, 7),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    max_active_tasks=3,
    tags=["f1", "snowpark", "etl", "racing", "dbt", "cosmos"],
)
def f1_data_pipeline_backfill():
    @task
    def initialize_ingestion(**context) -> Dict[str, Any]:
        execution_date = context["data_interval_start"]
        date_str = execution_date.strftime("%Y-%m-%d")
        year = execution_date.year

        logging.info("Processing logical date: %s", date_str)
        logging.info("Fetching F1 sessions for year: %s", year)

        all_sessions = get_all_sessions(year)
        matching_sessions = []

        for session_data in all_sessions:
            session_date = datetime.fromisoformat(
                session_data["date_start"].replace("Z", "+00:00")
            ).strftime("%Y-%m-%d")

            if session_date == date_str:
                matching_sessions.append(
                    {
                        "meeting_key": session_data["meeting_key"],
                        "session_key": session_data["session_key"],
                        "execution_date": execution_date.isoformat(),
                    }
                )

        if not matching_sessions:
            raise AirflowSkipException(f"{date_str} is not an F1 session day. Skipping pipeline.")

        logging.info("Found %s F1 sessions for %s", len(matching_sessions), date_str)
        return {
            "execution_date": execution_date.isoformat(),
            "sessions": matching_sessions,
            "status": "initialized",
            "is_session_day": True,
        }

    @task
    def get_meeting_data(init_status: Dict[str, Any]) -> List[Dict[str, Any]]:
        session = get_snowpark_session()
        ingestion = F1DataIngestion(session)
        ingestion.execution_date = datetime.fromisoformat(init_status["execution_date"]).date()

        meetings = []
        for session_info in init_status["sessions"]:
            try:
                data = ingestion._make_request(
                    "meetings",
                    {"meeting_key": session_info["meeting_key"]},
                )
                if not data:
                    continue

                meeting = data[0]
                if not ingestion.record_exists("F1_MEETINGS", meeting["meeting_key"]):
                    ingestion.create_table("F1_MEETINGS", meeting)
                    ingestion.load_data("F1_MEETINGS", [meeting])
                    logging.info("Loaded new meeting %s", meeting["meeting_key"])

                meetings.append(meeting)
            except Exception as exc:
                logging.exception(
                    "Error processing meeting %s: %s",
                    session_info["meeting_key"],
                    exc,
                )

        if not meetings:
            raise AirflowSkipException("No meeting data found for this session day.")

        return meetings

    @task
    def get_sessions(meetings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        session = get_snowpark_session()
        ingestion = F1DataIngestion(session)

        all_sessions = []
        for meeting in meetings:
            try:
                sessions = ingestion.get_session_data(meeting["meeting_key"])
                if not sessions:
                    continue

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
            except Exception as exc:
                logging.exception(
                    "Error processing sessions for meeting %s: %s",
                    meeting["meeting_key"],
                    exc,
                )

        if not all_sessions:
            raise AirflowSkipException("No session rows were loaded or found.")

        return all_sessions

    @task
    def process_endpoint(
        sessions_list: List[Dict[str, Any]],
        init_data: Dict[str, Any],
        endpoint: str,
    ) -> Dict[str, Any]:
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
            except Exception as exc:
                results.append(
                    {
                        "session_key": session_key,
                        "status": "error",
                        "reason": str(exc),
                        "meeting_key": meeting_key,
                    }
                )
                logging.exception("Error processing endpoint %s for session %s", endpoint, session_key)

        return {
            "endpoint": endpoint,
            "processed": True,
            "execution_date": execution_date.isoformat(),
            "results": results,
        }

    @task
    def process_race_data(
        sessions_list: List[Dict[str, Any]],
        init_data: Dict[str, Any],
    ) -> Dict[str, Any]:
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
            except Exception as exc:
                results.append(
                    {
                        "endpoint": "intervals",
                        "session_key": session_key,
                        "status": "error",
                        "error": str(exc),
                    }
                )
                logging.exception("Error processing intervals for session %s", session_key)

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
            except Exception as exc:
                results.append(
                    {
                        "endpoint": "position",
                        "session_key": session_key,
                        "status": "error",
                        "error": str(exc),
                    }
                )
                logging.exception("Error processing position for session %s", session_key)

        return {
            "processed": True,
            "execution_date": execution_date.isoformat(),
            "results": results,
        }

    @task
    def process_technical_data(
        sessions_list: List[Dict[str, Any]],
        init_data: Dict[str, Any],
    ) -> Dict[str, Any]:
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
                except Exception as exc:
                    results.append(
                        {
                            "endpoint": endpoint,
                            "session_key": session_key,
                            "status": "error",
                            "error": str(exc),
                        }
                    )
                    logging.exception("Error processing %s for session %s", endpoint, session_key)

        return {
            "processed": True,
            "execution_date": execution_date.isoformat(),
            "results": results,
        }

    @task(trigger_rule="none_failed_min_one_success")
    def cleanup_staging() -> None:
        try:
            session = get_snowpark_session()
            ingestion = F1DataIngestion(session)
            ingestion.cleanup_staging_tables()
            logging.info("Completed staging table cleanup")
        except Exception as exc:
            logging.exception("Error in staging table cleanup task: %s", exc)
            raise

    @task
    def validate_dbt_environment() -> None:
        required_paths = {
            "dbt project": PATH_TO_DBT_PROJECT / "dbt_project.yml",
            "dbt profiles.yml": PATH_TO_DBT_PROFILES,
            "dbt executable": PATH_TO_DBT_EXECUTABLE,
        }

        missing_paths = [f"{name}: {path}" for name, path in required_paths.items() if not Path(path).exists()]
        if missing_paths:
            raise FileNotFoundError("Missing dbt/Cosmos path(s): " + "; ".join(missing_paths))

        logging.info("dbt/Cosmos environment validated successfully")

    pre_dbt_workflow = EmptyOperator(task_id="pre_dbt_workflow")

    dbt_transformations = DbtTaskGroup(
        group_id="f1_dbt_transformations",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
        operator_args={
            "retries": 2,
            "retry_delay": timedelta(minutes=2),
            "execution_timeout": timedelta(minutes=30),
        },
    )

    post_dbt_workflow = EmptyOperator(
        task_id="post_dbt_workflow",
        trigger_rule="none_failed_min_one_success",
        retries=2,
    )

    init_data = initialize_ingestion()
    meeting_data = get_meeting_data(init_data)
    sessions_data = get_sessions(meeting_data)

    endpoints = ["drivers", "car_data", "laps", "team_radio", "weather"]
    endpoint_tasks = []

    for endpoint_name in endpoints:
        current_task = process_endpoint.override(task_id=f"process_{endpoint_name}")(
            sessions_data,
            init_data,
            endpoint_name,
        )
        endpoint_tasks.append(current_task)

    race_data = process_race_data(sessions_data, init_data)
    technical_data = process_technical_data(sessions_data, init_data)
    cleanup = cleanup_staging()
    validate_dbt = validate_dbt_environment()

    init_data >> meeting_data >> sessions_data

    for endpoint_task in endpoint_tasks:
        sessions_data >> endpoint_task >> cleanup

    sessions_data >> race_data >> cleanup
    sessions_data >> technical_data >> cleanup

    cleanup >> validate_dbt >> pre_dbt_workflow >> dbt_transformations >> post_dbt_workflow


dag = f1_data_pipeline_backfill()
