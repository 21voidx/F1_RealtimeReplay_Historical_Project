# f1_data_pipeline.py

from airflow.decorators import dag, task
from airflow.utils.dates import datetime
from datetime import timedelta, timezone
import logging
from typing import Dict, List, Any
from include.hitesh.scripts.f1_snowflake_etl_2 import F1DataIngestion
from include.eczachly.snowflake_queries import get_snowpark_session
import os
from dotenv import load_dotenv
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from airflow.operators.empty import EmptyOperator



dbt_env_path = os.path.join(os.environ['AIRFLOW_HOME'], 'hitesh_dbt_project', 'dbt.env')
load_dotenv(dbt_env_path)

# DBT paths
airflow_home = os.getenv('AIRFLOW_HOME')
PATH_TO_DBT_PROJECT = f'{airflow_home}/hitesh_dbt_project'
PATH_TO_DBT_PROFILES = f'{airflow_home}/hitesh_dbt_project/profiles.yml'

# DBT profile configuration
profile_config = ProfileConfig(
    profile_name="formula_one",
    target_name="dev",
    profiles_yml_filepath=PATH_TO_DBT_PROFILES,
)

def get_f1_calendar(year: int) -> List[Dict]:
    """Get all race meetings for a given year"""
    try:
        session = get_snowpark_session()
        ingestion = F1DataIngestion(session)

        year_params = {'year': year}
        meetings = ingestion._make_request('meetings', year_params)

        if meetings:
            meetings.sort(key=lambda x: x['date_start'])
            return meetings
        return []
    except Exception as e:
        logging.error(f"Error getting F1 calendar for year {year}: {str(e)}")
        return []


def get_all_sessions(year: int) -> List[Dict[str, List]]:
    """Get all session dates for a given year"""
    try:
        session = get_snowpark_session()
        ingestion = F1DataIngestion(session)

        year_params = {'year': year}
        sessions = ingestion._make_request('sessions', year_params)

        if sessions:
            sessions.sort(key=lambda x: x['date_start'])
            return [
                {datetime.fromisoformat(m['date_start'].replace('Z', '+00:00')).strftime('%Y-%m-%d'):
                     [m['meeting_key'], m['session_key']]}
                for m in sessions
            ]
        return []
    except Exception as e:
        logging.error(f"Error getting session dates for year {year}: {str(e)}")
        return []


# Get F1 calendar and determine start date
f1_calendar_2024 = get_f1_calendar(2024)
first_race_date = None
if f1_calendar_2024:
    first_race_date = datetime.fromisoformat(f1_calendar_2024[0]['date_start'].replace('Z', '+00:00'))
    logging.info(f"First race of 2024 is on {first_race_date}")

# Get session dates before DAG definition
session_dates_2024 = get_all_sessions(2024)
session_dates = sorted(list({
    date for date_dict in session_dates_2024
    for date in date_dict.keys()
}))


@dag(
    dag_id='f1_data_pipeline_backfill',
    description="F1 Data Ingestion Pipeline - Session Days Only",
    default_args={
        "owner": "Hitesh Kaushik",
        "retries": 3,                    # Increased retries
        "retry_delay": timedelta(minutes=2),
        "depends_on_past": False,
        "execution_timeout": timedelta(hours=1),
        "email_on_retry": True,
        "max_active_tasks": 3,        # Limit concurrent tasks
        #"start_date": first_race_date or datetime(2024, 1, 1),
        "start_date": datetime(2024, 12, 7)
    },
    max_active_runs=1,
    schedule_interval="@daily",
    catchup=True,
    tags=["f1", "snowpark", "etl", "racing"]
)
def f1_data_pipeline_backfill():

    @task
    def initialize_ingestion(**context):
        execution_date = context['data_interval_start']
        logging.info(f"Processing date: {execution_date}")

        date_str = execution_date.strftime('%Y-%m-%d')
        logging.info(f"Checking if {date_str} is a session date")

        if date_str not in session_dates:
            logging.info(f"Skipping non-session day {date_str}")
            return None

        matching_sessions = []
        for session_dict in session_dates_2024:
            if date_str in session_dict:
                meeting_key = session_dict[date_str][0]
                session_key = session_dict[date_str][1]
                matching_sessions.append({
                    'meeting_key': meeting_key,
                    'session_key': session_key,
                    'execution_date': execution_date
                })

        if not matching_sessions:
            logging.info(f"No sessions found for date {date_str}")
            return None

        logging.info(f"Found {len(matching_sessions)} sessions for {date_str}")
        return {
            'execution_date': execution_date.isoformat(),
            'sessions': matching_sessions,
            'status': 'initialized',
            'is_race_day': True
        }

    @task
    def get_meeting_data(init_status: dict):
        if not init_status:
            logging.info("No valid init status, skipping meeting data fetch")
            return None

        session = get_snowpark_session()
        ingestion = F1DataIngestion(session)
        ingestion.execution_date = datetime.fromisoformat(init_status['execution_date']).date()

        meetings = []
        for session_info in init_status['sessions']:
            params = {'meeting_key': session_info['meeting_key']}

            try:
                data = ingestion._make_request('meetings', params)
                if data:
                    meeting = data[0]
                    if not ingestion.record_exists('F1_MEETINGS', meeting['meeting_key']):
                        ingestion.create_table('F1_MEETINGS', meeting)
                        ingestion.load_data('F1_MEETINGS', [meeting])
                        logging.info(f"Loaded new meeting {meeting['meeting_key']}")
                    meetings.append(meeting)
            except Exception as e:
                logging.error(f"Error processing meeting {session_info['meeting_key']}: {str(e)}")

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
                sessions = ingestion.get_session_data(meeting['meeting_key'])
                if sessions:
                    ingestion.create_table('F1_SESSIONS', sessions[0])
                    for sess in sessions:
                        if not ingestion.record_exists('F1_SESSIONS', meeting['meeting_key'], sess['session_key']):
                            ingestion.load_data('F1_SESSIONS', [sess])
                            logging.info(f"Loaded new session {sess['session_key']}")
                    all_sessions.extend(sessions)
            except Exception as e:
                logging.error(f"Error processing sessions for meeting {meeting['meeting_key']}: {str(e)}")

        return all_sessions

    @task(trigger_rule='none_failed')
    def process_endpoint(sessions_list: List[dict], init_data: dict, endpoint: str) -> Dict[str, Any]:
        """Process endpoint data for race sessions"""
        if init_data is None:
            return {
                'endpoint': endpoint,
                'processed': False,
                'reason': 'no_init_data',
                'results': []
            }

        results = []
        session = get_snowpark_session()
        ingestion = F1DataIngestion(session)
        execution_date = datetime.fromisoformat(init_data['execution_date']).date()
        ingestion.execution_date = execution_date

        table_name = f'F1_{endpoint.upper()}'

        for session_info in sessions_list:
            session_key = session_info['session_key']
            meeting_key = session_info['meeting_key']

            try:
                if ingestion.record_exists(table_name, meeting_key, session_key):
                    results.append({
                        'session_key': session_key,
                        'status': 'skipped',
                        'reason': 'data_exists',
                        'records': 0
                    })
                    continue

                data = ingestion.get_session_endpoint_data(session_info, endpoint)

                if not data:
                    results.append({
                        'session_key': session_key,
                        'status': 'skipped',
                        'reason': 'no_data',
                        'records': 0
                    })
                    continue

                if not ingestion.table_exists(table_name):
                    ingestion.create_table(table_name, data[0])

                ingestion.load_data(table_name, data)

                results.append({
                    'session_key': session_key,
                    'status': 'success',
                    'records': len(data),
                    'meeting_key': meeting_key
                })

            except Exception as e:
                results.append({
                    'session_key': session_key,
                    'status': 'error',
                    'reason': str(e),
                    'meeting_key': meeting_key
                })

        return {
            'endpoint': endpoint,
            'processed': True,
            'execution_date': execution_date.isoformat(),
            'results': results
        }

    @task(trigger_rule='none_failed')
    def process_race_data(sessions_list: List[dict], init_data: dict) -> Dict[str, Any]:
        """Process race-specific data (positions, intervals) sequentially"""
        if not init_data or not init_data.get('is_race_day'):
            return {
                'processed': False,
                'reason': 'not_race_day',
                'results': []
            }

        results = []
        session = get_snowpark_session()
        ingestion = F1DataIngestion(session)
        execution_date = datetime.fromisoformat(init_data['execution_date']).date()
        ingestion.execution_date = execution_date

        # Process intervals first, then positions
        for session_info in sessions_list:
            session_key = session_info['session_key']
            meeting_key = session_info['meeting_key']

            # Process intervals data
            try:
                if not ingestion.record_exists('F1_INTERVALS', meeting_key, session_key):
                    data = ingestion.get_intervals_data(session_key)
                    if data:
                        if not ingestion.table_exists('F1_INTERVALS'):
                            ingestion.create_table('F1_INTERVALS', data[0])
                        ingestion.load_data('F1_INTERVALS', data)
                        results.append({
                            'endpoint': 'intervals',
                            'session_key': session_key,
                            'status': 'success',
                            'records': len(data)
                        })
            except Exception as e:
                results.append({
                    'endpoint': 'intervals',
                    'session_key': session_key,
                    'status': 'error',
                    'error': str(e)
                })

            # Process position data only after intervals are done
            try:
                if not ingestion.record_exists('F1_POSITION', meeting_key, session_key):
                    data = ingestion.get_position_data(session_key)
                    if data:
                        if not ingestion.table_exists('F1_POSITION'):
                            ingestion.create_table('F1_POSITION', data[0])
                        ingestion.load_data('F1_POSITION', data)
                        results.append({
                            'endpoint': 'position',
                            'session_key': session_key,
                            'status': 'success',
                            'records': len(data)
                        })
            except Exception as e:
                results.append({
                    'endpoint': 'position',
                    'session_key': session_key,
                    'status': 'error',
                    'error': str(e)
                })

        return {
            'processed': True,
            'execution_date': execution_date.isoformat(),
            'results': results
        }


    @task(trigger_rule='none_failed')
    def process_technical_data(sessions_list: List[dict], init_data: dict) -> Dict[str, Any]:
        """Process technical data (stints, pit stops, race control)"""
        if not init_data:
            return {
                'processed': False,
                'reason': 'no_init_data',
                'results': []
            }

        results = []
        session = get_snowpark_session()
        ingestion = F1DataIngestion(session)
        execution_date = datetime.fromisoformat(init_data['execution_date']).date()
        ingestion.execution_date = execution_date

        technical_endpoints = ['stints', 'pit', 'race_control']

        for session_info in sessions_list:
            session_key = session_info['session_key']
            meeting_key = session_info['meeting_key']

            for endpoint in technical_endpoints:
                table_name = f'F1_{endpoint.upper()}'
                try:
                    if ingestion.record_exists(table_name, meeting_key, session_key):
                        continue

                    # Call appropriate method based on endpoint
                    if endpoint == 'stints':
                        data = ingestion.get_stints_data(session_key)
                    elif endpoint == 'pit':
                        data = ingestion.get_pit_data(session_key)
                    else:  # race_control
                        data = ingestion.get_race_control_data(session_key)

                    if data:
                        if not ingestion.table_exists(table_name):
                            ingestion.create_table(table_name, data[0])
                        ingestion.load_data(table_name, data)

                        results.append({
                            'endpoint': endpoint,
                            'session_key': session_key,
                            'status': 'success',
                            'records': len(data)
                        })

                except Exception as e:
                    results.append({
                        'endpoint': endpoint,
                        'session_key': session_key,
                        'status': 'error',
                        'error': str(e)
                    })

        return {
            'processed': True,
            'execution_date': execution_date.isoformat(),
            'results': results
        }

    @task(trigger_rule='all_done')
    def cleanup_staging():  # Changed name to avoid conflicts
        """Clean up any remaining staging tables"""
        try:
            session = get_snowpark_session()
            ingestion = F1DataIngestion(session)
            ingestion.cleanup_staging_tables()
            logging.info("Completed staging table cleanup")
        except Exception as e:
            logging.error(f"Error in staging table cleanup task: {str(e)}")

    pre_dbt_workflow = EmptyOperator(
        task_id="pre_dbt_workflow",
        trigger_rule="all_done"
    )

    # DBT transformation group
    dbt_transformations = DbtTaskGroup(
        group_id="f1_dbt_transformations",
        project_config=ProjectConfig(PATH_TO_DBT_PROJECT),
        profile_config=profile_config,
        render_config=RenderConfig(
            select=[
                "staging.stg_f1_*",  # All staging models
                "intermediate.int_*",  # All intermediate models
                "marts.f1_*"  # All final models
            ],
        ),
        operator_args={
            "retries": 2,
            "retry_delay": timedelta(minutes=2),
            "execution_timeout": timedelta(minutes=30)
        }
    )

    # Final checkpoint
    post_dbt_workflow = EmptyOperator(
        task_id="post_dbt_workflow",
        trigger_rule="all_done",
        retries = 2
    )





    # To Execute the pipeline
    init_data = initialize_ingestion() # Here it skips races if it is not a race day (SAVE COMPUTE !! )
    meeting_data = get_meeting_data(init_data)
    sessions_data = get_sessions(meeting_data)

    # Process original endpoints
    endpoints = ['drivers', 'car_data', 'laps', 'team_radio', 'weather']
    endpoint_tasks = []

    for endpoint_name in endpoints:
        task_id = f'process_{endpoint_name}'
        current_task = process_endpoint.override(task_id=task_id)(
            sessions_data,
            init_data,
            endpoint_name
        )
        endpoint_tasks.append(current_task)

    # Add new data processing tasks
    race_data = process_race_data(sessions_data, init_data)
    technical_data = process_technical_data(sessions_data, init_data)
    cleanup = cleanup_staging()

    # Set dependencies
    init_data >> meeting_data >> sessions_data

    # Set individual dependencies for each endpoint task
    for endpoint_task in endpoint_tasks:
        sessions_data >> endpoint_task >> cleanup

    # Set dependencies for race and technical data
    sessions_data >> race_data >> cleanup
    sessions_data >> technical_data >> cleanup

    # Connect DBT workflow
    cleanup >> pre_dbt_workflow >> dbt_transformations  >> post_dbt_workflow


# Create DAG instance
dag = f1_data_pipeline_backfill()