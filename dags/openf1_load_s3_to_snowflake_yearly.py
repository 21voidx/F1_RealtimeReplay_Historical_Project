"""
openf1_load_s3_to_snowflake_check.py

DAG khusus untuk reload data dari S3 external stage ke Snowflake.
DAG ini tetap memakai check_new_sessions, tetapi tidak menjalankan extract_api_to_s3.

Alur:
    check_new_sessions
        ↓
    load_s3_to_snowflake

Mode jalan:
1. Scheduled/backfill:
   Airflow memakai data_interval_start dan data_interval_end untuk mencari session OpenF1.

2. Manual dengan tanggal:
   {
     "date_start": "2024-03-01T00:00:00Z",
     "date_end": "2024-04-01T00:00:00Z"
   }

3. Manual session tertentu:
   {
     "meeting_key": 1234,
     "session_key": 5678
   }

4. Manual banyak session:
   {
     "sessions": [
       {"meeting_key": 1234, "session_key": 5678},
       {"meeting_key": 1234, "session_key": 5679}
     ]
   }

5. Optional reload endpoint tertentu:
   {
     "meeting_key": 1234,
     "session_key": 5678,
     "endpoints": ["drivers", "sessions", "weather"]
   }
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

import requests
from airflow.sdk import dag, task, get_current_context
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.timetables.interval import CronDataIntervalTimetable

log = logging.getLogger(__name__)

# ── Airflow connections ───────────────────────────────────────────────────────
SNOW_CONN_ID = "snowflake_default"

# ── Snowflake ─────────────────────────────────────────────────────────────────
SNOW_DB = "dbt_db"
SNOW_SCHEMA = "dbt_schema"
SNOW_STAGE = f"{SNOW_DB}.{SNOW_SCHEMA}.openf1_s3_stage"

OPENF1_BASE = "https://api.openf1.org/v1"
API_SLEEP_SECONDS = 2

# ── Endpoint catalogue ────────────────────────────────────────────────────────
ENDPOINTS: dict[str, dict] = {
    "car_data": {
        "table": "RAW_CAR_DATA",
        "columns": {
            "session_key": "$1:session_key::INTEGER",
            "meeting_key": "$1:meeting_key::INTEGER",
            "driver_number": "$1:driver_number::INTEGER",
            "date": "$1:date::TIMESTAMP_NTZ",
            "rpm": "$1:rpm::INTEGER",
            "speed": "$1:speed::INTEGER",
            "throttle": "$1:throttle::INTEGER",
            "brake": "$1:brake::INTEGER",
            "n_gear": "$1:n_gear::INTEGER",
            "drs": "$1:drs::INTEGER",
        },
    },
    "drivers": {
        "table": "RAW_DRIVERS",
        "columns": {
            "meeting_key": "$1:meeting_key::INTEGER",
            "driver_number": "$1:driver_number::INTEGER",
            "broadcast_name": "$1:broadcast_name::VARCHAR",
            "first_name": "$1:first_name::VARCHAR",
            "last_name": "$1:last_name::VARCHAR",
            "full_name": "$1:full_name::VARCHAR",
            "name_acronym": "$1:name_acronym::VARCHAR",
            "headshot_url": "$1:headshot_url::VARCHAR",
            "team_name": "$1:team_name::VARCHAR",
            "team_colour": "$1:team_colour::VARCHAR",
            "country_code": "$1:country_code::VARCHAR",
            "session_key": "$1:session_key::INTEGER",
        },
    },
    "intervals": {
        "table": "RAW_INTERVALS",
        "columns": {
            "meeting_key": "$1:meeting_key::INTEGER",
            "session_key": "$1:session_key::INTEGER",
            "driver_number": "$1:driver_number::INTEGER",
            "date": "$1:date::TIMESTAMP_NTZ",
            "gap_to_leader": "$1:gap_to_leader::VARCHAR",
            "interval": "$1:interval::VARCHAR",
        },
    },
    "laps": {
        "table": "RAW_LAPS",
        "columns": {
            "meeting_key": "$1:meeting_key::INTEGER",
            "session_key": "$1:session_key::INTEGER",
            "driver_number": "$1:driver_number::INTEGER",
            "date_start": "$1:date_start::TIMESTAMP_NTZ",
            "lap_duration": "$1:lap_duration::FLOAT",
            "lap_number": "$1:lap_number::INTEGER",
            "is_pit_out_lap": "$1:is_pit_out_lap::BOOLEAN",
            "duration_sector_1": "$1:duration_sector_1::FLOAT",
            "duration_sector_2": "$1:duration_sector_2::FLOAT",
            "duration_sector_3": "$1:duration_sector_3::FLOAT",
            "i1_speed": "$1:i1_speed::INTEGER",
            "i2_speed": "$1:i2_speed::INTEGER",
            "segments_sector_1": "$1:segments_sector_1::VARIANT",
            "segments_sector_2": "$1:segments_sector_2::VARIANT",
            "segments_sector_3": "$1:segments_sector_3::VARIANT",
            "st_speed": "$1:st_speed::INTEGER",
        },
    },
    "meetings": {
        "table": "RAW_MEETINGS",
        "columns": {
            "meeting_key": "$1:meeting_key::INTEGER",
            "meeting_name": "$1:meeting_name::VARCHAR",
            "meeting_official_name": "$1:meeting_official_name::VARCHAR",
            "country_name": "$1:country_name::VARCHAR",
            "circuit_key": "$1:circuit_key::INTEGER",
            "circuit_image": "$1:circuit_image::VARCHAR",
            "circuit_info_url": "$1:circuit_info_url::VARCHAR",
            "circuit_short_name": "$1:circuit_short_name::VARCHAR",
            "circuit_type": "$1:circuit_type::VARCHAR",
            "circuit_code": "$1:circuit_code::VARCHAR",
            "circuit_flag": "$1:circuit_flag::VARCHAR",
            "year": "$1:year::INTEGER",
            "date_start": "$1:date_start::TIMESTAMP_NTZ",
            "date_end": "$1:date_end::TIMESTAMP_NTZ",
            "gmt_offset": "$1:gmt_offset::VARCHAR",
            "is_cancelled": "$1:is_cancelled::BOOLEAN",
            "location": "$1:location::VARCHAR",
        },
    },
    "pit": {
        "table": "RAW_PIT",
        "columns": {
            "session_key": "$1:session_key::INTEGER",
            "meeting_key": "$1:meeting_key::INTEGER",
            "driver_number": "$1:driver_number::INTEGER",
            "date": "$1:date::TIMESTAMP_NTZ",
            "lap_number": "$1:lap_number::INTEGER",
            "pit_duration": "$1:pit_duration::FLOAT",
            "lane_duration": "$1:lane_duration::FLOAT",
            "stop_duration": "$1:stop_duration::FLOAT",
        },
    },
    "position": {
        "table": "RAW_POSITION",
        "columns": {
            "session_key": "$1:session_key::INTEGER",
            "driver_number": "$1:driver_number::INTEGER",
            "date": "$1:date::TIMESTAMP_NTZ",
            "meeting_key": "$1:meeting_key::INTEGER",
            "position": "$1:position::INTEGER",
        },
    },
    "sessions": {
        "table": "RAW_SESSIONS",
        "columns": {
            "session_key": "$1:session_key::INTEGER",
            "meeting_key": "$1:meeting_key::INTEGER",
            "country_name": "$1:country_name::VARCHAR",
            "session_name": "$1:session_name::VARCHAR",
            "session_type": "$1:session_type::VARCHAR",
            "year": "$1:year::INTEGER",
            "circuit_key": "$1:circuit_key::INTEGER",
            "circuit_short_name": "$1:circuit_short_name::VARCHAR",
            "country_code": "$1:country_code::VARCHAR",
            "date_start": "$1:date_start::TIMESTAMP_NTZ",
            "date_end": "$1:date_end::TIMESTAMP_NTZ",
            "gmt_offset": "$1:gmt_offset::VARCHAR",
            "is_cancelled": "$1:is_cancelled::BOOLEAN",
            "location": "$1:location::VARCHAR",
        },
    },
    "stints": {
        "table": "RAW_STINTS",
        "columns": {
            "meeting_key": "$1:meeting_key::INTEGER",
            "session_key": "$1:session_key::INTEGER",
            "driver_number": "$1:driver_number::INTEGER",
            "stint_number": "$1:stint_number::INTEGER",
            "lap_start": "$1:lap_start::INTEGER",
            "lap_end": "$1:lap_end::INTEGER",
            "compound": "$1:compound::VARCHAR",
            "tyre_age_at_start": "$1:tyre_age_at_start::INTEGER",
        },
    },
    "weather": {
        "table": "RAW_WEATHER",
        "columns": {
            "meeting_key": "$1:meeting_key::INTEGER",
            "session_key": "$1:session_key::INTEGER",
            "date": "$1:date::TIMESTAMP_NTZ",
            "air_temperature": "$1:air_temperature::FLOAT",
            "track_temperature": "$1:track_temperature::FLOAT",
            "pressure": "$1:pressure::FLOAT",
            "humidity": "$1:humidity::FLOAT",
            "rainfall": "$1:rainfall::BOOLEAN",
            "wind_speed": "$1:wind_speed::FLOAT",
            "wind_direction": "$1:wind_direction::INTEGER",
        },
    },
}


def _format_query_value(value) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    if value is None:
        return ""
    return str(value)


def _build_openf1_url(endpoint_or_url: str, params: dict | None = None) -> str:
    if endpoint_or_url.startswith(("http://", "https://")):
        base_url = endpoint_or_url
    else:
        base_url = f"{OPENF1_BASE}/{endpoint_or_url.lstrip('/')}"

    if not params:
        return base_url

    query_parts = []
    for key, value in params.items():
        key = str(key)
        formatted_value = _format_query_value(value)
        encoded_value = quote(formatted_value, safe=":-TZ")

        if key.endswith((">=", "<=")):
            query_parts.append(f"{key}{encoded_value}")
        elif key.endswith((">", "<")):
            query_parts.append(f"{key}{encoded_value}=")
        else:
            query_parts.append(f"{key}={encoded_value}")

    separator = "&" if "?" in base_url else "?"
    return base_url + separator + "&".join(query_parts)


def _get_json(url: str, params: dict | None = None, timeout: int = 120) -> list[dict]:
    final_url = _build_openf1_url(url, params=params)
    log.info("GET %s", final_url)

    response = requests.get(final_url, timeout=timeout)

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        if response.status_code == 404:
            log.warning("OpenF1 404 untuk %s. Mengembalikan list kosong.", final_url)
            return []
        raise exc

    return response.json()


def _parse_openf1_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        if not value:
            raise ValueError("Nilai datetime kosong.")
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def _format_openf1_datetime(value: str | datetime) -> str:
    dt = _parse_openf1_datetime(value).replace(microsecond=0)
    return dt.isoformat().replace("+00:00", "Z")


def _format_openf1_date(value: str | datetime) -> str:
    return _parse_openf1_datetime(value).date().isoformat()


def _normalize_manual_sessions(raw_sessions) -> list[dict]:
    if not raw_sessions:
        return []

    normalized: list[dict] = []
    seen: set[tuple[int, int]] = set()

    for item in raw_sessions:
        meeting_key = int(item["meeting_key"])
        session_key = int(item["session_key"])
        key = (meeting_key, session_key)

        if key in seen:
            continue

        seen.add(key)
        normalized.append(
            {
                "meeting_key": meeting_key,
                "session_key": session_key,
                "date_start": item.get("date_start"),
                "date_end": item.get("date_end"),
            }
        )

    return normalized


def _build_jinja_sql_template(endpoint: str, cfg: dict) -> str:
    full_table = f"{SNOW_DB}.{SNOW_SCHEMA}.{cfg['table']}"
    col_names = ",\n    ".join(cfg["columns"].keys())
    col_exprs = ",\n        ".join(cfg["columns"].values())

    id_col = "meeting_key" if endpoint == "meetings" else "session_key"
    id_val = "{{ session.meeting_key }}" if endpoint == "meetings" else "{{ session.session_key }}"

    return f"""
    {{% set selected_endpoints = dag_run.conf.get('endpoints') if dag_run and dag_run.conf else none %}}
    {{% set sessions = ti.xcom_pull(task_ids='check_new_sessions') %}}

    {{% if sessions and (not selected_endpoints or '{endpoint}' in selected_endpoints) %}}
        {{% for session in sessions %}}
            BEGIN;

            DELETE FROM {full_table}
            WHERE {id_col} = {id_val};

            COPY INTO {full_table} (
                {col_names}
            )
            FROM (
                SELECT
                    {col_exprs}
                FROM @{SNOW_STAGE}/{endpoint}/meeting_key={{{{ session.meeting_key }}}}/session_key={{{{ session.session_key }}}}/
            )
            FILE_FORMAT = (TYPE = 'PARQUET')
            FORCE = TRUE
            ON_ERROR = 'ABORT_STATEMENT';

            COMMIT;
        {{% endfor %}}
    {{% endif %}}
    """


_LOAD_SQL_TEMPLATE: str = "\n".join(
    _build_jinja_sql_template(endpoint, cfg)
    for endpoint, cfg in ENDPOINTS.items()
)


@dag(
    dag_id="openf1_load_s3_to_snowflake_yearly",
    description="Load-only OpenF1 S3 Parquet external stage ke Snowflake, tetap memakai check_new_sessions.",
    schedule=CronDataIntervalTimetable("0 0 15 5 *", timezone="UTC"),
    start_date=datetime(2024, 1, 1),
    catchup=True,
    max_active_runs=3,
    tags=["openf1", "snowflake", "load-only"],
    default_args={
        "owner": "data-engineering",
        "retries": 3,
    },
)
def openf1_load_s3_to_snowflake_check() -> None:

    @task.short_circuit(task_id="check_new_sessions")
    def check_new_sessions(data_interval_start=None, data_interval_end=None) -> list[dict] | bool:
        """
        Menghasilkan daftar session untuk task load_s3_to_snowflake.

        Prioritas:
        1. dag_run.conf["sessions"]
        2. dag_run.conf["meeting_key"] + dag_run.conf["session_key"]
        3. dag_run.conf["date_start"] + dag_run.conf["date_end"]
        4. data_interval_start + data_interval_end dari timetable Airflow
        """
        context = get_current_context()
        dag_run = context.get("dag_run")
        conf = dag_run.conf or {} if dag_run else {}

        if conf.get("sessions"):
            manual_sessions = _normalize_manual_sessions(conf["sessions"])
            log.info("Manual reload untuk %d session dari dag_run.conf['sessions'].", len(manual_sessions))
            return manual_sessions or False

        if conf.get("meeting_key") and conf.get("session_key"):
            one_session = _normalize_manual_sessions(
                [
                    {
                        "meeting_key": conf["meeting_key"],
                        "session_key": conf["session_key"],
                        "date_start": conf.get("date_start"),
                        "date_end": conf.get("date_end"),
                    }
                ]
            )
            log.info("Manual reload untuk satu session: %s", one_session)
            return one_session or False

        if conf.get("date_start") and conf.get("date_end"):
            start_dt = _parse_openf1_datetime(conf["date_start"])
            end_dt = _parse_openf1_datetime(conf["date_end"])
        else:
            start_dt = _parse_openf1_datetime(data_interval_start)
            end_dt = _parse_openf1_datetime(data_interval_end)

        start_iso = _format_openf1_datetime(start_dt)
        end_iso = _format_openf1_datetime(end_dt)
        start_date = _format_openf1_date(start_dt)
        end_inclusive_iso = _format_openf1_datetime(end_dt - timedelta(seconds=1))

        sessions_url = f"{OPENF1_BASE}/sessions"
        sessions_params = {
            "date_start>=": start_date,
            "date_end<=": end_inclusive_iso,
        }

        try:
            sessions = _get_json(sessions_url, params=sessions_params)
            log.info(
                "Fetched %d sessions dari OpenF1. Filter awal: date_start >= %s dan date_end <= %s.",
                len(sessions),
                start_date,
                end_inclusive_iso,
            )
        except requests.exceptions.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None

            if status_code not in {500, 502, 503, 504}:
                raise

            log.warning(
                "OpenF1 HTTP %s untuk filter tanggal. Fallback ke /sessions?year=YYYY.",
                status_code,
            )

            sessions = []
            for year in range(start_dt.year, end_dt.year + 1):
                time.sleep(API_SLEEP_SECONDS)
                sessions.extend(_get_json(sessions_url, params={"year": year}))

        if not sessions:
            log.info("Tidak ada session dari OpenF1 untuk interval %s sampai %s.", start_iso, end_iso)
            return False

        active_sessions: list[dict] = []
        seen_session_keys: set[int] = set()

        for session in sessions:
            if not session.get("date_start") or not session.get("date_end"):
                log.warning("Session dilewati karena date_start/date_end kosong: %s", session)
                continue

            session_end_dt = _parse_openf1_datetime(session["date_end"])

            if not (start_dt <= session_end_dt < end_dt):
                continue

            session_key = int(session["session_key"])
            meeting_key = int(session["meeting_key"])

            if session_key in seen_session_keys:
                continue

            seen_session_keys.add(session_key)
            active_sessions.append(
                {
                    "meeting_key": meeting_key,
                    "session_key": session_key,
                    "date_start": session["date_start"],
                    "date_end": session["date_end"],
                }
            )

        if not active_sessions:
            log.info("Tidak ada session aktif setelah filter lokal %s sampai %s.", start_iso, end_iso)
            return False

        log.info("Ditemukan %d session untuk reload Snowflake: %s", len(active_sessions), active_sessions)
        return active_sessions

    load_s3_to_snowflake = SQLExecuteQueryOperator(
        task_id="load_s3_to_snowflake",
        conn_id=SNOW_CONN_ID,
        sql=_LOAD_SQL_TEMPLATE,
        split_statements=True,
        return_last=False,
    )

    sessions = check_new_sessions()
    sessions >> load_s3_to_snowflake


openf1_load_s3_to_snowflake_check()
