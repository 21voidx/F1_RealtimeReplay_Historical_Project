"""
openf1_pipeline.py
══════════════════════════════════════════════════════════════════════════════
OpenF1 API  →  S3  →  Snowflake raw_* tables  →  dbt  (Event-driven, partitioned by session)

Pipeline:
  [check_new_sessions]         @task.short_circuit (Gatekeeper)
      ↓  .expand()
  [extract_api_to_s3]          @task (Dynamic Task Mapping — 1 task per session)
      ↓
  [load_s3_to_snowflake]       SQLExecuteQueryOperator (Jinja templated loop)
      ↓
  [dbt_run_staging]            DockerOperator
      ↓
  [dbt_run_intermediate]       DockerOperator
      ↓
  [dbt_run_marts]              DockerOperator

S3 Partition Layout:
  s3://{S3_BUCKET}/openf1/{endpoint}/meeting_key={MK}/session_key={SK}/data.parquet

Idempotency Strategy:
  • S3        : replace=True (selalu menimpa file partisi sesi yang sama)
  • Snowflake : DELETE WHERE session_key = {SK} + COPY INTO raw_* tables
                (Dijalankan dalam satu blok BEGIN..COMMIT secara dinamis via Jinja)

Memory Strategy:
  • Setiap sesi diproses oleh worker Airflow yang berbeda (Dynamic Task Mapping).
  • Di dalam setiap worker, data di-del dari memori segera setelah di-upload ke S3
    sehingga RAM tidak menumpuk lintas endpoint.
  • Format Parquet (bukan JSON) memberikan kompresi kolumnar dan schema yang lebih kuat.
"""

from __future__ import annotations

import io
import logging
import random
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.timetables.interval import CronDataIntervalTimetable
from docker.types import Mount

log = logging.getLogger(__name__)

# ── Airflow connections ───────────────────────────────────────────────────────
AWS_CONN_ID  = "aws_default"
SNOW_CONN_ID = "snowflake_default"

# ── S3 ────────────────────────────────────────────────────────────────────────
S3_BUCKET = "openf1-api-json"
S3_ROOT   = "openf1"

# ── Snowflake ─────────────────────────────────────────────────────────────────
SNOW_DB     = "dbt_db"
SNOW_SCHEMA = "dbt_schema"
SNOW_STAGE  = f"{SNOW_DB}.{SNOW_SCHEMA}.openf1_s3_stage"

# ── dbt Docker image + host bind mounts ──────────────────────────────────────
DBT_IMAGE = "f1-dbt-snowflake:1.0"

# Path ini dibaca oleh Docker daemon di host, bukan dari filesystem container Airflow.
# Pastikan kedua path ini ada di mesin yang menjalankan Docker daemon.
DBT_PROJECT_HOST_PATH = "/home/void/F1_RealtimeReplay_Historical_Project/dbt/dbt_project"
DBT_PROFILES_HOST_PATH = "/home/void/F1_RealtimeReplay_Historical_Project/dbt/dbt_profiles"

DBT_PROJECT_CONTAINER_PATH = "/app"
DBT_PROFILES_CONTAINER_PATH = "/root/.dbt"

DBT_ENV: dict[str, str] = {
    "SNOWFLAKE_ACCOUNT":   "{{ var.value.SNOWFLAKE_ACCOUNT }}",
    "SNOWFLAKE_USER":      "{{ var.value.SNOWFLAKE_USER }}",
    "SNOWFLAKE_PASSWORD":  "{{ var.value.SNOWFLAKE_PASSWORD }}",
    "SNOWFLAKE_ROLE":      "dbt_role",
    "SNOWFLAKE_DATABASE":  SNOW_DB,
    "SNOWFLAKE_WAREHOUSE": "dbt_wh",
    "SNOWFLAKE_SCHEMA":    SNOW_SCHEMA,
    "DBT_PROFILES_DIR":    DBT_PROFILES_CONTAINER_PATH,
    "DBT_PROJECT_DIR":     DBT_PROJECT_CONTAINER_PATH,
}

OPENF1_BASE = "https://api.openf1.org/v1"

# ── API throttle ──────────────────────────────────────────────────────────────
# OpenF1 free tier: 3 req/s dan 30 req/min.
# Pakai buffer supaya tidak persis menyentuh batas 30 request/menit.
API_SLEEP_SECONDS = 2.5
API_MAX_ATTEMPTS = 8
API_BACKOFF_MAX_SECONDS = 300

# Endpoint telemetry frekuensi tinggi. Satu request per driver untuk satu session
# masih bisa terlalu besar, sehingga perlu dipecah per rentang waktu.
TELEMETRY_ENDPOINTS = {"car_data", "location"}
TELEMETRY_WINDOW_MINUTES = 10
TELEMETRY_MIN_WINDOW_SECONDS = 30

# ── Endpoint catalogue ────────────────────────────────────────────────────────
# Filter berbasis tanggal telah dihapus karena kita menarik data
# berdasarkan identitas event (session_key / meeting_key).
#
# Kunci "columns" masih dipakai untuk membangun SQL COPY INTO Snowflake.
# Kunci "table" sekarang memakai prefix RAW_* agar raw/source layer jelas terpisah dari dbt models.
# Format file sudah diubah ke PARQUET — ekspresi $1:col::TYPE tetap valid
# di Snowflake untuk staged Parquet files.
ENDPOINTS: dict[str, dict] = {
    "car_data": {
        "table": "RAW_CAR_DATA",
        "columns": {
            "session_key":   "$1:session_key::INTEGER",
            "meeting_key":   "$1:meeting_key::INTEGER",
            "driver_number": "$1:driver_number::INTEGER",
            "date":          "$1:date::TIMESTAMP_NTZ",
            "rpm":           "$1:rpm::INTEGER",
            "speed":         "$1:speed::INTEGER",
            "throttle":      "$1:throttle::INTEGER",
            "brake":         "$1:brake::INTEGER",
            "n_gear":         "$1:n_gear::INTEGER",
            "drs":           "$1:drs::INTEGER",
        },
    },
    "drivers": {
        "table": "RAW_DRIVERS",
        "columns": {
            "meeting_key":     "$1:meeting_key::INTEGER",
            "driver_number":   "$1:driver_number::INTEGER",
            "broadcast_name":  "$1:broadcast_name::VARCHAR",
            "first_name":      "$1:first_name::VARCHAR",
            "last_name":       "$1:last_name::VARCHAR",
            "full_name":       "$1:full_name::VARCHAR",
            "name_acronym":    "$1:name_acronym::VARCHAR",
            "headshot_url":     "$1:headshot_url::VARCHAR",
            "team_name":       "$1:team_name::VARCHAR",
            "team_colour":     "$1:team_colour::VARCHAR",
            "country_code":    "$1:country_code::VARCHAR",
            "session_key":     "$1:session_key::INTEGER",
        },
    },
    "intervals": {
        "table": "RAW_INTERVALS",
        "columns": {
            "meeting_key":   "$1:meeting_key::INTEGER",
            "session_key":   "$1:session_key::INTEGER",
            "driver_number": "$1:driver_number::INTEGER",
            "date":          "$1:date::TIMESTAMP_NTZ",
            "gap_to_leader": "$1:gap_to_leader::VARCHAR",
            "interval":      "$1:interval::VARCHAR",
        },
    },
    "laps": {
        "table": "RAW_LAPS",
        "columns": {
            "meeting_key":       "$1:meeting_key::INTEGER",
            "session_key":       "$1:session_key::INTEGER",
            "driver_number":     "$1:driver_number::INTEGER",
            "date_start":        "$1:date_start::TIMESTAMP_NTZ",
            "lap_duration":      "$1:lap_duration::FLOAT",
            "lap_number":        "$1:lap_number::INTEGER",
            "is_pit_out_lap":    "$1:is_pit_out_lap::BOOLEAN",
            "duration_sector_1": "$1:duration_sector_1::FLOAT",
            "duration_sector_2": "$1:duration_sector_2::FLOAT",
            "duration_sector_3": "$1:duration_sector_3::FLOAT",
            "i1_speed":          "$1:i1_speed::INTEGER",
            "i2_speed":          "$1:i2_speed::INTEGER",
            "segments_sector_1":  "$1:segments_sector_1::VARIANT",
            "segments_sector_2":  "$1:segments_sector_2::VARIANT",
            "segments_sector_3":  "$1:segments_sector_3::VARIANT",
            "st_speed":          "$1:st_speed::INTEGER",
        },
    },
    "meetings": {
        "table": "RAW_MEETINGS",
        "columns": {
            "meeting_key":           "$1:meeting_key::INTEGER",
            "meeting_name":          "$1:meeting_name::VARCHAR",
            "meeting_official_name": "$1:meeting_official_name::VARCHAR",
            "country_name":          "$1:country_name::VARCHAR",
            "circuit_key":           "$1:circuit_key::INTEGER",
            "circuit_image":         "$1:circuit_image::VARCHAR",
            "circuit_info_url":       "$1:circuit_info_url::VARCHAR",
            "circuit_short_name":    "$1:circuit_short_name::VARCHAR",
            "circuit_type":          "$1:circuit_type::VARCHAR",
            "circuit_code":          "$1:circuit_code::VARCHAR",
            "circuit_flag":          "$1:circuit_flag::VARCHAR",
            "year":                  "$1:year::INTEGER",
            "date_start":            "$1:date_start::TIMESTAMP_NTZ",
            "date_end":              "$1:date_end::TIMESTAMP_NTZ",
            "gmt_offset":            "$1:gmt_offset::VARCHAR",
            "is_cancelled":          "$1:is_cancelled::BOOLEAN",
            "location":             "$1:location::VARCHAR",
        },
    },
    "pit": {
        "table": "RAW_PIT",
        "columns": {
            "session_key":   "$1:session_key::INTEGER",
            "meeting_key":   "$1:meeting_key::INTEGER",
            "driver_number": "$1:driver_number::INTEGER",
            "date":          "$1:date::TIMESTAMP_NTZ",
            "lap_number":    "$1:lap_number::INTEGER",
            "pit_duration":  "$1:pit_duration::FLOAT",
            "lane_duration": "$1:lane_duration::FLOAT",
            "stop_duration":   "$1:stop_duration::FLOAT",
        },
    },
    "position": {
        "table": "RAW_POSITION",
        "columns": {
            "session_key":   "$1:session_key::INTEGER",
            "driver_number": "$1:driver_number::INTEGER",
            "date":          "$1:date::TIMESTAMP_NTZ",
            "meeting_key":   "$1:meeting_key::INTEGER",
            "position":      "$1:position::INTEGER",
        },
    },
    "sessions": {
        "table": "RAW_SESSIONS",
        "columns": {
            "session_key":        "$1:session_key::INTEGER",
            "meeting_key":        "$1:meeting_key::INTEGER",
            "country_name":       "$1:country_name::VARCHAR",
            "session_name":       "$1:session_name::VARCHAR",
            "session_type":       "$1:session_type::VARCHAR",
            "year":               "$1:year::INTEGER",
            "circuit_key":       "$1:circuit_key::INTEGER",
            "circuit_short_name": "$1:circuit_short_name::VARCHAR",
            "country_code":       "$1:country_code::VARCHAR",
            "country_name":       "$1:country_name::VARCHAR",
            "date_start":         "$1:date_start::TIMESTAMP_NTZ",
            "date_end":           "$1:date_end::TIMESTAMP_NTZ",
            "gmt_offset":         "$1:gmt_offset::VARCHAR",
            "is_cancelled":       "$1:is_cancelled::BOOLEAN",
            "location":           "$1:location::VARCHAR",
        },
    },
    "stints": {
        "table": "RAW_STINTS",
        "columns": {
            "meeting_key":       "$1:meeting_key::INTEGER",
            "session_key":       "$1:session_key::INTEGER",
            "driver_number":     "$1:driver_number::INTEGER",
            "stint_number":      "$1:stint_number::INTEGER",
            "lap_start":         "$1:lap_start::INTEGER",
            "lap_end":           "$1:lap_end::INTEGER",
            "compound":          "$1:compound::VARCHAR",
            "tyre_age_at_start": "$1:tyre_age_at_start::INTEGER",
        },
    },
    "weather": {
        "table": "RAW_WEATHER",
        "columns": {
            "meeting_key":       "$1:meeting_key::INTEGER",
            "session_key":       "$1:session_key::INTEGER",
            "date":              "$1:date::TIMESTAMP_NTZ",
            "air_temperature":   "$1:air_temperature::FLOAT",
            "track_temperature": "$1:track_temperature::FLOAT",
            "pressure":         "$1:pressure::FLOAT",
            "humidity":          "$1:humidity::FLOAT",
            "rainfall":          "$1:rainfall::BOOLEAN",
            "wind_speed":        "$1:wind_speed::FLOAT",
            "wind_direction":    "$1:wind_direction::INTEGER",
        },
    },
}

# ═══════════════════════════════════════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════════════════════════════════════

class OpenF1UnprocessableEntity(requests.exceptions.HTTPError):
    """Dipakai untuk 422 agar endpoint telemetry bisa dipecah menjadi window lebih kecil."""


def _sleep_before_request(attempt: int, retry_after: str | None = None) -> None:
    """Rate-limit sederhana yang menghormati Retry-After saat API mengirimkannya."""
    if retry_after:
        try:
            sleep_seconds = int(retry_after)
        except ValueError:
            sleep_seconds = API_SLEEP_SECONDS
    elif attempt <= 1:
        sleep_seconds = API_SLEEP_SECONDS
    else:
        sleep_seconds = min(
            API_BACKOFF_MAX_SECONDS,
            (2 ** attempt) + random.uniform(0, 1),
        )

    log.debug("Sleep %.2f detik sebelum request OpenF1", sleep_seconds)
    time.sleep(sleep_seconds)


def _get_json(endpoint_or_url: str, params: dict | None = None, timeout: int = 120) -> list[dict]:
    """
    GET JSON dari OpenF1 dengan retry untuk 429 dan 5xx.
    422 tidak langsung di-retry sebagai request yang sama, karena pada telemetry
    biasanya artinya request terlalu besar atau filter belum cukup spesifik.
    """
    url = endpoint_or_url if endpoint_or_url.startswith("http") else f"{OPENF1_BASE}/{endpoint_or_url.lstrip('/')}"
    retry_after = None

    for attempt in range(1, API_MAX_ATTEMPTS + 1):
        _sleep_before_request(attempt, retry_after=retry_after)
        retry_after = None

        log.debug("GET %s params=%s attempt=%s", url, params, attempt)
        resp = requests.get(url, params=params, timeout=timeout)

        if resp.status_code == 200:
            return resp.json()

        if resp.status_code == 404:
            log.warning("Data tidak ditemukan (404) untuk %s. Mengembalikan array kosong.", resp.url)
            return []

        if resp.status_code == 422:
            body = resp.text[:1000]
            msg = f"422 dari OpenF1 untuk {resp.url}. Response body: {body}"
            raise OpenF1UnprocessableEntity(msg, response=resp)

        if resp.status_code == 429 or 500 <= resp.status_code < 600:
            retry_after = resp.headers.get("Retry-After")
            log.warning(
                "OpenF1 status=%s untuk %s. Retry attempt %s/%s. Retry-After=%s",
                resp.status_code, resp.url, attempt, API_MAX_ATTEMPTS, retry_after,
            )
            continue

        # 400/401/403 dan error client lain tidak usah diulang sampai kiamat.
        resp.raise_for_status()

    raise RuntimeError(f"OpenF1 request gagal setelah {API_MAX_ATTEMPTS} attempt: {url} params={params}")


def _parse_openf1_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _to_openf1_datetime(value: datetime) -> str:
    # OpenF1 menerima ISO string. Biarkan offset UTC tetap eksplisit.
    return value.isoformat()


def _iter_time_windows(start: datetime, end: datetime, minutes: int):
    current = start
    step = timedelta(minutes=minutes)

    while current < end:
        next_dt = min(current + step, end)
        yield current, next_dt
        current = next_dt


def _get_session_bounds(session: dict) -> tuple[datetime, datetime]:
    """Ambil date_start dan date_end session. Fallback ke endpoint sessions jika XCom lama belum membawa tanggal."""
    if session.get("date_start") and session.get("date_end"):
        return (
            _parse_openf1_datetime(session["date_start"]),
            _parse_openf1_datetime(session["date_end"]),
        )

    sk = session["session_key"]
    rows = _get_json("sessions", params={"session_key": sk})
    if not rows or not rows[0].get("date_start") or not rows[0].get("date_end"):
        raise ValueError(f"Tidak bisa menentukan date_start/date_end untuk session_key={sk}")

    return (
        _parse_openf1_datetime(rows[0]["date_start"]),
        _parse_openf1_datetime(rows[0]["date_end"]),
    )


def _fetch_telemetry_window(
    endpoint: str,
    session_key: int,
    driver_number: int,
    start: datetime,
    end: datetime,
) -> list[dict]:
    """
    Ambil telemetry dalam window. Jika 422, belah window secara rekursif.
    Ini yang menyelesaikan kasus seperti:
    /car_data?session_key=9462&driver_number=1
    """
    params = {
        "session_key": session_key,
        "driver_number": driver_number,
        "date>=": _to_openf1_datetime(start),
        "date<": _to_openf1_datetime(end),
    }

    try:
        return _get_json(endpoint, params=params, timeout=180)
    except OpenF1UnprocessableEntity:
        window_seconds = (end - start).total_seconds()

        if window_seconds <= TELEMETRY_MIN_WINDOW_SECONDS:
            log.exception(
                "422 tetap terjadi pada window minimum. endpoint=%s session_key=%s driver=%s start=%s end=%s",
                endpoint, session_key, driver_number, start, end,
            )
            raise

        midpoint = start + timedelta(seconds=window_seconds / 2)
        log.warning(
            "422 pada %s session=%s driver=%s window=%s - %s. Window dibelah menjadi dua.",
            endpoint, session_key, driver_number, start, end,
        )

        left = _fetch_telemetry_window(endpoint, session_key, driver_number, start, midpoint)
        right = _fetch_telemetry_window(endpoint, session_key, driver_number, midpoint, end)
        return left + right


def _fetch_telemetry_endpoint(
    endpoint: str,
    session_key: int,
    driver_numbers: list[int],
    session_start: datetime,
    session_end: datetime,
) -> list[dict]:
    data: list[dict] = []

    for driver_no in sorted(driver_numbers):
        for window_start, window_end in _iter_time_windows(
            session_start,
            session_end,
            TELEMETRY_WINDOW_MINUTES,
        ):
            rows = _fetch_telemetry_window(
                endpoint=endpoint,
                session_key=session_key,
                driver_number=driver_no,
                start=window_start,
                end=window_end,
            )
            data.extend(rows)

            log.info(
                "[%s] session_key=%s driver=%s window=%s - %s rows=%s",
                endpoint, session_key, driver_no, window_start, window_end, len(rows),
            )

    return data


def _to_parquet_bytes(data: list[dict], endpoint: str) -> bytes:
    """
    Mengonversi list of dicts ke bytes Parquet via pandas
    dengan DYNAMIC SCHEMA ENFORCEMENT berdasarkan dictionary ENDPOINTS.
    """
    df = pd.DataFrame(data) if data else pd.DataFrame()

    if not df.empty:
        # 1. Bangun mapping tipe data (Snowflake -> Pandas) secara dinamis
        schema_mapping = {}
        
        for col_name, sql_expr in ENDPOINTS[endpoint]["columns"].items():
            # Skip kolom jika tidak dikembalikan oleh API pada respons kali ini
            if col_name not in df.columns:
                continue 
            
            # Ekstrak tipe data dari string (contoh: "$1:gap_to_leader::VARCHAR" -> "VARCHAR")
            snow_type = sql_expr.split("::")[-1].upper()
            
            # Petakan ke Nullable Types milik Pandas yang aman terhadap NaN/Null
            if snow_type == "VARCHAR":
                schema_mapping[col_name] = "string"
            elif snow_type == "INTEGER":
                schema_mapping[col_name] = "Int64"   # Wajib huruf besar 'I'
            elif snow_type == "FLOAT":
                schema_mapping[col_name] = "Float64" # Wajib huruf besar 'F'
            elif snow_type == "BOOLEAN":
                schema_mapping[col_name] = "boolean" # Wajib huruf kecil
            elif snow_type == "TIMESTAMP_NTZ":
                # Biarkan timestamp sebagai string di Parquet, 
                # Snowflake via COPY INTO sangat handal dalam mem-parsing string ISO ke TIMESTAMP
                schema_mapping[col_name] = "string" 
            elif snow_type in {"VARIANT", "ARRAY", "OBJECT"}:
                continue

        # 2. Terapkan (cast) tipe data ke DataFrame sebelum diubah ke Parquet
        try:
            df = df.astype(schema_mapping)
        except Exception as e:
            log.error("Gagal enforce schema pada endpoint %s. Mapping: %s", endpoint, schema_mapping)
            raise e

    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    parquet_bytes = buf.getvalue()
    
    # Bebaskan memori
    del df
    buf.close()
    
    return parquet_bytes


def _build_jinja_sql_template(endpoint: str, cfg: dict) -> str:
    """
    Menghasilkan string SQL berisi Jinja for-loop.
    Saat dieksekusi oleh SQLExecuteQueryOperator, Airflow akan merender
    blok BEGIN..COMMIT ini sebanyak jumlah session yang diteruskan dari task awal.

    FILE_FORMAT diubah ke PARQUET — ekspresi $1:col::TYPE tetap valid
    di Snowflake untuk staged Parquet files sehingga tidak perlu mengubah
    definisi kolom di ENDPOINTS.
    """
    full_table = f"{SNOW_DB}.{SNOW_SCHEMA}.{cfg['table']}"
    col_names = ",\n    ".join(cfg["columns"].keys())
    col_exprs = ",\n        ".join(cfg["columns"].values())

    # Khusus untuk tabel F1_MEETINGS, id kolomnya adalah meeting_key. Sisanya session_key.
    id_col = "meeting_key" if endpoint == "meetings" else "session_key"
    id_val = "{{ session.meeting_key }}" if endpoint == "meetings" else "{{ session.session_key }}"

    return f"""
    {{% set sessions = ti.xcom_pull(task_ids='check_new_sessions') %}}
    {{% if sessions %}}
        {{% for session in sessions %}}
            BEGIN;

            -- 1. DELETE (Idempotent Overwrite per Session/Meeting)
            DELETE FROM {full_table} WHERE {id_col} = {id_val};

            -- 2. COPY INTO dari Parquet
            COPY INTO {full_table} (
                {col_names}
            )
            FROM (
                SELECT
                    {col_exprs}
                FROM @{SNOW_STAGE}/{endpoint}/meeting_key={{{{ session.meeting_key }}}}/session_key={{{{ session.session_key }}}}/
            )
            FILE_FORMAT = (TYPE = 'PARQUET')
            FORCE = TRUE;
            COMMIT;
        {{% endfor %}}
    {{% endif %}}
    """


# Build query templates saat parse time DAG
_LOAD_SQL_TEMPLATE: str = "\n".join(
    _build_jinja_sql_template(ep, cfg) for ep, cfg in ENDPOINTS.items()
)

# ═══════════════════════════════════════════════════════════════════════════════
#  DAG
# ═══════════════════════════════════════════════════════════════════════════════

@dag(
    dag_id="openf1_session_pipeline_v3",
    description="OpenF1 API → S3 (Parquet) → Snowflake → dbt (Event-driven, partitioned by session)",
    schedule=CronDataIntervalTimetable("0 0 1 * *", timezone="UTC"),  # Berjalan setiap tanggal 1 jam 00:00 UTC untuk mengecek jadwal balapan bulan sebelumnya
    start_date=datetime(2024, 1, 1),
    catchup=True,
    max_active_runs=1,
    tags=["openf1", "f1", "snowflake", "dbt"],
    doc_md=__doc__,
    default_args={
        "retries": 6,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
        "owner": "data-engineering",
    },
)
def openf1_session_pipeline() -> None:

    # ── Task 1 : Penjaga Gerbang (Short Circuit) ─────────────────────────────
    @task.short_circuit
    def check_new_sessions(data_interval_start=None, data_interval_end=None) -> list[dict] | bool:
        """
        Mengecek apakah ada sesi balapan yang selesai dalam interval ini.
        Jika tidak ada, pipeline otomatis di-skip. Jika ada, teruskan datanya ke XCom.
        Output list[dict] ini yang nantinya di-.expand() oleh extract_api_to_s3.
        """
        start_iso = data_interval_start.isoformat()
        end_iso   = data_interval_end.isoformat()

        url = f"{OPENF1_BASE}/sessions?date_end>={start_iso}&date_end<{end_iso}"
        sessions = _get_json(url)

        if not sessions:
            log.info("Tidak ada sesi F1 yang berakhir antara %s dan %s.", start_iso, end_iso)
            return False  # Menghentikan eksekusi task di bawahnya

        active_sessions = [
            {
                "meeting_key": s["meeting_key"],
                "session_key": s["session_key"],
                "date_start": s.get("date_start"),
                "date_end": s.get("date_end"),
            }
            for s in sessions
        ]
        log.info("Ditemukan %d sesi baru: %s", len(active_sessions), active_sessions)
        log.info("url: %s", url)
        return active_sessions  # Otomatis masuk ke XCom, lalu di-expand()

# ── Task 2 : Extract (Dynamic Task Mapping — 1 worker per session) ───────
    @task(max_active_tis_per_dagrun=1, pool="openf1_api")
    def extract_api_to_s3(session: dict) -> None:
        sk = session["session_key"]
        mk = session["meeting_key"]

        s3 = S3Hook(aws_conn_id=AWS_CONN_ID)

        # 1. Ambil daftar pembalap dan batas waktu session.
        #    Untuk telemetry, kombinasi session_key + driver_number saja belum cukup aman
        #    karena payload bisa terlalu besar dan memicu HTTP 422.
        drivers_data = _get_json("drivers", params={"session_key": sk})
        driver_numbers = list({
            d.get("driver_number")
            for d in drivers_data
            if d.get("driver_number") is not None
        })
        session_start, session_end = _get_session_bounds(session)

        for idx, endpoint in enumerate(ENDPOINTS.keys()):

            # 2. Fetch Data
            if endpoint in TELEMETRY_ENDPOINTS:
                data = _fetch_telemetry_endpoint(
                    endpoint=endpoint,
                    session_key=sk,
                    driver_numbers=driver_numbers,
                    session_start=session_start,
                    session_end=session_end,
                )

            elif endpoint == "meetings":
                data = _get_json("meetings", params={"meeting_key": mk})

            else:
                # Endpoint biasa cukup difilter per session_key.
                data = _get_json(endpoint, params={"session_key": sk})

            # ── JSON → Parquet ────────────────────────────────────────────────
            parquet_bytes = _to_parquet_bytes(data, endpoint)
            del data 

            # ── Upload ke S3 ──────────────────────────────────────────────────
            s3_key = f"{S3_ROOT}/{endpoint}/meeting_key={mk}/session_key={sk}/data.parquet"
            s3.load_bytes(
                bytes_data=parquet_bytes,
                key=s3_key,
                bucket_name=S3_BUCKET,
                replace=True,  
            )
            del parquet_bytes 

            log.info(
                "[%s] session_key=%s tersimpan → s3://%s/%s",
                endpoint, sk, S3_BUCKET, s3_key,
            )

    # ── Task 3 : Load S3 → Snowflake (Idempotent per-session) ───────────────
    load = SQLExecuteQueryOperator(
        task_id="load_s3_to_snowflake",
        conn_id=SNOW_CONN_ID,
        sql=_LOAD_SQL_TEMPLATE,
        split_statements=True,  # Diperlukan karena terdapat blok BEGIN..COMMIT
        return_last=False,
    )

    # # ── Task 4 : dbt ─────────────────────────────────────────────────────────
    # def _dbt_task(task_id: str, select: str) -> DockerOperator:
    #     # dbt project dan profiles di-bind mount dari host agar perubahan file lokal
    #     # langsung terbaca tanpa rebuild image. dbt deps dijalankan setiap task untuk
    #     # memastikan dbt_packages sinkron dengan packages.yml pada project host.
    #     dbt_command = (
    #         "dbt deps "
    #         f"--profiles-dir {DBT_PROFILES_CONTAINER_PATH} "
    #         f"--project-dir {DBT_PROJECT_CONTAINER_PATH} && "
    #         "dbt run "
    #         f"--select {select} "
    #         f"--profiles-dir {DBT_PROFILES_CONTAINER_PATH} "
    #         f"--project-dir {DBT_PROJECT_CONTAINER_PATH} "
    #         "--target dev"
    #     )

    #     return DockerOperator(
    #         task_id=task_id,
    #         image=DBT_IMAGE,
    #         command=["bash", "-lc", dbt_command],
    #         environment=DBT_ENV,
    #         docker_url="unix://var/run/docker.sock",
    #         network_mode="bridge",
    #         auto_remove="force",
    #         mount_tmp_dir=False,
    #         mounts=[
    #             Mount(
    #                 target=DBT_PROJECT_CONTAINER_PATH,
    #                 source=DBT_PROJECT_HOST_PATH,
    #                 type="bind",
    #             ),
    #             Mount(
    #                 target=DBT_PROFILES_CONTAINER_PATH,
    #                 source=DBT_PROFILES_HOST_PATH,
    #                 type="bind",
    #             ),
    #             Mount(
    #                 target=f"{DBT_PROJECT_CONTAINER_PATH}/target",
    #                 source="dbt_target_vol",
    #                 type="volume",
    #             ),
    #             Mount(
    #                 target=f"{DBT_PROJECT_CONTAINER_PATH}/logs",
    #                 source="dbt_logs_vol",
    #                 type="volume",
    #             ),
    #         ],
    #     )

    # dbt_staging      = _dbt_task("dbt_run_staging",      "staging")
    # dbt_intermediate = _dbt_task("dbt_run_intermediate", "intermediate")
    # dbt_marts        = _dbt_task("dbt_run_marts",         "marts")

    # ── Wiring ───────────────────────────────────────────────────────────────
    # check_new_sessions() mengembalikan list[dict] → menjadi input .expand().
    # Airflow akan membuat N instance extract_api_to_s3, satu per sesi.
    # load_s3_to_snowflake akan menunggu SEMUA instance selesai sebelum berjalan.
    active_sessions = check_new_sessions()

    # Dynamic Task Mapping: ganti .expand() dari loop tunggal menjadi N task paralel
    extract_tasks = extract_api_to_s3.expand(session=active_sessions)

    extract_tasks >> load # >> dbt_staging >> dbt_intermediate >> dbt_marts


openf1_session_pipeline()