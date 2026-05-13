"""
openf1_pipeline.py
══════════════════════════════════════════════════════════════════════════════
OpenF1 API  →  S3  →  Snowflake  →  dbt  (Event-driven, partitioned by session)

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
  • Snowflake : DELETE WHERE session_key = {SK} + COPY INTO
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

# ── dbt Docker image ──────────────────────────────────────────────────────────
DBT_IMAGE = "f1-dbt-snowflake:1.0"
DBT_ENV: dict[str, str] = {
    "SNOWFLAKE_ACCOUNT":   "{{ var.value.SNOWFLAKE_ACCOUNT }}",
    "SNOWFLAKE_USER":      "{{ var.value.SNOWFLAKE_USER }}",
    "SNOWFLAKE_PASSWORD":  "{{ var.value.SNOWFLAKE_PASSWORD }}",
    "SNOWFLAKE_ROLE":      "dbt_role",
    "SNOWFLAKE_DATABASE":  SNOW_DB,
    "SNOWFLAKE_WAREHOUSE": "dbt_wh",
    "SNOWFLAKE_SCHEMA":    SNOW_SCHEMA,
}

OPENF1_BASE = "https://api.openf1.org/v1"

# ── API throttle ──────────────────────────────────────────────────────────────
# Jeda antar-request agar IP home server tidak diblokir oleh OpenF1.
API_SLEEP_SECONDS = 2

# ── Endpoint catalogue ────────────────────────────────────────────────────────
# Filter berbasis tanggal telah dihapus karena kita menarik data
# berdasarkan identitas event (session_key / meeting_key).
#
# Kunci "columns" masih dipakai untuk membangun SQL COPY INTO Snowflake.
# Format file sudah diubah ke PARQUET — ekspresi $1:col::TYPE tetap valid
# di Snowflake untuk staged Parquet files.
ENDPOINTS: dict[str, dict] = {
    "car_data": {
        "table": "F1_CAR_DATA",
        "columns": {
            "session_key":   "$1:session_key::INTEGER",
            "driver_number": "$1:driver_number::INTEGER",
            "date":          "$1:date::TIMESTAMP_NTZ",
            "rpm":           "$1:rpm::INTEGER",
            "speed":         "$1:speed::INTEGER",
            "throttle":      "$1:throttle::INTEGER",
            "brake":         "$1:brake::INTEGER",
            "drs":           "$1:drs::INTEGER",
        },
    },
    "drivers": {
        "table": "F1_DRIVERS",
        "columns": {
            "driver_number":   "$1:driver_number::INTEGER",
            "broadcast_name":  "$1:broadcast_name::VARCHAR",
            "first_name":      "$1:first_name::VARCHAR",
            "last_name":       "$1:last_name::VARCHAR",
            "full_name":       "$1:full_name::VARCHAR",
            "name_acronym":    "$1:name_acronym::VARCHAR",
            "team_name":       "$1:team_name::VARCHAR",
            "team_colour":     "$1:team_colour::VARCHAR",
            "country_code":    "$1:country_code::VARCHAR",
            "session_key":     "$1:session_key::INTEGER",
        },
    },
    "intervals": {
        "table": "F1_INTERVALS",
        "columns": {
            "session_key":   "$1:session_key::INTEGER",
            "driver_number": "$1:driver_number::INTEGER",
            "date":          "$1:date::TIMESTAMP_NTZ",
            "gap_to_leader": "$1:gap_to_leader::VARCHAR",
            "interval":      "$1:interval::VARCHAR",
        },
    },
    "laps": {
        "table": "F1_LAPS",
        "columns": {
            "session_key":       "$1:session_key::INTEGER",
            "driver_number":     "$1:driver_number::INTEGER",
            "date_start":        "$1:date_start::TIMESTAMP_NTZ",
            "lap_duration":      "$1:lap_duration::FLOAT",
            "lap_number":        "$1:lap_number::INTEGER",
            "is_pit_out_lap":    "$1:is_pit_out_lap::BOOLEAN",
            "duration_sector_1": "$1:duration_sector_1::FLOAT",
            "duration_sector_2": "$1:duration_sector_2::FLOAT",
            "duration_sector_3": "$1:duration_sector_3::FLOAT",
        },
    },
    "meetings": {
        "table": "F1_MEETINGS",
        "columns": {
            "meeting_key":           "$1:meeting_key::INTEGER",
            "meeting_name":          "$1:meeting_name::VARCHAR",
            "meeting_official_name": "$1:meeting_official_name::VARCHAR",
            "country_name":          "$1:country_name::VARCHAR",
            "circuit_key":           "$1:circuit_key::INTEGER",
            "circuit_short_name":    "$1:circuit_short_name::VARCHAR",
            "year":                  "$1:year::INTEGER",
            "date_start":            "$1:date_start::TIMESTAMP_NTZ",
        },
    },
    "pit": {
        "table": "F1_PIT",
        "columns": {
            "session_key":   "$1:session_key::INTEGER",
            "driver_number": "$1:driver_number::INTEGER",
            "date":          "$1:date::TIMESTAMP_NTZ",
            "lap_number":    "$1:lap_number::INTEGER",
            "pit_duration":  "$1:pit_duration::FLOAT",
        },
    },
    "position": {
        "table": "F1_POSITION",
        "columns": {
            "session_key":   "$1:session_key::INTEGER",
            "driver_number": "$1:driver_number::INTEGER",
            "date":          "$1:date::TIMESTAMP_NTZ",
            "meeting_key":   "$1:meeting_key::INTEGER",
            "position":      "$1:position::INTEGER",
        },
    },
    "sessions": {
        "table": "F1_SESSIONS",
        "columns": {
            "session_key":        "$1:session_key::INTEGER",
            "meeting_key":        "$1:meeting_key::INTEGER",
            "country_name":       "$1:country_name::VARCHAR",
            "session_name":       "$1:session_name::VARCHAR",
            "session_type":       "$1:session_type::VARCHAR",
            "year":               "$1:year::INTEGER",
            "circuit_short_name": "$1:circuit_short_name::VARCHAR",
            "date_start":         "$1:date_start::TIMESTAMP_NTZ",
            "date_end":           "$1:date_end::TIMESTAMP_NTZ",
        },
    },
    "stints": {
        "table": "F1_STINTS",
        "columns": {
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
        "table": "F1_WEATHER",
        "columns": {
            "session_key":       "$1:session_key::INTEGER",
            "date":              "$1:date::TIMESTAMP_NTZ",
            "air_temperature":   "$1:air_temperature::FLOAT",
            "track_temperature": "$1:track_temperature::FLOAT",
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

def _get_json(url: str, timeout: int = 120) -> list[dict]:
    log.debug("GET %s", url)
    resp = requests.get(url, timeout=timeout)
    
    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        # Jika API merespons 404 (Not Found), artinya data untuk 
        # endpoint pada sesi ini memang kosong di database OpenF1.
        if resp.status_code == 404:
            log.warning("Data tidak ditemukan (404) untuk %s. Mengembalikan array kosong.", url)
            return []
            
        # Jika menerima error lain (seperti 429 Too Many Requests atau 5xx Server Error),
        # tetap raise exception agar mekanisme retry & backoff Airflow Anda tetap terpicu.
        raise e
        
    return resp.json()


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
            FILE_FORMAT = (TYPE = 'PARQUET');

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
    dag_id="openf1_session_pipeline_v2",
    description="OpenF1 API → S3 (Parquet) → Snowflake → dbt (Event-driven, partitioned by session)",
    schedule=CronDataIntervalTimetable("0 0 1 * *", timezone="UTC"),  # Berjalan setiap tanggal 1 jam 00:00 UTC untuk mengecek jadwal balapan bulan sebelumnya
    start_date=datetime(2024, 1, 1),
    catchup=True,
    max_active_runs=1,
    tags=["openf1", "f1", "snowflake", "dbt"],
    doc_md=__doc__,
    default_args={
        "retries": 3,                           # Increase retries
        "retry_delay": timedelta(seconds=60),   # Start with a 1-minute delay
        "retry_exponential_backoff": True,      # Back off exponentially (1m, 2m, 4m...)
        "max_retry_delay": timedelta(minutes=10), # Cap the maximum delay
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
            {"meeting_key": s["meeting_key"], "session_key": s["session_key"]}
            for s in sessions
        ]
        log.info("Ditemukan %d sesi baru: %s", len(active_sessions), active_sessions)
        log.info("url: %s", url)
        return active_sessions  # Otomatis masuk ke XCom, lalu di-expand()

# ── Task 2 : Extract (Dynamic Task Mapping — 1 worker per session) ───────
    @task(max_active_tis_per_dagrun=1)
    def extract_api_to_s3(session: dict) -> None:
        sk = session["session_key"]
        mk = session["meeting_key"]

        s3 = S3Hook(aws_conn_id=AWS_CONN_ID)

        # 1. Ambil daftar pembalap terlebih dahulu untuk paginasi endpoint berat
        time.sleep(API_SLEEP_SECONDS)
        drivers_url = f"{OPENF1_BASE}/drivers?session_key={sk}"
        drivers_data = _get_json(drivers_url)
        driver_numbers = list({d.get("driver_number") for d in drivers_data if d.get("driver_number") is not None})

        for idx, endpoint in enumerate(ENDPOINTS.keys()):
            
            # 2. Fetch Data (Penanganan khusus untuk endpoint dengan payload masif)
            if endpoint in ["car_data", "location"]:
                # Wajib diloop per driver agar tidak terkena 422 Payload Too Large
                data = []
                for driver_no in driver_numbers:
                    time.sleep(API_SLEEP_SECONDS)
                    url = f"{OPENF1_BASE}/{endpoint}?session_key={sk}&driver_number={driver_no}"
                    data.extend(_get_json(url))
                    
            elif endpoint == "meetings":
                time.sleep(API_SLEEP_SECONDS)
                url = f"{OPENF1_BASE}/meetings?meeting_key={mk}"
                data = _get_json(url)
                
            else:
                # intervals dan position aman ditarik hanya dengan session_key
                time.sleep(API_SLEEP_SECONDS)
                url = f"{OPENF1_BASE}/{endpoint}?session_key={sk}"
                data = _get_json(url)

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

    # ── Task 4 : dbt ─────────────────────────────────────────────────────────
    def _dbt_task(task_id: str, select: str) -> DockerOperator:
        return DockerOperator(
            task_id=task_id,
            image=DBT_IMAGE,
            command=[
                "dbt", "run",
                "--select",       select,
                "--profiles-dir", "/root/.dbt",
                "--project-dir",  "/app",
                "--target",       "dev",
            ],
            environment=DBT_ENV,
            docker_url="unix://var/run/docker.sock",
            network_mode="bridge",
            auto_remove="force",
            mount_tmp_dir=False,
            mounts=[
                Mount(target="/app/target", source="dbt_target_vol", type="volume"),
                Mount(target="/app/logs",   source="dbt_logs_vol",   type="volume"),
            ],
        )

    dbt_staging      = _dbt_task("dbt_run_staging",      "staging")
    dbt_intermediate = _dbt_task("dbt_run_intermediate", "intermediate")
    dbt_marts        = _dbt_task("dbt_run_marts",         "marts")

    # ── Wiring ───────────────────────────────────────────────────────────────
    # check_new_sessions() mengembalikan list[dict] → menjadi input .expand().
    # Airflow akan membuat N instance extract_api_to_s3, satu per sesi.
    # load_s3_to_snowflake akan menunggu SEMUA instance selesai sebelum berjalan.
    active_sessions = check_new_sessions()

    # Dynamic Task Mapping: ganti .expand() dari loop tunggal menjadi N task paralel
    extract_tasks = extract_api_to_s3.expand(session=active_sessions)

    extract_tasks >> load >> dbt_staging >> dbt_intermediate >> dbt_marts


openf1_session_pipeline()