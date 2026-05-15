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
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

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
# Jeda antar-request agar IP home server tidak diblokir oleh OpenF1.
API_SLEEP_SECONDS = 2

# Endpoint dengan payload besar.
# car_data adalah telemetry berfrekuensi tinggi ±3.7 Hz, sehingga satu sesi penuh
# per driver masih dapat terlalu besar untuk satu request API.
TELEMETRY_ENDPOINTS = {"car_data", "location"}

# Sesuai kebutuhan: rentang waktu sesi dibagi 2 terlebih dahulu.
# Jika 422 tetap muncul pada salah satu chunk, helper akan split lagi secara rekursif.
TELEMETRY_TIME_CHUNKS = 2
TELEMETRY_MAX_SPLIT_DEPTH = 5

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

def _format_query_value(value) -> str:
    """
    Format nilai query agar aman untuk URL OpenF1.

    Kunci filter seperti date_start>= dan date_end<= sengaja tidak di-encode,
    karena dokumentasi OpenF1 memakai operator langsung pada nama parameter.
    Nilai tetap di-encode seperlunya agar karakter berisiko tidak merusak URL.
    """
    if isinstance(value, bool):
        return str(value).lower()
    if value is None:
        return ""
    return str(value)


def _build_openf1_url(endpoint_or_url: str, params: dict | None = None) -> str:
    """
    Membuat URL OpenF1 dengan operator filter sesuai dokumentasi.

    Catatan penting:
    OpenF1 memakai sintaks filter seperti:
    - lap_duration>=120
    - date_start>=2023-09-01
    - date_end<=2023-09-30
    - date>=2024-02-21T07:00:00Z
    - date<2024-02-21T11:30:00Z

    Karena itu parameter beroperator tidak boleh dibentuk sebagai dict requests
    standar seperti {"date>=": value}. Jika dipakai langsung oleh requests,
    hasilnya menjadi date%3E%3D=value, yang secara raw query berbeda dari
    pola dokumentasi. Fungsi ini membentuk query string sendiri agar hasilnya
    menjadi date%3E=value untuk >= dan date%3Cvalue untuk <.
    """
    if endpoint_or_url.startswith("http://") or endpoint_or_url.startswith("https://"):
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
            # Contoh: key="date>=", value="2024-02-21T07:00:00Z"
            # Raw URL: date>=2024-02-21T07:00:00Z
            # Prepared requests URL: date%3E=2024-02-21T07:00:00Z
            query_parts.append(f"{key}{encoded_value}")
        elif key.endswith((">", "<")):
            # Untuk operator strict seperti date<end, browser/requests akan menyiapkan URL
            # menjadi date%3Cend=. Bentuk ini mengikuti pola yang terbukti berhasil pada OpenF1.
            query_parts.append(f"{key}{encoded_value}=")
        else:
            query_parts.append(f"{key}={encoded_value}")

    separator = "&" if "?" in base_url else "?"
    return base_url + separator + "&".join(query_parts)


def _get_json(url: str, params: dict | None = None, timeout: int = 120) -> list[dict]:
    """
    GET JSON dari OpenF1 API.

    Catatan penting:
    - Operator filter dibuat eksplisit di URL agar struktur query sama dengan dokumentasi OpenF1.
    - Fallback tetap disediakan pada check_new_sessions jika server OpenF1 mengembalikan 5xx.
    """
    final_url = _build_openf1_url(url, params=params)
    log.debug("GET %s", final_url)

    resp = requests.get(final_url, timeout=timeout)

    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        # Jika API merespons 404 (Not Found), artinya data untuk
        # endpoint pada sesi ini memang kosong di database OpenF1.
        if resp.status_code == 404:
            log.warning(
                "Data tidak ditemukan (404) untuk %s. Mengembalikan array kosong.",
                final_url,
            )
            return []

        # 422 biasanya muncul ketika request terlalu besar.
        # Untuk endpoint telemetry, pemecahan request ditangani di helper khusus.
        if resp.status_code == 422:
            log.error(
                "OpenF1 menolak request karena payload terlalu besar atau parameter tidak valid. "
                "URL=%s Response=%s",
                final_url,
                resp.text[:500],
            )

        # Error lain seperti 429 atau 5xx tetap raise agar retry Airflow aktif,
        # kecuali dipanggil dari helper yang memang punya fallback eksplisit.
        raise e

    return resp.json()


def _parse_openf1_datetime(value: str | datetime) -> datetime:
    """
    Parse datetime OpenF1 dari string ISO 8601 atau objek datetime.
    Semua nilai dinormalisasi ke UTC agar komparasi interval Airflow stabil.
    """
    if isinstance(value, datetime):
        dt = value
    else:
        if not value:
            raise ValueError("Nilai datetime kosong. date_start/date_end wajib tersedia.")
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _format_openf1_datetime(value: str | datetime) -> str:
    """
    Format datetime untuk parameter OpenF1.
    Menggunakan suffix Z agar URL tidak membawa karakter '+' pada timezone.
    """
    dt = _parse_openf1_datetime(value).replace(microsecond=0)
    return dt.isoformat().replace("+00:00", "Z")


def _format_openf1_date(value: str | datetime) -> str:
    """
    Format date-only untuk query /sessions agar sesuai contoh dokumentasi OpenF1.
    """
    return _parse_openf1_datetime(value).date().isoformat()


def _split_time_range(date_start: str | datetime, date_end: str | datetime, chunks: int = 2) -> list[tuple[str, str]]:
    """
    Membagi rentang waktu menjadi beberapa chunk half-open: [start, end).
    Untuk kebutuhan ini default dibagi 2.
    """
    start_dt = _parse_openf1_datetime(date_start)
    end_dt = _parse_openf1_datetime(date_end)

    if end_dt <= start_dt:
        raise ValueError(f"date_end harus lebih besar dari date_start. date_start={date_start}, date_end={date_end}")

    chunks = max(1, int(chunks))
    total_seconds = (end_dt - start_dt).total_seconds()

    ranges: list[tuple[str, str]] = []
    for i in range(chunks):
        chunk_start = start_dt + timedelta(seconds=(total_seconds * i / chunks))
        chunk_end = start_dt + timedelta(seconds=(total_seconds * (i + 1) / chunks))
        ranges.append((_format_openf1_datetime(chunk_start), _format_openf1_datetime(chunk_end)))

    return ranges


def _fetch_endpoint_time_chunk(
    endpoint: str,
    session_key: int,
    driver_number: int,
    date_start: str,
    date_end: str,
    split_depth: int = TELEMETRY_MAX_SPLIT_DEPTH,
) -> list[dict]:
    """
    Mengambil satu endpoint telemetry untuk satu driver pada satu rentang waktu.

    Jika OpenF1 tetap mengembalikan 422, chunk dibagi dua lagi secara rekursif.
    Ini menjaga pipeline tetap selesai untuk sesi yang panjang atau data yang padat.
    """
    url = f"{OPENF1_BASE}/{endpoint}"
    params = {
        "session_key": session_key,
        "driver_number": driver_number,
        "date>=": date_start,
        "date<": date_end,
    }

    time.sleep(API_SLEEP_SECONDS)

    try:
        return _get_json(url, params=params)
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code if e.response is not None else None

        if status_code != 422 or split_depth <= 0:
            raise

        start_dt = _parse_openf1_datetime(date_start)
        end_dt = _parse_openf1_datetime(date_end)
        duration_seconds = (end_dt - start_dt).total_seconds()

        # Hindari split tanpa akhir jika range sudah terlalu kecil.
        if duration_seconds <= 1:
            raise

        mid_dt = start_dt + timedelta(seconds=duration_seconds / 2)
        mid_iso = _format_openf1_datetime(mid_dt)

        log.warning(
            "422 pada %s session_key=%s driver_number=%s range=%s sampai %s. "
            "Chunk dipecah lagi menjadi dua. Sisa depth=%s.",
            endpoint,
            session_key,
            driver_number,
            date_start,
            date_end,
            split_depth,
        )

        left_data = _fetch_endpoint_time_chunk(
            endpoint=endpoint,
            session_key=session_key,
            driver_number=driver_number,
            date_start=date_start,
            date_end=mid_iso,
            split_depth=split_depth - 1,
        )
        right_data = _fetch_endpoint_time_chunk(
            endpoint=endpoint,
            session_key=session_key,
            driver_number=driver_number,
            date_start=mid_iso,
            date_end=date_end,
            split_depth=split_depth - 1,
        )

        return left_data + right_data


def _fetch_telemetry_by_driver_and_time(
    endpoint: str,
    session_key: int,
    driver_numbers: list[int],
    date_start: str,
    date_end: str,
) -> list[dict]:
    """
    Fetch telemetry endpoint dengan strategi:
    1. loop per driver;
    2. bagi rentang session date_start-date_end menjadi 2;
    3. fallback recursive split jika 422 masih muncul.
    """
    initial_ranges = _split_time_range(date_start, date_end, chunks=TELEMETRY_TIME_CHUNKS)

    all_data: list[dict] = []

    log.info(
        "Mulai fetch %s session_key=%s untuk %d driver. Rentang sesi %s sampai %s dibagi menjadi %d chunk.",
        endpoint,
        session_key,
        len(driver_numbers),
        date_start,
        date_end,
        len(initial_ranges),
    )

    for driver_no in driver_numbers:
        for chunk_idx, (chunk_start, chunk_end) in enumerate(initial_ranges, start=1):
            log.info(
                "Fetch %s session_key=%s driver_number=%s chunk=%s/%s range=%s sampai %s",
                endpoint,
                session_key,
                driver_no,
                chunk_idx,
                len(initial_ranges),
                chunk_start,
                chunk_end,
            )

            chunk_data = _fetch_endpoint_time_chunk(
                endpoint=endpoint,
                session_key=session_key,
                driver_number=driver_no,
                date_start=chunk_start,
                date_end=chunk_end,
            )
            all_data.extend(chunk_data)

    log.info(
        "Selesai fetch %s session_key=%s. Total rows=%d",
        endpoint,
        session_key,
        len(all_data),
    )

    return all_data

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
        start_dt = _parse_openf1_datetime(data_interval_start)
        end_dt = _parse_openf1_datetime(data_interval_end)

        start_iso = _format_openf1_datetime(start_dt)
        end_iso = _format_openf1_datetime(end_dt)
        start_date = _format_openf1_date(start_dt)

        # Airflow memakai interval half-open [start, end).
        # Agar tetap memakai operator dokumentasi OpenF1 date_end<=,
        # end dikurangi 1 detik sehingga tidak menarik sesi bulan berikutnya.
        end_inclusive_iso = _format_openf1_datetime(end_dt - timedelta(seconds=1))

        sessions_url = f"{OPENF1_BASE}/sessions"
        sessions_params = {
            "date_start>=": start_date,
            "date_end<=": end_inclusive_iso,
        }

        try:
            sessions = _get_json(sessions_url, params=sessions_params)
            log.info(
                "Fetched %d sessions dari OpenF1 dengan filter dokumentasi: date_start >= %s dan date_end <= %s.",
                len(sessions),
                start_date,
                end_inclusive_iso,
            )
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else None

            # Fallback defensif: jika OpenF1 memberi 5xx untuk filter tanggal,
            # ambil per year lalu filter lokal. Ini mencegah DAG gagal karena masalah server API.
            if status_code not in {500, 502, 503, 504}:
                raise

            log.warning(
                "OpenF1 mengembalikan HTTP %s untuk filter tanggal /sessions. "
                "Fallback ke /sessions?year=YYYY lalu filter lokal. Interval=%s sampai %s",
                status_code,
                start_iso,
                end_iso,
            )

            sessions = []
            for year in range(start_dt.year, end_dt.year + 1):
                time.sleep(API_SLEEP_SECONDS)
                year_sessions = _get_json(sessions_url, params={"year": year})
                sessions.extend(year_sessions)
                log.info("Fetched %d sessions dari OpenF1 untuk year=%s.", len(year_sessions), year)

        if not sessions:
            log.info("Tidak ada sessions dari OpenF1 untuk interval %s sampai %s.", start_iso, end_iso)
            return False

        active_sessions = []
        seen_session_keys: set[int] = set()

        for s in sessions:
            if not s.get("date_start") or not s.get("date_end"):
                log.warning(
                    "Session dilewati karena date_start/date_end kosong: meeting_key=%s session_key=%s raw=%s",
                    s.get("meeting_key"),
                    s.get("session_key"),
                    s,
                )
                continue

            session_end_dt = _parse_openf1_datetime(s["date_end"])

            # Tetap filter lokal untuk menjaga idempotency interval Airflow.
            if not (start_dt <= session_end_dt < end_dt):
                continue

            session_key = s["session_key"]
            if session_key in seen_session_keys:
                continue
            seen_session_keys.add(session_key)

            active_sessions.append(
                {
                    "meeting_key": s["meeting_key"],
                    "session_key": session_key,
                    "date_start": s["date_start"],
                    "date_end": s["date_end"],
                }
            )

        if not active_sessions:
            log.info("Tidak ada sesi F1 yang berakhir antara %s dan %s setelah filter lokal.", start_iso, end_iso)
            return False

        log.info("Ditemukan %d sesi baru: %s", len(active_sessions), active_sessions)
        log.info("Filter akhir check_new_sessions: date_end >= %s dan date_end < %s", start_iso, end_iso)
        return active_sessions  # Otomatis masuk ke XCom, lalu di-expand()

# ── Task 2 : Extract (Dynamic Task Mapping — 1 worker per session) ───────
    @task(max_active_tis_per_dagrun=1)
    def extract_api_to_s3(session: dict) -> None:
        sk = session["session_key"]
        mk = session["meeting_key"]
        date_start = session["date_start"]
        date_end = session["date_end"]

        s3 = S3Hook(aws_conn_id=AWS_CONN_ID)

        # 1. Ambil daftar pembalap terlebih dahulu untuk paginasi endpoint berat.
        time.sleep(API_SLEEP_SECONDS)
        drivers_url = f"{OPENF1_BASE}/drivers"
        drivers_data = _get_json(drivers_url, params={"session_key": sk})
        driver_numbers = sorted(
            {d.get("driver_number") for d in drivers_data if d.get("driver_number") is not None}
        )

        if not driver_numbers:
            log.warning("Tidak ada driver_number untuk session_key=%s. Telemetry endpoint akan kosong.", sk)

        for endpoint in ENDPOINTS.keys():

            # 2. Fetch Data.
            # Endpoint telemetry seperti car_data wajib dibatasi per driver dan per time range
            # agar request tidak terlalu besar dan tidak memicu HTTP 422.
            if endpoint in TELEMETRY_ENDPOINTS:
                data = _fetch_telemetry_by_driver_and_time(
                    endpoint=endpoint,
                    session_key=sk,
                    driver_numbers=driver_numbers,
                    date_start=date_start,
                    date_end=date_end,
                )

            elif endpoint == "meetings":
                time.sleep(API_SLEEP_SECONDS)
                url = f"{OPENF1_BASE}/meetings"
                data = _get_json(url, params={"meeting_key": mk})

            else:
                time.sleep(API_SLEEP_SECONDS)
                url = f"{OPENF1_BASE}/{endpoint}"
                data = _get_json(url, params={"session_key": sk})

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