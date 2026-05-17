"""
openf1_dbt_snowflake_dag.py
══════════════════════════════════════════════════════════════════════════════
DAG khusus dbt untuk project OpenF1.

Tujuan:
  Snowflake RAW tables  →  dbt-snowflake transformations  →  ANALYTICS marts

Desain:
  • DAG ini sengaja dipisah dari DAG extract-load utama.
  • DAG ini dapat dijalankan manual dari Airflow UI.
  • DAG ini juga dapat dipanggil dari DAG utama setelah task load_s3_to_snowflake selesai
    memakai TriggerDagRunOperator.
  • dbt dijalankan melalui DockerOperator agar environment dbt-snowflake tetap terisolasi.
  • dbt project dan profiles.yml di-bind mount dari host agar perubahan dbt tidak perlu rebuild image.

Default run order:
  [dbt_deps]
      ↓
  [dbt_debug]
      ↓
  [dbt_build_staging]
      ↓
  [dbt_build_intermediate]
      ↓
  [dbt_build_marts]
      ↓
  [dbt_docs_generate]

Manual DAG run config opsional:
  {
    "full_refresh": false,
    "target": "dev",
    "threads": 4,
    "vars": "{}"
  }

Catatan penting:
  • Path host dibaca oleh Docker daemon, bukan oleh container Airflow.
  • Pastikan path DBT_PROJECT_HOST_PATH dan DBT_PROFILES_HOST_PATH benar di mesin host Docker.
  • Pastikan image f1-dbt-snowflake:1.0 sudah berisi dbt-snowflake==1.11.4.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import dag
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# ═══════════════════════════════════════════════════════════════════════════════
# Airflow / Docker / dbt configuration
# ═══════════════════════════════════════════════════════════════════════════════

DAG_ID = "openf1_dbt_snowflake"

# Docker image dbt.
# Build image ini dari Dockerfile dbt kamu, misalnya berisi:
#   pip install dbt-snowflake==1.11.4
DBT_IMAGE = "f1-dbt-snowflake:1.0"

# Path ini dibaca oleh Docker daemon di host, bukan dari filesystem container Airflow.
# Sesuaikan dengan struktur project kamu.
DBT_PROJECT_HOST_PATH = "/home/void/F1_RealtimeReplay_Historical_Project/dbt/dbt_project"
DBT_PROFILES_HOST_PATH = "/home/void/F1_RealtimeReplay_Historical_Project/dbt/dbt_profiles"

# Path di dalam container dbt.
DBT_PROJECT_CONTAINER_PATH = "/app"
DBT_PROFILES_CONTAINER_PATH = "/root/.dbt"

# Default Snowflake/dbt settings mengikuti file openf1_pipeline_v3.py.
SNOWFLAKE_ROLE = "dbt_role"
SNOWFLAKE_DATABASE = "dbt_db"
SNOWFLAKE_WAREHOUSE = "dbt_wh"
SNOWFLAKE_SCHEMA = "dbt_schema"

# Environment ini akan dirender oleh Airflow pada runtime.
# Gunakan Airflow Variables:
#   SNOWFLAKE_ACCOUNT
#   SNOWFLAKE_USER
#   SNOWFLAKE_PASSWORD
# Opsional pada manual run config:
#   target, threads, full_refresh, vars
DBT_ENV: dict[str, str] = {
    "SNOWFLAKE_ACCOUNT": "{{ var.value.SNOWFLAKE_ACCOUNT }}",
    "SNOWFLAKE_USER": "{{ var.value.SNOWFLAKE_USER }}",
    "SNOWFLAKE_PASSWORD": "{{ var.value.SNOWFLAKE_PASSWORD }}",
    "SNOWFLAKE_ROLE": SNOWFLAKE_ROLE,
    "SNOWFLAKE_DATABASE": SNOWFLAKE_DATABASE,
    "SNOWFLAKE_WAREHOUSE": SNOWFLAKE_WAREHOUSE,
    "SNOWFLAKE_SCHEMA": SNOWFLAKE_SCHEMA,
    "DBT_PROFILES_DIR": DBT_PROFILES_CONTAINER_PATH,
    "DBT_PROJECT_DIR": DBT_PROJECT_CONTAINER_PATH,
    "DBT_TARGET": "{{ dag_run.conf.get('target', 'dev') if dag_run and dag_run.conf else 'dev' }}",
    "DBT_THREADS": "{{ dag_run.conf.get('threads', 4) if dag_run and dag_run.conf else 4 }}",
    "DBT_FULL_REFRESH": "{{ dag_run.conf.get('full_refresh', true) if dag_run and dag_run.conf else true }}",
    "DBT_VARS": "{{ dag_run.conf.get('vars', '{}') if dag_run and dag_run.conf else '{}' }}",
}

DBT_MOUNTS = [
    Mount(
        target=DBT_PROJECT_CONTAINER_PATH,
        source=DBT_PROJECT_HOST_PATH,
        type="bind",
    ),
    Mount(
        target=DBT_PROFILES_CONTAINER_PATH,
        source=DBT_PROFILES_HOST_PATH,
        type="bind",
    ),
    Mount(
        target=f"{DBT_PROJECT_CONTAINER_PATH}/target",
        source="dbt_target_vol",
        type="volume",
    ),
    Mount(
        target=f"{DBT_PROJECT_CONTAINER_PATH}/logs",
        source="dbt_logs_vol",
        type="volume",
    ),
]

BASE_DBT_FLAGS = (
    "--profiles-dir ${DBT_PROFILES_DIR} "
    "--project-dir ${DBT_PROJECT_DIR} "
    "--target ${DBT_TARGET}"
)


def _shell_prelude() -> str:
    """
    Shell helper untuk menjaga setiap dbt task konsisten.
    Full refresh dibuat opsional lewat dag_run.conf tanpa perlu mengubah DAG.
    """
    return r"""
set -euo pipefail

cd "${DBT_PROJECT_DIR}"

FULL_REFRESH_VALUE="$(echo "${DBT_FULL_REFRESH:-true}" | tr '[:upper:]' '[:lower:]')"
FULL_REFRESH_FLAG=""
if [ "${FULL_REFRESH_VALUE}" = "true" ] || [ "${FULL_REFRESH_VALUE}" = "1" ] || [ "${FULL_REFRESH_VALUE}" = "yes" ]; then
  FULL_REFRESH_FLAG="--full-refresh"
fi

DBT_COMMON_FLAGS="--profiles-dir ${DBT_PROFILES_DIR} --project-dir ${DBT_PROJECT_DIR} --target ${DBT_TARGET}"
DBT_THREADS_FLAG="--threads ${DBT_THREADS:-4}"
DBT_VARS_FLAG="--vars ${DBT_VARS:-'{}'}"

echo "============================================================"
echo "dbt project dir     : ${DBT_PROJECT_DIR}"
echo "dbt profiles dir    : ${DBT_PROFILES_DIR}"
echo "dbt target          : ${DBT_TARGET}"
echo "dbt threads         : ${DBT_THREADS:-4}"
echo "dbt full refresh    : ${FULL_REFRESH_VALUE}"
echo "Snowflake database  : ${SNOWFLAKE_DATABASE}"
echo "Snowflake schema    : ${SNOWFLAKE_SCHEMA}"
echo "Snowflake warehouse : ${SNOWFLAKE_WAREHOUSE}"
echo "============================================================"

test -f "${DBT_PROJECT_DIR}/dbt_project.yml"
test -f "${DBT_PROFILES_DIR}/profiles.yml"
""".strip()


def _dbt_task(task_id: str, command_body: str, retries: int = 2) -> DockerOperator:
    """
    Factory DockerOperator agar konfigurasi task dbt tidak duplikatif.
    """
    command = f"{_shell_prelude()}\n\n{command_body.strip()}"

    return DockerOperator(
        task_id=task_id,
        image=DBT_IMAGE,
        command=["bash", "-lc", command],
        environment=DBT_ENV,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        auto_remove="force",
        mount_tmp_dir=False,
        mounts=DBT_MOUNTS,
        working_dir=DBT_PROJECT_CONTAINER_PATH,
        tty=True,
        retries=retries,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(hours=3),
    )


@dag(
    dag_id=DAG_ID,
    description="Run dbt-snowflake transformations for OpenF1 Snowflake raw tables",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["openf1", "dbt", "snowflake", "analytics"],
    doc_md=__doc__,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=15),
    },
)
def openf1_dbt_snowflake() -> None:
    """
    DAG khusus dbt.

    Jalankan manual setelah raw tables terisi, atau trigger dari DAG utama setelah
    load_s3_to_snowflake sukses.
    """

    dbt_deps = _dbt_task(
        task_id="dbt_deps",
        command_body="""
dbt deps ${DBT_COMMON_FLAGS}
""",
    )

    dbt_debug = _dbt_task(
        task_id="dbt_debug",
        command_body="""
dbt debug ${DBT_COMMON_FLAGS}
""",
    )

    # Build staging lebih dulu agar source cleansing dan standardisasi tipe data selesai.
    dbt_build_staging = _dbt_task(
        task_id="dbt_build_staging",
        command_body="""
dbt build \
  --select path:models/staging \
  ${DBT_COMMON_FLAGS} \
  ${DBT_THREADS_FLAG} \
  ${DBT_VARS_FLAG} \
  ${FULL_REFRESH_FLAG}
""",
        retries=3,
    )

    # Build intermediate setelah staging tersedia.
    dbt_build_intermediate = _dbt_task(
        task_id="dbt_build_intermediate",
        command_body="""
dbt build \
  --select path:models/intermediate \
  ${DBT_COMMON_FLAGS} \
  ${DBT_THREADS_FLAG} \
  ${DBT_VARS_FLAG} \
  ${FULL_REFRESH_FLAG}
""",
        retries=3,
    )

    # Build marts terakhir karena layer ini dipakai Grafana Cloud.
    dbt_build_marts = _dbt_task(
        task_id="dbt_build_marts",
        command_body="""
dbt build \
  --select path:models/marts \
  ${DBT_COMMON_FLAGS} \
  ${DBT_THREADS_FLAG} \
  ${DBT_VARS_FLAG} \
  ${FULL_REFRESH_FLAG}
""",
        retries=3,
    )

    # Generate manifest/catalog untuk dokumentasi dan debugging lineage.
    # Task ini dibuat paling akhir agar tetap jalan hanya jika build sukses.
    dbt_docs_generate = _dbt_task(
        task_id="dbt_docs_generate",
        command_body="""
dbt docs generate \
  ${DBT_COMMON_FLAGS} \
  ${DBT_VARS_FLAG}
""",
        retries=1,
    )

    dbt_deps >> dbt_debug >> dbt_build_staging >> dbt_build_intermediate >> dbt_build_marts >> dbt_docs_generate


openf1_dbt_snowflake()
