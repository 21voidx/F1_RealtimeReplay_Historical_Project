FROM apache/airflow:3.0.6-python3.12

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /requirements.txt

# Keep dbt outside /opt/airflow so airflow-init does not recursively chown a large venv.
RUN python -m venv /opt/dbt_venv && \
    /opt/dbt_venv/bin/pip install --no-cache-dir \
    dbt-snowflake==1.11.4 && \
    /opt/dbt_venv/bin/dbt --version
