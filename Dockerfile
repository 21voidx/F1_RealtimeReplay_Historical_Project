FROM apache/airflow:3.0.6-python3.12

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /requirements.txt



# FROM apache/airflow:3.0.6-python3.12

# COPY requirements.txt /requirements.txt

# # Install Airflow-side dependencies as the default airflow user.
# # Keep apache-airflow pinned so dependency resolution does not upgrade/downgrade Airflow.
# RUN pip install --no-cache-dir \
#     "apache-airflow==${AIRFLOW_VERSION}" \
#     -r /requirements.txt

# # The Airflow production image runs as the airflow user by default.
# # /opt is owned by root, so create and grant the dbt venv directory as root first.
# USER root
# RUN mkdir -p /opt/dbt_venv && \
#     chown -R airflow:0 /opt/dbt_venv && \
#     chmod -R g+rwX /opt/dbt_venv

# USER airflow

# # Keep dbt outside /opt/airflow so airflow-init does not recursively chown a large venv.
# RUN python -m venv /opt/dbt_venv && \
#     /opt/dbt_venv/bin/pip install --no-cache-dir \
#     dbt-snowflake==1.11.4 && \
#     /opt/dbt_venv/bin/dbt --version