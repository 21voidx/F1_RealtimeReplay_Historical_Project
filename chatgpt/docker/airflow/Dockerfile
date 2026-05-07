FROM apache/airflow:3.0.6

USER root
RUN apt-get update         && apt-get install -y --no-install-recommends curl         && apt-get clean         && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir         apache-airflow-providers-docker==4.5.5         python-dotenv==1.0.1
