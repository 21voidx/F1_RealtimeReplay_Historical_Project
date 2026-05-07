FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1         PYTHONUNBUFFERED=1         DBT_PROFILES_DIR=/app/hitesh_dbt_project

RUN apt-get update         && apt-get install -y --no-install-recommends git build-essential ca-certificates         && apt-get clean         && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app/hitesh_dbt_project
ENTRYPOINT ["bash", "-lc"]
