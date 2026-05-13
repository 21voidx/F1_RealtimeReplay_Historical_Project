FROM python:3.10-slim

RUN apt-get update && apt-get install -y git bash && \
    rm -rf /var/lib/apt/lists/*

COPY docker/requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app

CMD ["dbt", "--version"]

# tree -d -L 2 dbt/

# docker build
#     ↓
# /dbt_packages/          ← packages di-install di sini (di dalam image)
#     ├── dbt_utils/
#     ├── audit_helper/
#     ├── dbt_expectations/
#     └── dbt_project_evaluator/

# Mount volume /app       ← hanya overwrite project files
#     ├── models/
#     ├── macros/
#     └── dbt_project.yml ← packages-install-path: /dbt_packages

# dbt run                 ← baca packages dari /dbt_packages 