# Orchestration. Tag: ecommerce/airflow:3.3.2-r1.
# Tasks start the Spark and Python images through the Docker socket
# (DockerOperator); this image only schedules, and parses the DAGs.
FROM apache/airflow:3.3.2-python3.12

ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.12
RUN pip install --no-cache-dir \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
      apache-airflow-providers-docker

# The package without its dependencies (Airflow's own pins must not move), so a
# DAG file can import ecomdata.settings and ecomdata.domain, which need only the
# standard library and python-dotenv. The DAGs are mounted from
# src/ecomdata/orchestration/dags.
# The airflow image refuses pip as root, so the build directory belongs to the
# airflow user and nothing here switches user. python-dotenv already ships with
# Airflow; the constraints file pins an older one, so it is not reinstalled.
COPY --chown=airflow:root pyproject.toml constraints.txt README.md /build/
COPY --chown=airflow:root src /build/src
RUN pip install --no-cache-dir --no-deps /build && rm -rf /build/*
