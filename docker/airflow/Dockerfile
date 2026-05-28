FROM apache/airflow:2.8.1-python3.10

USER root

# Install build dependencies if needed, then clean up
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python packages required for the batch processing DAG
RUN pip install --no-cache-dir \
    pandas \
    pyarrow \
    minio \
    clickhouse-connect \
    kafka-python-ng
