FROM apache/superset:latest

USER root

RUN pip install --target /app/.venv/lib/python3.10/site-packages --no-cache-dir clickhouse-connect clickhouse-driver clickhouse-sqlalchemy

USER superset

