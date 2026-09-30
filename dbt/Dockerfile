# Build context is the repository root (see docker-compose.yml), so the image
# can include the shared schema/ folder.
FROM python:3.12-slim
# Spark runs on the JVM.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
COPY dbt/requirements.txt /tmp/dbt-requirements.txt
RUN pip install --no-cache-dir -r /tmp/dbt-requirements.txt
# Shared Spark settings (catalog in the metastore-db Postgres) and the
# Postgres JDBC driver the catalog needs.
COPY spark/ /opt/spark-setup/
RUN python /opt/spark-setup/install-postgres-driver.py \
    && mkdir -p /opt/spark-conf && cp /opt/spark-setup/spark-defaults.conf /opt/spark-conf/
ENV SPARK_CONF_DIR=/opt/spark-conf
# The schema tools need a newer protobuf library than dbt, so they get their
# own virtual environment.
COPY schema/requirements.txt /opt/schema/requirements.txt
RUN python -m venv /opt/schema-venv \
    && /opt/schema-venv/bin/pip install --no-cache-dir -r /opt/schema/requirements.txt
# Generate the dbt source tests from the .proto files. The entrypoint copies
# them into the mounted project before running dbt, so rebuilding the image
# (docker compose build dbt) picks up schema changes.
COPY schema/ /opt/schema/
RUN DBT_SOURCES_DIR=/opt/generated-dbt-sources /opt/schema-venv/bin/python /opt/schema/generate.py
COPY dbt/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
WORKDIR /dbt
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["build"]
