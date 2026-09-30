# Build context is the repository root (see docker-compose.yml), so the image
# can include the shared spark/ folder.
FROM python:3.12-slim
# Spark runs on the JVM.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
COPY notebook/requirements.txt /tmp/notebook-requirements.txt
RUN pip install --no-cache-dir -r /tmp/notebook-requirements.txt
# The same Spark settings as dbt: the shared table catalog in metastore-db.
COPY spark/ /opt/spark-setup/
RUN python /opt/spark-setup/install-postgres-driver.py \
    && mkdir -p /opt/spark-conf && cp /opt/spark-setup/spark-defaults.conf /opt/spark-conf/
ENV SPARK_CONF_DIR=/opt/spark-conf
WORKDIR /notebooks
EXPOSE 8888 4040
# No token or password: the port is only published on 127.0.0.1 (see
# docker-compose.yml), so only you can reach it.
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", \
     "--IdentityProvider.token=", "--ServerApp.password=", "--ServerApp.root_dir=/notebooks"]
