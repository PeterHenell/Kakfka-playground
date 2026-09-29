FROM python:3.12-slim
# Spark runs on the JVM.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
WORKDIR /dbt
ENTRYPOINT ["dbt"]
CMD ["build"]
