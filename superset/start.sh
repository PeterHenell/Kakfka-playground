#!/bin/sh
# Prepares Superset on every start (all steps are idempotent), then starts
# the web server.
set -e

# Create or upgrade Superset's own metadata database.
superset db upgrade

# The login for the playground: admin / admin.
superset fab create-admin --username admin --password admin \
  --firstname Playground --lastname Admin --email admin@localhost || true

# Default roles and permissions.
superset init

# The connection to Spark, through the Spark Thrift Server. It opens in the
# vehicle_telemetry schema, where dbt builds its models.
superset set-database-uri \
  --database_name "Spark (vehicle telemetry)" \
  --uri "hive://hive@spark-thrift-server:10000/vehicle_telemetry"

exec /app/docker/entrypoints/run-server.sh
