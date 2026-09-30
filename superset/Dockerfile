# Build context is the repository root (see docker-compose.yml).
FROM apache/superset:6.1.0
USER root
# The SQLAlchemy driver for Hive and Spark SQL. Superset uses it to talk to
# the Spark Thrift Server (HiveServer2 protocol); pure-sasl avoids having
# to compile the C sasl library.
RUN uv pip install --no-cache --python /app/.venv/bin/python "pyhive[hive_pure_sasl]==0.7.0"
COPY superset/superset_config.py /app/pythonpath/superset_config.py
COPY superset/start.sh /app/docker/playground-start.sh
RUN chmod +x /app/docker/playground-start.sh
USER superset
CMD ["/app/docker/playground-start.sh"]
