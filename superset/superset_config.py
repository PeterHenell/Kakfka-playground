"""Superset settings for the Kafka playground.

Superset's own metadata (users, saved queries, charts, dashboards) is kept
in a SQLite database in the superset-home volume; `make clean` deletes it.
"""

import os

# Signs session cookies. Fine for a local playground; never use a fixed key
# in production.
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "kafka-playground-local-only")

SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

# Spark queries (especially the first one, while Spark warms up) can take
# longer than Superset's defaults.
SQLLAB_TIMEOUT = 300
SUPERSET_WEBSERVER_TIMEOUT = 300
