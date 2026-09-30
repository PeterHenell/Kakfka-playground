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


# PyHive's SQLAlchemy dialect was written for Hive, where `SHOW TABLES` returns
# one column, the table name. Spark returns three (namespace, tableName,
# isTemporary), and PyHive takes the first, so every table would be listed
# under the schema's name. It also lists every table again as a view.
# Superset's Hive engine spec has the same problem with `SHOW VIEWS`. Read
# Spark's columns instead, so SQL Lab's table browser works.
from pyhive.sqlalchemy_hive import HiveDialect  # noqa: E402
from sqlalchemy import text  # noqa: E402


def _spark_show(dialect, connection, command, schema):
    """Names from Spark's SHOW TABLES / SHOW VIEWS, without temporary views."""
    query = command
    if schema:
        query += " IN " + dialect.identifier_preparer.quote_identifier(schema)
    rows = connection.execute(text(query)).fetchall()
    if rows and len(rows[0]) == 1:  # Hive: just the name
        return [row[0] for row in rows]
    return [row[1] for row in rows if not row[2]]


def _get_table_names(self, connection, schema=None, **kw):
    # Spark's SHOW TABLES includes views; list those as views only.
    views = set(_get_view_names(self, connection, schema))
    return [name for name in _spark_show(self, connection, "SHOW TABLES", schema) if name not in views]


def _get_view_names(self, connection, schema=None, **kw):
    try:
        return _spark_show(self, connection, "SHOW VIEWS", schema)
    except Exception:  # Hive before 2.2 has no SHOW VIEWS
        return []


HiveDialect.get_table_names = _get_table_names
HiveDialect.get_view_names = _get_view_names


def FLASK_APP_MUTATOR(app):  # noqa: N802 (Superset's setting name)
    # Imported here, once Superset is initialised: importing it while the
    # config loads would be circular. SparkEngineSpec inherits this method.
    from superset.db_engine_specs.hive import HiveEngineSpec

    HiveEngineSpec.get_view_names = classmethod(
        lambda cls, database, inspector, schema: set(inspector.get_view_names(schema))
    )
