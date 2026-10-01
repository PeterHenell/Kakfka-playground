"""Downloads the extra jars Spark needs into PySpark's jars folder.

- The Postgres JDBC driver, for the table catalog in the metastore-db
  Postgres. The Hive metastore client looks it up on Spark's own classpath.
- spark-protobuf, for from_protobuf. The Spark Thrift Server runs each query
  with its own class loader, which doesn't see jars added with
  spark.jars.packages, so the jar has to be on Spark's own classpath too.
- Delta Lake, the table format Databricks uses. dbt needs it for
  incremental models that merge, and for snapshots.

Used by the dbt image (also used by spark-sql and the Spark Thrift Server)
while it is built.
"""

import os
import urllib.request

import pyspark

MAVEN = "https://repo1.maven.org/maven2"
DELTA_VERSION = "4.4.0"
JARS = [
    f"{MAVEN}/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar",
    # Must match the PySpark version.
    f"{MAVEN}/org/apache/spark/spark-protobuf_2.13/{pyspark.__version__}/spark-protobuf_2.13-{pyspark.__version__}.jar",
    # Each Delta release is built for one Spark version: 4.4.x is for Spark 4.2.
    f"{MAVEN}/io/delta/delta-spark_2.13/{DELTA_VERSION}/delta-spark_2.13-{DELTA_VERSION}.jar",
    f"{MAVEN}/io/delta/delta-storage/{DELTA_VERSION}/delta-storage-{DELTA_VERSION}.jar",
]

jars_dir = os.path.join(os.path.dirname(pyspark.__file__), "jars")
for url in JARS:
    target = os.path.join(jars_dir, url.rsplit("/", 1)[1])
    urllib.request.urlretrieve(url, target)
    print(f"Downloaded {url} to {target}")
