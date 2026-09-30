"""Downloads the Postgres JDBC driver into PySpark's jars folder.

The Hive metastore client looks up the JDBC driver on Spark's own classpath;
a jar added with spark.jars.packages isn't visible to it. Used by the dbt and
notebook images while they are built.
"""

import os
import urllib.request

import pyspark

VERSION = "42.7.4"
URL = f"https://repo1.maven.org/maven2/org/postgresql/postgresql/{VERSION}/postgresql-{VERSION}.jar"
target = os.path.join(os.path.dirname(pyspark.__file__), "jars", f"postgresql-{VERSION}.jar")
urllib.request.urlretrieve(URL, target)
print(f"Downloaded {URL} to {target}")
