# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

A Kafka learning playground: a Python producer simulates vehicles and publishes protobuf telemetry to Kafka; two consumer groups read the same topic (`vehicle-telemetry`, 3 partitions). `consumer` (group `elk-writer`) posts JSON to Logstash → Elasticsearch → Kibana; `protobuf-consumer` (group `protobuf-writer`) archives the raw records as binary protobuf files in `./data/protobuf`, which a dbt project decodes with Spark. Superset queries the dbt tables through a Spark Thrift Server. Everything runs in one Docker Compose project. README.md is the user-facing guide; dbt/README.md covers dbt, the shared catalog and the Databricks bundle.

## Commands

```bash
make up                      # docker compose up -d --build (Kafka, ELK, consumers, control panel, Superset)
make dbt                     # docker compose run --rm dbt build
make dbt ARGS="test --select source:raw"   # any dbt command, e.g. only the generated source tests
make spark                   # spark-sql shell on the shared catalog
make invalid                 # publish invalid messages so the dbt source tests fail
make clean                   # down -v and delete ./data (all Kafka/ES/Superset/Spark state)
make                         # list all targets
```

Unit tests (schema converters and the dbt test generator) run on the host and need the generated code first:

```bash
pip install -r requirements.txt pytest
python schema/generate.py                       # writes schema/generated/ (gitignored)
python -m pytest schema/tests
python -m pytest schema/tests/test_dbt_tests.py::test_every_message_is_checked_to_decode
```

There is no linter config. UIs: control panel :8090 (links to everything), Superset :8088 (admin/admin), Kibana :5601, Kafka UI :8080, Spark UI :4040; Kafka from the host on `localhost:9094`, Thrift JDBC on `localhost:10000`.

## Architecture: the schema drives everything

`schema/proto/` is the single source of truth (`vehicle/v1/vehicle_telemetry.proto`; `kafka/v1/kafka_record.proto` is the archive file format; `elk/options.proto` defines a custom `(elk.field_type)` option; `buf/validate/validate.proto` is vendored protovalidate). `schema/generate.py` runs `protoc` via grpcio-tools and produces, in `schema/generated/`, the `*_pb2.py` modules, `descriptors/descriptor_set.desc` and the Elasticsearch index template, plus the committed dbt source tests in `dbt/src/models/sources/raw_vehicle_telemetry.yml`.

- No hand-written per-field code anywhere. Runtime conversions walk protobuf descriptors: `schema/telemetry_schema/registry.py` loads all generated modules and finds message classes by full name (taken from the Kafka `message-type` header); `elk.py` maps proto types to ES types; `dbt_tests.py` translates protovalidate rules and enum checks into SQL source tests (rules it can't translate go to `meta.unchecked_rules`).
- Every Python image runs `schema/generate.py` at build time, so after a `.proto` change rebuild (`make up`). The dbt image generates the source tests at build time and its entrypoint copies them into the mounted project. Re-run `python schema/generate.py` and commit the regenerated YAML so schema changes show in the test diff.
- Schema evolution rules: only add fields with new numbers, never change numbers/types, `reserve` removed numbers. New fields must also be added to `dbt/src/models/staging/stg_vehicle_telemetry.sql`.

## Spark / dbt / catalog

- The protobuf consumer never decodes messages. Each file is one `kafka.v1.KafkaRecordBatch`; it also writes `_schema/descriptor_set.desc` next to the data on every start. Spark decodes in two `from_protobuf` steps (batch, then each record's bytes, with `emit.default.values`); see `dbt/src/macros/telemetry_source.sql` and `raw_protobuf_views.sql`.
- Delivery is at-least-once (offsets committed after Logstash accepts / after the file is written). ES dedupes by using `message_id` as document id; dbt dedupes on `message_id` in the staging model.
- All Spark processes (dbt in-process session mode, `spark-sql`, `spark-thrift-server`) share one Hive metastore in the `metastore-db` Postgres, configured in `spark/spark-defaults.conf` (copied into the image as `SPARK_CONF_DIR`). That is what lets them run concurrently. The metastore schema is pre-created by `metastore/init/*.sql`; `datanucleus.schema.autoCreateAll` must stay false (lazy creation deadlocks).
- The Postgres JDBC driver and spark-protobuf are installed into PySpark's own `jars` folder (`spark/install-jars.py`), not via `spark.jars.packages`: the metastore client and the Thrift Server's per-query class loader can't see packages-loaded jars. Keep the spark-protobuf version equal to the PySpark version.
- `dbt`, `spark-sql` and `spark-thrift-server` all use the `kafka-playground-dbt` image built from `dbt/Dockerfile`; `dbt` and `spark-sql` are in the `dbt` Compose profile with `pull_policy: build` (because `up --build` skips profiled services).
- `dbt/profiles.yml` is for local/Docker Spark; `dbt/dbt_profiles/profiles.yml` + `dbt/databricks.yml` + `dbt/resources/` are the Databricks Asset Bundle, which runs the same models on a SQL warehouse against a Unity Catalog Volume.

## Superset

`superset/` builds on `apache/superset` with PyHive; `start.sh` creates admin/admin and the "Spark (vehicle telemetry)" connection (`hive://hive@spark-thrift-server:10000/vehicle_telemetry`). `superset_config.py` monkeypatches PyHive's `HiveDialect.get_table_names`/`get_view_names` and Superset's `HiveEngineSpec.get_view_names` (in `FLASK_APP_MUTATOR`), because Spark's `SHOW TABLES`/`SHOW VIEWS` return `(namespace, name, isTemporary)` rather than Hive's single name column. Without this the SQL Lab table browser breaks.

## Control panel and pause/resume

`control-panel/app.py` (Flask + one static `index.html`) computes produced/consumed/lag from Kafka offsets via the admin API and samples history in a background thread. Pausing writes desired state to the compacted `playground-control` topic; `common/kafka_control.py` (`ControlState`, `ConsumerPauser`) is how the producer and consumers follow it. Consumers use `consumer.pause()` so they stay in their group without rebalancing. The `TOOLS` list in `app.py` drives the dashboard's tool links and health checks (`TOOLS_CHECK_HOST=localhost` when running outside Docker).

## Conventions

- All service configuration is via environment variables, documented in each script's module docstring.
- Docs and code comments are written in plain, short sentences aimed at someone learning Kafka; keep README.md / dbt/README.md in sync with behavior changes.
