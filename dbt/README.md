# vehicle_telemetry dbt project

Decodes the binary protobuf files written by the protobuf consumer and turns
them into tables that are easy to analyse. The same project runs in two places:

- **Locally on Apache Spark**: dbt starts Spark inside its own process
  (dbt-spark's `session` method, i.e. PySpark). Tables are stored as Parquet
  files in `../data/spark/warehouse`, and the table catalog (the Hive
  metastore) in the `metastore-db` Postgres container, shared with the
  `spark-sql` shell and the Jupyter notebooks.
- **On Databricks**: deployed as a Databricks Asset Bundle (DAB), running on
  a SQL warehouse and reading the protobuf files from a Unity Catalog Volume.

Both are Spark SQL, so the models are the same SQL in both places.

## Sources and models

```
source raw.vehicle_telemetry_records ──> stg_vehicle_telemetry ──┬──> dim_vehicles           one row per vehicle
(view over the protobuf files;           (view: decoded,        ├──> fct_vehicle_daily      distance and health per vehicle and day
 generated schema tests)                  flattened, de-dup.)   ├──> fct_vehicle_events     one row per event (harsh_braking, …)
                                                                 └──> vehicle_latest_status  latest reading per vehicle
```

### Sources and the generated schema tests

The files written by the protobuf consumer are exposed as two views, created
by `on-run-start` hooks in `dbt_project.yml` (`macros/raw_protobuf_views.sql`):

| Source                           | Rows                                                                          |
|----------------------------------|-------------------------------------------------------------------------------|
| `raw.vehicle_telemetry_files`    | one per file: `file_path`, `content` (the bytes)                              |
| `raw.vehicle_telemetry_records`  | one per Kafka record: the `kafka.v1.KafkaRecord` fields; the message is still serialized in `value` |

They are declared, together with their tests, in
`src/models/sources/raw_vehicle_telemetry.yml`. **That file is generated** by
`schema/generate.py` from the `.proto` files; don't edit it. The tests check
that every message decodes, uses only enum values defined in the schema, and
follows the protovalidate rules in the `.proto` file (see
[Validating the messages](../README.md#validating-the-messages)). They use two
generic tests from `macros/protobuf_tests.sql`:

- `protobuf_decodes`: fails for bytes that aren't a valid message of the
  given type (`from_protobuf` in PERMISSIVE mode returns NULL for them).
- `protobuf_field_valid`: fails for messages where the generated SQL
  condition for a field isn't true. The failing rows show the Kafka
  coordinates and the invalid value.

Because the models depend on the source, `dbt build` runs these tests first
and skips the models when one fails. The staging model also skips messages
that don't decode, so `dbt run` (which doesn't run tests) still works.

The Docker image runs the generator while it's built and copies the result
into the project before every dbt command (`docker-entrypoint.sh`). Running
dbt on your machine, regenerate it yourself after a schema change:
`python schema/generate.py` (in the producer/consumers' virtual environment).

### Models

- `stg_vehicle_telemetry` removes duplicates. Both consumers deliver
  *at-least-once*, so a message can show up in more than one file.
  It also computes the distance driven since each vehicle's previous reading,
  and lower-cases the protobuf enum names (`HARSH_BRAKING` → `harsh_braking`).
- The messages are decoded by the `telemetry_source()` macro, with Spark's
  `from_protobuf`: the records view has already decoded each file into a
  `KafkaRecordBatch`, and the macro decodes each record's bytes into the
  message type in the `message_type` var. It
  decodes with the descriptor set that the consumer writes to
  `_schema/descriptor_set.desc`, which always describes the newest schema.
  Protobuf can decode older messages with a newer schema; fields that didn't
  exist yet get their default value.
- `from_protobuf` is part of the spark-protobuf module, which PySpark doesn't
  include. Spark downloads it from Maven Central on the first run (see
  `spark.jars.packages` in `profiles.yml`). Databricks has it built in.
- Reading the files is the only part that differs between local Spark and
  Databricks, so that macro (`raw_protobuf_files()`) is *dispatched*: dbt
  picks the Spark or the Databricks implementation depending on the adapter.
- The staging model lists the columns it uses. When you add a field to the
  `.proto` file, it is available in `telemetry_source()` right away; add it
  to the staging model to use it downstream.
- Model tests (`dbt build` runs them) check uniqueness, not-null columns,
  accepted values and relationships. See the `_*.yml` files next to the models.

## Running locally with Spark

The easiest way is through Docker Compose, from the repository root:

```bash
docker compose run --rm dbt build                               # build models + run tests
docker compose run --rm dbt show --select fct_vehicle_daily     # preview a model
docker compose run --rm dbt show --inline "select * from {{ ref('fct_vehicle_events') }} where event_type = 'engine_overheating'"
```

The protobuf consumer writes a file every 60 seconds, so give it a minute
after starting the stack before the first build. Run `build` again to pick up
new data. Starting Spark takes a few seconds, so a build takes about half a
minute.

To explore the tables, open JupyterLab at http://localhost:8888 (see
`notebooks/explore_vehicle_telemetry.ipynb`), or a Spark SQL shell:

```bash
docker compose run --rm spark-sql
spark-sql (default)> show tables in vehicle_telemetry;
spark-sql (default)> select * from vehicle_telemetry.vehicle_latest_status;
```

Both can run while dbt is running. Every Spark process (dbt, the shell, each
notebook) keeps its table catalog in the `metastore-db` Postgres container:

- `spark/spark-defaults.conf` holds the shared Spark settings. The images
  copy it to `/opt/spark-conf` and point `SPARK_CONF_DIR` at it.
- `metastore/init/*.sql` is Hive's metastore schema for Postgres, created
  when the container first starts. (Letting Spark create it lazily deadlocks.)
- The Hive metastore client looks for the Postgres JDBC driver on Spark's
  own classpath, so the images download it into PySpark's `jars` folder
  (`spark/install-postgres-driver.py`).

By default Spark would keep the catalog in an embedded Derby database, which
only one process can open at a time.

Or run dbt on your machine, from this folder. That needs Java 17 or 21, and a
separate virtual environment from the producer and consumers, because dbt
and the protobuf tools need different versions of the protobuf library.
Without `SPARK_CONF_DIR`, Spark uses its defaults there: tables in
`./spark-warehouse` and the Derby catalog in `./metastore_db`, so one Spark
process at a time.

```bash
python -m venv .venv-dbt
source .venv-dbt/bin/activate
pip install -r requirements.txt
dbt build
```

`profiles.yml` in this folder configures the local Spark session.

### Troubleshooting

**The notebook says `SCHEMA_NOT_FOUND` for `vehicle_telemetry`, but dbt and
`spark-sql` work.** dbt and `spark-sql` are probably still running an image
from before the shared catalog, which keeps its own Derby catalog in
`./data/spark/metastore_db`: the tables exist there, but not in the shared
catalog the notebook reads. `docker compose up --build` doesn't rebuild them,
because they're in the `dbt` profile. Their services now set
`pull_policy: build`, so `docker compose run` rebuilds the image each time
(quick, thanks to the build cache). If you still have an old image, rebuild
it once with `docker compose build dbt`, then run `docker compose run --rm dbt build`.

**`LOCATION_ALREADY_EXISTS` when dbt creates a table.** The table's folder
exists in `./data/spark/warehouse` but the catalog doesn't know the table,
e.g. data from before the shared catalog, or `./data/metastore-db` deleted
on its own. The tables are rebuilt from the protobuf archive, so it's safe
to delete the folders: `rm -rf ./data/spark/warehouse ./data/spark/metastore_db`.
Then run `docker compose run --rm dbt build` again.

## Deploying to Databricks as an Asset Bundle

Files involved:

| File                                     | What it does                                                                  |
|------------------------------------------|-------------------------------------------------------------------------------|
| `databricks.yml`                         | The bundle: variables (catalog, schema, warehouse) and `dev`/`prod` targets   |
| `resources/vehicle_telemetry.schema.yml` | Creates the Unity Catalog schema                                              |
| `resources/raw.volume.yml`               | Creates the `raw` volume that holds the protobuf files                        |
| `resources/vehicle_telemetry.job.yml`    | A job with a dbt task that runs `dbt build` on serverless compute every hour  |
| `dbt_profiles/profiles.yml`              | The dbt profile the job uses; Databricks provides the host and token          |

Requirements: the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html),
a workspace with Unity Catalog, and a SQL warehouse.

1. **Log in** (once): `databricks auth login --host https://<your-workspace>.cloud.databricks.com`

2. **Configure the bundle** in `databricks.yml` if the defaults don't fit:
   - `catalog` (default `main`): you need permission to create schemas in it.
   - `warehouse_id`: looked up from the warehouse named *Serverless Starter
     Warehouse*. Change the name, or pass the id with
     `--var warehouse_id=<id>`.

3. **Deploy** the schema, volume and job. In the `dev` target, the schema is
   called `dev_<your_user_name>_vehicle_telemetry`, and the job is prefixed
   with `[dev <your_user_name>]` and has its schedule paused:
   ```bash
   cd dbt
   databricks bundle validate
   databricks bundle deploy
   ```

4. **Upload the protobuf files** (including the `_schema` folder) from the
   playground to the volume:
   ```bash
   databricks fs cp -r --overwrite ../data/protobuf/vehicle_telemetry \
     dbfs:/Volumes/main/dev_<your_user_name>_vehicle_telemetry/raw/vehicle_telemetry
   ```

5. **Run the job** and open the link it prints:
   ```bash
   databricks bundle run vehicle_telemetry_dbt_job
   ```
   The models end up as tables and views in the same schema.

Deploy to production with `databricks bundle deploy -t prod`. That deploys
the schema as plain `vehicle_telemetry`, with the hourly schedule active.
Remove everything the bundle created with `databricks bundle destroy`.

> The upload in step 4 is a one-off copy. To keep Databricks up to date,
> run it again (only new files are needed). You could also stream from Kafka
> into Databricks directly, but that requires Kafka to be reachable from
> Databricks, which a local playground usually isn't.
