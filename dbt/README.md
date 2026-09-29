# vehicle_telemetry dbt project

Turns the Parquet files written by the parquet consumer into tables that are
easy to analyse. The same project runs in two places:

- **Locally on DuckDB**: an in-process database stored in
  `../data/vehicle_telemetry.duckdb`. Nothing to install besides dbt.
- **On Databricks**: deployed as a Databricks Asset Bundle (DAB), running on
  a SQL warehouse and reading the Parquet files from a Unity Catalog Volume.

## Models

```
Parquet files ──> stg_vehicle_telemetry ──┬──> dim_vehicles           one row per vehicle
(event_date=…/     (view: flattened,      ├──> fct_vehicle_daily      distance and health per vehicle and day
 part-….parquet)    de-duplicated)        ├──> fct_vehicle_events     one row per event (harsh_braking, …)
                                          └──> vehicle_latest_status  latest reading per vehicle
```

- `stg_vehicle_telemetry` removes duplicates. Both consumers deliver
  *at-least-once*, so a message can show up in more than one Parquet file.
  It also computes the distance driven since each vehicle's previous reading.
- The raw files are read through the `telemetry_source()` macro, and arrays
  are unnested with `explode()`. Both macros are *dispatched*: dbt picks the
  DuckDB or the Databricks implementation depending on the adapter. The rest
  of the SQL is the same on both engines.
- Tests (`dbt build` runs them) check uniqueness, not-null columns, accepted
  values and relationships. See the `_*.yml` files next to the models.

## Running locally with DuckDB

The easiest way is through Docker Compose, from the repository root:

```bash
docker compose run --rm dbt build                               # build models + run tests
docker compose run --rm dbt show --select fct_vehicle_daily     # preview a model
docker compose run --rm dbt show --inline "select * from {{ ref('fct_vehicle_events') }} where event_type = 'engine_overheating'"
```

The parquet consumer writes a file every 60 seconds, so give it a minute
after starting the stack before the first build. Run `build` again to pick up
new data.

Or run dbt on your machine, from this folder:

```bash
pip install -r requirements.txt
dbt build
```

`profiles.yml` in this folder points dbt at DuckDB. To explore the results
with SQL, open the database with the [DuckDB CLI](https://duckdb.org/docs/installation/)
(`duckdb ../data/vehicle_telemetry.duckdb`). Stop any running dbt command
first, because only one process can write to the file at a time.

## Deploying to Databricks as an Asset Bundle

Files involved:

| File                                     | What it does                                                                  |
|------------------------------------------|-------------------------------------------------------------------------------|
| `databricks.yml`                         | The bundle: variables (catalog, schema, warehouse) and `dev`/`prod` targets   |
| `resources/vehicle_telemetry.schema.yml` | Creates the Unity Catalog schema                                              |
| `resources/raw.volume.yml`               | Creates the `raw` volume that holds the Parquet files                         |
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

4. **Upload the Parquet files** from the playground to the volume:
   ```bash
   databricks fs cp -r --overwrite ../data/parquet/vehicle_telemetry \
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
