# Learning dbt with the vehicle telemetry project

A one-hour introduction to dbt for data engineers who know Databricks.
Every concept comes with an example from this project, and each one builds
on the ones before it. The links go to the real files; open them as you go.

The data: simulated vehicles send telemetry (position, speed, engine
temperature, tire pressure, events like `harsh_braking`) through Kafka. A
consumer archives the raw messages as binary protobuf files. dbt turns those
files into tables you can query.

## Session plan

| #  | Topic                                                   | Minutes |
|----|---------------------------------------------------------|---------|
| 1  | [What dbt is](#1-what-dbt-is)                           | 4       |
| 2  | [The project and the profile](#2-the-project-and-the-profile) | 3 |
| 3  | [Models](#3-models)                                     | 4       |
| 4  | [`ref()` and the DAG](#4-ref-and-the-dag)               | 4       |
| 5  | [Materializations](#5-materializations)                 | 3       |
| 6  | [Sources](#6-sources)                                   | 3       |
| 7  | [Layers: staging and marts](#7-layers-staging-and-marts) | 3      |
| 8  | [Jinja and compiled SQL](#8-jinja-and-compiled-sql)     | 4       |
| 9  | [Macros](#9-macros)                                     | 4       |
| 10 | [Variables and hooks](#10-variables-and-hooks)          | 3       |
| 11 | [Tests](#11-tests)                                      | 6       |
| 12 | [Incremental models](#12-incremental-models)            | 6       |
| 13 | [Snapshots](#13-snapshots)                              | 4       |
| 14 | [Documentation](#14-documentation)                      | 2       |
| 15 | [Running on Databricks](#15-running-on-databricks)      | 3       |
| 16 | [What we didn't cover](#16-what-we-didnt-cover)         | 2       |

That's 58 minutes, plus a little time for questions.

## Before the session

Start the playground and let it collect data for a few minutes (see the
[main README](../README.md)). Then build everything once, so the demos are fast:

```bash
make up          # start Kafka, the producer, the consumers, Spark, Superset
make dbt         # dbt build: about 20 seconds
```

Then pause the producer in the control panel (http://localhost:8090), so the
data stays the same during the session. Sections 12 and 13 resume it for a
minute to show what happens when new data arrives.

All demo commands run dbt inside Docker/Podman: `make dbt ARGS="<dbt command>"`
is the same as running `dbt <dbt command>` in this folder.

---

## 1. What dbt is

dbt is the **T** in ELT. It doesn't load or move data. You write `select`
statements, and dbt turns them into tables and views in your warehouse, in the
right order. The SQL runs on the warehouse (Databricks, or Spark here); dbt
only generates it and sends it there.

What you get on top of plain SQL: dependencies between tables, tests,
documentation, and the same code running in dev and prod.

| On Databricks you might use…                 | In dbt                          | Section |
|----------------------------------------------|---------------------------------|---------|
| `CREATE OR REPLACE TABLE … AS SELECT` in a notebook | A model (a `.sql` file)  | 3       |
| Ordering job tasks by hand                   | `ref()` and the DAG             | 4       |
| Widgets / job parameters                     | Variables                       | 10      |
| Expectations, ad-hoc check queries           | Tests                           | 11      |
| `MERGE INTO` only the new rows, with a watermark | Incremental models          | 12      |
| An SCD type 2 `MERGE` written by hand        | Snapshots                       | 13      |
| Table and column comments in Unity Catalog   | Descriptions in YAML            | 14      |

Lakeflow Declarative Pipelines (DLT) is the closest Databricks feature: it also
lets you declare tables as queries and works out the order for you.

```
raw files ──> source ──> stg_vehicle_telemetry ──┬──> dim_vehicles ──> dim_vehicles_snapshot
                                                 ├──> fct_vehicle_daily
                                                 ├──> fct_vehicle_events (incremental)
                                                 └──> vehicle_latest_status
```

## 2. The project and the profile

A dbt **project** is a folder with a [dbt_project.yml](dbt_project.yml). It
names the project, says where the models are (`src/models` here) and sets
defaults for them.

The **profile** says *where* the SQL runs: which warehouse, which catalog and
schema, which credentials. It lives in `profiles.yml`, outside the project
code, because it differs per person and per environment. A profile has one or
more **targets** (e.g. `dev` and `prod`); you pick one with `--target`.

- [profiles.yml](profiles.yml): the local target, running on Apache Spark
  inside the dbt process.
- On Databricks the target is a SQL warehouse. The same models run on both
  (section 15).

**Demo:** show `profile: vehicle_telemetry` in `dbt_project.yml` and the
matching name in `profiles.yml`.

## 3. Models

A **model** is one `.sql` file with one `select` statement. The file name is
the table name. You never write `CREATE TABLE`: dbt wraps your `select` in the
right DDL.

Example: [dim_vehicles.sql](src/models/marts/dim_vehicles.sql), one row per
vehicle:

```sql
select
    vehicle_id,
    max_by(vin, recorded_at) as vin,
    min(recorded_at) as first_seen_at,
    ...
from {{ ref('stg_vehicle_telemetry') }}
group by vehicle_id
```

**Demo:**

```bash
make dbt ARGS="run --select dim_vehicles"      # build one model
make dbt ARGS="show --select dim_vehicles"     # preview its rows
```

## 4. `ref()` and the DAG

`{{ ref('stg_vehicle_telemetry') }}` in the model above is how a model reads
another model. It does two things:

1. dbt replaces it with the real table name, e.g.
   `vehicle_telemetry.stg_vehicle_telemetry`. The schema comes from the
   target, so the same code writes to your dev schema in dev and to the prod
   schema in prod.
2. dbt learns that `dim_vehicles` depends on `stg_vehicle_telemetry`. From
   all the `ref()`s it builds a graph of dependencies (the **DAG**) and runs
   the models in that order.

Never write a table name by hand; always use `ref()`.

The DAG also lets you select parts of the project:

```bash
make dbt ARGS="ls --select +fct_vehicle_daily --resource-type model"  # the model and everything upstream
make dbt ARGS="run --select stg_vehicle_telemetry+"                   # the model and everything downstream
```

## 5. Materializations

A **materialization** decides what dbt creates from a model:

| Materialization | What it creates                         | Used here for |
|-----------------|-----------------------------------------|---------------|
| `view`          | A view; the query runs when you read it | staging       |
| `table`         | A table, rebuilt completely every run   | marts         |
| `incremental`   | A table that only processes new rows    | `fct_vehicle_events` (section 12) |
| `ephemeral`     | Nothing; inlined as a CTE where it's used | (not used)  |

Set it for a whole folder in [dbt_project.yml](dbt_project.yml):

```yaml
models:
  vehicle_telemetry:
    staging:
      +materialized: view
    marts:
      +materialized: table
```

or for one model, at the top of its file: `{{ config(materialized='table') }}`.

`+file_format: delta` in the same file makes the tables Delta tables, as on
Databricks. Locally that's needed for the incremental model and the snapshot.

## 6. Sources

Models read other models with `ref()`. Data that dbt didn't create (loaded by
another tool) is a **source**. You declare it in YAML and read it with
`source()`:

```sql
from {{ source('raw', 'vehicle_telemetry_records') }}
```

- The declaration: [raw_vehicle_telemetry.yml](src/models/sources/raw_vehicle_telemetry.yml).
  Here it's generated from the protobuf schema, and it's mostly tests (section 11).
- Used in: [telemetry_source.sql](src/macros/telemetry_source.sql).

Sources show up as the starting points of the DAG, so you can see which
tables depend on which raw data. Sources can also have a **freshness** check
(how old is the newest row?).

## 7. Layers: staging and marts

A common convention: sort the models into layers.

- **Staging** (`stg_`): one model per source. Clean up the raw data: rename,
  cast, flatten, remove duplicates. No business logic.
  [stg_vehicle_telemetry.sql](src/models/staging/stg_vehicle_telemetry.sql)
  flattens the nested protobuf fields, lower-cases the enums, and keeps one
  row per `message_id` (Kafka delivers at-least-once, so there are duplicates).
- **Marts**: what people query. Facts (`fct_`, events and measurements) and
  dimensions (`dim_`, things). They only read staging models.
  - [dim_vehicles.sql](src/models/marts/dim_vehicles.sql): one row per vehicle
  - [fct_vehicle_daily.sql](src/models/marts/fct_vehicle_daily.sql): distance and health per vehicle and day
  - [fct_vehicle_events.sql](src/models/marts/fct_vehicle_events.sql): one row per event
  - [vehicle_latest_status.sql](src/models/marts/vehicle_latest_status.sql): the latest reading per vehicle

Also look at the style of the staging model: one CTE per step
(`source` → `deduplicated` → `with_previous` → final `select`). This is the
usual dbt style, and it makes long models readable.

## 8. Jinja and compiled SQL

Everything in `{{ … }}` and `{% … %}` is **Jinja**, a template language. dbt
first renders the template into plain SQL (**compiles** it), then runs that SQL.
`ref()` and `source()` are Jinja functions too.

[fct_vehicle_events.sql](src/models/marts/fct_vehicle_events.sql) has:

```sql
select
    {{ dbt.concat(['message_id', "'-'", 'event_type']) }} as event_id,
    *
from exploded
```

**Demo:** compile it and look at the result:

```bash
make dbt ARGS="compile --select fct_vehicle_events"
```

```sql
select
    concat(message_id, '-', event_type) as event_id,
    *
from exploded
```

and `ref('stg_vehicle_telemetry')` became `vehicle_telemetry.stg_vehicle_telemetry`.
The compiled SQL is also saved in `target/compiled/`. When a model fails,
look there first: it's the SQL that actually ran.

## 9. Macros

A **macro** is a reusable piece of Jinja/SQL, like a function. Macros live in
`src/macros/`.

- [telemetry_source.sql](src/macros/telemetry_source.sql): `telemetry_source()`
  returns a subquery that decodes the protobuf messages with Spark's
  `from_protobuf`. The staging model just calls
  `select * from {{ telemetry_source() }}`.
- `dbt.concat()` from section 8 is a macro that ships with dbt. It generates
  the right SQL for each warehouse.
- **Adapter dispatch**: one macro, a different implementation per warehouse.
  In the same file, `raw_protobuf_files()` calls `spark__raw_protobuf_files()`
  locally and `databricks__raw_protobuf_files()` on Databricks, because
  reading files works a little differently there. This is how one project
  runs on both.

Macros can also use objects dbt provides, like `target` (the current target:
`{{ target.schema }}`) and `adapter` (`{{ adapter.quote('timestamp') }}` in
the staging model quotes a column name that's also a SQL keyword).

## 10. Variables and hooks

**Variables** are project settings you can override per run. Defined in
[dbt_project.yml](dbt_project.yml):

```yaml
vars:
  raw_path: "{{ env_var('RAW_DATA_PATH', '../data/protobuf/vehicle_telemetry') }}"
  message_type: vehicle.v1.VehicleTelemetry
```

used as `{{ var('raw_path') }}`, and overridden on the command line:
`dbt build --vars '{"raw_path": "/Volumes/..."}'`. That's exactly what the
Databricks job does to read the files from a Unity Catalog volume.
`env_var()` reads environment variables, e.g. for secrets.

**Hooks** run SQL before or after a run (`on-run-start`, `on-run-end`) or
around each model (`pre-hook`, `post-hook`). This project uses `on-run-start`
in `dbt_project.yml` to create the views over the raw files before anything
else runs (the macros are in
[raw_protobuf_views.sql](src/macros/raw_protobuf_views.sql)).

## 11. Tests

A dbt **test** is a `select` that returns the rows that are wrong. No rows
means it passes.

**Generic tests** are declared in YAML next to the models and take arguments.
Four come with dbt, all used in
[_staging.yml](src/models/staging/_staging.yml) and
[_marts.yml](src/models/marts/_marts.yml):

| Test              | Fails when                                          |
|-------------------|-----------------------------------------------------|
| `unique`          | a value appears more than once                      |
| `not_null`        | a value is null                                     |
| `accepted_values` | a value isn't in the list                           |
| `relationships`   | a value has no match in another model (a foreign key) |

You can write your own generic tests. They are macros with `{% test %}`:

- [test_unique_combination_of_columns.sql](src/macros/test_unique_combination_of_columns.sql):
  "these columns together are unique", used on `fct_vehicle_daily`.
- [protobuf_tests.sql](src/macros/protobuf_tests.sql): "every message
  decodes" and "this field follows the schema rule". The source YAML applies
  them to the raw data.

A **singular test** is a one-off: a `.sql` file in `src/tests/` that returns the
bad rows. (There are none in this project.)

`dbt test` runs the tests. **`dbt build`** runs models and tests together, in
DAG order: each model's tests run right after it, and when a test fails, the
models downstream are skipped. Bad data doesn't reach the marts.

**Demo:** make a test fail. In [_staging.yml](src/models/staging/_staging.yml),
remove `"bus"` from the accepted values of `vehicle_type`, then:

```bash
make dbt
```

The test fails ("Got 1 result"), and the four marts and the snapshot are
skipped. The SQL of the failing test is in `target/compiled/`; run it to see
the bad rows. Put `"bus"` back afterwards.

## 12. Incremental models

A `table` model is rebuilt from scratch on every run. Kafka data only grows,
so that gets slower every day, while most of the data hasn't changed since
the last run. An **incremental** model only processes the new rows and adds
them to the existing table.

[fct_vehicle_events.sql](src/models/marts/fct_vehicle_events.sql) is
incremental. Events never change once they happened, so this is a good fit:

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id'
) }}

with readings as (
    select readings.*
    from {{ ref('stg_vehicle_telemetry') }} as readings

    {% if is_incremental() %}
    left join (
        select kafka_partition, max(kafka_offset) as max_kafka_offset
        from {{ this }}
        group by kafka_partition
    ) as loaded
        on readings.kafka_partition = loaded.kafka_partition
    where loaded.max_kafka_offset is null
       or readings.kafka_offset > loaded.max_kafka_offset
    {% endif %}
),
...
```

- `is_incremental()` is true when the table already exists (and you didn't
  pass `--full-refresh`). The first run builds the whole table; later runs
  only add.
- `{{ this }}` is the model's own table, as it was after the last run.
- What is "new"? This is where Kafka helps: within a partition, the offsets
  only grow. So new means an offset higher than the highest one already in
  the table, for the same partition. This is the same thing a Kafka consumer
  group does with its committed offsets.
- Kafka delivers **at-least-once**, so the same event can come again. With
  `unique_key` and the `merge` strategy, dbt updates the existing row instead
  of adding a second one. Running the model twice gives the same result.
- `dbt build --full-refresh` rebuilds the table from scratch. You need it after
  changing the model's columns, or when the logic changed.

Other strategies: `append` (just insert), `insert_overwrite` (replace whole
partitions) and, on Databricks, `replace_where` and `microbatch` (process the
data in time batches).

**Demo:**

```bash
make dbt ARGS="compile --select fct_vehicle_events"
```

The compiled SQL has the `left join` on the existing table, because the table
exists. The statement that actually ran is in
`target/run/vehicle_telemetry/src/models/marts/fct_vehicle_events.sql`:

```sql
merge into vehicle_telemetry.fct_vehicle_events as DBT_INTERNAL_DEST
    using fct_vehicle_events__dbt_tmp as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.event_id = DBT_INTERNAL_DEST.event_id
    when matched then update set *
    when not matched then insert *
```

Then resume the producer in the control panel for a minute and pause it again.
Wait for the protobuf consumer to write its file (once a minute) and run
`make dbt`: the log says `OK created sql incremental model`, and only the new
events were added.

A limitation in this project: the staging view still decodes all the files on
every run, so this saves work in the model, not in reading the files. On
Databricks you would also read only the new files, e.g. with Auto Loader
or a streaming table.

## 13. Snapshots

dbt rebuilds tables, so they only show the current state. `dim_vehicles` says
how far each vehicle has driven *now*, but not how far it had driven last
week. A **snapshot** keeps the history: every time it runs, it compares the
table with its last run, closes the rows that changed and adds their new
version. This is a slowly changing dimension of type 2 (SCD2).

[dim_vehicles_snapshot.yml](src/snapshots/dim_vehicles_snapshot.yml):

```yaml
snapshots:
  - name: dim_vehicles_snapshot
    relation: ref('dim_vehicles')
    config:
      unique_key: vehicle_id
      strategy: timestamp
      updated_at: last_seen_at
```

- `strategy: timestamp`: a row has changed when its `updated_at` column is
  newer. Here: when the vehicle sent new readings.
- `strategy: check` compares a list of columns instead (`check_cols`), for
  tables without a reliable "updated at" column.
- dbt adds the columns `dbt_valid_from` and `dbt_valid_to`. The current
  version has `dbt_valid_to = null`.
- `dbt snapshot` runs the snapshots. `dbt build` runs them too, in DAG order:
  after `dim_vehicles`.
- A snapshot can't be rebuilt: the history only exists in that table. That's
  why it's not a model, and `--full-refresh` doesn't touch it.

**Demo:** resume the producer for a minute (or use the run from section 12),
run `make dbt`, and look at one vehicle's history:

```bash
make dbt ARGS="show --inline \"select vehicle_id, last_seen_at, reading_count, dbt_valid_from, dbt_valid_to from {{ ref('dim_vehicles_snapshot') }} where vehicle_id = 'vehicle-001' order by dbt_valid_from\""
```

```
| vehicle_id  | last_seen_at         | reading_count | dbt_valid_from       | dbt_valid_to         |
| vehicle-001 | 2026-10-01 08:53:... |          1062 | 2026-10-01 08:53:... | 2026-10-01 14:42:... |
| vehicle-001 | 2026-10-01 14:42:... |          1192 | 2026-10-01 14:42:... |                      |
```

## 14. Documentation

The `description:` fields in the YAML files are documentation. dbt generates a
website from them, with every model, column, test and the lineage graph:

```bash
make dbt ARGS="docs generate --static"
```

Open `target/static_index.html` in a browser and click the lineage button at
the bottom right. On Databricks, `persist_docs` can also write the
descriptions to Unity Catalog as table and column comments.

## 15. Running on Databricks

The same project runs on Databricks, deployed as a Databricks Asset Bundle
(see [README.md](README.md#deploying-to-databricks-as-an-asset-bundle)):

- [resources/vehicle_telemetry.job.yml](resources/vehicle_telemetry.job.yml):
  a job with a **dbt task** that runs `dbt build` on a SQL warehouse. It
  passes the volume path with `--vars` (section 10).
- The job creates the profile itself, from the warehouse, catalog and schema
  in the task. You don't commit credentials.
- `raw_protobuf_files()` picks the Databricks implementation (section 9).
  Nothing else changes. Tables are Delta by default there, so the incremental
  model and the snapshot work the same way. The job runs every hour, so the
  snapshot gets a new version every hour.
- dev/prod: in the `dev` target the bundle deploys to
  `dev_<your_name>_vehicle_telemetry`, so everyone has their own copy.

```bash
make dab-deploy     # create the schema, volume and job
make dab-upload     # copy the protobuf files to the volume
make dab-run        # run dbt build on Databricks
```

## 16. What we didn't cover

- **Seeds**: small CSV files loaded as tables, e.g. lookup lists.
- **Packages**: reuse other people's macros and tests, like `dbt_utils` (`packages.yml`, `dbt deps`).
- **Model contracts and versions**: enforce a model's columns and types.
- **Exposures**: declare the dashboards that use your models.

## Try it yourself

1. Add a model `fct_vehicle_hourly` with the average speed per vehicle and
   hour. Read from `stg_vehicle_telemetry` with `ref()`, and run it with
   `make dbt ARGS="build --select fct_vehicle_hourly"`.
2. Add `unique_combination_of_columns` and a `description` for it in `_marts.yml`.
3. Add a singular test in `src/tests/` that fails if any reading has a
   negative `speed_kmh`.
4. Change the snapshot to `strategy: check` with
   `check_cols: [odometer_km]`. When does it add a new version now, compared
   with `strategy: timestamp`?
