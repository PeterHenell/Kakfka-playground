# Kafka playground

A small, self-contained setup for learning Apache Kafka. A Python producer
simulates a fleet of vehicles that send telemetry (position, speed, engine
temperature, fuel level, tire pressure...) to Kafka as
[Protocol Buffers](https://protobuf.dev/) messages. Two consumers read the
same stream, each in its own consumer group:

- **consumer** stores the messages in an ELK stack (Elasticsearch, Logstash,
  Kibana), where you can search, chart and map them in near real time.
- **protobuf-consumer** archives the messages, still in binary protobuf, to
  files. A [dbt project](dbt/README.md) decodes them with Apache Spark and
  turns them into analytics tables. The same dbt project can be deployed to
  Databricks as an Asset Bundle.

The message format is defined once, in a `.proto` file. Everything else
(the Python classes, the conversion to Elasticsearch documents, the
Elasticsearch mapping, the schema Spark decodes with) follows from it; see
[The message schema](#the-message-schema).

```
                                                 group elk-writer
┌──────────┐    ┌─────────────────────────┐    ┌──────────┐    ┌──────────┐    ┌───────────────┐    ┌────────┐
│ producer │ -> │ Kafka                   │ -> │ consumer │ -> │ Logstash │ -> │ Elasticsearch │ <- │ Kibana │
│ (python) │    │ topic vehicle-telemetry │    │ (python) │    │ (HTTP)   │    │               │    │        │
└──────────┘    │ 3 partitions            │    └──────────┘    └──────────┘    └───────────────┘    └────────┘
                └─────────────────────────┘
                             │                   group protobuf-writer
                             │                 ┌───────────────────┐    ┌────────────────┐    ┌─────────────────┐
                             └───────────────> │ protobuf-consumer │ -> │ data/protobuf/ │ -> │ dbt + Spark     │
                                               │ (python)          │    │ *.pb           │    │ (or Databricks) │
                                               └───────────────────┘    └────────────────┘    └─────────────────┘
```

Everything runs in a single Docker Compose project.

## Getting started

Requirements: Docker with Docker Compose, and about 3 GB of free memory for
the containers.

```bash
docker compose up -d --build
```

The first start takes a minute or two while Elasticsearch and Kibana boot.
Check progress with `docker compose ps` and `docker compose logs -f producer consumer protobuf-consumer`.

| Service       | URL                        | What it is                                      |
|---------------|----------------------------|-------------------------------------------------|
| Kibana        | http://localhost:5601      | Explore the stored telemetry                    |
| Kafka UI      | http://localhost:8080      | Topics, decoded messages, consumer groups       |
| Elasticsearch | http://localhost:9200      | REST API for the stored data                    |
| Logstash      | http://localhost:8081      | HTTP input the consumer posts to                |
| Kafka         | `localhost:9094`           | Bootstrap server for clients on your machine    |

Stop everything with `docker compose down`. Add `-v` to also delete the stored
Kafka and Elasticsearch data. The protobuf files and the Spark tables are in
`./data`; delete that folder to start over.

### Looking at the data in Kibana

A data view called **Vehicle telemetry** is created automatically.

- **Discover** (menu → Analytics → Discover): see the raw messages. Try the
  queries `events : "HARSH_BRAKING"`, `engine_temp_c > 100` or
  `check_engine_light : true`.
- **Maps** (menu → Analytics → Maps): add a *Documents* layer on the
  *Vehicle telemetry* data view to watch the vehicles move around Stockholm.
  Style it by `vehicle_id` and turn on auto-refresh.
- **Dashboards**: build charts of e.g. average `speed_kmh` or maximum
  `engine_temp_c` per `vehicle_id` over time.

### Processing the protobuf files with dbt

The protobuf consumer writes a file to `data/protobuf/vehicle_telemetry/date=YYYY-MM-DD/`
every 60 seconds (or every 5000 messages). Once the first file is there, build
the dbt models on Spark:

```bash
docker compose run --rm dbt build
docker compose run --rm dbt show --select fct_vehicle_daily
```

See [dbt/README.md](dbt/README.md) for the models, and for how to deploy the
project to Databricks as a Databricks Asset Bundle.

## What's in the repo

| Path                                   | Description                                                       |
|----------------------------------------|-------------------------------------------------------------------|
| `docker-compose.yml`                   | All services                                                      |
| `schema/proto/`                        | The protobuf message definitions                                  |
| `schema/generate.py`                   | Generates code and the Elasticsearch mapping from the .proto files |
| `schema/telemetry_schema/`             | Shared code: finding message classes, protobuf to Elasticsearch   |
| `producer/producer.py`                 | Vehicle simulator, publishes to Kafka                             |
| `consumer/consumer.py`                 | Reads from Kafka, posts batches to Logstash                       |
| `protobuf-consumer/protobuf_consumer.py` | Reads from Kafka, writes binary protobuf files to `./data/protobuf` |
| `dbt/`                                 | dbt project (Spark locally) and Databricks Asset Bundle           |
| `elk/logstash/pipeline/*.conf`         | Logstash pipeline: HTTP in, Elasticsearch out                     |
| `elk/setup/setup.py`                   | Installs the generated index template and the Kibana data view    |
| `kafka-ui/config.yml`                  | Lets Kafka UI decode the protobuf messages                        |

### A telemetry message

The producer uses `vehicle_id` as the message **key**. Kafka hashes the key
to choose a partition, so all messages from one vehicle go to the same
partition and keep their order.

The value is a `vehicle.v1.VehicleTelemetry` protobuf message, and a
`message-type` header names that type. As a document in Elasticsearch, a
message looks like this:

```json
{
  "message_id": "c00e4bbc-e4b7-41c2-a3cf-d432c3a5b0a6",
  "vehicle_id": "vehicle-003",
  "vin": "HUNWXCA4JH9S4SYEX",
  "vehicle_type": "BUS",
  "timestamp": "2026-09-29T08:02:54.428502Z",
  "position": { "lat": 59.351242, "lon": 18.090492 },
  "heading_deg": 357.2,
  "speed_kmh": 47.0,
  "rpm": 1109,
  "gear": 3,
  "engine_temp_c": 111.6,
  "fuel_level_pct": 54.02,
  "battery_voltage": 14.07,
  "odometer_km": 186607.526,
  "tire_pressure_kpa": { "front_left": 230.4, "front_right": 227.9, "rear_left": 226.0, "rear_right": 213.8 },
  "check_engine_light": true,
  "dtc_codes": ["P0217"],
  "events": ["ENGINE_OVERHEATING"]
}
```

Some simulated vehicles have a faulty cooling system or a leaking tire, so
over time you will see events like `ENGINE_OVERHEATING`, `LOW_TIRE_PRESSURE`,
`HARSH_BRAKING`, `SPEEDING` and `LOW_FUEL`, along with diagnostic trouble codes.

## The message schema

[`schema/proto/vehicle/v1/vehicle_telemetry.proto`](schema/proto/vehicle/v1/vehicle_telemetry.proto)
defines the message. `schema/generate.py` turns it into everything the
services need, in `schema/generated/`:

| Output                                  | Made by                       | Used by                              |
|-----------------------------------------|-------------------------------|--------------------------------------|
| `vehicle/v1/vehicle_telemetry_pb2.py`   | `protoc` (via `grpcio-tools`) | all services, to (de)serialize       |
| `kafka/v1/kafka_record_pb2.py`          | `protoc`                      | protobuf-consumer, file format       |
| `descriptors/descriptor_set.desc`       | `protoc`                      | Spark's `from_protobuf`, to decode   |
| `elasticsearch/index_template.json`     | `telemetry_schema/elk.py`     | `elk-setup`, to create the mapping   |

The conversions at runtime don't use any hand-written, per-field code. They
walk the protobuf *descriptors*, the schema information that `protoc` embeds
in the generated code:

- **Elasticsearch**: protobuf's own [`json_format`](https://googleapis.dev/python/protobuf/latest/google/protobuf/json_format.html)
  turns a message into a JSON document. The index mapping comes from
  `telemetry_schema/elk.py`. It maps each protobuf type to an Elasticsearch
  type and honors a custom field option for special cases:
  `Position position = 6 [(elk.field_type) = "geo_point"];`.
- **Spark**: the protobuf consumer doesn't decode anything. It stores the
  Kafka records as they are, and Spark decodes them with its built-in
  [`from_protobuf`](https://spark.apache.org/docs/latest/sql-data-sources-protobuf.html)
  function, using the *descriptor set* (the compiled schema) that the consumer
  writes next to the files. Nested messages become structs, repeated fields
  become arrays, enums become their names, and Timestamps become timestamps.
- **Kafka UI** decodes the messages with its built-in `ProtobufFile` serde,
  reading the same `.proto` files.

The ELK consumer picks the message class by the `message-type` header, and
the protobuf consumer stores the header with each record. Neither contains
any field names.

### The protobuf files

Each file is one `kafka.v1.KafkaRecordBatch` message
([`kafka_record.proto`](schema/proto/kafka/v1/kafka_record.proto)): a list
of Kafka records, each with the original message bytes, the message type,
and the topic, partition, offset and timestamp.

```
data/protobuf/vehicle_telemetry/
├── _schema/descriptor_set.desc              the compiled schema, rewritten on every start
└── date=2026-09-29/                         the date of the Kafka timestamp
    ├── part-20260929T100733-2ad5872b.pb     one KafkaRecordBatch per file
    └── part-20260929T100833-4c021d36.pb
```

Decoding takes two `from_protobuf` calls: one for the file, one for each
record's bytes. See `dbt/src/macros/telemetry_source.sql`, or try it in the
Spark SQL shell (`docker compose run --rm spark-sql`):

```sql
select from_protobuf(record.value, 'vehicle.v1.VehicleTelemetry', '/data/protobuf/vehicle_telemetry/_schema/descriptor_set.desc',
                     map('emit.default.values', 'true')) as message
from (
  select explode(from_protobuf(content, 'kafka.v1.KafkaRecordBatch', '/data/protobuf/vehicle_telemetry/_schema/descriptor_set.desc').records) as record
  from binaryFile.`/data/protobuf/vehicle_telemetry`
)
limit 5;
```

### Changing the schema

1. Edit the `.proto` file, e.g. add `double ambient_temp_c = 19;`.
2. Set the new field in `producer/producer.py`.
3. Rebuild and restart: `docker compose up -d --build`. Each image runs
   `schema/generate.py` while it is built.

The result:

- New Elasticsearch documents contain the field. `elk-setup` updates the
  index template and adds the field to the mapping of existing indices.
- The protobuf consumer writes the new descriptor set, so Spark can decode
  the field. It decodes the older files with the new schema too; in messages
  from before the change, the field has its default value (0, "", false).
- The same goes for messages still in Kafka that were produced with the old
  schema.

To keep old and new messages compatible, only add fields with new numbers.
Never change the number or type of an existing field, and `reserve` the
numbers of removed fields. The dbt models list the columns they use, so add
new fields to `dbt/src/models/staging/stg_vehicle_telemetry.sql` to use them
there.

### Delivery guarantees

- The **producer** uses `acks=all` and `enable.idempotence=true`, so a retried
  send can't create duplicates in Kafka.
- The **consumer** turns off auto-commit and commits offsets only after
  Logstash has accepted the batch (*at-least-once*). If it crashes before
  committing, it reads the same messages again after a restart.
- **Logstash** uses `message_id` as the Elasticsearch document id, so
  messages that are delivered twice overwrite the same document instead of
  creating a duplicate.
- The **protobuf consumer** also commits only after a file is written. A file
  can't be "overwritten" like a document, so duplicates are removed later, in
  the dbt staging model.

## Running the scripts from your machine

The producer and consumer also run in Compose, but you can run extra copies
on your machine (they connect to `localhost:9094` by default):

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python schema/generate.py          # again after every .proto change

python producer/producer.py
python consumer/consumer.py
python protobuf-consumer/protobuf_consumer.py
```

The scripts are configured with environment variables. The docstring at the
top of each script lists them, for example `NUM_VEHICLES=20 INTERVAL_SECONDS=0.2 python producer/producer.py`.

The converters have unit tests: `pip install pytest && python -m pytest schema/tests`.

## Things to try

1. **Scale out the consumer group.** Start a second consumer with the same group
   id: `docker compose up -d --scale consumer=2` (first remove the
   `container_name: consumer` line), or run `python consumer/consumer.py`
   on your machine. Watch the `Assigned partitions` lines in the logs as Kafka
   rebalances the 3 partitions. What happens with 4 consumers?
2. **Consumer groups.** `elk-writer` and `protobuf-writer` both read every
   message, independently. Add a third group by running
   `KAFKA_GROUP_ID=my-group python consumer/consumer.py`. The new group gets
   its own copy of every message, starting from the beginning of the topic.
3. **Consumer lag.** Stop the consumer (`docker compose stop consumer`), wait a
   minute, and look at the lag of the `elk-writer` group in Kafka UI or with:
   ```bash
   docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
     --bootstrap-server localhost:9092 --describe --group elk-writer
   ```
   Start it again and watch it catch up. Compare with `protobuf-writer`: its lag
   grows for up to a minute and then drops to 0, because it only commits after
   writing a file.
4. **Kafka CLI tools.** The broker image comes with the standard tools:
   ```bash
   docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic vehicle-telemetry
   docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
     --topic vehicle-telemetry --property print.key=true --property print.headers=true
   ```
   The values are binary protobuf, so they look garbled. That's the price of
   a compact format: you need the schema to read them (as Kafka UI does).
5. **Replay.** Reset the group's offsets to the beginning (stop the consumer first) and
   notice that Elasticsearch doesn't get duplicates, because documents are keyed by
   `message_id`:
   ```bash
   docker compose stop consumer
   docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
     --group elk-writer --topic vehicle-telemetry --reset-offsets --to-earliest --execute
   docker compose start consumer
   ```
6. **Evolve the schema.** Follow [Changing the schema](#changing-the-schema) to add a
   field, then find it in Kibana, Kafka UI and Spark
   (`docker compose run --rm dbt show --inline "select ambient_temp_c, count(*) from {{ telemetry_source() }} group by 1"`).
   What happens if you change the type of an existing field instead?
7. **Break things.** Stop Logstash (`docker compose stop logstash`) and see the
   consumer retry without committing. Stop Kafka and see what the producer does.

## Notes

- This is a learning setup: Kafka runs as a single node in KRaft mode (no
  ZooKeeper), and Elasticsearch security is turned off. Don't use this
  configuration in production.
- If Elasticsearch fails to start on Linux with a `vm.max_map_count` error, run
  `sudo sysctl -w vm.max_map_count=262144`.
