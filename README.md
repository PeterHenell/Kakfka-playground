# Kafka playground

A small, self-contained setup for learning Apache Kafka. A Python producer
simulates a fleet of vehicles that send telemetry (position, speed, engine
temperature, fuel level, tire pressure...) to Kafka. Two consumers read the
same stream, each in its own consumer group:

- **consumer** stores the messages in an ELK stack (Elasticsearch, Logstash,
  Kibana), where you can search, chart and map them in near real time.
- **parquet-consumer** writes the messages to Parquet files, which a
  [dbt project](dbt/README.md) turns into analytics tables with DuckDB. The
  same dbt project can be deployed to Databricks as an Asset Bundle.

```
                                                 group elk-writer
┌──────────┐    ┌─────────────────────────┐    ┌──────────┐    ┌──────────┐    ┌───────────────┐    ┌────────┐
│ producer │ -> │ Kafka                   │ -> │ consumer │ -> │ Logstash │ -> │ Elasticsearch │ <- │ Kibana │
│ (python) │    │ topic vehicle-telemetry │    │ (python) │    │ (HTTP)   │    │               │    │        │
└──────────┘    │ 3 partitions            │    └──────────┘    └──────────┘    └───────────────┘    └────────┘
                └─────────────────────────┘
                             │                   group parquet-writer
                             │                 ┌──────────────────┐    ┌────────────────┐    ┌─────────────────┐
                             └───────────────> │ parquet-consumer │ -> │ data/parquet/  │ -> │ dbt + DuckDB    │
                                               │ (python)         │    │ *.parquet      │    │ (or Databricks) │
                                               └──────────────────┘    └────────────────┘    └─────────────────┘
```

Everything runs in a single Docker Compose project.

## Getting started

Requirements: Docker with Docker Compose, and about 3 GB of free memory for
the containers.

```bash
docker compose up -d --build
```

The first start takes a minute or two while Elasticsearch and Kibana boot.
Check progress with `docker compose ps` and `docker compose logs -f producer consumer parquet-consumer`.

| Service       | URL                        | What it is                                      |
|---------------|----------------------------|-------------------------------------------------|
| Kibana        | http://localhost:5601      | Explore the stored telemetry                    |
| Kafka UI      | http://localhost:8080      | Browse topics, messages, consumer groups        |
| Elasticsearch | http://localhost:9200      | REST API for the stored data                    |
| Logstash      | http://localhost:8081      | HTTP input the consumer posts to                |
| Kafka         | `localhost:9094`           | Bootstrap server for clients on your machine    |

Stop everything with `docker compose down`. Add `-v` to also delete the stored
Kafka and Elasticsearch data. The Parquet files and the DuckDB database are
in `./data`; delete that folder to start over.

### Looking at the data in Kibana

A data view called **Vehicle telemetry** is created automatically.

- **Discover** (menu → Analytics → Discover): see the raw messages. Try the
  queries `events : "harsh_braking"`, `engine_temp_c > 100` or
  `check_engine_light : true`.
- **Maps** (menu → Analytics → Maps): add a *Documents* layer on the
  *Vehicle telemetry* data view to watch the vehicles move around Stockholm.
  Style it by `vehicle_id` and turn on auto-refresh.
- **Dashboards**: build charts of e.g. average `speed_kmh` or maximum
  `engine_temp_c` per `vehicle_id` over time.

### Processing the Parquet files with dbt

The parquet consumer writes a file to `data/parquet/vehicle_telemetry/event_date=YYYY-MM-DD/`
every 60 seconds (or every 5000 messages). Once the first file is there, build
the dbt models on DuckDB:

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
| `producer/producer.py`                 | Vehicle simulator, publishes to Kafka                             |
| `consumer/consumer.py`                 | Reads from Kafka, posts batches to Logstash                       |
| `parquet-consumer/parquet_consumer.py` | Reads from Kafka, writes Parquet files to `./data/parquet`        |
| `dbt/`                                 | dbt project (DuckDB locally) and Databricks Asset Bundle          |
| `elk/logstash/pipeline/*.conf`         | Logstash pipeline: HTTP in, Elasticsearch out                     |
| `elk/setup/setup.sh`                   | Creates the Elasticsearch index template and the Kibana data view |

### A telemetry message

The producer uses `vehicle_id` as the message **key**. Kafka hashes the key
to choose a partition, so all messages from one vehicle go to the same
partition and keep their order.

```json
{
  "message_id": "0b4b4a87-295c-4e31-bbf3-f06489035740",
  "vehicle_id": "vehicle-001",
  "vin": "7DNKTFZ2314N1LCN5",
  "vehicle_type": "bus",
  "timestamp": "2026-09-29T05:55:42.638+00:00",
  "position": { "lat": 59.325754, "lon": 18.037084 },
  "heading_deg": 255.0,
  "speed_kmh": 8.6,
  "rpm": 1720,
  "gear": 1,
  "engine_temp_c": 39.8,
  "fuel_level_pct": 89.56,
  "battery_voltage": 14.38,
  "odometer_km": 162871.571,
  "tire_pressure_kpa": { "front_left": 230.6, "front_right": 221.0, "rear_left": 232.8, "rear_right": 229.2 },
  "check_engine_light": false,
  "dtc_codes": [],
  "events": []
}
```

Some simulated vehicles have a faulty cooling system or a leaking tire, so
over time you will see events like `engine_overheating`, `low_tire_pressure`,
`harsh_braking`, `speeding` and `low_fuel`, along with diagnostic trouble codes.

### Delivery guarantees

- The **producer** uses `acks=all` and `enable.idempotence=true`, so a retried
  send can't create duplicates in Kafka.
- The **consumer** turns off auto-commit and commits offsets only after
  Logstash has accepted the batch (*at-least-once*). If it crashes before
  committing, it reads the same messages again after a restart.
- **Logstash** uses `message_id` as the Elasticsearch document id, so
  messages that are delivered twice overwrite the same document instead of
  creating a duplicate.
- The **parquet consumer** also commits only after a file is written. A file
  can't be "overwritten" like a document, so duplicates are removed later, in
  the dbt staging model.

## Running the scripts from your machine

The producer and consumer also run in Compose, but you can run extra copies
on your machine (they connect to `localhost:9094` by default):

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python producer/producer.py
python consumer/consumer.py
python parquet-consumer/parquet_consumer.py
```

Both scripts are configured with environment variables. The docstring at the
top of each script lists them, for example `NUM_VEHICLES=20 INTERVAL_SECONDS=0.2 python producer/producer.py`.

## Things to try

1. **Scale out the consumer group.** Start a second consumer with the same group
   id: `docker compose up -d --scale consumer=2` (first remove the
   `container_name: consumer` line), or run `python consumer/consumer.py`
   on your machine. Watch the `Assigned partitions` lines in the logs as Kafka
   rebalances the 3 partitions. What happens with 4 consumers?
2. **Consumer groups.** `elk-writer` and `parquet-writer` both read every
   message, independently. Add a third group by running
   `KAFKA_GROUP_ID=my-group python consumer/consumer.py`. The new group gets
   its own copy of every message, starting from the beginning of the topic.
3. **Consumer lag.** Stop the consumer (`docker compose stop consumer`), wait a
   minute, and look at the lag of the `elk-writer` group in Kafka UI or with:
   ```bash
   docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
     --bootstrap-server localhost:9092 --describe --group elk-writer
   ```
   Start it again and watch it catch up. Compare with `parquet-writer`: its lag
   grows for up to a minute and then drops to 0, because it only commits after
   writing a file.
4. **Kafka CLI tools.** The broker image comes with the standard tools:
   ```bash
   docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic vehicle-telemetry
   docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
     --topic vehicle-telemetry --property print.key=true --property print.partition=true
   ```
5. **Replay.** Reset the group's offsets to the beginning (stop the consumer first) and
   notice that Elasticsearch doesn't get duplicates, because documents are keyed by
   `message_id`:
   ```bash
   docker compose stop consumer
   docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
     --group elk-writer --topic vehicle-telemetry --reset-offsets --to-earliest --execute
   docker compose start consumer
   ```
6. **Break things.** Stop Logstash (`docker compose stop logstash`) and see the
   consumer retry without committing. Stop Kafka and see what the producer does.

## Notes

- This is a learning setup: Kafka runs as a single node in KRaft mode (no
  ZooKeeper), and Elasticsearch security is turned off. Don't use this
  configuration in production.
- If Elasticsearch fails to start on Linux with a `vm.max_map_count` error, run
  `sudo sysctl -w vm.max_map_count=262144`.
