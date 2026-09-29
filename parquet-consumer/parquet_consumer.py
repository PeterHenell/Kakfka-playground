"""Reads vehicle telemetry from Kafka and writes it to Parquet files.

This consumer uses its own consumer group (`parquet-writer`), separate from
the ELK consumer (`elk-writer`). Kafka tracks offsets per group, so both
groups receive every message in the topic, independently of each other.

Messages are buffered and written as one Parquet file per partition of the
output, using Hive-style partitioning by date:

    <OUTPUT_DIR>/event_date=2026-09-29/part-20260929T101500-3f2a.parquet

A file is written when the buffer holds FLUSH_MAX_MESSAGES messages or when
FLUSH_INTERVAL_SECONDS have passed, whichever comes first. Offsets are
committed only after the file is on disk (at-least-once), so the dbt project
de-duplicates on message_id.

Files are first written with a `.tmp` suffix and then renamed, so readers
that glob `*.parquet` never see a half-written file.

Configuration (environment variables):
    KAFKA_BOOTSTRAP_SERVERS  default: localhost:9094
    KAFKA_TOPIC              default: vehicle-telemetry
    KAFKA_GROUP_ID           default: parquet-writer
    OUTPUT_DIR               default: ./data/parquet/vehicle_telemetry
    FLUSH_MAX_MESSAGES       default: 5000
    FLUSH_INTERVAL_SECONDS   default: 60
"""

import json
import os
import signal
import time
import uuid
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = os.getenv("KAFKA_TOPIC", "vehicle-telemetry")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "parquet-writer")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./data/parquet/vehicle_telemetry")
FLUSH_MAX_MESSAGES = int(os.getenv("FLUSH_MAX_MESSAGES", "5000"))
FLUSH_INTERVAL_SECONDS = float(os.getenv("FLUSH_INTERVAL_SECONDS", "60"))

# An explicit schema keeps the files consistent (a column is never inferred
# as null just because a batch happened to have no values for it), and turns
# the ISO timestamp string into a real timestamp column.
TIRES = ["front_left", "front_right", "rear_left", "rear_right"]
SCHEMA = pa.schema(
    [
        ("message_id", pa.string()),
        ("vehicle_id", pa.string()),
        ("vin", pa.string()),
        ("vehicle_type", pa.string()),
        ("timestamp", pa.timestamp("ms", tz="UTC")),
        ("position", pa.struct([("lat", pa.float64()), ("lon", pa.float64())])),
        ("heading_deg", pa.float64()),
        ("speed_kmh", pa.float64()),
        ("rpm", pa.int32()),
        ("gear", pa.int32()),
        ("engine_temp_c", pa.float64()),
        ("fuel_level_pct", pa.float64()),
        ("battery_voltage", pa.float64()),
        ("odometer_km", pa.float64()),
        ("tire_pressure_kpa", pa.struct([(t, pa.float64()) for t in TIRES])),
        ("check_engine_light", pa.bool_()),
        ("dtc_codes", pa.list_(pa.string())),
        ("events", pa.list_(pa.string())),
        ("kafka_partition", pa.int32()),
        ("kafka_offset", pa.int64()),
        ("ingested_at", pa.timestamp("ms", tz="UTC")),
    ]
)


def to_row(msg) -> dict:
    record = json.loads(msg.value())
    record["timestamp"] = datetime.fromisoformat(record["timestamp"])
    record["kafka_partition"] = msg.partition()
    record["kafka_offset"] = msg.offset()
    record["ingested_at"] = datetime.now(timezone.utc)
    return record


def write_files(rows: list[dict]) -> None:
    """Write the rows as one Parquet file per event date."""
    by_date: dict[str, list[dict]] = {}
    for row in rows:
        by_date.setdefault(row["timestamp"].date().isoformat(), []).append(row)

    for event_date, date_rows in by_date.items():
        directory = os.path.join(OUTPUT_DIR, f"event_date={event_date}")
        os.makedirs(directory, exist_ok=True)
        name = f"part-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}.parquet"
        path = os.path.join(directory, name)
        table = pa.Table.from_pylist(date_rows, schema=SCHEMA)
        pq.write_table(table, path + ".tmp", compression="zstd")
        os.rename(path + ".tmp", path)
        print(f"Wrote {len(date_rows)} rows to {path}")


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])

    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    print(f"Consuming '{TOPIC}' from {BOOTSTRAP_SERVERS} as group '{GROUP_ID}', writing Parquet to {OUTPUT_DIR}")
    buffer: list[dict] = []
    last_flush = time.monotonic()

    def flush() -> None:
        nonlocal buffer, last_flush
        if buffer:
            write_files(buffer)
            # The files are on disk: now it is safe to tell Kafka we are done
            # with these messages.
            consumer.commit(asynchronous=False)
        buffer = []
        last_flush = time.monotonic()

    try:
        while running:
            for msg in consumer.consume(num_messages=500, timeout=1.0):
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"Kafka error: {msg.error()}")
                    continue
                try:
                    buffer.append(to_row(msg))
                except (ValueError, KeyError) as e:
                    print(f"Skipping bad message at partition {msg.partition()} offset {msg.offset()}: {e}")

            if len(buffer) >= FLUSH_MAX_MESSAGES or time.monotonic() - last_flush >= FLUSH_INTERVAL_SECONDS:
                flush()
        flush()
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
