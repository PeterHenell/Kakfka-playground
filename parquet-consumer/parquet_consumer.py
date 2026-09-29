"""Reads vehicle telemetry from Kafka and writes it to Parquet files.

This consumer uses its own consumer group (`parquet-writer`), separate from
the ELK consumer (`elk-writer`). Kafka tracks offsets per group, so both
groups receive every message in the topic, independently of each other.

Messages are protobuf. They are decoded with the class named in each
message's `message-type` header, and converted to Arrow with protarrow, which
derives the Parquet schema from the protobuf descriptor (see
schema/telemetry_schema/parquet.py). After changing the .proto file and
regenerating the code, new files get the new columns without changes here.

Files use Hive-style partitioning by the date of the message's timestamp:

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
    MESSAGE_TYPE             default: vehicle.v1.VehicleTelemetry (used when a
                             message has no message-type header)
    PARTITION_FIELD          default: timestamp (a google.protobuf.Timestamp
                             field; its date becomes the event_date folder)

Before running this outside Docker, generate the protobuf code:

    python schema/generate.py
"""

import os
import signal
import sys
import time
import uuid
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError
from google.protobuf.message import DecodeError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "schema"))
from telemetry_schema import message_class, parquet  # noqa: E402

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = os.getenv("KAFKA_TOPIC", "vehicle-telemetry")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "parquet-writer")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./data/parquet/vehicle_telemetry")
FLUSH_MAX_MESSAGES = int(os.getenv("FLUSH_MAX_MESSAGES", "5000"))
FLUSH_INTERVAL_SECONDS = float(os.getenv("FLUSH_INTERVAL_SECONDS", "60"))
MESSAGE_TYPE = os.getenv("MESSAGE_TYPE", "vehicle.v1.VehicleTelemetry")
PARTITION_FIELD = os.getenv("PARTITION_FIELD", "timestamp")


class Buffer:
    """Decoded messages waiting to be written, plus where they came from in Kafka."""

    def __init__(self):
        self.messages = []
        self.partitions = []
        self.offsets = []
        self.ingested_at = []

    def __len__(self):
        return len(self.messages)

    def add(self, message, kafka_msg) -> None:
        self.messages.append(message)
        self.partitions.append(kafka_msg.partition())
        self.offsets.append(kafka_msg.offset())
        self.ingested_at.append(datetime.now(timezone.utc))

    def to_table(self, message_type: type) -> pa.Table:
        table = parquet.to_table(self.messages, message_type)
        # Columns that aren't part of the protobuf message.
        table = table.append_column("kafka_partition", pa.array(self.partitions, pa.int32()))
        table = table.append_column("kafka_offset", pa.array(self.offsets, pa.int64()))
        return table.append_column("ingested_at", pa.array(self.ingested_at, pa.timestamp("us", tz="UTC")))


def event_date(message) -> date:
    if PARTITION_FIELD in message.DESCRIPTOR.fields_by_name and message.HasField(PARTITION_FIELD):
        return getattr(message, PARTITION_FIELD).ToDatetime(tzinfo=timezone.utc).date()
    return datetime.now(timezone.utc).date()


def write_file(buffer: Buffer, message_type: type, day: date) -> None:
    directory = os.path.join(OUTPUT_DIR, f"event_date={day.isoformat()}")
    os.makedirs(directory, exist_ok=True)
    name = f"part-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}.parquet"
    path = os.path.join(directory, name)
    pq.write_table(buffer.to_table(message_type), path + ".tmp", compression="zstd")
    os.rename(path + ".tmp", path)
    print(f"Wrote {len(buffer)} {message_type.DESCRIPTOR.name} messages to {path}")


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
    # One buffer per (message type, event date): each becomes one file.
    buffers: dict[tuple[type, date], Buffer] = defaultdict(Buffer)
    last_flush = time.monotonic()

    def flush() -> None:
        nonlocal last_flush
        if buffers:
            for (message_type, day), buffer in buffers.items():
                write_file(buffer, message_type, day)
            buffers.clear()
            # The files are on disk: now it is safe to tell Kafka we are done
            # with these messages.
            consumer.commit(asynchronous=False)
        last_flush = time.monotonic()

    try:
        while running:
            for msg in consumer.consume(num_messages=500, timeout=1.0):
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"Kafka error: {msg.error()}")
                    continue
                try:
                    headers = dict(msg.headers() or [])
                    message_type = message_class(headers.get("message-type", MESSAGE_TYPE.encode()).decode())
                    message = message_type.FromString(msg.value())
                except (DecodeError, KeyError) as e:
                    print(f"Skipping undecodable message at partition {msg.partition()} offset {msg.offset()}: {e!r}")
                    continue
                buffers[(message_type, event_date(message))].add(message, msg)

            buffered = sum(len(b) for b in buffers.values())
            if buffered >= FLUSH_MAX_MESSAGES or time.monotonic() - last_flush >= FLUSH_INTERVAL_SECONDS:
                flush()
        flush()
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
