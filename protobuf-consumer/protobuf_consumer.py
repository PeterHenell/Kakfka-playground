"""Reads records from Kafka and archives them as binary protobuf files.

This consumer uses its own consumer group (`protobuf-writer`), separate from
the ELK consumer (`elk-writer`). Kafka tracks offsets per group, so both
groups receive every message in the topic, independently of each other.

The consumer doesn't decode the messages. Each file is one
`kafka.v1.KafkaRecordBatch` protobuf message (see
schema/proto/kafka/v1/kafka_record.proto) holding the records exactly as
they came from Kafka: the message bytes plus topic, partition, offset,
timestamp and message type. Decoding and flattening happens later, in Spark
(see the dbt project), with `from_protobuf`.

Next to the files, the consumer writes the protobuf *descriptor set*: the
compiled schema of all .proto files, which Spark needs to decode the bytes.

    <OUTPUT_DIR>/_schema/descriptor_set.desc
    <OUTPUT_DIR>/date=2026-09-29/part-20260929T101500-3f2a1b2c.pb

The date folder is the date of the Kafka record timestamp. Spark ignores
files and folders that start with `_` or `.` when it lists the data files.

A file is written when the buffer holds FLUSH_MAX_MESSAGES messages or when
FLUSH_INTERVAL_SECONDS have passed, whichever comes first. Offsets are
committed only after the file is on disk (at-least-once), so the dbt project
de-duplicates on message_id. Files are first written under a hidden `.tmp`
name and then renamed, so readers never see a half-written file.

Configuration (environment variables):
    KAFKA_BOOTSTRAP_SERVERS  default: localhost:9094
    KAFKA_TOPIC              default: vehicle-telemetry
    KAFKA_GROUP_ID           default: protobuf-writer
    OUTPUT_DIR               default: ./data/protobuf/vehicle_telemetry
    FLUSH_MAX_MESSAGES       default: 5000
    FLUSH_INTERVAL_SECONDS   default: 60
    MESSAGE_TYPE             default: vehicle.v1.VehicleTelemetry (used when a
                             message has no message-type header)

Before running this outside Docker, generate the protobuf code:

    python schema/generate.py
"""

import os
import shutil
import signal
import sys
import time
import uuid
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

from confluent_kafka import TIMESTAMP_NOT_AVAILABLE, Consumer, KafkaError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "schema"))
from telemetry_schema.registry import DESCRIPTOR_SET, load_generated_modules  # noqa: E402

load_generated_modules()
from kafka.v1 import kafka_record_pb2  # noqa: E402

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = os.getenv("KAFKA_TOPIC", "vehicle-telemetry")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "protobuf-writer")
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "./data/protobuf/vehicle_telemetry"))
FLUSH_MAX_MESSAGES = int(os.getenv("FLUSH_MAX_MESSAGES", "5000"))
FLUSH_INTERVAL_SECONDS = float(os.getenv("FLUSH_INTERVAL_SECONDS", "60"))
MESSAGE_TYPE = os.getenv("MESSAGE_TYPE", "vehicle.v1.VehicleTelemetry")


def publish_descriptor_set() -> None:
    """Copies the compiled schema next to the data, so readers can decode it.

    The descriptor set is replaced on every start, so after a schema change
    it describes the newest schema. Protobuf can decode older messages with a
    newer schema, as long as the schema was changed compatibly (see the
    .proto file).
    """
    target = OUTPUT_DIR / "_schema" / "descriptor_set.desc"
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(DESCRIPTOR_SET, target.with_name(".descriptor_set.desc.tmp"))
    os.replace(target.with_name(".descriptor_set.desc.tmp"), target)
    print(f"Wrote descriptor set to {target}")


def to_record(msg) -> kafka_record_pb2.KafkaRecord:
    headers = dict(msg.headers() or [])
    record = kafka_record_pb2.KafkaRecord(
        topic=msg.topic(),
        partition=msg.partition(),
        offset=msg.offset(),
        key=(msg.key() or b"").decode(),
        message_type=headers.get("message-type", MESSAGE_TYPE.encode()).decode(),
        value=msg.value(),
    )
    timestamp_type, timestamp_ms = msg.timestamp()
    if timestamp_type != TIMESTAMP_NOT_AVAILABLE:
        record.timestamp.FromMilliseconds(timestamp_ms)
    record.ingested_at.GetCurrentTime()
    return record


def record_date(record: kafka_record_pb2.KafkaRecord) -> date:
    timestamp = record.timestamp if record.HasField("timestamp") else record.ingested_at
    return timestamp.ToDatetime(tzinfo=timezone.utc).date()


def write_file(batch: kafka_record_pb2.KafkaRecordBatch, day: date) -> None:
    directory = OUTPUT_DIR / f"date={day.isoformat()}"
    directory.mkdir(parents=True, exist_ok=True)
    name = f"part-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}.pb"
    tmp = directory / f".{name}.tmp"
    tmp.write_bytes(batch.SerializeToString())
    os.replace(tmp, directory / name)
    print(f"Wrote {len(batch.records)} records to {directory / name}")


def main() -> None:
    publish_descriptor_set()
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

    print(f"Consuming '{TOPIC}' from {BOOTSTRAP_SERVERS} as group '{GROUP_ID}', writing protobuf files to {OUTPUT_DIR}")
    # One batch per date: each becomes one file.
    batches: dict[date, kafka_record_pb2.KafkaRecordBatch] = defaultdict(kafka_record_pb2.KafkaRecordBatch)
    last_flush = time.monotonic()

    def flush() -> None:
        nonlocal last_flush
        if batches:
            for day, batch in batches.items():
                write_file(batch, day)
            batches.clear()
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
                record = to_record(msg)
                batches[record_date(record)].records.append(record)

            buffered = sum(len(b.records) for b in batches.values())
            if buffered >= FLUSH_MAX_MESSAGES or time.monotonic() - last_flush >= FLUSH_INTERVAL_SECONDS:
                flush()
        flush()
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
