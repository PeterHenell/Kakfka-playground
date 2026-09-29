"""Reads vehicle telemetry from Kafka and ships it to the ELK stack.

Messages are read in batches and POSTed to Logstash's HTTP input, which
indexes them into Elasticsearch (and from there you explore them in Kibana).

Offsets are committed manually, and only after Logstash has accepted the
batch. If the consumer crashes before committing, the batch is read again
after a restart: this is "at-least-once" delivery. Logstash uses each
message's message_id as the Elasticsearch document id, so a re-delivered
message overwrites itself instead of creating a duplicate.

Configuration (environment variables):
    KAFKA_BOOTSTRAP_SERVERS  default: localhost:9094
    KAFKA_TOPIC              default: vehicle-telemetry
    KAFKA_GROUP_ID           default: elk-writer
    LOGSTASH_URL             default: http://localhost:8081
    BATCH_SIZE               default: 100
"""

import json
import os
import signal
import time

import requests
from confluent_kafka import Consumer, KafkaError

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = os.getenv("KAFKA_TOPIC", "vehicle-telemetry")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "elk-writer")
LOGSTASH_URL = os.getenv("LOGSTASH_URL", "http://localhost:8081")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))


def on_assign(consumer, partitions):
    print(f"Assigned partitions: {[p.partition for p in partitions]}")


def on_revoke(consumer, partitions):
    print(f"Revoked partitions: {[p.partition for p in partitions]}")


def send_to_logstash(documents: list[dict]) -> None:
    """POST the batch to Logstash, retrying until it succeeds."""
    delay = 1
    while True:
        try:
            response = requests.post(LOGSTASH_URL, json=documents, timeout=10)
            response.raise_for_status()
            return
        except requests.RequestException as e:
            print(f"Logstash not accepting data ({e}), retrying in {delay}s")
            time.sleep(delay)
            delay = min(delay * 2, 30)


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            # Consumers with the same group.id share the partitions of a topic
            # between them. Start a second consumer with the same group id and
            # watch the partitions get rebalanced.
            "group.id": GROUP_ID,
            # Where to start when the group has no committed offset yet.
            "auto.offset.reset": "earliest",
            # We commit ourselves, after the data is safely stored.
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)

    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    print(f"Consuming '{TOPIC}' from {BOOTSTRAP_SERVERS} as group '{GROUP_ID}', writing to {LOGSTASH_URL}")
    total = 0
    try:
        while running:
            messages = consumer.consume(num_messages=BATCH_SIZE, timeout=1.0)
            if not messages:
                continue

            documents = []
            for msg in messages:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"Kafka error: {msg.error()}")
                    continue
                try:
                    document = json.loads(msg.value())
                except json.JSONDecodeError:
                    print(f"Skipping invalid JSON at partition {msg.partition()} offset {msg.offset()}")
                    continue
                # Keep track of where in Kafka the document came from.
                document["kafka"] = {"topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset()}
                documents.append(document)

            if documents:
                send_to_logstash(documents)
            # Commit the offsets of everything consume() returned.
            consumer.commit(asynchronous=False)

            total += len(documents)
            print(f"Stored {len(documents)} messages (total {total})")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
