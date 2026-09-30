"""Pause and resume playground components through a Kafka topic.

The control panel writes the desired state of each component to the
compacted topic `playground-control`:

    key   = component name, e.g. "producer", "consumer", "protobuf-consumer"
    value = {"paused": true, "updated_at": "2026-09-30T08:00:00+00:00"}

Each component follows that topic with a ControlState and checks
`paused()` in its main loop. Because the topic is compacted, Kafka keeps at
least the latest value per key, so a component that (re)starts reads the
current state from the beginning of the topic.

Configuration (environment variables):
    CONTROL_TOPIC   default: playground-control
"""

import json
import os
import uuid
from datetime import datetime, timezone

from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

CONTROL_TOPIC = os.getenv("CONTROL_TOPIC", "playground-control")


def ensure_control_topic(bootstrap_servers: str) -> None:
    """Creates the compacted control topic if it doesn't exist yet."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    if CONTROL_TOPIC in admin.list_topics(timeout=10).topics:
        return
    topic = NewTopic(CONTROL_TOPIC, num_partitions=1, replication_factor=1, config={"cleanup.policy": "compact"})
    try:
        admin.create_topics([topic])[CONTROL_TOPIC].result()
    except KafkaException:
        pass  # created by another component at the same time


class ControlState:
    """Follows the control topic and keeps the latest state of every component."""

    def __init__(self, bootstrap_servers: str, component: str | None = None):
        self.component = component
        self.states: dict[str, dict] = {}
        ensure_control_topic(bootstrap_servers)
        # No consumer group: every reader reads the whole topic itself, and
        # never commits offsets. (librdkafka still wants a group.id.)
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": f"control-reader-{uuid.uuid4().hex[:8]}",
                "enable.auto.commit": False,
            }
        )
        self._consumer.assign([TopicPartition(CONTROL_TOPIC, 0, OFFSET_BEGINNING)])
        self._load()

    def _load(self) -> None:
        """Reads the topic up to its current end, so the state is known before starting."""
        _, high = self._consumer.get_watermark_offsets(TopicPartition(CONTROL_TOPIC, 0), timeout=10)
        position = 0
        while position < high:
            msg = self._consumer.poll(1.0)
            if msg is None:
                break
            if not msg.error():
                self._apply(msg)
                position = msg.offset() + 1

    def _apply(self, msg) -> None:
        if msg.key() is None:
            return
        key = msg.key().decode()
        if msg.value() is None:  # a tombstone deletes the key
            self.states.pop(key, None)
        else:
            self.states[key] = json.loads(msg.value())

    def refresh(self) -> None:
        """Applies the control messages that arrived since the last call (doesn't block)."""
        while (msg := self._consumer.poll(0)) is not None:
            if not msg.error():
                self._apply(msg)

    def paused(self, component: str | None = None) -> bool:
        self.refresh()
        return bool(self.states.get(component or self.component, {}).get("paused", False))

    def close(self) -> None:
        self._consumer.close()


def set_paused(producer: Producer, component: str, paused: bool) -> None:
    """Publishes the desired state of a component (used by the control panel)."""
    value = {"paused": paused, "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    producer.produce(CONTROL_TOPIC, key=component, value=json.dumps(value))
    producer.flush(10)


class ConsumerPauser:
    """Pauses and resumes a Kafka consumer as the control state says.

    Uses Kafka's own consumer.pause()/resume(): a paused consumer keeps
    polling (so it stays in its consumer group and no rebalance happens), but
    gets no messages from its paused partitions. Call apply() in every loop
    iteration: partitions assigned after a rebalance are paused too.
    """

    def __init__(self, control: ControlState):
        self.control = control
        self.was_paused = False

    def apply(self, consumer: Consumer) -> bool:
        paused = self.control.paused()
        assignment = consumer.assignment()
        if paused and assignment:
            consumer.pause(assignment)
        if paused != self.was_paused:
            print(f"{self.control.component}: {'paused' if paused else 'resumed'} by the control panel")
            if not paused and assignment:
                consumer.resume(assignment)
        self.was_paused = paused
        return paused
