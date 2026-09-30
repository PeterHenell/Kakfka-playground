"""Control panel for the Kafka playground.

A small web app (http://localhost:8090) that shows how many messages have
been produced, consumed and not consumed yet, and pauses/resumes the
producer and the consumers.

- The numbers come from Kafka itself, through the admin API:
    produced      the sum of the topic's end offsets (the next offset each
                  partition will write)
    consumed      the sum of a consumer group's committed offsets
    not consumed  the difference: the group's *lag*
  A background thread takes a snapshot every SAMPLE_SECONDS and keeps the
  last HISTORY_MINUTES, for the rates and the lag chart.
- Pausing writes the desired state to the compacted `playground-control`
  topic; the components follow that topic (see common/kafka_control.py).

Configuration (environment variables):
    KAFKA_BOOTSTRAP_SERVERS  default: localhost:9094
    KAFKA_TOPIC              default: vehicle-telemetry
    SAMPLE_SECONDS           default: 2
    HISTORY_MINUTES          default: 10
    PORT                     default: 8090
"""

import os
import sys
import threading
import time
from collections import deque
from pathlib import Path

from confluent_kafka import ConsumerGroupTopicPartitions, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec
from flask import Flask, abort, jsonify, send_from_directory

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "common"))
from kafka_control import ControlState, set_paused  # noqa: E402

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = os.getenv("KAFKA_TOPIC", "vehicle-telemetry")
SAMPLE_SECONDS = float(os.getenv("SAMPLE_SECONDS", "2"))
HISTORY_MINUTES = float(os.getenv("HISTORY_MINUTES", "10"))
PORT = int(os.getenv("PORT", "8090"))
RATE_WINDOW_SECONDS = 10

# The components that can be paused, and the consumer group each one uses.
COMPONENTS = [
    {"name": "producer", "label": "Producer", "description": "Simulates the vehicles", "group": None},
    {"name": "consumer", "label": "ELK consumer", "description": "Writes to Logstash / Elasticsearch", "group": "elk-writer"},
    {
        "name": "protobuf-consumer",
        "label": "Protobuf consumer",
        "description": "Archives protobuf files for Spark",
        "group": "protobuf-writer",
    },
]
COMPONENT_GROUPS = {c["group"]: c["name"] for c in COMPONENTS if c["group"]}

app = Flask(__name__, static_folder="static")
admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
control_lock = threading.Lock()
control: ControlState | None = None

snapshot_lock = threading.Lock()
history: deque = deque(maxlen=int(HISTORY_MINUTES * 60 / SAMPLE_SECONDS))
last_error: str | None = None


def take_snapshot() -> dict:
    """Reads the topic's offsets and every consumer group's committed offsets."""
    metadata = admin.list_topics(TOPIC, timeout=10).topics[TOPIC]
    if metadata.error:
        raise RuntimeError(f"Topic '{TOPIC}': {metadata.error}")
    partitions = sorted(metadata.partitions)
    tps = [TopicPartition(TOPIC, p) for p in partitions]
    earliest = {tp.partition: f.result().offset for tp, f in admin.list_offsets({tp: OffsetSpec.earliest() for tp in tps}).items()}
    latest = {tp.partition: f.result().offset for tp, f in admin.list_offsets({tp: OffsetSpec.latest() for tp in tps}).items()}

    group_ids = {g.group_id for g in admin.list_consumer_groups().result().valid} | set(COMPONENT_GROUPS)
    groups = {}
    for group_id in sorted(group_ids):
        try:
            result = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group_id)])[group_id].result()
        except Exception:
            continue
        committed = {tp.partition: tp.offset for tp in result.topic_partitions if tp.topic == TOPIC}
        if not committed and group_id not in COMPONENT_GROUPS:
            continue  # a group that doesn't read this topic
        rows = []
        for p in partitions:
            offset = committed.get(p, -1)
            # No committed offset yet: the group starts at the earliest offset.
            start = offset if offset >= 0 else earliest[p]
            rows.append({"partition": p, "committed": offset if offset >= 0 else None, "end": latest[p], "lag": latest[p] - start})
        groups[group_id] = {
            "consumed": sum(r["committed"] or 0 for r in rows),
            "lag": sum(r["lag"] for r in rows),
            "partitions": rows,
        }

    if groups:
        for group_id, future in admin.describe_consumer_groups(list(groups)).items():
            try:
                description = future.result()
                groups[group_id]["state"] = description.state.name
                groups[group_id]["members"] = len(description.members)
            except Exception:
                groups[group_id]["state"] = "UNKNOWN"
                groups[group_id]["members"] = 0

    return {
        "t": time.time(),
        "produced": sum(latest.values()),
        "retained": sum(latest[p] - earliest[p] for p in partitions),
        "partitions": [{"partition": p, "earliest": earliest[p], "end": latest[p]} for p in partitions],
        "groups": groups,
    }


def sampler() -> None:
    global last_error
    while True:
        try:
            snapshot = take_snapshot()
            with snapshot_lock:
                history.append(snapshot)
                last_error = None
        except Exception as e:  # Kafka not up yet, topic not created yet, ...
            last_error = str(e)
        time.sleep(SAMPLE_SECONDS)


def rate(values: list[tuple[float, int]]) -> float:
    """Messages per second over the last RATE_WINDOW_SECONDS."""
    if len(values) < 2:
        return 0.0
    now_t, now_v = values[-1]
    for t, v in values:
        if now_t - t <= RATE_WINDOW_SECONDS:
            return (now_v - v) / (now_t - t) if now_t > t else 0.0
    return 0.0


def control_states() -> dict:
    global control
    with control_lock:
        if control is None:
            control = ControlState(BOOTSTRAP_SERVERS)
        control.refresh()
        return dict(control.states)


@app.get("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.get("/api/overview")
def overview():
    with snapshot_lock:
        snapshots = list(history)
        error = last_error
    try:
        states = control_states()
    except Exception as e:
        states, error = {}, error or str(e)
    if not snapshots:
        return jsonify({"topic": TOPIC, "ready": False, "error": error or "Waiting for the first sample"})

    latest = snapshots[-1]
    groups = []
    for group_id, group in latest["groups"].items():
        series = [(s["t"], s["groups"][group_id]["consumed"]) for s in snapshots if group_id in s["groups"]]
        groups.append(
            {
                "id": group_id,
                "component": COMPONENT_GROUPS.get(group_id),
                "state": group.get("state", "UNKNOWN"),
                "members": group.get("members", 0),
                "consumed": group["consumed"],
                "consumed_rate": rate(series),
                "lag": group["lag"],
                "partitions": group["partitions"],
            }
        )
    components = [
        {
            **component,
            "paused": bool(states.get(component["name"], {}).get("paused", False)),
            "updated_at": states.get(component["name"], {}).get("updated_at"),
        }
        for component in COMPONENTS
    ]
    return jsonify(
        {
            "topic": TOPIC,
            "ready": True,
            "error": error,
            "sampled_at": latest["t"],
            "sample_seconds": SAMPLE_SECONDS,
            "produced": latest["produced"],
            "produced_rate": rate([(s["t"], s["produced"]) for s in snapshots]),
            "retained": latest["retained"],
            "partitions": latest["partitions"],
            "groups": groups,
            "components": components,
            "history": [
                {"t": s["t"], "produced": s["produced"], "lag": {g: v["lag"] for g, v in s["groups"].items()}}
                for s in snapshots
            ],
        }
    )


@app.post("/api/components/<name>/<action>")
def control_component(name: str, action: str):
    if name not in {c["name"] for c in COMPONENTS} or action not in ("pause", "resume"):
        abort(404)
    set_paused(producer, name, action == "pause")
    return jsonify({"name": name, "paused": action == "pause"})


if __name__ == "__main__":
    threading.Thread(target=sampler, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, threaded=True)
