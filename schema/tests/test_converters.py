"""Tests for the schema-driven converters.

    python schema/generate.py
    python -m pytest schema/tests
"""

import sys
from pathlib import Path

from google.protobuf import descriptor_pb2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from telemetry_schema import elk, message_class  # noqa: E402
from telemetry_schema.registry import DESCRIPTOR_SET  # noqa: E402

VehicleTelemetry = message_class("vehicle.v1.VehicleTelemetry")


def sample() -> VehicleTelemetry:
    message = VehicleTelemetry(
        message_id="m-1",
        vehicle_id="vehicle-001",
        vehicle_type="TRUCK",
        position={"lat": 59.33, "lon": 18.07},
        speed_kmh=42.5,
        events=["HARSH_BRAKING", "SPEEDING"],
    )
    message.timestamp.FromJsonString("2026-09-29T08:00:00.123Z")
    return message


def test_elk_document_uses_field_names_enum_names_and_defaults():
    document = elk.to_document(sample())
    assert document["vehicle_type"] == "TRUCK"
    assert document["events"] == ["HARSH_BRAKING", "SPEEDING"]
    assert document["timestamp"] == "2026-09-29T08:00:00.123Z"
    assert document["position"] == {"lat": 59.33, "lon": 18.07}
    # Fields with default values are included, so all documents look alike.
    assert document["rpm"] == 0
    assert document["dtc_codes"] == []
    assert document["check_engine_light"] is False


def test_elk_mapping_follows_types_and_custom_option():
    properties = elk.index_properties(VehicleTelemetry.DESCRIPTOR)
    assert properties["position"] == {"type": "geo_point"}  # (elk.field_type) option
    assert properties["timestamp"] == {"type": "date"}  # google.protobuf.Timestamp
    assert properties["vehicle_type"] == {"type": "keyword"}  # enum
    assert properties["rpm"] == {"type": "integer"}
    assert properties["tire_pressure_kpa"]["properties"]["rear_left"] == {"type": "double"}
    # Every field in the message is mapped.
    assert set(properties) == {f.name for f in VehicleTelemetry.DESCRIPTOR.fields}


def test_record_batch_round_trips_message_bytes():
    KafkaRecordBatch = message_class("kafka.v1.KafkaRecordBatch")
    batch = KafkaRecordBatch()
    batch.records.add(offset=7, message_type="vehicle.v1.VehicleTelemetry", value=sample().SerializeToString())
    decoded = KafkaRecordBatch.FromString(batch.SerializeToString())
    assert VehicleTelemetry.FromString(decoded.records[0].value) == sample()


def test_descriptor_set_contains_all_messages_and_imports():
    descriptor_set = descriptor_pb2.FileDescriptorSet.FromString(DESCRIPTOR_SET.read_bytes())
    files = {f.name for f in descriptor_set.file}
    assert {"vehicle/v1/vehicle_telemetry.proto", "kafka/v1/kafka_record.proto"} <= files
    # Imports are included, so Spark can resolve google.protobuf.Timestamp.
    assert "google/protobuf/timestamp.proto" in files
