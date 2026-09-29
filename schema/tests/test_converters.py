"""Tests for the schema-driven converters.

    python schema/generate.py
    python -m pytest schema/tests
"""

import sys
from pathlib import Path

import protarrow
import pyarrow as pa

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from telemetry_schema import elk, message_class, parquet  # noqa: E402

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


def test_parquet_table_has_expected_types_and_round_trips():
    table = parquet.to_table([sample()], VehicleTelemetry)
    assert table.schema.field("timestamp").type == pa.timestamp("us", tz="UTC")
    assert table.schema.field("vehicle_type").type == pa.string()
    assert table.column("events").to_pylist() == [["HARSH_BRAKING", "SPEEDING"]]
    assert protarrow.table_to_messages(table, VehicleTelemetry) == [sample()]
