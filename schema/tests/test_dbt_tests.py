"""Tests for the dbt source test generator.

    python schema/generate.py
    python -m pytest schema/tests
"""

import os
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from telemetry_schema import dbt_tests, message_class  # noqa: E402

VehicleTelemetry = message_class("vehicle.v1.VehicleTelemetry")
KafkaRecord = message_class("kafka.v1.KafkaRecord")
SOURCES_FILE = Path(__file__).resolve().parents[2] / "dbt" / "src" / "models" / "sources" / "raw_vehicle_telemetry.yml"


def generated() -> str:
    return dbt_tests.source_yaml(VehicleTelemetry.DESCRIPTOR, KafkaRecord.DESCRIPTOR, "vehicle_telemetry", "vehicle/v1/vehicle_telemetry.proto")


def value_tests() -> dict:
    source = yaml.safe_load(generated())["sources"][0]
    records = next(t for t in source["tables"] if t["name"] == "vehicle_telemetry_records")
    value = next(c for c in records["columns"] if c["name"] == "value")
    return {next(iter(t.values()))["name"]: next(iter(t.values())) for t in value["data_tests"]}


def test_every_message_is_checked_to_decode():
    assert value_tests()["protobuf_decodes__vehicle_telemetry"]["arguments"]["message_type"] == "vehicle.v1.VehicleTelemetry"


def test_protovalidate_rules_become_sql_conditions():
    tests = value_tests()
    speed = tests["protobuf_valid__vehicle_telemetry__speed_kmh"]["arguments"]["condition"]
    assert "coalesce(m.`speed_kmh`, 0) >= 0.0" in speed
    assert "coalesce(m.`speed_kmh`, 0) <= 250.0" in speed
    # required on a message field, and rules of the fields nested in it
    position = tests["protobuf_valid__vehicle_telemetry__position"]["arguments"]["condition"]
    assert "m.`position` is not null" in position
    assert "m.`position`.`lat`" in position
    # repeated items
    dtc = tests["protobuf_valid__vehicle_telemetry__dtc_codes"]["arguments"]["condition"]
    assert "forall(m.`dtc_codes`, x0 -> x0 rlike '^[PCBU][0-9]{4}$')" in dtc


def test_enum_values_must_be_defined_even_without_rules():
    condition = value_tests()["protobuf_valid__vehicle_telemetry__vehicle_type"]["arguments"]["condition"]
    assert "'UNKNOWN_ENUM_VALUE_'" in condition


def test_fields_without_rules_get_no_test():
    assert "protobuf_valid__vehicle_telemetry__check_engine_light" not in value_tests()


def test_sql_string_escapes_backslashes_and_quotes():
    assert dbt_tests.sql_string("a\\d'b") == "'a\\\\d\\'b'"


def test_committed_dbt_sources_are_up_to_date():
    """The generated file is committed; regenerate it after changing a .proto file."""
    if os.getenv("ARCHIVE_NAME", "vehicle_telemetry") != "vehicle_telemetry" or not SOURCES_FILE.exists():
        return
    assert SOURCES_FILE.read_text() == generated(), "Run: python schema/generate.py"
