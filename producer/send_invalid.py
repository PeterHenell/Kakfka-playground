"""Publishes a few invalid messages, to see the generated dbt source tests fail.

Each message breaks the schema in a different way:

    garbage        bytes that aren't a protobuf message at all
    unknown_enum   a vehicle_type number the schema doesn't define
    out_of_range   speed_kmh = 999 (the schema allows 0..250)
    bad_pattern    vehicle_id doesn't match ^vehicle-[0-9]{3}$, a DTC code
                   doesn't match ^[PCBU][0-9]{4}$
    missing        no timestamp, position or tire pressures (required)

After the protobuf consumer has written them to a file (up to a minute),
`docker compose run --rm dbt build` reports the failing tests and skips the
models that depend on the source.

    python producer/send_invalid.py

The invalid messages stay in Kafka and in the archive. To get a clean state
again, stop everything, and delete the data (docker compose down -v, and the
./data folder).
"""

import os
import sys
import uuid
from pathlib import Path

from confluent_kafka import Producer

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "schema"))
from telemetry_schema.registry import load_generated_modules  # noqa: E402

load_generated_modules()
from vehicle.v1 import vehicle_telemetry_pb2 as pb  # noqa: E402

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = os.getenv("KAFKA_TOPIC", "vehicle-telemetry")


def valid() -> pb.VehicleTelemetry:
    message = pb.VehicleTelemetry(
        message_id=str(uuid.uuid4()),
        vehicle_id="vehicle-999",
        vin="INVALIDTESTVIN001",
        vehicle_type=pb.CAR,
        position=pb.Position(lat=59.33, lon=18.07),
        speed_kmh=50,
        tire_pressure_kpa=pb.TirePressure(front_left=230, front_right=230, rear_left=230, rear_right=230),
    )
    message.timestamp.GetCurrentTime()
    return message


def invalid_messages() -> dict[str, bytes]:
    unknown_enum = valid()
    unknown_enum.vehicle_type = 42  # proto3 enums accept unknown numbers
    out_of_range = valid()
    out_of_range.speed_kmh = 999
    bad_pattern = valid()
    bad_pattern.vehicle_id = "truck #7"
    bad_pattern.dtc_codes.append("OVERHEAT")
    missing = pb.VehicleTelemetry(message_id=str(uuid.uuid4()), vehicle_id="vehicle-998", vin="INVALIDTESTVIN002", vehicle_type=pb.BUS)
    return {
        "garbage": b"\xff\xff\xff\xff this is not protobuf",
        "unknown_enum": unknown_enum.SerializeToString(),
        "out_of_range": out_of_range.SerializeToString(),
        "bad_pattern": bad_pattern.SerializeToString(),
        "missing": missing.SerializeToString(),
    }


def main() -> None:
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    for name, value in invalid_messages().items():
        producer.produce(TOPIC, key=f"invalid-{name}", value=value, headers={"message-type": pb.VehicleTelemetry.DESCRIPTOR.full_name})
        print(f"Sent invalid message: {name}")
    producer.flush(10)


if __name__ == "__main__":
    main()
