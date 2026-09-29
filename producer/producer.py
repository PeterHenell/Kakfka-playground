"""Simulates a fleet of vehicles and publishes their telemetry to Kafka.

Each vehicle drives around (roughly) Stockholm, and every tick it sends one
message with its position, speed, engine temperature and so on.

Messages are serialized with Protocol Buffers, using the VehicleTelemetry
message defined in schema/proto/vehicle/v1/vehicle_telemetry.proto. Each
message carries a `message-type` header with the protobuf type name, so
consumers know how to decode it.

Messages are keyed by vehicle_id. Kafka hashes the key to pick a partition, so
all messages from the same vehicle land in the same partition and are read in
the order they were produced.

Before running this outside Docker, generate the protobuf code:

    python schema/generate.py

Configuration (environment variables):
    KAFKA_BOOTSTRAP_SERVERS  default: localhost:9094
    KAFKA_TOPIC              default: vehicle-telemetry
    KAFKA_PARTITIONS         default: 3   (only used when creating the topic)
    NUM_VEHICLES             default: 5
    INTERVAL_SECONDS         default: 1.0 (time between ticks)
"""

import math
import os
import random
import signal
import sys
import time
import uuid
from pathlib import Path

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "schema"))
from telemetry_schema.registry import load_generated_modules  # noqa: E402

load_generated_modules()
from vehicle.v1 import vehicle_telemetry_pb2 as pb  # noqa: E402

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = os.getenv("KAFKA_TOPIC", "vehicle-telemetry")
PARTITIONS = int(os.getenv("KAFKA_PARTITIONS", "3"))
NUM_VEHICLES = int(os.getenv("NUM_VEHICLES", "5"))
INTERVAL_SECONDS = float(os.getenv("INTERVAL_SECONDS", "1.0"))

# Vehicles start somewhere around central Stockholm.
START_LAT, START_LON = 59.3293, 18.0686
VEHICLE_TYPES = [pb.CAR, pb.VAN, pb.TRUCK, pb.BUS]
EARTH_RADIUS_M = 6_371_000


class Vehicle:
    """Very small physics-ish model of a vehicle. Good enough for fake data."""

    def __init__(self, index: int):
        self.vehicle_id = f"vehicle-{index:03d}"
        # Things that don't change for a vehicle (VIN, type, faults) come from
        # a random generator seeded with the vehicle number, so vehicle-001 is
        # the same vehicle every time the producer starts.
        fixed = random.Random(index)
        self.vin = "".join(fixed.choices("ABCDEFGHJKLMNPRSTUVWXYZ0123456789", k=17))
        self.vehicle_type = fixed.choice(VEHICLE_TYPES)
        self.max_speed = 90 if self.vehicle_type in (pb.TRUCK, pb.BUS) else 130

        self.lat = START_LAT + random.uniform(-0.05, 0.05)
        self.lon = START_LON + random.uniform(-0.08, 0.08)
        self.heading = random.uniform(0, 360)
        self.speed = 0.0
        self.target_speed = random.uniform(30, self.max_speed)
        self.engine_temp = random.uniform(20, 40)  # starts cold
        self.fuel_level = random.uniform(40, 100)
        self.odometer = fixed.uniform(5_000, 250_000)
        self.battery_voltage = 12.6
        self.tire_pressure = {t: random.uniform(220, 240) for t in ("front_left", "front_right", "rear_left", "rear_right")}
        # Some vehicles have a slow leak or a bad cooling system, so there is
        # something interesting to find in Kibana.
        self.leaking_tire = fixed.choice([None, None, None, "rear_left"])
        self.cooling_problem = fixed.random() < 0.2

    def tick(self, dt: float) -> pb.VehicleTelemetry:
        events = []

        # --- speed: drift towards a target speed, sometimes pick a new one ---
        if random.random() < 0.05:
            self.target_speed = random.choice([0, random.uniform(20, self.max_speed)])
        previous_speed = self.speed
        self.speed += (self.target_speed - self.speed) * 0.2 + random.gauss(0, 1.5)
        self.speed = max(0.0, min(self.speed, self.max_speed))
        acceleration = (self.speed - previous_speed) / 3.6 / dt  # m/s^2
        if acceleration < -4:
            events.append(pb.HARSH_BRAKING)
        elif acceleration > 3.5:
            events.append(pb.HARSH_ACCELERATION)
        if self.speed > 110:
            events.append(pb.SPEEDING)

        # --- position: move along the heading ---
        self.heading = (self.heading + random.gauss(0, 8)) % 360
        distance_m = self.speed / 3.6 * dt
        self.lat += math.degrees(distance_m * math.cos(math.radians(self.heading)) / EARTH_RADIUS_M)
        self.lon += math.degrees(
            distance_m * math.sin(math.radians(self.heading)) / (EARTH_RADIUS_M * math.cos(math.radians(self.lat)))
        )
        self.odometer += distance_m / 1000

        # --- engine ---
        if self.speed < 1:
            gear, rpm = 0, random.uniform(700, 900)  # idling
        else:
            gear = min(6, 1 + int(self.speed // 22))
            rpm = 800 + (self.speed - (gear - 1) * 22) * 110 + random.gauss(0, 50)
        operating_temp = 110 if self.cooling_problem else 90
        self.engine_temp += (operating_temp - self.engine_temp) * 0.02 + rpm / 10_000 + random.gauss(0, 0.3)
        if self.engine_temp > 105:
            events.append(pb.ENGINE_OVERHEATING)

        # --- consumables ---
        self.fuel_level = max(0.0, self.fuel_level - (0.0005 + rpm / 5_000_000) * dt * 10)
        if self.fuel_level < 10:
            events.append(pb.LOW_FUEL)
        if self.fuel_level == 0 or random.random() < 0.0005:
            self.fuel_level = random.uniform(80, 100)  # visited a gas station
            events.append(pb.REFUELED)
        self.battery_voltage = (14.2 if rpm > 1000 else 13.6) + random.gauss(0, 0.1)
        for tire in self.tire_pressure:
            self.tire_pressure[tire] += random.gauss(0, 0.2)
        if self.leaking_tire:
            # Loses pressure until the tire is flat.
            self.tire_pressure[self.leaking_tire] = max(0.0, self.tire_pressure[self.leaking_tire] - 0.05 * dt)
            if self.tire_pressure[self.leaking_tire] < 180:
                events.append(pb.LOW_TIRE_PRESSURE)

        dtc_codes = []
        if self.engine_temp > 105:
            dtc_codes.append("P0217")  # engine overtemperature
        if self.leaking_tire and self.tire_pressure[self.leaking_tire] < 180:
            dtc_codes.append("C0750")  # tire pressure sensor low

        message = pb.VehicleTelemetry(
            message_id=str(uuid.uuid4()),
            vehicle_id=self.vehicle_id,
            vin=self.vin,
            vehicle_type=self.vehicle_type,
            position=pb.Position(lat=round(self.lat, 6), lon=round(self.lon, 6)),
            # % 360 again: 359.96 would round up to 360.0, which is not a valid heading.
            heading_deg=round(self.heading, 1) % 360,
            speed_kmh=round(self.speed, 1),
            rpm=round(rpm),
            gear=gear,
            engine_temp_c=round(self.engine_temp, 1),
            fuel_level_pct=round(self.fuel_level, 2),
            battery_voltage=round(self.battery_voltage, 2),
            odometer_km=round(self.odometer, 3),
            tire_pressure_kpa=pb.TirePressure(**{t: round(p, 1) for t, p in self.tire_pressure.items()}),
            check_engine_light=bool(dtc_codes),
            dtc_codes=dtc_codes,
            events=events,
        )
        message.timestamp.GetCurrentTime()
        return message


def ensure_topic(admin: AdminClient) -> None:
    """Create the topic if it doesn't exist (auto-creation is disabled on the broker)."""
    if TOPIC in admin.list_topics(timeout=10).topics:
        return
    futures = admin.create_topics([NewTopic(TOPIC, num_partitions=PARTITIONS, replication_factor=1)])
    try:
        futures[TOPIC].result()
        print(f"Created topic '{TOPIC}' with {PARTITIONS} partitions")
    except KafkaException as e:
        # Another process may have created it at the same time.
        print(f"Topic creation: {e}")


def delivery_report(err, msg) -> None:
    """Called once per message when the broker has acknowledged (or rejected) it."""
    if err is not None:
        print(f"Delivery failed for key={msg.key()}: {err}")


def main() -> None:
    config = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        # Wait for all in-sync replicas to acknowledge, and let the producer
        # de-duplicate its own retries.
        "acks": "all",
        "enable.idempotence": True,
        # Batch messages for up to 50ms before sending them.
        "linger.ms": 50,
        "compression.type": "lz4",
    }
    ensure_topic(AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS}))
    producer = Producer(config)
    vehicles = [Vehicle(i + 1) for i in range(NUM_VEHICLES)]

    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    print(f"Producing telemetry for {NUM_VEHICLES} vehicles to '{TOPIC}' on {BOOTSTRAP_SERVERS} every {INTERVAL_SECONDS}s")
    sent = 0
    while running:
        for vehicle in vehicles:
            message = vehicle.tick(INTERVAL_SECONDS)
            producer.produce(
                TOPIC,
                key=vehicle.vehicle_id,
                value=message.SerializeToString(),
                # Tells consumers which protobuf message type the bytes are.
                headers={"message-type": message.DESCRIPTOR.full_name},
                on_delivery=delivery_report,
            )
            sent += 1
        # Serve delivery callbacks from previous produce() calls.
        producer.poll(0)
        if sent % (NUM_VEHICLES * 10) == 0:
            print(f"Sent {sent} messages")
        time.sleep(INTERVAL_SECONDS)

    print("Flushing outstanding messages...")
    producer.flush(10)


if __name__ == "__main__":
    main()
