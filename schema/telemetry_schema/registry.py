"""Finds the generated protobuf classes by their full name.

Importing a generated *_pb2 module registers its messages in protobuf's
default descriptor pool. We import all of them, so a consumer can look up a
message type by the name it gets from a Kafka header, e.g.
"vehicle.v1.VehicleTelemetry".
"""

import importlib
import sys
from functools import cache
from pathlib import Path

from google.protobuf import descriptor_pool, message_factory

GENERATED_DIR = Path(__file__).resolve().parent.parent / "generated"


@cache
def load_generated_modules() -> None:
    if not GENERATED_DIR.is_dir():
        raise RuntimeError(f"No generated code in {GENERATED_DIR}. Run: python schema/generate.py")
    # The generated modules import each other by their path relative to the
    # proto root (e.g. `from elk import options_pb2`), so that root must be
    # on the import path.
    sys.path.insert(0, str(GENERATED_DIR))
    for path in sorted(GENERATED_DIR.rglob("*_pb2.py")):
        module = ".".join(path.relative_to(GENERATED_DIR).with_suffix("").parts)
        importlib.import_module(module)


def message_class(full_name: str) -> type:
    """Returns the generated class for a message, e.g. message_class("vehicle.v1.VehicleTelemetry")."""
    load_generated_modules()
    descriptor = descriptor_pool.Default().FindMessageTypeByName(full_name)
    return message_factory.GetMessageClass(descriptor)
