"""Protobuf -> Apache Arrow (and from there Parquet), using protarrow.

protarrow derives the Arrow schema from the message descriptor: nested
messages become structs, repeated fields become lists, Timestamps become
timestamp columns. The config below picks the representations that Spark
and Databricks read best.
"""

from typing import Iterable

import pyarrow as pa
import protarrow
from google.protobuf.message import Message

CONFIG = protarrow.ProtarrowConfig(
    # Enum values as their names ("CAR") instead of their numbers (1).
    enum_type=pa.string(),
    # Spark can't read nanosecond timestamps from Parquet; microseconds it can.
    timestamp_type=pa.timestamp("us", tz="UTC"),
)


def arrow_schema(message_type: type) -> pa.Schema:
    return protarrow.message_type_to_schema(message_type, CONFIG)


def to_table(messages: Iterable[Message], message_type: type) -> pa.Table:
    return protarrow.messages_to_table(messages, message_type, CONFIG)
