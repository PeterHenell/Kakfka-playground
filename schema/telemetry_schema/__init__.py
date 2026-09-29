"""Runtime helpers shared by the producer and the consumers.

The code for the messages themselves is generated from the .proto files by
generate.py (into schema/generated/). The Elasticsearch converter doesn't
know about any specific field: it walks the protobuf descriptors, so it
keeps working when the schema changes.
"""

from .registry import message_class

__all__ = ["message_class"]
