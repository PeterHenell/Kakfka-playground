"""Protobuf -> Elasticsearch.

- to_document() turns a message into a JSON-ready dict, using protobuf's
  own json_format module.
- index_properties() builds the Elasticsearch mapping for a message type
  from its descriptor. generate.py uses it to write the index template.
"""

from google.protobuf import json_format
from google.protobuf.descriptor import Descriptor, FieldDescriptor
from google.protobuf.message import Message

from .registry import load_generated_modules

# Default Elasticsearch type for each protobuf scalar type.
SCALAR_TYPES = {
    FieldDescriptor.TYPE_DOUBLE: "double",
    FieldDescriptor.TYPE_FLOAT: "float",
    FieldDescriptor.TYPE_INT32: "integer",
    FieldDescriptor.TYPE_SINT32: "integer",
    FieldDescriptor.TYPE_SFIXED32: "integer",
    FieldDescriptor.TYPE_UINT32: "long",
    FieldDescriptor.TYPE_FIXED32: "long",
    FieldDescriptor.TYPE_INT64: "long",
    FieldDescriptor.TYPE_SINT64: "long",
    FieldDescriptor.TYPE_SFIXED64: "long",
    FieldDescriptor.TYPE_UINT64: "unsigned_long",
    FieldDescriptor.TYPE_FIXED64: "unsigned_long",
    FieldDescriptor.TYPE_BOOL: "boolean",
    FieldDescriptor.TYPE_STRING: "keyword",
    FieldDescriptor.TYPE_BYTES: "binary",
    FieldDescriptor.TYPE_ENUM: "keyword",  # enums are written as their names
}

# Well-known protobuf types that map to a single Elasticsearch type.
WELL_KNOWN_TYPES = {
    "google.protobuf.Timestamp": "date",  # json_format writes RFC 3339 strings
    "google.protobuf.Duration": "keyword",  # e.g. "1.5s"
    "google.protobuf.Struct": "flattened",
}


def to_document(message: Message) -> dict:
    """Converts a message to a dict with the proto field names as keys.

    Fields that have their default value (0, "", false, empty list) are
    included too, so every document has the same fields.
    """
    return json_format.MessageToDict(
        message,
        preserving_proto_field_name=True,
        always_print_fields_with_no_presence=True,
    )


def index_properties(descriptor: Descriptor) -> dict:
    """Builds the "properties" part of an Elasticsearch mapping for a message type."""
    load_generated_modules()  # needed to read the custom (elk.field_type) option
    from elk import options_pb2

    properties = {}
    for field in descriptor.fields:
        override = field.GetOptions().Extensions[options_pb2.field_type]
        if override:
            properties[field.name] = {"type": override}
        elif field.type == FieldDescriptor.TYPE_MESSAGE:
            message_type = field.message_type
            if message_type.full_name in WELL_KNOWN_TYPES:
                properties[field.name] = {"type": WELL_KNOWN_TYPES[message_type.full_name]}
            elif message_type.GetOptions().map_entry:
                properties[field.name] = {"type": "flattened"}
            else:
                properties[field.name] = {"properties": index_properties(message_type)}
        else:
            # Repeated fields need no special mapping: every Elasticsearch
            # field can hold a list of values.
            properties[field.name] = {"type": SCALAR_TYPES[field.type]}
    return properties
