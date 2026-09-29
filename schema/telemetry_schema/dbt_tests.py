"""Protobuf schema -> dbt source tests.

Generates a dbt sources file for the records archived by the protobuf
consumer, with tests that check every message against the schema:

- The bytes decode as the message type (protobuf_decodes).
- Every enum value is one of the values defined in the schema. Protobuf
  accepts unknown enum numbers on the wire; Spark decodes them as
  UNKNOWN_ENUM_VALUE_<Enum>_<number>.
- The protovalidate rules in the .proto file (buf.validate.field options),
  translated to Spark SQL conditions. For each top-level field, all rules of
  the field and of the messages nested in it become one
  protobuf_field_valid test.

Rules that can't be expressed in SQL here (CEL expressions, some well-known
string formats, ...) are listed under `meta.unchecked_rules` in the output,
so it's visible what is not checked.

The generic tests themselves (protobuf_decodes, protobuf_field_valid) are
dbt macros in dbt/src/macros/protobuf_tests.sql.
"""

from dataclasses import dataclass, field as dataclass_field

import yaml
from google.protobuf.descriptor import Descriptor, FieldDescriptor

from .registry import load_generated_modules

F = FieldDescriptor

NUMERIC_KINDS = {
    F.TYPE_DOUBLE: "double",
    F.TYPE_FLOAT: "float",
    F.TYPE_INT32: "int32",
    F.TYPE_INT64: "int64",
    F.TYPE_UINT32: "uint32",
    F.TYPE_UINT64: "uint64",
    F.TYPE_SINT32: "sint32",
    F.TYPE_SINT64: "sint64",
    F.TYPE_FIXED32: "fixed32",
    F.TYPE_FIXED64: "fixed64",
    F.TYPE_SFIXED32: "sfixed32",
    F.TYPE_SFIXED64: "sfixed64",
}

# Regular expressions for protovalidate's well-known string formats that are
# simple enough to check with a regex.
WELL_KNOWN_STRING_PATTERNS = {
    "uuid": "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
    "tuuid": "^[0-9a-fA-F]{32}$",
    "ulid": "^[0-7][0-9A-HJKMNP-TV-Za-hjkmnp-tv-z]{25}$",
    "email": "^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$",
}


def sql_string(value: str) -> str:
    """A Spark SQL string literal (Spark processes backslash escapes)."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def sql_number(value) -> str:
    return repr(value) if isinstance(value, float) else str(value)


@dataclass
class Checks:
    """SQL conditions that must be true for a valid message, with descriptions."""

    conditions: list[str] = dataclass_field(default_factory=list)
    descriptions: list[str] = dataclass_field(default_factory=list)
    unchecked: list[str] = dataclass_field(default_factory=list)

    def add(self, condition: str, description: str) -> None:
        self.conditions.append(condition)
        self.descriptions.append(description)

    def extend(self, other: "Checks", guard: str | None = None) -> None:
        """Adds another field's checks, only applied when `guard` is true."""
        for condition, description in zip(other.conditions, other.descriptions):
            self.add(f"({guard} or {condition})" if guard else condition, description)
        self.unchecked.extend(other.unchecked)


class RuleTranslator:
    """Translates protovalidate rules into Spark SQL conditions.

    Messages are decoded without emitting default values, so a field that is
    unset, or has its zero value (0, "", false), is NULL. That makes
    `required` a NOT NULL check; other rules are applied to the zero value
    instead of NULL, like protovalidate does.
    """

    def __init__(self):
        load_generated_modules()
        from buf.validate import validate_pb2

        self.pv = validate_pb2

    def rules(self, field: FieldDescriptor):
        options = field.GetOptions()
        if options.HasExtension(self.pv.field):
            return options.Extensions[self.pv.field]
        return self.pv.FieldRules()

    def field(self, field: FieldDescriptor, expr: str, path: str) -> Checks:
        """Checks for one field; `expr` is the SQL expression of its decoded value."""
        rules = self.rules(field)
        checks = Checks()
        if rules.ignore == self.pv.IGNORE_ALWAYS:
            return checks
        self._unsupported_common(rules, path, checks)
        is_map = field.type == F.TYPE_MESSAGE and field.message_type.GetOptions().map_entry
        if is_map:
            if rules.WhichOneof("type"):
                checks.unchecked.append(f"{path}: map rules")
        elif field.is_repeated:
            self._repeated(field, rules, expr, path, checks)
        else:
            if rules.required:
                checks.add(f"{expr} is not null", f"{path} is required")
            value_checks = self._value(field, rules, expr, path, nullable=True)
            guard = f"{expr} is null" if rules.ignore == self.pv.IGNORE_IF_ZERO_VALUE else None
            checks.extend(value_checks, guard)
        return checks

    def _repeated(self, field, rules, expr, path, checks: Checks) -> None:
        size = f"coalesce(size({expr}), 0)"
        if rules.required:
            checks.add(f"{size} > 0", f"{path} is required (not empty)")
        repeated = rules.repeated
        if repeated.HasField("min_items"):
            checks.add(f"{size} >= {repeated.min_items}", f"{path} has at least {repeated.min_items} items")
        if repeated.HasField("max_items"):
            checks.add(f"{size} <= {repeated.max_items}", f"{path} has at most {repeated.max_items} items")
        if repeated.unique:
            checks.add(f"({expr} is null or size(array_distinct({expr})) = size({expr}))", f"{path} items are unique")
        variable = f"x{path.count('.') + path.count('[')}"
        item_checks = self._value(field, repeated.items, variable, f"{path}[]", nullable=False)
        if item_checks.conditions:
            condition = " and ".join(item_checks.conditions)
            checks.add(f"({expr} is null or forall({expr}, {variable} -> {condition}))", "; ".join(item_checks.descriptions))
        checks.unchecked.extend(item_checks.unchecked)
        if repeated.items.required or repeated.items.cel:
            checks.unchecked.append(f"{path}[]: required/cel item rules")

    def _value(self, field, rules, expr, path, nullable: bool) -> Checks:
        """Checks on a single (non-repeated) value of the field's type."""
        checks = Checks()
        kind = rules.WhichOneof("type")
        if field.type in NUMERIC_KINDS:
            self._numeric(getattr(rules, NUMERIC_KINDS[field.type]), f"coalesce({expr}, 0)" if nullable else expr, path, checks)
        elif field.type == F.TYPE_STRING:
            self._string(rules.string, f"coalesce({expr}, '')" if nullable else expr, path, checks)
        elif field.type == F.TYPE_BOOL:
            if rules.bool.HasField("const"):
                value = f"coalesce({expr}, false)" if nullable else expr
                checks.add(f"{value} = {str(rules.bool.const).lower()}", f"{path} = {str(rules.bool.const).lower()}")
        elif field.type == F.TYPE_ENUM:
            self._enum(field, rules.enum, expr, path, checks, nullable)
        elif field.type == F.TYPE_MESSAGE:
            full_name = field.message_type.full_name
            if full_name == "google.protobuf.Timestamp":
                self._timestamp(rules.timestamp, expr, path, checks)
            elif full_name.startswith("google.protobuf."):
                if kind:
                    checks.unchecked.append(f"{path}: {kind} rules")
            else:
                # A nested message: check its fields, but only when it is set.
                for sub in field.message_type.fields:
                    sub_expr = f"{expr}.`{sub.name}`"
                    checks.extend(self.field(sub, sub_expr, f"{path}.{sub.name}"), guard=f"{expr} is null")
        elif kind:
            checks.unchecked.append(f"{path}: {kind} rules")
        return checks

    def _numeric(self, rules, value, path, checks: Checks) -> None:
        if rules.HasField("const"):
            checks.add(f"{value} = {sql_number(rules.const)}", f"{path} = {rules.const}")
        lower = upper = None
        if rules.HasField("gt"):
            lower = (f"{value} > {sql_number(rules.gt)}", f"{path} > {rules.gt}", rules.gt)
        if rules.HasField("gte"):
            lower = (f"{value} >= {sql_number(rules.gte)}", f"{path} >= {rules.gte}", rules.gte)
        if rules.HasField("lt"):
            upper = (f"{value} < {sql_number(rules.lt)}", f"{path} < {rules.lt}", rules.lt)
        if rules.HasField("lte"):
            upper = (f"{value} <= {sql_number(rules.lte)}", f"{path} <= {rules.lte}", rules.lte)
        if lower and upper and lower[2] > upper[2]:
            # protovalidate: a lower bound above the upper bound means
            # "outside the range".
            checks.add(f"({lower[0]} or {upper[0]})", f"{lower[1]} or {upper[1]}")
        else:
            for bound in (lower, upper):
                if bound:
                    checks.add(bound[0], bound[1])
        if rules.__class__.DESCRIPTOR.fields_by_name.get("finite") and rules.finite:
            checks.add(f"not isnan({value}) and abs({value}) != double('infinity')", f"{path} is finite")
        if rules.__getattribute__("in"):
            values = ", ".join(sql_number(v) for v in rules.__getattribute__("in"))
            checks.add(f"{value} in ({values})", f"{path} in [{values}]")
        if rules.not_in:
            values = ", ".join(sql_number(v) for v in rules.not_in)
            checks.add(f"{value} not in ({values})", f"{path} not in [{values}]")

    def _string(self, rules, value, path, checks: Checks) -> None:
        if rules.HasField("const"):
            checks.add(f"{value} = {sql_string(rules.const)}", f"{path} = {rules.const!r}")
        if rules.HasField("len"):
            checks.add(f"char_length({value}) = {rules.len}", f"{path} has {rules.len} characters")
        if rules.HasField("min_len"):
            checks.add(f"char_length({value}) >= {rules.min_len}", f"{path} has at least {rules.min_len} characters")
        if rules.HasField("max_len"):
            checks.add(f"char_length({value}) <= {rules.max_len}", f"{path} has at most {rules.max_len} characters")
        if rules.HasField("len_bytes"):
            checks.add(f"octet_length({value}) = {rules.len_bytes}", f"{path} is {rules.len_bytes} bytes")
        if rules.HasField("min_bytes"):
            checks.add(f"octet_length({value}) >= {rules.min_bytes}", f"{path} is at least {rules.min_bytes} bytes")
        if rules.HasField("max_bytes"):
            checks.add(f"octet_length({value}) <= {rules.max_bytes}", f"{path} is at most {rules.max_bytes} bytes")
        if rules.HasField("pattern"):
            checks.add(f"{value} rlike {sql_string(rules.pattern)}", f"{path} matches {rules.pattern}")
        if rules.HasField("prefix"):
            checks.add(f"startswith({value}, {sql_string(rules.prefix)})", f"{path} starts with {rules.prefix!r}")
        if rules.HasField("suffix"):
            checks.add(f"endswith({value}, {sql_string(rules.suffix)})", f"{path} ends with {rules.suffix!r}")
        if rules.HasField("contains"):
            checks.add(f"contains({value}, {sql_string(rules.contains)})", f"{path} contains {rules.contains!r}")
        if rules.HasField("not_contains"):
            checks.add(f"not contains({value}, {sql_string(rules.not_contains)})", f"{path} doesn't contain {rules.not_contains!r}")
        if rules.__getattribute__("in"):
            values = ", ".join(sql_string(v) for v in rules.__getattribute__("in"))
            checks.add(f"{value} in ({values})", f"{path} in [{values}]")
        if rules.not_in:
            values = ", ".join(sql_string(v) for v in rules.not_in)
            checks.add(f"{value} not in ({values})", f"{path} not in [{values}]")
        well_known = rules.WhichOneof("well_known")
        if well_known in WELL_KNOWN_STRING_PATTERNS:
            if getattr(rules, well_known):
                checks.add(f"{value} rlike {sql_string(WELL_KNOWN_STRING_PATTERNS[well_known])}", f"{path} is a valid {well_known}")
        elif well_known:
            checks.unchecked.append(f"{path}: string.{well_known}")

    def _enum(self, field, rules, expr, path, checks: Checks, nullable: bool) -> None:
        enum = field.enum_type
        zero = enum.values_by_number[0].name if 0 in enum.values_by_number else None
        value = f"coalesce({expr}, {sql_string(zero)})" if nullable and zero else expr
        # Always checked, whether or not the schema says defined_only: the
        # value must be one the schema knows.
        checks.add(f"not startswith({value}, 'UNKNOWN_ENUM_VALUE_')", f"{path} is a value defined in {enum.full_name}")

        def names(numbers):
            return ", ".join(sql_string(enum.values_by_number[n].name) for n in numbers if n in enum.values_by_number)

        if rules.HasField("const"):
            checks.add(f"{value} = {names([rules.const])}", f"{path} = {names([rules.const])}")
        if rules.__getattribute__("in"):
            checks.add(f"{value} in ({names(rules.__getattribute__('in'))})", f"{path} in [{names(rules.__getattribute__('in'))}]")
        if rules.not_in:
            checks.add(f"{value} not in ({names(rules.not_in)})", f"{path} not in [{names(rules.not_in)}]")

    def _timestamp(self, rules, expr, path, checks: Checks) -> None:
        def seconds(timestamp):
            return f"timestamp_micros({timestamp.seconds * 1_000_000 + timestamp.nanos // 1000})"

        comparisons = [
            ("lt", "<", None), ("lte", "<=", None), ("gt", ">", None), ("gte", ">=", None),
            ("lt_now", "<", "current_timestamp()"), ("gt_now", ">", "current_timestamp()"),
        ]
        for name, operator, now in comparisons:
            if rules.HasField(name) and (now is None or getattr(rules, name)):
                other = now or seconds(getattr(rules, name))
                checks.add(f"({expr} is null or {expr} {operator} {other})", f"{path} {operator} {'now' if now else other}")
        if rules.HasField("within"):
            micros = rules.within.seconds * 1_000_000 + rules.within.nanos // 1000
            checks.add(
                f"({expr} is null or abs(unix_micros({expr}) - unix_micros(current_timestamp())) <= {micros})",
                f"{path} within {rules.within.seconds}s of now",
            )
        if rules.HasField("const"):
            checks.add(f"({expr} is null or {expr} = {seconds(rules.const)})", f"{path} = {seconds(rules.const)}")

    def _unsupported_common(self, rules, path, checks: Checks) -> None:
        if rules.cel or rules.cel_expression:
            checks.unchecked.append(f"{path}: CEL expressions")


def snake(name: str) -> str:
    return "".join(f"_{c.lower()}" if c.isupper() and i else c.lower() for i, c in enumerate(name))


def source_yaml(message: Descriptor, record: Descriptor, table: str, generated_from: str) -> str:
    """Builds the dbt sources YAML for the archive of one message type."""
    translator = RuleTranslator()
    value_tests = [
        {
            "protobuf_decodes": {
                "name": f"protobuf_decodes__{table}",
                "arguments": {"message_type": message.full_name},
            }
        }
    ]
    unchecked = []
    options = message.GetOptions()
    if options.HasExtension(translator.pv.message):
        unchecked.append(f"{message.name}: message rules (CEL/oneof)")
    for field in message.fields:
        checks = translator.field(field, f"m.`{field.name}`", field.name)
        unchecked.extend(checks.unchecked)
        if not checks.conditions:
            continue
        value_tests.append(
            {
                "protobuf_field_valid": {
                    "name": f"protobuf_valid__{table}__{field.name}",
                    "arguments": {
                        "message_type": message.full_name,
                        "field": field.name,
                        "condition": "\nand ".join(f"({c})" for c in checks.conditions) + "\n",
                    },
                    "config": {"meta": {"rules": checks.descriptions}},
                }
            }
        )

    columns = [{"name": "file_path", "description": "The file the record was read from."}]
    for field in record.fields:
        column = {"name": field.name}
        if field.name == "value":
            column["description"] = f"The serialized {message.full_name} message."
            column["data_tests"] = value_tests
        elif field.name == "message_type":
            column["data_tests"] = [
                {"accepted_values": {"name": f"known_message_type__{table}", "arguments": {"values": [message.full_name]}}}
            ]
        columns.append(column)

    source = {
        "version": 2,
        "sources": [
            {
                "name": "raw",
                "description": "Binary protobuf files archived by the protobuf consumer, exposed as views "
                "by the on-run-start hooks in dbt_project.yml (macros/raw_protobuf_views.sql).",
                "schema": "{{ target.schema }}",
                "tables": [
                    {
                        "name": f"{table}_files",
                        "identifier": f"raw_{table}_files",
                        "description": "One row per file. Each file is one kafka.v1.KafkaRecordBatch message.",
                        "columns": [
                            {"name": "file_path"},
                            {
                                "name": "content",
                                "data_tests": [
                                    {
                                        "protobuf_decodes": {
                                            "name": f"protobuf_decodes__{table}_files",
                                            "arguments": {"message_type": "kafka.v1.KafkaRecordBatch", "message_type_column": None},
                                        }
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "name": f"{table}_records",
                        "identifier": f"raw_{table}_records",
                        "description": f"One row per Kafka record, with the {message.full_name} message still serialized in `value`.",
                        **({"meta": {"unchecked_rules": unchecked}} if unchecked else {}),
                        "columns": columns,
                    },
                ],
            }
        ],
    }
    header = (
        f"# GENERATED by schema/generate.py from {generated_from}. Do not edit.\n"
        "# Tests that the archived messages are valid according to the protobuf schema:\n"
        "# they decode, their enum values are defined, and they follow the protovalidate\n"
        "# rules ([(buf.validate.field)...] options) in the .proto file.\n"
    )
    return header + yaml.dump(source, Dumper=_Dumper, sort_keys=False, width=120, allow_unicode=True)


class _Dumper(yaml.SafeDumper):
    """Writes multi-line strings (the SQL conditions) as readable | blocks."""


_Dumper.add_representer(
    str,
    lambda dumper, value: dumper.represent_scalar("tag:yaml.org,2002:str", value, style="|" if "\n" in value else None),
)
