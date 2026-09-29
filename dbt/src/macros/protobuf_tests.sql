{#
  Generic tests for serialized protobuf messages. schema/generate.py applies
  them to the raw sources (src/models/sources/raw_*.yml), based on the
  .proto files. They decode with Spark's from_protobuf in PERMISSIVE mode,
  which returns NULL instead of failing when the bytes aren't a valid
  message.
#}

{#- Fails for every row whose `column_name` isn't a valid `message_type` message. -#}
{% test protobuf_decodes(model, column_name, message_type, message_type_column='message_type') %}

select * from (
    select
        *,
        from_protobuf(
            {{ column_name }}, '{{ message_type }}', '{{ var("descriptor_path") }}',
            map('mode', 'PERMISSIVE')
        ) as decoded
    from {{ model }}
    {% if message_type_column %}
    where {{ message_type_column }} = '{{ message_type }}'
    {% endif %}
) as messages
where decoded is null

{% endtest %}


{#-
  Fails for every message where `condition` isn't true. The condition is a
  Spark SQL expression over the decoded message `m`. Default values (0, "",
  false) are not emitted, so an unset or zero field is NULL; the generated
  conditions account for that. Messages that don't decode are left to
  protobuf_decodes.
-#}
{% test protobuf_field_valid(model, column_name, message_type, field, condition, message_type_column='message_type') %}

select
    * except (m, {{ column_name }}),
    string(m.`{{ field }}`) as invalid_value
from (
    select
        *,
        from_protobuf(
            {{ column_name }}, '{{ message_type }}', '{{ var("descriptor_path") }}',
            map('mode', 'PERMISSIVE')
        ) as m
    from {{ model }}
    where {{ message_type_column }} = '{{ message_type }}'
) as messages
where m is not null
  and not coalesce({{ condition }}, false)

{% endtest %}
