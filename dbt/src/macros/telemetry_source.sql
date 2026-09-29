{#
  Returns a relation with one row per message archived by the protobuf
  consumer: the decoded message fields as columns, plus the Kafka
  coordinates (kafka_partition, kafka_offset, kafka_timestamp, ingested_at).

  Each file is one kafka.v1.KafkaRecordBatch message. Spark decodes it in two
  steps with from_protobuf, using the descriptor set the consumer writes next
  to the files:

    1. file bytes     -> KafkaRecordBatch -> one row per KafkaRecord (explode)
    2. record.value   -> the message type in var('message_type'), e.g.
                         vehicle.v1.VehicleTelemetry

  The descriptor set always describes the newest schema. Protobuf decodes
  older messages with it too: fields that didn't exist yet get their default
  value.
#}
{% macro telemetry_source() %}
  {%- set descriptor = var('descriptor_path') -%}
  {%- set message_type = var('message_type') -%}
  (
    select
      decoded.*,
      record.partition as kafka_partition,
      record.offset as kafka_offset,
      record.timestamp as kafka_timestamp,
      record.ingested_at
    from (
      select
        record,
        from_protobuf(
          record.value,
          '{{ message_type }}',
          '{{ descriptor }}',
          -- proto3 doesn't store fields that have their default value (0, "",
          -- false). Without this option Spark would return them as null.
          map('emit.default.values', 'true')
        ) as decoded
      from (
        select explode(from_protobuf(content, 'kafka.v1.KafkaRecordBatch', '{{ descriptor }}').records) as record
        from {{ raw_protobuf_files() }}
      )
      where record.message_type = '{{ message_type }}'
    )
  )
{% endmacro %}

{#
  The raw files, as rows with a binary `content` column. Reading files
  differs a little between local Spark and Databricks, so this macro is
  "dispatched": dbt calls databricks__raw_protobuf_files on Databricks and
  spark__raw_protobuf_files on Spark.
#}
{% macro raw_protobuf_files() %}
  {{ return(adapter.dispatch('raw_protobuf_files')()) }}
{% endmacro %}

{% macro spark__raw_protobuf_files() %}
  {#- Spark skips files and folders starting with _ or . (the descriptor set, temp files). -#}
  binaryFile.`{{ var("raw_path") }}`
{% endmacro %}

{% macro databricks__raw_protobuf_files() %}
  read_files('{{ var("raw_path") }}', format => 'binaryFile', pathGlobFilter => '*.pb')
{% endmacro %}
