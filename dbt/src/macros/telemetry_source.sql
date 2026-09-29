{#
  Returns a relation with one row per archived message: the decoded message
  fields as columns, plus the Kafka coordinates (kafka_partition,
  kafka_offset, kafka_timestamp, ingested_at).

  It reads the raw_vehicle_telemetry_records source, a view with one row per
  Kafka record (see macros/raw_protobuf_views.sql), and decodes each record's
  `value` with Spark's from_protobuf into the message type in
  var('message_type'), using the descriptor set the consumer writes next to
  the files.

  The descriptor set always describes the newest schema. Protobuf decodes
  older messages with it too: fields that didn't exist yet get their default
  value.

  Messages that aren't valid protobuf are skipped (PERMISSIVE mode). The
  generated source tests in src/models/sources/ report them, and `dbt build`
  doesn't build the models while those tests fail.
#}
{% macro telemetry_source() %}
  {%- set message_type = var('message_type') -%}
  (
    select
      decoded.*,
      `partition` as kafka_partition,
      `offset` as kafka_offset,
      `timestamp` as kafka_timestamp,
      ingested_at
    from (
      select
        *,
        from_protobuf(
          value,
          '{{ message_type }}',
          '{{ var("descriptor_path") }}',
          -- proto3 doesn't store fields that have their default value (0, "",
          -- false). Without emit.default.values Spark would return them as null.
          map('mode', 'PERMISSIVE', 'emit.default.values', 'true')
        ) as decoded
      from {{ source('raw', 'vehicle_telemetry_records') }}
      where message_type = '{{ message_type }}'
    ) as records
    where decoded is not null
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
