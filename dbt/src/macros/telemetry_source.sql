{#
  Returns a relation that reads the raw Parquet files written by the parquet
  consumer. The syntax differs between engines, so the macro is "dispatched":
  dbt calls duckdb__telemetry_source on DuckDB, databricks__telemetry_source on
  Databricks, and so on.

  Both engines understand the Hive-style event_date=YYYY-MM-DD folders and
  add an event_date column.
#}
{% macro telemetry_source() %}
  {{ return(adapter.dispatch('telemetry_source')()) }}
{% endmacro %}

{% macro duckdb__telemetry_source() %}
  read_parquet('{{ var("parquet_path") }}/*/*.parquet', hive_partitioning = true, union_by_name = true)
{% endmacro %}

{% macro databricks__telemetry_source() %}
  parquet.`{{ var("parquet_path") }}`
{% endmacro %}
