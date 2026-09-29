{#
  Returns a relation that reads the raw Parquet files written by the parquet
  consumer. The syntax differs a little between local Spark and Databricks,
  so the macro is "dispatched": dbt calls databricks__telemetry_source on
  Databricks and spark__telemetry_source on Spark.

  Both understand the Hive-style event_date=YYYY-MM-DD folders and add an
  event_date column. Files written before and after a schema change have
  different columns, so their schemas are merged: locally through the
  spark.sql.parquet.mergeSchema setting in profiles.yml, on Databricks
  through the mergeSchema option of read_files.
#}
{% macro telemetry_source() %}
  {{ return(adapter.dispatch('telemetry_source')()) }}
{% endmacro %}

{% macro spark__telemetry_source() %}
  parquet.`{{ var("parquet_path") }}`
{% endmacro %}

{% macro databricks__telemetry_source() %}
  read_files('{{ var("parquet_path") }}', format => 'parquet', mergeSchema => true)
{% endmacro %}
