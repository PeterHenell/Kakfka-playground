{#
  Turns an array column into one row per element. Used in a select list:

    select vehicle_id, {{ explode('events') }} as event from ...
#}
{% macro explode(column) %}
  {{ return(adapter.dispatch('explode')(column)) }}
{% endmacro %}

{% macro duckdb__explode(column) %}unnest({{ column }}){% endmacro %}

{% macro databricks__explode(column) %}explode({{ column }}){% endmacro %}
