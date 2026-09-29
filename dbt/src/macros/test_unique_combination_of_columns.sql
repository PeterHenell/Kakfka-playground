{#
  A custom generic test: fails if the combination of the given columns is
  not unique. (dbt_utils has the same test; this keeps the project free of
  package dependencies.)
#}
{% test unique_combination_of_columns(model, columns) %}

select {{ columns | join(', ') }}, count(*) as row_count
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1

{% endtest %}
