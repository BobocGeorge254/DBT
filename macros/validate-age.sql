{% macro validate_age(column_name) %}
    datediff(year, {{ column_name }}, getdate()) >= 18
{% endmacro %}
