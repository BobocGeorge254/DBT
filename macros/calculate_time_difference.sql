{% macro calculate_time_difference(start_time, end_time, unit='MINUTE') %}
    CASE 
        WHEN {{ end_time }} IS NOT NULL THEN 
            DATEDIFF({{ unit }}, {{ start_time }}, {{ end_time }})
        ELSE NULL
    END 
{% endmacro %}
