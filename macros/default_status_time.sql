{% macro apply_default_status_time(status_column, time_diff_column) %}
    COALESCE({{ status_column }}, 'Applied') AS CandidateStatus,
    COALESCE({{ time_diff_column }}, 0) AS TimeDifferenceInHours
{% endmacro %}