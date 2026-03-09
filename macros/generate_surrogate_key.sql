{% macro generate_surrogate_key(column) %}
    MD5(CAST({{ column }} AS VARCHAR))
{% endmacro %}
