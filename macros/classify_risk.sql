{% macro classify_risk(score_column) %}
    CASE
        WHEN {{ score_column }} >= 80 THEN 'CRITICAL'
        WHEN {{ score_column }} >= 60 THEN 'HIGH'
        WHEN {{ score_column }} >= 30 THEN 'MEDIUM'
        ELSE                               'LOW'
    END
{% endmacro %}
