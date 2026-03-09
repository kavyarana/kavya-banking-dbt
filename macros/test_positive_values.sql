{% test positive_values(model, column_name) %}
-- Custom generic test: ensures a column never has negative values
-- Usage: add "- positive_values" under any numeric column in schema.yml

SELECT {{ column_name }}, COUNT(*) AS failing_rows
FROM {{ model }}
WHERE {{ column_name }} < 0
GROUP BY {{ column_name }}

{% endtest %}
