-- Singular test: fraud score must always be between 0 and 100
-- Returns rows that FAIL (dbt expects 0 rows = pass)

SELECT
    transaction_id,
    fraud_score
FROM {{ ref('stg_transactions') }}
WHERE fraud_score < 0
   OR fraud_score > 100
