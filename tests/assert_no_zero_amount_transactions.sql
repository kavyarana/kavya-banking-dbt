-- Singular test: ensure no transactions have a zero or negative amount
-- This returns rows that FAIL the test (dbt expects 0 rows = pass)

SELECT
    transaction_id,
    amount,
    transaction_date
FROM {{ ref('stg_transactions') }}
WHERE amount <= 0
