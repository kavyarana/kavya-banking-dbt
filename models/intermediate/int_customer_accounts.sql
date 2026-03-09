-- Intermediate model: int_customer_accounts
-- Purpose      : Join customers + accounts + branches into one reusable model
-- Materialized : table (used by multiple downstream marts)

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

accounts AS (
    SELECT * FROM {{ ref('stg_accounts') }}
),

branches AS (
    SELECT * FROM {{ ref('stg_branches') }}
),

final AS (
    SELECT
        -- Customer
        c.customer_id,
        c.full_name,
        c.customer_segment,
        c.kyc_status,
        c.credit_score,
        c.city                          AS customer_city,
        c.country                       AS customer_country,
        c.is_active                     AS customer_is_active,
        c.account_open_date             AS customer_since,

        -- Account
        a.account_id,
        a.account_type,
        a.account_status,
        a.current_balance,
        a.overdraft_limit,
        a.interest_rate,
        a.currency_code,
        a.open_date                     AS account_open_date,

        -- Branch
        b.branch_id,
        b.branch_name,
        b.branch_type,
        b.city                          AS branch_city,
        b.region                        AS branch_region

    FROM customers  c
    JOIN accounts   a ON c.customer_id = a.customer_id
    JOIN branches   b ON a.branch_id   = b.branch_id
)

SELECT * FROM final