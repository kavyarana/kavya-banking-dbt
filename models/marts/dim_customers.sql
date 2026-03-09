-- Mart model: dim_customers
-- Type        : Dimension table
-- Materialized: table
-- Grain       : One row per customer

WITH customer_accounts AS (
    SELECT * FROM {{ ref('int_customer_accounts') }}
),

-- Aggregate account-level stats up to customer level
customer_stats AS (
    SELECT
        customer_id,
        COUNT(DISTINCT account_id)          AS total_accounts,
        SUM(current_balance)                AS total_balance_gbp,
        MAX(current_balance)                AS max_account_balance,
        MIN(account_open_date)              AS earliest_account_date
    FROM customer_accounts
    GROUP BY customer_id
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

final AS (
    SELECT
        -- Surrogate key
        {{ generate_surrogate_key('c.customer_id') }}   AS customer_key,

        -- Natural key
        c.customer_id,

        -- Customer attributes
        c.full_name,
        c.first_name,
        c.last_name,
        c.date_of_birth,
        c.gender,
        c.city,
        c.country,
        c.postcode,
        c.customer_segment,
        c.kyc_status,
        c.credit_score,
        c.is_active,
        c.account_open_date                             AS customer_since,

        -- Derived stats
        cs.total_accounts,
        cs.total_balance_gbp,
        cs.max_account_balance,
        cs.earliest_account_date,

        -- Credit tier
        CASE
            WHEN c.credit_score >= 750 THEN 'EXCELLENT'
            WHEN c.credit_score >= 700 THEN 'GOOD'
            WHEN c.credit_score >= 650 THEN 'FAIR'
            ELSE                            'POOR'
        END                                             AS credit_tier,

        -- Audit
        c.load_timestamp

    FROM customers       c
    LEFT JOIN customer_stats cs ON c.customer_id = cs.customer_id
)

SELECT * FROM final
