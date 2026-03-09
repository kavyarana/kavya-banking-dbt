-- Mart model: dim_accounts
-- Type        : Dimension table
-- Materialized: table
-- Grain       : One row per account

WITH accounts AS (
    SELECT * FROM {{ ref('stg_accounts') }}
),

branches AS (
    SELECT branch_id, branch_name, branch_type, city AS branch_city, region
    FROM {{ ref('stg_branches') }}
),

products AS (
    SELECT product_id, product_name, monthly_fee, interest_rate AS product_interest_rate
    FROM {{ ref('stg_products') }}
),

customer_products AS (
    SELECT account_id, product_id
    FROM {{ ref('stg_customer_products') }}
    WHERE status = 'ACTIVE'
),

final AS (
    SELECT
        -- Surrogate key
        {{ generate_surrogate_key('a.account_id') }}    AS account_key,

        -- Natural key
        a.account_id,
        a.customer_id,

        -- Account attributes
        a.account_type,
        a.account_status,
        a.currency_code,
        a.current_balance,
        a.overdraft_limit,
        a.interest_rate,
        a.sort_code,
        a.open_date,
        a.close_date,

        -- Branch context
        b.branch_id,
        b.branch_name,
        b.branch_type,
        b.branch_city,
        b.region                                        AS branch_region,

        -- Product context
        p.product_name,
        p.monthly_fee,

        -- Derived
        DATEDIFF('day', a.open_date, CURRENT_DATE())   AS account_age_days,
        CASE
            WHEN a.account_status = 'ACTIVE' THEN TRUE
            ELSE FALSE
        END                                             AS is_active,

        -- Audit
        a.load_timestamp

    FROM accounts          a
    LEFT JOIN branches     b  ON a.branch_id = b.branch_id
    LEFT JOIN customer_products cp ON a.account_id = cp.account_id
    LEFT JOIN products     p  ON cp.product_id = p.product_id
)

SELECT * FROM final
