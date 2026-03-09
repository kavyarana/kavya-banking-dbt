-- Intermediate model: int_transaction_enriched
-- Purpose      : Enrich transactions with account + customer + FX data
-- Materialized : table (heavy join — cache for mart performance)
-- Why table?   : Downstream fact table and fraud mart both use this

WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

accounts AS (
    SELECT account_id, customer_id, account_type, current_balance
    FROM {{ ref('stg_accounts') }}
),

customers AS (
    SELECT customer_id, full_name, customer_segment, credit_score
    FROM {{ ref('stg_customers') }}
),

fx_rates AS (
    SELECT rate_date, base_currency, target_currency, exchange_rate
    FROM {{ ref('stg_exchange_rates') }}
    WHERE target_currency = 'USD'
),

final AS (
    SELECT
        -- Transaction core
        t.transaction_id,
        t.transaction_date,
        t.transaction_timestamp,
        t.transaction_type,
        t.transaction_category,
        t.merchant_name,
        t.amount,
        t.currency_code,
        t.channel,
        t.location_city,
        t.location_country,
        t.is_fraud_flagged,
        t.fraud_score,
        t.status,
        t.reference_number,

        -- Account context
        a.account_id,
        a.account_type,
        a.current_balance,

        -- Customer context
        c.customer_id,
        c.full_name                         AS customer_name,
        c.customer_segment,
        c.credit_score,

        -- FX enrichment
        fx.exchange_rate                    AS gbp_to_usd_rate,
        ROUND(t.amount * fx.exchange_rate, 2) AS amount_usd,

        -- Risk classification (reusable business logic)
        {{ classify_risk('t.fraud_score') }} AS risk_level,

        -- Flag: is this an international transaction?
        CASE
            WHEN t.location_country != 'United Kingdom' THEN TRUE
            ELSE FALSE
        END                                 AS is_international

    FROM transactions  t
    JOIN accounts      a  ON t.account_id    = a.account_id
    JOIN customers     c  ON a.customer_id   = c.customer_id
    LEFT JOIN fx_rates fx ON t.transaction_date = fx.rate_date
                          AND t.currency_code   = fx.base_currency
)

SELECT * FROM final
