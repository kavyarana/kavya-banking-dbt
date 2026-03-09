-- Mart model: fct_transactions
-- Type        : Fact table (INCREMENTAL)
-- Materialized: incremental
-- Grain       : One row per transaction
-- Unique key  : transaction_id
-- Late records: handled via is_incremental() filter on transaction_date

{{
    config(
        materialized  = 'incremental',
        unique_key    = 'transaction_id',
        on_schema_change = 'sync_all_columns'
    )
}}

WITH enriched AS (
    SELECT * FROM {{ ref('int_transaction_enriched') }}
),

dim_customers AS (
    SELECT customer_id, customer_key, credit_tier
    FROM {{ ref('dim_customers') }}
),

dim_accounts AS (
    SELECT account_id, account_key
    FROM {{ ref('dim_accounts') }}
),

final AS (
    SELECT
        -- Surrogate key for fact
        {{ generate_surrogate_key('e.transaction_id') }}    AS transaction_key,

        -- Foreign keys to dimensions
        dc.customer_key,
        da.account_key,

        -- Natural keys
        e.transaction_id,
        e.account_id,
        e.customer_id,

        -- Date dimension key
        TO_NUMBER(TO_CHAR(e.transaction_date, 'YYYYMMDD'))  AS date_key,

        -- Transaction measures
        e.transaction_date,
        e.transaction_timestamp,
        e.transaction_type,
        e.transaction_category,
        e.merchant_name,
        e.channel,
        e.location_country,
        e.is_international,
        e.amount                                            AS amount_gbp,
        e.amount_usd,
        e.gbp_to_usd_rate,

        -- Fraud measures
        e.is_fraud_flagged,
        e.fraud_score,
        e.risk_level,

        -- Status
        e.status,
        e.reference_number,

        -- Customer context (denormalised for query performance)
        e.customer_segment,
        dc.credit_tier,
        e.account_type

    FROM enriched       e
    LEFT JOIN dim_customers dc ON e.customer_id = dc.customer_id
    LEFT JOIN dim_accounts  da ON e.account_id  = da.account_id

    -- INCREMENTAL FILTER: only process new/updated records
    {% if is_incremental() %}
        WHERE e.transaction_date >= (
            SELECT DATEADD('day', -3, MAX(transaction_date))
            FROM {{ this }}
        )
    {% endif %}
)

SELECT * FROM final
