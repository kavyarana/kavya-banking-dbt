-- Intermediate model: int_fraud_summary
-- Purpose      : Pre-aggregate fraud metrics per account per day
-- Materialized : table (aggregation reused in fraud mart)

WITH transactions AS (
    SELECT * FROM {{ ref('int_transaction_enriched') }}
),

fraud_alerts AS (
    SELECT * FROM {{ ref('stg_fraud_alerts') }}
),

txn_agg AS (
    SELECT
        account_id,
        customer_id,
        customer_segment,
        transaction_date,
        COUNT(*)                                                            AS total_txns,
        SUM(amount)                                                         AS total_amount_gbp,
        SUM(CASE WHEN is_fraud_flagged THEN 1 ELSE 0 END)                  AS fraud_txn_count,
        SUM(CASE WHEN is_fraud_flagged THEN amount ELSE 0 END)             AS fraud_amount_gbp,
        ROUND(AVG(fraud_score), 2)                                         AS avg_fraud_score,
        MAX(fraud_score)                                                    AS max_fraud_score,
        SUM(CASE WHEN is_international THEN 1 ELSE 0 END)                  AS international_txn_count
    FROM transactions
    GROUP BY account_id, customer_id, customer_segment, transaction_date
),

alert_agg AS (
    SELECT
        account_id,
        DATE(alert_timestamp)                                               AS alert_date,
        COUNT(*)                                                            AS total_alerts,
        SUM(CASE WHEN alert_severity = 'CRITICAL' THEN 1 ELSE 0 END)      AS critical_alerts,
        SUM(CASE WHEN is_confirmed_fraud THEN 1 ELSE 0 END)                AS confirmed_fraud_count
    FROM fraud_alerts
    GROUP BY account_id, DATE(alert_timestamp)
),

final AS (
    SELECT
        t.account_id,
        t.customer_id,
        t.customer_segment,
        t.transaction_date,
        t.total_txns,
        t.total_amount_gbp,
        t.fraud_txn_count,
        t.fraud_amount_gbp,
        t.avg_fraud_score,
        t.max_fraud_score,
        t.international_txn_count,
        COALESCE(a.total_alerts,          0)    AS total_alerts,
        COALESCE(a.critical_alerts,       0)    AS critical_alerts,
        COALESCE(a.confirmed_fraud_count, 0)    AS confirmed_fraud_count,
        ROUND(
            t.fraud_txn_count * 100.0 / NULLIF(t.total_txns, 0), 2
        )                                       AS fraud_rate_pct
    FROM txn_agg      t
    LEFT JOIN alert_agg a ON t.account_id      = a.account_id
                          AND t.transaction_date = a.alert_date
)

SELECT * FROM final
