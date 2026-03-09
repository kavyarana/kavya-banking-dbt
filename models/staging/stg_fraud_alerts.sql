-- Staging model: stg_fraud_alerts
-- Grain        : One row per fraud alert (deduplicated)
-- Materialized : view

WITH source AS (
    SELECT * FROM {{ source('kavya_raw', 'RAW_FRAUD_ALERTS') }}
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY ALERT_ID
            ORDER BY _LOAD_TIMESTAMP DESC
        ) AS row_num
    FROM source
    WHERE ALERT_ID       IS NOT NULL
      AND TRANSACTION_ID IS NOT NULL
),

final AS (
    SELECT
        ALERT_ID                            AS alert_id,
        TRANSACTION_ID                      AS transaction_id,
        ACCOUNT_ID                          AS account_id,
        ALERT_TIMESTAMP                     AS alert_timestamp,
        UPPER(ALERT_TYPE)                   AS alert_type,
        UPPER(ALERT_SEVERITY)               AS alert_severity,
        CAST(FRAUD_SCORE AS FLOAT)          AS fraud_score,
        IS_CONFIRMED_FRAUD                  AS is_confirmed_fraud,
        UPPER(INVESTIGATION_STATUS)         AS investigation_status,
        RESOLVED_AT                         AS resolved_at,
        RESOLVED_BY                         AS resolved_by,
        NOTES                               AS notes,
        _SOURCE_FILE                        AS source_file,
        _LOAD_TIMESTAMP                     AS load_timestamp
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM final
