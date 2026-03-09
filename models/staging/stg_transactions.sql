-- Staging model: stg_transactions
-- Grain        : One row per unique transaction (deduplicated)
-- Materialized : view

WITH source AS (
    SELECT * FROM {{ source('kavya_raw', 'RAW_TRANSACTIONS') }}
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY TRANSACTION_ID
            ORDER BY _LOAD_TIMESTAMP DESC
        ) AS row_num
    FROM source
    WHERE TRANSACTION_ID IS NOT NULL
      AND ACCOUNT_ID     IS NOT NULL
      AND AMOUNT         IS NOT NULL
      AND AMOUNT         > 0              -- filter zero or negative amounts
),

final AS (
    SELECT
        TRANSACTION_ID                      AS transaction_id,
        ACCOUNT_ID                          AS account_id,
        TRANSACTION_DATE                    AS transaction_date,
        TRANSACTION_TIMESTAMP               AS transaction_timestamp,
        UPPER(TRANSACTION_TYPE)             AS transaction_type,
        UPPER(TRANSACTION_CATEGORY)         AS transaction_category,
        TRIM(MERCHANT_NAME)                 AS merchant_name,
        MERCHANT_CATEGORY_CODE              AS merchant_category_code,
        CAST(AMOUNT AS FLOAT)               AS amount,
        UPPER(CURRENCY_CODE)                AS currency_code,
        UPPER(CHANNEL)                      AS channel,
        TRIM(LOCATION_CITY)                 AS location_city,
        TRIM(LOCATION_COUNTRY)              AS location_country,
        IS_FRAUD_FLAGGED                    AS is_fraud_flagged,
        CAST(FRAUD_SCORE AS FLOAT)          AS fraud_score,
        REFERENCE_NUMBER                    AS reference_number,
        UPPER(STATUS)                       AS status,
        _SOURCE_FILE                        AS source_file,
        _LOAD_TIMESTAMP                     AS load_timestamp
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM final
