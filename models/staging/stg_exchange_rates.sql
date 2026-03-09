-- Staging model: stg_exchange_rates
-- Grain        : One row per currency pair per date (deduplicated)
-- Materialized : view

WITH source AS (
    SELECT * FROM {{ source('kavya_raw', 'RAW_EXCHANGE_RATES') }}
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY RATE_DATE, BASE_CURRENCY, TARGET_CURRENCY
            ORDER BY _LOAD_TIMESTAMP DESC
        ) AS row_num
    FROM source
    WHERE RATE_DATE      IS NOT NULL
      AND EXCHANGE_RATE  IS NOT NULL
      AND EXCHANGE_RATE  > 0
),

final AS (
    SELECT
        RATE_DATE                           AS rate_date,
        UPPER(BASE_CURRENCY)                AS base_currency,
        UPPER(TARGET_CURRENCY)              AS target_currency,
        CAST(EXCHANGE_RATE AS FLOAT)        AS exchange_rate,
        RATE_SOURCE                         AS rate_source,
        _SOURCE_FILE                        AS source_file,
        _LOAD_TIMESTAMP                     AS load_timestamp
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM final
