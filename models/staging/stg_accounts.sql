-- Staging model: stg_accounts
-- Grain        : One row per unique account (deduplicated)
-- Materialized : view

WITH source AS (
    SELECT * FROM {{ source('kavya_raw', 'RAW_ACCOUNTS') }}
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY ACCOUNT_ID
            ORDER BY _LOAD_TIMESTAMP DESC
        ) AS row_num
    FROM source
    WHERE ACCOUNT_ID  IS NOT NULL
      AND CUSTOMER_ID IS NOT NULL
),

final AS (
    SELECT
        ACCOUNT_ID                          AS account_id,
        CUSTOMER_ID                         AS customer_id,
        UPPER(ACCOUNT_TYPE)                 AS account_type,
        UPPER(ACCOUNT_STATUS)               AS account_status,
        OPEN_DATE                           AS open_date,
        CLOSE_DATE                          AS close_date,
        UPPER(CURRENCY_CODE)                AS currency_code,
        CAST(CURRENT_BALANCE AS FLOAT)      AS current_balance,
        CAST(OVERDRAFT_LIMIT AS FLOAT)      AS overdraft_limit,
        CAST(INTEREST_RATE   AS FLOAT)      AS interest_rate,
        BRANCH_ID                           AS branch_id,
        SORT_CODE                           AS sort_code,
        _SOURCE_FILE                        AS source_file,
        _LOAD_TIMESTAMP                     AS load_timestamp
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM final
