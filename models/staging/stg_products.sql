-- Staging model: stg_products
-- Grain        : One row per product (deduplicated)
-- Materialized : view

WITH source AS (
    SELECT * FROM {{ source('kavya_raw', 'RAW_PRODUCTS') }}
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY PRODUCT_ID
            ORDER BY _LOAD_TIMESTAMP DESC
        ) AS row_num
    FROM source
    WHERE PRODUCT_ID   IS NOT NULL
      AND PRODUCT_NAME IS NOT NULL
),

final AS (
    SELECT
        PRODUCT_ID                          AS product_id,
        TRIM(PRODUCT_NAME)                  AS product_name,
        UPPER(PRODUCT_TYPE)                 AS product_type,
        UPPER(PRODUCT_CATEGORY)             AS product_category,
        CAST(INTEREST_RATE   AS FLOAT)      AS interest_rate,
        CAST(MONTHLY_FEE     AS FLOAT)      AS monthly_fee,
        CAST(MIN_BALANCE     AS FLOAT)      AS min_balance,
        CAST(MAX_CREDIT_LIMIT AS FLOAT)     AS max_credit_limit,
        ELIGIBILITY_CRITERIA                AS eligibility_criteria,
        IS_ACTIVE                           AS is_active,
        LAUNCH_DATE                         AS launch_date,
        DISCONTINUE_DATE                    AS discontinue_date,
        _SOURCE_FILE                        AS source_file,
        _LOAD_TIMESTAMP                     AS load_timestamp
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM final
