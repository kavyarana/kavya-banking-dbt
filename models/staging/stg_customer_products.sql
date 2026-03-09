-- Staging model: stg_customer_products
-- Grain        : One row per customer-product subscription (deduplicated)
-- Materialized : view

WITH source AS (
    SELECT * FROM {{ source('kavya_raw', 'RAW_CUSTOMER_PRODUCTS') }}
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY SUBSCRIPTION_ID
            ORDER BY _LOAD_TIMESTAMP DESC
        ) AS row_num
    FROM source
    WHERE SUBSCRIPTION_ID IS NOT NULL
      AND CUSTOMER_ID     IS NOT NULL
      AND PRODUCT_ID      IS NOT NULL
),

final AS (
    SELECT
        SUBSCRIPTION_ID                     AS subscription_id,
        CUSTOMER_ID                         AS customer_id,
        PRODUCT_ID                          AS product_id,
        ACCOUNT_ID                          AS account_id,
        SUBSCRIPTION_DATE                   AS subscription_date,
        EXPIRY_DATE                         AS expiry_date,
        UPPER(STATUS)                       AS status,
        CAST(MONTHLY_CHARGE   AS FLOAT)     AS monthly_charge,
        CAST(DISCOUNT_APPLIED AS FLOAT)     AS discount_applied,
        _SOURCE_FILE                        AS source_file,
        _LOAD_TIMESTAMP                     AS load_timestamp
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM final
