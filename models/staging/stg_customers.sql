-- Staging model: stg_customers
-- Grain        : One row per unique customer (deduplicated)
-- Materialized : view (lightweight, always fresh)
-- Why view?    : Staging is just renaming + casting. No need to store a copy.

WITH source AS (
    SELECT * FROM {{ source('kavya_raw', 'RAW_CUSTOMERS') }}
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY CUSTOMER_ID
            ORDER BY _LOAD_TIMESTAMP DESC
        ) AS row_num
    FROM source
    WHERE CUSTOMER_ID IS NOT NULL      -- filter invalid records
      AND FIRST_NAME  IS NOT NULL
),

final AS (
    SELECT
        -- Keys
        CUSTOMER_ID                                     AS customer_id,

        -- Names
        TRIM(FIRST_NAME)                                AS first_name,
        TRIM(LAST_NAME)                                 AS last_name,
        TRIM(FIRST_NAME) || ' ' || TRIM(LAST_NAME)     AS full_name,

        -- Demographics
        DATE_OF_BIRTH                                   AS date_of_birth,
        UPPER(GENDER)                                   AS gender,

        -- Contact (kept for internal use — masked in secure view)
        LOWER(EMAIL)                                    AS email,
        PHONE_NUMBER                                    AS phone_number,

        -- Address
        ADDRESS_LINE1                                   AS address_line1,
        TRIM(CITY)                                      AS city,
        TRIM(COUNTRY)                                   AS country,
        UPPER(POSTCODE)                                 AS postcode,

        -- Account info
        ACCOUNT_OPEN_DATE                               AS account_open_date,
        UPPER(CUSTOMER_SEGMENT)                         AS customer_segment,
        UPPER(KYC_STATUS)                               AS kyc_status,
        CAST(CREDIT_SCORE AS INT)                       AS credit_score,
        IS_ACTIVE                                       AS is_active,

        -- Audit
        _SOURCE_FILE                                    AS source_file,
        _LOAD_TIMESTAMP                                 AS load_timestamp

    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM final
