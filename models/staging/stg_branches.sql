-- Staging model: stg_branches
-- Grain        : One row per branch (deduplicated)
-- Materialized : view

WITH source AS (
    SELECT * FROM {{ source('kavya_raw', 'RAW_BRANCHES') }}
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY BRANCH_ID
            ORDER BY _LOAD_TIMESTAMP DESC
        ) AS row_num
    FROM source
    WHERE BRANCH_ID   IS NOT NULL
      AND BRANCH_NAME IS NOT NULL
),

final AS (
    SELECT
        BRANCH_ID                           AS branch_id,
        TRIM(BRANCH_NAME)                   AS branch_name,
        UPPER(BRANCH_TYPE)                  AS branch_type,
        ADDRESS_LINE1                       AS address_line1,
        TRIM(CITY)                          AS city,
        TRIM(REGION)                        AS region,
        TRIM(COUNTRY)                       AS country,
        UPPER(POSTCODE)                     AS postcode,
        SORT_CODE                           AS sort_code,
        OPEN_DATE                           AS open_date,
        IS_ACTIVE                           AS is_active,
        MANAGER_NAME                        AS manager_name,
        PHONE_NUMBER                        AS phone_number,
        _SOURCE_FILE                        AS source_file,
        _LOAD_TIMESTAMP                     AS load_timestamp
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM final
