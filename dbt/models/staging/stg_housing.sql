-- Staging model: clean and standardise raw housing data

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_housing') }}
),

cleaned AS (
    SELECT
        TRANSACTION_ID,

        -- Convert price to integer
        TRY_CAST(PRICE AS INTEGER)                          AS price,

        -- Convert date string to proper date
        TRY_CAST(DATE_OF_TRANSFER AS DATE)                  AS date_of_transfer,

        POSTCODE,

        -- Decode property type codes
        CASE PROPERTY_TYPE
            WHEN 'D' THEN 'Detached'
            WHEN 'S' THEN 'Semi-Detached'
            WHEN 'T' THEN 'Terraced'
            WHEN 'F' THEN 'Flat'
            WHEN 'O' THEN 'Other'
            ELSE 'Unknown'
        END                                                  AS property_type,

        -- Decode old/new flag
        CASE OLD_NEW
            WHEN 'Y' THEN 'New Build'
            WHEN 'N' THEN 'Existing'
            ELSE 'Unknown'
        END                                                  AS old_new,

        -- Decode duration
        CASE DURATION
            WHEN 'F' THEN 'Freehold'
            WHEN 'L' THEN 'Leasehold'
            ELSE 'Unknown'
        END                                                  AS duration,

        PAON                                                 AS house_number,

        -- Replace nan with empty string
        CASE WHEN SAON = 'nan' THEN '' ELSE SAON END        AS flat_number,
        STREET,

        -- Replace nan locality with empty string
        CASE WHEN LOCALITY = 'nan' THEN '' ELSE LOCALITY END AS locality,

        TOWN,
        DISTRICT,
        COUNTY,
        PPD_CATEGORY,
        RECORD_STATUS

    FROM source
    WHERE RECORD_STATUS = 'A'  -- only include active records
)

SELECT * FROM cleaned