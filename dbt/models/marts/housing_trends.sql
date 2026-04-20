-- Mart model: house price trends by county and property type

WITH staging AS (
    SELECT * FROM {{ ref('stg_housing') }}
),

by_county AS (
    SELECT
        county,
        COUNT(*)                            AS total_sales,
        ROUND(AVG(price), 0)               AS avg_price,
        ROUND(MIN(price), 0)               AS min_price,
        ROUND(MAX(price), 0)               AS max_price
    FROM staging
    GROUP BY county
),

by_property_type AS (
    SELECT
        property_type,
        COUNT(*)                            AS total_sales,
        ROUND(AVG(price), 0)               AS avg_price
    FROM staging
    GROUP BY property_type
),

new_vs_existing AS (
    SELECT
        old_new,
        COUNT(*)                            AS total_sales,
        ROUND(AVG(price), 0)               AS avg_price
    FROM staging
    GROUP BY old_new
)

SELECT
    b.county,
    b.total_sales,
    b.avg_price,
    b.min_price,
    b.max_price
FROM by_county b
ORDER BY avg_price DESC