
WITH source_data AS (
    SELECT
        TRIM(sku) AS sku,
        valid_from,
        source_file,
        unit_cost,
        cost_sar
    FROM {{ source('raw', 'supplier_prices') }}
),

normalized AS (
    SELECT
        sku,
        TRY_CAST(valid_from AS DATE) AS valid_from,
        source_file,
        COALESCE(
            NULLIF(TRIM(cost_sar), ''),
            NULLIF(TRIM(unit_cost), '')
        ) AS raw_cost
    FROM source_data
),

cleaned AS (
    SELECT
        sku,
        valid_from,
        source_file,
        raw_cost,
        TRY_CAST(
            REPLACE(
                REPLACE(
                    TRANSLATE(
                        raw_cost,
                        '٠١٢٣٤٥٦٧٨٩٫٬',
                        '0123456789..'
                    ),
                    ',',
                    '.'
                ),
                ' ',
                ''
            ) AS DECIMAL(18, 2)
        ) AS unit_cost
    FROM normalized
)

SELECT
    sku,
    valid_from,
    source_file,
    raw_cost,
    unit_cost
FROM cleaned
