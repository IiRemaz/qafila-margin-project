with src as (
    select
        trim(sku) as sku,
        try_cast(valid_from as date) as valid_from,
        source_file,
        coalesce(nullif(trim(cost_sar), ''), nullif(trim(unit_cost), '')) as raw_cost_trim,
        coalesce(nullif(cost_sar, ''), nullif(unit_cost, '')) as raw_cost
    from {{ source('raw', 'supplier_prices') }}
)
select
    sku, valid_from, source_file, raw_cost,
    try_cast(
        replace(
            translate(
                raw_cost_trim,
                chr(1632)||chr(1633)||chr(1634)||chr(1635)||chr(1636)||chr(1637)
                ||chr(1638)||chr(1639)||chr(1640)||chr(1641)||chr(1643),
                '0123456789.'
            ),
            ',', '.'
        ) as double
    ) as cost
from src