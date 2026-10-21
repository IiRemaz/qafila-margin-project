with valid as (
    select * from {{ ref('int_supplier_prices_checked') }}
    where reason is null and valid_from is not null
),
dedup as (
    select sku, valid_from, round(cost, 2) as unit_cost_sar
    from (
        select *, row_number() over (partition by sku, valid_from order by source_file desc) as rn
        from valid
    )
    where rn = 1
),
with_prev as (
    select *, lag(unit_cost_sar) over (partition by sku order by valid_from) as prev_cost
    from dedup
),
versions as (
    select * from with_prev
    where prev_cost is null or unit_cost_sar <> prev_cost
)
select
    sku,
    unit_cost_sar,
    valid_from,
    lead(valid_from) over (partition by sku order by valid_from) as valid_to,
    lead(valid_from) over (partition by sku order by valid_from) is null as is_current
from versions