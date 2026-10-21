select
    s.*,
    case
        when p.sku is null then 'unknown_sku'
        when s.cost is null or s.cost <= 0 then 'invalid_cost'
    end as reason
from {{ ref('stg_supplier_prices') }} s
left join (select distinct sku from {{ ref('stg_products') }}) p on p.sku = s.sku