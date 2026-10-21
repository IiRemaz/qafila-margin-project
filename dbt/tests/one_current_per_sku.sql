select sku
from {{ ref('product_cost_history') }}
where is_current
group by sku
having count(*) <> 1s