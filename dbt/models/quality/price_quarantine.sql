select source_file, sku, valid_from, raw_cost, reason
from {{ ref('int_supplier_prices_checked') }}
where reason is not null