select
    order_id, store_id, status,
    cast(cast(substr(replace(created_at, 'T', ' '), 1, 19) as timestamp) + interval 3 hour as date) as order_date
from {{ source('raw', 'orders') }}