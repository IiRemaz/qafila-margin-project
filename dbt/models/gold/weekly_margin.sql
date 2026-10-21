with lines as (
    select
        o.order_date,
        cast(extract(week from o.order_date) as integer) as iso_week,
        st.city, p.category, p.sku,
        oi.quantity,
        round(oi.quantity * oi.unit_price - oi.discount, 2) as revenue
    from {{ ref('stg_order_items') }} oi
    join {{ ref('stg_orders') }} o on o.order_id = oi.order_id
    join {{ ref('stg_products') }} p on p.product_id = oi.product_id
    join {{ ref('stg_stores') }} st on st.store_id = o.store_id
    where o.status = 'delivered'
      and oi.quantity > 0
      and o.order_date >= date '2026-08-31'
),
costed as (
    select l.*, h.unit_cost_sar
    from lines l
    left join {{ ref('product_cost_history') }} h
      on h.sku = l.sku
     and h.valid_from <= l.order_date
     and (h.valid_to is null or l.order_date < h.valid_to)
)
select
    iso_week, city, category,
    cast(count(*) as integer) as lines,
    round(sum(revenue), 2) as revenue_sar,
    round(coalesce(sum(revenue) filter (where unit_cost_sar is not null), 0), 2) as costed_revenue_sar,
    round(coalesce(sum(quantity * unit_cost_sar), 0), 2) as cost_sar,
    round(coalesce(sum(revenue) filter (where unit_cost_sar is not null), 0)
          - coalesce(sum(quantity * unit_cost_sar), 0), 2) as margin_sar,
    cast(count(*) filter (where unit_cost_sar is null) as integer) as uncosted_lines
from costed
group by iso_week, city, category