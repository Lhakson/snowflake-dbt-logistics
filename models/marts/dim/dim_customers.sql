with customers as (select * from {{ ref('stg_customers') }}),
nations as (select * from {{ ref('stg_nations') }}),
order_summary as (
    select customer_id,
        count(order_id) as total_orders,
        round(sum(net_revenue_usd), 2) as lifetime_value_usd,
        round(avg(net_revenue_usd), 2) as avg_order_value_usd,
        min(order_date) as first_order_date,
        max(order_date) as latest_order_date
    from {{ ref('fct_order_summary') }}
    group by 1
)
select
    c.customer_id, c.customer_name, c.market_segment, c.account_balance_usd,
    n.nation_name, n.region_name,
    coalesce(o.total_orders, 0) as total_orders,
    coalesce(o.lifetime_value_usd, 0) as lifetime_value_usd,
    coalesce(o.avg_order_value_usd, 0) as avg_order_value_usd,
    o.first_order_date, o.latest_order_date,
    case
        when coalesce(o.lifetime_value_usd,0) > 1000000 then 'platinum'
        when coalesce(o.lifetime_value_usd,0) > 500000  then 'gold'
        when coalesce(o.lifetime_value_usd,0) > 100000  then 'silver'
        else 'bronze'
    end as value_tier
from customers c
left join nations n on c.nation_id = n.nation_id
left join order_summary o on c.customer_id = o.customer_id
