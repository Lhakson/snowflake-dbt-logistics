with line_items as (select * from {{ ref('stg_line_items') }}),
orders as (select * from {{ ref('stg_orders') }}),
suppliers as (select * from {{ ref('stg_suppliers') }}),
customers as (select * from {{ ref('stg_customers') }}),
parts as (select * from {{ ref('stg_parts') }}),
nations as (select * from {{ ref('stg_nations') }})
select
    li.order_id, li.line_number, li.part_id, li.supplier_id,
    o.order_date, o.order_status, o.order_priority, o.order_total_usd, o.customer_id,
    c.customer_name, c.market_segment,
    cust_nation.nation_name as customer_nation, cust_nation.region_name as customer_region,
    s.supplier_name,
    supp_nation.nation_name as supplier_nation, supp_nation.region_name as supplier_region,
    p.part_name, p.manufacturer, p.brand, p.part_type, p.container_type, p.retail_price_usd,
    li.quantity, li.extended_price_usd, li.discount_rate, li.tax_rate, li.net_revenue_usd,
    li.ship_date, li.commit_date, li.receipt_date, li.ship_mode, li.ship_instructions,
    li.transit_days, li.days_vs_commit, li.is_on_time, li.line_status, li.return_flag
from line_items li
inner join orders o on li.order_id = o.order_id
inner join customers c on o.customer_id = c.customer_id
inner join suppliers s on li.supplier_id = s.supplier_id
inner join parts p on li.part_id = p.part_id
left join nations cust_nation on c.nation_id = cust_nation.nation_id
left join nations supp_nation on s.nation_id = supp_nation.nation_id
