with shipments as (select * from {{ ref('fct_shipments') }})
select
    order_id, customer_id, customer_region, market_segment, order_date, order_status, order_priority,
    count(line_number) as total_line_items,
    sum(quantity) as total_quantity,
    round(sum(net_revenue_usd), 2) as net_revenue_usd,
    round(sum(discount_amount_usd), 2) as total_discount_usd,
    count(distinct supplier_id) as unique_suppliers,
    min(ship_date) as first_ship_date,
    max(receipt_date) as last_receipt_date,
    count(case when is_on_time then 1 end) as on_time_lines,
    count(case when not is_on_time then 1 end) as late_lines,
    case when count(case when not is_on_time then 1 end) = 0 then true else false end as is_fully_on_time
from shipments
group by 1,2,3,4,5,6,7
