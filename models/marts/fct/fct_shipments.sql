{{ config(materialized="incremental", unique_key=["order_id","line_number"], incremental_strategy="merge") }}
with enriched as (
    select * from {{ ref('int_shipments_enriched') }}
    {% if is_incremental() %}
    where ship_date > (select max(ship_date) from {{ this }})
    {% endif %}
)
select
    order_id, line_number, order_date, ship_date, commit_date, receipt_date,
    customer_id, supplier_id, part_id,
    order_status, order_priority, ship_mode, ship_instructions, line_status, return_flag,
    customer_region, supplier_region, market_segment, part_type, brand, container_type,
    quantity, extended_price_usd, discount_rate, tax_rate, net_revenue_usd, retail_price_usd,
    transit_days, days_vs_commit, is_on_time,
    round(extended_price_usd * discount_rate, 2) as discount_amount_usd,
    sum(net_revenue_usd) over (partition by customer_id order by ship_date rows between unbounded preceding and current row) as customer_running_revenue_usd,
    dense_rank() over (partition by supplier_id order by net_revenue_usd desc) as revenue_rank_for_supplier,
    current_timestamp() as loaded_at
from enriched
