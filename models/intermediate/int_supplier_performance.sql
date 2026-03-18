with enriched as (select * from {{ ref('int_shipments_enriched') }}),
supplier_stats as (
    select
        supplier_id, supplier_name, supplier_nation, supplier_region,
        count(*) as total_shipments,
        count(case when is_on_time then 1 end) as on_time_shipments,
        round(sum(net_revenue_usd), 2) as total_revenue_usd,
        round(avg(net_revenue_usd), 2) as avg_revenue_per_shipment_usd,
        round(avg(transit_days), 1) as avg_transit_days,
        round(avg(days_vs_commit), 1) as avg_days_vs_commit,
        round(100.0 * count(case when is_on_time then 1 end) / count(*), 1) as on_time_rate_pct
    from enriched
    group by 1,2,3,4
)
select *,
    rank() over (partition by supplier_region order by on_time_rate_pct desc) as rank_in_region,
    round(100.0 * total_revenue_usd / sum(total_revenue_usd) over (partition by supplier_region), 2) as pct_of_region_revenue
from supplier_stats
