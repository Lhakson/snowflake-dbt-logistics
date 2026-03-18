with performance as (select * from {{ ref('int_supplier_performance') }})
select
    supplier_id, supplier_name, supplier_nation, supplier_region,
    total_shipments, on_time_shipments, on_time_rate_pct,
    avg_transit_days, avg_days_vs_commit, total_revenue_usd,
    avg_revenue_per_shipment_usd, rank_in_region, pct_of_region_revenue,
    case
        when on_time_rate_pct >= 90 then 'elite'
        when on_time_rate_pct >= 75 then 'reliable'
        when on_time_rate_pct >= 60 then 'average'
        else 'at_risk'
    end as performance_tier
from performance
