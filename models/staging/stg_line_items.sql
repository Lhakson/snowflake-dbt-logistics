with source as (
    select * from {{ source('tpch', 'lineitem') }}
),
renamed as (
    select
        l_orderkey      as order_id,
        l_partkey       as part_id,
        l_suppkey       as supplier_id,
        l_linenumber    as line_number,
        l_quantity      as quantity,
        l_extendedprice as extended_price_usd,
        l_discount      as discount_rate,
        l_tax           as tax_rate,
        l_returnflag    as return_flag,
        l_linestatus    as line_status_code,
        l_shipdate      as ship_date,
        l_commitdate    as commit_date,
        l_receiptdate   as receipt_date,
        l_shipinstruct  as ship_instructions,
        l_shipmode      as ship_mode,
        round(l_extendedprice * (1 - l_discount), 2) as net_revenue_usd,
        datediff('day', l_shipdate, l_receiptdate)    as transit_days,
        datediff('day', l_commitdate, l_receiptdate)  as days_vs_commit,
        case when l_receiptdate <= l_commitdate then true else false end as is_on_time,
        case l_linestatus when 'O' then 'open' when 'F' then 'fulfilled' end as line_status
    from source
)
select * from renamed
