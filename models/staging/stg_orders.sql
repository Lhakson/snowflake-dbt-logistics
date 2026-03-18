with source as (
    select * from {{ source('tpch', 'orders') }}
),
renamed as (
    select
        o_orderkey      as order_id,
        o_custkey       as customer_id,
        o_orderstatus   as order_status_code,
        o_totalprice    as order_total_usd,
        o_orderdate     as order_date,
        o_orderpriority as order_priority,
        o_clerk         as clerk,
        o_shippriority  as ship_priority,
        case o_orderstatus
            when 'O' then 'open'
            when 'F' then 'fulfilled'
            when 'P' then 'pending'
        end as order_status
    from source
)
select * from renamed
