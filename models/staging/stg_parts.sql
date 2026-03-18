with source as (select * from {{ source('tpch', 'part') }})
select p_partkey as part_id, p_name as part_name, p_mfgr as manufacturer,
       p_brand as brand, p_type as part_type, p_size as part_size,
       p_container as container_type, p_retailprice as retail_price_usd
from source
