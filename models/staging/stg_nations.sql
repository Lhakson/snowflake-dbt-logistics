with nations as (select * from {{ source('tpch', 'nation') }}),
regions as (select * from {{ source('tpch', 'region') }})
select n.n_nationkey as nation_id, n.n_name as nation_name,
       r.r_regionkey as region_id, r.r_name as region_name
from nations n
inner join regions r on n.n_regionkey = r.r_regionkey
