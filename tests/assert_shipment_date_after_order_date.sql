select
    order_id
from {{ ref('stg_transportation_orders') }}
where shipment_date < order_date