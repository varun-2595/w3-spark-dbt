with staged as (
    select * from {{ ref('stg_taxi_zones') }}
)

select
    location_id,
    borough,
    zone_name,
    service_zone
from staged
