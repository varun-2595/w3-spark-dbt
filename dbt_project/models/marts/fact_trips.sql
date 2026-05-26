with trips as (
    select * from {{ ref('stg_taxi_trips') }}
)

select
    trip_fingerprint,
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    cast(pickup_datetime as date) as pickup_date,
    passenger_count,
    trip_distance,
    pickup_location_id,
    dropoff_location_id,
    rate_code_id,
    payment_type_id,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    store_and_fwd_flag,
    driver_initials,
    trip_duration_minutes
from trips
