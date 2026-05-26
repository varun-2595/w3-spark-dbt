with source as (
    select * from {{ source('raw', 'taxi_zones') }}
),

staged as (
    select
        cast("LocationID" as integer) as location_id,
        cast("Borough" as varchar(50)) as borough,
        cast("Zone" as varchar(100)) as zone_name,
        cast("service_zone" as varchar(50)) as service_zone
    from source
)

select * from staged
