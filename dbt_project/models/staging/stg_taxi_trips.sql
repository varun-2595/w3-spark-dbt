with source as (
    select * from {{ source('raw', 'taxi_trips') }}
),

staged as (
    select
        cast("VendorID" as integer) as vendor_id,
        cast("tpep_pickup_datetime" as timestamp) as pickup_datetime,
        cast("tpep_dropoff_datetime" as timestamp) as dropoff_datetime,
        cast("passenger_count" as integer) as passenger_count,
        cast("trip_distance" as numeric(10, 2)) as trip_distance,
        cast("PULocationID" as integer) as pickup_location_id,
        cast("DOLocationID" as integer) as dropoff_location_id,
        cast("fare_amount" as numeric(10, 2)) as fare_amount,
        cast("extra" as numeric(10, 2)) as extra,
        cast("mta_tax" as numeric(10, 2)) as mta_tax,
        cast("tip_amount" as numeric(10, 2)) as tip_amount,
        cast("tolls_amount" as numeric(10, 2)) as tolls_amount,
        cast("improvement_surcharge" as numeric(10, 2)) as improvement_surcharge,
        cast("total_amount" as numeric(10, 2)) as total_amount,
        cast("store_and_fwd_flag" as varchar(1)) as store_and_fwd_flag,
        cast("driver_name" as varchar(100)) as driver_name,

        -- Simulated dimension keys (kept for star-schema compatibility)
        case
            when cast("VendorID" as integer) = 1 then 1
            else (cast("passenger_count" as integer) % 5) + 1
        end as rate_code_id,
        (cast("passenger_count" as integer) % 2) + 1 as payment_type_id,

        -- Calculated fields
        extract(epoch from (cast("tpep_dropoff_datetime" as timestamp) - cast("tpep_pickup_datetime" as timestamp))) / 60.0 as trip_duration_minutes,

        -- Mask driver name to initials (real NYC TLC data carries no driver PII,
        -- so this is NULL-safe: NULL driver_name -> NULL initials)
        case
            when "driver_name" is null then null
            when array_length(string_to_array(cast("driver_name" as varchar(100)), ' '), 1) >= 2 then
                concat(
                    substring(split_part(cast("driver_name" as varchar(100)), ' ', 1), 1, 1), '.',
                    substring(split_part(cast("driver_name" as varchar(100)), ' ', 2), 1, 1), '.'
                )
            else
                concat(substring(cast("driver_name" as varchar(100)), 1, 1), '.')
        end as driver_initials,

        -- Unique fingerprint hash.
        -- SHA-256 (lowercase hex via pgcrypto digest), matching EXACTLY the
        -- normalized expression used in src/pyspark_pipeline.py (FINGERPRINT_SPEC):
        -- same algorithm, same column order, same normalization
        -- (ints -> bigint::text, timestamps -> 'YYYY-MM-DD HH24:MI:SS',
        --  monetary/decimal -> numeric(12,2)::text). concat_ws skips NULLs in
        -- both PostgreSQL and Spark, so columns absent on either side
        -- (e.g. driver_name in real TLC data) drop out identically.
        encode(digest(concat_ws('||',
            cast(cast("VendorID" as bigint) as varchar),
            to_char(cast("tpep_pickup_datetime" as timestamp), 'YYYY-MM-DD HH24:MI:SS'),
            to_char(cast("tpep_dropoff_datetime" as timestamp), 'YYYY-MM-DD HH24:MI:SS'),
            cast(cast("passenger_count" as bigint) as varchar),
            cast(cast("trip_distance" as numeric(12,2)) as varchar),
            cast(cast("RatecodeID" as bigint) as varchar),
            cast("store_and_fwd_flag" as varchar),
            cast(cast("PULocationID" as bigint) as varchar),
            cast(cast("DOLocationID" as bigint) as varchar),
            cast(cast("payment_type" as bigint) as varchar),
            cast(cast("fare_amount" as numeric(12,2)) as varchar),
            cast(cast("extra" as numeric(12,2)) as varchar),
            cast(cast("mta_tax" as numeric(12,2)) as varchar),
            cast(cast("tip_amount" as numeric(12,2)) as varchar),
            cast(cast("tolls_amount" as numeric(12,2)) as varchar),
            cast(cast("improvement_surcharge" as numeric(12,2)) as varchar),
            cast(cast("total_amount" as numeric(12,2)) as varchar),
            cast(cast("congestion_surcharge" as numeric(12,2)) as varchar),
            cast(cast("Airport_fee" as numeric(12,2)) as varchar),
            cast("driver_name" as varchar)
        ), 'sha256'), 'hex') as trip_fingerprint

    from source
),

filtered as (
    select * from staged
    where fare_amount >= 2.5
      and dropoff_datetime > pickup_datetime
      and pickup_location_id between 1 and 263
      and dropoff_location_id between 1 and 263
),

deduped as (
    select *,
           row_number() over (partition by trip_fingerprint order by pickup_datetime) as row_num
    from filtered
)

select
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    pickup_location_id,
    dropoff_location_id,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    store_and_fwd_flag,
    rate_code_id,
    payment_type_id,
    trip_duration_minutes,
    driver_initials,
    trip_fingerprint
from deduped
where row_num = 1
