with date_series as (
    select generate_series(
        '2024-01-01'::date,
        '2026-12-31'::date,
        '1 day'::interval
    )::date as date_day
)

select
    date_day,
    cast(extract(year from date_day) as integer) as year,
    cast(extract(month from date_day) as integer) as month,
    cast(extract(day from date_day) as integer) as day,
    cast(extract(dow from date_day) as integer) as day_of_week,
    to_char(date_day, 'Month') as month_name,
    to_char(date_day, 'Day') as day_name,
    case when extract(dow from date_day) in (0, 6) then true else false end as is_weekend
from date_series
