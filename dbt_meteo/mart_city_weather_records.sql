with weather as (
    select
        city_id,
        observed_at,
        temperature_min,
        temperature_max,
        precipitation_mm
    from {{ ref('stg_weather_history') }}
),

cities as (
    select
        city_id,
        city_name,
        region
    from {{ source('meteo_raw', 'dim_cities') }}
),

joined as (
    select
        c.city_name,
        c.region,
        w.observed_at,
        w.temperature_min,
        w.temperature_max,
        w.precipitation_mm
    from weather w
    join cities c on w.city_id = c.city_id
),

aggregated as (
    select
        city_name,
        region,
        min(temperature_min) as record_min_temp,
        max(temperature_max) as record_max_temp,
        max(precipitation_mm) as record_daily_precipitation,
        count(*) as total_days_monitored,
        min(observed_at) as data_since,
        max(observed_at) as data_until
    from joined
    group by 1, 2
)

select
    city_name,
    region,
    record_min_temp,
    record_max_temp,
    record_daily_precipitation,
    total_days_monitored,
    data_since,
    data_until
from aggregated