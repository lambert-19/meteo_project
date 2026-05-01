with source as (
    select
        weather_id,
        city_id,
        date_id,
        timestamp,
        temperature,
        temp_min,
        temp_max,
        precipitation_sum,
        weather_description,
        wind_speed,
        extracted_at
    from {{ source('meteo_raw', 'fct_weather_history') }}
),
cities_snapshot as (
    select * from {{ ref('snp_cities') }}
),
cleaned as (
    select
        weather_id,
        city_id,
        date_id,
        cast(timestamp as timestamp) as observed_at,
        cast(temperature as float) as temperature_avg,
        cast(temp_min as float) as temperature_min,
        cast(temp_max as float) as temperature_max,
        coalesce(cast(precipitation_sum as float), 0.0) as precipitation_mm,
        null::float as pressure_hpa,
        null::integer as humidity_pct,
        lower(weather_description) as weather_condition,
        cast(wind_speed as float) as wind_speed_ms,
        cast(extracted_at as timestamp) as loaded_at
    from source
),
joined as (
    select
        w.*,
        c.city_name,
        c.region,
        c.elevation
    from cleaned w
    left join cities_snapshot c
        on w.city_id = c.city_id
        and w.observed_at >= c.dbt_valid_from
        and w.observed_at < coalesce(c.dbt_valid_to, cast('9999-12-31' as timestamp))
)
select
    weather_id,
    city_id,
    city_name,
    region,
    elevation,
    date_id,
    observed_at,
    temperature_avg,
    temperature_min,
    temperature_max,
    precipitation_mm,
    pressure_hpa,
    humidity_pct,
    weather_condition,
    wind_speed_ms,
    loaded_at
from joined
