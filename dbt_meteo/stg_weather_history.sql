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
        pressure,
        humidity,
        weather_description,
        wind_speed,
        extracted_at
    from {{ source('meteo_raw', 'fct_weather_history') }}
),

cleaned as (
    select
        weather_id,
        city_id,
        date_id,
        -- Renommage pour clarifier qu'il s'agit de l'heure d'observation
        cast(timestamp as timestamp) as observed_at,
        cast(temperature as float) as temperature_avg,
        cast(temp_min as float) as temperature_min,
        cast(temp_max as float) as temperature_max,
        -- Gestion des précipitations nulles
        coalesce(cast(precipitation_sum as float), 0.0) as precipitation_mm,
        cast(pressure as float) as pressure_hpa,
        cast(humidity as integer) as humidity_pct,
        lower(weather_description) as weather_condition,
        cast(wind_speed as float) as wind_speed_ms,
        cast(extracted_at as timestamp) as loaded_at
    from source
)

select
    weather_id,
    city_id,
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
from cleaned