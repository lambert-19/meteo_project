with weather as (
    select
        city_id,
        temperature_min,
        temperature_max
    from {{ ref('stg_weather_history') }}
),

cities as (
    select
        city_id,
        city_name
    from {{ source('meteo_raw', 'dim_cities') }}
),

daily_amplitude as (
    select
        c.city_name,
        (w.temperature_max - w.temperature_min) as thermal_amplitude
    from weather w
    join cities c on w.city_id = c.city_id
),

aggregated as (
    select
        city_name,
        -- Moyenne de l'amplitude thermique
        round(avg(thermal_amplitude), 2) as avg_thermal_amplitude,
        -- Record d'amplitude minimale (journée la plus stable)
        round(min(thermal_amplitude), 2) as min_thermal_amplitude,
        -- Record d'amplitude maximale (journée avec le plus grand écart)
        round(max(thermal_amplitude), 2) as max_thermal_amplitude,
        -- Nombre de jours pris en compte
        count(*) as observation_count
    from daily_amplitude
    group by 1
)

select
    city_name,
    avg_thermal_amplitude,
    min_thermal_amplitude,
    max_thermal_amplitude,
    observation_count
from aggregated
order by avg_thermal_amplitude desc