with weather as (
    select
        city_id,
        observed_at,
        temperature_min
    from {{ ref('stg_weather_history') }}
),

cities as (
    select
        city_id,
        city_name
    from {{ source('meteo_raw', 'dim_cities') }}
),

joined as (
    select
        c.city_name,
        w.observed_at,
        w.temperature_min
    from weather w
    join cities c on w.city_id = c.city_id
),

aggregated as (
    select
        city_name,
        count(*) as total_days_monitored,
        -- Décompte des jours où la température min est sous zéro
        sum(case when temperature_min < 0 then 1 else 0 end) as frost_days_count,
        -- Calcul de la proportion de jours de gel
        round(100.0 * sum(case when temperature_min < 0 then 1 else 0 end) / count(*), 2) as frost_days_percentage,
        min(observed_at) as data_since,
        max(observed_at) as data_until
    from joined
    group by 1
)

select
    city_name,
    total_days_monitored,
    frost_days_count,
    frost_days_percentage,
    data_since,
    data_until
from aggregated
order by frost_days_count desc