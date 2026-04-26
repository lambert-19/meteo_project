with weather as (
    select
        city_id,
        precipitation_mm
    from {{ ref('stg_weather_history') }}
),

cities as (
    select
        city_id,
        region
    from {{ source('meteo_raw', 'dim_cities') }}
),

joined as (
    select
        c.region,
        w.precipitation_mm
    from weather w
    join cities c on w.city_id = c.city_id
),

regional_stats as (
    select
        region,
        -- Moyenne des précipitations journalières par région
        round(avg(precipitation_mm), 2) as avg_daily_precipitation_mm,
        -- Somme totale pour la période
        round(sum(precipitation_mm), 2) as total_precipitation_mm,
        -- Maximum enregistré en une journée dans la région
        max(precipitation_mm) as max_one_day_precipitation_mm,
        -- Nombre de points de données (villes * jours)
        count(*) as observation_count
    from joined
    group by 1
)

select
    region,
    avg_daily_precipitation_mm,
    total_precipitation_mm,
    max_one_day_precipitation_mm,
    observation_count,
    -- Calcul de la part relative (en pourcentage) si besoin d'analyse comparative
    round(100.0 * total_precipitation_mm / sum(total_precipitation_mm) over (), 2) as pct_of_total_precipitation
from regional_stats
order by avg_daily_precipitation_mm desc