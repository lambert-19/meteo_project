-- On utilise la macro avec des paramètres personnalisables
with heatwave_events as (
    {{ get_heatwave_events(
        source_model=ref('stg_weather_history')
    ) }}
),

cities as (
    select
        city_id,
        city_name
    from {{ source('meteo_raw', 'dim_cities') }}
)

-- 4. Jointure finale pour obtenir le nom de la ville et les détails de l'événement
select
    c.city_name,
    h.start_date,
    h.end_date,
    h.duration_days,
    h.avg_temp_max
from heatwave_events h
join cities c on h.city_id = c.city_id
order by 
    h.start_date desc, 
    h.duration_days desc