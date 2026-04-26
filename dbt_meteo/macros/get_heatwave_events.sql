{% macro get_heatwave_events(
    source_model, 
    temp_threshold=var('heatwave_temp_threshold', 30), 
    min_duration=var('heatwave_min_duration', 3)
) %}

with hot_days as (
    select
        city_id,
        observed_at,
        temperature_max
    from {{ source_model }}
    where temperature_max > {{ temp_threshold }}
),

-- Logique Gaps and Islands pour identifier les séquences consécutives
grouped_days as (
    select
        city_id,
        observed_at,
        temperature_max,
        -- Dans DuckDB, soustraire un entier à une date retire X jours
        cast(observed_at as date) - cast(row_number() over (partition by city_id order by observed_at) as integer) as group_id
    from hot_days
),

heatwave_events as (
    select
        city_id,
        min(observed_at) as start_date,
        max(observed_at) as end_date,
        count(*) as duration_days,
        round(avg(temperature_max), 2) as avg_temp_max
    from grouped_days
    group by city_id, group_id
    having count(*) >= {{ min_duration }}
)

select * from heatwave_events

{% endmacro %}