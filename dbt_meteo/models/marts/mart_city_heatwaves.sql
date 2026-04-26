{{
    config(
        materialized='table',
        description='Identifies heatwave episodes per city'
    )
}}

WITH weather_data AS (
    SELECT
        city_id,
        observed_at as date,
        temperature_max as temp_max,
        LAG(observed_at) OVER (PARTITION BY city_id ORDER BY observed_at) AS prev_date,
        LAG(temperature_max) OVER (PARTITION BY city_id ORDER BY observed_at) AS prev_temp_max
    FROM {{ ref('stg_weather_history') }}
),

-- Identify heatwave days (days where temp_max exceeds threshold)
heatwave_days AS (
    SELECT
        city_id,
        date,
        temp_max,
        CASE
            WHEN temp_max >= {{ var('heatwave_temp_threshold') }} THEN 1
            ELSE 0
        END AS is_heatwave_day,
        CASE
            WHEN prev_temp_max IS NULL OR prev_temp_max < {{ var('heatwave_temp_threshold') }} THEN 1
            ELSE 0
        END AS is_new_episode
    FROM weather_data
),

-- Assign episode numbers to consecutive heatwave days
episodes AS (
    SELECT
        city_id,
        date,
        temp_max,
        is_heatwave_day,
        SUM(is_new_episode) OVER (PARTITION BY city_id ORDER BY date) AS episode_id
    FROM heatwave_days
    WHERE is_heatwave_day = 1
),

-- Aggregate heatwave episodes by city and episode
heatwave_episodes AS (
    SELECT
        city_id,
        episode_id,
        COUNT(DISTINCT date) AS duration_days,
        ROUND(AVG(temp_max), 2) AS avg_temp_max,
        MIN(date) AS start_date,
        MAX(date) AS end_date
    FROM episodes
    GROUP BY 1, 2
    HAVING COUNT(DISTINCT date) >= {{ var('heatwave_min_duration') }}
)

-- Join with dimensions for readable output
SELECT
    c.city_name,
    h.episode_id,
    h.duration_days,
    h.avg_temp_max,
    h.start_date,
    h.end_date
FROM heatwave_episodes h
JOIN {{ source('meteo_raw', 'dim_cities') }} c ON h.city_id = c.city_id
ORDER BY city_name, start_date
