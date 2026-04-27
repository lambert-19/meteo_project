-- Ce test vérifie que chaque relevé météo a été correctement associé à une ville 
-- dans le snapshot SCD Type 2. Si un city_name est NULL, la jointure a échoué 
-- (ID inconnu ou date hors plage de validité).

select
    weather_id
from {{ ref('stg_weather_history') }}
where city_name is null