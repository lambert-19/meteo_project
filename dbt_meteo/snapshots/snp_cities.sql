{% snapshot snp_cities %}
{{
    config(
      target_schema='main',
      unique_key='city_id',
      strategy='check',
      check_cols=['city_name', 'region', 'elevation', 'latitude', 'longitude'],
      invalidate_hard_deletes=True,
    )
}}
select 
    city_id,
    city_name,
    region,
    elevation,
    latitude,
    longitude
from {{ source('meteo_raw', 'dim_cities') }}

{% endsnapshot %}