{#- weather_source = python: raw_weather.weather_hourly (one row per hour).
    weather_source = airbyte: raw_weather.open_meteo_archive. Assumes the Airbyte
    column `hourly` has type JSON and holds arrays time, temperature_2m,
    precipitation, rain, snowfall and wind_speed_10m, zipped here by array offset. -#}

{% if var('weather_source') == 'airbyte' %}

with src as (

    select * from {{ source('raw_weather', 'open_meteo_archive') }}

),

unnested as (

    select
        parse_datetime('%Y-%m-%dT%H:%M', t) as observed_at,
        safe_cast(json_value_array(hourly, '$.temperature_2m')[safe_offset(i)] as float64) as temperature_2m,
        safe_cast(json_value_array(hourly, '$.precipitation')[safe_offset(i)] as float64) as precipitation,
        safe_cast(json_value_array(hourly, '$.rain')[safe_offset(i)] as float64) as rain,
        safe_cast(json_value_array(hourly, '$.snowfall')[safe_offset(i)] as float64) as snowfall,
        safe_cast(json_value_array(hourly, '$.wind_speed_10m')[safe_offset(i)] as float64) as wind_speed_10m
    from src,
        unnest(json_value_array(hourly, '$.time')) as t with offset as i

)

select
    observed_at,
    date(observed_at) as observed_date,
    extract(hour from observed_at) as observed_hour,
    temperature_2m,
    precipitation,
    rain,
    snowfall,
    wind_speed_10m,
    rain > 0.1 as is_rainy,
    date_trunc(date(observed_at), month) as source_month
from unnested

{% else %}

select
    observed_at,
    date(observed_at) as observed_date,
    extract(hour from observed_at) as observed_hour,
    cast(temperature_2m as float64) as temperature_2m,
    cast(precipitation as float64) as precipitation,
    cast(rain as float64) as rain,
    cast(snowfall as float64) as snowfall,
    cast(wind_speed_10m as float64) as wind_speed_10m,
    rain > 0.1 as is_rainy,
    source_month
from {{ source('raw_weather', 'weather_hourly') }}

{% endif %}
