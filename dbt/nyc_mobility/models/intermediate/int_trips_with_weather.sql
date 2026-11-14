with valid_trips as (

    select * from {{ ref('int_trips_validated') }}
    where rejection_reason is null

)

select
    t.*,
    w.temperature_2m,
    w.precipitation,
    w.rain,
    w.snowfall,
    w.wind_speed_10m,
    coalesce(w.is_rainy, false) as is_rainy
from valid_trips as t
left join {{ ref('stg_weather_hourly') }} as w
    on datetime_trunc(pickup_datetime, hour) = observed_at
