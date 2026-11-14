{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'pickup_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by=['pu_location_id', 'do_location_id'],
    require_partition_filter=true
) }}

select
    trip_id,
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    pickup_date,
    pickup_hour,
    pu_location_id,
    do_location_id,
    passenger_count,
    trip_distance,
    trip_duration_min,
    payment_type,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,
    congestion_surcharge,
    airport_fee,
    temperature_2m,
    rain,
    snowfall,
    wind_speed_10m,
    is_rainy,
    source_month
from {{ ref('int_trips_with_weather') }}
{% if is_incremental() %}
where pickup_date between date_sub(date('{{ var("run_month_start") }}'), interval {{ var("lookback_days", 3) }} day)
    and last_day(date('{{ var("run_month_start") }}'))
{% endif %}
