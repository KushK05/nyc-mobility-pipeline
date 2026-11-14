with src as (

    select * from {{ source('raw_tlc', 'yellow_trips') }}

)

select
    cast(safe_cast(VendorID as float64) as int64) as vendor_id,
    datetime(tpep_pickup_datetime) as pickup_datetime,
    datetime(tpep_dropoff_datetime) as dropoff_datetime,
    cast(safe_cast(passenger_count as float64) as int64) as passenger_count,
    cast(trip_distance as float64) as trip_distance,
    cast(safe_cast(RatecodeID as float64) as int64) as ratecode_id,
    cast(store_and_fwd_flag as string) as store_and_fwd_flag,
    cast(PULocationID as int64) as pu_location_id,
    cast(DOLocationID as int64) as do_location_id,
    cast(payment_type as int64) as payment_type,
    cast(fare_amount as float64) as fare_amount,
    cast(extra as float64) as extra,
    cast(mta_tax as float64) as mta_tax,
    cast(tip_amount as float64) as tip_amount,
    cast(tolls_amount as float64) as tolls_amount,
    cast(improvement_surcharge as float64) as improvement_surcharge,
    cast(total_amount as float64) as total_amount,
    cast(congestion_surcharge as float64) as congestion_surcharge,
    cast(Airport_fee as float64) as airport_fee,
    date(datetime(tpep_pickup_datetime)) as pickup_date,
    extract(hour from datetime(tpep_pickup_datetime)) as pickup_hour,
    datetime_diff(datetime(tpep_dropoff_datetime), datetime(tpep_pickup_datetime), second) / 60.0 as trip_duration_min,
    source_month,
    loaded_at
from src
