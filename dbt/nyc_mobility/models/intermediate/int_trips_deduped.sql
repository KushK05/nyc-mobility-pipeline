with trips as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'vendor_id',
            'pickup_datetime',
            'dropoff_datetime',
            'pu_location_id',
            'do_location_id',
            'fare_amount'
        ]) }} as trip_id,
        *
    from {{ ref('stg_trips') }}
    where pickup_date between date_sub(date('{{ var("run_month_start") }}'), interval {{ var("lookback_days", 3) }} day)
        and last_day(date('{{ var("run_month_start") }}'))

)

select *
from trips
qualify row_number() over (partition by trip_id order by loaded_at desc) = 1
