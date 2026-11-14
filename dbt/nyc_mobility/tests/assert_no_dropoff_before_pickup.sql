select trip_id, pickup_datetime, dropoff_datetime
from {{ ref('fct_trips') }}
where pickup_date between date_sub(date('{{ var("run_month_start") }}'), interval {{ var("lookback_days", 3) }} day)
        and last_day(date('{{ var("run_month_start") }}'))
    and dropoff_datetime <= pickup_datetime
