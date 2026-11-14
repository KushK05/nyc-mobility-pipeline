select
    *,
    case
        when fare_amount < 0 then 'negative_fare'
        when trip_distance = 0 and fare_amount > 50 then 'zero_distance_high_fare'
        when dropoff_datetime <= pickup_datetime then 'dropoff_before_pickup'
        when trip_duration_min > 360 then 'duration_over_6h'
        when date_trunc(pickup_date, month) != source_month then 'pickup_outside_source_month'
    end as rejection_reason
from {{ ref('int_trips_deduped') }}
