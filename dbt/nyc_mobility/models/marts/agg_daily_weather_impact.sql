select
    t.pickup_date,
    t.is_rainy,
    count(*) as trips,
    avg(t.trip_duration_min) as avg_duration_min,
    avg(t.fare_amount) as avg_fare,
    sum(t.total_amount) as total_revenue,
    z.borough
from {{ ref('fct_trips') }} as t
left join {{ ref('dim_zone') }} as z
    on t.pu_location_id = z.location_id
where t.pickup_date >= date('2024-01-01')
group by
    t.pickup_date,
    t.is_rainy,
    z.borough
