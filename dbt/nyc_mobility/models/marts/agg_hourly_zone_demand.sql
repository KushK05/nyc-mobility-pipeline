{{ config(
    partition_by={'field': 'pickup_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by=['pu_location_id']
) }}

select
    t.pickup_date,
    t.pickup_hour,
    t.pu_location_id,
    z.borough,
    z.zone,
    count(*) as trips,
    avg(t.fare_amount) as avg_fare,
    avg(t.trip_duration_min) as avg_duration_min,
    avg(safe_divide(t.tip_amount, nullif(t.fare_amount, 0))) * 100 as avg_tip_pct,
    d.is_weekend,
    d.is_holiday
from {{ ref('fct_trips') }} as t
left join {{ ref('dim_zone') }} as z
    on t.pu_location_id = z.location_id
left join {{ ref('dim_date') }} as d
    on t.pickup_date = d.date_day
where t.pickup_date >= date('2024-01-01')
group by
    t.pickup_date,
    t.pickup_hour,
    t.pu_location_id,
    z.borough,
    z.zone,
    d.is_weekend,
    d.is_holiday
