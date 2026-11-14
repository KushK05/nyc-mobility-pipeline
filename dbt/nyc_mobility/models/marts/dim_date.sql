with spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2024-01-01' as date)",
        end_date="cast('2025-01-01' as date)"
    ) }}

),

days as (

    select
        cast(date_day as date) as date_day,
        extract(year from date_day) as year,
        extract(month from date_day) as month,
        extract(dayofweek from date_day) as day_of_week,
        format_date('%A', date_day) as day_name
    from spine

)

select
    d.date_day,
    d.year,
    d.month,
    d.day_of_week,
    d.day_name,
    d.day_of_week in (1, 7) as is_weekend,
    h.holiday_date is not null as is_holiday,
    h.holiday_name
from days as d
left join {{ ref('us_holidays') }} as h
    on d.date_day = h.holiday_date
