{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    schema='quarantine',
    partition_by={'field': 'pickup_date', 'data_type': 'date', 'granularity': 'day'}
) }}

select
    *,
    date('{{ var("run_month_start") }}') as run_month_start
from {{ ref('int_trips_validated') }}
where rejection_reason is not null
{% if is_incremental() %}
    and pickup_date between date_sub(date('{{ var("run_month_start") }}'), interval {{ var("lookback_days", 3) }} day)
        and last_day(date('{{ var("run_month_start") }}'))
{% endif %}
