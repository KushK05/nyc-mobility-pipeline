{{ config(
    materialized='table',
    schema='quarantine',
    partition_by={'field': 'pickup_date', 'data_type': 'date', 'granularity': 'day'}
) }}

select
    *,
    date('{{ var("run_month_start") }}') as run_month_start
from {{ ref('int_trips_validated') }}
where rejection_reason is not null
