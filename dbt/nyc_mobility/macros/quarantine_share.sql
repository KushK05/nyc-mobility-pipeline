{% macro quarantine_share(run_month_start) %}
(
    select safe_divide(countif(rejection_reason is not null), count(*))
    from {{ ref('int_trips_validated') }}
    where pickup_date between date('{{ run_month_start }}') and last_day(date('{{ run_month_start }}'))
)
{% endmacro %}
