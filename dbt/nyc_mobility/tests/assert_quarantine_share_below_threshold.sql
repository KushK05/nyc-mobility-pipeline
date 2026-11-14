{{ config(severity='error', warn_if='>0', error_if='>0') }}

-- Returns a row (failure) when more than 5% of the run month's trips are quarantined.
select share
from (select {{ quarantine_share(var('run_month_start')) }} as share)
where share > 0.05
