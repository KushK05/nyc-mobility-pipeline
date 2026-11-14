{{ config(severity='warn', warn_if='>0', error_if='>0') }}

-- Warns when more than 2% of the run month's trips are quarantined.
select share
from (select {{ quarantine_share(var('run_month_start')) }} as share)
where share > 0.02
