# Late-arriving data demo

Shows that re-running a later month picks up late rows for an earlier month without creating duplicates in `marts.fct_trips`.

Prerequisite: `GCP_PROJECT_ID` is set, Jan and Feb 2024 have been loaded, and `astro dev start` is running.

1. Inject 500 late-arriving trips for January (copies of Jan 29-31 trips, shifted by 37 seconds, with a fresh `loaded_at`):

   ```
   python scripts/inject_late_trips.py --month 2024-01 --rows 500
   ```

   Add `--dry-run` to print the SQL only.

2. Re-run the February DAG run, which rebuilds the models that cover January:

   ```
   astro dev run tasks clear nyc_mobility_pipeline --start-date 2024-02-01 --end-date 2024-02-01 --yes
   ```

   Or clear the 2024-02-01 run from the Airflow UI.

3. Verify January counts and duplicates:

   ```
   python scripts/verify_idempotency.py --month 2024-01
   ```

   Jan 29-31 counts should rise by the injected rows, and duplicate trip_ids should be 0 (exit code 0).

4. Clear and re-run February again, then verify again:

   ```
   astro dev run tasks clear nyc_mobility_pipeline --start-date 2024-02-01 --end-date 2024-02-01 --yes
   python scripts/verify_idempotency.py --month 2024-01
   ```

   Counts must be identical to step 3 and duplicates still 0.

5. Screenshots to capture:
   - Jan 29-31 per-day counts before (baseline) and after the injection
   - Duplicate trip_ids = 0 after both runs
   - The dbt `unique` test on `fct_trips.trip_id` passing in the DAG run
