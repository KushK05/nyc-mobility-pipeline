# Demo runbook

Demo length is about 8-10 minutes. Everything below except the pytest suite needs a GCP project, credentials loaded from `.env` (`set -a; source .env; set +a`), and months 2024-01 to 2024-06 already backfilled. Capture the screenshots in the checklist as you go.

## Demo script

1. **Architecture slide (1 min).** Show the diagram from the README and the four-problems table.
2. **Ingestion (1 min).** Ingestion is Airflow Python loaders. Open BigQuery, datasets `raw_tlc.yellow_trips` and `raw_weather.weather_hourly`, and show the `source_month` and `loaded_at` columns. If the optional Airbyte weather path is set up, show its connection in the Airbyte UI (see `airbyte/README.md`).
3. **Airflow (2 min).** Run `make airflow-start`, open http://localhost:8080, DAG `nyc_mobility_pipeline`: show the Graph view, the completed backfill runs for each month, and a run where `dbt_test` failed and `alert_and_stop` ran (to force one, mark `dbt_test` failed on a spare run).
4. **dbt (1 min).** From `dbt/nyc_mobility` run `dbt docs generate && dbt docs serve --profiles-dir .`, open the lineage graph, then show test results (`dbt test --profiles-dir .` or the `dbt_test` task log).
5. **BigQuery partition filter (1 min).** In the BigQuery console run this pair and compare "Bytes processed":

   ```sql
   -- pruning filter: one partition
   SELECT COUNT(*) FROM `<project>.marts.fct_trips` WHERE pickup_date = '2024-03-15';
   -- full scan: fct_trips requires a partition filter, so a query with none is rejected;
   -- this filter matches every partition, so it is accepted but prunes nothing
   SELECT COUNT(*) FROM `<project>.marts.fct_trips` WHERE pickup_date >= DATE '2000-01-01';
   ```

   Then run `make cost-compare` for the scripted dry-run comparison (filtered vs full scan).
6. **Late data (1 min).** Follow [late_data_demo.md](late_data_demo.md): inject late January trips, clear the 2024-02-01 DAG run, run `python scripts/verify_idempotency.py --month 2024-01`.
7. **Chatbot (2-3 min).** `make chatbot`, then ask in order:
   1. How does rain affect average trip duration in Manhattan on weekday evenings?
   2. Which pickup zones had the biggest demand drop on public holidays?
   3. Top 10 busiest airport routes in March?
   4. Show me every trip in the dataset. (blocked by the guard)
8. **Evaluation (30 s).** `make eval` and show accuracy, average bytes and blocked count.

**Backup plan:** record a screen video of the full demo the night before in case Wi-Fi, Docker or the LLM API fails during the lab.

## Screenshot checklist

| Done | Screenshot | File |
|---|---|---|
| [ ] | Airbyte connections (optional path) | `docs/screenshots/airbyte_connections.png` |
| [ ] | Airflow DAG graph | `docs/screenshots/airflow_dag_graph.png` |
| [ ] | Airflow backfill runs per month | `docs/screenshots/airflow_backfill_runs.png` |
| [ ] | Airflow failure branch (`alert_and_stop`) | `docs/screenshots/airflow_failure_branch.png` |
| [ ] | dbt lineage graph | `docs/screenshots/dbt_lineage.png` |
| [ ] | dbt test results | `docs/screenshots/dbt_tests.png` |
| [ ] | BigQuery bytes with vs without partition filter | `docs/screenshots/bigquery_bytes_comparison.png` |
| [ ] | Late January counts before and after | `docs/screenshots/late_data_counts.png` |
| [ ] | Chatbot answering the rain question | `docs/screenshots/chatbot_rain_question.png` |
| [ ] | Chatbot blocking the "every trip" question | `docs/screenshots/chatbot_blocked.png` |
| [ ] | Evaluation results | `docs/screenshots/eval_results.png` |

## Known limitations

- **Full refresh covers one month.** `dbt build --full-refresh` rebuilds only the current run month (`run_month_start`), because the intermediate models are month-windowed. To rebuild history, run the DAG backfill (or `make dbt-build MONTH=...` per month).
- **Airbyte path is unvalidated.** The Connector Builder manifest was not tested against a live Airbyte; the Python loader is the validated default.
- **dbt deprecation warning.** `dbt parse` reports `MissingArgumentsPropertyInGenericTestDeprecation` (generic tests without an `arguments:` property). It is a warning only.
- **Chatbot sidebar needs a valid `chatbot/metadata.json`.** It is written by the `refresh_chatbot_metadata` DAG task; `chatbot/metadata.json.example` shows the shape. Run at least one successful DAG run first.
