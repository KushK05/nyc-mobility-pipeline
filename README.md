# Cost-Aware NYC Mobility Analytics Pipeline

An ELT pipeline that loads monthly NYC yellow taxi trips and hourly Open-Meteo weather into BigQuery, transforms them with dbt (staging, intermediate, marts, plus US holiday and taxi zone seeds), orchestrates the monthly run with an Airflow DAG that supports idempotent backfills, and exposes the result through a Streamlit chatbot (Mistral text-to-SQL) that dry-runs every query and refuses anything that scans too much data. It is a DE microproject for a group of two (demo 16-17 Nov 2026).

## The four problems

| Problem | What we build |
|---|---|
| **Scale & cost** | Partitioned + clustered tables in BigQuery, with a mandatory partition filter |
| **Late-arriving data** | Incremental dbt models with a lookback window that rewrite affected partitions |
| **Reprocessing history** | Idempotent Airflow backfills (`catchup=True`) - re-running any month gives identical results |
| **Unsafe LLM-generated SQL** | A chatbot that dry-runs every query and blocks or reroutes anything that scans too much data |

## Architecture

```
 ┌──────────────────────┐
 │ NYC TLC Yellow Taxi  │  monthly Parquet (~3M rows/month)
 │ (CloudFront URLs)    │──────┐
 └──────────────────────┘      │
                               ▼
 ┌──────────────────────┐   ┌──────────────────────┐   ┌──────────────────┐   ┌───────────────────┐   ┌──────────────────────┐
 │ Open-Meteo Archive   │──▶│ Airflow Python       │──▶│ BigQuery: raw_*  │──▶│ dbt: staging →     │──▶│ Streamlit chatbot    │
 │ API (hourly weather) │   │ loaders (Airbyte     │   │ datasets         │   │ intermediate →     │   │ (Mistral text-to-SQL+│
 └──────────────────────┘   │ optional, weather)   │   └──────────────────┘   │ marts              │   │  dry-run cost guard) │
                            └──────────────────────┘                          └───────────────────┘   └──────────────────────┘
 dbt seeds: taxi zones, US holidays ──────────────────────────────────────────────────┘
                     ▲                       ▲                       ▲
                     └───────────── Apache Airflow DAG (monthly, catchup=True) ─────────────┘
```

## Quick start

Prerequisites: Python 3.12 and [uv](https://docs.astral.sh/uv/), Docker, the [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli), and a GCP project with billing enabled. Steps marked (GCP) need that project and a service-account key; the others run offline.

1. Configure the environment: `cp .env.example .env`, fill in `GCP_PROJECT_ID`, `GOOGLE_APPLICATION_CREDENTIALS`, `MISTRAL_API_KEY`, then load it into your shell: `set -a; source .env; set +a`.
2. `make venv` creates `.venv` and installs the `dev`, `dbt` and `chatbot` extras. (`make venv-airflow` additionally installs the optional, heavy `airflow` extra.)
3. (GCP) `scripts/setup_gcp.sh` creates the `raw_tlc`, `raw_weather`, `staging`, `marts` and `quarantine` datasets and prints the service-account commands (`CREATE_SA=1` runs them).
4. `make dbt-parse` checks that the dbt project parses (works offline).
5. (GCP) `make dbt-build MONTH=2024-01-01` runs `dbt deps`, `dbt seed` and `dbt build` for one run month. The raw tables must already be loaded (step 6 does this).
6. (GCP) Put the key at `airflow/include/gcp-key.json`, create `airflow/.env`, then `make airflow-start`. Add the connection `tlc_cloudfront` and the Variable `use_airbyte_weather` as described in [airflow/README.md](airflow/README.md); the `airbyte_default` connection and the `airbyte_weather_connection_id` Variable are only needed when `use_airbyte_weather=true`. Then unpause `nyc_mobility_pipeline` and let it backfill 2024-01 to 2024-06.
7. (GCP) `make chatbot` starts Streamlit (needs `MISTRAL_API_KEY`, `GCP_PROJECT_ID`, and `chatbot/metadata.json`, which the DAG writes in `refresh_chatbot_metadata`).
8. (GCP) `make eval` runs the 25-question evaluation; `make cost-compare` dry-runs the partition-filter comparison.

`make test` runs the offline pytest suite.

## Repository map

| Path | Contents |
|---|---|
| `Plan.md` | Project spec: scope, architecture, phases, demo script |
| `Makefile` | Shortcuts for venv, tests, dbt, Airflow, chatbot and evaluation |
| `pyproject.toml` | Python 3.12 package and extras (`dev`, `dbt`, `chatbot`, `airflow`) |
| `.env.example` | Environment variables for GCP and the chatbot |
| `airflow/` | Astro project: DAG, Python loaders and alerts under `include/nyc_mobility`, Docker config |
| `airbyte/` | Optional Open-Meteo Connector Builder manifest and notes |
| `dbt/nyc_mobility/` | dbt project: staging, intermediate and marts models, seeds, macros, tests |
| `chatbot/` | Streamlit app, SQL guard, LLM client, prompts, schema catalog, metadata |
| `eval/` | 25-question evaluation set and runner, partition cost comparison |
| `scripts/` | `setup_gcp.sh`, `inject_late_trips.py`, `verify_idempotency.py` |
| `tests/` | Offline pytest suite |
| `docs/` | Runbook, late-data demo, screenshots for the report |

## Design decisions

1. **Ingestion.** Airflow Python tasks download the monthly TLC Parquet and the Open-Meteo hourly JSON and load them into BigQuery directly (`raw_tlc.yellow_trips`, `raw_weather.weather_hourly`). Each load adds `source_month` and `loaded_at` and deletes that month's rows before appending, so re-runs are idempotent at the raw layer. Airbyte is kept as an optional path for weather only (Connector Builder manifest in `airbyte/`), toggled with the Airflow Variable `use_airbyte_weather=true` and the dbt var `weather_source=airbyte`. Plan.md section 5.2 sanctions this fallback, and section 8 lists Airbyte URL switching as the first thing to cut.
2. **LLM.** Mistral via the `mistralai` SDK. The model comes from `LLM_MODEL` (default `mistral-medium-latest`) and the key from `MISTRAL_API_KEY`.
3. **dbt layout.** Intermediate models materialize into the `staging` dataset. `quarantine_trips` is incremental `insert_overwrite` into `quarantine`, so rejected rows accumulate.
4. **Late data.** `fct_trips` uses `insert_overwrite` with a 3-day lookback. `--full-refresh` rebuilds only the current run month because the intermediates are month-windowed (see the runbook).
5. **Cost guard.** The chatbot pins SQL to this project's `marts` dataset, requires a `pickup_date` filter on `fct_trips`, dry-runs every query (500 MB limit), executes with `maximum_bytes_billed`, and rewrites once on a guard error and once on cost.
6. **Evaluation.** Results are compared by values (column names ignored) over 25 questions in five categories.

## Verification status

| Item | Status |
|---|---|
| pytest suite (99 tests) | Verified locally, passing |
| `dbt parse` (`make dbt-parse`) | Verified locally |
| Airflow DAG import (`DagBag`, in `tests/test_dag.py`) | Verified locally |
| Raw loads, `dbt build`, Airflow backfill | Needs GCP project and credentials; not run |
| Chatbot against BigQuery and Mistral, `make eval`, `make cost-compare` | Needs GCP and `MISTRAL_API_KEY`; not run |
| Late-data demo scripts | Needs GCP; not run |
| Airbyte weather path | Manifest not validated; needs a running Airbyte |

## Further reading

- [docs/runbook.md](docs/runbook.md) - demo script, screenshot checklist, known limitations
- [docs/late_data_demo.md](docs/late_data_demo.md) - late-arriving data walkthrough
- [airflow/README.md](airflow/README.md) - DAG, connections, variables, backfills
- [airbyte/README.md](airbyte/README.md) - optional Airbyte weather path
- [Plan.md](Plan.md) - project spec
