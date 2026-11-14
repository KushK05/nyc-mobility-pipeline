# Plan: Cost-Aware NYC Mobility Analytics Pipeline

**DE Microproject** · Group of 2 · Demo & submission: 16th–17th November 2026

---

## 1. Project Summary

We are building an automated, enterprise-style ELT pipeline that ingests large-scale NYC taxi trip data together with hourly weather and holiday data, transforms it into analytics-ready tables, and exposes it through a **conversational BI chatbot that refuses to run expensive queries**.

The project goes beyond a basic "load CSV → star schema" pipeline by tackling four real data engineering problems:

| Problem | What we build |
|---|---|
| **Scale & cost** | Partitioned + clustered tables in BigQuery, with a mandatory partition filter |
| **Late-arriving data** | Incremental dbt models with a lookback window that rewrite affected partitions |
| **Reprocessing history** | Idempotent Airflow backfills (`catchup=True`) – re-running any month gives identical results |
| **Unsafe LLM-generated SQL** | A chatbot that dry-runs every query and blocks or reroutes anything that scans too much data |

**Example question the system answers:**
> "How does rain affect average trip duration in Manhattan on weekday evenings?"

This needs three sources joined together (trips + weather + calendar), answered in plain English, with the generated SQL, bytes scanned and estimated cost shown beside the answer.

---

## 2. Objectives

1. Automate ingestion of batch (monthly Parquet) and API (hourly weather) data using **Airbyte**.
2. Orchestrate the full workflow with an **Apache Airflow** DAG that supports scheduling, backfills and failure alerts.
3. Build layered **dbt** models (staging → intermediate → marts) with data quality tests.
4. Design a **BigQuery** warehouse schema optimised with partitioning and clustering.
5. Handle duplicates, dirty records and late-arriving data correctly and idempotently.
6. Deliver a **conversational BI interface** with cost guardrails, and measure its accuracy.

---

## 3. Architecture

```
 ┌──────────────────────┐
 │ NYC TLC Yellow Taxi  │  monthly Parquet (~3M rows/month)
 │ (CloudFront URLs)    │──────┐
 └──────────────────────┘      │
                               ▼
 ┌──────────────────────┐   ┌─────────┐   ┌──────────────────┐   ┌───────────────────┐   ┌──────────────────────┐
 │ Open-Meteo Archive   │──▶│ Airbyte │──▶│ BigQuery: raw_*  │──▶│ dbt: staging →     │──▶│ Streamlit chatbot    │
 │ API (hourly weather) │   └─────────┘   │ datasets         │   │ intermediate →     │   │ (LLM text-to-SQL +   │
 └──────────────────────┘                 └──────────────────┘   │ marts              │   │  dry-run cost guard) │
                                                                 └───────────────────┘   └──────────────────────┘
 dbt seeds: taxi zones, US holidays ──────────────────────────────────────┘
                     ▲                       ▲                       ▲
                     └───────────── Apache Airflow DAG (monthly, catchup=True) ─────────────┘
```

**Data flow:** Source data → Airbyte → BigQuery (raw) → dbt transformations → BigQuery (marts) → Conversational BI

### Tech Stack

| Layer | Tool | Notes |
|---|---|---|
| Orchestration | Apache Airflow (via Astro CLI) | Runs locally in Docker |
| Ingestion | Airbyte OSS (via `abctl`) | File source for Parquet; Connector Builder for Open-Meteo |
| Warehouse | Google BigQuery | Free-trial credits (see §5.1 about Sandbox limits) |
| Transformation | dbt Core + `dbt-bigquery` | Plus `dbt_utils` package |
| BI interface | Streamlit + LLM API | Gemini or Claude API for text-to-SQL |
| Language | Python 3.10+, SQL | |

---

## 4. Data Sources

| Source | Details | Volume |
|---|---|---|
| **NYC TLC Yellow Taxi trips** | `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet` | Use 3–6 months (~3M rows each) |
| **Taxi zone lookup** | `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv` → dbt seed | 265 rows |
| **Hourly weather (NYC)** | `https://archive-api.open-meteo.com/v1/archive` (free, no key) | ~720 rows/month |
| **US holidays** | Hand-made CSV → dbt seed (or Nager.Date API) | ~15 rows/year |

**Key taxi columns:** `VendorID`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `passenger_count`, `trip_distance`, `PULocationID`, `DOLocationID`, `payment_type`, `fare_amount`, `tip_amount`, `tolls_amount`, `total_amount`, `congestion_surcharge`, `Airport_fee`.

**Open-Meteo request example:**
```
https://archive-api.open-meteo.com/v1/archive?latitude=40.71&longitude=-74.01
  &start_date=2024-01-01&end_date=2024-03-31
  &hourly=temperature_2m,precipitation,rain,snowfall,wind_speed_10m
  &timezone=America/New_York
```

> Pick months that are fully published (e.g. Jan–Jun 2024) so weather and trips line up.

---

## 5. Implementation Steps

### 5.1 Phase 0 — Environment Setup

**Google Cloud / BigQuery**
1. Create a GCP project (e.g. `nyc-mobility-de`).
2. ⚠️ As far as we know, the **BigQuery Sandbox does not allow DML** (`MERGE`, `INSERT`), which dbt incremental models need. Verify this early. If true, activate the **free-trial billing account** and set a **budget alert** (e.g. ₹500).
3. Create datasets: `raw_tlc`, `raw_weather`, `staging`, `marts`, `quarantine`.
4. Create a service account with roles **BigQuery Data Editor** + **BigQuery Job User**, download its JSON key. Never commit this key to Git.

**Local tools**
```bash
# Airbyte OSS
curl -LsfS https://get.airbyte.com | bash -
abctl local install          # UI at http://localhost:8000

# Airflow via Astro CLI
astro dev init
astro dev start              # UI at http://localhost:8080

# dbt + chatbot
python -m venv .venv && source .venv/bin/activate
pip install dbt-bigquery streamlit google-cloud-bigquery pandas
```

> Airbyte + Airflow together are RAM-heavy. Run them on the laptop with the most memory (16 GB ideal).

### 5.2 Phase 1 — Ingestion with Airbyte

**Connection 1: Taxi trips**
- Source: **File** connector → URL of the month's Parquet file, format `parquet`.
- Destination: **BigQuery** → dataset `raw_tlc`, stream name `yellow_trips`.
- Sync mode: **Full refresh – Append** (each monthly file is a new batch; dedupe happens in dbt).
- The URL changes every month. Airflow updates it via the Airbyte API for each run's logical month (§5.4).
- **Fallback if this gets fiddly:** let an Airflow Python task load the Parquet straight into `raw_tlc`, and keep Airbyte for weather. Document this as a design decision.

**Connection 2: Weather**
- Build a custom source in **Airbyte Connector Builder** for Open-Meteo.
- Parameters: `start_date`, `end_date` (incremental by date).
- Destination: BigQuery → dataset `raw_weather`.
- Note: the API returns arrays (`hourly.time[]`, `hourly.rain[]`…) that must be zipped into rows — either in the connector's record selector or in the dbt staging model with `UNNEST ... WITH OFFSET`.

**Check:** Airbyte adds metadata columns (`_airbyte_raw_id`, `_airbyte_extracted_at`, …). Confirm the exact table and column names it creates before writing dbt sources.

### 5.3 Phase 2 — Transformations with dbt

**Project layout**
```
dbt/nyc_mobility/
├── dbt_project.yml
├── packages.yml                    # dbt_utils
├── seeds/
│   ├── taxi_zone_lookup.csv
│   └── us_holidays.csv
├── models/
│   ├── staging/
│   │   ├── _sources.yml
│   │   ├── stg_trips.sql
│   │   └── stg_weather_hourly.sql
│   ├── intermediate/
│   │   ├── int_trips_deduped.sql
│   │   ├── int_trips_validated.sql      # splits good vs quarantined rows
│   │   └── int_trips_with_weather.sql
│   └── marts/
│       ├── fct_trips.sql                # partitioned, incremental
│       ├── dim_zone.sql
│       ├── dim_date.sql
│       ├── agg_hourly_zone_demand.sql
│       ├── agg_daily_weather_impact.sql
│       ├── quarantine_trips.sql
│       └── _marts.yml                   # tests + docs
└── tests/
    └── assert_no_dropoff_before_pickup.sql
```

**Model responsibilities**

| Model | Purpose |
|---|---|
| `stg_trips` | Rename columns to snake_case, cast types, derive `pickup_date`, `pickup_hour`, `trip_duration_min` |
| `stg_weather_hourly` | Flatten API arrays into one row per hour; flag `is_rainy` (rain > 0.1 mm) |
| `int_trips_deduped` | Build `trip_id` with `dbt_utils.generate_surrogate_key([vendor_id, pickup_ts, dropoff_ts, pu_location_id, do_location_id, fare_amount])`, keep one row per key |
| `int_trips_validated` | Flag bad rows: negative fare, zero distance with fare > $50, dropoff ≤ pickup, duration > 6 h, pickup outside file's month |
| `int_trips_with_weather` | Join each valid trip to weather for its pickup hour |
| `fct_trips` | Final trip fact table — partitioned, clustered, incremental |
| `dim_zone` | Zone, borough, service zone, `is_airport` flag |
| `dim_date` | Weekday, weekend, holiday, month, `is_holiday` |
| `agg_hourly_zone_demand` | Trips, avg fare, avg duration, avg tip % per zone per hour |
| `agg_daily_weather_impact` | Daily trips/duration/fares split by rainy vs dry |
| `quarantine_trips` | All rejected rows with a `rejection_reason` column |

**`fct_trips` config — the core of the project**
```sql
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'pickup_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by = ['pu_location_id', 'do_location_id'],
    require_partition_filter = true
) }}

select ...
from {{ ref('int_trips_with_weather') }}

{% if is_incremental() %}
  -- Lookback window: reprocess the run month PLUS the previous N days,
  -- so late-arriving trips land in their correct (older) partitions.
  where pickup_date between
        date_sub(date('{{ var("run_month_start") }}'), interval {{ var("lookback_days", 3) }} day)
    and last_day(date('{{ var("run_month_start") }}'))
{% endif %}
```

Why `insert_overwrite`: it **replaces whole partitions** instead of appending, so re-running the same month always produces the same result (idempotency), and late rows simply rewrite their partition. If BigQuery complains about the required partition filter during the merge, add an `incremental_predicates` filter on the date range.

**Tests (`_marts.yml`)**
- `unique` + `not_null` on `fct_trips.trip_id`
- `relationships`: `pu_location_id` / `do_location_id` → `dim_zone.location_id`
- `accepted_values` on `payment_type`
- `dbt_utils.accepted_range` on `fare_amount` (≥ 0) and `trip_duration_min` (0–360)
- Custom: no dropoff before pickup; quarantine share < 5% of the month's rows (`warn` above 2%, `error` above 5%)

**Run locally**
```bash
dbt deps
dbt seed
dbt build --vars '{"run_month_start": "2024-01-01"}'
dbt docs generate && dbt docs serve     # lineage graph for the report
```

### 5.4 Phase 3 — Orchestration with Airflow

**DAG: `nyc_mobility_pipeline`**
- `schedule="@monthly"`, `start_date=2024-01-01`, `end_date=2024-06-30`, `catchup=True`, `max_active_runs=1`
- `retries=2`, `on_failure_callback` → log/Slack/email alert
- Each run processes **its logical month** (`{{ ds }}` → `run_month_start`)

**Tasks**
```
check_source_available (HttpSensor on the month's Parquet URL)
        │
update_airbyte_source_url (PythonOperator → Airbyte API)
        │
        ├── sync_taxi_trips   (AirbyteTriggerSyncOperator)
        └── sync_weather      (AirbyteTriggerSyncOperator)
                 │
          dbt_seed  →  dbt_run  →  dbt_test
                                       │
                       ┌───────────────┴───────────────┐
              tests passed?                      tests failed
                       │                               │
          refresh_chatbot_metadata             alert_and_stop
                       │
               log_run_summary (row counts, quarantine %, bytes processed)
```

- Use the `apache-airflow-providers-airbyte` package for `AirbyteTriggerSyncOperator`.
- Run dbt with `BashOperator` (simplest), passing `--vars '{"run_month_start": "{{ ds }}"}'`.
- Use `BranchPythonOperator` (or `trigger_rule`) to route on the test outcome.
- From inside the Astro containers, reach Airbyte at `http://host.docker.internal:8000`. Check how your Airbyte version handles API authentication and store credentials in an Airflow Connection, not in code.
- Mount the dbt project and the service-account key into the Airflow container (Astro: put them under `include/`).

### 5.5 Phase 4 — Late-Arriving Data Simulation

TLC monthly files are already mostly complete, so we **simulate** late data and say so clearly in the report.

1. Load and process January and February normally.
2. Run `scripts/inject_late_trips.py`: insert ~500 trips with pickup dates in the **last 3 days of January** into `raw_tlc` (copy real rows, shift timestamps slightly so they get new `trip_id`s).
3. Re-run the **February** DAG run.
4. Show: January's last-3-day partitions now include the late trips; total row counts are correct; **no duplicates** (`trip_id` unique test passes).
5. Clear and re-run February again → counts unchanged (**idempotency proof**).

### 5.6 Phase 5 — Conversational BI with Cost Guard

**Flow for every question**
```
User question
   → LLM prompt (mart schemas + column descriptions + rules + few-shot examples)
   → Generated SQL
   → Guard 1: must be a single SELECT (reject INSERT/UPDATE/DELETE/DROP/MERGE)
   → Guard 2: BigQuery dry run → total_bytes_processed
   → Guard 3: if bytes > 500 MB → ask LLM to rewrite using agg_* tables or a narrower date range
              → still too big? refuse and ask the user to narrow the question
   → Execute with maximum_bytes_billed as a hard safety net
   → Show answer table/chart + SQL + bytes scanned + estimated cost
```

**Dry-run guard (core idea)**
```python
from google.cloud import bigquery

client = bigquery.Client()
LIMIT_BYTES = 500 * 1024**2  # 500 MB

def guarded_run(sql: str):
    dry = client.query(sql, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False))
    scanned = dry.total_bytes_processed
    if scanned > LIMIT_BYTES:
        return {"blocked": True, "bytes": scanned}
    cfg = bigquery.QueryJobConfig(maximum_bytes_billed=LIMIT_BYTES)
    df = client.query(sql, job_config=cfg).to_dataframe()
    return {"blocked": False, "bytes": scanned, "data": df}
```

**Prompt rules to give the LLM**
- Only use tables in the `marts` dataset.
- Prefer `agg_hourly_zone_demand` / `agg_daily_weather_impact` over `fct_trips` whenever possible.
- Any query on `fct_trips` must filter on `pickup_date`.
- Always add `LIMIT 100` unless aggregating.
- Return only SQL, no explanation or markdown fences.

**Streamlit UI**
- Chat input + history
- For each answer: result table, auto chart (bar/line), collapsible SQL, badge with "Scanned: 42 MB · Est. cost: $0.0002"
- Red banner when a query is blocked, with the reason
- Sidebar: data coverage (months loaded), last pipeline run time, quarantine %

### 5.7 Phase 6 — Evaluation

Create `eval/questions.csv` with **~25 questions** and hand-verified gold SQL:

| Category | Example |
|---|---|
| Simple aggregate | "Total trips in March 2024?" |
| Join with weather | "Average duration on rainy vs dry days?" |
| Join with calendar | "Which zones dropped most on holidays?" |
| Time + location filter | "Top 10 airport routes in February?" |
| Should be blocked | "List every trip in the dataset" |

**Script `eval/run_eval.py` reports:**
- **Execution accuracy** — % of questions where the chatbot's result matches the gold result
- **Average bytes scanned** per question
- **Guard actions** — how many were blocked / rerouted to aggregates
- **Cost comparison** — the same 5 queries on `fct_trips` with vs without a partition filter

---

## 6. Repository Structure

```
nyc-mobility-pipeline/
├── README.md
├── Plan.md
├── airflow/                      # Astro project
│   ├── dags/nyc_mobility_pipeline.py
│   ├── include/
│   │   ├── dbt/ → symlink or copy of dbt project
│   │   └── gcp-key.json          # gitignored
│   └── requirements.txt          # airbyte provider, dbt-bigquery
├── dbt/nyc_mobility/             # see §5.3
├── airbyte/
│   └── connector_builder_open_meteo.yaml
├── chatbot/
│   ├── app.py
│   ├── prompts.py
│   └── guard.py
├── scripts/
│   └── inject_late_trips.py
├── eval/
│   ├── questions.csv
│   └── run_eval.py
├── docs/screenshots/             # for the report
└── .gitignore                    # keys, .venv, target/, logs/
```

---

## 7. Work Split

| Member A — Platform & Orchestration | Member B — Modelling & BI |
|---|---|
| GCP project, datasets, service account, budget alert | dbt project, sources, seeds |
| Airbyte install + both connections | Staging, intermediate, mart models |
| Airflow DAG, branching, alerts, backfill | Partitioning, incremental logic, tests |
| Late-data injection script + idempotency demo | Streamlit chatbot + dry-run guard |
| Screenshots of Airbyte/Airflow runs | Evaluation set + results |
| Report §2, §3 (ingestion, orchestration) | Report §3 (transformation, BI), §4 |

Both: Abstract, Conclusion, demo rehearsal.

---

## 8. Timeline

| When | Tasks | Done when |
|---|---|---|
| **Sat 14 Nov** | GCP setup, Airbyte installed, Jan data + weather landed in BigQuery, dbt staging models | `select count(*)` works on raw + staging |
| **Sun 15 Nov (AM)** | Intermediate + mart models, `fct_trips` incremental with partitions, tests | `dbt build` passes for 2 months |
| **Sun 15 Nov (PM)** | Airflow DAG end-to-end, backfill 3–6 months, late-data demo, chatbot + guard | Chatbot answers 5 sample questions |
| **Sun 15 Nov (night)** | Evaluation run, screenshots, report draft | Report ~80% done |
| **Mon 16 Nov** | Demo rehearsal (×2), fix bugs, finalise report | Demo runs in < 10 min |

**If running out of time, cut in this order:**
1. Airbyte API URL-switching (load taxi files from Airflow instead)
2. Evaluation set (keep 10 questions instead of 25)
3. Sidebar extras in the chatbot

**Never cut:** partitioning, late-data handling, the dry-run guard — these are what make the project stand out.

---

## 9. Demo Script (~8–10 minutes)

1. **Architecture slide** (1 min) — the four problems we solve.
2. **Airbyte** (1 min) — show both connections and a successful sync.
3. **Airflow** (2 min) — DAG graph, completed backfill runs for each month, a test-failure branch.
4. **dbt** (1 min) — lineage graph from `dbt docs`, test results.
5. **BigQuery** (1 min) — same query with vs without partition filter: bytes scanned difference.
6. **Late data** (1 min) — row counts for late January before/after re-running February; uniqueness test passes.
7. **Chatbot** (2–3 min):
   - "How does rain affect average trip duration in Manhattan on weekday evenings?"
   - "Which pickup zones had the biggest demand drop on public holidays?"
   - "Top 10 busiest airport routes in March?"
   - "Show me every trip in the dataset." → **blocked by the guard** 🎯
8. **Evaluation numbers** (30 s) — accuracy, average bytes, blocked count.

**Backup plan:** record a screen video of the full demo on Sunday night in case Wi-Fi, Docker or the LLM API fails during the lab.

---

## 10. Risks & Mitigations

| Risk | Mitigation |
|---|---|
| BigQuery Sandbox blocks DML | Use free-trial billing with a budget alert |
| Laptop runs out of RAM | Run Airbyte and Airflow on the stronger laptop; stop Airbyte after syncs |
| Airbyte ↔ Airflow networking issues | Use `host.docker.internal`; fallback to Python load task |
| LLM generates wrong SQL | Few-shot examples, mart-only schema, column descriptions; show SQL to the user |
| LLM API quota / outage on demo day | Cache answers for demo questions; recorded backup video |
| Costs | Max 6 months of data, `maximum_bytes_billed` on every chatbot query, budget alert |
| Leaked credentials | `.gitignore` the key; use Airflow Connections and environment variables |

---

## 11. Mapping to the Report Format

| Report section | Content from this plan |
|---|---|
| Cover page | Common format (college template) |
| Table of contents | Auto-generated |
| **Abstract** | §1 summary in ~150 words |
| **1. Problem Statement & Objective** | §1 table of four problems + §2 objectives |
| **2. Architecture & Tech Stack** | §3 diagram + tech stack table + data flow line |
| **3. Implementation Details** | Ingestion → §5.2 · Orchestration → §5.4 · Transformation → §5.3 · Warehouse & BI → §5.3 partitioning + §5.6 |
| **4. Key Results & Demo Output** | Rows processed per month, quarantine %, partition cost comparison, late-data proof, chatbot Q&A screenshots, evaluation table (§5.7) |
| **5. Conclusion & Learning Outcomes** | Orchestration & backfills, cloud warehousing & cost optimisation, ELT with dbt, data quality, safe LLM-based BI; future work (streaming ingestion, dbt Semantic Layer, more cities) |

---

## 12. Final Checklist

- [ ] GCP project, datasets, service account, budget alert
- [ ] Airbyte: taxi + weather connections syncing to BigQuery
- [ ] dbt: seeds, staging, intermediate, marts all build
- [ ] `fct_trips` partitioned by `pickup_date`, clustered, `require_partition_filter = true`
- [ ] dbt tests passing; quarantine table populated
- [ ] Airflow DAG backfilled for all chosen months
- [ ] Late-data simulation + idempotency proof captured
- [ ] Chatbot answers demo questions and blocks the expensive one
- [ ] Evaluation results table
- [ ] Screenshots: Airbyte, Airflow graph + runs, dbt lineage, BigQuery bytes comparison, chatbot
- [ ] Backup demo video recorded
- [ ] Report written in the required format
- [ ] Code pushed to GitHub (no keys!)
