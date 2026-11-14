# NYC Mobility Analytics Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the cost-aware NYC mobility ELT pipeline described in `Plan.md`: ingestion loaders, dbt models with partitioned incremental facts, an Airflow DAG with backfills, a late-data simulation, a cost-guarded text-to-SQL chatbot, and an evaluation harness.

**Architecture:** Airflow Python tasks download monthly TLC Parquet and Open-Meteo hourly weather into BigQuery raw datasets (Airbyte remains an optional weather path via a Connector Builder manifest). dbt builds staging → intermediate → marts, with `fct_trips` as a day-partitioned, clustered `insert_overwrite` incremental model. A Streamlit chatbot turns questions into SQL with Claude, dry-runs every statement, and blocks or reroutes anything over 500 MB.

**Tech Stack:** Python 3.12 (uv-managed venv), `google-cloud-bigquery`, `pyarrow`, `requests`, dbt Core + `dbt-bigquery` + `dbt_utils`, Apache Airflow 3 (Astro CLI), `anthropic` SDK, `sqlglot`, Streamlit, pytest.

**Spec:** `Plan.md` (repo root). The spec is the binding authority; this plan argues from it.

## Global Constraints

- Python `>=3.12,<3.13`. Dependencies declared in root `pyproject.toml`; venv created with `uv venv --python 3.12 .venv` and installed with `uv pip install -e ".[dev]"`.
- All cloud configuration comes from environment variables, never literals: `GCP_PROJECT_ID`, `GOOGLE_APPLICATION_CREDENTIALS`, `BQ_LOCATION` (default `US`), `ANTHROPIC_API_KEY`, `LLM_MODEL` (default `claude-opus-5-5`). `.env.example` documents all of them. No key file is ever committed.
- BigQuery datasets (exact names): `raw_tlc`, `raw_weather`, `staging`, `marts`, `quarantine`. dbt intermediate models materialize into `staging`; `quarantine_trips` into `quarantine`; everything else under `models/marts/` into `marts`.
- Months in scope: `2024-01` through `2024-06`. A "month" is always identified by its first day as a `datetime.date` (e.g. `date(2024, 1, 1)`).
- Raw taxi table `raw_tlc.yellow_trips`: the original TLC column names unchanged (`VendorID`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `passenger_count`, `trip_distance`, `RatecodeID`, `store_and_fwd_flag`, `PULocationID`, `DOLocationID`, `payment_type`, `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `improvement_surcharge`, `total_amount`, `congestion_surcharge`, `Airport_fee`) plus `source_month DATE` and `loaded_at TIMESTAMP`.
- Raw weather table `raw_weather.weather_hourly` (Python path): `observed_at DATETIME` (NYC local wall-clock from the API, timezone `America/New_York`), `temperature_2m FLOAT64`, `precipitation FLOAT64`, `rain FLOAT64`, `snowfall FLOAT64`, `wind_speed_10m FLOAT64`, `source_month DATE`, `loaded_at TIMESTAMP`. Open-Meteo request: `latitude=40.71`, `longitude=-74.01`, `hourly=temperature_2m,precipitation,rain,snowfall,wind_speed_10m`, `timezone=America/New_York`.
- Raw-layer idempotency: loading a month first deletes all rows with that `source_month`, then appends. Re-running a month yields identical row counts.
- `trip_id = dbt_utils.generate_surrogate_key(['vendor_id','pickup_datetime','dropoff_datetime','pu_location_id','do_location_id','fare_amount'])`.
- Validation rules (exact, in `int_trips_validated`): `fare_amount < 0` → `negative_fare`; `trip_distance = 0 and fare_amount > 50` → `zero_distance_high_fare`; `dropoff_datetime <= pickup_datetime` → `dropoff_before_pickup`; `trip_duration_min > 360` → `duration_over_6h`; `pickup_date` outside the row's `source_month` → `pickup_outside_source_month`. `rejection_reason` is the first matching rule in that order; `NULL` means valid.
- `is_rainy` = `rain > 0.1` (mm).
- `fct_trips` config exactly: `materialized='incremental'`, `incremental_strategy='insert_overwrite'`, `partition_by={'field':'pickup_date','data_type':'date','granularity':'day'}`, `cluster_by=['pu_location_id','do_location_id']`, `require_partition_filter=true`; incremental window `pickup_date between date_sub(date(var('run_month_start')), interval var('lookback_days', 3) day) and last_day(date(var('run_month_start')))`.
- Quarantine share test: `warn` above 2%, `error` above 5% of the run month's rows.
- Airflow DAG id `nyc_mobility_pipeline`: `schedule="@monthly"`, `start_date=datetime(2024,1,1)`, `end_date=datetime(2024,6,30)`, `catchup=True`, `max_active_runs=1`, default `retries=2`, `on_failure_callback` set. dbt is invoked with `--vars '{"run_month_start": "<logical month start>"}'`.
- Chatbot guard: `LIMIT_BYTES = 500 * 1024**2`; dry run with `use_query_cache=False`; execution with `maximum_bytes_billed=LIMIT_BYTES`; only a single `SELECT` statement (reject `INSERT/UPDATE/DELETE/DROP/MERGE/CREATE/ALTER/TRUNCATE` and multi-statement input); estimated cost at BigQuery on-demand rate `$6.25 per TiB`.
- Chatbot SQL may reference only tables in the `marts` dataset; any query touching `fct_trips` must filter on `pickup_date`.
- Tests: `pytest` from repo root; `tests/` holds Python tests; dbt project must pass `dbt parse`. No network calls in tests (mock HTTP and BigQuery).
- Commit messages: conventional prefix (`feat:`, `test:`, `docs:`, `chore:`), no co-author lines, no em dashes anywhere in code or docs.

## Review Focus

1. A month re-run (same `run_month_start`) must not duplicate raw rows or fact rows: pinned by `test_load_month_deletes_existing_month_rows_before_append` (Task 2) and the `unique` test on `fct_trips.trip_id` plus the demo script in Task 8.
2. Open-Meteo returns a payload whose `hourly` arrays may be empty or absent for a bad date range: `flatten_hourly` must return `[]`, not raise (`test_flatten_hourly_empty_payload`, Task 3).
3. LLM output wrapped in markdown fences or with trailing semicolons must still pass the guard: `test_strip_sql_fences_and_semicolon` (Task 9).
4. A query that is a `SELECT` but hides a DML statement after a semicolon or in a CTE (`WITH x AS (DELETE ...)`) must be rejected: `test_rejects_multi_statement` and `test_rejects_dml_inside_cte` (Task 9).
5. A `fct_trips` query without a `pickup_date` filter must be rerouted or blocked before execution, not fail at BigQuery: `test_fct_trips_without_partition_filter_is_rejected` (Task 9).

---

### Task 1: Repository scaffold and settings module

**Files:**
- Create: `pyproject.toml`, `.env.example`, `conftest.py` (repo root), `tests/__init__.py`, `airflow/include/nyc_mobility/__init__.py`, `airflow/include/nyc_mobility/config.py`, `scripts/setup_gcp.sh`
- Modify: `.gitignore` (add `.env`, `*.parquet`, `.pytest_cache/`)
- Test: `tests/test_config.py`

**Interfaces:**
- Produces: `nyc_mobility.config.Settings` (frozen dataclass) with fields `project_id: str`, `location: str`, `raw_tlc_dataset: str = "raw_tlc"`, `raw_weather_dataset: str = "raw_weather"`, `staging_dataset: str = "staging"`, `marts_dataset: str = "marts"`, `quarantine_dataset: str = "quarantine"`; classmethod `Settings.from_env() -> Settings` (raises `KeyError` naming `GCP_PROJECT_ID` when unset; `BQ_LOCATION` defaults to `"US"`).
- Produces: `nyc_mobility.config.month_start(value: str | date) -> date` (accepts `"2024-02-15"` or a `date`, returns the first day of that month) and `month_end(start: date) -> date` (last day of the month), `month_range(first: date, last: date) -> list[date]` (inclusive list of month starts).
- Produces: `conftest.py` at repo root inserts `airflow/include` onto `sys.path` so `import nyc_mobility` works in tests.

- [ ] **Step 1: Write `pyproject.toml`**

Project name `nyc-mobility-pipeline`, `requires-python = ">=3.12,<3.13"`. Runtime deps: `google-cloud-bigquery>=3.25`, `pyarrow>=16`, `pandas>=2.2`, `requests>=2.32`, `db-dtypes>=1.2`. Optional groups: `dev = ["pytest>=8", "pytest-mock>=3.14", "responses>=0.25"]`, `dbt = ["dbt-core>=1.8", "dbt-bigquery>=1.8"]`, `chatbot = ["streamlit>=1.38", "anthropic>=0.40", "sqlglot>=25", "altair>=5"]`, `airflow = ["apache-airflow>=3.0", "apache-airflow-providers-http>=5", "apache-airflow-providers-airbyte>=5"]`. `[tool.pytest.ini_options] testpaths = ["tests"]`, `filterwarnings = ["error::DeprecationWarning:nyc_mobility.*"]`.

- [ ] **Step 2: Create the venv and install**

Run: `uv venv --python 3.12 .venv && uv pip install --python .venv/bin/python -e ".[dev]"`
Expected: install succeeds.

- [ ] **Step 3: Write the failing tests in `tests/test_config.py`**

```python
from datetime import date
import pytest
from nyc_mobility.config import Settings, month_start, month_end, month_range

def test_settings_from_env(monkeypatch):
    monkeypatch.setenv("GCP_PROJECT_ID", "proj-x")
    monkeypatch.delenv("BQ_LOCATION", raising=False)
    s = Settings.from_env()
    assert s.project_id == "proj-x" and s.location == "US" and s.marts_dataset == "marts"

def test_settings_missing_project_raises(monkeypatch):
    monkeypatch.delenv("GCP_PROJECT_ID", raising=False)
    with pytest.raises(KeyError, match="GCP_PROJECT_ID"):
        Settings.from_env()

def test_month_helpers():
    assert month_start("2024-02-15") == date(2024, 2, 1)
    assert month_start(date(2024, 2, 29)) == date(2024, 2, 1)
    assert month_end(date(2024, 2, 1)) == date(2024, 2, 29)
    assert month_range(date(2024, 1, 1), date(2024, 3, 1)) == [date(2024,1,1), date(2024,2,1), date(2024,3,1)]
```

- [ ] **Step 4: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_config.py -v`
Expected: FAIL with `ModuleNotFoundError: nyc_mobility`.

- [ ] **Step 5: Implement `config.py` and root `conftest.py`**

Use only the standard library (`dataclasses`, `os`, `datetime`, `calendar`).

- [ ] **Step 6: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_config.py -v`
Expected: 3 passed, no warnings.

- [ ] **Step 7: Write `.env.example` and `scripts/setup_gcp.sh`**

`.env.example` lists every variable from Global Constraints with a comment each. `scripts/setup_gcp.sh` is an idempotent bash script (`set -euo pipefail`) that reads `GCP_PROJECT_ID` and `BQ_LOCATION`, runs `bq --location=$BQ_LOCATION mk --dataset --force` for each of the five datasets, and prints the service-account creation commands (`gcloud iam service-accounts create nyc-mobility-sa`, bind `roles/bigquery.dataEditor` and `roles/bigquery.jobUser`, `gcloud iam service-accounts keys create`) as instructions it executes only when `CREATE_SA=1`.

- [ ] **Step 8: Commit**

```bash
git add pyproject.toml .env.example .gitignore conftest.py tests airflow/include/nyc_mobility scripts/setup_gcp.sh
git commit -m "chore: scaffold project, settings module and GCP setup script"
```

---

### Task 2: Taxi Parquet loader

**Files:**
- Create: `airflow/include/nyc_mobility/ingest/__init__.py`, `airflow/include/nyc_mobility/ingest/taxi.py`
- Test: `tests/test_ingest_taxi.py`

**Interfaces:**
- Consumes: `Settings`, `month_end` from Task 1.
- Produces in `nyc_mobility.ingest.taxi`:
  - `TAXI_URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{yyyy_mm}.parquet"`
  - `taxi_parquet_url(month: date) -> str`
  - `download_parquet(url: str, dest: Path, session: requests.Session | None = None) -> Path` (streams to disk in 8 MiB chunks, raises `requests.HTTPError` on non-2xx, skips download when `dest` already exists with size > 0)
  - `prepare_month_table(path: Path, month: date) -> pyarrow.Table` (reads the Parquet, casts `tpep_pickup_datetime`/`tpep_dropoff_datetime` to `timestamp[us]` without timezone, appends `source_month` (date32) and `loaded_at` (timestamp[us], UTC now) columns)
  - `delete_month_rows(client: bigquery.Client, table_id: str, month: date) -> None` (runs `DELETE FROM `{table_id}` WHERE source_month = @month` with a query parameter; tolerates `NotFound` for a table that does not exist yet)
  - `load_month(client: bigquery.Client, settings: Settings, month: date, workdir: Path) -> int` (download → prepare → delete existing → `client.load_table_from_file` with `write_disposition=WRITE_APPEND`, `source_format=PARQUET`, `schema_update_options=[ALLOW_FIELD_ADDITION]`; returns rows loaded). Table id is `f"{settings.project_id}.{settings.raw_tlc_dataset}.yellow_trips"`.

- [ ] **Step 1: Write failing tests**

Tests build a tiny Parquet with `pyarrow` in `tmp_path` (3 rows, the TLC columns), use the `responses` library for HTTP, and a `MagicMock(spec=bigquery.Client)` for BigQuery.

```python
def test_taxi_parquet_url():
    assert taxi_parquet_url(date(2024, 3, 1)).endswith("yellow_tripdata_2024-03.parquet")

def test_download_parquet_streams_to_disk(tmp_path):  # responses mock returns b"PAR1..."; file written; second call does not re-request

def test_prepare_month_table_adds_metadata(tmp_path):  # source_month column == date(2024,1,1) for all rows; loaded_at present; pickup column type is timestamp[us] with tz None

def test_load_month_deletes_existing_month_rows_before_append(tmp_path):
    # client.query called once with SQL containing "DELETE FROM" and "source_month = @month";
    # then client.load_table_from_file called once with job_config.write_disposition == "WRITE_APPEND";
    # call order asserted via mock manager; return value == 3
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_ingest_taxi.py -v` → FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Implement `taxi.py`**

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_ingest_taxi.py -v` → all pass, no warnings.

- [ ] **Step 5: Commit**

```bash
git add airflow/include/nyc_mobility/ingest tests/test_ingest_taxi.py
git commit -m "feat: idempotent monthly taxi parquet loader for BigQuery"
```

---

### Task 3: Weather loader and Airbyte connector manifest

**Files:**
- Create: `airflow/include/nyc_mobility/ingest/weather.py`, `airbyte/connector_builder_open_meteo.yaml`, `airbyte/README.md`
- Test: `tests/test_ingest_weather.py`

**Interfaces:**
- Consumes: `Settings`, `month_end` (Task 1); `delete_month_rows` (Task 2, reuse it, do not duplicate).
- Produces in `nyc_mobility.ingest.weather`:
  - `OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"`, `NYC_LAT = 40.71`, `NYC_LON = -74.01`, `HOURLY_VARS = ["temperature_2m", "precipitation", "rain", "snowfall", "wind_speed_10m"]`
  - `open_meteo_params(month: date) -> dict[str, str]` (`start_date`, `end_date`, `latitude`, `longitude`, `hourly` comma-joined, `timezone="America/New_York"`)
  - `fetch_month(month: date, session: requests.Session | None = None) -> dict` (GET with params, `raise_for_status`, returns JSON)
  - `flatten_hourly(payload: dict, month: date) -> list[dict]` (zips `hourly.time[i]` with each variable; row keys exactly the raw weather columns from Global Constraints; `observed_at` is a naive `datetime`; returns `[]` when `hourly` or `hourly.time` is missing or empty)
  - `load_month(client: bigquery.Client, settings: Settings, month: date) -> int` (fetch → flatten → `delete_month_rows` → `client.load_table_from_json` with an explicit `bigquery.SchemaField` list matching Global Constraints and `WRITE_APPEND`; returns row count). Table id `f"{settings.project_id}.{settings.raw_weather_dataset}.weather_hourly"`.

- [ ] **Step 1: Write failing tests**

```python
def test_open_meteo_params_for_february():  # start_date "2024-02-01", end_date "2024-02-29", timezone America/New_York

def test_flatten_hourly_zips_arrays():  # 2-element payload → 2 rows; observed_at == datetime(2024,1,1,1,0); rain value preserved; source_month == date(2024,1,1)

def test_flatten_hourly_empty_payload():  # {"hourly": {}} and {} both → []

def test_load_month_deletes_then_loads(responses_mock, tmp_path):  # responses mock on OPEN_METEO_URL; client.query DELETE then load_table_from_json called with 2 rows; returns 2
```

- [ ] **Step 2: Run to verify fail** → `ModuleNotFoundError`.

- [ ] **Step 3: Implement `weather.py`**

- [ ] **Step 4: Run tests to verify pass** → all green, no warnings.

- [ ] **Step 5: Write the Airbyte Connector Builder manifest**

`airbyte/connector_builder_open_meteo.yaml`: a declarative low-code manifest (`version: "0.50.0"` style) with one stream `open_meteo_archive`, `url_base` = `OPEN_METEO_URL`, request parameters from `open_meteo_params` with `start_date`/`end_date` driven by a `DatetimeBasedCursor` (`cursor_field: start_date`, `datetime_format: "%Y-%m-%d"`, `step: P1M`, `start_datetime: 2024-01-01`, `end_datetime: 2024-06-30`), record selector `extractor.field_path: []` (whole response as one record). `airbyte/README.md` explains: import the YAML in Connector Builder, destination BigQuery dataset `raw_weather`, stream lands as `raw_weather.open_meteo_archive` with the `hourly` arrays unflattened; dbt flattens it when `weather_source: airbyte` (Task 4). Note that this path is optional; the Python loader is the default.

- [ ] **Step 6: Commit**

```bash
git add airflow/include/nyc_mobility/ingest/weather.py tests/test_ingest_weather.py airbyte
git commit -m "feat: open-meteo hourly weather loader and airbyte connector manifest"
```

---

### Task 4: dbt project, seeds, sources and staging models

**Files:**
- Create: `dbt/nyc_mobility/dbt_project.yml`, `dbt/nyc_mobility/packages.yml`, `dbt/nyc_mobility/profiles.yml`, `dbt/nyc_mobility/seeds/taxi_zone_lookup.csv`, `dbt/nyc_mobility/seeds/us_holidays.csv`, `dbt/nyc_mobility/seeds/_seeds.yml`, `dbt/nyc_mobility/models/staging/_sources.yml`, `dbt/nyc_mobility/models/staging/_staging.yml`, `dbt/nyc_mobility/models/staging/stg_trips.sql`, `dbt/nyc_mobility/models/staging/stg_weather_hourly.sql`
- Test: `tests/test_dbt_parse.py`

**Interfaces:**
- Produces `stg_trips` columns: `vendor_id INT64`, `pickup_datetime DATETIME`, `dropoff_datetime DATETIME`, `passenger_count INT64`, `trip_distance FLOAT64`, `ratecode_id INT64`, `store_and_fwd_flag STRING`, `pu_location_id INT64`, `do_location_id INT64`, `payment_type INT64`, `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `improvement_surcharge`, `total_amount`, `congestion_surcharge`, `airport_fee` (all FLOAT64), `pickup_date DATE`, `pickup_hour INT64` (0-23), `trip_duration_min FLOAT64` (`timestamp_diff(dropoff, pickup, second)/60.0`), `source_month DATE`, `loaded_at TIMESTAMP`.
- Produces `stg_weather_hourly` columns: `observed_at DATETIME`, `observed_date DATE`, `observed_hour INT64`, `temperature_2m`, `precipitation`, `rain`, `snowfall`, `wind_speed_10m`, `is_rainy BOOL`, `source_month DATE`.
- `dbt_project.yml`: `name: nyc_mobility`, `profile: nyc_mobility`, `vars: {run_month_start: "2024-01-01", lookback_days: 3, weather_source: "python"}`, model configs: `staging: +materialized: view, +schema: staging`; `intermediate: +materialized: view, +schema: staging`; `marts: +materialized: table, +schema: marts`. A `generate_schema_name` macro in `macros/generate_schema_name.sql` that returns the custom schema name as-is (no `<target>_` prefix) when one is set.
- `profiles.yml` target `dev`: `type: bigquery`, `method: service-account`, `keyfile: "{{ env_var('GOOGLE_APPLICATION_CREDENTIALS', '') }}"`, `project: "{{ env_var('GCP_PROJECT_ID', 'local-parse') }}"`, `dataset: staging`, `location: "{{ env_var('BQ_LOCATION', 'US') }}"`, `threads: 4`, `job_execution_timeout_seconds: 600`.

- [ ] **Step 1: Install dbt into the venv**

Run: `uv pip install --python .venv/bin/python -e ".[dev,dbt]"`

- [ ] **Step 2: Write the failing test `tests/test_dbt_parse.py`**

```python
def test_dbt_parse_succeeds():
    env = {**os.environ, "GCP_PROJECT_ID": "local-parse", "DBT_PROFILES_DIR": str(PROJECT)}
    r = subprocess.run([sys.executable, "-m", "dbt.cli.main", "deps"], cwd=PROJECT, env=env, capture_output=True, text=True)
    r = subprocess.run([sys.executable, "-m", "dbt.cli.main", "parse", "--no-partial-parse"], cwd=PROJECT, env=env, capture_output=True, text=True)
    assert r.returncode == 0, r.stdout + r.stderr
```
`PROJECT = Path(__file__).resolve().parents[1] / "dbt" / "nyc_mobility"`. Mark it `@pytest.mark.dbt` and register the marker in `pyproject.toml`.

- [ ] **Step 3: Run it** → FAIL (no project).

- [ ] **Step 4: Seeds**

Download `taxi_zone_lookup.csv` from `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv` (265 data rows). Hand-write `us_holidays.csv` with columns `holiday_date,holiday_name` for the 11 US federal holidays in 2024 (2024-01-01 New Year's Day, 2024-01-15 Martin Luther King Jr. Day, 2024-02-19 Presidents' Day, 2024-05-27 Memorial Day, 2024-06-19 Juneteenth, 2024-07-04 Independence Day, 2024-09-02 Labor Day, 2024-10-14 Columbus Day, 2024-11-11 Veterans Day, 2024-11-28 Thanksgiving Day, 2024-12-25 Christmas Day). `_seeds.yml` sets `holiday_date` column type `date` and `LocationID` type `int64`.

- [ ] **Step 5: Sources and staging models**

`_sources.yml`: source `raw_tlc` (database from `env_var('GCP_PROJECT_ID')`, table `yellow_trips`), source `raw_weather` (tables `weather_hourly` and `open_meteo_archive`). `stg_weather_hourly.sql` branches on `var('weather_source')`: `python` selects from `weather_hourly`; `airbyte` selects from `open_meteo_archive` using `unnest(json_value_array(hourly, '$.time')) as t with offset i` joined to the other arrays by offset (document the assumed Airbyte column `hourly` of type JSON in a comment). `_staging.yml` documents every column and adds `not_null` tests on `pickup_datetime`, `pu_location_id`, `observed_at`.

- [ ] **Step 6: Run the parse test** → PASS.

- [ ] **Step 7: Commit**

```bash
git add dbt tests/test_dbt_parse.py pyproject.toml
git commit -m "feat: dbt project with seeds, sources and staging models"
```

---

### Task 5: dbt intermediate models

**Files:**
- Create: `dbt/nyc_mobility/models/intermediate/int_trips_deduped.sql`, `int_trips_validated.sql`, `int_trips_with_weather.sql`, `_intermediate.yml`
- Test: `tests/test_dbt_parse.py` (existing) plus `tests/test_dbt_models.py` (static assertions on model text)

**Interfaces:**
- Consumes: `stg_trips`, `stg_weather_hourly` (Task 4).
- Produces `int_trips_deduped`: all `stg_trips` columns plus `trip_id STRING`; one row per `trip_id` (`qualify row_number() over (partition by trip_id order by loaded_at desc) = 1`). Rows are restricted to the run window so each run stays cheap: `where pickup_date between date_sub(date('{{ var("run_month_start") }}'), interval {{ var("lookback_days", 3) }} day) and last_day(date('{{ var("run_month_start") }}'))`.
- Produces `int_trips_validated`: `int_trips_deduped` columns plus `rejection_reason STRING` (rules and order from Global Constraints via a single `case` expression).
- Produces `int_trips_with_weather`: only rows with `rejection_reason is null`, left-joined to `stg_weather_hourly` on `datetime_trunc(pickup_datetime, hour) = observed_at`, adding `temperature_2m`, `precipitation`, `rain`, `snowfall`, `wind_speed_10m`, `is_rainy` (coalesced to `false`).

- [ ] **Step 1: Write failing static tests in `tests/test_dbt_models.py`**

```python
def test_validated_model_contains_every_rule():  # file text contains each of the 5 rejection_reason literals, in order
def test_deduped_uses_surrogate_key():  # contains "generate_surrogate_key" and the six key columns
def test_with_weather_joins_on_hour():  # contains "datetime_trunc(pickup_datetime, hour)"
```

- [ ] **Step 2: Run** → FAIL (files missing).
- [ ] **Step 3: Write the three models and `_intermediate.yml`** (`unique` + `not_null` on `int_trips_deduped.trip_id`; `accepted_values` on `rejection_reason` with the five literals).
- [ ] **Step 4: Run `tests/test_dbt_models.py` and `tests/test_dbt_parse.py`** → PASS.
- [ ] **Step 5: Commit** `git commit -m "feat: dbt intermediate dedupe, validation and weather join models"`

---

### Task 6: dbt marts, quarantine and data tests

**Files:**
- Create: `dbt/nyc_mobility/models/marts/fct_trips.sql`, `dim_zone.sql`, `dim_date.sql`, `agg_hourly_zone_demand.sql`, `agg_daily_weather_impact.sql`, `quarantine_trips.sql`, `_marts.yml`, `dbt/nyc_mobility/tests/assert_no_dropoff_before_pickup.sql`, `dbt/nyc_mobility/tests/assert_quarantine_share_below_threshold.sql`, `dbt/nyc_mobility/macros/quarantine_share.sql`
- Modify: `tests/test_dbt_models.py`

**Interfaces:**
- Consumes: Task 5 models, seeds from Task 4.
- Produces `fct_trips` (config from Global Constraints, verbatim): columns `trip_id`, `vendor_id`, `pickup_datetime`, `dropoff_datetime`, `pickup_date`, `pickup_hour`, `pu_location_id`, `do_location_id`, `passenger_count`, `trip_distance`, `trip_duration_min`, `payment_type`, `fare_amount`, `tip_amount`, `tolls_amount`, `total_amount`, `congestion_surcharge`, `airport_fee`, `temperature_2m`, `rain`, `snowfall`, `wind_speed_10m`, `is_rainy`, `source_month`.
- Produces `dim_zone` (from seed): `location_id`, `borough`, `zone`, `service_zone`, `is_airport BOOL` (true when `zone` contains "Airport" or `location_id in (1, 132, 138)`).
- Produces `dim_date` (`dbt_utils.date_spine` from 2024-01-01 to 2025-01-01): `date_day`, `year`, `month`, `day_of_week` (1=Sunday per BigQuery), `day_name`, `is_weekend`, `is_holiday`, `holiday_name`.
- Produces `agg_hourly_zone_demand` (table, partitioned by `pickup_date` day, clustered by `pu_location_id`): `pickup_date`, `pickup_hour`, `pu_location_id`, `borough`, `zone`, `trips`, `avg_fare`, `avg_duration_min`, `avg_tip_pct` (`tip_amount / nullif(fare_amount,0)` averaged, as percent), `is_weekend`, `is_holiday`.
- Produces `agg_daily_weather_impact` (table): `pickup_date`, `is_rainy`, `trips`, `avg_duration_min`, `avg_fare`, `total_revenue`, `borough`.
- Produces `quarantine_trips` (schema `quarantine`, table, partitioned by `pickup_date`): every `int_trips_validated` row with `rejection_reason is not null`, same columns plus `rejection_reason`, `run_month_start DATE`.
- Macro `quarantine_share(run_month_start)` returns a SQL subquery computing `rejected / total` for rows whose `pickup_date` falls in that month.
- Both agg tables cover all loaded history: full tables rebuilt from `fct_trips` with `where pickup_date >= date('2024-01-01')` (satisfies the required partition filter).

- [ ] **Step 1: Add failing static tests**

```python
def test_fct_trips_config_is_partitioned_and_filtered():  # text contains insert_overwrite, require_partition_filter = true, cluster_by with both ids, lookback_days
def test_marts_yml_has_required_tests():  # yaml loads; fct_trips.trip_id has unique+not_null; relationships on pu/do_location_id to dim_zone; accepted_values on payment_type [1,2,3,4,5,6]; accepted_range on fare_amount and trip_duration_min
```

- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Write the models, yml, singular tests and macro.** `assert_quarantine_share_below_threshold.sql` uses `{{ config(severity='error', warn_if='>0', error_if='>0') }}` on a query that returns a row when share > 0.05, and a sibling `assert_quarantine_share_warn.sql` with `severity='warn'` for share > 0.02.
- [ ] **Step 4: Run `tests/test_dbt_models.py` and `tests/test_dbt_parse.py`** → PASS.
- [ ] **Step 5: Commit** `git commit -m "feat: dbt marts with partitioned incremental fct_trips, dims, aggregates and tests"`

---

### Task 7: Airflow DAG

**Files:**
- Create: `airflow/dags/nyc_mobility_pipeline.py`, `airflow/include/nyc_mobility/alerts.py`, `airflow/requirements.txt`, `airflow/Dockerfile`, `airflow/docker-compose.override.yml`, `airflow/README.md`, `airflow/packages.txt` (empty)
- Test: `tests/test_dag.py`

**Interfaces:**
- Consumes: `nyc_mobility.ingest.taxi.load_month`, `nyc_mobility.ingest.weather.load_month`, `Settings`, `month_start`.
- Produces `nyc_mobility.alerts.on_failure_callback(context: dict) -> None` (logs a structured line `PIPELINE FAILURE dag=<id> task=<id> run=<run_id> url=<log_url>` and posts to `SLACK_WEBHOOK_URL` via `requests.post` when that env var is set).
- Produces DAG `nyc_mobility_pipeline` with tasks, in this dependency order: `check_source_available` (`HttpSensor`, `http_conn_id="tlc_cloudfront"`, `method="HEAD"`, `endpoint` = month's Parquet path, `poke_interval=300`, `timeout=3600`, `mode="reschedule"`) → `load_taxi_trips` (PythonOperator calling taxi `load_month`) and `load_weather` (branch: if Airflow Variable `use_airbyte_weather` == "true" run `sync_weather_airbyte` (`AirbyteTriggerSyncOperator`, `airbyte_conn_id="airbyte_default"`, `connection_id` from Variable `airbyte_weather_connection_id`) else `load_weather_python`; joined by an `EmptyOperator` `weather_loaded` with `trigger_rule=NONE_FAILED_MIN_ONE_SUCCESS`) → `dbt_seed` → `dbt_run` → `dbt_test` (each a `BashOperator` running `cd $DBT_PROJECT_DIR && dbt <cmd> --profiles-dir . --vars '{"run_month_start": "{{ ds }}"}'`; `dbt_test` sets `do_xcom_push=True` and exits non-zero on failure) → `route_on_tests` (`BranchPythonOperator` with `trigger_rule=ALL_DONE` reading upstream `dbt_test` state: success → `refresh_chatbot_metadata`, else `alert_and_stop`) → `refresh_chatbot_metadata` (PythonOperator writing `chatbot/metadata.json` with `months_loaded`, `last_run_at`, `quarantine_pct` queried from BigQuery) → `log_run_summary` (PythonOperator logging raw/fact row counts and quarantine % for the run month). `alert_and_stop` is a PythonOperator calling `on_failure_callback` and raising `AirflowFailException`.
- `{{ ds }}` is the run's logical date; the DAG derives `run_month_start = month_start(ds)` in each callable.
- `airflow/requirements.txt`: `apache-airflow-providers-http`, `apache-airflow-providers-airbyte`, `dbt-bigquery`, `google-cloud-bigquery`, `pyarrow`, `requests`. `airflow/Dockerfile`: `FROM astrocrpublic.azurecr.io/runtime:3.0-latest` (Astro Runtime 3.x) with `ENV DBT_PROJECT_DIR=/usr/local/airflow/include/dbt`, `GOOGLE_APPLICATION_CREDENTIALS=/usr/local/airflow/include/gcp-key.json`. `docker-compose.override.yml` mounts `../dbt/nyc_mobility` at `/usr/local/airflow/include/dbt` for scheduler, webserver/api-server and triggerer.

- [ ] **Step 1: Install Airflow into the venv**

Run: `uv pip install --python .venv/bin/python -e ".[dev,airflow]"` (use the Airflow constraints file for Python 3.12 if resolution fails: `-c https://raw.githubusercontent.com/apache/airflow/constraints-3.0.0/constraints-3.12.txt`).

- [ ] **Step 2: Write failing `tests/test_dag.py`**

```python
def test_dag_imports_without_errors():  # DagBag(dag_folder="airflow/dags", include_examples=False); assert not bag.import_errors; dag = bag.get_dag("nyc_mobility_pipeline")
def test_dag_schedule_and_backfill_settings():  # catchup True, max_active_runs 1, start 2024-01-01, end 2024-06-30, default_args retries == 2, on_failure_callback is alerts.on_failure_callback
def test_dag_task_graph():  # task ids set equals the list above; dbt_run upstream == {"dbt_seed"}; route_on_tests downstream == {"refresh_chatbot_metadata", "alert_and_stop"}
def test_on_failure_callback_posts_to_slack_when_configured(monkeypatch):  # requests.post mocked; called with the webhook URL and text containing the dag id
```

- [ ] **Step 3: Run** → FAIL.
- [ ] **Step 4: Implement the DAG and alerts module** (Airflow 3 imports: `from airflow.sdk import DAG` if available, else `from airflow import DAG`; `airflow.providers.standard.operators.python`, `airflow.providers.standard.operators.bash`, `airflow.providers.standard.operators.empty`, `airflow.providers.http.sensors.http.HttpSensor`, `airflow.providers.airbyte.operators.airbyte.AirbyteTriggerSyncOperator`).
- [ ] **Step 5: Run `tests/test_dag.py`** → PASS, no import warnings in output.
- [ ] **Step 6: Write `airflow/README.md`**: `astro dev init` was replaced by the committed files; `astro dev start`; required Airflow Connections (`tlc_cloudfront` HTTP host `https://d37ci6vzurychx.cloudfront.net`, `airbyte_default` host `host.docker.internal` port 8000), Variables, how to backfill (`astro dev run dags backfill ...` or UI), where to put `gcp-key.json`.
- [ ] **Step 7: Commit** `git commit -m "feat: airflow dag for monthly backfillable pipeline with test routing and alerts"`

---

### Task 8: Late-arriving data simulation

**Files:**
- Create: `scripts/inject_late_trips.py`, `scripts/verify_idempotency.py`, `docs/late_data_demo.md`
- Test: `tests/test_inject_late_trips.py`

**Interfaces:**
- Consumes: `Settings`, `month_end`.
- Produces `scripts/inject_late_trips.py` with functions `build_injection_sql(settings: Settings, month: date, n_rows: int = 500, shift_seconds: int = 37) -> str` (an `INSERT INTO raw_tlc.yellow_trips SELECT ... FROM raw_tlc.yellow_trips WHERE source_month = @month AND DATE(tpep_pickup_datetime) BETWEEN <last 3 days of month> ORDER BY RAND() LIMIT n`, shifting both timestamps by `shift_seconds` and setting `loaded_at = CURRENT_TIMESTAMP()`; `source_month` stays the injected month) and `main(argv)` with `--month 2024-01 --rows 500 --dry-run`.
- Produces `scripts/verify_idempotency.py`: `count_sql(settings, month) -> str` returning per-day counts for the month from `marts.fct_trips` (with the partition filter) and `trip_id` duplicate count; `main` prints a table and exits 1 if duplicates > 0.
- `docs/late_data_demo.md`: the 5-step procedure from spec §5.5 with the exact commands (`inject_late_trips.py --month 2024-01`, re-run the February DAG run via `astro dev run tasks clear ...`/UI, `verify_idempotency.py --month 2024-01`, clear and re-run again, compare).

- [ ] **Step 1: Failing tests**: `test_injection_sql_targets_last_three_days` (SQL contains `DATE(tpep_pickup_datetime) BETWEEN '2024-01-29' AND '2024-01-31'`, `LIMIT 500`, `TIMESTAMP_ADD`/`DATETIME_ADD` with 37 seconds), `test_injection_dry_run_prints_sql_without_running` (client mock not called), `test_count_sql_has_partition_filter`.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `git commit -m "feat: late-arriving trip injection and idempotency verification scripts"`

---

### Task 9: Chatbot core: guard, prompts, LLM client and pipeline

**Files:**
- Create: `chatbot/__init__.py`, `chatbot/guard.py`, `chatbot/prompts.py`, `chatbot/llm.py`, `chatbot/pipeline.py`, `chatbot/schema_catalog.py`
- Test: `tests/test_guard.py`, `tests/test_chatbot_pipeline.py`

**Interfaces:**
- Produces `chatbot.guard`:
  - `LIMIT_BYTES = 500 * 1024**2`, `PRICE_PER_TIB_USD = 6.25`
  - `class GuardError(ValueError)`
  - `clean_sql(text: str) -> str` (strips ``` fences with optional `sql` tag, leading/trailing whitespace, one trailing `;`)
  - `validate_select(sql: str, allowed_dataset: str = "marts") -> None` (parses with `sqlglot` dialect `bigquery`; raises `GuardError` when: more than one statement, root is not `SELECT`/`WITH`, any DML/DDL expression type appears anywhere in the tree, any table reference is outside `allowed_dataset`, or `fct_trips` is referenced and no `pickup_date` predicate exists in any `WHERE`)
  - `estimate_cost_usd(bytes_: int) -> float` (= `bytes_ / 1024**4 * PRICE_PER_TIB_USD`)
  - `@dataclass GuardResult: blocked: bool; bytes_scanned: int; cost_usd: float; reason: str | None; data: pandas.DataFrame | None`
  - `dry_run(client, sql) -> int` and `guarded_run(client: bigquery.Client, sql: str, limit_bytes: int = LIMIT_BYTES) -> GuardResult` (dry run first; blocked result when over limit; otherwise execute with `maximum_bytes_billed=limit_bytes`).
- Produces `chatbot.schema_catalog.MART_SCHEMA: dict[str, list[tuple[str, str, str]]]` (table → list of `(column, type, description)` for `fct_trips`, `dim_zone`, `dim_date`, `agg_hourly_zone_demand`, `agg_daily_weather_impact`; types and names must match Task 6) and `render_schema(project_id: str) -> str`.
- Produces `chatbot.prompts.SYSTEM_PROMPT_TEMPLATE` (the five rules from spec §5.6 verbatim plus the `marts` dataset only rule and the fully-qualified name format `` `{project}.marts.<table>` ``), `FEW_SHOT_EXAMPLES: list[tuple[str, str]]` (4 question/SQL pairs, including the rain-vs-duration Manhattan weekday-evening question), `build_sql_prompt(question: str, project_id: str) -> tuple[str, str]` (system, user), `build_rewrite_prompt(question, sql, bytes_scanned, project_id) -> tuple[str, str]` (asks for a cheaper rewrite using `agg_*` tables or a narrower date range), `build_answer_prompt(question, sql, df_preview_csv) -> tuple[str, str]` (plain-English answer).
- Produces `chatbot.llm.complete(system: str, user: str, model: str | None = None) -> str` using `anthropic.Anthropic()` and `client.beta.messages.create(model=model or os.environ.get("LLM_MODEL", "claude-opus-5-5"), max_tokens=2048, system=system, messages=[{"role": "user", "content": user}], betas=["server-side-fallback-2026-07-01"], fallbacks="default")`; returns the concatenated `text` blocks; raises `RuntimeError("model refused")` when `stop_reason == "refusal"`. Reference for the SDK shape: `/private/tmp/claude-501/bundled-skills/2.1.325/84b7fa218127f7d3de75f8017797dcdf/claude-api/python/claude-api/README.md`.
- Produces `chatbot.pipeline.answer_question(question: str, client: bigquery.Client, project_id: str, complete=llm.complete, limit_bytes: int = LIMIT_BYTES) -> ChatAnswer` where `@dataclass ChatAnswer: question; sql; bytes_scanned; cost_usd; blocked: bool; rerouted: bool; reason: str | None; data: DataFrame | None; answer_text: str | None; attempts: list[str]`. Flow: generate → `clean_sql` → `validate_select` (a `GuardError` triggers one rewrite attempt with the error message appended) → `guarded_run`; if blocked, one rewrite attempt via `build_rewrite_prompt`, then `guarded_run` again; still blocked → final blocked answer with reason "Query would scan X MB (limit 500 MB). Please narrow the date range or ask for an aggregate."; on success call `build_answer_prompt` for `answer_text`.

- [ ] **Step 1: Install chatbot deps** `uv pip install --python .venv/bin/python -e ".[dev,chatbot]"`
- [ ] **Step 2: Write failing `tests/test_guard.py`**

```python
def test_strip_sql_fences_and_semicolon():  # "```sql\nSELECT 1;\n```" -> "SELECT 1"
def test_accepts_simple_select_on_marts():  # select from `p.marts.agg_daily_weather_impact` passes
def test_rejects_dml():  # "DELETE FROM `p.marts.fct_trips` WHERE pickup_date='2024-01-01'" raises GuardError
def test_rejects_multi_statement():  # "SELECT 1; DROP TABLE x" raises
def test_rejects_dml_inside_cte():  # "WITH x AS (DELETE FROM `p.marts.fct_trips` WHERE pickup_date='2024-01-01') SELECT 1" raises
def test_rejects_table_outside_marts():  # `p.raw_tlc.yellow_trips` raises
def test_fct_trips_without_partition_filter_is_rejected():  # select count(*) from `p.marts.fct_trips` raises; with "where pickup_date between ..." passes
def test_estimate_cost():  # 1 TiB -> 6.25
def test_guarded_run_blocks_over_limit():  # client.query mock returns job with total_bytes_processed = LIMIT_BYTES+1; blocked True; client.query called once (dry run only)
def test_guarded_run_executes_under_limit():  # second client.query call has job_config.maximum_bytes_billed == LIMIT_BYTES; data is a DataFrame
```

- [ ] **Step 3: Write failing `tests/test_chatbot_pipeline.py`** with a fake `complete` returning scripted SQL per call:
`test_happy_path_returns_answer`, `test_guard_error_triggers_one_rewrite`, `test_blocked_then_rerouted_to_aggregate` (first SQL over limit, rewrite under limit → `rerouted True`), `test_blocked_twice_gives_blocked_answer` (`blocked True`, `attempts` length 2, reason mentions `500 MB`).
- [ ] **Step 4: Run both** → FAIL. **Step 5: Implement the five modules.** **Step 6: Run** → PASS, no warnings.
- [ ] **Step 7: Commit** `git commit -m "feat: cost-guarded text-to-sql chatbot core"`

---

### Task 10: Streamlit chatbot UI

**Files:**
- Create: `chatbot/app.py`, `chatbot/metadata.py`, `chatbot/metadata.json.example`
- Test: `tests/test_chatbot_metadata.py`

**Interfaces:**
- Consumes: `answer_question`, `ChatAnswer` (Task 9), `Settings`.
- Produces `chatbot.metadata.load_metadata(path: Path = Path("chatbot/metadata.json")) -> dict` returning `{"months_loaded": [...], "last_run_at": str | None, "quarantine_pct": float | None}` with those defaults when the file is missing, and `format_bytes(n: int) -> str` (`"42.0 MB"`, `"1.2 GB"`).
- `app.py`: `st.chat_input` + `st.session_state["history"]`; per answer: result `st.dataframe`, auto chart via `st.bar_chart` when the frame has one categorical and one numeric column else `st.line_chart` when a date column exists; `st.expander("SQL")`; badge text `Scanned: {format_bytes} · Est. cost: ${cost:.4f}`; `st.error` red banner with the reason when blocked and a note when rerouted; sidebar from `load_metadata`. Reads `GCP_PROJECT_ID` via `Settings.from_env()`. Run with `streamlit run chatbot/app.py`.

- [ ] **Step 1: Failing tests** for `load_metadata` defaults and file reading, `format_bytes` at 3 magnitudes.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement `metadata.py` and `app.py`.** **Step 4: Run** → PASS; also `python -c "import ast,sys; ast.parse(open('chatbot/app.py').read())"` succeeds.
- [ ] **Step 5: Commit** `git commit -m "feat: streamlit chat ui with cost badge and guard banner"`

---

### Task 11: Evaluation set and runner

**Files:**
- Create: `eval/questions.csv`, `eval/run_eval.py`, `eval/partition_cost_comparison.py`
- Test: `tests/test_eval.py`

**Interfaces:**
- Consumes: `answer_question`, `guard.dry_run`, `Settings`.
- `eval/questions.csv` columns: `id,category,question,gold_sql,expect_blocked` with 25 rows: 6 simple aggregate, 5 join with weather, 4 join with calendar, 5 time + location filter, 5 should be blocked (`expect_blocked=true`, `gold_sql` empty). Gold SQL uses `{project}` placeholder and the Task 6 column names.
- `eval/run_eval.py`: `load_questions(path) -> list[dict]`; `compare_results(a: DataFrame, b: DataFrame) -> bool` (sorted, numeric columns rounded to 2 dp, equal shape and values); `run(questions, client, project_id, complete) -> EvalReport` with `@dataclass EvalReport: execution_accuracy: float; avg_bytes_scanned: float; blocked: int; rerouted: int; rows: list[dict]`; `main` writes `eval/results.csv` and `eval/results.md`. Blocked questions count as correct when `expect_blocked` and the answer is blocked.
- `eval/partition_cost_comparison.py`: `COMPARISON_QUERIES: list[tuple[str, str, str]]` (name, SQL with partition filter, SQL without) for 5 queries on `fct_trips`; `main` dry-runs each pair with `use_query_cache=False` (dry runs are free; if BigQuery rejects the unfiltered variant because of the required partition filter, record the error text as the result for that row) and prints a markdown table of bytes and cost.

- [ ] **Step 1: Failing tests**: `test_questions_csv_has_25_rows_and_categories`, `test_compare_results_ignores_order_and_rounding`, `test_run_counts_blocked_as_correct_when_expected` (fake client/complete).
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `git commit -m "feat: evaluation set, runner and partition cost comparison"`

---

### Task 12: README, runbook and plan tweaks

**Files:**
- Create: `README.md`, `docs/runbook.md`, `docs/screenshots/.gitkeep`, `Makefile`
- Modify: `Plan.md` (§5.2: record the ruling that Airflow Python loaders are the primary ingestion path and Airbyte is the optional weather path; §6 repo structure to match what was built; §12 checklist unchanged)

**Interfaces:** none new.

- [ ] **Step 1: `Makefile`** targets: `venv`, `test` (`.venv/bin/pytest`), `dbt-parse`, `dbt-build MONTH=2024-01-01`, `airflow-start`, `chatbot`, `eval`.
- [ ] **Step 2: `README.md`**: one-paragraph summary, architecture diagram (from spec §3), quick start (env, `make venv`, `scripts/setup_gcp.sh`, `make dbt-build`, `make airflow-start`, `make chatbot`), repo map, design decisions (ingestion ruling, `insert_overwrite` rationale, guard thresholds), links to `docs/runbook.md`, `docs/late_data_demo.md`, `airflow/README.md`, `airbyte/README.md`.
- [ ] **Step 3: `docs/runbook.md`**: demo script from spec §9 with the exact commands, screenshot checklist mapped to `docs/screenshots/<name>.png`, backup-video note.
- [ ] **Step 4: Verify** `.venv/bin/pytest -q` passes and `make dbt-parse` passes.
- [ ] **Step 5: Commit** `git commit -m "docs: readme, runbook, makefile and plan updates"`
