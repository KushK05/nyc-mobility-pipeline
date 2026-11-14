"""Monthly, backfillable NYC taxi + weather ELT into BigQuery.

Each run loads one month (the run's logical month, ``{{ ds }}``): raw taxi Parquet
and hourly weather land in BigQuery, dbt builds staging/marts for that month, and
dbt test results decide whether the chatbot metadata is refreshed or the run is
stopped with an alert.

Cloud configuration comes from environment variables (``GCP_PROJECT_ID``,
``BQ_LOCATION``, ``DBT_PROJECT_DIR``, ``CHATBOT_METADATA_PATH``,
``SLACK_WEBHOOK_URL``); credentials live in Airflow Connections and Variables.
Nothing here touches the metadata DB or cloud APIs at parse time.
"""

import json
import logging
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

# Astro does not put include/ on PYTHONPATH by default.
_INCLUDE = Path(__file__).resolve().parents[1] / "include"
if _INCLUDE.is_dir() and str(_INCLUDE) not in sys.path:
    sys.path.insert(0, str(_INCLUDE))

try:
    from airflow.sdk import DAG
except ImportError:  # pragma: no cover - Airflow 2 fallback
    from airflow import DAG

try:
    from airflow.sdk import TriggerRule
except ImportError:  # pragma: no cover - Airflow < 3.1 fallback
    from airflow.utils.trigger_rule import TriggerRule

try:
    from airflow.sdk.exceptions import AirflowFailException
except ImportError:  # pragma: no cover - Airflow < 3.1 fallback
    from airflow.exceptions import AirflowFailException

from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import (
    BranchPythonOperator,
    PythonOperator,
)
from airflow.utils.state import TaskInstanceState

from nyc_mobility import alerts
from nyc_mobility.config import Settings, month_end, month_start

log = logging.getLogger(__name__)

DAG_ID = "nyc_mobility_pipeline"
DEFAULT_METADATA_PATH = "/usr/local/airflow/include/chatbot/metadata.json"
DBT_VARS = """--vars '{"run_month_start": "{{ ds }}"}'"""


def _dbt_command(cmd: str) -> str:
    return f"cd $DBT_PROJECT_DIR && dbt {cmd} --profiles-dir . {DBT_VARS}"


def _get_variable(key: str, default: str) -> str:
    """Read an Airflow Variable at task run time (never at parse time)."""
    try:
        from airflow.sdk import Variable
    except ImportError:  # pragma: no cover - Airflow 2 fallback
        from airflow.models import Variable
    return Variable.get(key, default=default)


def _bq_client(settings: Settings):
    from google.cloud import bigquery

    return bigquery.Client(project=settings.project_id, location=settings.location)


def _month_params(run_month):
    from google.cloud import bigquery

    return bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start", "DATE", run_month),
            bigquery.ScalarQueryParameter("end", "DATE", month_end(run_month)),
        ]
    )


def load_taxi_trips(**context) -> int:
    from nyc_mobility.ingest import taxi

    settings = Settings.from_env()
    run_month = month_start(context["ds"])
    with tempfile.TemporaryDirectory(prefix="nyc_taxi_") as workdir:
        rows = taxi.load_month(_bq_client(settings), settings, run_month, Path(workdir))
    log.info("Loaded %s taxi rows for %s", rows, run_month)
    return rows


def load_weather_python(**context) -> int:
    from nyc_mobility.ingest import weather

    settings = Settings.from_env()
    run_month = month_start(context["ds"])
    rows = weather.load_month(_bq_client(settings), settings, run_month)
    log.info("Loaded %s weather rows for %s", rows, run_month)
    return rows


def choose_weather_path(**context) -> str:
    use_airbyte = _get_variable("use_airbyte_weather", "false")
    if str(use_airbyte).strip().lower() == "true":
        return "sync_weather_airbyte"
    return "load_weather_python"


def _dbt_test_state(context) -> str | None:
    dag_run = context["dag_run"]
    ti = context["ti"]
    if hasattr(ti, "get_task_states"):  # Airflow 3 Task SDK
        states = ti.get_task_states(
            dag_id=dag_run.dag_id, task_ids=["dbt_test"], run_ids=[dag_run.run_id]
        )
        return states.get(dag_run.run_id, {}).get("dbt_test")
    return dag_run.get_task_instance("dbt_test").state  # pragma: no cover - Airflow 2


def route_on_tests(**context) -> str:
    state = _dbt_test_state(context)
    log.info("dbt_test finished with state %s", state)
    if state == TaskInstanceState.SUCCESS:
        return "refresh_chatbot_metadata"
    return "alert_and_stop"


def alert_and_stop(**context) -> None:
    alerts.on_failure_callback(context)
    raise AirflowFailException(
        f"dbt tests failed for month {month_start(context['ds'])}; stopping the run"
    )


def refresh_chatbot_metadata(**context) -> dict:
    from nyc_mobility.summary import (
        build_metadata,
        months_loaded_sql,
        quarantine_share_sql,
    )

    settings = Settings.from_env()
    run_month = month_start(context["ds"])
    client = _bq_client(settings)
    months = [row["source_month"] for row in client.query(months_loaded_sql(settings)).result()]
    share_rows = list(
        client.query(
            quarantine_share_sql(settings), job_config=_month_params(run_month)
        ).result()
    )
    share = share_rows[0]["quarantine_share"] if share_rows else None
    metadata = build_metadata(months, share, datetime.now(timezone.utc))

    path = Path(os.environ.get("CHATBOT_METADATA_PATH", DEFAULT_METADATA_PATH))
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(metadata, indent=2) + "\n")
    tmp.replace(path)
    log.info("Wrote chatbot metadata to %s: %s", path, metadata)
    return metadata


def log_run_summary(**context) -> dict:
    from nyc_mobility.summary import run_summary_sql

    settings = Settings.from_env()
    run_month = month_start(context["ds"])
    client = _bq_client(settings)
    row = next(
        iter(client.query(run_summary_sql(settings), job_config=_month_params(run_month)).result())
    )
    raw_rows, fact_rows, quarantine_rows = (
        row["raw_rows"],
        row["fact_rows"],
        row["quarantine_rows"],
    )
    total = fact_rows + quarantine_rows
    quarantine_pct = round(100 * quarantine_rows / total, 2) if total else 0.0
    log.info(
        "RUN SUMMARY month=%s raw_rows=%s fact_rows=%s quarantine_rows=%s quarantine_pct=%s",
        run_month,
        raw_rows,
        fact_rows,
        quarantine_rows,
        quarantine_pct,
    )
    return {
        "month": run_month.isoformat(),
        "raw_rows": raw_rows,
        "fact_rows": fact_rows,
        "quarantine_rows": quarantine_rows,
        "quarantine_pct": quarantine_pct,
    }


default_args = {
    "owner": "data-eng",
    "retries": 2,
    "on_failure_callback": alerts.on_failure_callback,
}

with DAG(
    dag_id=DAG_ID,
    description="Monthly NYC taxi + weather ELT into BigQuery with dbt and test routing",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 6, 30),
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["nyc_mobility", "bigquery", "dbt"],
    doc_md=__doc__,
) as dag:
    check_source_available = HttpSensor(
        task_id="check_source_available",
        http_conn_id="tlc_cloudfront",
        method="HEAD",
        endpoint="trip-data/yellow_tripdata_{{ ds[:7] }}.parquet",
        # CloudFront answers 403 for a file that is not published yet: keep poking.
        response_error_codes_allowlist=["403", "404"],
        poke_interval=300,
        timeout=3600,
        mode="reschedule",
    )

    load_taxi = PythonOperator(task_id="load_taxi_trips", python_callable=load_taxi_trips)

    choose_weather = BranchPythonOperator(
        task_id="choose_weather_path", python_callable=choose_weather_path
    )

    sync_weather_airbyte = AirbyteTriggerSyncOperator(
        task_id="sync_weather_airbyte",
        airbyte_conn_id="airbyte_default",
        # Templated: the Variable is resolved only when this branch actually runs.
        connection_id="{{ var.value.airbyte_weather_connection_id }}",
    )

    load_weather = PythonOperator(
        task_id="load_weather_python", python_callable=load_weather_python
    )

    weather_loaded = EmptyOperator(
        task_id="weather_loaded", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    dbt_seed = BashOperator(task_id="dbt_seed", bash_command=_dbt_command("seed"))
    dbt_run = BashOperator(task_id="dbt_run", bash_command=_dbt_command("run"))
    # `dbt test` exits non-zero on any failing test, which fails this task.
    dbt_test = BashOperator(
        task_id="dbt_test", bash_command=_dbt_command("test"), do_xcom_push=True
    )

    route = BranchPythonOperator(
        task_id="route_on_tests",
        python_callable=route_on_tests,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    refresh_metadata = PythonOperator(
        task_id="refresh_chatbot_metadata", python_callable=refresh_chatbot_metadata
    )

    stop = PythonOperator(
        task_id="alert_and_stop",
        python_callable=alert_and_stop,
        retries=0,
        # The callable already alerted; avoid a duplicate alert when it fails.
        on_failure_callback=None,
    )

    summary = PythonOperator(task_id="log_run_summary", python_callable=log_run_summary)

    check_source_available >> [load_taxi, choose_weather]
    choose_weather >> [sync_weather_airbyte, load_weather] >> weather_loaded
    [load_taxi, weather_loaded] >> dbt_seed >> dbt_run >> dbt_test >> route
    route >> [refresh_metadata, stop]
    refresh_metadata >> summary
