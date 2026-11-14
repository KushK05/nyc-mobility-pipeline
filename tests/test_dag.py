import inspect
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO = Path(__file__).resolve().parents[1]
DAG_ID = "nyc_mobility_pipeline"
EXPECTED_TASKS = {
    "check_source_available",
    "load_taxi_trips",
    "choose_weather_path",
    "sync_weather_airbyte",
    "load_weather_python",
    "weather_loaded",
    "dbt_seed",
    "dbt_run",
    "dbt_test",
    "route_on_tests",
    "refresh_chatbot_metadata",
    "alert_and_stop",
    "log_run_summary",
}


@pytest.fixture(scope="module")
def dagbag(tmp_path_factory):
    # Airflow reads AIRFLOW_HOME on first import; point it at a temp dir so no
    # airflow.cfg or logs land in the repo. No GCP env vars are set on purpose:
    # the DAG must parse without cloud configuration.
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("AIRFLOW_HOME", str(tmp_path_factory.mktemp("airflow_home")))
        mp.setenv("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
        mp.delenv("GCP_PROJECT_ID", raising=False)
        try:
            from airflow.dag_processing.dagbag import DagBag
        except ImportError:  # pragma: no cover - older Airflow layout
            from airflow.models.dagbag import DagBag
        # Airflow 3.4 dropped include_examples (load_examples is off via env).
        kwargs = {}
        if "include_examples" in inspect.signature(DagBag.__init__).parameters:
            kwargs["include_examples"] = False
        yield DagBag(dag_folder=str(REPO / "airflow" / "dags"), **kwargs)


@pytest.fixture(scope="module")
def dag(dagbag):
    # dagbag.dags is the parsed result; get_dag() would query the metadata DB.
    return dagbag.dags.get(DAG_ID)


def test_dag_imports_without_errors(dagbag, dag):
    assert not dagbag.import_errors
    assert dag is not None


def test_dag_schedule_and_backfill_settings(dag):
    from nyc_mobility import alerts

    assert dag.catchup is True
    assert dag.max_active_runs == 1
    assert dag.start_date.date() == datetime(2024, 1, 1).date()
    assert dag.end_date.date() == datetime(2024, 6, 30).date()
    assert dag.default_args["retries"] == 2
    assert dag.default_args["on_failure_callback"] is alerts.on_failure_callback


def test_dag_task_graph(dag):
    assert set(dag.task_ids) == EXPECTED_TASKS
    up = lambda t: dag.get_task(t).upstream_task_ids  # noqa: E731
    down = lambda t: dag.get_task(t).downstream_task_ids  # noqa: E731
    assert down("check_source_available") == {"load_taxi_trips", "choose_weather_path"}
    assert down("choose_weather_path") == {"sync_weather_airbyte", "load_weather_python"}
    assert up("weather_loaded") == {"sync_weather_airbyte", "load_weather_python"}
    assert up("dbt_seed") == {"load_taxi_trips", "weather_loaded"}
    assert up("dbt_run") == {"dbt_seed"}
    assert up("dbt_test") == {"dbt_run"}
    assert up("route_on_tests") == {"dbt_test"}
    assert down("route_on_tests") == {"refresh_chatbot_metadata", "alert_and_stop"}
    assert up("log_run_summary") == {"refresh_chatbot_metadata"}


def test_task_settings(dag):
    sensor = dag.get_task("check_source_available")
    assert sensor.http_conn_id == "tlc_cloudfront"
    assert sensor.method == "HEAD"
    assert sensor.endpoint == "trip-data/yellow_tripdata_{{ ds[:7] }}.parquet"
    assert sensor.poke_interval == 300
    assert sensor.timeout == 3600
    assert sensor.mode == "reschedule"

    airbyte = dag.get_task("sync_weather_airbyte")
    assert airbyte.airbyte_conn_id == "airbyte_default"
    assert "airbyte_weather_connection_id" in airbyte.connection_id

    assert dag.get_task("weather_loaded").trigger_rule == "none_failed_min_one_success"
    assert dag.get_task("route_on_tests").trigger_rule == "all_done"

    for cmd in ("seed", "run", "test"):
        command = dag.get_task(f"dbt_{cmd}").bash_command
        assert f"dbt {cmd} --profiles-dir ." in command
        assert "cd $DBT_PROJECT_DIR" in command
        assert """--vars '{"run_month_start": "{{ ds }}"}'""" in command
    assert dag.get_task("dbt_test").do_xcom_push is True


def test_choose_weather_path_reads_variable(dag, monkeypatch):
    import importlib

    module = importlib.import_module(dag.get_task("choose_weather_path").python_callable.__module__)
    monkeypatch.setattr(module, "_get_variable", lambda key, default: "true")
    assert module.choose_weather_path() == "sync_weather_airbyte"
    monkeypatch.setattr(module, "_get_variable", lambda key, default: default)
    assert module.choose_weather_path() == "load_weather_python"


def test_route_on_tests_branches_on_dbt_test_state(dag):
    callable_ = dag.get_task("route_on_tests").python_callable

    def context(state):
        ti = MagicMock()
        ti.get_task_states.return_value = {"run_1": {"dbt_test": state}}
        dag_run = MagicMock(run_id="run_1", dag_id=DAG_ID)
        return {"ti": ti, "dag_run": dag_run}

    assert callable_(**context("success")) == "refresh_chatbot_metadata"
    assert callable_(**context("failed")) == "alert_and_stop"
    assert callable_(**context("upstream_failed")) == "alert_and_stop"


def test_alert_and_stop_raises_fail_exception(dag, monkeypatch):
    from airflow.sdk.exceptions import AirflowFailException

    from nyc_mobility import alerts

    calls = []
    monkeypatch.setattr(alerts, "on_failure_callback", lambda ctx: calls.append(ctx))
    with pytest.raises(AirflowFailException):
        dag.get_task("alert_and_stop").python_callable(ds="2024-01-01")
    assert calls


def _context(**extra):
    ti = MagicMock(task_id="dbt_test", log_url="http://airflow/log")
    dag = MagicMock(dag_id=DAG_ID)
    return {"ti": ti, "dag": dag, "run_id": "scheduled__2024-01-01", **extra}


def test_on_failure_callback_posts_to_slack_when_configured(monkeypatch, caplog):
    from nyc_mobility import alerts

    post = MagicMock()
    monkeypatch.setattr(alerts.requests, "post", post)
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/abc")
    with caplog.at_level("ERROR", logger="nyc_mobility.alerts"):
        alerts.on_failure_callback(_context())
    post.assert_called_once()
    args, kwargs = post.call_args
    assert args[0] == "https://hooks.slack.test/abc"
    assert DAG_ID in kwargs["json"]["text"]
    assert (
        f"PIPELINE FAILURE dag={DAG_ID} task=dbt_test run=scheduled__2024-01-01 "
        "url=http://airflow/log" in caplog.text
    )


def test_on_failure_callback_skips_slack_without_webhook(monkeypatch):
    from nyc_mobility import alerts

    post = MagicMock()
    monkeypatch.setattr(alerts.requests, "post", post)
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    alerts.on_failure_callback(_context())
    post.assert_not_called()


def test_on_failure_callback_never_raises_on_slack_error(monkeypatch):
    import requests

    from nyc_mobility import alerts

    monkeypatch.setattr(alerts.requests, "post", MagicMock(side_effect=requests.ConnectionError))
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/abc")
    alerts.on_failure_callback(_context())


def test_refresh_chatbot_metadata_writes_json(dag, monkeypatch, tmp_path):
    import importlib
    import json

    module = importlib.import_module(
        dag.get_task("refresh_chatbot_metadata").python_callable.__module__
    )
    client = MagicMock()
    months_rows = [{"source_month": datetime(2024, 1, 1).date()}, {"source_month": datetime(2024, 2, 1).date()}]
    share_rows = [{"quarantine_share": 0.0123}]
    client.query.return_value.result.side_effect = [months_rows, share_rows]
    monkeypatch.setattr(module, "_bq_client", lambda settings: client)
    monkeypatch.setenv("GCP_PROJECT_ID", "proj")
    out = tmp_path / "chatbot" / "metadata.json"
    monkeypatch.setenv("CHATBOT_METADATA_PATH", str(out))

    module.refresh_chatbot_metadata(ds="2024-02-01")

    data = json.loads(out.read_text())
    assert data["months_loaded"] == ["2024-01-01", "2024-02-01"]
    assert data["quarantine_pct"] == pytest.approx(1.23)
    assert datetime.fromisoformat(data["last_run_at"]).tzinfo == timezone.utc
    params = client.query.call_args_list[1].kwargs["job_config"].query_parameters
    assert {p.name: p.value for p in params} == {
        "start": datetime(2024, 2, 1).date(),
        "end": datetime(2024, 2, 29).date(),
    }


def test_log_run_summary_reports_counts(dag, monkeypatch, caplog):
    import importlib

    module = importlib.import_module(dag.get_task("log_run_summary").python_callable.__module__)
    client = MagicMock()
    client.query.return_value.result.return_value = [
        {"raw_rows": 1000, "fact_rows": 990, "quarantine_rows": 10}
    ]
    monkeypatch.setattr(module, "_bq_client", lambda settings: client)
    monkeypatch.setenv("GCP_PROJECT_ID", "proj")

    with caplog.at_level("INFO"):
        summary = module.log_run_summary(ds="2024-03-01")

    assert summary == {
        "month": "2024-03-01",
        "raw_rows": 1000,
        "fact_rows": 990,
        "quarantine_rows": 10,
        "quarantine_pct": 1.0,
    }
    assert "RUN SUMMARY month=2024-03-01" in caplog.text
