from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest
import yaml
from google.api_core.exceptions import BadRequest, Forbidden
from google.cloud import bigquery

from chatbot import guard, llm
from chatbot.guard import (
    LIMIT_BYTES,
    GuardError,
    clean_sql,
    estimate_cost_usd,
    guarded_run,
    validate_select,
)
from chatbot.prompts import FEW_SHOT_EXAMPLES, SYSTEM_PROMPT_TEMPLATE, build_sql_prompt
from chatbot.schema_catalog import MART_SCHEMA, render_schema

MARTS_YML = "dbt/nyc_mobility/models/marts/_marts.yml"
WEATHER = "SELECT pickup_date, trips FROM `p.marts.agg_daily_weather_impact` LIMIT 100"


def test_strip_sql_fences_and_semicolon():
    assert clean_sql("```sql\nSELECT 1;\n```") == "SELECT 1"
    assert clean_sql("```\nSELECT 1\n```") == "SELECT 1"
    assert clean_sql("  SELECT 1;  ") == "SELECT 1"


def test_accepts_simple_select_on_marts():
    validate_select(WEATHER)


def test_accepts_cte_and_union_on_marts():
    validate_select(
        "WITH w AS (SELECT * FROM `p.marts.agg_daily_weather_impact`) "
        "SELECT trips FROM w UNION ALL SELECT trips FROM `p.marts.agg_hourly_zone_demand`"
    )


def test_rejects_dml():
    with pytest.raises(GuardError):
        validate_select("DELETE FROM `p.marts.fct_trips` WHERE pickup_date='2024-01-01'")


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1; DROP TABLE x",
        "SELECT 1 FROM `p.marts.dim_zone`; SELECT 2 FROM `p.marts.dim_zone`",
    ],
)
def test_rejects_multi_statement(sql):
    with pytest.raises(GuardError):
        validate_select(sql)


def test_rejects_dml_inside_cte():
    with pytest.raises(GuardError):
        validate_select(
            "WITH x AS (DELETE FROM `p.marts.fct_trips` WHERE pickup_date='2024-01-01') SELECT 1"
        )


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM `p.raw_tlc.yellow_trips`",
        "SELECT * FROM `p.marts.dim_zone` WHERE location_id IN "
        "(SELECT PULocationID FROM `p.raw_tlc.yellow_trips`)",
        "SELECT * FROM yellow_trips",
    ],
)
def test_rejects_table_outside_marts(sql):
    with pytest.raises(GuardError):
        validate_select(sql)


@pytest.mark.parametrize(
    "sql",
    ["UPDATE `p.marts.dim_zone` SET zone = 'x' WHERE true", "", "SELEC 1 FROM"],
)
def test_rejects_non_select_and_unparseable(sql):
    with pytest.raises(GuardError):
        validate_select(sql)


def test_fct_trips_without_partition_filter_is_rejected():
    with pytest.raises(GuardError, match="pickup_date"):
        validate_select("SELECT count(*) FROM `p.marts.fct_trips`")
    with pytest.raises(GuardError, match="pickup_date"):
        validate_select(
            "SELECT count(*) FROM `p.marts.fct_trips` t JOIN `p.marts.dim_date` d "
            "ON t.pickup_date = d.date_day AND t.pickup_date = '2024-01-01'"
        )
    validate_select(
        "SELECT count(*) FROM `p.marts.fct_trips` "
        "WHERE pickup_date BETWEEN '2024-01-01' AND '2024-01-31'"
    )


def test_estimate_cost():
    assert estimate_cost_usd(1024**4) == pytest.approx(6.25)


def _job(bytes_processed=None, df=None):
    job = MagicMock()
    job.total_bytes_processed = bytes_processed
    job.to_dataframe.return_value = df
    return job


def test_guarded_run_blocks_over_limit():
    client = MagicMock(spec=bigquery.Client)
    client.query.return_value = _job(LIMIT_BYTES + 1)
    result = guarded_run(client, WEATHER)
    assert result.blocked is True
    assert result.data is None
    assert result.bytes_scanned == LIMIT_BYTES + 1
    assert "limit 500 MB" in result.reason
    client.query.assert_called_once()
    config = client.query.call_args.kwargs["job_config"]
    assert config.dry_run is True
    assert config.use_query_cache is False


def test_guarded_run_executes_under_limit():
    client = MagicMock(spec=bigquery.Client)
    df = pd.DataFrame({"trips": [1]})
    client.query.side_effect = [_job(1024**2), _job(df=df)]
    result = guarded_run(client, WEATHER)
    assert result.blocked is False
    assert result.reason is None
    assert isinstance(result.data, pd.DataFrame)
    assert result.cost_usd == pytest.approx(estimate_cost_usd(1024**2))
    second = client.query.call_args_list[1]
    assert second.args[0] == WEATHER
    assert second.kwargs["job_config"].maximum_bytes_billed == LIMIT_BYTES


def test_dry_run_returns_bytes_without_executing():
    client = MagicMock(spec=bigquery.Client)
    client.query.return_value = _job(1234)
    assert guard.dry_run(client, WEATHER) == 1234
    config = client.query.call_args.kwargs["job_config"]
    assert config.dry_run is True and config.use_query_cache is False


def test_table_error_names_the_table():
    with pytest.raises(GuardError, match=r"Table `p\.raw_tlc\.yellow_trips` is outside"):
        validate_select("SELECT * FROM `p.raw_tlc.yellow_trips`")


def test_mart_schema_mirrors_dbt_marts():
    with open(MARTS_YML) as f:
        models = {m["name"]: [c["name"] for c in m["columns"]] for m in yaml.safe_load(f)["models"]}
    assert set(MART_SCHEMA) == {
        "fct_trips", "dim_zone", "dim_date", "agg_hourly_zone_demand", "agg_daily_weather_impact"
    }
    for table, columns in MART_SCHEMA.items():
        assert [c[0] for c in columns] == models[table]
    rendered = render_schema("p")
    assert "`p.marts.fct_trips`" in rendered


def test_system_prompt_rules_and_few_shots_pass_the_guard():
    for rule in [
        "- Only use tables in the `marts` dataset.",
        "- Prefer `agg_hourly_zone_demand` / `agg_daily_weather_impact` over `fct_trips` whenever possible.",
        "- Any query on `fct_trips` must filter on `pickup_date`.",
        "- Always add `LIMIT 100` unless aggregating.",
        "- Return only SQL, no explanation or markdown fences.",
        "- Fully qualify every table as `{project}.marts.<table>`.",
        "- Use BigQuery Standard SQL.",
    ]:
        assert rule in SYSTEM_PROMPT_TEMPLATE
    assert len(FEW_SHOT_EXAMPLES) == 4
    for _, sql in FEW_SHOT_EXAMPLES:
        validate_select(sql.replace("{project}", "p"))
    system, user = build_sql_prompt("How many trips?", "p")
    assert user == "How many trips?"
    assert "`p.marts.agg_daily_weather_impact`" in system
    assert "{project}" not in system


def test_headline_few_shot_uses_hourly_aggregate():
    question, sql = FEW_SHOT_EXAMPLES[0]
    assert "rain" in question.lower() and "manhattan" in question.lower()
    text = " ".join(sql.lower().split())
    assert "marts.agg_hourly_zone_demand" in text
    assert "fct_trips" not in text
    assert "sum(avg_duration_min * trips) / sum(trips)" in text
    for clause in [
        "borough = 'manhattan'",
        "pickup_hour between 17 and 20",
        "not is_weekend",
        "pickup_date between '2024-01-01' and '2024-06-30'",
        "group by is_rainy",
    ]:
        assert clause in text, clause


class _FakeMistral:
    calls: list = []
    api_keys: list = []
    finish_reason = "stop"
    content = "select 1"

    def __init__(self, api_key):
        _FakeMistral.api_keys.append(api_key)
        self.chat = self

    def complete(self, **kwargs):
        _FakeMistral.calls.append(kwargs)
        message = SimpleNamespace(content=_FakeMistral.content)
        choice = SimpleNamespace(finish_reason=_FakeMistral.finish_reason, message=message)
        return SimpleNamespace(choices=[choice])


@pytest.fixture
def fake_mistral(monkeypatch):
    _FakeMistral.calls = []
    _FakeMistral.api_keys = []
    _FakeMistral.finish_reason = "stop"
    _FakeMistral.content = "select 1"
    monkeypatch.setenv("MISTRAL_API_KEY", "test-key")
    monkeypatch.delenv("LLM_MODEL", raising=False)
    monkeypatch.setattr(llm, "Mistral", _FakeMistral)
    return _FakeMistral


def test_complete_uses_default_model_and_system_prompt(fake_mistral):
    assert llm.complete("sys", "question") == "select 1"
    call = fake_mistral.calls[0]
    assert call["model"] == "mistral-medium-latest"
    assert call["messages"] == [
        {"role": "system", "content": "sys"},
        {"role": "user", "content": "question"},
    ]
    assert call["max_tokens"] == 4096
    assert call["temperature"] == 0
    assert fake_mistral.api_keys == ["test-key"]


def test_complete_model_from_env_and_argument(fake_mistral, monkeypatch):
    monkeypatch.setenv("LLM_MODEL", "env-model")
    llm.complete("s", "u")
    llm.complete("s", "u", model="arg-model")
    assert [c["model"] for c in fake_mistral.calls] == ["env-model", "arg-model"]


@pytest.mark.parametrize("reason", ["length", "model_length"])
def test_complete_raises_on_truncation(fake_mistral, reason):
    fake_mistral.finish_reason = reason
    with pytest.raises(RuntimeError, match="truncated"):
        llm.complete("s", "u")


def test_complete_raises_on_error_finish_reason(fake_mistral):
    fake_mistral.finish_reason = "error"
    with pytest.raises(RuntimeError, match="model error"):
        llm.complete("s", "u")


def test_complete_joins_chunked_content(fake_mistral):
    fake_mistral.content = [SimpleNamespace(text="sel"), SimpleNamespace(text="ect 1")]
    assert llm.complete("s", "u") == "select 1"


def test_complete_requires_api_key(fake_mistral, monkeypatch):
    monkeypatch.delenv("MISTRAL_API_KEY")
    with pytest.raises(RuntimeError, match="MISTRAL_API_KEY"):
        llm.complete("s", "u")
    assert fake_mistral.calls == []


def test_project_pinning():
    other = "SELECT * FROM `bigquery-public-data.marts.dim_zone`"
    validate_select(other)  # no project pinned: dataset rule only
    with pytest.raises(GuardError, match="outside the `p` project"):
        validate_select(other, allowed_project="p")
    with pytest.raises(GuardError, match="outside the `p` project"):
        validate_select(
            "SELECT * FROM `p.marts.dim_zone` z JOIN `q.marts.dim_date` d ON true",
            allowed_project="p",
        )
    validate_select("SELECT * FROM `p.marts.dim_zone`", allowed_project="p")
    validate_select("SELECT * FROM marts.dim_zone", allowed_project="p")
    validate_select(
        "WITH c AS (SELECT * FROM `p.marts.dim_zone`) SELECT * FROM c", allowed_project="p"
    )


def test_dry_run_wraps_bigquery_errors():
    client = MagicMock(spec=bigquery.Client)
    client.query.side_effect = BadRequest("Unrecognized name: foo")
    with pytest.raises(GuardError, match="BigQuery rejected the query: Unrecognized name: foo"):
        guarded_run(client, WEATHER)


def test_guarded_run_wraps_execution_errors():
    client = MagicMock(spec=bigquery.Client)
    failing = MagicMock()
    failing.to_dataframe.side_effect = Forbidden("Query exceeded limit for bytes billed")
    client.query.side_effect = [_job(1024**2), failing]
    with pytest.raises(GuardError, match="BigQuery rejected the query: Query exceeded"):
        guarded_run(client, WEATHER)
