from unittest.mock import MagicMock

import pandas as pd
import pytest
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

from chatbot.guard import LIMIT_BYTES
from chatbot.pipeline import ChatAnswer, answer_question

AGG_SQL = "SELECT sum(trips) AS trips FROM `p.marts.agg_hourly_zone_demand`"
BIG_SQL = (
    "SELECT count(*) AS trips FROM `p.marts.fct_trips` "
    "WHERE pickup_date BETWEEN '2024-01-01' AND '2024-12-31'"
)
BAD_SQL = "SELECT count(*) FROM `p.raw_tlc.yellow_trips`"


class ScriptedComplete:
    """Fake LLM: returns scripted replies in order and records every prompt."""

    def __init__(self, *replies):
        self.replies = list(replies)
        self.calls = []

    def __call__(self, system, user):
        self.calls.append((system, user))
        return self.replies.pop(0)


def _job(bytes_processed=None, df=None):
    job = MagicMock()
    job.total_bytes_processed = bytes_processed
    job.to_dataframe.return_value = df
    return job


def _client(*jobs):
    client = MagicMock(spec=bigquery.Client)
    client.query.side_effect = list(jobs)
    return client


DF = pd.DataFrame({"trips": [42]})


def test_happy_path_returns_answer():
    complete = ScriptedComplete(f"```sql\n{AGG_SQL};\n```", "There were 42 trips.")
    client = _client(_job(10 * 1024**2), _job(df=DF))
    answer = answer_question("How many trips?", client, "p", complete=complete)
    assert isinstance(answer, ChatAnswer)
    assert answer.sql == AGG_SQL
    assert answer.attempts == [AGG_SQL]
    assert answer.blocked is False
    assert answer.rerouted is False
    assert answer.reason is None
    assert answer.bytes_scanned == 10 * 1024**2
    assert answer.cost_usd > 0
    assert answer.data is DF
    assert answer.answer_text == "There were 42 trips."
    answer_user = complete.calls[1][1]
    assert AGG_SQL in answer_user and "42" in answer_user


def test_guard_error_triggers_one_rewrite():
    complete = ScriptedComplete(BAD_SQL, AGG_SQL, "42 trips.")
    client = _client(_job(1024**2), _job(df=DF))
    answer = answer_question("How many trips?", client, "p", complete=complete)
    assert answer.attempts == [BAD_SQL, AGG_SQL]
    assert answer.sql == AGG_SQL
    assert answer.rerouted is True
    assert answer.blocked is False
    rewrite_user = complete.calls[1][1]
    assert BAD_SQL in rewrite_user
    assert "marts" in rewrite_user


def test_guard_error_twice_is_blocked_without_running():
    complete = ScriptedComplete(BAD_SQL, BAD_SQL)
    client = _client()
    answer = answer_question("How many trips?", client, "p", complete=complete)
    assert answer.blocked is True
    assert answer.attempts == [BAD_SQL, BAD_SQL]
    assert "marts" in answer.reason
    assert answer.data is None and answer.answer_text is None
    client.query.assert_not_called()


def test_blocked_then_rerouted_to_aggregate():
    complete = ScriptedComplete(BIG_SQL, AGG_SQL, "42 trips.")
    client = _client(_job(LIMIT_BYTES + 1), _job(1024**2), _job(df=DF))
    answer = answer_question("How many trips?", client, "p", complete=complete)
    assert answer.rerouted is True
    assert answer.blocked is False
    assert answer.sql == AGG_SQL
    assert answer.attempts == [BIG_SQL, AGG_SQL]
    assert answer.bytes_scanned == 1024**2
    assert answer.answer_text == "42 trips."
    rewrite_user = complete.calls[1][1]
    assert BIG_SQL in rewrite_user
    assert "500 MB" in rewrite_user


def test_blocked_twice_gives_blocked_answer():
    complete = ScriptedComplete(BIG_SQL, BIG_SQL)
    client = _client(_job(LIMIT_BYTES * 2), _job(LIMIT_BYTES * 2))
    answer = answer_question("How many trips?", client, "p", complete=complete)
    assert answer.blocked is True
    assert answer.rerouted is False
    assert len(answer.attempts) == 2
    assert "500 MB" in answer.reason
    assert answer.reason.startswith("Query would scan 1000 MB")
    assert answer.data is None and answer.answer_text is None
    assert len(complete.calls) == 2


def test_other_project_table_triggers_rewrite():
    other = "SELECT sum(trips) AS trips FROM `bigquery-public-data.marts.agg_hourly_zone_demand`"
    complete = ScriptedComplete(other, AGG_SQL, "42 trips.")
    client = _client(_job(1024**2), _job(df=DF))
    answer = answer_question("How many trips?", client, "p", complete=complete)
    assert answer.attempts == [other, AGG_SQL]
    assert answer.blocked is False
    assert "outside the `p` project" in complete.calls[1][1]


def test_bigquery_error_triggers_one_rewrite():
    complete = ScriptedComplete(AGG_SQL, AGG_SQL.replace("trips)", "trips) AS t, 1"), "42 trips.")
    client = _client(BadRequest("Unrecognized name: foo"), _job(1024**2), _job(df=DF))
    answer = answer_question("How many trips?", client, "p", complete=complete)
    assert answer.blocked is False
    assert answer.rerouted is True
    assert len(answer.attempts) == 2
    assert answer.answer_text == "42 trips."
    assert "BigQuery rejected the query: Unrecognized name: foo" in complete.calls[1][1]


def test_bigquery_error_twice_gives_blocked_answer():
    complete = ScriptedComplete(AGG_SQL, AGG_SQL)
    client = _client(BadRequest("Unrecognized name: foo"), BadRequest("Unrecognized name: bar"))
    answer = answer_question("How many trips?", client, "p", complete=complete)
    assert answer.blocked is True
    assert "BigQuery rejected" in answer.reason
    assert "bar" in answer.reason
    assert answer.attempts == [AGG_SQL, AGG_SQL]
    assert answer.data is None and answer.answer_text is None


def test_cost_rewrite_rejected_reports_original_query():
    complete = ScriptedComplete(BIG_SQL, BAD_SQL)
    client = _client(_job(LIMIT_BYTES * 2))
    answer = answer_question("How many trips?", client, "p", complete=complete)
    assert answer.blocked is True
    assert answer.sql == BIG_SQL
    assert answer.bytes_scanned == LIMIT_BYTES * 2
    assert answer.attempts == [BIG_SQL, BAD_SQL]
    assert answer.reason.startswith("Query would scan 1000 MB (limit 500 MB).")
    assert "outside the `marts` dataset" in answer.reason
    client.query.assert_called_once()


STAR_SQL = (
    "SELECT * FROM `p.marts.fct_trips` "
    "WHERE pickup_date = '2024-01-01' LIMIT 100"
)
DUMP_SQL = "SELECT * FROM `p.marts.fct_trips` LIMIT 100"


@pytest.mark.parametrize("reply", ["CANNOT_ANSWER_CHEAPLY", "```\ncannot_answer_cheaply;\n```"])
def test_cost_rewrite_cannot_answer_is_blocked(reply):
    complete = ScriptedComplete(BIG_SQL, reply)
    client = _client(_job(LIMIT_BYTES * 2))
    answer = answer_question("Show me every trip.", client, "p", complete=complete)
    assert answer.blocked is True
    assert answer.sql == BIG_SQL
    assert answer.reason.startswith("Query would scan 1000 MB (limit 500 MB).")
    assert answer.bytes_scanned == LIMIT_BYTES * 2
    assert answer.data is None and answer.answer_text is None
    assert len(complete.calls) == 2
    client.query.assert_called_once()


@pytest.mark.parametrize(
    "rewrite", [STAR_SQL, STAR_SQL.replace("SELECT *", "SELECT t.*").replace("`p.marts.fct_trips`", "`p.marts.fct_trips` AS t")]
)
def test_cost_rewrite_to_select_star_on_fct_trips_is_blocked(rewrite):
    complete = ScriptedComplete(BIG_SQL, rewrite)
    client = _client(_job(LIMIT_BYTES * 2))
    answer = answer_question("Show me every trip.", client, "p", complete=complete)
    assert answer.blocked is True
    assert answer.sql == BIG_SQL
    assert answer.reason.startswith("Query would scan 1000 MB (limit 500 MB).")
    assert answer.attempts == [BIG_SQL, rewrite]
    assert len(complete.calls) == 2
    client.query.assert_called_once()


def test_guard_rewrite_cannot_answer_is_blocked_without_running():
    complete = ScriptedComplete(DUMP_SQL, "CANNOT_ANSWER_CHEAPLY")
    client = _client()
    answer = answer_question("Show me every trip.", client, "p", complete=complete)
    assert answer.blocked is True
    assert answer.sql == DUMP_SQL
    assert "must filter on `pickup_date`" in answer.reason
    client.query.assert_not_called()


def test_guard_rewrite_to_select_star_on_fct_trips_is_blocked():
    complete = ScriptedComplete(DUMP_SQL, STAR_SQL)
    client = _client()
    answer = answer_question("Show me every trip.", client, "p", complete=complete)
    assert answer.blocked is True
    assert answer.sql == DUMP_SQL
    client.query.assert_not_called()


def test_rewrite_prompt_forbids_narrowing_full_table_requests():
    from chatbot.prompts import build_rewrite_prompt

    _, user = build_rewrite_prompt("Show me every trip.", BIG_SQL, LIMIT_BYTES * 2, "p")
    assert (
        "If the question requires every row or the whole table, do not narrow it; "
        "reply with exactly CANNOT_ANSWER_CHEAPLY."
    ) in user
