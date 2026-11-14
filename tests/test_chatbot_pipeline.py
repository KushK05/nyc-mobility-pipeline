from unittest.mock import MagicMock

import pandas as pd
import pytest
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
