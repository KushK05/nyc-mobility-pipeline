import csv
import re
import sys
from pathlib import Path

import pandas as pd
import pytest
from google.api_core.exceptions import BadRequest

from chatbot.guard import LIMIT_BYTES, GuardError, validate_select

EVAL_DIR = Path(__file__).resolve().parents[1] / "eval"
sys.path.insert(0, str(EVAL_DIR))

import partition_cost_comparison as pcc  # noqa: E402
import run_eval  # noqa: E402

QUESTIONS_CSV = EVAL_DIR / "questions.csv"
EXPECTED_COUNTS = {
    "simple_aggregate": 6,
    "join_weather": 5,
    "join_calendar": 4,
    "time_location": 5,
    "should_block": 5,
}


# questions.csv


def test_questions_csv_has_25_rows_and_categories():
    with QUESTIONS_CSV.open(newline="") as f:
        reader = csv.DictReader(f)
        assert reader.fieldnames == ["id", "category", "question", "gold_sql", "expect_blocked"]
        raw = list(reader)
    assert len(raw) == 25
    assert {r["expect_blocked"] for r in raw} <= {"true", "false"}

    questions = run_eval.load_questions(QUESTIONS_CSV)
    counts: dict[str, int] = {}
    for q in questions:
        counts[q["category"]] = counts.get(q["category"], 0) + 1
    assert counts == EXPECTED_COUNTS
    assert len({q["id"] for q in questions}) == 25
    for q in questions:
        assert isinstance(q["expect_blocked"], bool)
        assert q["expect_blocked"] == (q["category"] == "should_block")
        assert bool(q["gold_sql"]) != q["expect_blocked"]
        assert "\u2014" not in q["question"] + q["gold_sql"]


def test_gold_sql_passes_guard_with_project():
    for q in run_eval.load_questions(QUESTIONS_CSV):
        if q["expect_blocked"]:
            continue
        assert "{project}" in q["gold_sql"], q["id"]
        sql = q["gold_sql"].replace("{project}", "dummy-proj")
        validate_select(sql, allowed_project="dummy-proj")


def test_gold_sql_multi_row_queries_are_ordered():
    for q in run_eval.load_questions(QUESTIONS_CSV):
        sql = q["gold_sql"].lower()
        if "group by" in sql:
            assert "order by" in sql, q["id"]


# compare_results


def test_compare_results_ignores_order_and_rounding():
    a = pd.DataFrame({"Borough": ["Queens", "Manhattan"], "trips": [10, 20], "fare": [1.234, 5.0]})
    b = pd.DataFrame(
        {"fare": [5.001, 1.2341], "borough": ["Manhattan", "Queens"], "TRIPS": [20.0, 10.0]}
    )
    assert run_eval.compare_results(a, b) is True


def test_compare_results_nan_equals_nan_and_detects_differences():
    a = pd.DataFrame({"x": [1.0, float("nan")], "y": ["a", "b"]})
    b = pd.DataFrame({"x": [float("nan"), 1.0], "y": ["b", "a"]})
    assert run_eval.compare_results(a, b) is True
    assert run_eval.compare_results(a, b.assign(x=[float("nan"), 2.0])) is False
    assert run_eval.compare_results(a, b.rename(columns={"y": "z"})) is False
    assert run_eval.compare_results(a, b.head(1)) is False
    assert run_eval.compare_results(a, None) is False
    assert run_eval.compare_results(None, a) is False


# run


class FakeJob:
    def __init__(self, bytes_processed=0, df=None):
        self.total_bytes_processed = bytes_processed
        self._df = df

    def to_dataframe(self):
        return self._df


class FakeClient:
    """Dry runs return `bytes_by_sql`; real runs return `df_by_sql`."""

    def __init__(self, bytes_by_sql, df_by_sql):
        self.bytes_by_sql = bytes_by_sql
        self.df_by_sql = df_by_sql
        self.queries = []

    def query(self, sql, job_config=None):
        dry = bool(job_config is not None and job_config.dry_run)
        self.queries.append((sql, dry))
        if dry:
            return FakeJob(bytes_processed=self.bytes_by_sql[sql])
        return FakeJob(df=self.df_by_sql[sql])


class SqlByQuestion:
    """Fake LLM: SQL keyed by question (a list is replayed in order, last one repeats),
    and a fixed reply for answer prompts."""

    def __init__(self, sql_by_question):
        self.sql_by_question = {
            q: list(v) if isinstance(v, list) else [v] for q, v in sql_by_question.items()
        }

    def __call__(self, system, user):
        if "Result preview" in user:
            return "Answer."
        question = user.split("\n", 1)[0].removeprefix("Question: ")
        replies = self.sql_by_question[question]
        return replies.pop(0) if len(replies) > 1 else replies[0]


GOOD = "select sum(trips) as trips from `p.marts.agg_hourly_zone_demand`"
WRONG = "select sum(trips) as trips from `p.marts.agg_daily_weather_impact`"
HUGE = "select * from `p.marts.fct_trips` where pickup_date >= '2024-01-01'"
RAW = "select sum(trips) as trips from `p.raw_tlc.yellow_trips`"
GOLD_GOOD = "select sum(trips) as trips from `{project}.marts.agg_hourly_zone_demand`"
GOLD_OTHER = "select 2 as trips from `{project}.marts.dim_zone`"


def test_run_counts_blocked_as_correct_when_expected():
    questions = [
        {"id": "q1", "category": "simple_aggregate", "question": "Q good",
         "gold_sql": GOLD_GOOD, "expect_blocked": False},
        {"id": "q2", "category": "simple_aggregate", "question": "Q wrong",
         "gold_sql": GOLD_OTHER, "expect_blocked": False},
        {"id": "q3", "category": "should_block", "question": "Q dump",
         "gold_sql": "", "expect_blocked": True},
        {"id": "q4", "category": "should_block", "question": "Q leak",
         "gold_sql": "", "expect_blocked": True},
    ]
    complete = SqlByQuestion(
        {"Q good": [RAW, GOOD], "Q wrong": WRONG, "Q dump": HUGE, "Q leak": GOOD}
    )
    big = LIMIT_BYTES + 1
    client = FakeClient(
        bytes_by_sql={GOOD: 1000, WRONG: 3000, HUGE: big},
        df_by_sql={
            GOOD: pd.DataFrame({"trips": [5]}),
            WRONG: pd.DataFrame({"trips": [7]}),
            GOLD_GOOD.replace("{project}", "p"): pd.DataFrame({"trips": [5]}),
            GOLD_OTHER.replace("{project}", "p"): pd.DataFrame({"trips": [2]}),
        },
    )

    report = run_eval.run(questions, client, "p", complete)

    assert isinstance(report, run_eval.EvalReport)
    by_id = {row["id"]: row for row in report.rows}
    assert [row["id"] for row in report.rows] == ["q1", "q2", "q3", "q4"]
    assert by_id["q1"]["correct"] is True
    assert by_id["q2"]["correct"] is False
    assert by_id["q3"]["correct"] is True and by_id["q3"]["blocked"] is True
    assert by_id["q4"]["correct"] is False and by_id["q4"]["blocked"] is False
    assert report.execution_accuracy == pytest.approx(0.5)
    assert report.blocked == 1
    assert report.rerouted == 1  # "Q good" is rewritten from RAW to GOOD
    assert by_id["q1"]["rerouted"] is True
    assert report.avg_bytes_scanned == pytest.approx((1000 + 3000 + 1000) / 3)
    for key in ("id", "category", "correct", "blocked", "rerouted", "bytes_scanned",
                "cost_usd", "sql"):
        assert key in by_id["q1"]
    assert by_id["q1"]["sql"] == GOOD
    # Gold SQL runs with the project substituted.
    assert (GOLD_GOOD.replace("{project}", "p"), False) in client.queries


def test_run_with_no_answers_has_zero_avg_bytes():
    report = run_eval.run([], FakeClient({}, {}), "p", SqlByQuestion({}))
    assert report.execution_accuracy == 0.0
    assert report.avg_bytes_scanned == 0.0
    assert report.rows == []


def test_write_report_writes_csv_and_markdown(tmp_path):
    report = run_eval.EvalReport(
        execution_accuracy=0.5,
        avg_bytes_scanned=2048.0,
        blocked=1,
        rerouted=0,
        rows=[
            {"id": "q1", "category": "simple_aggregate", "correct": True, "blocked": False,
             "rerouted": False, "bytes_scanned": 2048, "cost_usd": 0.0001,
             "sql": "select 1\nfrom x | y"},
            {"id": "q2", "category": "should_block", "correct": False, "blocked": True,
             "rerouted": False, "bytes_scanned": 0, "cost_usd": 0.0, "sql": ""},
        ],
    )
    csv_path, md_path = tmp_path / "results.csv", tmp_path / "results.md"
    run_eval.write_report(report, csv_path, md_path)

    with csv_path.open(newline="") as f:
        rows = list(csv.DictReader(f))
    assert [r["id"] for r in rows] == ["q1", "q2"]
    assert rows[0]["sql"] == "select 1\nfrom x | y"

    md = md_path.read_text()
    assert "50.0%" in md
    assert "| q1 | simple_aggregate |" in md
    assert "| q2 | should_block |" in md
    # Table cells stay on one line and escape pipes.
    assert "select 1 from x \\| y" in md
    assert "\u2014" not in md


# partition_cost_comparison


def test_comparison_queries_shape():
    assert len(pcc.COMPARISON_QUERIES) == 5
    for name, filtered, unfiltered in pcc.COMPARISON_QUERIES:
        assert name
        assert "{project}.marts.fct_trips" in filtered
        assert "{project}.marts.fct_trips" in unfiltered
        assert "pickup_date" in filtered.split("where", 1)[1]
        assert not re.search(r"\bpickup_date\b", unfiltered)
        validate_select(filtered.replace("{project}", "dummy"), allowed_project="dummy")
        with pytest.raises(GuardError, match="pickup_date"):
            validate_select(unfiltered.replace("{project}", "dummy"), allowed_project="dummy")


class FakeDryRunClient:
    def __init__(self, fail_sql):
        self.fail_sql = fail_sql
        self.configs = []

    def query(self, sql, job_config=None):
        self.configs.append(job_config)
        if sql == self.fail_sql:
            raise BadRequest("Cannot query over table without a filter on pickup_date")
        return FakeJob(bytes_processed=len(sql) * 1024**3)


def test_compare_dry_runs_and_records_errors():
    _, _, first_unfiltered = pcc.COMPARISON_QUERIES[0]
    fail_sql = first_unfiltered.replace("{project}", "p")
    client = FakeDryRunClient(fail_sql)

    rows = pcc.compare(client, "p")

    assert len(rows) == 5
    assert all(c.dry_run and c.use_query_cache is False for c in client.configs)
    assert len(client.configs) == 10
    first = rows[0]
    assert first["name"] == pcc.COMPARISON_QUERIES[0][0]
    assert first["unfiltered_bytes"] is None
    assert "without a filter" in first["unfiltered_error"]
    assert first["filtered_bytes"] > 0 and first["filtered_error"] is None
    second = rows[1]
    assert second["unfiltered_error"] is None
    assert second["unfiltered_cost_usd"] == pytest.approx(
        second["unfiltered_bytes"] / 1024**4 * 6.25
    )


def test_format_markdown_renders_bytes_cost_and_errors():
    rows = [
        {"name": "one day", "filtered_bytes": 1024**3, "filtered_cost_usd": 6.25 / 1024,
         "filtered_error": None, "unfiltered_bytes": None, "unfiltered_cost_usd": None,
         "unfiltered_error": "400 Cannot query | without filter"},
        {"name": "month", "filtered_bytes": 2 * 1024**2, "filtered_cost_usd": 0.0,
         "filtered_error": None, "unfiltered_bytes": 1024**4, "unfiltered_cost_usd": 6.25,
         "unfiltered_error": None},
    ]
    md = pcc.format_markdown(rows)
    lines = md.strip().splitlines()
    assert lines[0].startswith("| Query |")
    assert len(lines) == 4
    assert "one day" in lines[2] and "1.00 GiB" in lines[2]
    assert "error: 400 Cannot query \\| without filter" in lines[2]
    assert "1.00 TiB" in lines[3] and "$6.2500" in lines[3]
