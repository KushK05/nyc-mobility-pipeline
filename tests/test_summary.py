from datetime import date, datetime, timezone

import pytest

from nyc_mobility.config import Settings
from nyc_mobility.summary import (
    build_metadata,
    months_loaded_sql,
    quarantine_share_sql,
    run_summary_sql,
)

SETTINGS = Settings(project_id="proj", location="US")


def test_months_loaded_sql():
    sql = months_loaded_sql(SETTINGS)
    assert "select distinct source_month" in sql
    assert "`proj.raw_tlc.yellow_trips`" in sql
    assert sql.rstrip().endswith("order by 1")


def test_quarantine_share_sql_uses_params_and_both_tables():
    sql = quarantine_share_sql(SETTINGS)
    assert "safe_divide(" in sql
    assert "`proj.quarantine.quarantine_trips`" in sql
    assert "`proj.marts.fct_trips`" in sql
    assert sql.count("pickup_date between @start and @end") == 3
    assert "as quarantine_share" in sql


def test_run_summary_sql_counts_raw_fact_and_quarantine():
    sql = run_summary_sql(SETTINGS)
    assert "`proj.raw_tlc.yellow_trips` where source_month = @start" in sql
    assert "`proj.marts.fct_trips` where pickup_date between @start and @end" in sql
    for alias in ("raw_rows", "fact_rows", "quarantine_rows"):
        assert f"as {alias}" in sql


def test_build_metadata():
    now = datetime(2024, 7, 1, 12, tzinfo=timezone.utc)
    meta = build_metadata([date(2024, 1, 1), date(2024, 2, 1)], 0.01234, now)
    assert meta == {
        "months_loaded": ["2024-01-01", "2024-02-01"],
        "last_run_at": "2024-07-01T12:00:00+00:00",
        "quarantine_pct": pytest.approx(1.23),
    }


def test_build_metadata_handles_missing_share():
    meta = build_metadata([], None, datetime(2024, 7, 1, tzinfo=timezone.utc))
    assert meta["quarantine_pct"] is None
    assert meta["months_loaded"] == []
