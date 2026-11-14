import sys
from datetime import date
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import inject_late_trips  # noqa: E402
import verify_idempotency  # noqa: E402
from nyc_mobility.config import Settings  # noqa: E402

SETTINGS = Settings(project_id="proj", location="US")


def test_injection_sql_targets_last_three_days():
    sql = inject_late_trips.build_injection_sql(SETTINGS, date(2024, 1, 1))
    assert "DATE(tpep_pickup_datetime) BETWEEN '2024-01-29' AND '2024-01-31'" in sql
    assert "LIMIT 500" in sql
    assert "TIMESTAMP_ADD" in sql
    assert "37 SECOND" in sql
    assert "SELECT *" not in sql
    assert "CURRENT_TIMESTAMP()" in sql


def test_injection_dry_run_prints_sql_without_running(capsys, monkeypatch):
    monkeypatch.setenv("GCP_PROJECT_ID", "proj")
    with mock.patch("google.cloud.bigquery.Client") as client:
        rc = inject_late_trips.main(["--month", "2024-01", "--dry-run"])
    client.assert_not_called()
    assert rc in (0, None)
    assert "INSERT INTO" in capsys.readouterr().out


def test_count_sql_has_partition_filter():
    sql = verify_idempotency.count_sql(SETTINGS, date(2024, 1, 1))
    assert "where pickup_date between '2024-01-01' and '2024-01-31'" in sql
    assert "count(distinct trip_id)" in sql
    assert "proj.marts.fct_trips" in sql
