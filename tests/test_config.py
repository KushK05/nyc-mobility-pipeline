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
