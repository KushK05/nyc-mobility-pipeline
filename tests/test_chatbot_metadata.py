import json

import pandas as pd

from chatbot.charts import choose_chart
from chatbot.metadata import format_bytes, load_metadata

DEFAULTS = {"months_loaded": [], "last_run_at": None, "quarantine_pct": None}


def test_load_metadata_missing_file_returns_defaults(tmp_path):
    assert load_metadata(tmp_path / "nope.json") == DEFAULTS


def test_load_metadata_corrupt_file_returns_defaults(tmp_path):
    p = tmp_path / "m.json"
    p.write_text("{not json")
    assert load_metadata(p) == DEFAULTS


def test_load_metadata_reads_and_merges(tmp_path):
    p = tmp_path / "m.json"
    p.write_text(json.dumps({"months_loaded": ["2024-01-01"], "quarantine_pct": 1.5}))
    assert load_metadata(p) == {
        "months_loaded": ["2024-01-01"],
        "last_run_at": None,
        "quarantine_pct": 1.5,
    }


def test_format_bytes_magnitudes():
    assert format_bytes(512) == "0.5 KB"
    assert format_bytes(42 * 1024**2) == "42.0 MB"
    assert format_bytes(int(1.2 * 1024**3)) == "1.2 GB"


def test_choose_chart_bar():
    df = pd.DataFrame({"borough": ["a", "b"], "trips": [1, 2]})
    assert choose_chart(df) == "bar"


def test_choose_chart_line():
    df = pd.DataFrame({"pickup_date": pd.to_datetime(["2024-01-01", "2024-01-02"]), "a": [1, 2], "b": ["x", "y"]})
    assert choose_chart(df) == "line"


def test_choose_chart_none():
    assert choose_chart(pd.DataFrame({"a": ["x"], "b": ["y"]})) is None
    assert choose_chart(pd.DataFrame()) is None
    big = pd.DataFrame({"k": [str(i) for i in range(60)], "v": range(60)})
    assert choose_chart(big) is None
