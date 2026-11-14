from datetime import date, datetime
from unittest.mock import MagicMock

import responses
from google.cloud import bigquery

from nyc_mobility.config import Settings
from nyc_mobility.ingest import weather
from nyc_mobility.ingest.weather import (
    OPEN_METEO_URL,
    flatten_hourly,
    load_month,
    open_meteo_params,
)


def _payload():
    return {
        "hourly": {
            "time": ["2024-01-01T00:00", "2024-01-01T01:00"],
            "temperature_2m": [1.5, None],
            "precipitation": [0.0, 0.2],
            "rain": [0.0, 0.2],
            "snowfall": [0.0, 0.0],
            "wind_speed_10m": [10.0, 12.5],
        }
    }


def test_open_meteo_params_for_february():
    params = open_meteo_params(date(2024, 2, 1))
    assert params["start_date"] == "2024-02-01"
    assert params["end_date"] == "2024-02-29"
    assert params["timezone"] == "America/New_York"
    assert params["latitude"] == "40.71"
    assert params["longitude"] == "-74.01"
    assert params["hourly"] == "temperature_2m,precipitation,rain,snowfall,wind_speed_10m"


def test_flatten_hourly_zips_arrays():
    rows = flatten_hourly(_payload(), date(2024, 1, 1))
    assert len(rows) == 2
    assert rows[1]["observed_at"] == datetime(2024, 1, 1, 1, 0)
    assert rows[1]["rain"] == 0.2
    assert rows[1]["temperature_2m"] is None
    assert rows[0]["source_month"] == date(2024, 1, 1)


def test_flatten_hourly_empty_payload():
    assert flatten_hourly({"hourly": {}}, date(2024, 1, 1)) == []
    assert flatten_hourly({}, date(2024, 1, 1)) == []


@responses.activate
def test_load_month_deletes_then_loads():
    responses.add(responses.GET, OPEN_METEO_URL, json=_payload(), status=200)
    client = MagicMock(spec=bigquery.Client)
    manager = MagicMock()
    manager.attach_mock(client.query, "query")
    manager.attach_mock(client.load_table_from_json, "load")

    settings = Settings(project_id="proj", location="US")
    count = load_month(client, settings, date(2024, 1, 1))

    assert count == 2
    assert [c[0] for c in manager.mock_calls if c[0] in ("query", "load")] == [
        "query",
        "load",
    ]
    assert "DELETE FROM `proj.raw_weather.weather_hourly`" in client.query.call_args[0][0]
    rows, table_id = client.load_table_from_json.call_args[0]
    assert table_id == "proj.raw_weather.weather_hourly"
    assert len(rows) == 2
    assert rows[0]["observed_at"] == "2024-01-01T00:00:00"
    assert rows[0]["source_month"] == "2024-01-01"
    cfg = client.load_table_from_json.call_args.kwargs["job_config"]
    assert cfg.write_disposition == bigquery.WriteDisposition.WRITE_APPEND
    assert [f.name for f in cfg.schema] == list(weather.RAW_COLUMNS)
    client.load_table_from_json.return_value.result.assert_called_once()
