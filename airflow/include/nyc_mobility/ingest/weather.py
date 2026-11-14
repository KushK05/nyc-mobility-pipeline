"""Idempotent monthly loader for Open-Meteo hourly NYC weather."""

from datetime import date, datetime, timezone

import requests
from google.cloud import bigquery

from nyc_mobility.config import Settings, month_end
from nyc_mobility.ingest.taxi import delete_month_rows

OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"
NYC_LAT = 40.71
NYC_LON = -74.01
HOURLY_VARS = ["temperature_2m", "precipitation", "rain", "snowfall", "wind_speed_10m"]
TIMEZONE = "America/New_York"

SCHEMA = [
    bigquery.SchemaField("observed_at", "DATETIME"),
    *[bigquery.SchemaField(name, "FLOAT64") for name in HOURLY_VARS],
    bigquery.SchemaField("source_month", "DATE"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP"),
]
RAW_COLUMNS = tuple(field.name for field in SCHEMA)


def open_meteo_params(month: date) -> dict[str, str]:
    return {
        "start_date": month.isoformat(),
        "end_date": month_end(month).isoformat(),
        "latitude": str(NYC_LAT),
        "longitude": str(NYC_LON),
        "hourly": ",".join(HOURLY_VARS),
        "timezone": TIMEZONE,
    }


def fetch_month(month: date, session: requests.Session | None = None) -> dict:
    http = session or requests
    response = http.get(OPEN_METEO_URL, params=open_meteo_params(month), timeout=60)
    response.raise_for_status()
    return response.json()


def flatten_hourly(payload: dict, month: date) -> list[dict]:
    hourly = payload.get("hourly") or {}
    times = hourly.get("time") or []
    rows = []
    for i, stamp in enumerate(times):
        row = {"observed_at": datetime.fromisoformat(stamp)}
        for name in HOURLY_VARS:
            values = hourly.get(name) or []
            row[name] = values[i] if i < len(values) else None
        row["source_month"] = month
        rows.append(row)
    return rows


def _to_json_row(row: dict, loaded_at: str) -> dict:
    return {
        **row,
        "observed_at": row["observed_at"].strftime("%Y-%m-%dT%H:%M:%S"),
        "source_month": row["source_month"].isoformat(),
        "loaded_at": loaded_at,
    }


def load_month(client: bigquery.Client, settings: Settings, month: date) -> int:
    table_id = f"{settings.project_id}.{settings.raw_weather_dataset}.weather_hourly"
    rows = flatten_hourly(fetch_month(month), month)
    loaded_at = datetime.now(timezone.utc).isoformat()
    json_rows = [_to_json_row(row, loaded_at) for row in rows]

    delete_month_rows(client, table_id, month)
    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    client.load_table_from_json(json_rows, table_id, job_config=job_config).result()
    return len(json_rows)
