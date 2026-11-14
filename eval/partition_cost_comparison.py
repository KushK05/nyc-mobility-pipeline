"""Dry-run fct_trips queries with and without a pickup_date filter to show partition pruning.

Dry runs are free. If BigQuery rejects an unfiltered query because the table requires a
partition filter, the error text is reported for that side instead of bytes.

Usage: GCP_PROJECT_ID=... python eval/partition_cost_comparison.py
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from google.api_core.exceptions import GoogleAPICallError  # noqa: E402
from google.cloud import bigquery  # noqa: E402

from chatbot.guard import estimate_cost_usd  # noqa: E402

T = "`{project}.marts.fct_trips`"

COMPARISON_QUERIES: list[tuple[str, str, str]] = [
    (
        "Trips on one day",
        f"select count(*) as trips from {T} where pickup_date = '2024-03-15'",
        f"select count(*) as trips from {T}",
    ),
    (
        "Average fare in January",
        f"select avg(fare_amount) as avg_fare from {T} "
        "where pickup_date between '2024-01-01' and '2024-01-31'",
        f"select avg(fare_amount) as avg_fare from {T} "
        "where extract(month from pickup_datetime) = 1",
    ),
    (
        "Rainy trip duration in one week",
        f"select is_rainy, avg(trip_duration_min) as avg_duration_min from {T} "
        "where pickup_date between '2024-04-01' and '2024-04-07' group by is_rainy",
        f"select is_rainy, avg(trip_duration_min) as avg_duration_min from {T} "
        "group by is_rainy",
    ),
    (
        "Airport fees in February",
        f"select sum(airport_fee) as airport_fees from {T} "
        "where pickup_date between '2024-02-01' and '2024-02-29' and airport_fee > 0",
        f"select sum(airport_fee) as airport_fees from {T} where airport_fee > 0",
    ),
    (
        "Trips per hour on one Saturday",
        f"select pickup_hour, count(*) as trips from {T} "
        "where pickup_date = '2024-06-01' group by pickup_hour order by pickup_hour",
        f"select pickup_hour, count(*) as trips from {T} "
        "group by pickup_hour order by pickup_hour",
    ),
]


def _dry_run(client: bigquery.Client, sql: str) -> tuple[int | None, float | None, str | None]:
    config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    try:
        scanned = client.query(sql, job_config=config).total_bytes_processed or 0
    except GoogleAPICallError as exc:
        return None, None, str(exc)
    return scanned, estimate_cost_usd(scanned), None


def compare(client: bigquery.Client, project_id: str) -> list[dict]:
    rows = []
    for name, filtered, unfiltered in COMPARISON_QUERIES:
        row: dict = {"name": name}
        for side, sql in (("filtered", filtered), ("unfiltered", unfiltered)):
            scanned, cost, error = _dry_run(client, sql.replace("{project}", project_id))
            row[f"{side}_bytes"] = scanned
            row[f"{side}_cost_usd"] = cost
            row[f"{side}_error"] = error
        rows.append(row)
    return rows


def _format_bytes(n: int) -> str:
    value = float(n)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if value < 1024:
            return f"{value:.2f} {unit}"
        value /= 1024
    return f"{value:.2f} TiB"


def _side(row: dict, side: str) -> tuple[str, str]:
    error = row.get(f"{side}_error")
    if error:
        text = " ".join(str(error).split()).replace("|", "\\|")
        return f"error: {text}", ""
    return _format_bytes(row[f"{side}_bytes"]), f"${row[f'{side}_cost_usd']:.4f}"


def format_markdown(rows: list[dict]) -> str:
    lines = [
        "| Query | Filtered bytes | Filtered cost | Unfiltered bytes | Unfiltered cost |",
        "| --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        f_bytes, f_cost = _side(row, "filtered")
        u_bytes, u_cost = _side(row, "unfiltered")
        lines.append(f"| {row['name']} | {f_bytes} | {f_cost} | {u_bytes} | {u_cost} |")
    return "\n".join(lines) + "\n"


def main() -> None:
    project_id = os.environ["GCP_PROJECT_ID"]
    client = bigquery.Client(project=project_id, location=os.environ.get("BQ_LOCATION", "US"))
    print(format_markdown(compare(client, project_id)))


if __name__ == "__main__":
    main()
