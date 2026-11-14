"""Simulate late-arriving taxi data by re-inserting shifted copies of month-end trips."""

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "airflow" / "include"))

from nyc_mobility.config import Settings, month_end, month_start  # noqa: E402

PASSTHROUGH = [
    "VendorID", "passenger_count", "trip_distance", "RatecodeID",
    "store_and_fwd_flag", "PULocationID", "DOLocationID", "payment_type",
    "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "congestion_surcharge",
    "Airport_fee", "source_month",
]  # fmt: skip


def build_injection_sql(
    settings: Settings, month: date, n_rows: int = 500, shift_seconds: int = 37
) -> str:
    end = month_end(month)
    last3_start = end - timedelta(days=2)
    table = f"`{settings.project_id}.{settings.raw_tlc_dataset}.yellow_trips`"
    columns = [
        "VendorID",
        f"TIMESTAMP_ADD(tpep_pickup_datetime, INTERVAL {shift_seconds} SECOND) AS tpep_pickup_datetime",
        f"TIMESTAMP_ADD(tpep_dropoff_datetime, INTERVAL {shift_seconds} SECOND) AS tpep_dropoff_datetime",
        *PASSTHROUGH[1:],
        "CURRENT_TIMESTAMP() AS loaded_at",
    ]
    names = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", *PASSTHROUGH[1:], "loaded_at"]
    return (
        f"INSERT INTO {table} ({', '.join(names)})\n"
        f"SELECT {', '.join(columns)}\n"
        f"FROM {table}\n"
        f"WHERE source_month = @month AND DATE(tpep_pickup_datetime) "
        f"BETWEEN '{last3_start}' AND '{end}'\n"
        f"ORDER BY RAND() LIMIT {n_rows}"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", required=True, help="YYYY-MM")
    parser.add_argument("--rows", type=int, default=500)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    month = month_start(f"{args.month}-01")
    settings = Settings.from_env()
    sql = build_injection_sql(settings, month, n_rows=args.rows)
    if args.dry_run:
        print(sql)
        return 0

    from google.cloud import bigquery

    client = bigquery.Client(project=settings.project_id)
    config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("month", "DATE", month)]
    )
    job = client.query(sql, job_config=config)
    job.result()
    print(f"Inserted {job.num_dml_affected_rows} late-arriving rows into raw for {month:%Y-%m}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
