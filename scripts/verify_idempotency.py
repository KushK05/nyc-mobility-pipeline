"""Show per-day trip counts and duplicate trip_ids in fct_trips for one month."""

import argparse
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "airflow" / "include"))

from nyc_mobility.config import Settings, month_end, month_start  # noqa: E402


def count_sql(settings: Settings, month: date) -> str:
    fact = f"`{settings.project_id}.{settings.marts_dataset}.fct_trips`"
    window = f"where pickup_date between '{month}' and '{month_end(month)}'"
    return (
        "with daily as (\n"
        f"  select pickup_date, count(*) as trips from {fact} {window} group by pickup_date\n"
        "),\n"
        "dupes as (\n"
        "  select count(*) - count(distinct trip_id) as duplicate_trip_ids\n"
        f"  from {fact} {window}\n"
        ")\n"
        "select pickup_date, trips, duplicate_trip_ids from daily cross join dupes\n"
        "order by pickup_date"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", required=True, help="YYYY-MM")
    args = parser.parse_args(argv)

    month = month_start(f"{args.month}-01")
    settings = Settings.from_env()

    from google.cloud import bigquery

    client = bigquery.Client(project=settings.project_id)
    rows = list(client.query(count_sql(settings, month)).result())
    print(f"{'pickup_date':<12} {'trips':>10}")
    for row in rows:
        print(f"{row['pickup_date']!s:<12} {row['trips']:>10}")
    duplicates = rows[0]["duplicate_trip_ids"] if rows else 0
    print(f"\nduplicate trip_ids: {duplicates}")
    return 1 if duplicates > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
