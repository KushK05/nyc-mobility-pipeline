"""Environment-driven settings and month helpers shared across the pipeline."""

import calendar
import os
from dataclasses import dataclass
from datetime import date, timedelta


@dataclass(frozen=True)
class Settings:
    project_id: str
    location: str
    raw_tlc_dataset: str = "raw_tlc"
    raw_weather_dataset: str = "raw_weather"
    staging_dataset: str = "staging"
    marts_dataset: str = "marts"
    quarantine_dataset: str = "quarantine"

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            project_id=os.environ["GCP_PROJECT_ID"],
            location=os.environ.get("BQ_LOCATION", "US"),
        )


def month_start(value: str | date) -> date:
    d = date.fromisoformat(value) if isinstance(value, str) else value
    return d.replace(day=1)


def month_end(start: date) -> date:
    return start.replace(day=calendar.monthrange(start.year, start.month)[1])


def month_range(first: date, last: date) -> list[date]:
    months = []
    current = month_start(first)
    stop = month_start(last)
    while current <= stop:
        months.append(current)
        current = month_end(current) + timedelta(days=1)
    return months
