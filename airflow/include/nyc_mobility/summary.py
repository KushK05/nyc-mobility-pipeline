"""SQL builders and metadata shaping for post-run reporting (no Airflow imports)."""

from datetime import date, datetime

from nyc_mobility.config import Settings


def _table(settings: Settings, dataset: str, table: str) -> str:
    return f"`{settings.project_id}.{dataset}.{table}`"


def months_loaded_sql(settings: Settings) -> str:
    raw = _table(settings, settings.raw_tlc_dataset, "yellow_trips")
    return f"select distinct source_month from {raw} order by 1"


def quarantine_share_sql(settings: Settings) -> str:
    """Share of the run month's trips that were quarantined. Params: @start, @end."""
    quarantine = _table(settings, settings.quarantine_dataset, "quarantine_trips")
    fact = _table(settings, settings.marts_dataset, "fct_trips")
    window = "where pickup_date between @start and @end"
    return (
        "select safe_divide(\n"
        f"  (select count(*) from {quarantine} {window}),\n"
        f"  (select count(*) from {fact} {window})\n"
        f"    + (select count(*) from {quarantine} {window})\n"
        ") as quarantine_share"
    )


def run_summary_sql(settings: Settings) -> str:
    """Raw, fact and quarantine row counts for the run month. Params: @start, @end."""
    raw = _table(settings, settings.raw_tlc_dataset, "yellow_trips")
    fact = _table(settings, settings.marts_dataset, "fct_trips")
    quarantine = _table(settings, settings.quarantine_dataset, "quarantine_trips")
    window = "where pickup_date between @start and @end"
    return (
        "select\n"
        f"  (select count(*) from {raw} where source_month = @start) as raw_rows,\n"
        f"  (select count(*) from {fact} {window}) as fact_rows,\n"
        f"  (select count(*) from {quarantine} {window}) as quarantine_rows"
    )


def build_metadata(
    months_loaded: list[date], quarantine_share: float | None, now: datetime
) -> dict:
    """Shape the chatbot sidebar metadata. quarantine_pct is a percentage (0-100)."""
    return {
        "months_loaded": [m.isoformat() for m in months_loaded],
        "last_run_at": now.isoformat(),
        "quarantine_pct": (
            None if quarantine_share is None else round(quarantine_share * 100, 2)
        ),
    }
