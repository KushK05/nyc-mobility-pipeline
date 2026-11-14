"""SQL safety checks and a dry-run cost guard for chatbot queries."""

import re
from dataclasses import dataclass

import pandas as pd
import sqlglot
from google.cloud import bigquery
from sqlglot import exp

LIMIT_BYTES = 500 * 1024**2
PRICE_PER_TIB_USD = 6.25

_FORBIDDEN = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Drop,
    exp.Merge,
    exp.Create,
    exp.Alter,
    exp.TruncateTable,
    exp.Command,
)
_FENCE = re.compile(r"^```[ \t]*(?:sql)?[ \t]*\n?(.*?)\n?```$", re.DOTALL | re.IGNORECASE)


class GuardError(ValueError):
    """Raised when generated SQL is not a safe, single read-only query on the marts."""


@dataclass
class GuardResult:
    blocked: bool
    bytes_scanned: int
    cost_usd: float
    reason: str | None
    data: pd.DataFrame | None


def clean_sql(text: str) -> str:
    """Strip markdown fences (optional `sql` tag), whitespace and one trailing `;`."""
    sql = text.strip()
    match = _FENCE.match(sql)
    if match:
        sql = match.group(1).strip()
    if sql.endswith(";"):
        sql = sql[:-1].rstrip()
    return sql


def _is_select_root(expression: exp.Expression) -> bool:
    if isinstance(expression, (exp.Select, exp.Union)):
        return True
    return isinstance(expression, exp.With) and isinstance(expression.this, exp.Select)


def validate_select(sql: str, allowed_dataset: str = "marts") -> None:
    """Raise GuardError unless `sql` is one read-only SELECT on `allowed_dataset` tables."""
    if re.search(r";\s*\S", clean_sql(sql)):
        raise GuardError("Only a single SQL statement is allowed.")
    try:
        expressions = [e for e in sqlglot.parse(sql, read="bigquery") if e is not None]
    except sqlglot.errors.ParseError as err:
        raise GuardError(f"Could not parse SQL: {err}") from err
    if len(expressions) != 1:
        raise GuardError("Only a single SQL statement is allowed.")
    root = expressions[0]
    if not _is_select_root(root):
        raise GuardError("Only SELECT queries are allowed.")

    nodes = list(root.walk())
    for node in nodes:
        if isinstance(node, _FORBIDDEN):
            raise GuardError(f"Statement type {type(node).__name__.upper()} is not allowed.")

    cte_names = {cte.alias_or_name for cte in root.find_all(exp.CTE)}
    tables = [node for node in nodes if isinstance(node, exp.Table)]
    for table in tables:
        if table.db == allowed_dataset:
            continue
        if not table.db and table.name in cte_names:
            continue
        name = ".".join(part.name for part in table.parts)
        raise GuardError(f"Table `{name}` is outside the `{allowed_dataset}` dataset.")

    if any(table.name == "fct_trips" for table in tables):
        has_filter = any(
            column.name == "pickup_date"
            for where in root.find_all(exp.Where)
            for column in where.find_all(exp.Column)
        )
        if not has_filter:
            raise GuardError("Queries on `fct_trips` must filter on `pickup_date` in a WHERE clause.")


def estimate_cost_usd(bytes_: int) -> float:
    return bytes_ / 1024**4 * PRICE_PER_TIB_USD


def dry_run(client: bigquery.Client, sql: str) -> int:
    """Return the bytes BigQuery would scan for `sql`, without running it."""
    config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    return client.query(sql, job_config=config).total_bytes_processed or 0


def blocked_reason(bytes_scanned: int, limit_bytes: int = LIMIT_BYTES) -> str:
    mb = bytes_scanned / 1024**2
    limit_mb = limit_bytes / 1024**2
    return (
        f"Query would scan {mb:.0f} MB (limit {limit_mb:.0f} MB). "
        "Please narrow the date range or ask for an aggregate."
    )


def guarded_run(
    client: bigquery.Client, sql: str, limit_bytes: int = LIMIT_BYTES
) -> GuardResult:
    """Dry-run `sql`; block it when it would scan more than `limit_bytes`, else execute it."""
    scanned = dry_run(client, sql)
    cost = estimate_cost_usd(scanned)
    if scanned > limit_bytes:
        return GuardResult(True, scanned, cost, blocked_reason(scanned, limit_bytes), None)
    config = bigquery.QueryJobConfig(maximum_bytes_billed=limit_bytes)
    df = client.query(sql, job_config=config).to_dataframe()
    return GuardResult(False, scanned, cost, None, df)
