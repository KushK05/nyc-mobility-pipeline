"""Question -> guarded SQL -> BigQuery -> plain-English answer."""

from collections.abc import Callable
from dataclasses import dataclass, field

import pandas as pd
from google.cloud import bigquery

from chatbot import llm
from chatbot.guard import (
    LIMIT_BYTES,
    GuardError,
    GuardResult,
    clean_sql,
    guarded_run,
    validate_select,
)
from chatbot.prompts import build_answer_prompt, build_rewrite_prompt, build_sql_prompt

Complete = Callable[[str, str], str]


@dataclass
class ChatAnswer:
    question: str
    sql: str
    bytes_scanned: int
    cost_usd: float
    blocked: bool
    rerouted: bool
    reason: str | None
    data: pd.DataFrame | None
    answer_text: str | None
    attempts: list[str] = field(default_factory=list)


def _rewrite(
    complete: Complete,
    question: str,
    sql: str,
    bytes_scanned: int | None,
    project_id: str,
    extra: str = "",
) -> str:
    system, user = build_rewrite_prompt(question, sql, bytes_scanned, project_id)
    return clean_sql(complete(system, user + extra))


def _checked_run(
    client: bigquery.Client, sql: str, project_id: str, limit_bytes: int
) -> GuardResult:
    """Validate then dry-run/execute `sql`. Raises GuardError on any rejection."""
    validate_select(sql, allowed_project=project_id)
    return guarded_run(client, sql, limit_bytes)


def _blocked(question, sql, attempts, reason, result: GuardResult | None = None) -> ChatAnswer:
    return ChatAnswer(
        question=question,
        sql=sql,
        bytes_scanned=result.bytes_scanned if result else 0,
        cost_usd=result.cost_usd if result else 0.0,
        blocked=True,
        rerouted=False,
        reason=reason,
        data=None,
        answer_text=None,
        attempts=attempts,
    )


def answer_question(
    question: str,
    client: bigquery.Client,
    project_id: str,
    complete: Complete = llm.complete,
    limit_bytes: int = LIMIT_BYTES,
) -> ChatAnswer:
    attempts: list[str] = []
    sql = clean_sql(complete(*build_sql_prompt(question, project_id)))
    attempts.append(sql)
    try:
        result = _checked_run(client, sql, project_id, limit_bytes)
    except GuardError as err:
        error = f"\n\nThe previous query was rejected: {err}"
        sql = _rewrite(complete, question, sql, None, project_id, error)
        attempts.append(sql)
        try:
            result = _checked_run(client, sql, project_id, limit_bytes)
        except GuardError as err2:
            return _blocked(question, sql, attempts, str(err2))

    if result.blocked:
        rewritten = _rewrite(complete, question, sql, result.bytes_scanned, project_id)
        attempts.append(rewritten)
        try:
            retry = _checked_run(client, rewritten, project_id, limit_bytes)
        except GuardError as err:
            # The rewrite is unusable; report the original query and its scan size.
            reason = f"{result.reason} The cheaper rewrite was rejected: {err}"
            return _blocked(question, sql, attempts, reason, result)
        sql, result = rewritten, retry
        if result.blocked:
            return _blocked(question, sql, attempts, result.reason, result)

    preview = result.data.head(20).to_csv(index=False)
    answer_text = complete(*build_answer_prompt(question, sql, preview))
    return ChatAnswer(
        question=question,
        sql=sql,
        bytes_scanned=result.bytes_scanned,
        cost_usd=result.cost_usd,
        blocked=False,
        rerouted=len(attempts) > 1,
        reason=None,
        data=result.data,
        answer_text=answer_text,
        attempts=attempts,
    )
