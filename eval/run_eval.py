"""Run the text-to-SQL evaluation set against BigQuery and report execution accuracy.

Usage: GCP_PROJECT_ID=... ANTHROPIC_API_KEY=... python eval/run_eval.py
"""

import csv
import os
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd  # noqa: E402
from google.api_core.exceptions import GoogleAPICallError  # noqa: E402
from google.cloud import bigquery  # noqa: E402

from chatbot import llm  # noqa: E402
from chatbot.guard import LIMIT_BYTES  # noqa: E402
from chatbot.pipeline import Complete, answer_question  # noqa: E402

EVAL_DIR = Path(__file__).resolve().parent
ROW_FIELDS = [
    "id",
    "category",
    "correct",
    "blocked",
    "rerouted",
    "bytes_scanned",
    "cost_usd",
    "sql",
    "error",
]


@dataclass
class EvalReport:
    execution_accuracy: float
    avg_bytes_scanned: float
    blocked: int
    rerouted: int
    rows: list[dict]


def load_questions(path: Path) -> list[dict]:
    """Read questions.csv; `expect_blocked` becomes a bool."""
    with Path(path).open(newline="") as f:
        rows = list(csv.DictReader(f))
    for row in rows:
        row["expect_blocked"] = row["expect_blocked"].strip().lower() == "true"
    return rows


def _normalise(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).lower() for c in df.columns]
    df = df[sorted(df.columns)]
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].astype("float64").round(2)
    return df.sort_values(list(df.columns), na_position="last").reset_index(drop=True)


def compare_results(a: pd.DataFrame | None, b: pd.DataFrame | None) -> bool:
    """True when both frames hold the same rows, ignoring row order, column order,
    column name case and differences below 2 decimal places. NaN equals NaN."""
    if a is None or b is None:
        return False
    if a.shape != b.shape:
        return False
    if sorted(str(c).lower() for c in a.columns) != sorted(str(c).lower() for c in b.columns):
        return False
    if len(set(str(c).lower() for c in a.columns)) != a.shape[1]:
        return False
    try:
        pd.testing.assert_frame_equal(
            _normalise(a), _normalise(b), check_dtype=False, check_exact=True
        )
    except (AssertionError, TypeError, ValueError):
        return False
    return True


def _run_gold(client: bigquery.Client, gold_sql: str) -> pd.DataFrame:
    config = bigquery.QueryJobConfig(maximum_bytes_billed=LIMIT_BYTES)
    return client.query(gold_sql, job_config=config).to_dataframe()


def run(
    questions: list[dict], client: bigquery.Client, project_id: str, complete: Complete
) -> EvalReport:
    rows: list[dict] = []
    for q in questions:
        answer = answer_question(q["question"], client, project_id, complete=complete)
        error = answer.reason
        if q["expect_blocked"]:
            correct = answer.blocked
        elif answer.blocked:
            correct = False
        else:
            gold_sql = q["gold_sql"].replace("{project}", project_id)
            try:
                gold_df = _run_gold(client, gold_sql)
            except GoogleAPICallError as exc:
                gold_df, error = None, f"gold query failed: {exc}"
            correct = compare_results(answer.data, gold_df)
        rows.append(
            {
                "id": q["id"],
                "category": q["category"],
                "correct": bool(correct),
                "blocked": answer.blocked,
                "rerouted": answer.rerouted,
                "bytes_scanned": answer.bytes_scanned,
                "cost_usd": answer.cost_usd,
                "sql": answer.sql,
                "error": error,
            }
        )

    answered = [r["bytes_scanned"] for r in rows if not r["blocked"]]
    return EvalReport(
        execution_accuracy=sum(r["correct"] for r in rows) / len(rows) if rows else 0.0,
        avg_bytes_scanned=sum(answered) / len(answered) if answered else 0.0,
        blocked=sum(r["blocked"] for r in rows),
        rerouted=sum(r["rerouted"] for r in rows),
        rows=rows,
    )


def _cell(value) -> str:
    text = "" if value is None else str(value)
    return " ".join(text.split()).replace("|", "\\|")


def write_report(report: EvalReport, csv_path: Path, md_path: Path) -> None:
    """Write per-question rows to CSV and a summary plus per-question table to markdown."""
    with Path(csv_path).open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=ROW_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in report.rows:
            writer.writerow({key: row.get(key) for key in ROW_FIELDS})

    lines = [
        "# Text-to-SQL evaluation results",
        "",
        "| Metric | Value |",
        "| --- | --- |",
        f"| Questions | {len(report.rows)} |",
        f"| Execution accuracy | {report.execution_accuracy:.1%} |",
        f"| Avg bytes scanned (answered) | {report.avg_bytes_scanned / 1024**2:,.2f} MB |",
        f"| Blocked | {report.blocked} |",
        f"| Rerouted | {report.rerouted} |",
        "",
        "## Per question",
        "",
        "| ID | Category | Correct | Blocked | Rerouted | Bytes scanned | Cost (USD) | SQL |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in report.rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    _cell(row["id"]),
                    _cell(row["category"]),
                    "yes" if row["correct"] else "no",
                    "yes" if row["blocked"] else "no",
                    "yes" if row["rerouted"] else "no",
                    f"{row['bytes_scanned']:,}",
                    f"{row['cost_usd']:.6f}",
                    f"`{_cell(row['sql'])}`" if row["sql"] else "",
                ]
            )
            + " |"
        )
    Path(md_path).write_text("\n".join(lines) + "\n")


def main() -> None:
    project_id = os.environ["GCP_PROJECT_ID"]
    client = bigquery.Client(project=project_id, location=os.environ.get("BQ_LOCATION", "US"))
    questions = load_questions(EVAL_DIR / "questions.csv")
    report = run(questions, client, project_id, llm.complete)
    write_report(report, EVAL_DIR / "results.csv", EVAL_DIR / "results.md")
    print(
        f"Execution accuracy {report.execution_accuracy:.1%} over {len(report.rows)} questions; "
        f"blocked {report.blocked}, rerouted {report.rerouted}."
    )


if __name__ == "__main__":
    main()
