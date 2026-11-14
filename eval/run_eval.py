"""Run the text-to-SQL evaluation set against BigQuery and report execution accuracy.

Usage: GCP_PROJECT_ID=... MISTRAL_API_KEY=... python eval/run_eval.py
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
    errors: int = 0  # questions whose pipeline raised or whose gold query failed


def load_questions(path: Path) -> list[dict]:
    """Read questions.csv; `expect_blocked` becomes a bool."""
    with Path(path).open(newline="") as f:
        rows = list(csv.DictReader(f))
    for row in rows:
        row["expect_blocked"] = row["expect_blocked"].strip().lower() == "true"
    return rows


def _column_values(series: pd.Series) -> list:
    """Sorted values of one column: numbers rounded to 2 dp, everything else as strings.

    Missing values (NaN, None) sort last and compare equal to each other.
    """
    if pd.api.types.is_numeric_dtype(series):
        values = series.astype("float64").round(2)
        present = sorted(float(v) for v in values if not pd.isna(v))
    else:
        present = sorted(str(v) for v in series if not pd.isna(v))
    return present + [None] * int(series.isna().sum())


def _match(gold_cols: list[list], answer_cols: list[list], used: set[int]) -> bool:
    if not gold_cols:
        return True
    first, rest = gold_cols[0], gold_cols[1:]
    for i, candidate in enumerate(answer_cols):
        if i not in used and candidate == first and _match(rest, answer_cols, used | {i}):
            return True
    return False


def compare_results(answer: pd.DataFrame | None, gold: pd.DataFrame | None) -> bool:
    """True when every gold column's values equal those of a distinct answer column.

    Column names and order are ignored and extra answer columns are allowed. Values are
    compared per column after sorting, with numbers rounded to 2 dp and NaN equal to NaN.
    """
    if answer is None or gold is None or len(answer) != len(gold):
        return False
    gold_cols = [_column_values(gold.iloc[:, i]) for i in range(gold.shape[1])]
    answer_cols = [_column_values(answer.iloc[:, i]) for i in range(answer.shape[1])]
    return _match(gold_cols, answer_cols, set())


def _run_gold(client: bigquery.Client, gold_sql: str) -> pd.DataFrame:
    config = bigquery.QueryJobConfig(maximum_bytes_billed=LIMIT_BYTES)
    return client.query(gold_sql, job_config=config).to_dataframe()


def run(
    questions: list[dict], client: bigquery.Client, project_id: str, complete: Complete
) -> EvalReport:
    rows: list[dict] = []
    errors = 0
    for q in questions:
        try:
            answer = answer_question(q["question"], client, project_id, complete=complete)
        except Exception as exc:  # noqa: BLE001 - one bad question must not stop the run
            errors += 1
            rows.append(
                {
                    "id": q["id"],
                    "category": q["category"],
                    "correct": False,
                    "blocked": False,
                    "rerouted": False,
                    "bytes_scanned": 0,
                    "cost_usd": 0.0,
                    "sql": "",
                    "error": str(exc),
                }
            )
            continue
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
                errors += 1
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

    answered = [r["bytes_scanned"] for r in rows if not r["blocked"] and r["sql"]]
    return EvalReport(
        execution_accuracy=sum(r["correct"] for r in rows) / len(rows) if rows else 0.0,
        avg_bytes_scanned=sum(answered) / len(answered) if answered else 0.0,
        blocked=sum(r["blocked"] for r in rows),
        rerouted=sum(r["rerouted"] for r in rows),
        rows=rows,
        errors=errors,
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
        f"| Gold/runtime errors | {report.errors} |",
        "",
        "## Per question",
        "",
        "| ID | Category | Correct | Blocked | Rerouted | Bytes scanned | Cost (USD) | SQL | Error |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
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
                    _cell(row.get("error")),
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
        f"blocked {report.blocked}, rerouted {report.rerouted}, errors {report.errors}."
    )


if __name__ == "__main__":
    main()
