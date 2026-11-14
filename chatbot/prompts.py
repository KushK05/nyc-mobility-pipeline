"""Prompt templates for SQL generation, cheaper rewrites and plain-English answers."""

from chatbot.guard import LIMIT_BYTES
from chatbot.schema_catalog import render_schema

SYSTEM_PROMPT_TEMPLATE = """You translate questions about NYC yellow taxi trips and weather into one BigQuery SQL query.

Rules:
- Only use tables in the `marts` dataset.
- Fully qualify every table as `{project}.marts.<table>`.
- Use BigQuery Standard SQL.
- Prefer `agg_hourly_zone_demand` / `agg_daily_weather_impact` over `fct_trips` whenever possible.
- Any query on `fct_trips` must filter on `pickup_date`.
- Always add `LIMIT 100` unless aggregating.
- Return only SQL, no explanation or markdown fences.

Schema:
{schema}

Examples:
{examples}
"""

FEW_SHOT_EXAMPLES: list[tuple[str, str]] = [
    (
        "Does rain make trips longer in Manhattan on weekday evenings?",
        """select
    t.is_rainy,
    count(*) as trips,
    avg(t.trip_duration_min) as avg_duration_min
from `{project}.marts.fct_trips` as t
join `{project}.marts.dim_zone` as z
    on t.pu_location_id = z.location_id
where t.pickup_date between '2024-01-01' and '2024-06-30'
    and z.borough = 'Manhattan'
    and t.pickup_hour between 17 and 20
    and extract(dayofweek from t.pickup_date) between 2 and 6
group by t.is_rainy""",
    ),
    (
        "How many trips were there in March 2024?",
        """select sum(trips) as trips
from `{project}.marts.agg_hourly_zone_demand`
where pickup_date between '2024-03-01' and '2024-03-31'""",
    ),
    (
        "What were the top 10 airport pickup zones in February?",
        """select
    z.zone,
    sum(a.trips) as trips
from `{project}.marts.agg_hourly_zone_demand` as a
join `{project}.marts.dim_zone` as z
    on a.pu_location_id = z.location_id
where a.pickup_date between '2024-02-01' and '2024-02-29'
    and z.is_airport
group by z.zone
order by trips desc
limit 10""",
    ),
    (
        "Are rainy days slower than dry days on average?",
        """select
    is_rainy,
    sum(trips) as trips,
    sum(avg_duration_min * trips) / sum(trips) as avg_duration_min
from `{project}.marts.agg_daily_weather_impact`
group by is_rainy""",
    ),
]


def _fill(template: str, project_id: str) -> str:
    return template.replace("{project}", project_id)


def build_system_prompt(project_id: str) -> str:
    examples = "\n\n".join(
        f"Question: {question}\nSQL:\n{_fill(sql, project_id)}"
        for question, sql in FEW_SHOT_EXAMPLES
    )
    return (
        SYSTEM_PROMPT_TEMPLATE.replace("{schema}", render_schema(project_id))
        .replace("{examples}", examples)
        .replace("{project}", project_id)
    )


def build_sql_prompt(question: str, project_id: str) -> tuple[str, str]:
    return build_system_prompt(project_id), question


def build_rewrite_prompt(
    question: str, sql: str, bytes_scanned: int | None, project_id: str
) -> tuple[str, str]:
    """Ask for a cheaper or corrected query. `bytes_scanned=None` when the query never ran."""
    limit_mb = LIMIT_BYTES / 1024**2
    if bytes_scanned is None:
        problem = "The query below cannot be used."
    else:
        problem = (
            f"The query below would scan {bytes_scanned / 1024**2:.0f} MB, "
            f"over the {limit_mb:.0f} MB limit."
        )
    user = (
        f"Question: {question}\n\n"
        f"{problem}\n\n"
        f"Query:\n{sql}\n\n"
        f"Rewrite it to answer the same question while scanning under {limit_mb:.0f} MB: "
        "use the `agg_hourly_zone_demand` or `agg_daily_weather_impact` tables, "
        "or a narrower `pickup_date` range. Use only tables in the `marts` dataset. "
        "Return only SQL."
    )
    return build_system_prompt(project_id), user


def build_answer_prompt(question: str, sql: str, df_preview_csv: str) -> tuple[str, str]:
    system = (
        "You explain SQL query results about NYC taxi trips to a non-technical reader. "
        "Answer in plain English in at most 3 sentences, based only on the result preview. "
        "If the preview does not answer the question, say so."
    )
    user = (
        f"Question: {question}\n\n"
        f"SQL:\n{sql}\n\n"
        f"Result preview (CSV, up to 20 rows):\n{df_preview_csv}"
    )
    return system, user
