"""Streamlit chat UI. Run with: streamlit run chatbot/app.py"""

import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from google.cloud import bigquery  # noqa: E402

from chatbot.charts import choose_chart  # noqa: E402
from chatbot.guard import LIMIT_BYTES  # noqa: E402
from chatbot.metadata import format_bytes, load_metadata  # noqa: E402
from chatbot.pipeline import ChatAnswer, answer_question  # noqa: E402

st.set_page_config(page_title="NYC Mobility Chat", layout="wide")

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
LOCATION = os.environ.get("BQ_LOCATION", "US")


@st.cache_resource
def get_client(project_id: str, location: str) -> bigquery.Client:
    return bigquery.Client(project=project_id, location=location)


def render_sidebar() -> None:
    meta = load_metadata()
    with st.sidebar:
        st.header("Data coverage")
        months = [str(m)[:7] for m in meta["months_loaded"]]
        st.write(", ".join(months) if months else "No data loaded yet")
        st.subheader("Last pipeline run")
        st.write(meta["last_run_at"] or "Unknown")
        st.subheader("Quarantine share")
        pct = meta["quarantine_pct"]
        st.write(f"{pct:.2f}%" if pct is not None else "Unknown")
        st.caption(f"Query limit: {format_bytes(LIMIT_BYTES)} scanned per question")


def render_chart(df: pd.DataFrame) -> None:
    kind = choose_chart(df)
    if kind == "bar":
        label = next(c for c in df.columns if not pd.api.types.is_numeric_dtype(df[c]))
        st.bar_chart(df.set_index(label))
    elif kind == "line":
        date_col = next(
            c
            for c in df.columns
            if pd.api.types.is_datetime64_any_dtype(df[c]) or "date" in str(c).lower()
        )
        st.line_chart(df.set_index(date_col).select_dtypes("number"))


def render_answer(a: ChatAnswer) -> None:
    if a.blocked:
        st.error(a.reason)
    if a.rerouted:
        st.info("Rerouted to a cheaper query.")
    if a.answer_text:
        st.markdown(a.answer_text)
    if a.data is not None:
        st.dataframe(a.data)
        render_chart(a.data)
    if a.sql:
        with st.expander("SQL"):
            st.code(a.sql, language="sql")
    st.caption(f"Scanned: {format_bytes(a.bytes_scanned)} · Est. cost: ${a.cost_usd:.4f}")


def main() -> None:
    st.title("NYC Mobility Chat")
    if not PROJECT_ID:
        st.error("GCP_PROJECT_ID is not set. Export it and restart the app.")
        st.stop()
    render_sidebar()
    client = get_client(PROJECT_ID, LOCATION)
    history = st.session_state.setdefault("history", [])

    for a in history:
        with st.chat_message("user"):
            st.write(a.question)
        with st.chat_message("assistant"):
            render_answer(a)

    question = st.chat_input("Ask about NYC taxi trips and weather")
    if question:
        with st.chat_message("user"):
            st.write(question)
        with st.chat_message("assistant"):
            try:
                with st.spinner("Thinking..."):
                    answer = answer_question(question, client, PROJECT_ID)
            except Exception as exc:  # LLM/API failures must not crash the app
                st.error(f"Unexpected error: {exc}")
            else:
                history.append(answer)
                render_answer(answer)


main()
