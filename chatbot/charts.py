"""Chart selection for chatbot result tables."""

import pandas as pd

MAX_BAR_ROWS = 50


def choose_chart(df: pd.DataFrame) -> str | None:
    """Return "bar", "line" or None for a result frame."""
    if df is None or df.empty:
        return None
    numeric = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    non_numeric = [c for c in df.columns if c not in numeric]
    if len(non_numeric) == 1 and numeric and len(df) <= MAX_BAR_ROWS:
        return "bar"
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]) or "date" in str(col).lower():
            return "line"
    return None
