"""Pipeline run metadata shown in the chatbot sidebar."""

import json
import os
from pathlib import Path

DEFAULT_METADATA_PATH = Path(
    os.environ.get("CHATBOT_METADATA_PATH", Path(__file__).with_name("metadata.json"))
)

_DEFAULTS = {"months_loaded": [], "last_run_at": None, "quarantine_pct": None}


def load_metadata(path: Path = DEFAULT_METADATA_PATH) -> dict:
    """Return metadata with defaults; a missing or corrupt file yields defaults."""
    meta = {**_DEFAULTS, "months_loaded": []}
    try:
        loaded = json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return meta
    if isinstance(loaded, dict):
        meta.update(loaded)
    return meta


def format_bytes(n: int) -> str:
    """Binary-unit size: KB below 1 MB, MB below 1 GB, else GB."""
    if n < 1024**2:
        return f"{n / 1024:.1f} KB"
    if n < 1024**3:
        return f"{n / 1024**2:.1f} MB"
    return f"{n / 1024**3:.1f} GB"
