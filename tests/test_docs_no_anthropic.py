from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SKIP = {".venv", ".superpowers", "__pycache__", "dbt_packages", "target", ".git"}
OLD_PROVIDER = "anthro" + "pic"


def _files():
    for p in ROOT.rglob("*"):
        rel = p.relative_to(ROOT).parts
        if p.is_file() and not (set(rel) & SKIP or rel[:2] == ("docs", "superpowers")):
            if p.suffix in {".py", ".md", ".toml"} or p.name in {".env.example", "Makefile"}:
                yield p


def test_no_old_provider_references_remain():
    hits = [str(p) for p in _files() if OLD_PROVIDER in p.read_text(errors="ignore").lower()]
    assert hits == []
    env = (ROOT / ".env.example").read_text()
    assert "MISTRAL_API_KEY=" in env
    assert "LLM_MODEL=mistral-medium-latest" in env
