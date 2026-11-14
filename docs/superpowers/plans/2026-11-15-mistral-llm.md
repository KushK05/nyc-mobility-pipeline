# Switch Chatbot LLM to Mistral Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the Anthropic Claude client in the chatbot with Mistral (`mistral-medium-latest`) so the text-to-SQL path runs on the Mistral API, with the key supplied later by the user.

**Architecture:** `chatbot/llm.py` keeps its public contract (`complete(system, user, model=None) -> str`, `DEFAULT_MODEL`) and only swaps the provider underneath, so `pipeline.py`, `app.py` and `eval/` are untouched. Config and docs follow the rename of the key and default model.

**Tech Stack:** `mistralai>=3.0` Python SDK (client at `mistralai.client.Mistral`), pytest.

**Spec:** User request on 2026-11-15: "change llm to a mistral medium type of model, I will add the api key later". Prior spec `Plan.md` allowed any LLM API.

## Global Constraints

- Provider: Mistral via the official `mistralai` SDK, imported as `from mistralai.client import Mistral`. Client built lazily inside `complete` as `Mistral(api_key=os.environ["MISTRAL_API_KEY"])`; a missing key raises `RuntimeError("MISTRAL_API_KEY is not set")` (never log or print the key).
- Model: `DEFAULT_MODEL = "mistral-medium-latest"`, overridable by `LLM_MODEL` env var, then by the `model` argument (same precedence as today).
- Call: `client.chat.complete(model=..., messages=[{"role": "system", "content": system}, {"role": "user", "content": user}], max_tokens=4096, temperature=0)`.
- Response handling: `choice = response.choices[0]`; `finish_reason` of `"length"` or `"model_length"` raises `RuntimeError("model output truncated")`; `"error"` raises `RuntimeError("model error")`; content may be a `str` or a list of chunks, so text is `content` if it is a string, else the concatenation of `chunk.text` for chunks that have a `text` attribute.
- `pyproject.toml` `chatbot` extra: replace `anthropic>=2.8` with `mistralai>=3.0`. No `anthropic` import may remain anywhere outside `docs/superpowers/`.
- Env vars: `MISTRAL_API_KEY` replaces `ANTHROPIC_API_KEY` everywhere (`.env.example`, README, eval usage docstring); `LLM_MODEL` default documented as `mistral-medium-latest`.
- Tests mock the SDK by monkeypatching `llm.Mistral` with a fake class; no network. No em dashes. Conventional commit prefix, no co-author lines.

## Review Focus

1. Content returned as a list of chunks (Mistral can return `TextChunk` lists): pinned by `test_complete_joins_chunked_content` (Task 1).
2. Missing API key must fail with a clear message before any network call: `test_complete_requires_api_key` (Task 1).
3. Truncation (`finish_reason == "length"`) must not silently return partial SQL: `test_complete_raises_on_truncation` (Task 1).

---

### Task 1: Mistral client in `chatbot/llm.py`

**Files:**
- Modify: `chatbot/llm.py`, `pyproject.toml` (chatbot extra)
- Test: `tests/test_guard.py` (replace the `_FakeAnthropic` fixture and the four `test_complete_*` tests)

**Interfaces:**
- Produces: unchanged `complete(system: str, user: str, model: str | None = None) -> str` and `DEFAULT_MODEL = "mistral-medium-latest"`; module-level name `Mistral` imported from `mistralai.client` so tests can monkeypatch `llm.Mistral`.

- [ ] **Step 1: Replace the fixture and tests in `tests/test_guard.py`**

```python
class _FakeMistral:
    calls: list[dict] = []
    finish_reason = "stop"
    content = "select 1"
    def __init__(self, api_key):
        _FakeMistral.api_keys.append(api_key)
        self.chat = self
    def complete(self, **kwargs):
        _FakeMistral.calls.append(kwargs)
        message = SimpleNamespace(content=_FakeMistral.content)
        choice = SimpleNamespace(finish_reason=_FakeMistral.finish_reason, message=message)
        return SimpleNamespace(choices=[choice])

@pytest.fixture
def fake_mistral(monkeypatch):  # resets calls/api_keys/finish_reason/content, sets MISTRAL_API_KEY=test-key, deletes LLM_MODEL, monkeypatches llm.Mistral

def test_complete_uses_default_model_and_system_prompt(fake_mistral):  # model == "mistral-medium-latest"; messages == [system, user] dicts; temperature == 0; returns "select 1"; api key passed is "test-key"
def test_complete_model_from_env_and_argument(fake_mistral, monkeypatch):  # LLM_MODEL env then explicit argument wins
def test_complete_raises_on_truncation(fake_mistral):  # finish_reason "length" -> RuntimeError "truncated"; also "model_length"
def test_complete_joins_chunked_content(fake_mistral):  # content = [SimpleNamespace(text="sel"), SimpleNamespace(text="ect 1")] -> "select 1"
def test_complete_requires_api_key(fake_mistral, monkeypatch):  # delenv MISTRAL_API_KEY -> RuntimeError mentioning MISTRAL_API_KEY and no calls recorded
```

- [ ] **Step 2: Run** `.venv/bin/pytest tests/test_guard.py -k complete -v` → FAIL (`llm.Mistral` missing).
- [ ] **Step 3: Rewrite `chatbot/llm.py`** per Global Constraints and update `pyproject.toml`.
- [ ] **Step 4: Run** the same tests → PASS; then `.venv/bin/pytest -q` → all green.
- [ ] **Step 5: Commit** `git commit -m "feat: switch chatbot llm to mistral medium"`

---

### Task 2: Configuration and documentation follow-through

**Files:**
- Modify: `.env.example`, `README.md` (summary line, architecture box text, quick start steps 1 and 7, design decision 2, verification table), `Plan.md` (§3 BI row only), `eval/run_eval.py` (usage docstring)
- Test: `tests/test_docs_no_anthropic.py`

**Interfaces:** none.

- [ ] **Step 1: Write the failing test**

```python
def test_no_anthropic_references_remain():
    # rg-equivalent walk over *.py, *.md, *.toml, .env.example, Makefile excluding .venv and docs/superpowers;
    # assert no file contains "anthropic" or "ANTHROPIC" (case-insensitive); assert .env.example contains "MISTRAL_API_KEY" and "LLM_MODEL=mistral-medium-latest"
```

- [ ] **Step 2: Run** → FAIL. **Step 3: Edit the files.** **Step 4: Run** `.venv/bin/pytest -q` → PASS.
- [ ] **Step 5: Commit** `git commit -m "docs: mistral api key and model in config and docs"`
