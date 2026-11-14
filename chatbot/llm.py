"""Thin Anthropic client wrapper. The SDK reads ANTHROPIC_API_KEY from the environment."""

import os

import anthropic

DEFAULT_MODEL = "claude-opus-5-5"


def complete(system: str, user: str, model: str | None = None) -> str:
    """Send one user turn and return the concatenated text of the reply."""
    client = anthropic.Anthropic()
    response = client.beta.messages.create(
        model=model or os.environ.get("LLM_MODEL", DEFAULT_MODEL),
        max_tokens=2048,
        system=system,
        messages=[{"role": "user", "content": user}],
        betas=["server-side-fallback-2026-07-01"],
        fallbacks="default",
    )
    if response.stop_reason == "refusal":
        raise RuntimeError("model refused")
    return "".join(
        block.text for block in response.content if getattr(block, "type", "") == "text"
    )
