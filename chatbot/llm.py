"""Thin Mistral client wrapper. Reads MISTRAL_API_KEY from the environment."""

import os

from mistralai.client import Mistral

DEFAULT_MODEL = "mistral-medium-latest"


def complete(system: str, user: str, model: str | None = None) -> str:
    """Send one user turn and return the text of the reply."""
    api_key = os.environ.get("MISTRAL_API_KEY")
    if not api_key:
        raise RuntimeError("MISTRAL_API_KEY is not set")
    client = Mistral(api_key=api_key)
    response = client.chat.complete(
        model=model or os.environ.get("LLM_MODEL", DEFAULT_MODEL),
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        max_tokens=4096,
        temperature=0,
    )
    choice = response.choices[0]
    if choice.finish_reason in ("length", "model_length"):
        raise RuntimeError("model output truncated")
    if choice.finish_reason == "error":
        raise RuntimeError("model error")
    content = choice.message.content
    if isinstance(content, str):
        return content
    return "".join(chunk.text for chunk in content or [] if hasattr(chunk, "text"))
