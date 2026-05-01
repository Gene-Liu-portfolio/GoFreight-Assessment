"""LLM-powered natural language to SQL generation with hardened prompt.

Stream-then-validate: when the caller passes ``on_token``, the response is
consumed via the Anthropic streaming API and each text delta is forwarded
to the callback. Validation still runs on the fully-accumulated string —
streaming only changes UX, not correctness.
"""

from typing import Callable

import anthropic

from src.db.schema import get_schema_description
from src.shared import extract_sql
from prompts.system_prompt import SYSTEM_PROMPT_TEMPLATE

OnToken = Callable[[str], None]

# Lazy client — instantiated on first call so module import is side-effect-free
# (no env-var requirement, no network probe at test collection time).
_client: anthropic.Anthropic | None = None


def _get_client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        _client = anthropic.Anthropic()
    return _client


def _run(
    system_prompt: str,
    messages: list[dict],
    on_token: OnToken | None,
) -> str:
    """Call Claude and return the raw text. Streams when on_token is given."""
    if on_token is None:
        response = _get_client().messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            system=system_prompt,
            messages=messages,
        )
        return response.content[0].text

    chunks: list[str] = []
    with _get_client().messages.stream(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        system=system_prompt,
        messages=messages,
    ) as stream:
        for text in stream.text_stream:
            chunks.append(text)
            on_token(text)
    return "".join(chunks)


def generate_sql(
    user_input: str,
    context: str | None = None,
    on_token: OnToken | None = None,
) -> str:
    """Convert natural language to SQL using Claude.

    Args:
        user_input: Natural language question from the user.
        context: Optional extra context to prepend (e.g., contradiction warnings,
                 ambiguity notes from the preprocessor).
        on_token: Optional callback for streamed text deltas. When provided,
                  the request is issued via the streaming API.

    Returns:
        A SQL SELECT statement string (may include leading -- comments).
    """
    schema = get_schema_description()
    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(schema=schema)

    user_message = user_input
    if context:
        user_message = f"{context}\n\nUser question: {user_input}"

    raw_output = _run(
        system_prompt=system_prompt,
        messages=[{"role": "user", "content": user_message}],
        on_token=on_token,
    )
    return extract_sql(raw_output)


def generate_sql_with_retry(
    user_input: str,
    validation_error: str,
    previous_sql: str,
    context: str | None = None,
    on_token: OnToken | None = None,
) -> str:
    """Re-generate SQL after a validation failure (Agentic Retry Loop).

    Feeds the error back to the LLM so it can self-correct.
    """
    schema = get_schema_description()
    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(schema=schema)

    user_message = user_input
    if context:
        user_message = f"{context}\n\nUser question: {user_input}"

    messages = [
        {"role": "user", "content": user_message},
        {"role": "assistant", "content": previous_sql},
        {
            "role": "user",
            "content": (
                f"The SQL you generated has errors:\n{validation_error}\n\n"
                f"Please fix the SQL. Output ONLY the corrected SELECT statement."
            ),
        },
    ]

    raw_output = _run(
        system_prompt=system_prompt,
        messages=messages,
        on_token=on_token,
    )
    return extract_sql(raw_output)
