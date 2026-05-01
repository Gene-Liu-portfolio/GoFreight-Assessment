"""Input preprocessing — classify intent, detect contradictions, reject OOS."""

import json
import re
from dataclasses import dataclass

import anthropic

# Lazy client — instantiated on first use so module import does not require
# the API key or any network access (matters for tests and tooling).
_client: anthropic.Anthropic | None = None


def _get_client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        _client = anthropic.Anthropic()
    return _client


CLASSIFICATION_PROMPT = """You are a query classifier for a music store database (Chinook).
The database contains: artists, albums, tracks, genres, media types, playlists,
customers, employees, invoices, and invoice line items.

Analyze the user input and return ONLY a raw JSON object. Do NOT wrap in markdown code fences.

Return exactly this structure (use true/false/null, not True/False/None):
{{"is_database_query": true, "rejection_reason": null, "has_contradiction": false, "contradiction_detail": null, "ambiguity_note": null, "normalized_query": "the query in English"}}

Rules:
1. is_database_query = false if the question has NOTHING to do with music, customers, employees, invoices, or the store. Examples: weather, jokes, recipes, math, comparisons of external platforms (Spotify, Apple Music, YouTube).
2. is_database_query = false if the query asks about data that does NOT exist in this database (e.g., "Spotify revenue", "YouTube views").
3. has_contradiction = true if the conditions are logically impossible to satisfy simultaneously (e.g., "> 5 minutes AND < 2 minutes", "from France AND lives in Germany").
4. For ambiguous terms (best, popular, senior), note your interpretation in ambiguity_note.
5. For non-English input, set normalized_query to a clear English translation.

User input: {input}"""


@dataclass
class PreprocessResult:
    """Result of input preprocessing."""
    is_valid: bool
    normalized_query: str
    rejection_reason: str | None = None
    has_contradiction: bool = False
    contradiction_detail: str | None = None
    ambiguity_note: str | None = None


def _extract_json(text: str) -> dict | None:
    """Robustly extract a JSON object from LLM output."""
    text = text.strip()

    # Remove markdown code fences
    text = re.sub(r"^```(?:json)?\s*\n?", "", text)
    text = re.sub(r"\n?```\s*$", "", text)
    text = text.strip()

    # Try direct parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Try to find JSON object with regex
    match = re.search(r"\{[^{}]*\}", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass

    # Try finding outermost braces for nested JSON
    first_brace = text.find("{")
    last_brace = text.rfind("}")
    if first_brace != -1 and last_brace != -1 and last_brace > first_brace:
        try:
            return json.loads(text[first_brace:last_brace + 1])
        except json.JSONDecodeError:
            pass

    return None


class InputPreprocessor:
    """Classify and validate user input before SQL generation."""

    def process(self, raw_input: str) -> PreprocessResult:
        prompt = CLASSIFICATION_PROMPT.format(input=raw_input)

        response = _get_client().messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}],
        )

        raw_text = response.content[0].text
        data = _extract_json(raw_text)

        # If parsing fails completely, allow through
        if data is None:
            return PreprocessResult(
                is_valid=True,
                normalized_query=raw_input,
            )

        is_db_query = data.get("is_database_query", True)
        has_contradiction = data.get("has_contradiction", False)

        # Reject out-of-scope
        if not is_db_query:
            return PreprocessResult(
                is_valid=False,
                normalized_query=raw_input,
                rejection_reason=data.get("rejection_reason", "Query is not related to the music database."),
            )

        # Warn about contradictions but still allow through (with flag)
        return PreprocessResult(
            is_valid=True,
            normalized_query=data.get("normalized_query", raw_input),
            has_contradiction=has_contradiction,
            contradiction_detail=data.get("contradiction_detail"),
            ambiguity_note=data.get("ambiguity_note"),
        )
