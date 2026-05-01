"""Shared constants and utilities used by both CLI (Part 1) and eval pipeline (Part 2)."""

import re

# Forbidden SQL patterns — used by both SQLValidator and SQLExecutor
FORBIDDEN_PATTERNS = [
    r"\b(DROP|DELETE|UPDATE|INSERT|CREATE|ALTER|ATTACH|DETACH)\b",
    r"\bPRAGMA\b",
    r";\s*\S",  # multiple statements (SQL injection vector)
]


def extract_sql(raw_output: str) -> str:
    """Extract clean SQL from LLM output, preserving leading -- comments."""
    text = raw_output.strip()

    # Remove markdown code fences if present
    if text.startswith("```"):
        text = re.sub(r"^```(?:sql)?\s*\n?", "", text)
        text = re.sub(r"\n?```\s*$", "", text)
        text = text.strip()

    # Remove trailing semicolons for consistent handling
    text = text.rstrip(";").strip()

    return text
