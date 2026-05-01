"""Project-level pytest configuration.

Loads .env from the repo root before tests import any module that constructs
an Anthropic / OpenAI client at module load time (preprocessor, sql_generator).
Without this, importing those modules in a fresh shell would fail with
``AnthropicError: api_key client option must be set``.
"""

from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")
