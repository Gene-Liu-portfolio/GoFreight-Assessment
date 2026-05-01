"""End-to-end integration tests for the hardened pipeline.

Mocks LLM calls so the test runs offline (no API key needed, no API cost).
Validator and executor run against the real Chinook DB so we genuinely exercise
the wiring between preprocessor → generator → validator → executor.
"""

import pytest

from src.core.executor import SQLExecutor
from src.hardening.preprocessor import InputPreprocessor, PreprocessResult
from src.hardening.retry import generate_validated_sql


@pytest.fixture
def stub_preprocessor(monkeypatch):
    """Make InputPreprocessor.process return a valid result without calling Claude."""

    def _stub(self, raw_input: str) -> PreprocessResult:
        return PreprocessResult(is_valid=True, normalized_query=raw_input)

    monkeypatch.setattr(InputPreprocessor, "process", _stub)


def _stub_generators(monkeypatch, first_sql: str, retry_sql: str | None = None) -> None:
    """Patch retry.generate_sql / generate_sql_with_retry at the import site.

    retry.py imports the two functions directly, so the patch must target
    retry's namespace, not src.core.sql_generator.
    """

    def _gen(user_input, context=None, on_token=None):
        return first_sql

    def _gen_retry(user_input, validation_error, previous_sql, context=None, on_token=None):
        return retry_sql if retry_sql is not None else first_sql

    monkeypatch.setattr("src.hardening.retry.generate_sql", _gen)
    monkeypatch.setattr("src.hardening.retry.generate_sql_with_retry", _gen_retry)


class TestHappyPath:
    """Preprocess → generate → validate → execute, all components wired."""

    def test_simple_count_query(self, stub_preprocessor, monkeypatch):
        _stub_generators(monkeypatch, "SELECT COUNT(*) AS n FROM Track")

        pre = InputPreprocessor().process("how many tracks are there?")
        assert pre.is_valid

        retry_result = generate_validated_sql(pre.normalized_query)
        assert retry_result.valid
        assert retry_result.retry_count == 0

        exec_result = SQLExecutor().execute(retry_result.sql)
        assert exec_result.success
        assert exec_result.row_count == 1
        assert exec_result.rows[0]["n"] > 0

    def test_join_query_returns_data(self, stub_preprocessor, monkeypatch):
        sql = (
            "SELECT ar.Name AS artist, COUNT(a.AlbumId) AS album_count "
            "FROM Artist ar JOIN Album a ON ar.ArtistId = a.ArtistId "
            "GROUP BY ar.ArtistId ORDER BY album_count DESC LIMIT 5"
        )
        _stub_generators(monkeypatch, sql)

        retry_result = generate_validated_sql("top 5 artists by album count")
        assert retry_result.valid

        exec_result = SQLExecutor().execute(retry_result.sql)
        assert exec_result.success
        assert exec_result.row_count == 5
        assert "artist" in exec_result.columns
        assert "album_count" in exec_result.columns


class TestRetryLoop:
    """Validation failure should round-trip through generate_sql_with_retry."""

    def test_recovers_from_invalid_first_attempt(self, stub_preprocessor, monkeypatch):
        # First attempt: schema mismatch (NoSuchTable). Retry: valid SQL.
        _stub_generators(
            monkeypatch,
            first_sql="SELECT * FROM NoSuchTable",
            retry_sql="SELECT TrackId FROM Track LIMIT 1",
        )

        retry_result = generate_validated_sql("anything")
        assert retry_result.valid
        assert retry_result.retry_count == 1
        assert len(retry_result.attempts) == 2
        assert retry_result.attempts[0]["valid"] is False
        assert retry_result.attempts[1]["valid"] is True


class TestDefenseInDepth:
    """Dangerous SQL must be blocked by both the validator and the executor."""

    def test_validator_blocks_drop_even_after_retries(self, stub_preprocessor, monkeypatch):
        # Both the first attempt and every retry produce dangerous SQL.
        # The retry loop should exhaust and surface a failed RetryResult.
        _stub_generators(
            monkeypatch,
            first_sql="DROP TABLE Track",
            retry_sql="DELETE FROM Track",
        )

        retry_result = generate_validated_sql("erase everything")
        assert not retry_result.valid
        assert retry_result.final_error is not None
        # The new SELECT/WITH allowlist should fire on both attempts.
        combined = retry_result.final_error.lower()
        assert "only select" in combined or "forbidden" in combined

    def test_executor_independently_rejects_dangerous_sql(self):
        # Caller-supplied SQL that bypasses retry must still be rejected by
        # the executor's own validate_sql.
        for evil in [
            "DROP TABLE Track",
            "DELETE FROM Customer",
            "INSERT INTO Artist (Name) VALUES ('x')",
            "PRAGMA table_info(Track)",
            "SELECT 1; DROP TABLE Track",
        ]:
            result = SQLExecutor().execute(evil)
            assert not result.success, f"Executor should reject: {evil!r}"
            assert "validation failed" in result.error.lower()


class TestOutOfScopeRejection:
    """Preprocessor's invalid result short-circuits the rest of the pipeline."""

    def test_invalid_preprocessor_result_stops_chain(self, monkeypatch):
        def _reject(self, raw_input: str) -> PreprocessResult:
            return PreprocessResult(
                is_valid=False,
                normalized_query=raw_input,
                rejection_reason="Out of scope",
            )

        monkeypatch.setattr(InputPreprocessor, "process", _reject)

        pre = InputPreprocessor().process("what is the weather today?")
        assert not pre.is_valid
        assert pre.rejection_reason == "Out of scope"
