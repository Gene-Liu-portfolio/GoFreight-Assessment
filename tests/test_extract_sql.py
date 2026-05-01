"""Unit tests for extract_sql — extracting clean SQL from LLM output."""

import pytest

from src.shared import extract_sql


class TestBasicExtraction:
    """Basic SQL extraction from clean and wrapped output."""

    def test_plain_sql(self):
        assert extract_sql("SELECT * FROM Track") == "SELECT * FROM Track"

    def test_strips_trailing_semicolon(self):
        assert extract_sql("SELECT * FROM Track;") == "SELECT * FROM Track"

    def test_strips_whitespace(self):
        assert extract_sql("  SELECT * FROM Track  \n") == "SELECT * FROM Track"

    def test_strips_multiple_semicolons(self):
        assert extract_sql("SELECT * FROM Track;;;") == "SELECT * FROM Track"


class TestMarkdownFences:
    """Extract SQL from markdown code fences (common LLM output)."""

    def test_sql_fence(self):
        raw = "```sql\nSELECT * FROM Track\n```"
        assert extract_sql(raw) == "SELECT * FROM Track"

    def test_plain_fence(self):
        raw = "```\nSELECT * FROM Track\n```"
        assert extract_sql(raw) == "SELECT * FROM Track"

    def test_fence_with_semicolon(self):
        raw = "```sql\nSELECT * FROM Track;\n```"
        assert extract_sql(raw) == "SELECT * FROM Track"

    def test_fence_with_extra_whitespace(self):
        raw = "```sql\n\n  SELECT * FROM Track  \n\n```"
        assert extract_sql(raw) == "SELECT * FROM Track"


class TestPreservesComments:
    """Leading SQL comments should be preserved."""

    def test_preserves_leading_comment(self):
        raw = "-- Note: interpreting 'best' as highest sales\nSELECT * FROM Track"
        result = extract_sql(raw)
        assert result.startswith("-- Note:")
        assert "SELECT * FROM Track" in result

    def test_preserves_multiline_comments(self):
        raw = "-- Line 1\n-- Line 2\nSELECT 1"
        result = extract_sql(raw)
        assert "-- Line 1" in result
        assert "SELECT 1" in result


class TestEdgeCases:
    """Edge cases in LLM output."""

    def test_empty_string(self):
        assert extract_sql("") == ""

    def test_only_semicolons(self):
        assert extract_sql(";;;") == ""

    def test_multiline_sql(self):
        raw = """SELECT t.Name, a.Title
FROM Track t
JOIN Album a ON t.AlbumId = a.AlbumId
LIMIT 5;"""
        result = extract_sql(raw)
        assert "SELECT" in result
        assert "JOIN" in result
        assert not result.endswith(";")
