"""Unit tests for SQL Validator — safety checks and syntax validation."""

import pytest

from src.hardening.sql_validator import SQLValidator

validator = SQLValidator()


class TestForbiddenPatterns:
    """Validator must reject dangerous SQL patterns."""

    @pytest.mark.parametrize("sql", [
        "DROP TABLE Customer",
        "DELETE FROM Track WHERE TrackId = 1",
        "UPDATE Customer SET Email = 'x' WHERE CustomerId = 1",
        "INSERT INTO Artist (Name) VALUES ('Evil')",
        "CREATE TABLE hack (id INT)",
        "ALTER TABLE Customer ADD COLUMN password TEXT",
        "ATTACH DATABASE '/etc/passwd' AS pw",
        "DETACH DATABASE main",
    ])
    def test_rejects_ddl_dml(self, sql):
        result = validator.validate(sql)
        assert not result.valid
        assert "Forbidden" in result.error_summary

    @pytest.mark.parametrize("sql", [
        "SELECT 1; DROP TABLE Customer",
        "SELECT * FROM Track; DELETE FROM Track",
        "SELECT 1; SELECT 2",
    ])
    def test_rejects_multiple_statements(self, sql):
        result = validator.validate(sql)
        assert not result.valid

    def test_rejects_pragma(self):
        result = validator.validate("PRAGMA table_info(Customer)")
        assert not result.valid

    def test_case_insensitive_rejection(self):
        result = validator.validate("drop table Customer")
        assert not result.valid

    def test_rejects_mixed_case(self):
        result = validator.validate("DrOp TaBlE Customer")
        assert not result.valid


class TestValidSQL:
    """Validator must accept valid SELECT queries."""

    @pytest.mark.parametrize("sql", [
        "SELECT COUNT(*) FROM Track",
        "SELECT * FROM Customer WHERE Country = 'USA'",
        "SELECT t.Name FROM Track t JOIN Album a ON t.AlbumId = a.AlbumId",
        "SELECT COUNT(*) AS cnt FROM Track GROUP BY AlbumId HAVING cnt > 10",
        "SELECT * FROM Track ORDER BY Milliseconds DESC LIMIT 5",
    ])
    def test_accepts_valid_select(self, sql):
        result = validator.validate(sql)
        assert result.valid
        assert result.errors == []

    def test_accepts_sql_with_comments(self):
        sql = "-- Interpreting 'best' as highest sales\nSELECT * FROM Track LIMIT 5"
        result = validator.validate(sql)
        assert result.valid

    def test_accepts_subquery(self):
        sql = """SELECT AVG(cnt) FROM (
            SELECT COUNT(*) AS cnt FROM Track GROUP BY AlbumId
        )"""
        result = validator.validate(sql)
        assert result.valid

    def test_accepts_window_function(self):
        sql = """SELECT Name, ROW_NUMBER() OVER (ORDER BY Milliseconds DESC) AS rn
        FROM Track LIMIT 10"""
        result = validator.validate(sql)
        assert result.valid


class TestSyntaxValidation:
    """Validator must catch SQL syntax errors via EXPLAIN."""

    def test_rejects_invalid_table(self):
        result = validator.validate("SELECT * FROM NonExistentTable")
        assert not result.valid
        assert "syntax error" in result.error_summary.lower() or "no such table" in result.error_summary.lower()

    def test_rejects_invalid_column(self):
        result = validator.validate("SELECT NonExistentColumn FROM Track")
        assert not result.valid

    def test_rejects_malformed_sql(self):
        result = validator.validate("SELEC * FORM Track")
        assert not result.valid


class TestCommentStripping:
    """Comments with forbidden words should not trigger false positives."""

    def test_comment_with_drop_keyword(self):
        sql = "-- Note: We drop the NULL rows\nSELECT * FROM Track WHERE Name IS NOT NULL LIMIT 5"
        result = validator.validate(sql)
        assert result.valid

    def test_comment_with_delete_keyword(self):
        sql = "-- Delete scenario: showing tracks not purchased\nSELECT * FROM Track LIMIT 5"
        result = validator.validate(sql)
        assert result.valid
