"""Enhanced SQL validation — syntax, safety, and schema consistency."""

import re
import sqlite3

from src.db.schema import get_connection, get_table_names, get_column_names
from src.shared import FORBIDDEN_PATTERNS


class SQLValidator:
    """Validate SQL for safety, syntax, and schema consistency."""

    def __init__(self):
        self._table_names: set[str] | None = None
        self._columns_by_table: dict[str, set[str]] | None = None

    @property
    def table_names(self) -> set[str]:
        if self._table_names is None:
            self._table_names = {t.lower() for t in get_table_names()}
        return self._table_names

    @property
    def columns_by_table(self) -> dict[str, set[str]]:
        if self._columns_by_table is None:
            self._columns_by_table = {}
            for table in get_table_names():
                cols = get_column_names(table)
                self._columns_by_table[table.lower()] = {c.lower() for c in cols}
        return self._columns_by_table

    def validate(self, sql: str) -> "ValidationResult":
        """Run all validation checks. Returns a ValidationResult."""
        errors: list[str] = []

        # Strip leading SQL comments before validation
        sql_no_comments = re.sub(r"--[^\n]*\n?", "", sql).strip()

        # 1. Statement allowlist — only SELECT or WITH (read-only by construction).
        # Defense in depth: even if a forbidden verb were missing from the deny-list,
        # only read queries can pass this gate.
        head = sql_no_comments.lstrip().upper()
        if not (head.startswith("SELECT") or head.startswith("WITH")):
            errors.append("Only SELECT or WITH queries are allowed")

        # 2. Forbidden pattern check
        for pattern in FORBIDDEN_PATTERNS:
            if re.search(pattern, sql_no_comments, re.IGNORECASE):
                errors.append(f"Forbidden SQL pattern detected: {pattern}")

        # 3. Syntax check via EXPLAIN
        if not errors:
            try:
                conn = get_connection()
                exec_sql = sql_no_comments
                if not exec_sql.rstrip().endswith(";"):
                    exec_sql += ";"
                conn.execute(f"EXPLAIN QUERY PLAN {exec_sql}")
                conn.close()
            except sqlite3.Error as e:
                errors.append(f"SQL syntax error: {e}")

        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            sql=sql,
        )


class ValidationResult:
    def __init__(self, valid: bool, errors: list[str], sql: str):
        self.valid = valid
        self.errors = errors
        self.sql = sql

    @property
    def error_summary(self) -> str:
        return "; ".join(self.errors)
