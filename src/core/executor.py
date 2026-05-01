"""SQLite query execution engine with safety controls."""

import re
import sqlite3
from dataclasses import dataclass, field

from src.db.schema import get_connection
from src.shared import FORBIDDEN_PATTERNS


@dataclass
class ExecutionResult:
    """Result of a SQL query execution."""

    success: bool
    sql: str
    columns: list[str] = field(default_factory=list)
    rows: list[dict] = field(default_factory=list)
    row_count: int = 0
    error: str | None = None


class SQLExecutor:
    """Execute SQL queries against Chinook DB with safety controls."""

    MAX_ROWS = 100
    TIMEOUT_MS = 5000

    def validate_sql(self, sql: str) -> list[str]:
        """Check SQL for forbidden patterns. Returns list of error messages."""
        # Strip SQL comments before checking patterns to avoid false positives
        # (e.g., a semicolon at the end of a comment line)
        sql_no_comments = re.sub(r"--[^\n]*\n?", "", sql).strip()
        errors: list[str] = []

        # Statement allowlist — only SELECT or WITH (defense in depth).
        head = sql_no_comments.lstrip().upper()
        if not (head.startswith("SELECT") or head.startswith("WITH")):
            errors.append("Only SELECT or WITH queries are allowed")

        for pattern in FORBIDDEN_PATTERNS:
            if re.search(pattern, sql_no_comments, re.IGNORECASE):
                errors.append(f"Forbidden SQL pattern: {pattern}")
        return errors

    def execute(self, sql: str) -> ExecutionResult:
        """Execute a SQL query and return structured results.

        The connection is read-only. Dangerous statements are rejected
        before execution.
        """
        # Safety check
        errors = self.validate_sql(sql)
        if errors:
            return ExecutionResult(
                success=False,
                sql=sql,
                error=f"SQL validation failed: {'; '.join(errors)}",
            )

        conn = get_connection()
        try:
            conn.execute(f"PRAGMA busy_timeout = {self.TIMEOUT_MS}")

            # Add semicolon back for execution
            exec_sql = sql if sql.rstrip().endswith(";") else sql + ";"
            cursor = conn.execute(exec_sql)

            columns = (
                [desc[0] for desc in cursor.description] if cursor.description else []
            )
            raw_rows = cursor.fetchmany(self.MAX_ROWS)
            rows = [dict(zip(columns, row)) for row in raw_rows]

            return ExecutionResult(
                success=True,
                sql=sql,
                columns=columns,
                rows=rows,
                row_count=len(rows),
            )
        except sqlite3.Error as e:
            return ExecutionResult(
                success=False,
                sql=sql,
                error=f"SQL execution error: {e}",
            )
        finally:
            conn.close()
