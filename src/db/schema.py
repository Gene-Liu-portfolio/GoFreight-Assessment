"""Schema extraction from SQLite database for LLM context."""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "chinook.db"


def get_connection() -> sqlite3.Connection:
    """Get a read-only connection to the Chinook database."""
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def get_schema_description() -> str:
    """Extract full schema with sample values for LLM System Prompt.

    Returns CREATE TABLE statements plus 3 sample rows per table,
    giving the LLM enough context to generate accurate SQL.
    """
    conn = get_connection()
    cursor = conn.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    tables = [(row["name"], row["sql"]) for row in cursor.fetchall() if row["sql"]]

    schema_parts: list[str] = []
    for table_name, create_sql in tables:
        schema_parts.append(create_sql + ";")

        # Extract sample values to help LLM understand data content
        try:
            sample_cursor = conn.execute(f'SELECT * FROM "{table_name}" LIMIT 3')
            cols = [desc[0] for desc in sample_cursor.description]
            rows = sample_cursor.fetchall()
            if rows:
                schema_parts.append(f"-- Sample data from {table_name}:")
                for row in rows:
                    schema_parts.append(f"--   {dict(zip(cols, tuple(row)))}")
        except sqlite3.Error:
            pass

        schema_parts.append("")

    conn.close()
    return "\n".join(schema_parts)


def get_table_names() -> list[str]:
    """Return all table names in the database."""
    conn = get_connection()
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    names = [row["name"] for row in cursor.fetchall()]
    conn.close()
    return names


def get_column_names(table: str) -> list[str]:
    """Return all column names for a given table."""
    conn = get_connection()
    cursor = conn.execute(f'PRAGMA table_info("{table}")')
    cols = [row["name"] for row in cursor.fetchall()]
    conn.close()
    return cols
