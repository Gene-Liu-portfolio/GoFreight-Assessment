"""Output formatting for query results."""

import json

from rich.console import Console
from rich.table import Table

from src.core.executor import ExecutionResult

console = Console()


def display_result(result: ExecutionResult, fmt: str = "table", show_sql: bool = False) -> None:
    """Display an ExecutionResult in the requested format.

    Args:
        result: The query execution result.
        fmt: Output format — "table" or "json".
        show_sql: Whether to print the generated SQL above the results.
    """
    if show_sql:
        console.print(f"\n[dim]Generated SQL:[/dim]")
        console.print(f"[cyan]{result.sql}[/cyan]\n")

    if not result.success:
        console.print(f"[bold red]Error:[/bold red] {result.error}")
        return

    if not result.rows:
        console.print("[yellow]Query returned no results.[/yellow]")
        return

    if fmt == "json":
        _display_json(result)
    else:
        _display_table(result)


def _display_table(result: ExecutionResult) -> None:
    """Render results as a rich table."""
    table = Table(show_header=True, header_style="bold magenta")

    for col in result.columns:
        table.add_column(col)

    for row in result.rows:
        table.add_row(*[str(v) if v is not None else "" for v in row.values()])

    console.print(table)
    console.print(f"[dim]({result.row_count} rows)[/dim]")


def _display_json(result: ExecutionResult) -> None:
    """Render results as formatted JSON."""
    console.print_json(json.dumps(result.rows, ensure_ascii=False, default=str))
