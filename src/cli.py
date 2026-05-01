"""CLI entry point — natural language to SQL for Chinook database (hardened)."""

import time
from contextlib import contextmanager
from pathlib import Path
from typing import Annotated

import typer
from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

# Load .env from project root
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from src.core.executor import ExecutionResult
from src.core.executor import SQLExecutor
from src.core.formatter import display_result
from src.hardening.preprocessor import PreprocessResult
from src.hardening.preprocessor import InputPreprocessor
from src.hardening.retry import generate_validated_sql
from src.hardening.retry import RetryResult

app = typer.Typer(
    name="nlsql",
    help="Ask questions about a music store database in natural language.",
)
console = Console()
executor = SQLExecutor()
preprocessor = InputPreprocessor()


class StageReporter:
    """Print per-stage status with cumulative elapsed time.

    Each stage prints a header line on entry and a "done" line on exit,
    both annotated with `+<elapsed>s` measured from reporter creation.
    Streamed tokens (or other inline output) printed inside a stage land
    between those two lines.
    """

    def __init__(self, console: Console):
        self.console = console
        self.t0 = time.perf_counter()

    @property
    def elapsed(self) -> float:
        return time.perf_counter() - self.t0

    @contextmanager
    def stage(self, label: str):
        start = time.perf_counter()
        self.console.print(
            f"[dim]+{self.elapsed:5.1f}s[/dim]  [bold cyan]▸[/bold cyan] {label}…"
        )
        try:
            yield self
        finally:
            dt = time.perf_counter() - start
            self.console.print(
                f"[dim]+{self.elapsed:5.1f}s     ✓ {label} ({dt:.1f}s)[/dim]"
            )


@app.command()
def query(
    question: Annotated[str, typer.Argument(help="Your question in natural language")],
    format: Annotated[
        str, typer.Option("--format", "-f", help="Output format: table or json")
    ] = "table",
    show_sql: Annotated[
        bool, typer.Option("--show-sql", "-s", help="Show the generated SQL query")
    ] = False,
    trace: Annotated[
        bool,
        typer.Option(
            "--trace/--no-trace",
            help="Show system checks and the final generated SQL query.",
        ),
    ] = True,
    stream: Annotated[
        bool,
        typer.Option(
            "--stream/--no-stream",
            help="Stream SQL tokens as they're generated (stream-then-validate).",
        ),
    ] = True,
) -> None:
    """Convert a natural language question to SQL, execute it, and display results."""
    is_json = format == "json"
    # When format=json, stdout must remain parseable JSON; route status to stderr.
    status_console = Console(stderr=is_json)
    reporter = StageReporter(status_console)

    # ── 1. Preprocess: intent classification + contradiction detection ──
    with reporter.stage("Analyze query (Haiku)"):
        try:
            pre = preprocessor.process(question)
        except Exception as e:
            status_console.print(f"[bold red]Preprocessing Error:[/bold red] {e}")
            raise typer.Exit(code=1)

    if not pre.is_valid:
        if trace:
            _display_system_trace(pre=pre, retry_result=None, result=None, fmt=format)
        console.print(f"[bold yellow]Query rejected:[/bold yellow] {pre.rejection_reason}")
        raise typer.Exit(code=0)

    # ── 2. Build context for SQL generator ──
    context_parts: list[str] = []
    if pre.has_contradiction:
        context_parts.append(
            f"WARNING: The user's query has contradictory conditions: "
            f"{pre.contradiction_detail}. "
            f"Do NOT silently drop conditions. Output a warning message via SELECT."
        )
    if pre.ambiguity_note:
        context_parts.append(f"Ambiguity note: {pre.ambiguity_note}")

    context = "\n".join(context_parts) if context_parts else None

    # ── 3. Generate SQL with validation + retry (streamed) ──
    on_token, on_attempt = _make_stream_hooks(status_console, enabled=stream)

    with reporter.stage("Generate + validate SQL"):
        try:
            retry_result = generate_validated_sql(
                pre.normalized_query,
                context=context,
                on_token=on_token,
                on_attempt=on_attempt,
            )
        except Exception as e:
            if stream:
                status_console.print()  # close any open stream line
            status_console.print(f"[bold red]LLM Error:[/bold red] {e}")
            raise typer.Exit(code=1)
        if stream:
            status_console.print()  # newline after streamed tokens

    if not retry_result.valid:
        if trace:
            _display_system_trace(pre=pre, retry_result=retry_result, result=None, fmt=format)
        console.print(
            f"[bold red]SQL generation failed after {retry_result.retry_count + 1} attempts:[/bold red] "
            f"{retry_result.final_error}"
        )
        raise typer.Exit(code=1)

    if retry_result.retry_count > 0:
        status_console.print(
            f"[dim]SQL corrected after {retry_result.retry_count} "
            f"{'retry' if retry_result.retry_count == 1 else 'retries'}[/dim]"
        )

    sql = retry_result.sql

    # ── 4. Execute ──
    with reporter.stage("Execute SQL"):
        result = executor.execute(sql)

    # ── 5. Display trace + result ──
    if trace:
        _display_system_trace(pre=pre, retry_result=retry_result, result=result, fmt=format)

    # Avoid duplicate SQL output when trace is enabled because trace already
    # includes the final generated SQL.
    display_result(result, fmt=format, show_sql=show_sql and not trace)


def _make_stream_hooks(status_console: Console, enabled: bool):
    """Build (on_token, on_attempt) callbacks for streamed SQL generation.

    Returns (None, None) when streaming is disabled so the generator falls
    back to the non-streaming API.
    """
    if not enabled:
        return None, None

    state = {"current_attempt": 0}

    def on_attempt(n: int) -> None:
        state["current_attempt"] = n
        prefix = "" if n == 1 else "\n"
        status_console.print(
            f"{prefix}[dim]    attempt {n}:[/dim] ", end="", markup=True, highlight=False
        )

    def on_token(tok: str) -> None:
        # markup=False prevents brackets in SQL (e.g. `t.[Name]`) from being
        # interpreted as Rich markup tags.
        status_console.print(tok, end="", markup=False, highlight=False)

    return on_token, on_attempt


def _display_system_trace(
    pre: PreprocessResult,
    retry_result: RetryResult | None,
    result: ExecutionResult | None,
    fmt: str,
) -> None:
    """Display user-facing system checks and generated SQL.

    For JSON output, trace is printed to stderr so stdout remains parseable JSON.
    """
    trace_console = Console(stderr=fmt == "json")
    trace_console.print()
    trace_console.print("[bold cyan]System Checks[/bold cyan]")

    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Stage", no_wrap=True)
    table.add_column("Check")
    table.add_column("Status", no_wrap=True)
    table.add_column("Details")

    _add_preprocessor_rows(table, pre)
    _add_validation_rows(table, retry_result)
    _add_execution_rows(table, result)

    trace_console.print(table)

    if retry_result and retry_result.sql:
        syntax = Syntax(retry_result.sql, "sql", word_wrap=True)
        trace_console.print(Panel(syntax, title="Generated SQL", border_style="cyan"))


def _add_preprocessor_rows(table: Table, pre: PreprocessResult) -> None:
    """Add input preprocessing checks to the trace table."""
    if pre.is_valid:
        table.add_row(
            "Input",
            "Database scope",
            "[green]PASS[/green]",
            "Query is related to the Chinook music store database.",
        )
    else:
        table.add_row(
            "Input",
            "Database scope",
            "[yellow]REJECTED[/yellow]",
            pre.rejection_reason or "Query is outside the database scope.",
        )
        return

    if pre.has_contradiction:
        table.add_row(
            "Input",
            "Contradiction detection",
            "[yellow]WARNING[/yellow]",
            pre.contradiction_detail or "Potential contradictory conditions detected.",
        )
    else:
        table.add_row(
            "Input",
            "Contradiction detection",
            "[green]PASS[/green]",
            "No contradictory conditions detected.",
        )

    if pre.ambiguity_note:
        table.add_row(
            "Input",
            "Ambiguity detection",
            "[yellow]NOTE[/yellow]",
            pre.ambiguity_note,
        )
    else:
        table.add_row(
            "Input",
            "Ambiguity detection",
            "[green]PASS[/green]",
            "No ambiguous business term detected.",
        )

    table.add_row(
        "Input",
        "Language normalization",
        "[green]PASS[/green]",
        f"Normalized query: {pre.normalized_query}",
    )


def _add_validation_rows(table: Table, retry_result: RetryResult | None) -> None:
    """Add SQL generation and validation checks to the trace table."""
    if retry_result is None:
        table.add_row("SQL", "Generation", "[dim]SKIPPED[/dim]", "No SQL generated.")
        return

    table.add_row(
        "SQL",
        "Generation",
        "[green]PASS[/green]" if retry_result.valid else "[red]FAIL[/red]",
        f"{len(retry_result.attempts)} generation attempt(s), {retry_result.retry_count} retry attempt(s).",
    )

    for attempt in retry_result.attempts:
        if attempt["valid"]:
            table.add_row(
                "SQL",
                f"Validation attempt {attempt['attempt']}",
                "[green]PASS[/green]",
                "Forbidden pattern check and SQLite EXPLAIN QUERY PLAN passed.",
            )
        else:
            errors = "; ".join(attempt["errors"]) or "Unknown validation error."
            table.add_row(
                "SQL",
                f"Validation attempt {attempt['attempt']}",
                "[red]FAIL[/red]",
                errors,
            )


def _add_execution_rows(table: Table, result: ExecutionResult | None) -> None:
    """Add executor checks to the trace table."""
    if result is None:
        table.add_row("Execution", "SQLite execution", "[dim]SKIPPED[/dim]", "No query executed.")
        return

    if result.success:
        table.add_row(
            "Execution",
            "Executor safety check",
            "[green]PASS[/green]",
            "Forbidden pattern check passed; SQLite connection opened in read-only mode.",
        )
        table.add_row(
            "Execution",
            "SQLite execution",
            "[green]PASS[/green]",
            f"Query executed successfully; returned {result.row_count} row(s).",
        )
    else:
        table.add_row(
            "Execution",
            "SQLite execution",
            "[red]FAIL[/red]",
            result.error or "Unknown execution error.",
        )


if __name__ == "__main__":
    app()
