"""Agentic Retry Loop — feed validation errors back to LLM for self-correction."""

from typing import Callable

from src.core.sql_generator import generate_sql, generate_sql_with_retry
from src.hardening.sql_validator import SQLValidator

OnToken = Callable[[str], None]
OnAttempt = Callable[[int], None]

validator = SQLValidator()

MAX_RETRIES = 2


def generate_validated_sql(
    user_input: str,
    context: str | None = None,
    on_token: OnToken | None = None,
    on_attempt: OnAttempt | None = None,
) -> "RetryResult":
    """Generate SQL with automatic retry on validation failure.

    Flow:
        1. Generate SQL from user input
        2. Validate the SQL (syntax + safety + schema)
        3. If invalid, feed error back to LLM and retry (up to MAX_RETRIES)
        4. Return the final result with attempt history

    on_token streams text deltas (stream-then-validate). on_attempt fires
    once per attempt with the 1-based attempt number, so the UI can mark
    each retry boundary in the streamed output.
    """
    attempts: list[dict] = []

    if on_attempt:
        on_attempt(1)
    sql = generate_sql(user_input, context=context, on_token=on_token)
    result = validator.validate(sql)
    attempts.append({"attempt": 1, "sql": sql, "valid": result.valid, "errors": result.errors})

    if result.valid:
        return RetryResult(sql=sql, valid=True, attempts=attempts)

    for i in range(MAX_RETRIES):
        if on_attempt:
            on_attempt(i + 2)
        sql = generate_sql_with_retry(
            user_input=user_input,
            validation_error=result.error_summary,
            previous_sql=sql,
            context=context,
            on_token=on_token,
        )
        result = validator.validate(sql)
        attempts.append({"attempt": i + 2, "sql": sql, "valid": result.valid, "errors": result.errors})

        if result.valid:
            return RetryResult(sql=sql, valid=True, attempts=attempts)

    return RetryResult(
        sql=sql,
        valid=False,
        attempts=attempts,
        final_error=result.error_summary,
    )


class RetryResult:
    def __init__(
        self,
        sql: str,
        valid: bool,
        attempts: list[dict],
        final_error: str | None = None,
    ):
        self.sql = sql
        self.valid = valid
        self.attempts = attempts
        self.final_error = final_error
        self.retry_count = len(attempts) - 1
