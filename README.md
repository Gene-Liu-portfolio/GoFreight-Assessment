# NL-SQL CLI — GoFreight AI Engineer Take-Home

This project is a command-line tool that converts natural-language questions into SQL, executes the SQL against a local Chinook SQLite database, and returns the result to the user. It also includes a multi-model Text-to-SQL evaluation pipeline with manually labeled ground-truth SQL.

I used AI coding tools during implementation, but the system design, evaluation methodology, failure analysis, and trade-off decisions below are my own engineering work. The goal of this README is to explain those decisions clearly enough that the project can be reviewed without relying on any separate design notes.

---

## 1. What Is Included

| Area | Files |
|---|---|
| CLI application | `src/cli.py` |
| SQL generation | `src/core/sql_generator.py`, `prompts/system_prompt.py` |
| SQLite execution | `src/core/executor.py`, `src/db/chinook.db`, `src/db/schema.py` |
| Hardening layer | `src/hardening/preprocessor.py`, `src/hardening/sql_validator.py`, `src/hardening/retry.py` |
| Evaluation dataset | `eval/test_cases/cases.json`, `eval/test_cases/holdout.json` |
| Evaluation pipeline | `eval/pipeline.py`, `eval/model_runner.py`, `eval/scorer.py` |
| Saved eval results | `eval/results/run_20260424_223115.json` and prior iteration results |
| Tests | `tests/test_extract_sql.py`, `tests/test_validator.py`, `tests/test_scorer.py`, `tests/test_integration.py` |

The project satisfies the assignment requirements:

- A CLI accepts natural-language input.
- The system generates a structured SQL query.
- The query is executed against a real database.
- The result is returned to the user.
- Ambiguous, conflicting, typo-heavy, non-English, injection, and out-of-scope cases were tested.
- Critical failures were hardened through a layered architecture.
- 30 challenging eval cases (including 5 adversarial) have manually written ground-truth SQL.
- Three models are evaluated: Claude Sonnet 4.6, GPT-4o, and DeepSeek-V3.
- All three models exceed the required 85% accuracy threshold on the main eval set.

---

## 2. Quick Start

```bash
cd /path/to/nl-sql-cli

uv sync
uv sync --extra eval --extra dev

cp .env.example .env
# Fill in:
# ANTHROPIC_API_KEY=...
# OPENAI_API_KEY=...
# DEEPSEEK_API_KEY=...
```

Run the CLI:

```bash
python -m src.cli "Show me the top 5 artists by revenue"
python -m src.cli "Which country has the most customers?"
python -m src.cli "哪個國家的客戶花最多錢？"
```

By default, the CLI prints:

1. per-stage timing lines such as `+0.0s ▸ Analyze query (Haiku)` and `✓ Execute SQL (0.0s)`,
2. streamed SQL tokens while the model is generating,
3. the system checks that passed,
4. the final generated SQL,
5. the query result.

For cleaner output:

```bash
python -m src.cli --no-trace "Show me the top 5 artists by revenue"
python -m src.cli --no-stream "Show me the top 5 artists by revenue"
python -m src.cli --format json --no-trace "List all genres"
```

Run tests:

```bash
python -m pytest tests/ -q
```

Run evaluation:

```bash
python -m eval.pipeline
python -m eval.pipeline --models claude-sonnet-4.6
python -m eval.pipeline --cases holdout.json
```

The eval commands call external model APIs and may incur cost.

---

## 3. Project Structure

```text
src/
  cli.py                    # Typer CLI, Rich output, stage timing, streamed SQL tokens
  shared.py                 # shared forbidden SQL patterns and SQL extraction
  core/
    sql_generator.py        # natural language -> SQL using Claude Sonnet; streaming/non-streaming paths
    executor.py             # read-only SQLite execution
    formatter.py            # table / JSON rendering
  db/
    chinook.db              # local SQLite database
    schema.py               # schema introspection for prompt context
  hardening/
    preprocessor.py         # Haiku intent classification, OOS rejection, contradiction detection
    sql_validator.py        # SQL allowlist, denylist, syntax/schema validation
    retry.py                # validation-error feedback loop with token/attempt callbacks

prompts/
  system_prompt.py          # shared Text-to-SQL system prompt

eval/
  test_cases/
    cases.json              # 30-case development eval set
    holdout.json            # 10-case held-out set for overfitting checks
  model_runner.py           # model configuration and provider dispatch
  scorer.py                 # execution-accuracy scorer
  pipeline.py               # multi-model eval runner
  results/                  # saved eval outputs

tests/
  test_extract_sql.py
  test_validator.py
  test_scorer.py
  test_integration.py
```

---

## 4. Dataset Choice

I chose the Chinook SQLite database instead of a live public API because the second part of the assignment depends on repeatable evaluation.

| Option | Trade-off |
|---|---|
| Public API | More realistic integration surface, but data can change, rate limits can interfere, and ground truth can become stale. |
| Local SQLite database | Less operationally complex, but fully deterministic and well-suited for repeated eval runs. |

For this assignment, determinism mattered more. If the underlying data changes while I am iterating on prompts or scorers, I cannot tell whether an accuracy change came from the model, the prompt, the scorer, or the data source. A fixed database removes that ambiguity.

Chinook is also complex enough to test real Text-to-SQL behavior:

- multi-table joins,
- customer and invoice analysis,
- artist / album / track relationships,
- aggregation,
- subqueries,
- window functions,
- NULL handling,
- date filtering.

Key tables:

| Table | Rows | Role |
|---|---:|---|
| `Artist` | 275 | music artists |
| `Album` | 347 | albums |
| `Track` | 3,503 | tracks |
| `Genre` | 25 | music genres |
| `Customer` | 59 | customers |
| `Employee` | 8 | support reps and managers |
| `Invoice` | 412 | customer purchases |
| `InvoiceLine` | 2,240 | purchased tracks |
| `PlaylistTrack` | 8,715 | playlist-track bridge table |

Important domain facts:

- Invoice dates range from `2021-01-01` to `2025-12-22`.
- Revenue must be calculated as `SUM(InvoiceLine.UnitPrice * InvoiceLine.Quantity)`.
- The database does not contain ratings, play counts, release dates, streaming platform data, churn labels, VIP labels, or continent mappings.

---

## 5. CLI Architecture

The hardened CLI uses this flow:

```text
User question
  -> InputPreprocessor (Claude Haiku)
  -> SQL generator (Claude Sonnet; streamed when enabled)
  -> SQLValidator
  -> retry loop if validation fails
  -> SQLExecutor
  -> table / JSON output
```

### 5.1 InputPreprocessor

The preprocessor classifies the user request before SQL generation. It handles:

- out-of-scope requests,
- questions about data that is not in the database,
- contradictory constraints,
- ambiguous terms,
- non-English normalization.

Examples:

| Input | Behavior |
|---|---|
| `What's the weather in Tokyo?` | rejected as out-of-scope |
| `Compare Spotify and Apple Music revenue` | rejected because platform revenue is not in the database |
| `Tracks longer than 5 minutes but shorter than 2 minutes` | flagged as contradictory |
| `哪個國家的客戶花最多錢？` | normalized to an English query before SQL generation |

The preprocessor uses `claude-haiku-4-5-20251001`. I originally used the same Sonnet model for both preprocessing and SQL generation, but classification does not need Sonnet-level SQL reasoning. Moving this stage to Haiku keeps the safety layer while reducing the cost and latency of normal queries.

A normal query still makes two LLM calls: Haiku for preprocessing and Sonnet for SQL generation. I accepted that cost because the preprocessor catches failures that post-SQL validation cannot catch, especially plausible but fabricated queries about missing data.

### 5.2 Prompt Grounding

The SQL generator uses a shared system prompt in `prompts/system_prompt.py`. The prompt includes:

- the SQLite schema,
- sample rows from each table,
- database-specific constraints,
- date-range guidance,
- rules for safe SQL shape,
- rules for common Text-to-SQL mistakes.

The prompt explicitly tells the model to avoid common errors:

- use explicit `JOIN ... ON`,
- qualify ambiguous columns,
- use `IS NULL` for NULL values,
- use `>=` and `<` for date ranges instead of `BETWEEN`,
- use `ROW_NUMBER()` for top-N-per-group queries,
- calculate revenue with `UnitPrice * Quantity`,
- preserve the aggregation grain requested by the user.

Sample rows are included because schema alone is not enough. For example, the schema says `Country`, but sample values help the model learn that the database stores `USA`, `Germany`, and `Brazil`, not every possible localized country spelling.

The generator supports a stream-then-validate path. When the CLI passes an `on_token` callback, `src/core/sql_generator.py` uses Anthropic streaming and forwards text deltas to the CLI. The full response is still accumulated, cleaned with `extract_sql()`, validated, and only then executed. Streaming improves perceived latency, but it does not bypass validation.

### 5.3 Validator + Retry + Executor

The third layer is the final safety gate before the database is touched.

`SQLValidator` checks:

- the statement starts with `SELECT` or `WITH`,
- dangerous patterns such as `DROP`, `DELETE`, `UPDATE`, `INSERT`, `CREATE`, `ALTER`, `ATTACH`, `DETACH`, and `PRAGMA` are absent,
- multiple statements are rejected,
- SQLite can build a query plan with `EXPLAIN QUERY PLAN`.

If validation fails, the retry loop sends the previous SQL and the validation error back to the model. This is more useful than blind resampling because the model receives concrete feedback such as "no such column" or "ambiguous column name".

The retry layer also propagates two callbacks:

- `on_attempt`: lets the CLI mark attempt boundaries, including retries.
- `on_token`: streams generated SQL tokens for each attempt.

`SQLExecutor` repeats the safety check independently and opens the database in read-only mode. This is deliberate defense in depth: the executor should not blindly trust that validation already happened correctly.

---

## 6. Break-It Findings and Hardening Decisions

The original baseline flow was intentionally simple:

```text
User question
  -> SQL generator
  -> SQLExecutor
  -> table / JSON output
```

That baseline was useful because it proved the end-to-end path first: natural language could be converted into SQL, executed against Chinook, and returned to the user. It also made the main risks easier to see. The model could generate plausible SQL for requests that were out-of-scope, ambiguous, contradictory, or based on data that did not exist in the database.

After break-it testing, I added three hardening layers:

1. **Pre-generation checks** to reject out-of-scope requests, flag contradictory constraints, and normalize non-English queries before SQL generation.
2. **Prompt grounding** to constrain the SQL generator with the real schema, sample rows, database-specific rules, and known missing fields.
3. **Post-generation validation** to reject unsafe or invalid SQL, retry with concrete validation feedback, and execute only through a read-only database path.

The hardened flow was therefore a response to observed failures, not a layer of generic complexity added upfront.

I tested the baseline with 27 adversarial cases across nine categories:

| Category | What it tested |
|---|---|
| semantic ambiguity | terms like `best`, `popular`, `senior` |
| conflicting constraints | logically impossible filters |
| typos / malformed phrasing | misspellings and incomplete questions |
| non-English queries | Chinese, Japanese, German, French |
| complex subqueries | multi-hop joins, self-joins, nested aggregation |
| schema hallucination | fields such as rating, play count, release date |
| injection attacks | SQL injection and prompt injection |
| out-of-scope requests | weather, jokes, external platform data |
| temporal references | last month, recently, past year |

The two most serious baseline failures were:

1. **Fabricated external-platform analysis**  
   A query about Spotify vs. Apple Music revenue produced plausible-looking SQL even though the database has no platform dimension.

2. **Silently dropped contradictory constraints**  
   A query asking for tracks longer than five minutes and shorter than two minutes resulted in one condition being ignored.

Those failures drove the final architecture. The preprocessor catches out-of-scope and contradictory requests before SQL generation. The prompt reduces schema hallucination. The validator and executor prevent invalid or dangerous SQL from running.

After hardening, the same 27 cases resulted in:

```text
executed_ok  : 18
rejected_oos :  9
fail_*       :  0
```

---

## 7. Remaining Hard Problems

Some failures are not just prompt problems. They require additional product or data design.

### 7.1 Business Definitions

Example: `Which customers have churned?`

The database does not define churn. The model could invent a definition such as "no purchase in 12 months", but that may be wrong for the business. The correct production solution is a metrics catalog or a clarification flow.

### 7.2 External Taxonomies

Example: `Show me all European customers.`

The database has `Country`, but no `Continent` table. The model may list European countries from world knowledge and miss edge cases. The production solution is a reference dimension table, not a stronger prompt.

### 7.3 Statistical Reasoning

Example: `Is there a correlation between track length and sales?`

SQL can produce descriptive summaries, but correlation or causal analysis requires a statistical method and clear assumptions. A production assistant should route this to an analysis tool or state the limitation.

---

## 8. Evaluation Design

### 8.1 Test Sets

There are two evaluation datasets:

| File | Size | Purpose |
|---|---:|---|
| `eval/test_cases/cases.json` | 30 | main development and reporting set |
| `eval/test_cases/holdout.json` | 10 | held-out set for checking prompt overfitting |

The main set has six categories:

| Category | Count | Purpose |
|---|---:|---|
| `tricky_filtering` | 5 | NULLs, dates, units, LIKE filters |
| `multi_hop_join` | 5 | 4-5 table joins and self-joins |
| `advanced_aggregation` | 5 | HAVING, percentages, aggregate-of-aggregate |
| `advanced_sql` | 5 | windows, correlated subqueries, relational division |
| `messy_adversarial` | 5 | typos, informal phrasing, negation |
| `non_english_complex` | 5 | non-English prompts with complex SQL |

The holdout set uses different entities, phrasings, and thresholds from the main set. I added it because the main set was used for failure analysis and prompt iteration. Without a held-out set, a high final score could simply mean that the prompt became tuned to the development cases.

### 8.2 Model Selection

I selected three models that are close enough in coding and structured-output capability to make the comparison meaningful:

| Model | Type | How it is used |
|---|---|---|
| Claude Sonnet 4.6 | closed-source | main CLI model and eval model |
| GPT-4o | closed-source | closed-source baseline |
| DeepSeek-V3 | open-weight model family via hosted API | cost / latency comparison |

I avoided comparing models from obviously different tiers because that would mostly show that a stronger model is stronger. The useful question is: when models are in a similar capability range, what failure modes do they have on this specific structured-output task?

Model settings are centralized in `eval/model_runner.py`:

- provider,
- model id,
- temperature,
- max tokens,
- base URL for OpenAI-compatible providers.

This makes the eval easier to reproduce and easier to extend.

### 8.3 Scoring Method

The scorer uses execution accuracy, not SQL string matching.

The pipeline executes:

```text
predicted SQL
ground-truth SQL
```

against the same database, then compares the result sets.

This is necessary because equivalent SQL can look very different:

- join order can differ,
- aliases can differ,
- CTEs and subqueries can be interchangeable,
- models may return extra context columns,
- names can be returned as `FirstName, LastName` or concatenated as `FullName`.

The scorer evolved in three stages:

| Version | Change | Why |
|---|---|---|
| v1 | exact tuple-set match | too strict; many correct SQLs were marked wrong |
| v2 | value-only and subset-column matching | handled aliases, column order, and extra columns |
| v3 | numeric tolerance, LIMIT stripping, name splitting, key overlap | handled common result-format differences without accepting unrelated results |

I treated scorer looseness as a real risk. To reduce false positives, I reviewed the pass cases that used looser strategies and checked that they were semantically equivalent rather than merely overlapping by accident. I also checked the key-overlap threshold: raising the threshold from 40% to 60% did not change the final result because the overlap cases were effectively full matches.

---

## 9. Evaluation Results

Final main-set results:

| Model | Correct / Total | Accuracy | Avg Latency |
|---|---:|---:|---:|
| Claude Sonnet 4.6 | 30 / 30 | 100.0% | 7.93s |
| GPT-4o | 29 / 30 | 96.7% | 6.42s |
| DeepSeek-V3 | 29 / 30 | 96.7% | 1.89s |

Per-category results:

| Category | Claude | GPT-4o | DeepSeek |
|---|---:|---:|---:|
| tricky_filtering | 5/5 | 5/5 | 5/5 |
| multi_hop_join | 5/5 | 5/5 | 5/5 |
| advanced_aggregation | 5/5 | 5/5 | 5/5 |
| advanced_sql | 5/5 | 5/5 | 5/5 |
| messy_adversarial | 5/5 | 4/5 | 4/5 |
| non_english_complex | 5/5 | 5/5 | 5/5 |

Iteration history:

| Stage | Claude | GPT-4o | DeepSeek | Main change |
|---|---:|---:|---:|---|
| v1 scorer | 26.7% | 26.7% | 23.3% | strict exact matching |
| v2 scorer | 70.0% | 33.3% | 36.7% | value/subset matching |
| v3 scorer + GT fixes | 90.0% | 73.3% | 80.0% | numeric tolerance, LIMIT handling, ground-truth cleanup |
| prompt v1 | 93.3% | 96.7% | 93.3% | shared prompt baseline |
| prompt v2 | 100.0% | 96.7% | 96.7% | rules from observed model failures |

Common initial mistakes:

- Claude used `RANK()` where `ROW_NUMBER()` was needed for exactly N rows per group.
- Claude sometimes treated sales as quantity instead of revenue.
- GPT-4o used `BETWEEN` for date ranges, which can be wrong with timestamp boundaries.
- DeepSeek-V3 sometimes aggregated at a finer grain than the question requested.

The final prompt rules were added because of these concrete failures, not as generic prompt tuning.

---

## 10. Tests

```bash
python -m pytest tests/ -q
```

Current result:

```text
69 passed
```

Test coverage:

| Test file | What it covers |
|---|---|
| `tests/test_extract_sql.py` | markdown fences, semicolons, comments, multiline SQL |
| `tests/test_validator.py` | DDL/DML rejection, multi-statement rejection, `PRAGMA`, `SELECT`/`WITH` allowlist, syntax errors |
| `tests/test_scorer.py` | exact match, value match, numeric tolerance, subset columns, LIMIT stripping, key overlap |
| `tests/test_integration.py` | mocked LLM calls with real validator, retry loop, executor, and Chinook DB |

The integration tests intentionally mock the model calls. They are not trying to test whether the LLM is smart. They test whether the system wiring is correct:

```text
preprocessor -> generator -> validator -> retry -> executor
```

---

## 11. Production Considerations

This is a take-home prototype, not a production service. The most important production changes would be:

| Area | Prototype | Production direction |
|---|---|---|
| Latency | usually one Haiku call and one Sonnet call; SQL generation streams tokens | rule-first preprocessor, caching, query plan cost guard |
| Database | local SQLite file | PostgreSQL or warehouse read replica |
| SQL safety | regex + `SELECT`/`WITH` allowlist + read-only DB | SQL AST parser, schema allowlist, query cost guard, statement timeout |
| Authorization | no user permission layer | table / row-level permissions |
| Observability | CLI trace | structured logs, trace IDs, latency, token usage, model version |
| Business metrics | model may infer definitions | metrics catalog and clarification flow |
| Evaluation | offline dev + holdout sets | CI regression suite plus production query sampling |

The highest-risk failure mode in NL-to-SQL is not a crash. It is a plausible-looking but semantically wrong answer. Most of the architecture is designed around reducing that risk.

---

## 12. Notes on AI-Assisted Development

AI tools helped accelerate implementation, especially for boilerplate code, tests, and refactoring. I did not treat generated code as correct by default.

The engineering decisions I made and validated were:

- choosing a deterministic local database over a live API,
- using a three-layer defense instead of a single validator,
- adding both an allowlist and denylist for SQL safety,
- fixing the scorer before changing the prompt,
- using similarly capable models so failure modes could be compared fairly,
- adding a holdout set to check prompt overfitting,
- centralizing model settings for reproducibility,
- moving preprocessing to a lighter model while keeping Sonnet for SQL generation,
- adding stream-then-validate so the CLI feels responsive without weakening safety,
- adding integration tests that mock model calls but exercise the real database path.

Those decisions are the core of the project. The implementation supports them.

---

## 13. Environment

- Python `>=3.11`
- `uv`
- Anthropic API key for CLI and Claude eval
- OpenAI API key for GPT-4o eval
- DeepSeek API key for DeepSeek-V3 eval

Do not commit `.env`, `.venv/`, `__pycache__/`, `.pytest_cache/`, or `.DS_Store`.
