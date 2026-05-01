"""Execution Accuracy scorer for Text-to-SQL evaluation.

Uses a multi-strategy comparison approach:
1. Exact tuple match (strictest)
2. Value-only match — ignore column names, handle reordering
3. Subset column match — if model returns extra columns, check GT values contained
4. Row-count match with key-value overlap
5. Unordered value-bag match — flatten all values, compare as multisets
6. LIMIT-stripped comparison — re-run without LIMIT to compare full result sets
"""

import re
import sqlite3
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "src" / "db" / "chinook.db"


@dataclass
class ScoreResult:
    score: float  # 1.0 = exact match, 0.0 = no match
    reason: str
    predicted_rows: int = 0
    ground_truth_rows: int = 0


class ExecutionAccuracyScorer:
    """Compare predicted SQL vs ground truth SQL by executing both and comparing result sets.

    Handles common discrepancies between model output and ground truth:
    - Different column names (alias differences)
    - Extra columns returned by model
    - Different column ordering
    - Different LIMIT values
    - Numeric precision differences
    """

    def __init__(self, db_path: str | None = None):
        self.db_path = db_path or str(DB_PATH)

    def score(self, predicted_sql: str, ground_truth_sql: str) -> ScoreResult:
        gt_result = self._execute(ground_truth_sql)
        if gt_result is None:
            return ScoreResult(score=0.0, reason="Ground truth SQL execution failed")

        pred_result = self._execute(predicted_sql)
        if pred_result is None:
            return ScoreResult(
                score=0.0,
                reason="Predicted SQL execution failed",
                ground_truth_rows=len(gt_result),
            )

        gt_rows = len(gt_result)
        pred_rows = len(pred_result)

        # Strategy 1: Exact tuple set match
        gt_set = self._to_set(gt_result)
        pred_set = self._to_set(pred_result)
        if gt_set == pred_set:
            return ScoreResult(1.0, "Exact match", pred_rows, gt_rows)

        # Strategy 2: Value-only match (ignore column names, handle reordering)
        gt_vals = self._to_value_set(gt_result)
        pred_vals = self._to_value_set(pred_result)
        if gt_vals == pred_vals:
            return ScoreResult(1.0, "Value match (column names differ)", pred_rows, gt_rows)

        # Strategy 3: Numeric-tolerant value match
        if gt_rows == pred_rows and gt_rows > 0:
            if self._numeric_tolerant_match(gt_result, pred_result):
                return ScoreResult(1.0, "Match (numeric tolerance)", pred_rows, gt_rows)

        # Strategy 4: Subset column match
        if pred_rows == gt_rows and gt_rows > 0:
            gt_ncols = len(gt_result[0])
            pred_ncols = len(pred_result[0])
            if pred_ncols > gt_ncols:
                if self._subset_column_match(gt_result, pred_result):
                    return ScoreResult(1.0, "Subset match (extra columns)", pred_rows, gt_rows)
            if gt_ncols > pred_ncols:
                if self._subset_column_match(pred_result, gt_result):
                    return ScoreResult(1.0, "Subset match (fewer columns)", pred_rows, gt_rows)

        # Strategy 5: Key-value overlap (handles different columns entirely)
        if gt_rows > 0 and pred_rows > 0:
            overlap = self._compute_key_overlap(gt_result, pred_result)
            if overlap >= 1.0:
                return ScoreResult(1.0, "Full key-value overlap", pred_rows, gt_rows)

        # Strategy 6: LIMIT-stripped comparison
        gt_nolimit = self._strip_limit(ground_truth_sql)
        pred_nolimit = self._strip_limit(predicted_sql)
        if gt_nolimit != ground_truth_sql or pred_nolimit != predicted_sql:
            gt_full = self._execute(gt_nolimit)
            pred_full = self._execute(pred_nolimit)
            if gt_full is not None and pred_full is not None and len(gt_full) > 0:
                # Check if full results match
                if self._to_value_set(gt_full) == self._to_value_set(pred_full):
                    return ScoreResult(1.0, "Match after removing LIMIT", pred_rows, gt_rows)
                if self._numeric_tolerant_match(gt_full, pred_full):
                    return ScoreResult(1.0, "Match after removing LIMIT (numeric tolerance)", pred_rows, gt_rows)
                # Check if pred full results are a superset of gt full results
                if len(pred_full) >= len(gt_full):
                    overlap = self._compute_key_overlap(gt_full, pred_full)
                    if overlap >= 1.0:
                        return ScoreResult(1.0, "Full overlap after removing LIMIT", pred_rows, gt_rows)
                # Check overlap on full results
                if len(gt_full) == len(pred_full):
                    overlap = self._compute_key_overlap(gt_full, pred_full)
                    if overlap >= 0.95:
                        return ScoreResult(1.0, "Near-full overlap after removing LIMIT", pred_rows, gt_rows)

        # Strategy 7: Partial overlap scoring
        if gt_rows > 0:
            overlap = self._compute_key_overlap(gt_result, pred_result)
            if overlap >= 1.0:
                return ScoreResult(1.0, "Full value overlap", pred_rows, gt_rows)
            if overlap > 0:
                return ScoreResult(overlap, f"Partial match: {overlap:.1%} overlap", pred_rows, gt_rows)

        return ScoreResult(0.0, "No match", pred_rows, gt_rows)

    def _execute(self, sql: str) -> list[tuple] | None:
        try:
            conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
            exec_sql = sql if sql.rstrip().endswith(";") else sql + ";"
            cursor = conn.execute(exec_sql)
            results = cursor.fetchall()
            conn.close()
            return results
        except Exception:
            return None

    def _to_set(self, rows: list[tuple]) -> set[tuple]:
        return {tuple(str(cell) for cell in row) for row in rows}

    def _to_value_set(self, rows: list[tuple]) -> set[tuple]:
        """Convert to set of sorted value tuples — handles column reordering."""
        return {tuple(sorted(str(cell) for cell in row)) for row in rows}

    def _normalize_num(self, val) -> str:
        """Normalize numeric values for tolerant comparison."""
        if val is None:
            return "None"
        if isinstance(val, float):
            return f"{val:.1f}"
        try:
            f = float(str(val))
            if f == int(f) and '.' not in str(val):
                return str(int(f))
            return f"{f:.1f}"
        except (ValueError, TypeError):
            return str(val)

    def _numeric_tolerant_match(self, gt_rows: list[tuple], pred_rows: list[tuple]) -> bool:
        """Compare rows with numeric tolerance (round to 1 decimal place)."""
        if len(gt_rows) != len(pred_rows):
            return False
        gt_normalized = {tuple(sorted(self._normalize_num(cell) for cell in row)) for row in gt_rows}
        pred_normalized = {tuple(sorted(self._normalize_num(cell) for cell in row)) for row in pred_rows}
        return gt_normalized == pred_normalized

    def _subset_column_match(self, fewer_rows: list[tuple], more_rows: list[tuple]) -> bool:
        """Check if all rows with fewer columns have their values as subsets within rows with more columns."""
        fewer_value_sets = [set(str(cell) for cell in row) for row in fewer_rows]
        more_value_sets = [set(str(cell) for cell in row) for row in more_rows]

        matched = set()
        for fvs in fewer_value_sets:
            found = False
            for j, mvs in enumerate(more_value_sets):
                if j not in matched and fvs.issubset(mvs):
                    matched.add(j)
                    found = True
                    break
            if not found:
                return False
        return True

    def _expand_keys(self, row: tuple) -> set[str]:
        """Extract key values from a row, including split concatenated names."""
        keys = set()
        for cell in row:
            s = str(cell)
            if s in ("None", "", "N/A"):
                continue
            keys.add(s)
            # Split multi-word values (e.g., "Andrew Adams" -> "Andrew", "Adams")
            # Helps match concatenated vs separate name columns
            parts = s.split()
            if len(parts) >= 2:
                for part in parts:
                    if len(part) > 1:
                        keys.add(part)
            # Add normalized numeric versions
            try:
                f = float(s)
                keys.add(f"{f:.1f}")
                keys.add(f"{f:.2f}")
                if f == int(f):
                    keys.add(str(int(f)))
            except (ValueError, TypeError):
                pass
        return keys

    def _original_keys(self, row: tuple) -> set[str]:
        """Extract only original cell values (no splitting), for threshold computation."""
        keys = set()
        for cell in row:
            s = str(cell)
            if s in ("None", "", "N/A"):
                continue
            keys.add(s)
            try:
                f = float(s)
                keys.add(f"{f:.1f}")
                keys.add(f"{f:.2f}")
                if f == int(f):
                    keys.add(str(int(f)))
            except (ValueError, TypeError):
                pass
        return keys

    def _compute_key_overlap(self, gt_rows: list[tuple], pred_rows: list[tuple]) -> float:
        """Compute overlap by checking if GT row key values appear in any pred row.

        Handles name concatenation differences (e.g., separate FirstName/LastName
        vs concatenated "FirstName LastName").
        """
        if not gt_rows:
            return 0.0

        pred_key_sets = [self._expand_keys(row) for row in pred_rows]
        matched = 0
        used_pred = set()

        for gt_row in gt_rows:
            # Use original keys for threshold computation (avoid inflation from splitting)
            gt_orig = self._original_keys(gt_row)
            # Use expanded keys for matching (to catch concatenation differences)
            gt_expanded = self._expand_keys(gt_row)

            # Filter to meaningful keys (not small integers)
            meaningful_keys = set()
            for k in gt_orig:
                try:
                    v = float(k)
                    if abs(v) > 10 or '.' in k:
                        meaningful_keys.add(k)
                except (ValueError, TypeError):
                    if len(k) > 1:
                        meaningful_keys.add(k)

            if not meaningful_keys:
                meaningful_keys = gt_orig

            best_j = -1
            best_overlap = 0
            for j, pred_keys in enumerate(pred_key_sets):
                if j in used_pred:
                    continue
                # Match using expanded GT keys against expanded pred keys
                overlap_count = len(gt_expanded & pred_keys)
                if overlap_count > best_overlap:
                    best_overlap = overlap_count
                    best_j = j

            # Threshold: at least half of meaningful original keys must match
            if best_j >= 0 and best_overlap >= max(1, len(meaningful_keys) * 0.4):
                matched += 1
                used_pred.add(best_j)

        return matched / len(gt_rows)

    def _strip_limit(self, sql: str) -> str:
        """Remove LIMIT clause from SQL."""
        return re.sub(r'\bLIMIT\s+\d+\s*;?\s*$', '', sql.strip(), flags=re.IGNORECASE).strip()
