"""Unit tests for ExecutionAccuracyScorer — testing each comparison strategy."""

import pytest

from eval.scorer import ExecutionAccuracyScorer

scorer = ExecutionAccuracyScorer()


class TestStrategy1ExactMatch:
    """Strategy 1: Exact tuple set match."""

    def test_identical_queries(self):
        sql = "SELECT COUNT(*) FROM Track"
        result = scorer.score(sql, sql)
        assert result.score == 1.0
        assert "Exact match" in result.reason

    def test_same_results_different_sql(self):
        gt = "SELECT COUNT(*) AS cnt FROM Track"
        pred = "SELECT COUNT(*) AS total FROM Track"
        result = scorer.score(pred, gt)
        assert result.score == 1.0  # same value, different alias


class TestStrategy2ValueMatch:
    """Strategy 2: Value-only match (ignore column names)."""

    def test_different_aliases(self):
        gt = "SELECT Name AS track_name FROM Genre ORDER BY Name LIMIT 3"
        pred = "SELECT Name AS genre FROM Genre ORDER BY Name LIMIT 3"
        result = scorer.score(pred, gt)
        assert result.score == 1.0

    def test_different_column_order(self):
        gt = "SELECT GenreId, Name FROM Genre WHERE GenreId = 1"
        pred = "SELECT Name, GenreId FROM Genre WHERE GenreId = 1"
        result = scorer.score(pred, gt)
        assert result.score == 1.0


class TestStrategy3NumericTolerance:
    """Strategy 3: Numeric-tolerant value match."""

    def test_float_precision_difference(self):
        gt = "SELECT ROUND(AVG(Total), 2) AS avg FROM Invoice"
        pred = "SELECT AVG(Total) AS avg FROM Invoice"
        result = scorer.score(pred, gt)
        # Should pass with numeric tolerance (round to 1 decimal)
        assert result.score == 1.0

    def test_integer_vs_float(self):
        gt = "SELECT COUNT(*) FROM Track"
        pred = "SELECT CAST(COUNT(*) AS REAL) FROM Track"
        result = scorer.score(pred, gt)
        assert result.score == 1.0


class TestStrategy4SubsetColumn:
    """Strategy 4: Subset column match (model returns extra columns)."""

    def test_extra_columns_in_prediction(self):
        gt = "SELECT Name FROM Genre WHERE GenreId = 1"
        pred = "SELECT GenreId, Name FROM Genre WHERE GenreId = 1"
        result = scorer.score(pred, gt)
        assert result.score == 1.0

    def test_fewer_columns_in_prediction(self):
        gt = "SELECT GenreId, Name FROM Genre WHERE GenreId = 1"
        pred = "SELECT Name FROM Genre WHERE GenreId = 1"
        result = scorer.score(pred, gt)
        assert result.score == 1.0


class TestStrategy6LimitStripped:
    """Strategy 6: LIMIT-stripped comparison."""

    def test_different_limit_values(self):
        gt = "SELECT Name FROM Genre ORDER BY Name LIMIT 5"
        pred = "SELECT Name FROM Genre ORDER BY Name LIMIT 10"
        result = scorer.score(pred, gt)
        # After stripping LIMIT, full results should match
        assert result.score == 1.0

    def test_with_vs_without_limit(self):
        gt = "SELECT Name FROM Genre ORDER BY Name"
        pred = "SELECT Name FROM Genre ORDER BY Name LIMIT 20"
        result = scorer.score(pred, gt)
        assert result.score == 1.0


class TestExecutionFailure:
    """Handle SQL execution failures gracefully."""

    def test_invalid_predicted_sql(self):
        gt = "SELECT COUNT(*) FROM Track"
        pred = "SELECT * FROM NonExistentTable"
        result = scorer.score(pred, gt)
        assert result.score == 0.0
        assert "execution failed" in result.reason.lower()

    def test_invalid_ground_truth_sql(self):
        gt = "SELECT * FROM NonExistentTable"
        pred = "SELECT COUNT(*) FROM Track"
        result = scorer.score(pred, gt)
        assert result.score == 0.0
        assert "ground truth" in result.reason.lower()


class TestNoMatch:
    """Completely different results should score 0."""

    def test_different_tables(self):
        gt = "SELECT COUNT(*) FROM Track"
        pred = "SELECT COUNT(*) FROM Artist"
        result = scorer.score(pred, gt)
        assert result.score == 0.0


class TestKeyOverlap:
    """Strategy 5/7: Key-value overlap matching."""

    def test_name_concatenation_vs_separate(self):
        # Model returns concatenated name, GT returns separate
        gt = "SELECT FirstName, LastName FROM Customer WHERE CustomerId = 1"
        pred = "SELECT FirstName || ' ' || LastName AS name FROM Customer WHERE CustomerId = 1"
        result = scorer.score(pred, gt)
        assert result.score == 1.0

    def test_partial_overlap_below_threshold(self):
        # Use completely different data to ensure no overlap
        gt = "SELECT Name FROM Genre WHERE GenreId <= 3 ORDER BY Name"
        pred = "SELECT Name FROM Genre WHERE GenreId > 22 ORDER BY Name"
        result = scorer.score(pred, gt)
        assert result.score < 1.0


class TestHelperMethods:
    """Test internal helper methods directly."""

    def test_strip_limit(self):
        assert scorer._strip_limit("SELECT * FROM Track LIMIT 5") == "SELECT * FROM Track"
        assert scorer._strip_limit("SELECT * FROM Track LIMIT 100;") == "SELECT * FROM Track"
        assert scorer._strip_limit("SELECT * FROM Track") == "SELECT * FROM Track"

    def test_normalize_num_float(self):
        assert scorer._normalize_num(3.14159) == "3.1"
        assert scorer._normalize_num(10.0) == "10.0"

    def test_normalize_num_int(self):
        assert scorer._normalize_num(42) == "42"

    def test_normalize_num_none(self):
        assert scorer._normalize_num(None) == "None"

    def test_normalize_num_string(self):
        assert scorer._normalize_num("hello") == "hello"

    def test_expand_keys_splits_names(self):
        keys = scorer._expand_keys(("John Smith", 100))
        assert "John Smith" in keys
        assert "John" in keys
        assert "Smith" in keys
        assert "100" in keys

    def test_expand_keys_numeric_variants(self):
        keys = scorer._expand_keys((88.12,))
        assert "88.1" in keys
        assert "88.12" in keys
        # 88.12 != int(88.12) so no integer form added
        assert "88" not in keys

    def test_expand_keys_integer_float(self):
        keys = scorer._expand_keys((100.0,))
        assert "100" in keys
        assert "100.0" in keys
