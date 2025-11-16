"""Tests for analyzer classes."""

import pytest
import polars as pl
from pydeequ_polars.analyzers import (
    Size, Completeness, Uniqueness, Distinctness,
    Minimum, Maximum, Mean, Sum, StandardDeviation,
    ApproxQuantile, PatternMatch, Correlation
)


class TestSize:
    def test_size_basic(self):
        df = pl.DataFrame({"a": [1, 2, 3]})
        analyzer = Size()
        metric = analyzer.calculate(df)
        assert metric.value == 3

    def test_size_empty(self):
        df = pl.DataFrame({"a": []})
        analyzer = Size()
        metric = analyzer.calculate(df)
        assert metric.value == 0


class TestCompleteness:
    def test_completeness_full(self):
        df = pl.DataFrame({"a": [1, 2, 3]})
        analyzer = Completeness("a")
        metric = analyzer.calculate(df)
        assert metric.value == 1.0

    def test_completeness_partial(self):
        df = pl.DataFrame({"a": [1, None, 3]})
        analyzer = Completeness("a")
        metric = analyzer.calculate(df)
        assert metric.value == pytest.approx(2/3)

    def test_completeness_none(self):
        df = pl.DataFrame({"a": [None, None, None]})
        analyzer = Completeness("a")
        metric = analyzer.calculate(df)
        assert metric.value == 0.0

    def test_completeness_missing_column(self):
        df = pl.DataFrame({"a": [1, 2, 3]})
        analyzer = Completeness("b")
        metric = analyzer.calculate(df)
        assert metric.value is None


class TestUniqueness:
    def test_uniqueness_full(self):
        df = pl.DataFrame({"a": [1, 2, 3]})
        analyzer = Uniqueness("a")
        metric = analyzer.calculate(df)
        assert metric.value == 1.0

    def test_uniqueness_partial(self):
        df = pl.DataFrame({"a": [1, 2, 2, 3]})
        analyzer = Uniqueness("a")
        metric = analyzer.calculate(df)
        assert metric.value == 0.75  # 3 unique out of 4

    def test_uniqueness_duplicates(self):
        df = pl.DataFrame({"a": [1, 1, 1]})
        analyzer = Uniqueness("a")
        metric = analyzer.calculate(df)
        assert metric.value == pytest.approx(1/3)


class TestDistinctness:
    def test_distinctness_basic(self):
        df = pl.DataFrame({"a": [1, 2, 2, 3, 3, 3]})
        analyzer = Distinctness("a")
        metric = analyzer.calculate(df)
        assert metric.value == 3


class TestMinMax:
    def test_minimum(self):
        df = pl.DataFrame({"a": [5, 2, 8, 1, 9]})
        analyzer = Minimum("a")
        metric = analyzer.calculate(df)
        assert metric.value == 1

    def test_maximum(self):
        df = pl.DataFrame({"a": [5, 2, 8, 1, 9]})
        analyzer = Maximum("a")
        metric = analyzer.calculate(df)
        assert metric.value == 9


class TestMean:
    def test_mean_basic(self):
        df = pl.DataFrame({"a": [1, 2, 3, 4, 5]})
        analyzer = Mean("a")
        metric = analyzer.calculate(df)
        assert metric.value == 3.0


class TestSum:
    def test_sum_basic(self):
        df = pl.DataFrame({"a": [1, 2, 3, 4, 5]})
        analyzer = Sum("a")
        metric = analyzer.calculate(df)
        assert metric.value == 15


class TestStandardDeviation:
    def test_std_basic(self):
        df = pl.DataFrame({"a": [1, 2, 3, 4, 5]})
        analyzer = StandardDeviation("a")
        metric = analyzer.calculate(df)
        assert metric.value > 0


class TestApproxQuantile:
    def test_median(self):
        df = pl.DataFrame({"a": [1, 2, 3, 4, 5]})
        analyzer = ApproxQuantile("a", 0.5)
        metric = analyzer.calculate(df)
        assert metric.value == 3

    def test_invalid_quantile(self):
        with pytest.raises(ValueError):
            ApproxQuantile("a", 1.5)


class TestPatternMatch:
    def test_url_pattern(self):
        df = pl.DataFrame({
            "url": [
                "http://example.com",
                "https://test.com",
                "not a url",
                "http://another.com"
            ]
        })
        analyzer = PatternMatch("url", r"https?://")
        metric = analyzer.calculate(df)
        assert metric.value == 0.75  # 3 out of 4

    def test_email_pattern(self):
        df = pl.DataFrame({
            "email": [
                "user@example.com",
                "invalid",
                "another@test.com"
            ]
        })
        analyzer = PatternMatch("email", r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
        metric = analyzer.calculate(df)
        assert metric.value == pytest.approx(2/3)


class TestCorrelation:
    def test_perfect_correlation(self):
        df = pl.DataFrame({
            "a": [1, 2, 3, 4, 5],
            "b": [2, 4, 6, 8, 10]
        })
        analyzer = Correlation("a", "b")
        metric = analyzer.calculate(df)
        assert metric.value == pytest.approx(1.0)

    def test_no_correlation(self):
        df = pl.DataFrame({
            "a": [1, 2, 3, 4, 5],
            "b": [5, 4, 3, 2, 1]
        })
        analyzer = Correlation("a", "b")
        metric = analyzer.calculate(df)
        assert metric.value == pytest.approx(-1.0)
