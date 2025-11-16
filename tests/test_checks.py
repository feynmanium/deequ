"""Tests for Check class."""

import pytest
import polars as pl
from pydeequ_polars.checks.check import Check, CheckLevel


class TestCheck:
    def test_create_check(self):
        check = Check(CheckLevel.ERROR, "test check")
        assert check.level == CheckLevel.ERROR
        assert check.description == "test check"
        assert len(check.constraints) == 0

    def test_add_size_constraint(self):
        check = Check(CheckLevel.ERROR, "test")
        check.has_size(lambda x: x > 0)
        assert len(check.constraints) == 1

    def test_add_completeness_constraint(self):
        check = Check(CheckLevel.ERROR, "test")
        check.is_complete("column")
        assert len(check.constraints) == 1

    def test_add_uniqueness_constraint(self):
        check = Check(CheckLevel.ERROR, "test")
        check.is_unique("column")
        assert len(check.constraints) == 1

    def test_chaining(self):
        check = (
            Check(CheckLevel.ERROR, "test")
            .has_size(lambda x: x > 0)
            .is_complete("a")
            .is_unique("b")
        )
        assert len(check.constraints) == 3

    def test_contains_url(self):
        check = Check(CheckLevel.ERROR, "test")
        check.contains_url("url_column", lambda x: x >= 0.5)
        assert len(check.constraints) == 1

    def test_contains_email(self):
        check = Check(CheckLevel.ERROR, "test")
        check.contains_email("email_column", lambda x: x >= 0.5)
        assert len(check.constraints) == 1

    def test_is_contained_in(self):
        check = Check(CheckLevel.ERROR, "test")
        check.is_contained_in("status", ["active", "inactive"])
        assert len(check.constraints) == 1

    def test_is_non_negative(self):
        check = Check(CheckLevel.ERROR, "test")
        check.is_non_negative("value")
        assert len(check.constraints) == 1

    def test_is_positive(self):
        check = Check(CheckLevel.ERROR, "test")
        check.is_positive("value")
        assert len(check.constraints) == 1

    def test_has_mean(self):
        check = Check(CheckLevel.ERROR, "test")
        check.has_mean("value", lambda x: x > 0)
        assert len(check.constraints) == 1

    def test_has_correlation(self):
        check = Check(CheckLevel.ERROR, "test")
        check.has_correlation("a", "b", lambda x: abs(x) < 0.9)
        assert len(check.constraints) == 1
