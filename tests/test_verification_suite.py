"""Tests for VerificationSuite."""

import pytest
import polars as pl
from pydeequ_polars import VerificationSuite, Check, CheckLevel, CheckStatus
from pydeequ_polars.constraints.constraint import ConstraintStatus


class TestVerificationSuite:
    def test_basic_verification_success(self):
        df = pl.DataFrame({"a": [1, 2, 3]})

        result = (
            VerificationSuite()
            .on_data(df)
            .add_check(
                Check(CheckLevel.ERROR, "test")
                .has_size(lambda x: x == 3)
            )
            .run()
        )

        assert result.status == CheckStatus.SUCCESS

    def test_basic_verification_failure(self):
        df = pl.DataFrame({"a": [1, 2, 3]})

        result = (
            VerificationSuite()
            .on_data(df)
            .add_check(
                Check(CheckLevel.ERROR, "test")
                .has_size(lambda x: x == 5)
            )
            .run()
        )

        assert result.status == CheckStatus.ERROR

    def test_completeness_check(self):
        df = pl.DataFrame({"a": [1, None, 3]})

        result = (
            VerificationSuite()
            .on_data(df)
            .add_check(
                Check(CheckLevel.ERROR, "test")
                .is_complete("a")
            )
            .run()
        )

        assert result.status == CheckStatus.ERROR

    def test_uniqueness_check(self):
        df = pl.DataFrame({"a": [1, 2, 2, 3]})

        result = (
            VerificationSuite()
            .on_data(df)
            .add_check(
                Check(CheckLevel.ERROR, "test")
                .is_unique("a")
            )
            .run()
        )

        assert result.status == CheckStatus.ERROR

    def test_multiple_checks(self):
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "value": [10, 20, 30]
        })

        result = (
            VerificationSuite()
            .on_data(df)
            .add_check(
                Check(CheckLevel.ERROR, "size check")
                .has_size(lambda x: x == 3)
            )
            .add_check(
                Check(CheckLevel.ERROR, "completeness check")
                .is_complete("id")
                .is_complete("value")
            )
            .run()
        )

        assert result.status == CheckStatus.SUCCESS
        assert len(result.check_results) == 2

    def test_warning_level(self):
        df = pl.DataFrame({"a": [1, None, 3]})

        result = (
            VerificationSuite()
            .on_data(df)
            .add_check(
                Check(CheckLevel.WARNING, "test")
                .is_complete("a")
            )
            .run()
        )

        assert result.status == CheckStatus.WARNING

    def test_mixed_levels(self):
        df = pl.DataFrame({"a": [1, 2, 3]})

        result = (
            VerificationSuite()
            .on_data(df)
            .add_check(
                Check(CheckLevel.ERROR, "error check")
                .has_size(lambda x: x == 3)
            )
            .add_check(
                Check(CheckLevel.WARNING, "warning check")
                .has_mean("a", lambda x: x > 100)  # This will fail
            )
            .run()
        )

        assert result.status == CheckStatus.WARNING

    def test_no_data_raises_error(self):
        with pytest.raises(ValueError):
            VerificationSuite().run()

    def test_constraint_results_accessible(self):
        df = pl.DataFrame({"a": [1, 2, 3]})

        result = (
            VerificationSuite()
            .on_data(df)
            .add_check(
                Check(CheckLevel.ERROR, "test")
                .has_size(lambda x: x == 3)
                .is_complete("a")
            )
            .run()
        )

        for check, check_result in result.check_results.items():
            assert len(check_result.constraint_results) == 2
            for constraint_result in check_result.constraint_results:
                assert constraint_result.status == ConstraintStatus.SUCCESS

    def test_complex_scenario(self):
        df = pl.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "email": [
                "alice@example.com",
                "bob@test.com",
                "invalid",
                None,
                "charlie@test.com"
            ],
            "age": [25, 30, 35, 40, 45],
            "status": ["active", "active", "inactive", "active", "pending"]
        })

        result = (
            VerificationSuite()
            .on_data(df)
            .add_check(
                Check(CheckLevel.ERROR, "basic checks")
                .has_size(lambda x: x == 5)
                .is_complete("id")
                .is_unique("id")
            )
            .add_check(
                Check(CheckLevel.WARNING, "email validation")
                .has_completeness("email", lambda x: x >= 0.8)
                .contains_email("email", lambda x: x >= 0.6)
            )
            .add_check(
                Check(CheckLevel.ERROR, "business rules")
                .is_non_negative("age")
                .has_mean("age", lambda x: 20 <= x <= 50)
            )
            .run()
        )

        # Basic checks should pass
        # Email validation might have warnings
        # Business rules should pass
        assert result.status in [CheckStatus.SUCCESS, CheckStatus.WARNING]
