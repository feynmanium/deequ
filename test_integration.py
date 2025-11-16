"""
Quick integration test to verify the Polars implementation works end to end.
"""

import polars as pl
from pydeequ_polars import VerificationSuite, Check, CheckLevel, CheckStatus
from pydeequ_polars.constraints.constraint import ConstraintStatus


def test_basic_integration():
    """Test basic verification suite functionality."""
    print("Testing basic integration...")

    df = pl.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "age": [25, 30, 35, 40, 45]
    })

    result = (
        VerificationSuite()
        .on_data(df)
        .add_check(
            Check(CheckLevel.ERROR, "Basic checks")
            .has_size(lambda x: x == 5)
            .is_complete("id")
            .is_unique("id")
        )
        .run()
    )

    assert result.status == CheckStatus.SUCCESS
    print("✓ Basic integration test passed")


def test_constraint_failures():
    """Test that constraint failures are detected."""
    print("\nTesting constraint failures...")

    df = pl.DataFrame({
        "id": [1, 2, 2, 4, 5],  # Duplicate
        "value": [10, None, 30, 40, 50]  # Null value
    })

    result = (
        VerificationSuite()
        .on_data(df)
        .add_check(
            Check(CheckLevel.ERROR, "Failure checks")
            .is_unique("id")
            .is_complete("value")
        )
        .run()
    )

    assert result.status == CheckStatus.ERROR
    failures = sum(
        1 for check_result in result.check_results.values()
        for constraint_result in check_result.constraint_results
        if constraint_result.status == ConstraintStatus.FAILURE
    )
    assert failures == 2
    print("✓ Constraint failure detection works")


def test_statistical_metrics():
    """Test statistical analyzers."""
    print("\nTesting statistical metrics...")

    df = pl.DataFrame({
        "value": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    })

    result = (
        VerificationSuite()
        .on_data(df)
        .add_check(
            Check(CheckLevel.ERROR, "Statistical checks")
            .has_mean("value", lambda x: x == 5.5)
            .has_min("value", lambda x: x == 1)
            .has_max("value", lambda x: x == 10)
            .has_sum("value", lambda x: x == 55)
            .has_approx_quantile("value", 0.5, lambda x: 5 <= x <= 6)  # Median can be 5 or 5.5
        )
        .run()
    )

    # Print results if failed
    if result.status != CheckStatus.SUCCESS:
        for check, check_result in result.check_results.items():
            for cr in check_result.constraint_results:
                if cr.status == ConstraintStatus.FAILURE:
                    print(f"  Failed: {cr.constraint.name} - {cr.message}")

    assert result.status == CheckStatus.SUCCESS
    print("✓ Statistical metrics work correctly")


def test_pattern_matching():
    """Test pattern matching functionality."""
    print("\nTesting pattern matching...")

    df = pl.DataFrame({
        "email": [
            "alice@example.com",
            "bob@test.com",
            "invalid-email",
            "charlie@example.com"
        ],
        "url": [
            "http://example.com",
            "https://test.com",
            "not-a-url",
            "https://another.com"
        ]
    })

    result = (
        VerificationSuite()
        .on_data(df)
        .add_check(
            Check(CheckLevel.WARNING, "Pattern checks")
            .contains_email("email", lambda x: x >= 0.5)
            .contains_url("url", lambda x: x >= 0.5)
        )
        .run()
    )

    # Should pass with warnings since some values don't match
    assert result.status in [CheckStatus.SUCCESS, CheckStatus.WARNING]
    print("✓ Pattern matching works")


def test_metrics_repository():
    """Test metrics repository functionality."""
    print("\nTesting metrics repository...")

    from pydeequ_polars.repository import InMemoryMetricsRepository
    from pydeequ_polars.repository.metrics_repository import MetricEntry
    from pydeequ_polars.analyzers import Size, Completeness
    from datetime import datetime

    repo = InMemoryMetricsRepository()
    df = pl.DataFrame({"a": [1, 2, 3]})

    # Save metrics
    size_metric = Size().calculate(df)
    completeness_metric = Completeness("a").calculate(df)

    repo.save(MetricEntry(size_metric, datetime.now(), {"env": "test"}))
    repo.save(MetricEntry(completeness_metric, datetime.now(), {"env": "test"}))

    # Load metrics
    all_metrics = repo.load()
    assert len(all_metrics) == 2

    size_metrics = repo.load(metric_name="Size")
    assert len(size_metrics) == 1
    assert size_metrics[0].metric.value == 3

    print("✓ Metrics repository works")


def test_complex_scenario():
    """Test a complex real-world scenario."""
    print("\nTesting complex scenario...")

    df = pl.DataFrame({
        "customer_id": [1, 2, 3, 4, 5],
        "email": ["a@test.com", "b@test.com", "c@test.com", "d@test.com", "e@test.com"],
        "age": [25, 30, 35, 40, 45],
        "balance": [100.0, 200.0, 150.0, 300.0, 250.0],
        "status": ["active", "active", "inactive", "active", "active"]
    })

    result = (
        VerificationSuite()
        .on_data(df)
        .add_check(
            Check(CheckLevel.ERROR, "Critical validations")
            .has_size(lambda x: x == 5)
            .is_complete("customer_id")
            .is_unique("customer_id")
            .is_unique("email")
        )
        .add_check(
            Check(CheckLevel.WARNING, "Business rules")
            .is_contained_in("status", ["active", "inactive"])
            .is_non_negative("age")
            .is_positive("balance")
            .has_mean("balance", lambda x: 100 <= x <= 300)
        )
        .run()
    )

    assert result.status in [CheckStatus.SUCCESS, CheckStatus.WARNING]
    print("✓ Complex scenario works")


if __name__ == "__main__":
    test_basic_integration()
    test_constraint_failures()
    test_statistical_metrics()
    test_pattern_matching()
    test_metrics_repository()
    test_complex_scenario()

    print("\n" + "="*80)
    print("All integration tests passed! ✓")
    print("="*80)
