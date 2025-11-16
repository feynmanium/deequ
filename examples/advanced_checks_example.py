"""
Advanced example showing various types of data quality checks.

This demonstrates more sophisticated validation scenarios.
"""

import polars as pl
from pydeequ_polars import VerificationSuite, Check, CheckLevel, CheckStatus
from pydeequ_polars.constraints.constraint import ConstraintStatus


def main():
    # Create a more complex dataset
    data = {
        "customer_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "email": [
            "alice@example.com",
            "bob@test.com",
            "invalid-email",
            "charlie@example.com",
            "dave@test.com",
            "eve@example.com",
            None,
            "frank@test.com",
            "grace@example.com",
            "henry@test.com"
        ],
        "age": [25, 30, 35, 40, 45, -5, 50, 55, 60, 65],  # Note: negative age
        "balance": [100.0, 200.0, 150.0, 300.0, 250.0, 180.0, 220.0, 190.0, 280.0, 210.0],
        "account_type": ["premium", "standard", "premium", "premium", "standard",
                        "premium", "standard", "premium", "standard", "invalid"],  # Note: invalid type
        "credit_score": [720, 680, 750, 700, 690, 730, 710, 740, 720, 700]
    }

    df = pl.DataFrame(data)

    print("Dataset Overview:")
    print(df)
    print("\n" + "="*80 + "\n")

    # Define multiple checks with different levels
    verification_result = (
        VerificationSuite()
        .on_data(df)
        .add_check(
            Check(CheckLevel.ERROR, "Critical data quality requirements")
            .has_size(lambda x: x > 0)
            .is_complete("customer_id")
            .is_unique("customer_id")
            .is_non_negative("age")  # This will fail
            .is_positive("balance")
        )
        .add_check(
            Check(CheckLevel.WARNING, "Email validation")
            .has_completeness("email", lambda x: x >= 0.9)  # Allow some missing
            .contains_email("email", lambda x: x >= 0.8)  # At least 80% valid emails
        )
        .add_check(
            Check(CheckLevel.ERROR, "Business rules")
            .is_contained_in("account_type", ["premium", "standard"])  # This will fail
            .has_mean("balance", lambda x: 100 <= x <= 300)
            .has_min("credit_score", lambda x: x >= 600)
            .has_max("age", lambda x: x <= 100)
        )
        .add_check(
            Check(CheckLevel.WARNING, "Statistical checks")
            .has_standard_deviation("balance", lambda x: x > 0)
            .has_distinctness("account_type", lambda x: x >= 2)
            .has_approx_quantile("age", 0.5, lambda x: 30 <= x <= 60)
        )
        .run()
    )

    # Report results
    print(f"Overall Status: {verification_result.status.value}")
    print("\n" + "="*80 + "\n")

    for check, check_result in verification_result.check_results.items():
        print(f"Check: {check.description}")
        print(f"Level: {check.level.value}")
        print(f"Status: {check_result.status.value}")
        print("-" * 80)

        success_count = 0
        failure_count = 0

        for constraint_result in check_result.constraint_results:
            if constraint_result.status == ConstraintStatus.SUCCESS:
                success_count += 1
                status_symbol = "✓"
                color = ""
            else:
                failure_count += 1
                status_symbol = "✗"
                color = ""

            print(f"{status_symbol} {constraint_result.constraint.name}")
            print(f"  {constraint_result.message}")

        print(f"\nSummary: {success_count} passed, {failure_count} failed")
        print("="*80 + "\n")

    # Generate a summary report
    total_constraints = sum(
        len(cr.constraint_results)
        for cr in verification_result.check_results.values()
    )

    failed_constraints = sum(
        sum(1 for r in cr.constraint_results if r.status == ConstraintStatus.FAILURE)
        for cr in verification_result.check_results.values()
    )

    print("Overall Summary:")
    print(f"  Total Checks: {len(verification_result.check_results)}")
    print(f"  Total Constraints: {total_constraints}")
    print(f"  Passed Constraints: {total_constraints - failed_constraints}")
    print(f"  Failed Constraints: {failed_constraints}")
    print(f"  Success Rate: {((total_constraints - failed_constraints) / total_constraints * 100):.1f}%")


if __name__ == "__main__":
    main()
