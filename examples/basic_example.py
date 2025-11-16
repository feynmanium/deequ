"""
Basic example of using PyDeequ Polars for data quality validation.

This example demonstrates the core functionality of PyDeequ Polars,
mirroring the Scala example from the original Deequ library.
"""

import polars as pl
from pydeequ_polars import VerificationSuite, Check, CheckLevel, CheckStatus
from pydeequ_polars.constraints.constraint import ConstraintStatus


def main():
    # Create sample data
    data = {
        "id": [1, 2, 3, 4, 5],
        "productName": ["Thingy A", "Thingy B", None, "Thingy D", "Thingy E"],
        "description": [
            "awesome thing.",
            "available at http://thingb.com",
            None,
            "checkout https://thingd.ca",
            None
        ],
        "priority": ["high", None, "low", "low", "high"],
        "numViews": [0, 0, 5, 10, 12]
    }

    df = pl.DataFrame(data)

    print("Input DataFrame:")
    print(df)
    print("\n" + "="*80 + "\n")

    # Define verification suite with checks
    verification_result = (
        VerificationSuite()
        .on_data(df)
        .add_check(
            Check(CheckLevel.ERROR, "unit testing my data")
            .has_size(lambda x: x == 5)  # we expect 5 rows
            .is_complete("id")  # should never be NULL
            .is_unique("id")  # should not contain duplicates
            .is_complete("productName")  # should never be NULL
            # should only contain the values "high" and "low"
            .is_contained_in("priority", ["high", "low"])
            .is_non_negative("numViews")  # should not contain negative values
            # at least half of the descriptions should contain a url
            .contains_url("description", lambda x: x >= 0.5)
            # half of the items should have less than 10 views
            .has_approx_quantile("numViews", 0.5, lambda x: x <= 10)
        )
        .run()
    )

    # Check the results
    if verification_result.status == CheckStatus.SUCCESS:
        print("The data passed the test, everything is fine!")
    else:
        print("We found errors in the data:\n")

        for check, check_result in verification_result.check_results.items():
            for constraint_result in check_result.constraint_results:
                if constraint_result.status != ConstraintStatus.SUCCESS:
                    print(f"{constraint_result.constraint.name}: {constraint_result.message}")

    print("\n" + "="*80 + "\n")
    print(f"Overall Status: {verification_result.status.value}")
    print(f"Number of checks: {len(verification_result.check_results)}")

    # Print detailed results
    print("\nDetailed Results:")
    for check, check_result in verification_result.check_results.items():
        print(f"\nCheck: {check.description} (Level: {check.level.value})")
        print(f"Status: {check_result.status.value}")
        print(f"Constraints evaluated: {len(check_result.constraint_results)}")

        for constraint_result in check_result.constraint_results:
            status_symbol = "✓" if constraint_result.status == ConstraintStatus.SUCCESS else "✗"
            print(f"  {status_symbol} {constraint_result.constraint.name}: {constraint_result.message}")


if __name__ == "__main__":
    main()
