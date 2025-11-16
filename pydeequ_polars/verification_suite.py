"""VerificationSuite for running data quality checks."""

from typing import List, Dict, Optional
import polars as pl

from pydeequ_polars.checks.check import Check, CheckResult, CheckStatus, CheckLevel
from pydeequ_polars.constraints.constraint import ConstraintResult, ConstraintStatus
from pydeequ_polars.verification_result import VerificationResult
from pydeequ_polars.analyzers.analyzer import Metric


class VerificationSuite:
    """
    Main entry point for defining and running data quality checks.

    Example:
        result = VerificationSuite() \\
            .on_data(df) \\
            .add_check(
                Check(CheckLevel.ERROR, "data quality")
                .has_size(lambda x: x > 0)
                .is_complete("id")
            ) \\
            .run()
    """

    def __init__(self):
        self._df: Optional[pl.DataFrame] = None
        self._checks: List[Check] = []

    def on_data(self, df: pl.DataFrame) -> "VerificationSuite":
        """
        Set the DataFrame to run checks on.

        Args:
            df: Polars DataFrame to validate

        Returns:
            Self for method chaining
        """
        self._df = df
        return self

    def add_check(self, check: Check) -> "VerificationSuite":
        """
        Add a check to the verification suite.

        Args:
            check: Check to add

        Returns:
            Self for method chaining
        """
        self._checks.append(check)
        return self

    def run(self) -> VerificationResult:
        """
        Run all checks on the data.

        Returns:
            VerificationResult containing the results of all checks

        Raises:
            ValueError: If no data has been set
        """
        if self._df is None:
            raise ValueError("No data has been set. Use on_data() to set the DataFrame.")

        check_results: Dict[Check, CheckResult] = {}
        overall_status = CheckStatus.SUCCESS

        for check in self._checks:
            # Compute metrics for all analyzers in this check
            metrics: Dict[str, Metric] = {}
            for analyzer, _ in check.constraints:
                metric = analyzer.calculate(self._df)
                metrics[metric.name] = metric

            # Evaluate constraints
            constraint_results: List[ConstraintResult] = []
            check_has_failures = False

            for analyzer, constraint in check.constraints:
                metric = metrics[analyzer.metric_name()]
                result = constraint.evaluate(metric)
                constraint_results.append(result)

                if result.status == ConstraintStatus.FAILURE:
                    check_has_failures = True

            # Determine check status
            if check_has_failures:
                if check.level == CheckLevel.ERROR:
                    check_status = CheckStatus.ERROR
                    overall_status = CheckStatus.ERROR
                else:
                    check_status = CheckStatus.WARNING
                    if overall_status != CheckStatus.ERROR:
                        overall_status = CheckStatus.WARNING
            else:
                check_status = CheckStatus.SUCCESS

            check_results[check] = CheckResult(check, check_status, constraint_results)

        return VerificationResult(overall_status, check_results)
