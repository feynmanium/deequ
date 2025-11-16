"""Check class for grouping constraints."""

from enum import Enum
from typing import List, Callable, Optional, Any
import polars as pl

from pydeequ_polars.constraints.constraint import Constraint, ConstraintResult, ConstraintStatus
from pydeequ_polars.analyzers.size import Size
from pydeequ_polars.analyzers.completeness import Completeness
from pydeequ_polars.analyzers.uniqueness import Uniqueness
from pydeequ_polars.analyzers.distinctness import Distinctness
from pydeequ_polars.analyzers.min_max import Minimum, Maximum
from pydeequ_polars.analyzers.mean import Mean
from pydeequ_polars.analyzers.sum import Sum
from pydeequ_polars.analyzers.standard_deviation import StandardDeviation
from pydeequ_polars.analyzers.approx_quantile import ApproxQuantile
from pydeequ_polars.analyzers.pattern_match import PatternMatch
from pydeequ_polars.analyzers.correlation import Correlation
from pydeequ_polars.analyzers.analyzer import Analyzer


class CheckLevel(Enum):
    """Level/severity of a check."""
    WARNING = "Warning"
    ERROR = "Error"


class CheckStatus(Enum):
    """Status of a check after evaluation."""
    SUCCESS = "Success"
    WARNING = "Warning"
    ERROR = "Error"


class CheckResult:
    """Result of running a check."""

    def __init__(
        self,
        check: "Check",
        status: CheckStatus,
        constraint_results: List[ConstraintResult]
    ):
        self.check = check
        self.status = status
        self.constraint_results = constraint_results

    def __repr__(self):
        return f"CheckResult(check={self.check.description}, status={self.status.value}, constraints={len(self.constraint_results)})"


class Check:
    """
    A check groups multiple constraints together with a description and level.

    Example:
        check = Check(CheckLevel.ERROR, "Data quality check")
        check.has_size(lambda x: x == 100)
        check.is_complete("customer_id")
        check.is_unique("customer_id")
    """

    def __init__(self, level: CheckLevel, description: str):
        self.level = level
        self.description = description
        self.constraints: List[tuple[Analyzer, Constraint]] = []

    def has_size(self, assertion: Callable[[int], bool], hint: Optional[str] = None) -> "Check":
        """Add a constraint on the number of rows."""
        analyzer = Size()
        constraint = Constraint("SizeConstraint", assertion, hint)
        self.constraints.append((analyzer, constraint))
        return self

    def is_complete(self, column: str, hint: Optional[str] = None) -> "Check":
        """Add a constraint that a column must be 100% complete (no nulls)."""
        analyzer = Completeness(column)
        constraint = Constraint(
            f"CompletenessConstraint({column})",
            lambda x: x == 1.0,
            hint or f"Column {column} must not contain null values"
        )
        self.constraints.append((analyzer, constraint))
        return self

    def has_completeness(
        self,
        column: str,
        assertion: Callable[[float], bool],
        hint: Optional[str] = None
    ) -> "Check":
        """Add a constraint on the completeness ratio of a column."""
        analyzer = Completeness(column)
        constraint = Constraint(f"CompletenessConstraint({column})", assertion, hint)
        self.constraints.append((analyzer, constraint))
        return self

    def is_unique(self, column: str, hint: Optional[str] = None) -> "Check":
        """Add a constraint that a column must have all unique values."""
        analyzer = Uniqueness(column)
        constraint = Constraint(
            f"UniquenessConstraint({column})",
            lambda x: x == 1.0,
            hint or f"Column {column} must contain only unique values"
        )
        self.constraints.append((analyzer, constraint))
        return self

    def has_uniqueness(
        self,
        column: str,
        assertion: Callable[[float], bool],
        hint: Optional[str] = None
    ) -> "Check":
        """Add a constraint on the uniqueness ratio of a column."""
        analyzer = Uniqueness(column)
        constraint = Constraint(f"UniquenessConstraint({column})", assertion, hint)
        self.constraints.append((analyzer, constraint))
        return self

    def has_distinctness(
        self,
        column: str,
        assertion: Callable[[int], bool],
        hint: Optional[str] = None
    ) -> "Check":
        """Add a constraint on the number of distinct values."""
        analyzer = Distinctness(column)
        constraint = Constraint(f"DistinctnessConstraint({column})", assertion, hint)
        self.constraints.append((analyzer, constraint))
        return self

    def has_min(
        self,
        column: str,
        assertion: Callable[[Any], bool],
        hint: Optional[str] = None
    ) -> "Check":
        """Add a constraint on the minimum value of a column."""
        analyzer = Minimum(column)
        constraint = Constraint(f"MinimumConstraint({column})", assertion, hint)
        self.constraints.append((analyzer, constraint))
        return self

    def has_max(
        self,
        column: str,
        assertion: Callable[[Any], bool],
        hint: Optional[str] = None
    ) -> "Check":
        """Add a constraint on the maximum value of a column."""
        analyzer = Maximum(column)
        constraint = Constraint(f"MaximumConstraint({column})", assertion, hint)
        self.constraints.append((analyzer, constraint))
        return self

    def has_mean(
        self,
        column: str,
        assertion: Callable[[float], bool],
        hint: Optional[str] = None
    ) -> "Check":
        """Add a constraint on the mean value of a column."""
        analyzer = Mean(column)
        constraint = Constraint(f"MeanConstraint({column})", assertion, hint)
        self.constraints.append((analyzer, constraint))
        return self

    def has_sum(
        self,
        column: str,
        assertion: Callable[[Any], bool],
        hint: Optional[str] = None
    ) -> "Check":
        """Add a constraint on the sum of values in a column."""
        analyzer = Sum(column)
        constraint = Constraint(f"SumConstraint({column})", assertion, hint)
        self.constraints.append((analyzer, constraint))
        return self

    def has_standard_deviation(
        self,
        column: str,
        assertion: Callable[[float], bool],
        hint: Optional[str] = None
    ) -> "Check":
        """Add a constraint on the standard deviation of a column."""
        analyzer = StandardDeviation(column)
        constraint = Constraint(f"StandardDeviationConstraint({column})", assertion, hint)
        self.constraints.append((analyzer, constraint))
        return self

    def has_approx_quantile(
        self,
        column: str,
        quantile: float,
        assertion: Callable[[Any], bool],
        hint: Optional[str] = None
    ) -> "Check":
        """Add a constraint on an approximate quantile of a column."""
        analyzer = ApproxQuantile(column, quantile)
        constraint = Constraint(
            f"ApproxQuantileConstraint({column},{quantile})",
            assertion,
            hint
        )
        self.constraints.append((analyzer, constraint))
        return self

    def is_non_negative(self, column: str, hint: Optional[str] = None) -> "Check":
        """Add a constraint that a column contains no negative values."""
        analyzer = Minimum(column)
        constraint = Constraint(
            f"NonNegativeConstraint({column})",
            lambda x: x >= 0,
            hint or f"Column {column} must not contain negative values"
        )
        self.constraints.append((analyzer, constraint))
        return self

    def is_positive(self, column: str, hint: Optional[str] = None) -> "Check":
        """Add a constraint that a column contains only positive values."""
        analyzer = Minimum(column)
        constraint = Constraint(
            f"PositiveConstraint({column})",
            lambda x: x > 0,
            hint or f"Column {column} must contain only positive values"
        )
        self.constraints.append((analyzer, constraint))
        return self

    def satisfies(
        self,
        column_condition: str,
        constraint_name: str,
        assertion: Optional[Callable[[float], bool]] = None,
        hint: Optional[str] = None
    ) -> "Check":
        """
        Add a constraint based on a custom SQL-like condition.

        Args:
            column_condition: A Polars expression as string (e.g., "price > 0")
            constraint_name: Name for the constraint
            assertion: Optional assertion on the ratio of rows satisfying the condition
            hint: Optional hint message
        """
        # This requires custom implementation - placeholder for now
        # In a full implementation, this would parse and evaluate the condition
        raise NotImplementedError("Custom satisfies constraints not yet implemented")

    def contains_url(
        self,
        column: str,
        assertion: Callable[[float], bool],
        hint: Optional[str] = None
    ) -> "Check":
        """Add a constraint that checks if values contain URLs."""
        # Simple URL pattern
        url_pattern = r"https?://[^\s]+"
        analyzer = PatternMatch(column, url_pattern)
        constraint = Constraint(
            f"ContainsURLConstraint({column})",
            assertion,
            hint or f"Column {column} must contain URLs"
        )
        self.constraints.append((analyzer, constraint))
        return self

    def contains_email(
        self,
        column: str,
        assertion: Callable[[float], bool],
        hint: Optional[str] = None
    ) -> "Check":
        """Add a constraint that checks if values contain email addresses."""
        # Simple email pattern
        email_pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
        analyzer = PatternMatch(column, email_pattern)
        constraint = Constraint(
            f"ContainsEmailConstraint({column})",
            assertion,
            hint or f"Column {column} must contain email addresses"
        )
        self.constraints.append((analyzer, constraint))
        return self

    def matches_pattern(
        self,
        column: str,
        pattern: str,
        assertion: Callable[[float], bool],
        hint: Optional[str] = None
    ) -> "Check":
        """Add a constraint that checks if values match a regex pattern."""
        analyzer = PatternMatch(column, pattern)
        constraint = Constraint(
            f"PatternMatchConstraint({column})",
            assertion,
            hint
        )
        self.constraints.append((analyzer, constraint))
        return self

    def is_contained_in(
        self,
        column: str,
        allowed_values: List[Any],
        hint: Optional[str] = None
    ) -> "Check":
        """
        Add a constraint that all values in a column are contained in a set of allowed values.
        This is implemented by checking if all values are in the allowed set.
        """
        # This requires a custom analyzer - we'll create a simple implementation
        from pydeequ_polars.analyzers.analyzer import Analyzer, Metric

        class ContainedInAnalyzer(Analyzer):
            def __init__(self, column: str, allowed_values: List[Any]):
                self.column = column
                self.allowed_values = set(allowed_values)

            def calculate(self, df: pl.DataFrame) -> Metric:
                if self.column not in df.columns:
                    return Metric(
                        name=self.metric_name(),
                        value=None,
                        entity=f"Column({self.column})"
                    )

                total = df.height
                if total == 0:
                    return Metric(
                        name=self.metric_name(),
                        value=1.0,
                        entity=f"Column({self.column})"
                    )

                # Count values that are in the allowed set
                contained_count = df.select(
                    pl.col(self.column).is_in(list(self.allowed_values)).sum()
                ).item()

                ratio = contained_count / total
                return Metric(
                    name=self.metric_name(),
                    value=ratio,
                    entity=f"Column({self.column})"
                )

            def metric_name(self) -> str:
                return f"ContainedIn({self.column})"

        analyzer = ContainedInAnalyzer(column, allowed_values)
        constraint = Constraint(
            f"ContainedInConstraint({column})",
            lambda x: x == 1.0,
            hint or f"Column {column} must only contain values from {allowed_values}"
        )
        self.constraints.append((analyzer, constraint))
        return self

    def has_correlation(
        self,
        column1: str,
        column2: str,
        assertion: Callable[[float], bool],
        hint: Optional[str] = None
    ) -> "Check":
        """Add a constraint on the correlation between two columns."""
        analyzer = Correlation(column1, column2)
        constraint = Constraint(
            f"CorrelationConstraint({column1},{column2})",
            assertion,
            hint
        )
        self.constraints.append((analyzer, constraint))
        return self

    def __repr__(self):
        return f"Check(level={self.level.value}, description='{self.description}', constraints={len(self.constraints)})"
