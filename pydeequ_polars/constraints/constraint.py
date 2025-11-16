"""Base Constraint classes."""

from enum import Enum
from typing import Callable, Optional, Any
from pydeequ_polars.analyzers.analyzer import Metric


class ConstraintStatus(Enum):
    """Status of a constraint after evaluation."""
    SUCCESS = "Success"
    FAILURE = "Failure"


class ConstraintResult:
    """Result of evaluating a constraint."""

    def __init__(
        self,
        constraint: "Constraint",
        status: ConstraintStatus,
        message: Optional[str] = None,
        metric: Optional[Metric] = None
    ):
        self.constraint = constraint
        self.status = status
        self.message = message
        self.metric = metric

    def __repr__(self):
        return f"ConstraintResult(constraint={self.constraint}, status={self.status.value}, message={self.message})"


class Constraint:
    """Base class for all constraints."""

    def __init__(
        self,
        name: str,
        assertion: Callable[[Any], bool],
        hint: Optional[str] = None
    ):
        self.name = name
        self.assertion = assertion
        self.hint = hint

    def evaluate(self, metric: Metric) -> ConstraintResult:
        """
        Evaluate the constraint against a metric value.

        Args:
            metric: The computed metric to check

        Returns:
            ConstraintResult indicating success or failure
        """
        if metric.value is None:
            return ConstraintResult(
                constraint=self,
                status=ConstraintStatus.FAILURE,
                message=f"Metric {metric.name} could not be computed",
                metric=metric
            )

        try:
            passed = self.assertion(metric.value)

            if passed:
                return ConstraintResult(
                    constraint=self,
                    status=ConstraintStatus.SUCCESS,
                    message=f"Value: {metric.value} meets the constraint",
                    metric=metric
                )
            else:
                message = f"Value: {metric.value} does not meet the constraint"
                if self.hint:
                    message += f". Hint: {self.hint}"
                return ConstraintResult(
                    constraint=self,
                    status=ConstraintStatus.FAILURE,
                    message=message,
                    metric=metric
                )
        except Exception as e:
            return ConstraintResult(
                constraint=self,
                status=ConstraintStatus.FAILURE,
                message=f"Error evaluating constraint: {str(e)}",
                metric=metric
            )

    def __repr__(self):
        return f"Constraint({self.name})"
