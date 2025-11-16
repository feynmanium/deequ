"""Completeness analyzer for measuring non-null ratio."""

import polars as pl
from pydeequ_polars.analyzers.analyzer import Analyzer, Metric


class Completeness(Analyzer):
    """Analyzer to compute the completeness (non-null ratio) of a column."""

    def __init__(self, column: str):
        self.column = column

    def calculate(self, df: pl.DataFrame) -> Metric:
        """Calculate the completeness ratio (non-null / total)."""
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

        non_null_count = df.select(pl.col(self.column).is_not_null().sum()).item()
        completeness = non_null_count / total

        return Metric(
            name=self.metric_name(),
            value=completeness,
            entity=f"Column({self.column})"
        )

    def metric_name(self) -> str:
        return f"Completeness({self.column})"
