"""Uniqueness analyzer for measuring unique value ratio."""

import polars as pl
from pydeequ_polars.analyzers.analyzer import Analyzer, Metric


class Uniqueness(Analyzer):
    """Analyzer to compute the uniqueness (ratio of unique values) of a column."""

    def __init__(self, column: str):
        self.column = column

    def calculate(self, df: pl.DataFrame) -> Metric:
        """Calculate the uniqueness ratio (unique / total)."""
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

        unique_count = df.select(pl.col(self.column).n_unique()).item()
        uniqueness = unique_count / total

        return Metric(
            name=self.metric_name(),
            value=uniqueness,
            entity=f"Column({self.column})"
        )

    def metric_name(self) -> str:
        return f"Uniqueness({self.column})"
