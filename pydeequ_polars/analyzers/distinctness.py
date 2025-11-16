"""Distinctness analyzer for counting distinct values."""

import polars as pl
from pydeequ_polars.analyzers.analyzer import Analyzer, Metric


class Distinctness(Analyzer):
    """Analyzer to compute the number of distinct values in a column."""

    def __init__(self, column: str):
        self.column = column

    def calculate(self, df: pl.DataFrame) -> Metric:
        """Calculate the number of distinct values."""
        if self.column not in df.columns:
            return Metric(
                name=self.metric_name(),
                value=None,
                entity=f"Column({self.column})"
            )

        distinct_count = df.select(pl.col(self.column).n_unique()).item()

        return Metric(
            name=self.metric_name(),
            value=distinct_count,
            entity=f"Column({self.column})"
        )

    def metric_name(self) -> str:
        return f"Distinctness({self.column})"
