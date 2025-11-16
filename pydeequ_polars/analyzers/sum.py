"""Sum analyzer for computing sum of values."""

import polars as pl
from pydeequ_polars.analyzers.analyzer import Analyzer, Metric


class Sum(Analyzer):
    """Analyzer to compute the sum of values in a column."""

    def __init__(self, column: str):
        self.column = column

    def calculate(self, df: pl.DataFrame) -> Metric:
        """Calculate the sum of values."""
        if self.column not in df.columns:
            return Metric(
                name=self.metric_name(),
                value=None,
                entity=f"Column({self.column})"
            )

        sum_value = df.select(pl.col(self.column).sum()).item()

        return Metric(
            name=self.metric_name(),
            value=sum_value,
            entity=f"Column({self.column})"
        )

    def metric_name(self) -> str:
        return f"Sum({self.column})"
