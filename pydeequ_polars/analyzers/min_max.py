"""Min and Max analyzers for computing minimum and maximum values."""

import polars as pl
from pydeequ_polars.analyzers.analyzer import Analyzer, Metric


class Minimum(Analyzer):
    """Analyzer to compute the minimum value of a column."""

    def __init__(self, column: str):
        self.column = column

    def calculate(self, df: pl.DataFrame) -> Metric:
        """Calculate the minimum value."""
        if self.column not in df.columns:
            return Metric(
                name=self.metric_name(),
                value=None,
                entity=f"Column({self.column})"
            )

        min_value = df.select(pl.col(self.column).min()).item()

        return Metric(
            name=self.metric_name(),
            value=min_value,
            entity=f"Column({self.column})"
        )

    def metric_name(self) -> str:
        return f"Minimum({self.column})"


class Maximum(Analyzer):
    """Analyzer to compute the maximum value of a column."""

    def __init__(self, column: str):
        self.column = column

    def calculate(self, df: pl.DataFrame) -> Metric:
        """Calculate the maximum value."""
        if self.column not in df.columns:
            return Metric(
                name=self.metric_name(),
                value=None,
                entity=f"Column({self.column})"
            )

        max_value = df.select(pl.col(self.column).max()).item()

        return Metric(
            name=self.metric_name(),
            value=max_value,
            entity=f"Column({self.column})"
        )

    def metric_name(self) -> str:
        return f"Maximum({self.column})"
