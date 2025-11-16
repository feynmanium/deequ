"""Mean analyzer for computing average values."""

import polars as pl
from pydeequ_polars.analyzers.analyzer import Analyzer, Metric


class Mean(Analyzer):
    """Analyzer to compute the mean (average) value of a column."""

    def __init__(self, column: str):
        self.column = column

    def calculate(self, df: pl.DataFrame) -> Metric:
        """Calculate the mean value."""
        if self.column not in df.columns:
            return Metric(
                name=self.metric_name(),
                value=None,
                entity=f"Column({self.column})"
            )

        mean_value = df.select(pl.col(self.column).mean()).item()

        return Metric(
            name=self.metric_name(),
            value=mean_value,
            entity=f"Column({self.column})"
        )

    def metric_name(self) -> str:
        return f"Mean({self.column})"
