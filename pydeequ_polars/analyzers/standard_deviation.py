"""Standard Deviation analyzer."""

import polars as pl
from pydeequ_polars.analyzers.analyzer import Analyzer, Metric


class StandardDeviation(Analyzer):
    """Analyzer to compute the standard deviation of a column."""

    def __init__(self, column: str):
        self.column = column

    def calculate(self, df: pl.DataFrame) -> Metric:
        """Calculate the standard deviation."""
        if self.column not in df.columns:
            return Metric(
                name=self.metric_name(),
                value=None,
                entity=f"Column({self.column})"
            )

        std_value = df.select(pl.col(self.column).std()).item()

        return Metric(
            name=self.metric_name(),
            value=std_value,
            entity=f"Column({self.column})"
        )

    def metric_name(self) -> str:
        return f"StandardDeviation({self.column})"
