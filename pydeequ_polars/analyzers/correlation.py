"""Correlation analyzer for computing correlation between columns."""

import polars as pl
from pydeequ_polars.analyzers.analyzer import Analyzer, Metric


class Correlation(Analyzer):
    """Analyzer to compute the correlation between two columns."""

    def __init__(self, column1: str, column2: str):
        self.column1 = column1
        self.column2 = column2

    def calculate(self, df: pl.DataFrame) -> Metric:
        """Calculate the Pearson correlation coefficient."""
        if self.column1 not in df.columns or self.column2 not in df.columns:
            return Metric(
                name=self.metric_name(),
                value=None,
                entity=f"Columns({self.column1},{self.column2})"
            )

        # Compute correlation using Polars
        corr_matrix = df.select([self.column1, self.column2]).corr()

        # Extract the correlation value (off-diagonal element)
        corr_value = corr_matrix[self.column1][1]

        return Metric(
            name=self.metric_name(),
            value=corr_value,
            entity=f"Columns({self.column1},{self.column2})"
        )

    def metric_name(self) -> str:
        return f"Correlation({self.column1},{self.column2})"
