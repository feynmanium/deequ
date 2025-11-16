"""Approximate Quantile analyzer."""

import polars as pl
from pydeequ_polars.analyzers.analyzer import Analyzer, Metric


class ApproxQuantile(Analyzer):
    """Analyzer to compute an approximate quantile of a column."""

    def __init__(self, column: str, quantile: float):
        self.column = column
        self.quantile = quantile

        if not 0 <= quantile <= 1:
            raise ValueError(f"Quantile must be between 0 and 1, got {quantile}")

    def calculate(self, df: pl.DataFrame) -> Metric:
        """Calculate the approximate quantile."""
        if self.column not in df.columns:
            return Metric(
                name=self.metric_name(),
                value=None,
                entity=f"Column({self.column})"
            )

        quantile_value = df.select(pl.col(self.column).quantile(self.quantile)).item()

        return Metric(
            name=self.metric_name(),
            value=quantile_value,
            entity=f"Column({self.column})"
        )

    def metric_name(self) -> str:
        return f"ApproxQuantile({self.column},{self.quantile})"
