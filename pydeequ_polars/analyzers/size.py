"""Size analyzer for counting rows in a DataFrame."""

import polars as pl
from pydeequ_polars.analyzers.analyzer import Analyzer, Metric


class Size(Analyzer):
    """Analyzer to compute the number of rows in a DataFrame."""

    def calculate(self, df: pl.DataFrame) -> Metric:
        """Calculate the number of rows."""
        count = df.height
        return Metric(name=self.metric_name(), value=count, entity="Dataset")

    def metric_name(self) -> str:
        return "Size"
