"""Pattern Match analyzer for regex matching."""

import polars as pl
from pydeequ_polars.analyzers.analyzer import Analyzer, Metric


class PatternMatch(Analyzer):
    """Analyzer to compute the ratio of values matching a regex pattern."""

    def __init__(self, column: str, pattern: str):
        self.column = column
        self.pattern = pattern

    def calculate(self, df: pl.DataFrame) -> Metric:
        """Calculate the ratio of values matching the pattern."""
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

        # Count non-null values that match the pattern
        match_count = df.select(
            pl.col(self.column).str.contains(self.pattern).fill_null(False).sum()
        ).item()

        ratio = match_count / total

        return Metric(
            name=self.metric_name(),
            value=ratio,
            entity=f"Column({self.column})"
        )

    def metric_name(self) -> str:
        return f"PatternMatch({self.column})"
