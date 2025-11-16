"""Base Analyzer class for computing metrics."""

from abc import ABC, abstractmethod
from typing import Any, Optional
import polars as pl


class Metric:
    """Represents a computed metric value."""

    def __init__(self, name: str, value: Optional[Any], entity: str = "Dataset"):
        self.name = name
        self.value = value
        self.entity = entity

    def __repr__(self):
        return f"Metric(name='{self.name}', entity='{self.entity}', value={self.value})"


class Analyzer(ABC):
    """Base class for all analyzers that compute metrics on DataFrames."""

    @abstractmethod
    def calculate(self, df: pl.DataFrame) -> Metric:
        """
        Calculate the metric on the given DataFrame.

        Args:
            df: Polars DataFrame to analyze

        Returns:
            Metric object containing the computed value
        """
        pass

    @abstractmethod
    def metric_name(self) -> str:
        """Return the name of the metric this analyzer computes."""
        pass
