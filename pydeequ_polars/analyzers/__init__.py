"""Analyzers for computing data quality metrics on Polars DataFrames."""

from pydeequ_polars.analyzers.analyzer import Analyzer
from pydeequ_polars.analyzers.size import Size
from pydeequ_polars.analyzers.completeness import Completeness
from pydeequ_polars.analyzers.uniqueness import Uniqueness
from pydeequ_polars.analyzers.distinctness import Distinctness
from pydeequ_polars.analyzers.min_max import Minimum, Maximum
from pydeequ_polars.analyzers.mean import Mean
from pydeequ_polars.analyzers.sum import Sum
from pydeequ_polars.analyzers.standard_deviation import StandardDeviation
from pydeequ_polars.analyzers.approx_quantile import ApproxQuantile
from pydeequ_polars.analyzers.pattern_match import PatternMatch
from pydeequ_polars.analyzers.correlation import Correlation

__all__ = [
    "Analyzer",
    "Size",
    "Completeness",
    "Uniqueness",
    "Distinctness",
    "Minimum",
    "Maximum",
    "Mean",
    "Sum",
    "StandardDeviation",
    "ApproxQuantile",
    "PatternMatch",
    "Correlation",
]
