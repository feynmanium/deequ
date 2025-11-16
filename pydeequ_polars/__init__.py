"""
PyDeequ Polars - Data Quality Library for Polars DataFrames

A Python implementation of Deequ for data quality validation using Polars.
"""

from pydeequ_polars.verification_suite import VerificationSuite
from pydeequ_polars.checks.check import Check, CheckLevel, CheckStatus
from pydeequ_polars.verification_result import VerificationResult

__version__ = "0.1.0"
__all__ = ["VerificationSuite", "Check", "CheckLevel", "CheckStatus", "VerificationResult"]
