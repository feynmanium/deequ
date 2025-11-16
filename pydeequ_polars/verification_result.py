"""Verification result classes."""

from typing import Dict
from pydeequ_polars.checks.check import Check, CheckResult, CheckStatus


class VerificationResult:
    """
    Result of running a VerificationSuite.

    Contains the overall status and results for each check.
    """

    def __init__(self, status: CheckStatus, check_results: Dict[Check, CheckResult]):
        self.status = status
        self.check_results = check_results

    def __repr__(self):
        return f"VerificationResult(status={self.status.value}, checks={len(self.check_results)})"
