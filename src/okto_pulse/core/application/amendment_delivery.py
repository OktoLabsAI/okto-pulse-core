"""Edition-facing current amendment coverage and immutable lineage facts.

Persistence adapters use this contract to evaluate existing reserved coverage
against the current evidence basis. Implementations and eligibility policy stay
owned by Core; this surface does not confer attestation or mutation authority.
"""

from okto_pulse.core.services.amendment_coverage import current_coverage_basis
from okto_pulse.core.services.bug_regression_scenarios import (
    AmendmentLineageFact,
    CoverageConfirmationFact,
    evaluate_path_b_for_scenario,
)

__all__ = [
    "AmendmentLineageFact", "CoverageConfirmationFact",
    "current_coverage_basis", "evaluate_path_b_for_scenario",
]
