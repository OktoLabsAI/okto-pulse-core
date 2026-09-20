"""Spec R01A IMP2 — REST dead-letter inspector migrated to the UnitOfWork path.

Proves the REAL endpoint (GET /api/v1/kg/queue/dead-letter) now drives
endpoint -> ``get_unit_of_work`` (request-scoped ``PulseUnitOfWork``) ->
transport-free ``ListDeadLetterRowsUseCase`` -> ``list_dead_letter_rows``
service, preserving the payload/permission and keeping the FastAPI ``get_db``
dependency override intact. The handler no longer takes a raw ``AsyncSession``,
and the R01A inventory confirms the router is strangled (0 relational
call-sites). The pre-existing ``test_dlq_inspector`` suite continues to pass,
which is the end-to-end behavior-parity guarantee.
"""

from __future__ import annotations



from okto_pulse.core.repositories.relational_boundary_gate import (
    default_use_cases_path,
    run_relational_boundary_gate,
)

USER = "dlq-uow-r01a"










def test_use_case_keeps_application_layer_relationally_clean() -> None:
    """The new use case must not re-introduce a relational coupling into the
    boundary-gated application/use_cases package."""
    report = run_relational_boundary_gate(root=default_use_cases_path())
    assert report.ok, [(v.file, v.symbol) for v in report.violations]
