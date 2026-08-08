from __future__ import annotations

import inspect

from okto_pulse.core.ports.research_decision_ledger import (
    ResearchDecisionLedgerPersistencePort,
    ResearchDecisionLedgerReadPort,
)


def test_ports_are_edition_and_orm_neutral() -> None:
    source = inspect.getsource(
        __import__(
            "okto_pulse.core.ports.research_decision_ledger",
            fromlist=["*"],
        )
    ).lower()

    assert "sqlalchemy" not in source
    assert "okto_pulse.community" not in source
    assert "okto_pulse.core.models" not in source


def test_persistence_port_has_no_transaction_owner_operations() -> None:
    assert hasattr(ResearchDecisionLedgerPersistencePort, "apply_bundle_cas")
    assert not hasattr(ResearchDecisionLedgerPersistencePort, "commit")
    assert not hasattr(ResearchDecisionLedgerPersistencePort, "rollback")
    assert not hasattr(ResearchDecisionLedgerPersistencePort, "close")
    assert issubclass(
        ResearchDecisionLedgerPersistencePort,
        ResearchDecisionLedgerReadPort,
    )
