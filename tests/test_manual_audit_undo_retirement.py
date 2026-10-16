"""F4: manual graph undo has no hidden Core implementation or exclusive port."""
from okto_pulse.core.kg import governance
from okto_pulse.core.kg.interfaces.audit_repository import AuditRepository
from okto_pulse.core.ports import kg_governance


def test_manual_undo_contracts_are_absent_but_audit_contract_is_preserved():
    assert not hasattr(governance, "undo_session")
    assert not hasattr(kg_governance, "GovernanceUndoFact")
    assert not hasattr(kg_governance.KGGovernanceStore, "get_undo_fact")
    assert not hasattr(kg_governance.KGGovernanceStore, "mark_session_undone")
    assert callable(AuditRepository.mark_audit_undone)
    assert callable(governance.purge_expired_audit)
    assert callable(governance.right_to_erasure)
