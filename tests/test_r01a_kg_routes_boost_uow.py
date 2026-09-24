"""F4 retires manual node score tuning; no hidden use case or writer remains."""

from okto_pulse.core.application import use_cases
from okto_pulse.core.application.use_cases import kg_routes_crud
from okto_pulse.core.kg import governance
from okto_pulse.core.ports import kg_governance
from okto_pulse.core.ports.permission_policy import flatten_permission_flags, registered_permission_flags
from okto_pulse.core.ports.permission_retirement import retired_feature_permission_flags


def test_manual_boost_is_absent_from_public_and_internal_contracts():
    for module in (use_cases, kg_routes_crud):
        for name in ("BoostNodeCommand", "BoostNodeResult", "BoostNodeUseCase"):
            assert not hasattr(module, name)
    for name in ("boost_node", "mutate_boost_node_graph", "stage_boost_node_audit", "BoostPersistError"):
        assert not hasattr(governance, name)
    assert not hasattr(kg_governance, "BoostAuditRecord")
    assert not hasattr(kg_governance.KGGovernanceStore, "add_boost_audit")
    assert "kg.operations.node.boost" not in flatten_permission_flags(registered_permission_flags())
    assert "kg.operations.node.boost" in retired_feature_permission_flags()
    assert callable(governance.purge_expired_audit)
    assert callable(governance.right_to_erasure)
