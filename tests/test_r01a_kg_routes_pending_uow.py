"""F4: retired queue routes and contracts cannot resolve a work unit."""
from okto_pulse.community.api import kg_routes
from okto_pulse.core.application import use_cases
from okto_pulse.core.kg import dashboard_readers, governance
from okto_pulse.core.ports import kg_operational


def test_public_queue_contracts_are_absent():
    for name in ("list_pending", "list_pending_tree", "retry_pending_entry"):
        assert not hasattr(kg_routes, name)
    for name in ("ListPendingUseCase", "ListPendingTreeUseCase", "RetryPendingEntryUseCase"):
        assert not hasattr(use_cases, name)
    for name in ("list_pending_entries", "build_pending_tree"):
        assert not hasattr(dashboard_readers, name)
        assert not hasattr(kg_operational.KGOperationalReadModelPort, name)
    assert not hasattr(governance, "retry_pending_entry")
    assert not hasattr(kg_operational.KGWorkerQueuePort, "retry_pending_entry")
    assert hasattr(kg_operational.KGWorkerQueuePort, "reprocess_dead_letter_rows")
