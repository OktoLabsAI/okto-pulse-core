from __future__ import annotations

import hashlib

from okto_pulse.core.application.boundary.public_contract_manifest import (
    PUBLIC_CORE_CONTRACT_MANIFEST_DIGEST,
    PUBLIC_CORE_CONTRACT_MANIFEST_VERSION,
    PUBLIC_CORE_CONTRACT_SURFACES,
    is_public_core_contract,
)


def test_public_contract_manifest_is_stable_unique_and_package_safe() -> None:
    assert PUBLIC_CORE_CONTRACT_MANIFEST_VERSION == "1.0"
    assert PUBLIC_CORE_CONTRACT_SURFACES == tuple(sorted(PUBLIC_CORE_CONTRACT_SURFACES))
    assert len(PUBLIC_CORE_CONTRACT_SURFACES) == len(set(PUBLIC_CORE_CONTRACT_SURFACES))
    assert PUBLIC_CORE_CONTRACT_MANIFEST_DIGEST == hashlib.sha256(
        "\n".join(PUBLIC_CORE_CONTRACT_SURFACES).encode("utf-8")
    ).hexdigest()

    forbidden = (
        "okto_pulse.core.infra.database",
        "okto_pulse.core.models.db",
        "okto_pulse.core.repositories.sqlalchemy",
        "okto_pulse.core.services.main",
        "okto_pulse.core.mcp.server",
    )
    assert not any(
        item == prefix or item.startswith(prefix + ".")
        for item in PUBLIC_CORE_CONTRACT_SURFACES
        for prefix in forbidden
    )


def test_public_contract_resolution_is_exact_or_descendant_only() -> None:
    assert (
        "okto_pulse.core.composition.isolated_runtime_provider_scope"
        in PUBLIC_CORE_CONTRACT_SURFACES
    )
    assert is_public_core_contract(
        "okto_pulse.core.composition.isolated_runtime_provider_scope"
    )
    assert is_public_core_contract("okto_pulse.core.ports.design_system")
    assert is_public_core_contract(
        "okto_pulse.core.domain.knowledge_selection.KnowledgeSelection"
    )
    assert is_public_core_contract(
        "okto_pulse.core.domain.resource_revision.ResourceRevisionStamp"
    )
    assert is_public_core_contract(
        "okto_pulse.core.ports.knowledge_propagation.KnowledgePropagationPort"
    )
    assert is_public_core_contract(
        "okto_pulse.core.services.resource_lineage.ResolvedResourceLineageService"
    )
    assert is_public_core_contract(
        "okto_pulse.core.domain.knowledge_fingerprint."
        "resolve_knowledge_content_sha256"
    )
    assert not is_public_core_contract("okto_pulse.core.services.main")
    assert not is_public_core_contract("okto_pulse.core.models.db.Card")
    assert not is_public_core_contract("okto_pulse.community.app")
