"""Focused contract tests for Core-owned data-bootstrap domains."""

from __future__ import annotations

from typing import get_args

from okto_pulse.core.ports.data_bootstrapper import (
    BOOTSTRAP_DOMAINS,
    BootstrapDomain,
    DataBootstrapStep,
)


def test_knowledge_propagation_is_a_canonical_data_bootstrap_domain() -> None:
    assert BOOTSTRAP_DOMAINS == (
        "presets",
        "permissions",
        "discovery_intents",
        "knowledge_propagation",
    )
    assert set(get_args(BootstrapDomain)) == set(BOOTSTRAP_DOMAINS)

    step = DataBootstrapStep(
        step_id="backfill_knowledge_propagation_v2",
        order=5,
        owner="community",
        domain="knowledge_propagation",
        idempotent=True,
        metadata={
            "phase": "post_schema",
            "execution": "resumable",
        },
    )

    assert step.domain == "knowledge_propagation"
    assert step.idempotent is True
