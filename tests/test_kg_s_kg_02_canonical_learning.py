"""Canonical classification is preserved after retiring its public report.

Real publication/worker coverage remains in test_kg_r7_imp5.py.
"""
from okto_pulse.core.kg.canonical_partition_integrity import (
    CLASSIFICATION_CANONICAL_LEARNING_RESOLVED, CLASSIFICATION_INVALID_ORPHAN_LEARNING,
    CLASSIFICATION_MISSING_SOURCE, CLASSIFICATION_UNRESOLVED_SOURCE,
    CLASSIFICATION_WEAK_PROVENANCE, classify_canonical_learning,
)
from okto_pulse.core.kg.cognitive_policy import LEARNING_RELATES_TO_TARGETS
from okto_pulse.core.kg.source_maturity import GRAPH_LAYER_CANONICAL, GRAPH_LAYER_WORKING

def test_classifier_consumes_skg01_taxonomy_not_redefined():
    # The classifier must canonize for EXACTLY the seven S-KG-01 endpoints and no
    # others — proving it consumes LEARNING_RELATES_TO_TARGETS rather than holding
    # its own copy.
    for endpoint_type in LEARNING_RELATES_TO_TARGETS:
        assert classify_canonical_learning(
            source_ref="spec:spec-1:learning:0", is_bug_derived=False,
            relates_to_endpoints=((endpoint_type, GRAPH_LAYER_CANONICAL),),
        ) == CLASSIFICATION_CANONICAL_LEARNING_RESOLVED, endpoint_type

def test_classifier_missing_then_unresolved_source():
    assert classify_canonical_learning(
        source_ref="", is_bug_derived=False,
    ) == CLASSIFICATION_MISSING_SOURCE
    assert classify_canonical_learning(
        source_ref="   ", is_bug_derived=False,
    ) == CLASSIFICATION_MISSING_SOURCE
    assert classify_canonical_learning(
        source_ref="mystery:xyz", is_bug_derived=False,
    ) == CLASSIFICATION_UNRESOLVED_SOURCE
    assert classify_canonical_learning(
        source_ref="no-colon-ref", is_bug_derived=False,
    ) == CLASSIFICATION_UNRESOLVED_SOURCE

def test_classifier_bug_derived_paths():
    # >=1 canonical Bug canonizes (even mixed with working); working-only never.
    assert classify_canonical_learning(
        source_ref="card:bug:abc:learning:0", is_bug_derived=True,
        canonical_bug_count=1, working_bug_count=2,
    ) == CLASSIFICATION_CANONICAL_LEARNING_RESOLVED
    assert classify_canonical_learning(
        source_ref="card:bug:abc:learning:0", is_bug_derived=True,
        canonical_bug_count=0, working_bug_count=3,
    ) == CLASSIFICATION_WEAK_PROVENANCE

def test_classifier_non_bug_layer_and_no_edge_are_weak():
    # Right taxonomy type but WORKING layer is fail-closed (not canonical).
    assert classify_canonical_learning(
        source_ref="spec:spec-1:learning:0", is_bug_derived=False,
        relates_to_endpoints=(("Decision", GRAPH_LAYER_WORKING),),
    ) == CLASSIFICATION_WEAK_PROVENANCE
    # No association edge at all -> weak provenance (the refined provenance-only).
    assert classify_canonical_learning(
        source_ref="spec:spec-1:learning:0", is_bug_derived=False,
    ) == CLASSIFICATION_WEAK_PROVENANCE

def test_ts_kg02_07_off_taxonomy_endpoint_is_invalid_orphan_fail_closed():
    # The board graph cannot materialize a relates_to from a Learning to a
    # non-taxonomy type (the Grafx rel table only declares the seven endpoints), so
    # the fail-closed branch is exercised at the classification authority itself.
    for off_type in ("Alternative", "Assumption", "Bug", "Learning"):
        assert classify_canonical_learning(
            source_ref="spec:spec-1:learning:0", is_bug_derived=False,
            relates_to_endpoints=((off_type, GRAPH_LAYER_CANONICAL),),
        ) == CLASSIFICATION_INVALID_ORPHAN_LEARNING, off_type
    # A valid canonical taxonomy endpoint alongside an off-taxonomy one still
    # canonizes — off-taxonomy never MASKS a genuine canonical association.
    assert classify_canonical_learning(
        source_ref="spec:spec-1:learning:0", is_bug_derived=False,
        relates_to_endpoints=(
            ("Alternative", GRAPH_LAYER_CANONICAL),
            ("Decision", GRAPH_LAYER_CANONICAL),
        ),
    ) == CLASSIFICATION_CANONICAL_LEARNING_RESOLVED
