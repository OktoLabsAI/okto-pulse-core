"""Authored batch shape, stable scopes and exact adopted-source preflight."""

import copy
import json
from dataclasses import replace

import pytest
from pydantic import ValidationError

from okto_pulse.core.domain.architecture_candidates import (
    AdoptedArchitectureDesign,
    ArchitectureCandidateIssue,
    project_architecture_candidates,
)
from okto_pulse.core.domain.architecture_classification import (
    MAX_CLASSIFICATION_BYTES,
    ArchitectureClassificationBatch,
    ArchitectureClassificationError,
    resolve_architecture_classification,
)


def population():
    return project_architecture_candidates(
        board_id="board",
        spec_id="spec",
        spec_edition=2,
        source_complete=True,
        designs=(
            AdoptedArchitectureDesign(
                "board",
                "adopted",
                "root",
                3,
                (
                    {
                        "id": "boundary",
                        "contract_type": "event",
                        "event_schema": {
                            "properties": {"order/id": {}, "items": ["first", "second"]}
                        },
                        "error_contract": {},
                        "schema_ref": "https://private.invalid/contract",
                        "participants": ["producer-or-consumer", "other-participant"],
                    },
                ),
            ),
        ),
    )


def decision(**overrides):
    source = population().candidates[0]
    return (
        dict(
            candidate_ref=source.id,
            expected_source_digest=source.source_digest,
            disposition="context_only",
            reason="Existing interaction outside this change",
        )
        | overrides
    )


def request(*decisions, **overrides):
    return (
        dict(
            expected_spec_version=7,
            expected_spec_edition=2,
            idempotency_key="classify-orders",
            decisions=list(decisions or [decision()]),
        )
        | overrides
    )


def resolve(batch, *, sources=None, irs=()):
    return resolve_architecture_classification(
        batch,
        spec_id="spec",
        population=sources or population(),
        integration_requirements=irs,
    )


def test_classification_preserves_source_and_does_not_infer_provider_consumer_or_http():
    raw = request(
        decision(
            disposition="promote_to_ir",
            reason=None,
            integration_requirements=[
                {
                    "title": "Publish orders",
                    "integration_type": "event",
                }
            ],
        )
    )
    original = copy.deepcopy(raw)
    resolved = resolve(ArchitectureClassificationBatch.model_validate(raw))
    assert raw == original
    assert resolved[0].candidate.contract == population().candidates[0].contract
    assert resolved[0].intent.integration_requirements == (
        {"title": "Publish orders", "integration_type": "event"},
    )
    assert "provider" not in resolved[0].intent.integration_requirements[0]
    assert (
        resolved[0].candidate.contract["schema_ref"]
        == "https://private.invalid/contract"
    )


@pytest.mark.parametrize(
    "change",
    [
        {"unexpected": True},
        {"expected_spec_version": True},
        {"expected_spec_edition": 0},
        {"idempotency_key": "   "},
        {"idempotency_key": "k" * 256},
        {"decisions": []},
    ],
)
def test_batch_rejects_unknown_fields_missing_identity_and_invalid_fences(change):
    with pytest.raises(ValidationError):
        ArchitectureClassificationBatch.model_validate(request(**change))


@pytest.mark.parametrize(
    "change",
    [
        {"unexpected": True},
        {"reason": " "},
        {"expected_source_digest": "unknown"},
        {"scope_paths": []},
        {"scope_paths": ["/event_schema~x"], "remainder_reason": "Other context"},
        {"disposition": "pending"},
        {"integration_requirement_refs": ["ir_existing"]},
        {
            "disposition": "promote_to_ir",
            "reason": None,
            "integration_requirements": [{"title": "Ambiguous"}],
        },
        {
            "disposition": "promote_to_ir",
            "reason": None,
            "integration_requirements": [
                {
                    "title": "Waived",
                    "integration_type": "event",
                    "status": "not_applicable",
                }
            ],
        },
        {
            "disposition": "associate_existing_ir",
            "reason": None,
            "integration_requirement_refs": ["ir_same", "ir_same"],
        },
        {"scope_paths": ["/event_schema"]},
        {"remainder_reason": "There is no remainder"},
    ],
)
def test_decision_rejects_implicit_or_incoherent_classification(change):
    with pytest.raises(ValidationError):
        ArchitectureClassificationBatch.model_validate(request(decision(**change)))


def test_batch_count_and_utf8_byte_limit_are_independent():
    raw = request(
        *[decision(candidate_ref=f"candidate-{index}") for index in range(50)]
    )
    assert len(ArchitectureClassificationBatch.model_validate(raw).decisions) == 50
    raw["decisions"].append(decision(candidate_ref="one-too-many"))
    with pytest.raises(ValidationError):
        ArchitectureClassificationBatch.model_validate(raw)
    raw = request()
    raw["decisions"][0]["reason"] = ""
    overhead = len(
        json.dumps(
            raw, sort_keys=True, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
    )
    available = MAX_CLASSIFICATION_BYTES - overhead
    raw["decisions"][0]["reason"] = "á" * (available // 2) + "x" * (available % 2)
    ArchitectureClassificationBatch.model_validate(raw)
    raw["decisions"][0]["reason"] += "x"
    with pytest.raises(ValidationError, match="payload_too_large"):
        ArchitectureClassificationBatch.model_validate(raw)


def test_request_digest_binds_actor_scope_versions_and_authored_intent():
    raw = request()
    batch = ArchitectureClassificationBatch.model_validate(raw)
    digest = batch.request_digest(board_id="board", spec_id="spec", actor_id="author")
    reordered = ArchitectureClassificationBatch.model_validate(
        dict(reversed(list(raw.items())))
    )
    assert (
        reordered.request_digest(board_id="board", spec_id="spec", actor_id="author")
        == digest
    )
    for scope in [{"board_id": "other"}, {"spec_id": "other"}, {"actor_id": "other"}]:
        args = dict(board_id="board", spec_id="spec", actor_id="author") | scope
        assert batch.request_digest(**args) != digest
    revised = ArchitectureClassificationBatch.model_validate(
        request(expected_spec_version=8)
    )
    assert (
        revised.request_digest(board_id="board", spec_id="spec", actor_id="author")
        != digest
    )


def test_named_scope_fragments_allow_shared_remainder_reason_and_many_to_many_irs():
    first = decision(
        disposition="associate_existing_ir",
        reason=None,
        integration_requirement_refs=["ir_one", "ir_two"],
        scope_paths=["/event_schema/properties/order~1id"],
        remainder_reason="Other clauses provide context",
    )
    second = decision(scope_paths=["/error_contract"], reason="Errors unchanged")
    batch = ArchitectureClassificationBatch.model_validate(request(first, second))
    resolved = resolve(
        batch,
        irs=(
            {"id": "ir_one", "status": "active"},
            {"id": "ir_two", "status": "active"},
        ),
    )
    assert len(resolved) == 2
    assert resolved[0].intent.integration_requirement_refs == ("ir_one", "ir_two")


@pytest.mark.parametrize(
    "paths",
    [
        ("", "/event_schema"),
        ("/event_schema", "/event_schema/properties"),
        ("/error_contract", "/error_contract"),
    ],
)
def test_overlapping_scope_fragments_do_not_silently_win_by_order(paths):
    with pytest.raises(ValidationError, match="scope_overlap"):
        ArchitectureClassificationBatch.model_validate(
            request(
                decision(scope_paths=[paths[0]], remainder_reason="Other clauses"),
                decision(scope_paths=[paths[1]]),
            )
        )


@pytest.mark.parametrize(
    "path", ["/missing", "/request_schema", "/event_schema/properties/items/0"]
)
def test_missing_null_and_positional_array_fragments_are_not_stable_scopes(path):
    batch = ArchitectureClassificationBatch.model_validate(
        request(decision(scope_paths=[path], remainder_reason="Other clauses"))
    )
    with pytest.raises(ArchitectureClassificationError, match="scope_unresolved"):
        resolve(batch)


@pytest.mark.parametrize(
    "case",
    ["unknown", "digest", "edition", "foreign_spec", "unavailable", "conflicting"],
)
def test_source_preflight_rejects_stale_foreign_missing_and_divergent_candidates(case):
    sources = population()
    intent = decision()
    if case == "unknown":
        intent["candidate_ref"] = "unknown"
    elif case == "digest":
        intent["expected_source_digest"] = "d" * 64
    elif case == "unavailable":
        sources = replace(sources, source_complete=False)
    else:
        candidate = sources.candidates[0]
        if case == "edition":
            sources = replace(sources, candidates=(replace(candidate, spec_edition=3),))
        elif case == "foreign_spec":
            sources = replace(
                sources, candidates=(replace(candidate, spec_id="other"),)
            )
        else:
            sources = replace(
                sources,
                candidates=(
                    *sources.candidates,
                    replace(candidate, source_digest="e" * 64),
                ),
            )
    with pytest.raises(ArchitectureClassificationError):
        resolve(
            ArchitectureClassificationBatch.model_validate(request(intent)),
            sources=sources,
        )


@pytest.mark.parametrize(
    "status", ["revoked", "superseded", "not_applicable", "missing"]
)
def test_reuse_requires_an_active_ir_in_this_spec(status):
    batch = ArchitectureClassificationBatch.model_validate(
        request(
            decision(
                disposition="associate_existing_ir",
                reason=None,
                integration_requirement_refs=["ir_one"],
            )
        )
    )
    irs = () if status == "missing" else ({"id": "ir_one", "status": status},)
    with pytest.raises(ArchitectureClassificationError, match="ir_not_active_in_spec"):
        resolve(batch, irs=irs)


def test_context_classification_never_mutates_existing_normative_irs():
    irs = ({"id": "ir_one", "status": "active", "title": "Still normative"},)
    original = copy.deepcopy(irs)
    assert (
        len(resolve(ArchitectureClassificationBatch.model_validate(request()), irs=irs))
        == 1
    )
    assert irs == original


@pytest.mark.parametrize(
    "irs,reference",
    [
        (({"id": None, "status": "active"},), "None"),
        (({"id": 12, "status": "active"},), "12"),
        (({"id": "duplicate"}, {"id": "duplicate"}), "duplicate"),
    ],
)
def test_reuse_never_coerces_missing_ids_or_chooses_between_duplicate_ids(
    irs, reference
):
    batch = ArchitectureClassificationBatch.model_validate(
        request(
            decision(
                disposition="associate_existing_ir",
                reason=None,
                integration_requirement_refs=[reference],
            )
        )
    )
    with pytest.raises(ArchitectureClassificationError, match="ir_not_active_in_spec"):
        resolve(batch, irs=irs)


def test_only_targeted_candidate_issues_prevent_resolving_an_available_source():
    sources = population()
    issue = ArchitectureCandidateIssue(
        "architecture_contract_unresolved", candidate_id="other"
    )
    batch = ArchitectureClassificationBatch.model_validate(request())
    assert len(resolve(batch, sources=replace(sources, issues=(issue,)))) == 1
    issue = replace(issue, candidate_id=sources.candidates[0].id)
    with pytest.raises(ArchitectureClassificationError, match="candidate_unresolved"):
        resolve(batch, sources=replace(sources, issues=(issue,)))
