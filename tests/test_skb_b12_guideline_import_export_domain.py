"""SK-B/B12 pure ``guideline-export/v2`` codec and import-plan contracts."""

from __future__ import annotations

import copy
import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from okto_pulse.core.domain.guideline_import_export import (
    GUIDELINE_EXPORT_CONTRACT_VERSION,
    GUIDELINE_EXPORT_LEGACY_BASELINE_VERSION,
    ExistingGuidelineRevision,
    GuidelineBindingMaterialization,
    GuidelineExportAggregate,
    GuidelineExportBinding,
    GuidelineExportRevision,
    GuidelineExportSnapshot,
    GuidelineHistoryStatus,
    GuidelineImportBindingDisposition,
    GuidelineImportExportError,
    GuidelineImportResult,
    GuidelineImportRevisionDisposition,
    GuidelineImportTransactionStatus,
    build_guideline_export_v2,
    canonical_guideline_json_bytes,
    guideline_export_json_bytes,
    guideline_export_payload,
    parse_guideline_export,
    plan_guideline_import,
)
from okto_pulse.core.domain.guideline_lifecycle import (
    guideline_revision_content_digest_v1,
)
from okto_pulse.core.domain.guideline_policy import (
    BoardGuidelineBinding,
    Guideline,
    GuidelineBindingProvenance,
    GuidelineBindingState,
    GuidelineEnforcement,
    GuidelineHead,
    GuidelineLifecycleStatus,
    GuidelinePredicate,
    GuidelineRetirement,
    GuidelineRevision,
    GuidelineRule,
    GuidelineRuleOperator,
    GuidelineScope,
    POLICY_SQL_INTEGER_MAX,
    PolicyEntityType,
)


NOW = datetime(2026, 7, 29, 18, tzinfo=timezone.utc)


def _rule(*, blocking: bool = False) -> GuidelineRule:
    return GuidelineRule(
        rule_id="rule-1",
        code="quality.require_review",
        title="Require review",
        description="The artifact must carry review evidence.",
        target_entity_types=(PolicyEntityType.SPEC,),
        predicates=(
            GuidelinePredicate(
                "exists",
                (("fact", "review_evidence"),),
            ),
        ),
        enforcement=(
            GuidelineEnforcement.BLOCKING if blocking else GuidelineEnforcement.ADVISORY
        ),
        operator=GuidelineRuleOperator.ALL,
    )


def _revision(
    *,
    guideline_id: str = "guideline-1",
    revision_id: str = "revision-1",
    revision_number: int = 1,
    semantic_version: str = "1.0.0",
    parent_revision_id: str | None = None,
    blocking: bool = False,
    content: str | None = None,
) -> GuidelineRevision:
    title = f"Guideline {revision_number}"
    resolved_content = content or f"Content {revision_number}"
    rules = (_rule(blocking=blocking),)
    tags = ("quality",)
    return GuidelineRevision(
        revision_id=revision_id,
        guideline_id=guideline_id,
        revision_number=revision_number,
        semantic_version=semantic_version,
        title=title,
        content=resolved_content,
        content_digest=guideline_revision_content_digest_v1(
            title=title,
            content=resolved_content,
            rules=rules,
            tags=tags,
        ),
        rules=rules,
        tags=tags,
        created_by="actor-1",
        created_at=NOW + timedelta(minutes=revision_number),
        parent_revision_id=parent_revision_id,
    )


def _exported_revision(
    revision: GuidelineRevision,
) -> GuidelineExportRevision:
    return GuidelineExportRevision(
        revision=revision,
        published_head_revision=revision.revision_number,
        published_head_updated_at=revision.created_at,
    )


def _exported_binding(
    revision: GuidelineRevision,
    *,
    board_id: str = "board-1",
    binding_id: str = "binding-1",
    binding_revision: int = 1,
    state: GuidelineBindingState = GuidelineBindingState.ACTIVE,
    materialization: GuidelineBindingMaterialization = (
        GuidelineBindingMaterialization.LIVE
    ),
    evidence_suffix: str = "a",
) -> GuidelineExportBinding:
    return GuidelineExportBinding(
        binding=BoardGuidelineBinding(
            binding_id=binding_id,
            board_id=board_id,
            guideline_id=revision.guideline_id,
            revision_id=revision.revision_id,
            semantic_version=revision.semantic_version,
            revision_digest=revision.content_digest,
            priority=10,
            binding_revision=binding_revision,
            adopted_by="actor-1",
            adopted_at=NOW + timedelta(minutes=10 + binding_revision),
            default_enforcement=GuidelineEnforcement.ADVISORY,
            state=state,
            source_kind=GuidelineBindingProvenance.NATIVE,
        ),
        physical_source_kind="guideline_board_bindings",
        binding_origin="native",
        materialization=materialization,
        evidence_refs=(
            ("impact_receipt_id", f"impact-{evidence_suffix}"),
            ("request_digest", evidence_suffix * 64),
        ),
    )


def _aggregate(
    *,
    guideline_id: str = "guideline-1",
    owner_id: str = "actor-1",
    board_id: str | None = None,
    revisions: tuple[GuidelineRevision, ...] | None = None,
    bindings: tuple[GuidelineExportBinding, ...] | None = None,
    retired: bool = False,
) -> GuidelineExportAggregate:
    resolved_revisions = revisions or (_revision(guideline_id=guideline_id),)
    latest = resolved_revisions[-1]
    scope = GuidelineScope.INLINE if board_id is not None else GuidelineScope.GLOBAL
    retirement = (
        GuidelineRetirement(
            retirement_id=f"retirement-{guideline_id}",
            guideline_id=guideline_id,
            status=GuidelineLifecycleStatus.RETIRED,
            retired_revision_id=latest.revision_id,
            retired_revision_number=latest.revision_number,
            retired_semantic_version=latest.semantic_version,
            retired_revision_digest=latest.content_digest,
            retired_head_revision=latest.revision_number,
            reason="No longer used.",
            retired_by="actor-1",
            retired_at=NOW + timedelta(hours=1),
        )
        if retired
        else None
    )
    return GuidelineExportAggregate(
        identity=Guideline(
            guideline_id=guideline_id,
            owner_id=owner_id,
            scope=scope,
            board_id=board_id,
            created_at=NOW,
        ),
        revisions=tuple(_exported_revision(item) for item in resolved_revisions),
        head=GuidelineHead(
            guideline_id=guideline_id,
            revision_id=latest.revision_id,
            revision_number=latest.revision_number,
            semantic_version=latest.semantic_version,
            head_revision=latest.revision_number,
            updated_at=latest.created_at,
        ),
        retirement=retirement,
        bindings=(bindings if bindings is not None else (_exported_binding(latest),)),
    )


def _envelope(
    aggregate: GuidelineExportAggregate,
    *,
    source_board_id: str | None = None,
):
    return build_guideline_export_v2(
        GuidelineExportSnapshot(
            aggregates=(aggregate,),
            source_board_id=source_board_id,
        ),
        exported_at=NOW + timedelta(days=1),
    )


def test_v2_round_trip_is_closed_complete_and_canonical() -> None:
    first = _revision(blocking=True)
    second = _revision(
        revision_id="revision-2",
        revision_number=2,
        semantic_version="2.0.0",
        parent_revision_id=first.revision_id,
        blocking=True,
    )
    binding_1 = _exported_binding(first)
    binding_2 = _exported_binding(
        second,
        binding_revision=2,
        evidence_suffix="b",
    )
    aggregate = _aggregate(
        revisions=(first, second),
        bindings=(binding_1, binding_2),
        retired=True,
    )
    envelope = _envelope(aggregate)
    payload = guideline_export_payload(envelope)
    parsed = parse_guideline_export(payload)

    assert envelope.contract_version == GUIDELINE_EXPORT_CONTRACT_VERSION
    assert parsed == envelope
    assert parsed.guidelines[0].identity == aggregate.identity
    assert parsed.guidelines[0].retirement == aggregate.retirement
    assert parsed.guidelines[0].bindings[1].binding_digest == (
        aggregate.bindings[1].binding_digest
    )
    assert parsed.guidelines[0].bindings[1].physical_source_kind == (
        "guideline_board_bindings"
    )
    assert parsed.guidelines[0].revisions[1].published_head_revision == 2
    assert guideline_export_json_bytes(parsed) == canonical_guideline_json_bytes(
        payload
    )
    assert b'"kind":"guidelines"' in guideline_export_json_bytes(parsed)
    assert b'": "' not in guideline_export_json_bytes(parsed)
    assert b'", "' not in guideline_export_json_bytes(parsed)


def test_digest_and_bytes_ignore_object_key_order_but_reject_unknown_fields() -> None:
    payload = guideline_export_payload(_envelope(_aggregate()))
    reordered = {key: copy.deepcopy(payload[key]) for key in reversed(tuple(payload))}
    reordered["guidelines"][0]["identity"] = {
        key: reordered["guidelines"][0]["identity"][key]
        for key in reversed(tuple(reordered["guidelines"][0]["identity"]))
    }

    assert parse_guideline_export(reordered).content_digest == payload["content_digest"]
    assert canonical_guideline_json_bytes(reordered) == (
        canonical_guideline_json_bytes(payload)
    )

    invalid = copy.deepcopy(payload)
    invalid["guidelines"][0]["identity"]["surprise"] = True
    with pytest.raises(
        GuidelineImportExportError,
        match="guideline_export_unknown_field",
    ):
        parse_guideline_export(invalid)


def test_tampered_revision_or_envelope_digest_fails_before_planning() -> None:
    payload = guideline_export_payload(_envelope(_aggregate()))
    tampered_revision = copy.deepcopy(payload)
    tampered_revision["guidelines"][0]["revisions"][0]["content"] = "tampered"
    with pytest.raises(
        GuidelineImportExportError,
        match="guideline_export_revision_digest_mismatch",
    ):
        parse_guideline_export(tampered_revision)

    tampered_envelope = copy.deepcopy(payload)
    tampered_envelope["content_digest"] = "f" * 64
    with pytest.raises(
        GuidelineImportExportError,
        match="guideline_export_content_digest_mismatch",
    ):
        parse_guideline_export(tampered_envelope)


def test_publication_provenance_rejects_revision_local_tampering() -> None:
    revision = _revision()

    with pytest.raises(
        GuidelineImportExportError,
        match="guideline_export_published_head_revision_invalid",
    ):
        GuidelineExportRevision(
            revision=revision,
            published_head_revision=revision.revision_number + 1,
            published_head_updated_at=revision.created_at,
        )

    with pytest.raises(
        GuidelineImportExportError,
        match="guideline_export_published_head_time_before_revision",
    ):
        GuidelineExportRevision(
            revision=revision,
            published_head_revision=revision.revision_number,
            published_head_updated_at=revision.created_at - timedelta(seconds=1),
        )


def test_legacy_version_text_and_integer_projection_match_durable_bounds() -> None:
    revision = _revision()
    boundary = GuidelineExportRevision(
        revision=revision,
        legacy_version="9" * 64,
        legacy_version_unresolvable=True,
    )
    assert boundary.legacy_version == "9" * 64
    assert boundary.legacy_version_as_int is None

    persisted_integer = GuidelineExportRevision(
        revision=revision,
        legacy_version=str(POLICY_SQL_INTEGER_MAX),
        legacy_version_unresolvable=True,
    )
    assert persisted_integer.legacy_version_as_int == POLICY_SQL_INTEGER_MAX

    with pytest.raises(
        GuidelineImportExportError,
        match="guideline_export_legacy_version_invalid",
    ):
        GuidelineExportRevision(
            revision=revision,
            legacy_version="9" * 65,
            legacy_version_unresolvable=True,
        )


def test_publication_provenance_rejects_history_and_head_time_tampering() -> None:
    first = _revision()
    second = _revision(
        revision_id="revision-2",
        revision_number=2,
        semantic_version="1.1.0",
        parent_revision_id=first.revision_id,
    )
    aggregate = _aggregate(
        revisions=(first, second),
        bindings=(),
    )
    first_publication = first.created_at + timedelta(hours=2)
    second_publication = second.created_at + timedelta(hours=1)

    with pytest.raises(
        GuidelineImportExportError,
        match="guideline_export_published_head_time_not_monotonic",
    ):
        replace(
            aggregate,
            revisions=(
                GuidelineExportRevision(
                    revision=first,
                    published_head_updated_at=first_publication,
                ),
                GuidelineExportRevision(
                    revision=second,
                    published_head_updated_at=second_publication,
                ),
            ),
            head=replace(
                aggregate.head,
                updated_at=second_publication,
            ),
        )

    with pytest.raises(
        GuidelineImportExportError,
        match="guideline_export_head_publication_time_mismatch",
    ):
        replace(
            aggregate,
            head=replace(
                aggregate.head,
                updated_at=aggregate.head.updated_at + timedelta(seconds=1),
            ),
        )


def test_legacy_v1_becomes_contextual_baseline_and_drops_blocking_rules() -> None:
    envelope = parse_guideline_export(
        {
            "schema_version": 1,
            "kind": "guidelines",
            "items": [
                {
                    "title": "Legacy policy",
                    "content": "Legacy prose.",
                    "tags": ["legacy"],
                    "scope": "inline",
                    "board_id": "source-board",
                    "version": 17,
                    "blocking": True,
                    "rules": [
                        {
                            "code": "legacy.block",
                            "enforcement": "blocking",
                        }
                    ],
                }
            ],
        }
    )
    aggregate = envelope.guidelines[0]
    revision = aggregate.revisions[0]

    assert envelope.exported_at == datetime(1970, 1, 1, tzinfo=timezone.utc)
    assert envelope.source_schema_version == "1"
    assert aggregate.history_status is GuidelineHistoryStatus.BASELINE_ONLY
    assert aggregate.bindings == ()
    assert revision.semantic_version == GUIDELINE_EXPORT_LEGACY_BASELINE_VERSION
    assert revision.revision.rules == ()
    assert revision.legacy_version == "17"
    assert revision.legacy_version_as_int == 17
    assert revision.legacy_version_unresolvable is True
    assert revision.legacy_tags == ("legacy",)
    assert "legacy_rules_dropped_contextual_baseline" in aggregate.migration_notes
    assert "legacy_blocking_downgraded_to_advisory" in aggregate.migration_notes

    plan = plan_guideline_import(
        envelope,
        target_owner_id="actor-target",
        target_board_id="target-board",
    )
    assert plan.entries[0].aggregate.identity.owner_id == "actor-target"
    assert plan.entries[0].aggregate.identity.board_id == "target-board"
    assert plan.entries[0].binding_disposition is (
        GuidelineImportBindingDisposition.NO_BINDINGS
    )


@pytest.mark.parametrize(
    "invalid_rules",
    ("not-a-list", [1], [{"enforcement": "sometimes"}]),
)
def test_legacy_rule_hints_are_validated_not_executed(
    invalid_rules: object,
) -> None:
    with pytest.raises(GuidelineImportExportError):
        parse_guideline_export(
            {
                "schema_version": "1",
                "kind": "guidelines",
                "items": [
                    {
                        "title": "Legacy",
                        "content": "Prose",
                        "scope": "global",
                        "board_id": None,
                        "rules": invalid_rules,
                    }
                ],
            }
        )


def test_fresh_binding_import_is_inert_and_pending_native_adoption() -> None:
    aggregate = _aggregate()
    envelope = _envelope(aggregate)
    plan = plan_guideline_import(
        envelope,
        target_owner_id="actor-target",
        target_board_id="board-target",
    )
    entry = plan.entries[0]

    assert plan.can_apply is True
    assert plan.live_binding_writes == ()
    assert entry.live_binding_writes == ()
    assert entry.binding_disposition is (
        GuidelineImportBindingDisposition.PENDING_ADOPTION
    )
    assert entry.binding_candidates[0].live_write_forbidden is True
    assert entry.binding_candidates[0].source_board_id == "board-1"
    assert entry.binding_candidates[0].target_board_id == "board-target"
    assert entry.aggregate.bindings[0].board_id == "board-target"
    assert entry.aggregate.bindings[0].materialization is (
        GuidelineBindingMaterialization.CANDIDATE
    )
    assert "source_active_binding_pending_explicit_preview_and_adoption" in (
        entry.diagnostics
    )

    repeated = plan_guideline_import(
        envelope,
        target_owner_id="actor-target",
        target_board_id="board-target",
        existing_aggregates=(entry.aggregate,),
    )
    assert repeated.error_code is None
    assert repeated.entries[0].binding_disposition is (
        GuidelineImportBindingDisposition.SKIP_IDENTICAL_HISTORY
    )


def test_retired_or_unlinked_binding_history_is_stored_inert() -> None:
    revision = _revision()
    unlinked = _exported_binding(
        revision,
        state=GuidelineBindingState.UNLINKED,
    )
    for aggregate in (
        _aggregate(bindings=(unlinked,)),
        _aggregate(bindings=(_exported_binding(revision),), retired=True),
    ):
        plan = plan_guideline_import(
            _envelope(aggregate),
            target_owner_id="actor-1",
        )
        assert plan.entries[0].binding_disposition is (
            GuidelineImportBindingDisposition.STORE_INERT_HISTORY
        )
        assert plan.live_binding_writes == ()


def test_exact_same_environment_round_trip_skips_everything() -> None:
    aggregate = _aggregate()
    plan = plan_guideline_import(
        _envelope(aggregate),
        target_owner_id="actor-1",
        existing_aggregates=(aggregate,),
    )
    entry = plan.entries[0]

    assert entry.is_identical is True
    assert entry.binding_disposition is (
        GuidelineImportBindingDisposition.SKIP_IDENTICAL_HISTORY
    )
    assert entry.revision_actions[0].disposition is (
        GuidelineImportRevisionDisposition.SKIP_IDENTICAL
    )
    assert plan.entries_to_apply == ()
    assert plan.skip_identical_count == 1
    assert plan.overwritten_row_count == 0


def test_import_plan_rejects_entries_from_another_envelope() -> None:
    plan_a = plan_guideline_import(
        _envelope(_aggregate(guideline_id="guideline-a", bindings=())),
        target_owner_id="actor-target",
    )
    plan_b = plan_guideline_import(
        _envelope(_aggregate(guideline_id="guideline-b", bindings=())),
        target_owner_id="actor-target",
    )

    with pytest.raises(
        GuidelineImportExportError,
        match="guideline_import_plan_entries_mismatch",
    ):
        replace(plan_a, entries=plan_b.entries)

    source_b = _aggregate(
        guideline_id="guideline-a",
        revisions=(
            _revision(
                guideline_id="guideline-a",
                content="Different source content.",
            ),
        ),
        bindings=(),
    )
    same_identity_plan_b = plan_guideline_import(
        _envelope(source_b),
        target_owner_id="actor-target",
    )
    with pytest.raises(
        GuidelineImportExportError,
        match="guideline_import_plan_aggregate_source_mismatch",
    ):
        replace(plan_a, entries=same_identity_plan_b.entries)


@pytest.mark.parametrize(
    ("identity_field", "invalid_value"),
    (
        ("owner_id", "wrong-owner"),
        ("board_id", "wrong-board"),
    ),
)
def test_import_plan_rejects_non_target_identity_remap(
    identity_field: str,
    invalid_value: str,
) -> None:
    source = _aggregate(
        guideline_id="inline-guideline",
        board_id="source-board",
        bindings=(),
    )
    plan = plan_guideline_import(
        _envelope(source, source_board_id="source-board"),
        target_owner_id="target-owner",
        target_board_id="target-board",
    )
    entry = plan.entries[0]
    invalid_identity = replace(
        entry.aggregate.identity,
        **{identity_field: invalid_value},
    )
    invalid_entry = replace(
        entry,
        aggregate=replace(entry.aggregate, identity=invalid_identity),
    )

    with pytest.raises(
        GuidelineImportExportError,
        match="guideline_import_plan_aggregate_source_mismatch",
    ):
        replace(plan, entries=(invalid_entry,))


def test_same_identity_semver_and_digest_aliases_revision_id() -> None:
    source = _aggregate(bindings=())
    source_revision = source.revisions[0].revision
    existing = ExistingGuidelineRevision(
        guideline_id=source.guideline_id,
        revision_id="existing-revision-id",
        semantic_version=source_revision.semantic_version,
        revision_digest=source_revision.content_digest,
    )
    plan = plan_guideline_import(
        _envelope(source),
        target_owner_id="actor-1",
        existing_revisions=(existing,),
    )
    action = plan.entries[0].revision_actions[0]

    assert action.disposition is GuidelineImportRevisionDisposition.SKIP_IDENTICAL
    assert action.revision_id == source_revision.revision_id
    assert action.resolved_revision_id == "existing-revision-id"
    assert plan.entries[0].resolved_head_revision_id == "existing-revision-id"
    assert plan.error_code is None


def test_digest_conflict_rolls_back_complete_plan_with_zero_overwrites() -> None:
    aggregate = _aggregate(bindings=())
    revision = aggregate.revisions[0].revision
    plan = plan_guideline_import(
        _envelope(aggregate),
        target_owner_id="actor-1",
        existing_revisions=(
            ExistingGuidelineRevision(
                guideline_id=aggregate.guideline_id,
                revision_id=revision.revision_id,
                semantic_version=revision.semantic_version,
                revision_digest="f" * 64,
            ),
        ),
    )

    assert plan.error_code == "conflict"
    assert plan.transaction_status is (GuidelineImportTransactionStatus.ROLLED_BACK)
    assert plan.conflict_count == 1
    assert plan.overwritten_row_count == 0
    assert plan.can_apply is False
    assert plan.entries_to_apply == ()


def test_foreign_owner_scope_and_retirement_collisions_fail_closed() -> None:
    source = _aggregate(bindings=())
    foreign_owner = _aggregate(owner_id="foreign", bindings=())
    owner_plan = plan_guideline_import(
        _envelope(source),
        target_owner_id="actor-1",
        existing_aggregates=(foreign_owner,),
    )
    assert owner_plan.error_code == "conflict"
    assert "identity_owner_conflict" in (owner_plan.entries[0].identity_conflicts)

    existing_retired = _aggregate(bindings=(), retired=True)
    retirement_plan = plan_guideline_import(
        _envelope(source),
        target_owner_id="actor-1",
        existing_aggregates=(existing_retired,),
    )
    assert retirement_plan.error_code == "conflict"
    assert "identity_retirement_state_conflict" in (
        retirement_plan.entries[0].identity_conflicts
    )

    inline_source = _aggregate(
        guideline_id="inline-1",
        board_id="source-board",
        bindings=(),
    )
    inline_existing = _aggregate(
        guideline_id="inline-1",
        board_id="other-board",
        bindings=(),
    )
    board_plan = plan_guideline_import(
        _envelope(inline_source),
        target_owner_id="actor-1",
        existing_aggregates=(inline_existing,),
    )
    assert "identity_board_conflict" in (board_plan.entries[0].identity_conflicts)


def test_head_ahead_skips_old_source_but_source_ahead_appends() -> None:
    revision_1 = _revision()
    revision_2 = _revision(
        revision_id="revision-2",
        revision_number=2,
        semantic_version="1.1.0",
        parent_revision_id=revision_1.revision_id,
    )
    local_ahead = _aggregate(
        revisions=(revision_1, revision_2),
        bindings=(),
    )
    source_old = _aggregate(revisions=(revision_1,), bindings=())
    old_plan = plan_guideline_import(
        _envelope(source_old),
        target_owner_id="actor-1",
        existing_aggregates=(local_ahead,),
    )
    assert old_plan.error_code is None
    assert "local_head_ahead_source_skipped" in (old_plan.entries[0].diagnostics)
    assert old_plan.entries_to_apply == ()

    append_plan = plan_guideline_import(
        _envelope(local_ahead),
        target_owner_id="actor-1",
        existing_aggregates=(source_old,),
    )
    assert [
        action.disposition for action in append_plan.entries[0].revision_actions
    ] == [
        GuidelineImportRevisionDisposition.SKIP_IDENTICAL,
        GuidelineImportRevisionDisposition.CREATE,
    ]
    assert append_plan.entries_to_apply


def test_binding_candidate_digest_conflict_rolls_back() -> None:
    revision = _revision()
    source = _aggregate(
        bindings=(_exported_binding(revision, evidence_suffix="a"),),
    )
    existing = _aggregate(
        bindings=(_exported_binding(revision, evidence_suffix="b"),),
    )
    plan = plan_guideline_import(
        _envelope(source),
        target_owner_id="actor-1",
        existing_aggregates=(existing,),
    )

    assert plan.error_code == "conflict"
    assert plan.entries[0].binding_conflicts
    assert plan.overwritten_row_count == 0


def test_dry_run_and_real_plan_share_intention_digest_and_never_report_writes() -> None:
    envelope = _envelope(_aggregate(bindings=()))
    dry = plan_guideline_import(
        envelope,
        target_owner_id="actor-1",
        dry_run=True,
    )
    real = plan_guideline_import(
        envelope,
        target_owner_id="actor-1",
        dry_run=False,
    )

    assert dry.import_digest == real.import_digest
    assert dry.transaction_status is GuidelineImportTransactionStatus.DRY_RUN
    assert dry.can_apply is False
    assert GuidelineImportResult.from_plan(dry).created_count == 0
    committed = GuidelineImportResult.from_plan(real, committed=True)
    assert committed.transaction_status is (GuidelineImportTransactionStatus.COMMITTED)
    assert committed.created_count == 1
    assert committed.overwritten_row_count == 0


def test_payload_is_plain_json_and_preserves_unicode_as_utf8() -> None:
    revision = _revision(content="Política íntegra — revisão.")
    payload = guideline_export_payload(
        _envelope(_aggregate(revisions=(revision,), bindings=()))
    )
    encoded = canonical_guideline_json_bytes(payload)

    assert "Política íntegra".encode() in encoded
    assert json.loads(encoded)["content_digest"] == payload["content_digest"]
