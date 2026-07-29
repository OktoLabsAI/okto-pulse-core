"""MCPContextProjectionService (spec R2, cards R2.1 + R2.2).

Post-assembly, post-authorization projection of the high-frequency context
responses (``get_task_context`` / ``get_spec_context``) through ONE shared
projection path (tr_5b60166b). It is PURE response shaping — it never touches DB
queries, permissions, or the ``include_*`` flags; it only reshapes an
already-assembled, already-authorized context dict.

Profiles (reuses the R5.1 envelope contract):
- ``summary`` (DEFAULT) — compact exploration projection with a deterministic
  32 KiB response budget. It keeps the card/spec identity and workflow state,
  linked scenarios, validations and gate summaries, while removing duplicated
  artifact bodies and bounding unusually large collections/strings.
    * R2.1 FR-3 — ``resolved_references`` re-stating spec facts as full bodies
      (compacted to id/ref/links).
    * R2.1 FR-4 — semantically-empty null/default fields.
    * R2.2 FR-7 — ``decisions_markdown`` (the redundant third rendering of the
      decisions, already present as ``decisions[]`` + resolved refs); replaced by
      a compact ``render_decisions_markdown`` follow_up. ``decisions[]`` and
      ``decisions_stats`` are KEPT.
    * R2.2 FR-8 — Architecture Design bodies (``global_description``/``entities``/
      ``interfaces``/``diagrams``) repeated across the card, spec, top-level and
      ``resolved_references`` sections; summarized once to identifying fields +
      ``counts`` drilldown, with a ``read_full_architecture`` follow_up.
- ``detail`` — ``summary`` plus prose ``description``/scope fields, bounded
  2 KiB primary-artifact previews, and a larger deterministic 64 KiB response
  budget (full artifact bodies still live behind ``profile=full``).
- ``full`` / ``legacy`` with the default task ``context_scope=all`` — the
  assembled payload UNCHANGED (FR-2/FR-9 back-compat).
- task ``full`` + ``context_scope=gate`` — bounded 32 KiB pre-mutation slice:
  current gate/readiness metadata plus a SHA-256/count manifest for omitted
  bodies and explicit drilldowns.
- unsupported profile → structured ``unsupported_projection`` error.

The live byte budget is currently applied to ``get_task_context`` only, after
authorization and full context assembly. When that selected projection exceeds
its budget, deterministic collection/string limits are applied and
``projection.truncated`` becomes true with a ``read_full_context`` follow-up.
``full/all`` and ``legacy`` are never budget-truncated. The additive
``full/gate`` task scope is the mandatory in-band transition/evaluation read;
mutations still resolve and fingerprint full context server-side. Other context
families continue to share the dedup/envelope path without inheriting a
task-shaped budget.

``include_architecture=false`` is honored upstream (the sections are already
empty/absent); this layer only ever SUMMARIZES sections that are present — it
never synthesizes architecture refs (ac_d5f7d04a).

Observability (fr_610801cc): every projected response emits the SAFE
``mcp_context_projection_usage_total`` + ``mcp_context_projection_payload_bytes``
diagnostics (counts + identifiers only, never a body).

``copy_architecture_to_card`` compaction is R2.3 (FR-9).
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Mapping
from typing import Any

from okto_pulse.core.mcp.payload_budget import (
    METRIC_CONTEXT_PROJECTION_BYTES,
    METRIC_CONTEXT_PROJECTION_USAGE,
)
from okto_pulse.core.mcp.payload_compaction import compact_payload
from okto_pulse.core.mcp.projection_envelope import (
    OUTCOME_OK,
    _stable_payload_bytes,
    resolve_profile,
    unsupported_projection_error,
)

_LOG = logging.getLogger("okto_pulse.mcp.context_projection")

# Profiles that return the assembled payload unchanged (back-compat).
_PASSTHROUGH_PROFILES = frozenset({"full", "legacy"})

# ``get_task_context`` keeps its historical full/all response byte-compatible,
# but status-changing callers can request the complete *gate* slice in-band.
# The omitted content is represented by a deterministic manifest and remains
# available through the historical full/all response and dedicated drilldowns.
TASK_CONTEXT_SCOPES: tuple[str, ...] = ("all", "gate")
CONTEXT_GATE_BUDGET_BYTES = 32 * 1024

# A compacted ``resolved_references`` entry DROPS only the body/content fields
# that already live in ``spec``/``card`` — and KEEPS every identifying / linkage
# field (id, source refs, parent, type, status, index, …). This is a DENYLIST,
# not a whitelist: we never risk dropping a semantic identifying field we did not
# anticipate (FR-3 + owner "don't dilute" decision).
_REF_BODY_KEYS_DROP = frozenset({"content", "text"})
# Larger prose bodies dropped in ``summary`` but kept in ``detail`` drilldown.
_REF_SUMMARY_BODY_DROP = frozenset(
    {"description", "problem_statement", "proposed_approach", "scope_assessment"}
)

# R2.2 FR-8: the heavy architecture body fields deduplicated in summary/detail.
# DENYLIST — identifying/linkage fields (id, title, parent_*, version, source_*)
# are KEPT. List bodies become ``counts``; prose becomes a ``has_*`` flag. The full
# bodies remain reachable behind ``profile=full`` (the ``read_full_architecture``
# follow_up), mirroring how ``content``/``text`` are always dropped from refs.
_ARCH_LIST_BODY_KEYS = ("entities", "interfaces", "diagrams")
_ARCH_PROSE_BODY_KEYS = ("global_description",)
_ARCH_BODY_KEYS = frozenset(_ARCH_LIST_BODY_KEYS + _ARCH_PROSE_BODY_KEYS)

# Live response budgets. They include the projection metadata itself; a small
# reserve is used while shaping the body so the final stable JSON remains below
# the advertised limit.
CONTEXT_SUMMARY_BUDGET_BYTES = 32 * 1024
CONTEXT_DETAIL_BUDGET_BYTES = 64 * 1024
_PROJECTION_METADATA_RESERVE_BYTES = 2048
_PROFILE_BUDGET_BYTES = {
    "summary": CONTEXT_SUMMARY_BUDGET_BYTES,
    "detail": CONTEXT_DETAIL_BUDGET_BYTES,
}

# Bodies repeated in primary artifact collections. ``resolved_references`` has
# its own compactor; without this second pass, spec/card KB content and mockup
# markup remain in the primary sections and dominate the response.
_PRIMARY_ARTIFACT_BODY_KEYS = frozenset(
    {
        "content",
        "html",
        "html_content",
        "css",
        "css_content",
        "svg",
        "markup",
        "rendered_html",
        "image_data",
        "data_url",
    }
)
_DETAIL_ARTIFACT_PREVIEW_KEYS = (
    "content",
    "html",
    "html_content",
    "css",
    "css_content",
    "svg",
    "markup",
    "rendered_html",
)
_DETAIL_ARTIFACT_PREVIEW_BYTES = 2048

# Optional drill-down paths removed, largest first, only if deterministic
# string/list bounding is still insufficient. Gate/readiness/validation fields
# are deliberately absent from this list.
_OPTIONAL_BUDGET_PATHS: tuple[tuple[str, ...], ...] = (
    ("resolved_references",),
    ("card_knowledge_bases",),
    ("knowledge_bases",),
    ("screen_mockups",),
    ("architecture_designs",),
    ("qa_items",),
    ("card", "comments"),
    ("card", "qa_items"),
    ("card", "screen_mockups"),
    ("card", "architecture_designs"),
    ("spec", "knowledge_bases"),
    ("spec", "qa_items"),
    ("spec", "screen_mockups"),
    ("spec", "architecture_designs"),
)

# Structured families are useful in exploration, so they are removed only as a
# last resort after artifact/reference drill-downs have been exhausted.
_STRUCTURED_BUDGET_PATHS: tuple[tuple[str, ...], ...] = (
    ("spec", "business_rules"),
    ("spec", "api_contracts"),
    ("spec", "integration_requirements"),
    ("spec", "observability_requirements"),
    ("spec", "decisions"),
    ("spec", "acceptance_criteria"),
    ("spec", "technical_requirements"),
    ("spec", "functional_requirements"),
    ("spec", "test_scenarios"),
)

_RECENT_FIRST_COLLECTION_KEYS = frozenset({"comments", "validations", "evaluations"})
_TRUNCATION_SUFFIX = "…[truncated]"
_POST_ASSEMBLY_SEMANTIC_BLOCKS = (
    "test_card_operational_flow",
    "gate_readiness",
)

_GATE_CARD_KEYS = (
    "id",
    "title",
    "status",
    "archived",
    "pre_archive_status",
    "cancellation_reason",
    "cancelled_at",
    "cancelled_by",
    "priority",
    "assignee_id",
    "labels",
    "card_type",
    "spec_id",
    "sprint_id",
    "position",
    "test_scenario_ids",
    "linked_test_task_ids",
    "due_date",
    "created_by",
    "created_at",
    "origin_task_id",
    "severity",
    "depends_on",
)
_GATE_SPEC_KEYS = (
    "id",
    "title",
    "status",
    "edition",
    "version",
    "archived",
    "pre_archive_status",
)
_GATE_VALIDATION_KEYS = (
    "id",
    "validation_id",
    "outcome",
    "verdict",
    "recommendation",
    "reviewer_id",
    "reviewer_name",
    "confidence",
    "estimated_confidence",
    "completeness",
    "estimated_completeness",
    "drift",
    "estimated_drift",
    "threshold_violations",
    "reviewer_separation",
    "created_at",
)
_GATE_RESOURCE_STATE_KEYS = (
    "resource_type",
    "state",
    "direct_count",
    "inherited_count",
    "total_count",
    "blocking",
    "authority",
    "required",
)
_GATE_NA_MARK_KEYS = (
    "id",
    "active",
    "effective",
    "inherited",
    "resource_type",
    "source_entity_type",
    "source_entity_id",
)
_GATE_CONTENT_PATHS: tuple[tuple[str, ...], ...] = (
    ("card", "description"),
    ("card", "details"),
    ("card", "screen_mockups"),
    ("card", "architecture_designs"),
    ("card", "qa_items"),
    ("card", "comments"),
    ("card_knowledge_bases",),
    ("spec", "description"),
    ("spec", "context"),
    ("spec", "functional_requirements"),
    ("spec", "technical_requirements"),
    ("spec", "acceptance_criteria"),
    ("spec", "test_scenarios"),
    ("spec", "business_rules"),
    ("spec", "api_contracts"),
    ("spec", "integration_requirements"),
    ("spec", "observability_requirements"),
    ("spec", "decisions"),
    ("spec", "decisions_markdown"),
    ("spec", "screen_mockups"),
    ("spec", "architecture_designs"),
    ("spec", "qa_items"),
    ("spec", "knowledge_bases"),
    ("my_test_scenarios",),
    ("resolved_references",),
    ("validations",),
)


def _compact_ref(entry: Any, *, detail: bool) -> Any:
    if not isinstance(entry, Mapping):
        return entry
    drop = set(_REF_BODY_KEYS_DROP)
    if not detail:
        drop |= _REF_SUMMARY_BODY_DROP
    return {k: v for k, v in entry.items() if k not in drop}


def _compact_resolved_references(
    resolved: Any, *, detail: bool
) -> tuple[Any, int]:
    """Compact every resolved-reference section to id/ref/link fields. Returns
    (compacted, deduped_count) where deduped_count counts the body-carrying refs
    that were slimmed.

    NOTE: architecture refs are left to ``_dedup_architecture`` — their bodies are
    ``global_description``/``entities``/… not ``content``/``text``, so this denylist
    leaves them untouched here and they are summarized (and counted) there."""
    if not isinstance(resolved, Mapping):
        return resolved, 0
    deduped = 0
    out: dict[str, Any] = {}
    for section, items in resolved.items():
        if isinstance(items, list):
            new_items = []
            for item in items:
                slim = _compact_ref(item, detail=detail)
                if isinstance(item, Mapping) and slim != dict(item):
                    deduped += 1
                new_items.append(slim)
            out[section] = new_items
        else:
            out[section] = items
    return out, deduped


def _compact_primary_artifact(entry: Any, *, detail: bool) -> tuple[Any, bool]:
    """Compact a primary KB/mockup record without mutating the source.

    Artifact bodies are always available through ``profile=full``. Summary also
    drops descriptive prose; detail retains it plus bounded ``*_preview`` fields,
    which keeps the exploratory profiles observably distinct without repeating
    full content.
    """

    if not isinstance(entry, Mapping):
        return entry, False
    drop = set(_PRIMARY_ARTIFACT_BODY_KEYS)
    if not detail:
        drop |= _REF_SUMMARY_BODY_DROP
    compacted = {key: value for key, value in entry.items() if key not in drop}
    if detail:
        for key in _DETAIL_ARTIFACT_PREVIEW_KEYS:
            body = entry.get(key)
            if not isinstance(body, str) or not body:
                continue
            preview, truncated = _truncate_utf8(
                body,
                _DETAIL_ARTIFACT_PREVIEW_BYTES,
            )
            compacted[f"{key}_preview"] = preview
            compacted[f"{key}_preview_truncated"] = truncated
    return compacted, compacted != dict(entry)


def _compact_primary_artifact_list(
    items: Any, *, detail: bool
) -> tuple[Any, int]:
    if not isinstance(items, list):
        return items, 0
    compacted: list[Any] = []
    deduped = 0
    for item in items:
        projected, changed = _compact_primary_artifact(item, detail=detail)
        compacted.append(projected)
        deduped += int(changed)
    return compacted, deduped


def _omit_primary_artifact_bodies(
    projected: dict[str, Any], *, detail: bool
) -> int:
    """Remove full artifact bodies from their primary context sections."""

    omitted = 0

    def _compact_nested(container_key: str, collection_key: str) -> None:
        nonlocal omitted
        container = projected.get(container_key)
        if not isinstance(container, Mapping):
            return
        items = container.get(collection_key)
        if not isinstance(items, list):
            return
        compacted, changed = _compact_primary_artifact_list(items, detail=detail)
        projected[container_key] = {**container, collection_key: compacted}
        omitted += changed

    for collection_key in ("knowledge_bases", "screen_mockups"):
        _compact_nested("card", collection_key)
        _compact_nested("spec", collection_key)
        if isinstance(projected.get(collection_key), list):
            compacted, changed = _compact_primary_artifact_list(
                projected[collection_key], detail=detail
            )
            projected[collection_key] = compacted
            omitted += changed

    if isinstance(projected.get("card_knowledge_bases"), list):
        compacted, changed = _compact_primary_artifact_list(
            projected["card_knowledge_bases"], detail=detail
        )
        projected["card_knowledge_bases"] = compacted
        omitted += changed

    return omitted


def _truncate_utf8(value: str, max_bytes: int) -> tuple[str, bool]:
    encoded = value.encode("utf-8")
    if len(encoded) <= max_bytes:
        return value, False
    suffix = _TRUNCATION_SUFFIX.encode("utf-8")
    available = max(0, max_bytes - len(suffix))
    prefix = encoded[:available].decode("utf-8", errors="ignore")
    return prefix + _TRUNCATION_SUFFIX, True


def _bounded_clone(
    value: Any,
    *,
    string_limit: int,
    list_limit: int,
    mapping_limit: int,
    path: tuple[str, ...] = (),
) -> tuple[Any, int, bool]:
    """Return a deterministic bounded clone and omission count.

    This is invoked only after the complete exploratory projection exceeds its
    byte budget. Mapping insertion order is stable for assembled MCP payloads.
    Recent-first collections keep their tail; every other list keeps its head.
    """

    if isinstance(value, Mapping):
        items = list(value.items())
        omitted = 0
        truncated = False
        if len(items) > mapping_limit:
            omitted += len(items) - mapping_limit
            items = items[:mapping_limit]
            truncated = True
        out: dict[Any, Any] = {}
        for key, child in items:
            bounded, child_omitted, child_truncated = _bounded_clone(
                child,
                string_limit=string_limit,
                list_limit=list_limit,
                mapping_limit=mapping_limit,
                path=(*path, str(key)),
            )
            out[key] = bounded
            omitted += child_omitted
            truncated = truncated or child_truncated
        return out, omitted, truncated

    if isinstance(value, list):
        items = value
        omitted = 0
        truncated = False
        if len(items) > list_limit:
            omitted += len(items) - list_limit
            items = (
                items[-list_limit:]
                if path and path[-1] in _RECENT_FIRST_COLLECTION_KEYS
                else items[:list_limit]
            )
            truncated = True
        out_list: list[Any] = []
        for child in items:
            bounded, child_omitted, child_truncated = _bounded_clone(
                child,
                string_limit=string_limit,
                list_limit=list_limit,
                mapping_limit=mapping_limit,
                path=path,
            )
            out_list.append(bounded)
            omitted += child_omitted
            truncated = truncated or child_truncated
        return out_list, omitted, truncated

    if isinstance(value, str):
        bounded, truncated = _truncate_utf8(value, string_limit)
        return bounded, int(truncated), truncated

    return value, 0, False


def _path_value(root: Mapping[str, Any], path: tuple[str, ...]) -> Any:
    current: Any = root
    for component in path:
        if not isinstance(current, Mapping) or component not in current:
            return None
        current = current[component]
    return current


def _drop_path(root: dict[str, Any], path: tuple[str, ...]) -> int:
    current: Any = root
    for component in path[:-1]:
        if not isinstance(current, dict):
            return 0
        current = current.get(component)
    if not isinstance(current, dict) or path[-1] not in current:
        return 0
    removed = current.pop(path[-1])
    return max(1, _count_fields(removed))


def _drop_largest_optional_path(
    projected: dict[str, Any], paths: tuple[tuple[str, ...], ...]
) -> int:
    candidates: list[tuple[int, tuple[str, ...]]] = []
    for path in paths:
        value = _path_value(projected, path)
        if value is not None:
            candidates.append((_stable_payload_bytes(value), path))
    if not candidates:
        return 0
    _, largest = max(candidates, key=lambda item: (item[0], item[1]))
    return _drop_path(projected, largest)


def resolve_task_context_scope(scope: str | None) -> str | None:
    """Normalize the additive task-context scope without changing old callers."""

    normalized = str(scope or "all").strip().lower()
    return normalized if normalized in TASK_CONTEXT_SCOPES else None


def unsupported_task_context_scope_error(
    scope: str | None,
    *,
    profile: str | None = None,
) -> dict[str, Any]:
    received = str(scope or "")
    return {
        "error": "unsupported_context_scope",
        "error_code": "unsupported_context_scope",
        "message": (
            f"Unsupported task context scope '{received}'. "
            "Use context_scope='all', or profile='full' with "
            "context_scope='gate' for the bounded pre-mutation view."
        ),
        "received_scope": received,
        "received_profile": str(profile or ""),
        "supported_context_scopes": list(TASK_CONTEXT_SCOPES),
        "gate_scope_profile": "full",
    }


def _canonical_json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
            ensure_ascii=False,
        ).encode("utf-8")
    except (TypeError, ValueError):
        return json.dumps(str(value), ensure_ascii=False).encode("utf-8")


def _content_manifest(source: Mapping[str, Any]) -> dict[str, Any]:
    """Describe omitted bodies without repeating them in the gate response."""

    source_bytes = _canonical_json_bytes(source)
    sections: list[dict[str, Any]] = []
    for path in _GATE_CONTENT_PATHS:
        value = _path_value(source, path)
        if value in (None, "", [], {}):
            continue
        encoded = _canonical_json_bytes(value)
        item: dict[str, Any] = {
            "path": ".".join(path),
            "payload_bytes": len(encoded),
            "sha256": hashlib.sha256(encoded).hexdigest(),
        }
        if isinstance(value, list):
            item["item_count"] = len(value)
        elif isinstance(value, Mapping):
            item["field_count"] = len(value)
        sections.append(item)
    return {
        "canonicalization": "json-sort-keys-compact-utf8-v1",
        "source_payload_bytes": len(source_bytes),
        "source_payload_sha256": hashlib.sha256(source_bytes).hexdigest(),
        "section_count": len(sections),
        "sections": sections,
    }


def _slim_gate_resource_state(value: Any) -> Any:
    if not isinstance(value, Mapping):
        return value
    out = {key: value[key] for key in _GATE_RESOURCE_STATE_KEYS if key in value}
    na_mark = value.get("na_mark")
    if isinstance(na_mark, Mapping):
        out["na_mark"] = {
            key: na_mark[key] for key in _GATE_NA_MARK_KEYS if key in na_mark
        }
    return out


def _gate_count_projection(value: Any) -> Any:
    """Keep scalar counts; replace high-cardinality count detail with totals."""

    if isinstance(value, Mapping):
        return {str(key): _gate_count_projection(child) for key, child in value.items()}
    if isinstance(value, list):
        return {"item_count": len(value)}
    return value


def _slim_resource_gate_summary(value: Any) -> Any:
    if not isinstance(value, Mapping):
        return value
    out: dict[str, Any] = {
        key: value[key]
        for key in (
            "board_id",
            "entity_type",
            "entity_id",
            "blocking",
            "architecture_findings_blocking",
            "architecture_propagation_blocking",
        )
        if key in value
    }
    for key in (
        "resources",
        "missing_resources",
        "advisory_resources",
        "advisory_missing_resources",
    ):
        entries = value.get(key)
        if isinstance(entries, list):
            out[key] = [_slim_gate_resource_state(item) for item in entries]

    authority_policy = value.get("authority_policy")
    if isinstance(authority_policy, Mapping):
        out["authority_policy"] = dict(authority_policy)

    warnings = value.get("warnings")
    if isinstance(warnings, list):
        out["warnings"] = [
            {
                key: item[key]
                for key in (
                    "code",
                    "message",
                    "active_count",
                    "ineligible_source_count",
                    "top_remediation",
                )
                if isinstance(item, Mapping) and key in item
            }
            for item in warnings
        ]

    findings = value.get("architecture_findings")
    if isinstance(findings, Mapping):
        out["architecture_findings"] = {
            key: child
            for key, child in findings.items()
            if not isinstance(child, (list, tuple, set, Mapping))
        }
        if "top_remediation" in findings:
            out["architecture_findings"]["top_remediation"] = findings[
                "top_remediation"
            ]

    propagation = value.get("architecture_propagation")
    if isinstance(propagation, Mapping):
        out["architecture_propagation"] = {
            key: propagation[key]
            for key in ("blocking", "remediation")
            if key in propagation
        }
        sources = propagation.get("ineligible_sources")
        if isinstance(sources, list):
            out["architecture_propagation"]["ineligible_source_count"] = len(sources)

    lineage_counts = value.get("lineage_counts")
    if isinstance(lineage_counts, Mapping):
        out["lineage_counts"] = _gate_count_projection(lineage_counts)
    return out


def _slim_validation_history(value: Any) -> tuple[list[Any], int]:
    if not isinstance(value, list):
        return [], 0
    # Validation history is append-only. The current gate decision depends on
    # the newest records, while the complete history is content-addressed in the
    # manifest and available from okto_pulse_list_task_validations.
    recent = value[-5:]
    out: list[Any] = []
    for item in recent:
        if not isinstance(item, Mapping):
            out.append(item)
            continue
        out.append({key: item[key] for key in _GATE_VALIDATION_KEYS if key in item})
    return out, len(value)


def _slim_scenario_state(value: Any) -> Any:
    if not isinstance(value, Mapping):
        return value
    return {
        key: value[key]
        for key in ("id", "title", "status", "evidence_present")
        if key in value
    }


def _project_task_gate_context(
    source: Mapping[str, Any],
    *,
    card_id: str,
    tool_name: str,
) -> dict[str, Any]:
    """Bounded full gate/readiness slice for the mandatory mutation pre-flight.

    ``profile=full, context_scope=all`` remains untouched for compatibility.
    This additive scope carries every current gate decision input exposed by the
    context tool, plus a content-addressed manifest of the bodies intentionally
    left to drill-down reads.
    """

    projected: dict[str, Any] = {"context_scope": "gate"}
    card = source.get("card")
    if isinstance(card, Mapping):
        projected["card"] = {
            key: card[key] for key in _GATE_CARD_KEYS if key in card
        }

    spec = source.get("spec")
    if isinstance(spec, Mapping):
        spec_projection = {
            key: spec[key] for key in _GATE_SPEC_KEYS if key in spec
        }
        if "resource_gate_summary" in spec:
            spec_projection["resource_gate_summary"] = _slim_resource_gate_summary(
                spec["resource_gate_summary"]
            )
        projected["spec"] = spec_projection

    if "resource_gate_summary" in source:
        projected["resource_gate_summary"] = _slim_resource_gate_summary(
            source["resource_gate_summary"]
        )

    validations, validation_count = _slim_validation_history(
        source.get("validations")
    )
    projected["validations"] = validations
    projected["validation_history"] = {
        "total_count": validation_count,
        "returned_count": len(validations),
        "has_more": validation_count > len(validations),
        "order": "oldest_to_newest_within_latest_window",
    }

    for key in (
        "validation_config",
        "reviewer_separation",
        "test_card_operational_flow",
        "gate_readiness",
    ):
        if key in source:
            projected[key] = source[key]

    scenarios = source.get("my_test_scenarios")
    if isinstance(scenarios, list):
        projected["my_test_scenarios"] = [
            _slim_scenario_state(item) for item in scenarios
        ]

    projected["content_manifest"] = _content_manifest(source)
    follow_up = [
        {
            "rel": "read_full_context",
            "target_ref": (
                f"{tool_name}:card={card_id}:profile=full:context_scope=all"
            ),
        },
        {
            "rel": "read_bounded_content",
            "target_ref": f"{tool_name}:card={card_id}:profile=detail",
        },
        {
            "rel": "list_task_validations",
            "target_ref": f"okto_pulse_list_task_validations:card={card_id}",
        },
    ]

    omitted = max(0, _count_fields(source) - _count_fields(projected))
    truncated = False
    body_budget = CONTEXT_GATE_BUDGET_BYTES - _PROJECTION_METADATA_RESERVE_BYTES
    if _stable_payload_bytes(projected) > body_budget:
        projected, bounded_omitted, truncated = _bounded_clone(
            projected,
            string_limit=512,
            list_limit=32,
            mapping_limit=80,
        )
        omitted += bounded_omitted
    if _stable_payload_bytes(projected) > body_budget:
        # This branch is adversarial protection, not the normal shape. Preserve
        # the actual gate booleans/required tools and the aggregate source digest.
        manifest = projected.get("content_manifest")
        projected = {
            key: projected[key]
            for key in (
                "context_scope",
                "card",
                "spec",
                "resource_gate_summary",
                "validation_config",
                "reviewer_separation",
                "test_card_operational_flow",
                "gate_readiness",
                "validation_history",
            )
            if key in projected
        }
        if isinstance(manifest, Mapping):
            projected["content_manifest"] = {
                key: manifest[key]
                for key in (
                    "canonicalization",
                    "source_payload_bytes",
                    "source_payload_sha256",
                    "section_count",
                )
                if key in manifest
            }
        projected, strict_omitted, _ = _bounded_clone(
            projected,
            string_limit=256,
            list_limit=12,
            mapping_limit=48,
        )
        omitted += strict_omitted
        truncated = True

    projection: dict[str, Any] = {
        "profile": "full",
        "context_scope": "gate",
        "outcome": OUTCOME_OK,
        "payload_bytes": 0,
        "budget_bytes": CONTEXT_GATE_BUDGET_BYTES,
        "truncated": truncated,
        "content_omitted": True,
        "omitted_count": omitted,
        "deduped_count": 0,
        "follow_up": follow_up,
    }
    if truncated:
        projection["truncation_reason"] = "gate_scope_payload_budget"
    projected["projection"] = projection
    for _ in range(8):
        payload_bytes = _stable_payload_bytes(projected)
        if projection["payload_bytes"] == payload_bytes:
            break
        projection["payload_bytes"] = payload_bytes
    _emit_context_projection_metric(projection, tool_name=tool_name)
    return projected


def _essential_context_projection(
    source: Mapping[str, Any], *, profile: str
) -> tuple[dict[str, Any], int]:
    """Last-resort, gate-aware skeleton for an adversarially large payload."""

    card_keys = (
        "id",
        "title",
        "description",
        "details",
        "status",
        "priority",
        "assignee_id",
        "labels",
        "card_type",
        "test_scenario_ids",
        "due_date",
        "created_by",
        "created_at",
        "depends_on",
    )
    spec_keys = (
        "id",
        "title",
        "description",
        "context",
        "status",
        "edition",
        "version",
        "functional_requirements",
        "technical_requirements",
        "acceptance_criteria",
        "test_scenarios",
        "business_rules",
        "api_contracts",
        "integration_requirements",
        "observability_requirements",
        "decisions",
        "resource_gate_summary",
    )
    top_keys = (
        "my_test_scenarios",
        "validations",
        "resource_gate_summary",
        "validation_config",
        "reviewer_separation",
        "test_card_operational_flow",
        "gate_readiness",
    )

    skeleton: dict[str, Any] = {}
    card = source.get("card")
    if isinstance(card, Mapping):
        skeleton["card"] = {key: card[key] for key in card_keys if key in card}
    spec = source.get("spec")
    if isinstance(spec, Mapping):
        compact_spec = {key: spec[key] for key in spec_keys if key in spec}
        counts = {
            key: len(spec[key])
            for key in (
                "functional_requirements",
                "technical_requirements",
                "acceptance_criteria",
                "test_scenarios",
                "business_rules",
                "api_contracts",
                "integration_requirements",
                "observability_requirements",
                "decisions",
            )
            if isinstance(spec.get(key), list)
        }
        if counts:
            compact_spec["structured_counts"] = counts
        skeleton["spec"] = compact_spec
    for key in top_keys:
        if key in source:
            skeleton[key] = source[key]

    bounded, bounded_omitted, _ = _bounded_clone(
        skeleton,
        string_limit=256 if profile == "summary" else 768,
        list_limit=3 if profile == "summary" else 8,
        mapping_limit=48 if profile == "summary" else 80,
    )
    omitted = max(0, _count_fields(source) - _count_fields(bounded)) + bounded_omitted
    return bounded, omitted


def _apply_profile_budget(
    projected: dict[str, Any], *, profile: str
) -> tuple[dict[str, Any], int, bool]:
    """Fit summary/detail under their advertised response budget."""

    budget = _PROFILE_BUDGET_BYTES[profile]
    body_budget = budget - _PROJECTION_METADATA_RESERVE_BYTES
    if _stable_payload_bytes(projected) <= body_budget:
        return projected, 0, False

    stages = (
        ((2048, 40, 100), (768, 20, 72), (256, 8, 48))
        if profile == "summary"
        else (
            (8192, 100, 160),
            (3072, 50, 120),
            (1024, 20, 80),
            (384, 8, 56),
        )
    )
    current: dict[str, Any] = projected
    omitted = 0
    truncated = False
    for string_limit, list_limit, mapping_limit in stages:
        bounded, stage_omitted, stage_truncated = _bounded_clone(
            current,
            string_limit=string_limit,
            list_limit=list_limit,
            mapping_limit=mapping_limit,
        )
        current = bounded
        omitted += stage_omitted
        truncated = truncated or stage_truncated
        if _stable_payload_bytes(current) <= body_budget:
            return current, omitted, True

    while _stable_payload_bytes(current) > body_budget:
        removed = _drop_largest_optional_path(current, _OPTIONAL_BUDGET_PATHS)
        if not removed:
            break
        omitted += removed
        truncated = True

    while _stable_payload_bytes(current) > body_budget:
        removed = _drop_largest_optional_path(current, _STRUCTURED_BUDGET_PATHS)
        if not removed:
            break
        omitted += removed
        truncated = True

    if _stable_payload_bytes(current) > body_budget:
        current, skeleton_omitted = _essential_context_projection(
            current, profile=profile
        )
        omitted += skeleton_omitted
        truncated = True

    if _stable_payload_bytes(current) > body_budget:
        # Adversarial deeply nested gate/evidence dictionaries can still be
        # larger than the normal skeleton. Preserve their leading typed shape
        # under a final strict bound instead of violating the advertised budget.
        current, strict_omitted, _ = _bounded_clone(
            current,
            string_limit=128,
            list_limit=2,
            mapping_limit=24,
        )
        omitted += strict_omitted
        truncated = True

    return current, omitted, truncated


def _summarize_architecture_design(design: Any) -> tuple[Any, bool]:
    """Drop the heavy architecture bodies, keep every identifying field, and add a
    ``counts`` drilldown hint. Returns ``(summary, had_body)`` where ``had_body`` is
    True only when a NON-empty body was removed. Non-mutating."""
    if not isinstance(design, Mapping):
        return design, False
    out: dict[str, Any] = {}
    counts: dict[str, Any] = {}
    had_body = False
    for key, value in design.items():
        if key in _ARCH_BODY_KEYS:
            if isinstance(value, list):
                if value:
                    had_body = True
                    counts[key] = len(value)
            elif value:  # non-empty global_description prose
                had_body = True
                counts[f"has_{key}"] = True
            # empty/None body: dropped silently (no drilldown, no dedup credit)
            continue
        out[key] = value
    if counts:
        out["counts"] = counts
    return out, had_body


def _summarize_architecture_list(designs: Any) -> tuple[Any, int]:
    """Summarize every architecture design in a list. Returns ``(list, deduped)``
    where ``deduped`` counts the designs whose full body was dropped."""
    if not isinstance(designs, list):
        return designs, 0
    out: list[Any] = []
    deduped = 0
    for design in designs:
        slim, had_body = _summarize_architecture_design(design)
        out.append(slim)
        if had_body:
            deduped += 1
    return out, deduped


def _decisions_markdown_follow_up(spec_id: str) -> dict[str, str]:
    target = f"spec:{spec_id}:decisions_markdown" if spec_id else "decisions_markdown"
    return {"rel": "render_decisions_markdown", "target_ref": target}


def _gate_decisions_markdown(
    projected: dict[str, Any], follow_up: list[dict[str, str]]
) -> int:
    """R2.2 FR-7: drop the redundant ``decisions_markdown`` third rendering from the
    summary/detail payload (task context carries it under ``spec``; spec context at
    the top level) and add a compact ``render_decisions_markdown`` follow_up.
    ``decisions[]`` and ``decisions_stats`` are KEPT. Returns the number of markdown
    blocks gated. Non-mutating w.r.t. the original input — touched sections are
    replaced with fresh dicts (never popped in place on shared references)."""
    gated = 0
    # task context: result["spec"]["decisions_markdown"]
    spec = projected.get("spec")
    if isinstance(spec, Mapping) and spec.get("decisions_markdown"):
        spec_id = str(spec.get("id") or "")
        projected["spec"] = {
            k: v for k, v in spec.items() if k != "decisions_markdown"
        }
        follow_up.append(_decisions_markdown_follow_up(spec_id))
        gated += 1
    # spec context: result["decisions_markdown"] at the top level
    if projected.get("decisions_markdown"):
        spec_id = str(projected.get("id") or "")
        del projected["decisions_markdown"]  # del from the shallow copy, not input
        follow_up.append(_decisions_markdown_follow_up(spec_id))
        gated += 1
    return gated


def _dedup_architecture(
    projected: dict[str, Any],
    follow_up: list[dict[str, str]],
    *,
    tool_name: str,
) -> int:
    """R2.2 FR-8: summarize architecture designs wherever they appear (``card`` /
    ``spec`` / top-level / ``resolved_references``) so the full bodies are not
    repeated across sections. Only sections ALREADY PRESENT and non-empty are
    touched — never synthesized — so ``include_architecture=false`` (empty/absent
    everywhere) stays empty (ac_d5f7d04a). Returns the deduped body count (one per
    physical body-carrying occurrence slimmed) and appends a single
    ``read_full_architecture`` drilldown follow_up when anything was slimmed.
    Non-mutating w.r.t. the original input."""
    deduped = 0

    def _slim_nested(container_key: str) -> None:
        nonlocal deduped
        container = projected.get(container_key)
        if not isinstance(container, Mapping):
            return
        designs = container.get("architecture_designs")
        if isinstance(designs, list) and designs:
            slim, dropped = _summarize_architecture_list(designs)
            projected[container_key] = {**container, "architecture_designs": slim}
            deduped += dropped

    # card.architecture_designs + spec.architecture_designs (task context)
    _slim_nested("card")
    _slim_nested("spec")

    # top-level architecture_designs (spec context)
    top = projected.get("architecture_designs")
    if isinstance(top, list) and top:
        slim, dropped = _summarize_architecture_list(top)
        projected["architecture_designs"] = slim
        deduped += dropped

    # resolved_references.architecture_designs (both shapes)
    rr = projected.get("resolved_references")
    if isinstance(rr, Mapping):
        designs = rr.get("architecture_designs")
        if isinstance(designs, list) and designs:
            slim, dropped = _summarize_architecture_list(designs)
            projected["resolved_references"] = {**rr, "architecture_designs": slim}
            deduped += dropped

    if deduped:
        follow_up.append(
            {"rel": "read_full_architecture", "target_ref": tool_name}
        )
    return deduped


def _emit_context_projection_metric(
    envelope: Mapping[str, Any], *, tool_name: str
) -> dict[str, Any]:
    """Emit the SAFE context-projection diagnostics (R2.2 observability) under
    ``mcp_context_projection_usage_total`` + ``mcp_context_projection_payload_bytes``.
    Counts + identifiers only — never a body (mirrors ``emit_compaction_metric``)."""
    labels = {
        "tool_name": tool_name,
        "profile": str(envelope.get("profile", "")),
        "outcome": str(envelope.get("outcome", "")),
        "payload_bytes": int(envelope.get("payload_bytes", 0) or 0),
        "omitted_count": int(envelope.get("omitted_count", 0) or 0),
        "deduped_count": int(envelope.get("deduped_count", 0) or 0),
        "truncated": bool(envelope.get("truncated", False)),
    }
    _LOG.info(METRIC_CONTEXT_PROJECTION_USAGE, extra={"context_projection": labels})
    _LOG.info(METRIC_CONTEXT_PROJECTION_BYTES, extra={"context_projection": labels})
    return labels


def _follow_up(tool_name: str) -> list[dict[str, str]]:
    return [{"rel": "read_full_context", "target_ref": tool_name}]


class MCPContextProjectionService:
    """Apply a projection profile to an assembled context response (R2.1 + R2.2)."""

    def _project(
        self, result: Mapping[str, Any], *, profile: str | None, tool_name: str
    ) -> dict[str, Any]:
        """The single shared projection path for both context tools (tr_5b60166b)."""
        resolved_profile = resolve_profile(profile)
        if resolved_profile is None:
            return unsupported_projection_error(profile)

        if resolved_profile in _PASSTHROUGH_PROFILES:
            # FR-2/FR-9: full/legacy preserve the assembled payload exactly. We still
            # record usage telemetry (no envelope is injected into the payload).
            out = dict(result)
            _emit_context_projection_metric(
                {
                    "profile": resolved_profile,
                    "outcome": OUTCOME_OK,
                    "payload_bytes": _stable_payload_bytes(out),
                    "truncated": False,
                    "omitted_count": 0,
                    "deduped_count": 0,
                },
                tool_name=tool_name,
            )
            return out

        detail = resolved_profile == "detail"
        projected: dict[str, Any] = dict(result)
        follow_up = _follow_up(tool_name)

        # R2.1 FR-3: dedup resolved_references — keep ids/refs/links, drop the bodies
        # already represented in ``spec``/``card``.
        deduped = 0
        if "resolved_references" in projected:
            compacted, deduped = _compact_resolved_references(
                projected["resolved_references"], detail=detail
            )
            projected["resolved_references"] = compacted

        # R2.2 FR-7: gate decisions_markdown (drop the redundant third rendering).
        deduped += _gate_decisions_markdown(projected, follow_up)

        # R2.2 FR-8: dedup architecture bodies across card/spec/top/resolved refs.
        deduped += _dedup_architecture(projected, follow_up, tool_name=tool_name)

        # Primary KB/mockup collections previously retained the same full body
        # already removed from resolved_references. Keep metadata/provenance in
        # summary/detail and leave the complete artifact behind profile=full.
        profile_omitted = _omit_primary_artifact_bodies(
            projected,
            detail=detail,
        )

        # R2.1 FR-4: omit semantically-empty null/default fields. Gate/operational
        # blocks are assembled late and their empty collections are meaningful
        # (for example pending_scenarios=[] means ready), so preserve them exactly.
        semantic_blocks = {
            key: projected.pop(key)
            for key in _POST_ASSEMBLY_SEMANTIC_BLOCKS
            if key in projected
        }
        before = _count_fields(projected)
        projected = compact_payload(projected)
        omitted = profile_omitted + max(0, before - _count_fields(projected))
        projected.update(semantic_blocks)

        # Apply the deterministic live-response budget to the high-volume task
        # context surface after all semantic compaction. Other context families
        # share this projector for dedup/envelope behavior but have different
        # shapes and therefore require their own explicitly reviewed budgets.
        task_context_budgeted = tool_name == "okto_pulse_get_task_context"
        truncated = False
        if task_context_budgeted:
            projected, budget_omitted, truncated = _apply_profile_budget(
                projected,
                profile=resolved_profile,
            )
            omitted += budget_omitted

        # FR-10 / ac_622687f9: canonical R5 projection metadata. The byte counter
        # is finalized over the complete response (including this metadata), so it
        # is directly comparable to what the MCP client receives.
        projection: dict[str, Any] = {
            "profile": resolved_profile,
            "outcome": OUTCOME_OK,
            "payload_bytes": 0,
            "truncated": truncated,
            "omitted_count": omitted,
            "deduped_count": deduped,
            "follow_up": follow_up,
        }
        if task_context_budgeted:
            projection["budget_bytes"] = _PROFILE_BUDGET_BYTES[resolved_profile]
        if truncated:
            projection["truncation_reason"] = "profile_payload_budget"
        projected["projection"] = projection
        # The payload_bytes value affects its own digit count. Iteration converges
        # once that digit count is stable (normally two passes).
        for _ in range(8):
            payload_bytes = _stable_payload_bytes(projected)
            if projection["payload_bytes"] == payload_bytes:
                break
            projection["payload_bytes"] = payload_bytes

        _emit_context_projection_metric(projection, tool_name=tool_name)
        return projected

    def project_task_context(
        self,
        result: Mapping[str, Any],
        *,
        card_id: str,
        profile: str | None,
        context_scope: str | None = "all",
    ) -> dict[str, Any]:
        resolved_scope = resolve_task_context_scope(context_scope)
        resolved_profile = resolve_profile(profile)
        if resolved_scope is None or (
            resolved_scope == "gate" and resolved_profile != "full"
        ):
            return unsupported_task_context_scope_error(
                context_scope,
                profile=profile,
            )
        if resolved_scope == "gate":
            return _project_task_gate_context(
                result,
                card_id=card_id,
                tool_name="okto_pulse_get_task_context",
            )
        return self._project(
            result, profile=profile, tool_name="okto_pulse_get_task_context"
        )

    def project_spec_context(
        self, result: Mapping[str, Any], *, profile: str | None
    ) -> dict[str, Any]:
        return self._project(
            result, profile=profile, tool_name="okto_pulse_get_spec_context"
        )

    def project_entity_context(
        self,
        result: Mapping[str, Any],
        *,
        profile: str | None,
        tool_name: str,
    ) -> dict[str, Any]:
        """Apply the same profile/error/metadata contract to another context tool."""

        return self._project(result, profile=profile, tool_name=tool_name)


def _count_fields(obj: Any) -> int:
    """Recursively count dict keys + list items, to measure null-omission impact."""
    if isinstance(obj, Mapping):
        return len(obj) + sum(_count_fields(v) for v in obj.values())
    if isinstance(obj, list):
        return sum(_count_fields(v) for v in obj)
    return 0


_SERVICE = MCPContextProjectionService()


def project_task_context(
    result: Mapping[str, Any],
    *,
    card_id: str,
    profile: str | None,
    context_scope: str | None = "all",
) -> dict[str, Any]:
    return _SERVICE.project_task_context(
        result,
        card_id=card_id,
        profile=profile,
        context_scope=context_scope,
    )


def project_spec_context(
    result: Mapping[str, Any], *, profile: str | None
) -> dict[str, Any]:
    return _SERVICE.project_spec_context(result, profile=profile)


def project_entity_context(
    result: Mapping[str, Any], *, profile: str | None, tool_name: str
) -> dict[str, Any]:
    return _SERVICE.project_entity_context(
        result,
        profile=profile,
        tool_name=tool_name,
    )
