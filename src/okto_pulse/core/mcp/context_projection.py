"""MCPContextProjectionService (spec R2, cards R2.1 + R2.2).

Post-assembly, post-authorization projection of the high-frequency context
responses (``get_task_context`` / ``get_spec_context``) through ONE shared
projection path (tr_5b60166b). It is PURE response shaping — it never touches DB
queries, permissions, or the ``include_*`` flags; it only reshapes an
already-assembled, already-authorized context dict.

Profiles (reuses the R5.1 envelope contract):
- ``summary`` (DEFAULT) — CONSERVATIVE dedup (owner decision, R2.1 card comment):
  keeps the UNIQUE content an agent needs (the card body, the spec's structured
  requirement texts, the card's own ``my_test_scenarios``, validations) and
  removes only the DUPLICATION:
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
- ``detail`` — ``summary`` plus the prose ``description``/scope fields on refs
  (still NOT the full ``content``/``text``/architecture bodies that live in
  ``spec`` / behind ``profile=full``).
- ``full`` / ``legacy`` — the assembled payload UNCHANGED (FR-2/FR-9 back-compat).
- unsupported profile → structured ``unsupported_projection`` error.

``include_architecture=false`` is honored upstream (the sections are already
empty/absent); this layer only ever SUMMARIZES sections that are present — it
never synthesizes architecture refs (ac_d5f7d04a).

Observability (fr_610801cc): every projected response emits the SAFE
``mcp_context_projection_usage_total`` + ``mcp_context_projection_payload_bytes``
diagnostics (counts + identifiers only, never a body).

``copy_architecture_to_card`` compaction is R2.3 (FR-9).
"""

from __future__ import annotations

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

        # R2.1 FR-4: omit semantically-empty null/default fields (read-projection).
        before = _count_fields(projected)
        projected = compact_payload(projected)
        omitted = max(0, before - _count_fields(projected))

        # FR-10 / ac_622687f9: canonical R5 projection metadata (shared payload_bytes
        # counter). ``deduped_count`` totals every duplicated body removed (resolved
        # refs + decisions_markdown + repeated architecture bodies).
        projected["projection"] = {
            "profile": resolved_profile,
            "outcome": OUTCOME_OK,
            "payload_bytes": _stable_payload_bytes(projected),
            "truncated": False,
            "omitted_count": omitted,
            "deduped_count": deduped,
            "follow_up": follow_up,
        }
        _emit_context_projection_metric(projected["projection"], tool_name=tool_name)
        return projected

    def project_task_context(
        self, result: Mapping[str, Any], *, card_id: str, profile: str | None
    ) -> dict[str, Any]:
        return self._project(
            result, profile=profile, tool_name="okto_pulse_get_task_context"
        )

    def project_spec_context(
        self, result: Mapping[str, Any], *, profile: str | None
    ) -> dict[str, Any]:
        return self._project(
            result, profile=profile, tool_name="okto_pulse_get_spec_context"
        )


def _count_fields(obj: Any) -> int:
    """Recursively count dict keys + list items, to measure null-omission impact."""
    if isinstance(obj, Mapping):
        return len(obj) + sum(_count_fields(v) for v in obj.values())
    if isinstance(obj, list):
        return sum(_count_fields(v) for v in obj)
    return 0


_SERVICE = MCPContextProjectionService()


def project_task_context(
    result: Mapping[str, Any], *, card_id: str, profile: str | None
) -> dict[str, Any]:
    return _SERVICE.project_task_context(result, card_id=card_id, profile=profile)


def project_spec_context(
    result: Mapping[str, Any], *, profile: str | None
) -> dict[str, Any]:
    return _SERVICE.project_spec_context(result, profile=profile)
