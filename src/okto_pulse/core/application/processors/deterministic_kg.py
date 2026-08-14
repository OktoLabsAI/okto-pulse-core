"""Layer 1 Deterministic Worker — KG Pipeline v2 (spec c48a5c33).

Reads structured fields from the pulse.db artifact
(Story/Ideation/Refinement/Spec/Sprint/Card) and
emits node + edge candidates with provenance metadata `{layer, rule_id,
confidence, created_by}`. NO LLM calls. Any relationship that would require
semantic judgement is emitted as a `missing_link_candidate` for the
cognitive agent to resolve later (fallback policy, BR `Cognitive Fallback
Confidence Cap`).

Contract (FR1 of spec c48a5c33):
    worker = DeterministicWorker()
    result = worker.process_spec(spec_dict)
    # result.nodes: list[EmittedNode]
    # result.edges: list[EmittedEdge]
    # result.missing_link_candidates: list[MissingLinkCandidate]
    # result.content_hash: str  (SHA256 used by idempotent commit BR)

The worker is pure — it never touches graph backend directly. The caller hands the
output to the transaction orchestrator / primitives for actual persistence.
Making the worker pure is what lets the CLI `--dry-run` mode work without
partial writes (BR `CLI dry-run reporta diff sem escrever`).
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

from okto_pulse.core.domain.code_traceability_kg import (
    CODE_INVESTIGATION_RECEIPT_KG_STATUSES,
    CodeInvestigationReceiptKGStatus,
)
from okto_pulse.core.kg.interfaces.graph_transaction import (
    SpecLineageParentIntent,
)
from okto_pulse.core.kg.source_maturity import (
    GRAPH_LAYER_CANONICAL,
    GRAPH_LAYER_NONE,
    GRAPH_LAYER_WORKING,
    MATURITY_CANONICAL_ELIGIBLE,
    classify_source_for_kg,
)

logger = logging.getLogger("okto_pulse.kg.deterministic_worker")

# Package version exposed in edge.rule_id so consumers can audit which worker
# produced which edges. Bump when extraction semantics change in ways that
# callers should know about.
WORKER_VERSION = "v2.0"
WORKER_ID = "worker_layer1"
LAYER = "deterministic"


# =====================================================================
# Output DTOs
# =====================================================================


@dataclass
class EmittedNode:
    """One node scheduled for graph backend insertion by the Layer 1 worker."""

    candidate_id: str
    node_type: str  # matches schema.NODE_TYPES
    title: str
    content: str
    source_artifact_ref: str
    source_confidence: float = 1.0
    context: str = ""
    graph_layer: str = GRAPH_LAYER_CANONICAL
    maturity_status: str = MATURITY_CANONICAL_ELIGIBLE
    # v0.3.1: additive score boost resolved from the source card's priority.
    # Non-zero only on the root node emitted from a Card — belongs_to child
    # nodes (FR/TR/AC per Spec) stay at 0.0. Cap +0.2 (CRITICAL).
    priority_boost: float = 0.0
    kind_of: str | None = None
    investigation_receipt_id: str | None = None
    source_ref: str | None = None
    attestor_actor_id: str | None = None
    declared_revision: str | None = None
    workspace_state_id: str | None = None
    code_path: str | None = None
    symbol_qualified_name: str | None = None
    symbol_kind: str | None = None
    selector_kind: str | None = None
    selector_fingerprint: str | None = None
    resolution_state: str | None = None
    source_span_start: int | None = None
    source_span_end: int | None = None
    source_content_hash: str | None = None


@dataclass
class EmittedEdge:
    """One edge scheduled for graph backend insertion, carrying full v0.2.0 metadata."""

    candidate_id: str
    edge_type: str  # matches schema.REL_TYPES
    from_candidate_id: str
    to_candidate_id: str
    confidence: float
    # v0.2.0 provenance. layer is always "deterministic" in this worker;
    # rule_id identifies which extraction rule fired (useful for debugging
    # miscategorisations and for the /metrics endpoint's rule histogram).
    rule_id: str
    layer: str = LAYER
    created_by: str = WORKER_ID
    fallback_reason: str = ""


@dataclass
class MissingLinkCandidate:
    """An edge the worker REFUSED to emit because deterministic data is partial.

    The cognitive agent consumes these to propose a fallback edge with
    capped confidence (BR `Cognitive Fallback Confidence Cap`, ≤0.85).
    """

    edge_type: str
    from_candidate_id: str
    from_candidate_title: str  # denormalised for LLM prompt efficiency
    reason: str  # machine-readable, matches BR wording
    suggested_candidates: list[str] = field(default_factory=list)
    artifact_ref: str = ""


@dataclass(frozen=True, slots=True)
class RelationalProjectionActiveRef:
    """Exact relationally-owned node identity retained by active-set cleanup."""

    node_type: str
    candidate_id: str
    source_artifact_ref: str


@dataclass(frozen=True, slots=True)
class RelationalProjectionActiveEdgeRef:
    """Exact deterministic edge retained by relational active-set cleanup."""

    candidate_id: str
    edge_type: str
    from_candidate_id: str
    to_candidate_id: str
    rule_id: str


@dataclass(frozen=True, slots=True)
class RelationalProjectionActiveSetIntent:
    """Exact desired set for one relational projection namespace."""

    owner_type: str
    owner_id: str
    namespace: str
    active_refs: tuple[RelationalProjectionActiveRef, ...]
    active_edges: tuple[RelationalProjectionActiveEdgeRef, ...] = ()


@dataclass
class WorkerResult:
    nodes: list[EmittedNode] = field(default_factory=list)
    edges: list[EmittedEdge] = field(default_factory=list)
    missing_link_candidates: list[MissingLinkCandidate] = field(default_factory=list)
    # Explicit, server-internal intent. Absence of an edge is not interpreted
    # as deletion because cognitive/partial sessions may emit incomplete sets.
    spec_lineage_parent_intent: SpecLineageParentIntent = (
        SpecLineageParentIntent.PRESERVE
    )
    # Relational projections are tracked explicitly so graph cleanup never
    # guesses ownership from candidate-id or source-ref prefixes.
    relational_projection_candidate_ids: set[str] = field(default_factory=set)
    relational_projection_active_set_intent: (
        RelationalProjectionActiveSetIntent | None
    ) = None
    content_hash: str = ""
    raw_content: str = ""

    def deterministic_edge_ratio(self) -> float:
        """Share of emitted edges tagged layer=deterministic (sanity check)."""
        if not self.edges:
            return 0.0
        det = sum(1 for e in self.edges if e.layer == LAYER)
        return det / len(self.edges)


# =====================================================================
# Tech entities whitelist (NER)
# =====================================================================


_TECH_WHITELIST_PATH = Path(__file__).parent / "tech_entities.yml"


@dataclass
class TechEntity:
    canonical: str
    aliases: tuple[str, ...]
    stem: bool


@lru_cache(maxsize=1)
def _load_tech_whitelist() -> tuple[list[TechEntity], int]:
    """Parse tech_entities.yml into TechEntity dataclasses.

    Uses PyYAML when available; falls back to a hand-rolled parser covering
    the narrow shape we emit. Cached for the life of the process — call
    reset_tech_whitelist_cache() in tests to force reload.
    """
    text = _TECH_WHITELIST_PATH.read_text(encoding="utf-8")
    try:
        import yaml  # type: ignore

        data = yaml.safe_load(text)
    except ImportError:
        data = _parse_whitelist_minimal(text)

    version = int(data.get("version", 1))
    entities = []
    for raw in data.get("entities", []) or []:
        canonical = raw["canonical"]
        aliases = tuple(
            str(a).lower() for a in (raw.get("aliases") or []) + [canonical]
        )
        entities.append(TechEntity(
            canonical=canonical,
            aliases=tuple(sorted(set(aliases), key=len, reverse=True)),
            stem=bool(raw.get("stem", False)),
        ))
    return entities, version


def _parse_whitelist_minimal(text: str) -> dict:
    """Hand-rolled YAML parser for tech_entities.yml — zero deps.

    Handles only the shape we control: top-level `version` + `entities` list
    of dicts with `canonical`, `aliases` (list), and `stem` (bool). Anything
    richer requires PyYAML.
    """
    out: dict = {"version": 1, "entities": []}
    current: dict | None = None
    in_aliases = False
    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        if not line or line.lstrip().startswith("#"):
            continue
        if line.startswith("version:"):
            out["version"] = int(line.split(":", 1)[1].strip())
            continue
        if line.startswith("entities:"):
            continue
        if line.startswith("  - canonical:"):
            if current:
                out["entities"].append(current)
            current = {"canonical": line.split(":", 1)[1].strip(),
                       "aliases": [], "stem": False}
            in_aliases = False
            continue
        if current is None:
            continue
        if line.startswith("    aliases:"):
            rest = line.split(":", 1)[1].strip()
            if rest.startswith("[") and rest.endswith("]"):
                raw_items = rest[1:-1].split(",")
                current["aliases"] = [
                    s.strip().strip('"').strip("'") for s in raw_items if s.strip()
                ]
                in_aliases = False
            else:
                in_aliases = True
            continue
        if line.startswith("    stem:"):
            val = line.split(":", 1)[1].strip().lower()
            current["stem"] = val in ("true", "yes", "1")
            in_aliases = False
            continue
        if in_aliases and line.startswith("      -"):
            current["aliases"].append(
                line.split("-", 1)[1].strip().strip('"').strip("'")
            )
    if current:
        out["entities"].append(current)
    return out


def reset_tech_whitelist_cache() -> None:
    _load_tech_whitelist.cache_clear()


def _board_root_candidate_id(board_id: str) -> str:
    return f"board_{board_id[:8]}_entity"


def _has_node(result: WorkerResult, candidate_id: str) -> bool:
    return any(node.candidate_id == candidate_id for node in result.nodes)


def _has_edge(result: WorkerResult, candidate_id: str) -> bool:
    return any(edge.candidate_id == candidate_id for edge in result.edges)


def _append_board_root(result: WorkerResult, board_id: str | None) -> str | None:
    if not board_id:
        return None
    board_id = str(board_id)
    candidate_id = _board_root_candidate_id(board_id)
    if not _has_node(result, candidate_id):
        result.nodes.append(EmittedNode(
            candidate_id=candidate_id,
            node_type="Entity",
            title=f"Board {board_id}",
            content="Deterministic KG board root.",
            source_artifact_ref=f"board:{board_id}",
            source_confidence=1.0,
        ))
    return candidate_id


def _layer_attrs_for_artifact(
    artifact_type: str,
    status: Any,
    *,
    has_minimal_evidence: bool = True,
    lineage_complete: bool = True,
) -> tuple[str, str]:
    classification = classify_source_for_kg(
        artifact_type=artifact_type,
        artifact_status=status,
        content_hash="deterministic-worker",
        has_minimal_evidence=has_minimal_evidence,
        lineage_complete=lineage_complete,
    )
    graph_layer = classification.graph_layer
    if graph_layer == GRAPH_LAYER_NONE:
        graph_layer = GRAPH_LAYER_WORKING
    return graph_layer, classification.maturity_status


def _card_source_artifact_type(card_type: Any) -> str:
    normalized = str(card_type or "normal").lower()
    if normalized == "test":
        return "test"
    if normalized == "bug":
        return "bug"
    return "task"


def _apply_layer_to_result(
    result: WorkerResult,
    *,
    graph_layer: str,
    maturity_status: str,
) -> None:
    for node in result.nodes:
        if node.source_artifact_ref == "tech_entities.yml":
            continue
        if node.source_artifact_ref.startswith("board:"):
            continue
        node.graph_layer = graph_layer
        node.maturity_status = maturity_status


def _attach_to_board_root(
    result: WorkerResult,
    *,
    board_id: str | None,
    child_candidate_id: str,
    rule_slot: str,
) -> None:
    board_root_id = _append_board_root(result, board_id)
    if not board_root_id:
        return
    edge_id = f"{child_candidate_id}_belongs_to_board"
    if _has_edge(result, edge_id):
        return
    result.edges.append(EmittedEdge(
        candidate_id=edge_id,
        edge_type="belongs_to",
        from_candidate_id=child_candidate_id,
        to_candidate_id=board_root_id,
        confidence=1.0,
        rule_id=f"belongs_to/{rule_slot}_to_board@{WORKER_VERSION}",
    ))


def _extract_tech_mentions(text: str) -> list[str]:
    """Return canonical names of whitelisted techs mentioned in `text`.

    Case-insensitive, word-boundary match on the canonical name and every
    alias. Order preserved (first mention wins) and duplicates removed.
    """
    if not text:
        return []
    entities, _ = _load_tech_whitelist()
    matches: list[str] = []
    lower_text = text.lower()
    for ent in entities:
        for alias in ent.aliases:
            # Escape regex-special chars in alias, word-boundary anchors.
            pattern = r"\b" + re.escape(alias.lower()) + r"\b"
            if re.search(pattern, lower_text):
                if ent.canonical not in matches:
                    matches.append(ent.canonical)
                break
    return matches


# =====================================================================
# Markdown parsing — spec.context "## Decisions" section
# =====================================================================


_DECISIONS_HEADER = re.compile(r"^\s*##\s*decisions\s*$", re.IGNORECASE | re.MULTILINE)
_NEXT_HEADER = re.compile(r"^\s*##\s+", re.MULTILINE)
_BULLET_LINE = re.compile(r"^\s*[-*]\s+(.+?)\s*$", re.MULTILINE)


def _extract_decisions_from_context(context: str) -> list[str]:
    """Pull the bulleted items under a `## Decisions` header of spec.context.

    Tolerates markdown irregularities per TR `tr_1b5646c0`:
    - Header case-insensitive ("Decisions", "decisions", "DECISIONS")
    - Bullet character may be `-` or `*`
    - Leading/trailing whitespace ignored
    - Wrapped lines NOT joined (keeps each bullet atomic)
    """
    if not context:
        return []
    m = _DECISIONS_HEADER.search(context)
    if not m:
        return []
    start = m.end()
    after = context[start:]
    next_m = _NEXT_HEADER.search(after)
    section = after[: next_m.start()] if next_m else after
    return [b.group(1).strip() for b in _BULLET_LINE.finditer(section)]


def _inherits_refinement_decision_context(spec: dict[str, Any]) -> bool:
    """Return whether ``context`` contains refinement-owned decisions.

    ``RefinementService.derive_spec`` deliberately copies the refinement's
    ``decisions`` into the child context under a ``## Decisions`` heading.  The
    refinement is independently materialized in the KG, so parsing that heading
    as legacy *spec* decisions duplicates ownership and can create ungrounded
    Decision nodes before (or after) the spec is populated.

    A spec derived directly from an ideation does not receive such a heading;
    direct/legacy and ideation-derived specs therefore retain the compatibility
    parser.  A refinement-derived spec expresses its own decisions through the
    structured ``spec.decisions`` collection, which is handled separately.
    """

    return bool(spec.get("refinement_id"))


# =====================================================================
# Core extractor
# =====================================================================


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _projection_records(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list | tuple):
        raise ValueError("relational_projection_records_invalid")
    if any(not isinstance(item, dict) for item in value):
        raise ValueError("relational_projection_record_invalid")
    return [dict(item) for item in value]


def _projection_raw(label: str, records: list[dict[str, Any]]) -> str:
    normalized_records: list[dict[str, Any]] = []
    for item in records:
        normalized = dict(item)
        if label == "research_decisions":
            alternatives = normalized.get("alternatives")
            if isinstance(alternatives, list | tuple):
                normalized["alternatives"] = sorted(
                    (_one_line(value) for value in alternatives),
                    key=lambda value: (value.casefold(), value),
                )
            evidence_refs = normalized.get("evidence_refs")
            if isinstance(evidence_refs, list | tuple):
                normalized["evidence_refs"] = sorted(
                    str(value) for value in evidence_refs
                )
        normalized_records.append(normalized)
    if label == "quality_assessments":
        normalized_records.sort(
            key=lambda row: (
                str(row.get("assessment_kind") or ""),
                str(row.get("receipt_id") or ""),
            )
        )
    elif label == "research_decisions":
        normalized_records.sort(
            key=lambda row: (
                str(row.get("ledger_id") or ""),
                str(row.get("entry_id") or ""),
            )
        )
    elif label == "spec_dependencies":
        normalized_records.sort(
            key=lambda row: (
                str(row.get("prerequisite_spec_id") or ""),
                str(row.get("dependency_id") or ""),
            )
        )
    return json.dumps(
        {label: normalized_records},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        default=str,
    )


def _one_line(value: Any) -> str:
    rendered = "" if value is None else str(value)
    return re.sub(r"\s+", " ", rendered).strip()


def _quality_context_block(records: list[dict[str, Any]]) -> str:
    if not records:
        return ""
    lines = ["Quality assessments (current heads):"]
    for item in sorted(
        records,
        key=lambda row: (
            str(row.get("assessment_kind") or ""),
            str(row.get("receipt_id") or ""),
        ),
    ):
        kind = _one_line(item.get("assessment_kind")) or "unknown"
        score = _one_line(item.get("score"))
        scale = _one_line(item.get("scale_kind"))
        direction = _one_line(item.get("scale_direction"))
        outcome = _one_line(item.get("outcome"))
        justification = _one_line(item.get("justification"))
        version = _one_line(item.get("subject_version"))
        revision = _one_line(item.get("head_revision"))
        lines.append(
            f"- {kind}: score={score} scale={scale} direction={direction} "
            f"outcome={outcome} subject_version={version} "
            f"head_revision={revision}; {justification}"
        )
    return "\n".join(lines)


def _research_decision_context_block(records: list[dict[str, Any]]) -> str:
    if not records:
        return ""
    lines = ["Research decisions (current heads):"]
    for item in sorted(
        records,
        key=lambda row: (
            str(row.get("ledger_id") or ""),
            str(row.get("entry_id") or ""),
        ),
    ):
        ledger_id = _one_line(item.get("ledger_id"))
        status = _one_line(item.get("status"))
        unknown = _one_line(item.get("unknown"))
        decision = _one_line(item.get("decision"))
        summary = f"unknown={unknown}"
        if decision:
            summary += f"; decision={decision}"
        lines.append(f"- ledger={ledger_id} status={status}; {summary}")
    return "\n".join(lines)


def _join_context(*parts: str) -> str:
    return "\n\n".join(part.strip() for part in parts if part and part.strip())


def _relational_projection_candidate_id(source_artifact_ref: str) -> str:
    return f"relproj_{_sha256(source_artifact_ref)[:32]}"


def _relational_projection_edge_id(
    edge_type: str,
    from_candidate_id: str,
    to_candidate_id: str,
) -> str:
    identity = f"{edge_type}:{from_candidate_id}:{to_candidate_id}"
    return f"relproj_edge_{_sha256(identity)[:32]}"


def _normalized_alternative(value: Any) -> tuple[str, str]:
    rendered = _one_line(value)
    if not rendered:
        raise ValueError("research_decision_projection_alternative_invalid")
    identity = rendered.casefold()
    return rendered, _sha256(identity)


def _project_research_decisions(
    *,
    refinement_id: str,
    refinement_candidate_id: str,
    records: list[dict[str, Any]],
    result: WorkerResult,
) -> None:
    active_refs: list[RelationalProjectionActiveRef] = []
    for item in sorted(
        records,
        key=lambda row: (
            str(row.get("ledger_id") or ""),
            str(row.get("entry_id") or ""),
        ),
    ):
        ledger_id = _one_line(item.get("ledger_id"))
        entry_id = _one_line(item.get("entry_id"))
        status = _one_line(item.get("status"))
        if not ledger_id or not entry_id or not status:
            raise ValueError("research_decision_projection_identity_invalid")
        if status != "resolved":
            continue

        decision = _one_line(item.get("decision"))
        rationale = _one_line(item.get("rationale"))
        if not decision or not rationale:
            raise ValueError("research_decision_projection_resolved_content_invalid")
        alternatives = item.get("alternatives") or []
        if not isinstance(alternatives, list | tuple):
            raise ValueError("research_decision_projection_alternatives_invalid")

        decision_ref = (
            f"refinement:{refinement_id}:rdl:{ledger_id}:decision"
        )
        decision_cid = _relational_projection_candidate_id(decision_ref)
        evidence_refs = item.get("evidence_refs") or []
        if not isinstance(evidence_refs, list | tuple):
            raise ValueError("research_decision_projection_evidence_invalid")
        decision_context = _join_context(
            f"ledger_id={ledger_id}\nentry_id={entry_id}\nstatus=resolved",
            (
                f"anchor={_one_line(item.get('anchor_type'))}:"
                f"{_one_line(item.get('anchor_ref'))}"
            ),
            f"confidence={_one_line(item.get('confidence'))}",
            "evidence_refs="
            + json.dumps(
                sorted(str(value) for value in evidence_refs),
                ensure_ascii=False,
            ),
        )
        result.nodes.append(
            EmittedNode(
                candidate_id=decision_cid,
                node_type="Decision",
                title=decision[:120],
                content=_join_context(
                    decision,
                    f"Rationale: {rationale}",
                    f"Unknown: {_one_line(item.get('unknown'))}",
                ),
                context=decision_context,
                source_artifact_ref=decision_ref,
                source_confidence=1.0,
            )
        )
        result.edges.extend(
            (
                EmittedEdge(
                    candidate_id=_relational_projection_edge_id(
                        "belongs_to",
                        decision_cid,
                        refinement_candidate_id,
                    ),
                    edge_type="belongs_to",
                    from_candidate_id=decision_cid,
                    to_candidate_id=refinement_candidate_id,
                    confidence=1.0,
                    rule_id=(
                        f"belongs_to/relational_rdl_decision@{WORKER_VERSION}"
                    ),
                ),
                # A resolved RDL entry is valid without alternatives.  Decision
                # connectivity nevertheless requires a judgement edge in
                # addition to deterministic ownership.  Ground the projection
                # in its Refinement Entity through the existing, schema-valid
                # ``mentions`` taxonomy instead of weakening the generic
                # Decision guard or inventing a new edge type.
                EmittedEdge(
                    candidate_id=_relational_projection_edge_id(
                        "mentions",
                        decision_cid,
                        refinement_candidate_id,
                    ),
                    edge_type="mentions",
                    from_candidate_id=decision_cid,
                    to_candidate_id=refinement_candidate_id,
                    confidence=1.0,
                    rule_id=f"mentions/relational_rdl_owner@{WORKER_VERSION}",
                ),
            )
        )
        result.relational_projection_candidate_ids.add(decision_cid)
        active_refs.append(
            RelationalProjectionActiveRef(
                node_type="Decision",
                candidate_id=decision_cid,
                source_artifact_ref=decision_ref,
            )
        )

        normalized_alternatives = {
            alternative_hash: rendered
            for rendered, alternative_hash in (
                _normalized_alternative(value) for value in alternatives
            )
        }
        for alternative_hash, alternative in sorted(
            normalized_alternatives.items()
        ):
            alternative_ref = (
                f"refinement:{refinement_id}:rdl:{ledger_id}:"
                f"alternative:{alternative_hash}"
            )
            alternative_cid = _relational_projection_candidate_id(
                alternative_ref
            )
            result.nodes.append(
                EmittedNode(
                    candidate_id=alternative_cid,
                    node_type="Alternative",
                    title=alternative[:120],
                    content=alternative,
                    context=f"ledger_id={ledger_id}\nentry_id={entry_id}",
                    source_artifact_ref=alternative_ref,
                    source_confidence=1.0,
                )
            )
            result.edges.extend(
                (
                    EmittedEdge(
                        candidate_id=_relational_projection_edge_id(
                            "belongs_to",
                            alternative_cid,
                            refinement_candidate_id,
                        ),
                        edge_type="belongs_to",
                        from_candidate_id=alternative_cid,
                        to_candidate_id=refinement_candidate_id,
                        confidence=1.0,
                        rule_id=(
                            "belongs_to/relational_rdl_alternative"
                            f"@{WORKER_VERSION}"
                        ),
                    ),
                    EmittedEdge(
                        candidate_id=_relational_projection_edge_id(
                            "relates_to",
                            decision_cid,
                            alternative_cid,
                        ),
                        edge_type="relates_to",
                        from_candidate_id=decision_cid,
                        to_candidate_id=alternative_cid,
                        confidence=1.0,
                        rule_id=(
                            "relates_to/relational_rdl_alternative"
                            f"@{WORKER_VERSION}"
                        ),
                    ),
                )
            )
            result.relational_projection_candidate_ids.add(alternative_cid)
            active_refs.append(
                RelationalProjectionActiveRef(
                    node_type="Alternative",
                    candidate_id=alternative_cid,
                    source_artifact_ref=alternative_ref,
                )
            )

    result.relational_projection_active_set_intent = (
        RelationalProjectionActiveSetIntent(
            owner_type="refinement",
            owner_id=refinement_id,
            namespace="rdl",
            active_refs=tuple(
                sorted(
                    active_refs,
                    key=lambda value: (
                        value.node_type,
                        value.source_artifact_ref,
                    ),
                )
            ),
        )
    )


def _ref_token(value: Any) -> str:
    token = str(value).strip()
    if not token:
        return ""
    token = re.sub(r"\s+", "_", token)
    return token.replace(":", "_")


def _source_item_key(item: Any, index: int) -> str:
    if isinstance(item, dict):
        for field_name in (
            "id",
            "decision_id",
            "scenario_id",
            "contract_id",
            "rule_id",
        ):
            value = item.get(field_name)
            if value not in (None, ""):
                token = _ref_token(value)
                if token:
                    return token
    return str(index)


def _spec_child_ref(spec_id: str, section: str, item: Any, index: int) -> str:
    return f"spec:{spec_id}:{section}:{_source_item_key(item, index)}"


def _arch_value(value: Any) -> str:
    if value in (None, "", [], {}):
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def _arch_lines(*items: tuple[str, Any]) -> str:
    lines = []
    for label, value in items:
        rendered = _arch_value(value)
        if rendered:
            lines.append(f"{label}: {rendered}")
    return "\n".join(lines)


def _architecture_entity_content(entity: dict[str, Any]) -> str:
    return _arch_lines(
        ("Type", entity.get("entity_type")),
        ("Responsibility", entity.get("responsibility")),
        ("Boundaries", entity.get("boundaries")),
        ("Technologies", entity.get("technologies")),
        ("Relationships", entity.get("relationships")),
        ("Notes", entity.get("notes")),
    )


def _architecture_interface_content(interface: dict[str, Any]) -> str:
    return _arch_lines(
        ("Endpoint", interface.get("endpoint")),
        ("Description", interface.get("description")),
        ("Participants", interface.get("participants")),
        ("Direction", interface.get("direction")),
        ("Protocol", interface.get("protocol")),
        ("Contract type", interface.get("contract_type")),
        ("Request schema", interface.get("request_schema")),
        ("Response schema", interface.get("response_schema")),
        ("Event schema", interface.get("event_schema")),
        ("Error contract", interface.get("error_contract")),
        ("Schema ref", interface.get("schema_ref")),
        ("Notes", interface.get("notes")),
    )


def _append_architecture_designs(
    result: WorkerResult,
    *,
    prefix: str,
    artifact_ref: str,
    parent_candidate_id: str,
    raw_parts: list[str],
    architecture_designs: list[Any],
) -> None:
    """Project Architecture Design into existing KG node types.

    v1 intentionally avoids a new graph backend schema. Architecture designs and their
    entities become Entity nodes; interfaces/contracts become APIContract
    nodes. The parent artifact owns all nodes through belongs_to edges.
    """
    for arch_index, raw_design in enumerate(architecture_designs or []):
        if not isinstance(raw_design, dict):
            continue
        design_id = raw_design.get("id") or f"{arch_index}"
        design_ref = f"architecture_design:{design_id}"
        title = raw_design.get("title") or f"Architecture {arch_index + 1}"
        description = raw_design.get("global_description") or ""
        arch_cid = f"{prefix}_arch_{arch_index}"

        raw_parts.extend([title, description])
        result.nodes.append(EmittedNode(
            candidate_id=arch_cid,
            node_type="Entity",
            title=title[:120],
            content=description or title,
            context=f"Architecture design attached to {artifact_ref}",
            source_artifact_ref=design_ref,
            source_confidence=1.0,
        ))
        result.edges.append(EmittedEdge(
            candidate_id=f"{prefix}_belongs_arch_{arch_index}",
            edge_type="belongs_to",
            from_candidate_id=arch_cid,
            to_candidate_id=parent_candidate_id,
            confidence=1.0,
            rule_id=f"belongs_to/architecture_design@{WORKER_VERSION}",
        ))

        for entity_index, raw_entity in enumerate(raw_design.get("entities") or []):
            if not isinstance(raw_entity, dict):
                continue
            entity_id = str(raw_entity.get("id") or entity_index)
            entity_ref = f"{design_ref}:entity:{entity_id}"
            name = raw_entity.get("name") or f"Architecture entity {entity_index + 1}"
            content = _architecture_entity_content(raw_entity)
            raw_parts.extend([name, content])
            entity_cid = f"{prefix}_arch_{arch_index}_entity_{entity_index}"
            result.nodes.append(EmittedNode(
                candidate_id=entity_cid,
                node_type="Entity",
                title=name[:120],
                content=content or name,
                source_artifact_ref=entity_ref,
                source_confidence=1.0,
            ))
            result.edges.append(EmittedEdge(
                candidate_id=f"{prefix}_belongs_arch_{arch_index}_entity_{entity_index}",
                edge_type="belongs_to",
                from_candidate_id=entity_cid,
                to_candidate_id=arch_cid,
                confidence=1.0,
                rule_id=f"belongs_to/architecture_entity@{WORKER_VERSION}",
            ))

        for interface_index, raw_interface in enumerate(raw_design.get("interfaces") or []):
            if not isinstance(raw_interface, dict):
                continue
            interface_id = str(raw_interface.get("id") or interface_index)
            interface_ref = f"{design_ref}:interface:{interface_id}"
            name = raw_interface.get("name") or f"Architecture interface {interface_index + 1}"
            content = _architecture_interface_content(raw_interface)
            raw_parts.extend([name, content])
            interface_cid = f"{prefix}_arch_{arch_index}_interface_{interface_index}"
            result.nodes.append(EmittedNode(
                candidate_id=interface_cid,
                node_type="APIContract",
                title=name[:120],
                content=content or name,
                source_artifact_ref=interface_ref,
                source_confidence=1.0,
            ))
            result.edges.append(EmittedEdge(
                candidate_id=f"{prefix}_belongs_arch_{arch_index}_interface_{interface_index}",
                edge_type="belongs_to",
                from_candidate_id=interface_cid,
                to_candidate_id=arch_cid,
                confidence=1.0,
                rule_id=f"belongs_to/architecture_interface@{WORKER_VERSION}",
            ))

        for diagram in raw_design.get("diagrams") or []:
            if not isinstance(diagram, dict):
                continue
            raw_parts.append(_arch_lines(
                ("Diagram", diagram.get("title")),
                ("Type", diagram.get("diagram_type")),
                ("Format", diagram.get("format")),
                ("Description", diagram.get("description")),
                ("Content hash", diagram.get("content_hash")),
            ))


_CODE_TRACEABILITY_SPEC_ENDPOINTS: dict[str, tuple[str, str]] = {
    "spec": ("Entity", ""),
    "functional_requirement": ("Requirement", "fr"),
    "technical_requirement": ("Constraint", "tr"),
    "acceptance_criterion": ("Criterion", "ac"),
    "business_rule": ("Constraint", "business_rule"),
    "api_contract": ("APIContract", "api_contract"),
    "integration_requirement": ("Requirement", "integration_requirement"),
    "observability_requirement": ("Constraint", "observability_requirement"),
    "decision": ("Decision", "decision"),
    "test_scenario": ("TestScenario", "test_scenario"),
}


def _required_traceability_string(
    payload: dict[str, Any],
    key: str,
    *,
    max_length: int,
) -> str:
    value = str(payload.get(key) or "").strip()
    if not value or len(value) > max_length:
        raise ValueError(f"code_traceability_{key}_invalid")
    return value


def _required_traceability_choice(
    payload: dict[str, Any],
    key: str,
    *,
    allowed: frozenset[str],
    value: Any | None = None,
) -> str:
    normalized = str(payload.get(key) if value is None else value).strip()
    if normalized not in allowed:
        raise ValueError(f"code_traceability_{key}_invalid")
    return normalized


def _optional_traceability_string(
    payload: dict[str, Any],
    key: str,
    *,
    max_length: int,
) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized or len(normalized) > max_length:
        raise ValueError(f"code_traceability_{key}_invalid")
    return normalized


def _reject_traceability_unexpected_fields(
    payload: dict[str, Any],
    allowed_fields: frozenset[str],
) -> None:
    """Fail closed before relational projection data reaches the graph.

    The Community adapter owns the explicit row-to-dict contract.  Rejecting
    additions here keeps transport secrets, excerpts and other accidental
    columns out of raw-content hashes and graph properties.
    """

    if set(payload).difference(allowed_fields):
        raise ValueError("code_traceability_projection_fields_invalid")


def _required_traceability_digest(
    payload: dict[str, Any],
    key: str,
) -> str:
    value = _required_traceability_string(payload, key, max_length=64)
    if re.fullmatch(r"[0-9a-fA-F]{64}", value) is None:
        raise ValueError(f"code_traceability_{key}_invalid")
    return value


def _optional_traceability_digest(
    payload: dict[str, Any],
    key: str,
) -> str | None:
    value = _optional_traceability_string(payload, key, max_length=64)
    if value is not None and re.fullmatch(r"[0-9a-fA-F]{64}", value) is None:
        raise ValueError(f"code_traceability_{key}_invalid")
    return value


def _traceability_sequence(
    payload: dict[str, Any],
    key: str,
    *,
    limit: int,
) -> tuple[dict[str, Any], ...]:
    value = payload.get(key, ())
    if value is None:
        return ()
    if not isinstance(value, list | tuple) or len(value) > limit:
        raise ValueError(f"code_traceability_{key}_invalid")
    if any(not isinstance(item, dict) for item in value):
        raise ValueError(f"code_traceability_{key}_invalid")
    return tuple(dict(item) for item in value)


def _traceability_id_sequence(
    payload: dict[str, Any],
    key: str,
    *,
    limit: int,
) -> tuple[str, ...]:
    value = payload.get(key, ())
    if value is None:
        return ()
    if not isinstance(value, list | tuple) or len(value) > limit:
        raise ValueError(f"code_traceability_{key}_invalid")
    normalized = tuple(str(item or "").strip() for item in value)
    if any(not item or len(item) > 255 for item in normalized):
        raise ValueError(f"code_traceability_{key}_invalid")
    return tuple(dict.fromkeys(normalized))


def _traceability_raw(payload: dict[str, Any], fields: tuple[str, ...]) -> str:
    """Hash input contains only the explicit relational projection allowlist."""

    return json.dumps(
        {key: payload.get(key) for key in fields},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        default=str,
    )


def _source_ref_endpoint(node_type: str, source_artifact_ref: str) -> str:
    return f"kgref:{node_type}:{source_artifact_ref}"


def _spec_link_endpoint(link: dict[str, Any]) -> tuple[str, str]:
    spec_id = _required_traceability_string(link, "spec_id", max_length=255)
    entity_type = _required_traceability_string(
        link,
        "entity_type",
        max_length=64,
    )
    endpoint = _CODE_TRACEABILITY_SPEC_ENDPOINTS.get(entity_type)
    if endpoint is None:
        raise ValueError("code_traceability_spec_entity_type_invalid")
    node_type, section = endpoint
    if not section:
        return node_type, f"spec:{spec_id}"
    entity_id = _required_traceability_string(link, "entity_id", max_length=255)
    return node_type, f"spec:{spec_id}:{section}:{_ref_token(entity_id)}"


class DeterministicWorker:
    """Stateless Layer 1 extractor.

    Not thread-safe; instantiate per-request or per-task. State lives on the
    returned WorkerResult, so callers can persist or diff between runs.
    """

    def __init__(self, worker_id: str = WORKER_ID) -> None:
        self.worker_id = worker_id

    # ------------------------------------------------------------------
    # Spec entry point
    # ------------------------------------------------------------------

    def process_spec(self, spec: dict[str, Any]) -> WorkerResult:
        """Extract a full node/edge graph from a Spec dict shape.

        `spec` is the JSON shape the SpecService already serialises — the
        same dict callers get from `okto_pulse_get_spec_context`. Using
        the dict (vs. the SQLAlchemy row) keeps the worker pure and unit-
        testable without a DB.
        """
        spec_id = spec["id"]
        board_id = spec.get("board_id")
        prefix = f"spec_{spec_id[:8]}"
        artifact_ref = f"spec:{spec_id}"
        result = WorkerResult(raw_content="")
        quality_assessments = _projection_records(
            spec.get("quality_assessments")
        )
        spec_dependencies = _projection_records(spec.get("spec_dependencies"))
        if not spec.get("refinement_id") and not spec.get("ideation_id"):
            result.spec_lineage_parent_intent = SpecLineageParentIntent.CLEAR
        raw_parts: list[str] = [
            spec.get("title") or "",
            spec.get("description") or "",
            spec.get("context") or "",
        ]
        if "quality_assessments" in spec:
            raw_parts.append(
                _projection_raw(
                    "quality_assessments",
                    quality_assessments,
                )
            )
        if "spec_dependencies" in spec:
            raw_parts.append(
                _projection_raw("spec_dependencies", spec_dependencies)
            )

        # 1. Spec entity (anchor node) — used by hierarchy edges on caller.
        spec_entity_id = f"{prefix}_entity"
        result.nodes.append(EmittedNode(
            candidate_id=spec_entity_id,
            node_type="Entity",
            title=spec.get("title") or f"Spec {spec_id}",
            content=spec.get("description") or "",
            context=_join_context(
                spec.get("context") or "",
                _quality_context_block(quality_assessments),
            ),
            source_artifact_ref=artifact_ref,
            source_confidence=1.0,
        ))

        # Operational precedence is a relational projection, not a cognitive
        # inference. The authoritative loader supplies only active outgoing
        # dependencies for this dependent Spec. Direction is prerequisite ->
        # dependent so graph walks follow execution order.
        active_precedence_edges: list[RelationalProjectionActiveEdgeRef] = []
        if "spec_dependencies" in spec:
            for dependency in spec_dependencies:
                dependency_id = str(dependency.get("dependency_id") or "")
                dependent_spec_id = str(
                    dependency.get("dependent_spec_id") or ""
                )
                prerequisite_spec_id = str(
                    dependency.get("prerequisite_spec_id") or ""
                )
                if (
                    not dependency_id
                    or dependent_spec_id != str(spec_id)
                    or not prerequisite_spec_id
                    or prerequisite_spec_id == str(spec_id)
                ):
                    raise ValueError("spec_dependency_projection_invalid")
                # This is a reference to the prerequisite's canonical root,
                # never a partial candidate for that root. A dependency
                # projection must not overwrite title/content/context when the
                # prerequisite was already consolidated. The exact source-ref
                # endpoint is resolved by the graph transaction at commit.
                prerequisite_cid = (
                    f"kgref:Entity:spec:{prerequisite_spec_id}"
                )
                edge_cid = f"{prefix}_precedes_{dependency_id}"
                rule_id = (
                    "precedes/spec_dependency/"
                    f"{dependency_id}@{WORKER_VERSION}"
                )
                result.edges.append(
                    EmittedEdge(
                        candidate_id=edge_cid,
                        edge_type="precedes",
                        from_candidate_id=prerequisite_cid,
                        to_candidate_id=spec_entity_id,
                        confidence=1.0,
                        rule_id=rule_id,
                    )
                )
                active_precedence_edges.append(
                    RelationalProjectionActiveEdgeRef(
                        candidate_id=edge_cid,
                        edge_type="precedes",
                        from_candidate_id=prerequisite_cid,
                        to_candidate_id=spec_entity_id,
                        rule_id=rule_id,
                    )
                )
            result.relational_projection_active_set_intent = (
                RelationalProjectionActiveSetIntent(
                    owner_type="spec",
                    owner_id=str(spec_id),
                    namespace="dependencies",
                    active_refs=(),
                    active_edges=tuple(
                        sorted(
                            active_precedence_edges,
                            key=lambda edge: edge.candidate_id,
                        )
                    ),
                )
            )

        parent_refinement_id = spec.get("refinement_id")
        if parent_refinement_id:
            result.edges.append(EmittedEdge(
                candidate_id=f"{prefix}_belongs_to_refinement",
                edge_type="belongs_to",
                from_candidate_id=spec_entity_id,
                to_candidate_id=f"refinement_{str(parent_refinement_id)[:8]}_entity",
                confidence=1.0,
                rule_id=f"belongs_to/spec_to_refinement@{WORKER_VERSION}",
            ))
        elif spec.get("ideation_id"):
            parent_ideation_id = str(spec["ideation_id"])
            result.edges.append(EmittedEdge(
                candidate_id=f"{prefix}_belongs_to_ideation",
                edge_type="belongs_to",
                from_candidate_id=spec_entity_id,
                to_candidate_id=f"ideation_{parent_ideation_id[:8]}_entity",
                confidence=1.0,
                rule_id=f"belongs_to/spec_to_ideation@{WORKER_VERSION}",
            ))
        _attach_to_board_root(
            result,
            board_id=board_id,
            child_candidate_id=spec_entity_id,
            rule_slot="spec",
        )

        # Helper to attach `belongs_to` edges from each child node to the
        # spec entity, building the hierarchy backbone the UI relies on.
        def _add_belongs_to(child_cid: str, slot: str, idx: int) -> None:
            result.edges.append(EmittedEdge(
                candidate_id=f"{prefix}_belongs_{slot}_{idx}",
                edge_type="belongs_to",
                from_candidate_id=child_cid,
                to_candidate_id=spec_entity_id,
                confidence=1.0,
                rule_id=f"belongs_to/{slot}@{WORKER_VERSION}",
            ))

        # 2. Functional requirements → Requirement (confidence 1.0, deterministic)
        fr_ids: list[tuple[str, str]] = []  # (candidate_id, text)
        # fr_id_to_cid: maps the canonical fr_id (persisted by IMPL-1 on the
        # FR dict) to its candidate_id so that linked_requirements references
        # expressed as fr_ids (rather than positional ints or full text) can
        # resolve deterministically in sections (a) and (b) below.
        fr_id_to_cid: dict[str, str] = {}
        for i, req in enumerate(spec.get("functional_requirements") or []):
            text = req if isinstance(req, str) else (
                req.get("text") or req.get("description") or json.dumps(req)
            )
            raw_parts.append(text)
            cid = f"{prefix}_fr_{i}"
            fr_ids.append((cid, text))
            if isinstance(req, dict):
                fr_id = req.get("id")
                if fr_id not in (None, ""):
                    fr_id_to_cid[str(fr_id)] = cid
            result.nodes.append(EmittedNode(
                candidate_id=cid,
                node_type="Requirement",
                title=text[:120],
                content=text,
                source_artifact_ref=_spec_child_ref(spec_id, "fr", req, i),
                source_confidence=1.0,
            ))
            _add_belongs_to(cid, "fr", i)

        # 3. Technical requirements → Constraint
        tr_ids: list[tuple[str, str]] = []  # (candidate_id, text)
        tr_id_to_cid: dict[str, str] = {}
        for i, req in enumerate(spec.get("technical_requirements") or []):
            if isinstance(req, dict):
                text = req.get("text") or req.get("description") or json.dumps(req)
            else:
                text = str(req)
            raw_parts.append(text)
            cid = f"{prefix}_tr_{i}"
            tr_ids.append((cid, text))
            if isinstance(req, dict):
                tr_id = req.get("id")
                if tr_id not in (None, ""):
                    tr_id_to_cid[str(tr_id)] = cid
            result.nodes.append(EmittedNode(
                candidate_id=cid,
                node_type="Constraint",
                title=text[:120],
                content=text,
                source_artifact_ref=_spec_child_ref(spec_id, "tr", req, i),
                source_confidence=1.0,
            ))
            _add_belongs_to(cid, "tr", i)

        # 4. Acceptance criteria → Criterion (indexed by position — tests
        #    reference them by index in the JSON, mirroring linked_criteria).
        ac_by_index: dict[int, str] = {}
        ac_by_text: dict[str, str] = {}
        for i, crit in enumerate(spec.get("acceptance_criteria") or []):
            text = crit if isinstance(crit, str) else (
                crit.get("text") or crit.get("description") or json.dumps(crit)
            )
            raw_parts.append(text)
            cid = f"{prefix}_ac_{i}"
            ac_by_index[i] = cid
            ac_by_text[text.strip()] = cid
            result.nodes.append(EmittedNode(
                candidate_id=cid,
                node_type="Criterion",
                title=text[:120],
                content=text,
                source_artifact_ref=_spec_child_ref(spec_id, "ac", crit, i),
                source_confidence=1.0,
            ))
            _add_belongs_to(cid, "ac", i)

        # 5. Business rules → Constraint. BR→Requirement linkage is extracted
        #    via `linked_requirements` list so the /violates lookups (Bug→BR)
        #    can resolve to the actual Constraint node later.
        br_ids: list[tuple[str, str]] = []
        for i, rule in enumerate(spec.get("business_rules") or []):
            if isinstance(rule, dict):
                text = rule.get("rule") or rule.get("description") or json.dumps(rule)
                title = rule.get("title") or text[:120]
            else:
                text = str(rule)
                title = text[:120]
            raw_parts.append(text)
            cid = f"{prefix}_br_{i}"
            br_ids.append((cid, text))
            result.nodes.append(EmittedNode(
                candidate_id=cid,
                node_type="Constraint",
                title=title,
                content=text,
                source_artifact_ref=_spec_child_ref(spec_id, "business_rule", rule, i),
                source_confidence=1.0,
            ))
            _add_belongs_to(cid, "br", i)

        # 6. TestScenario + `tests` edges to Criterion.
        for i, ts in enumerate(spec.get("test_scenarios") or []):
            if isinstance(ts, dict):
                title = ts.get("title") or f"TS-{i+1}"
                parts = [f"Given: {ts.get('given','')}",
                         f"When: {ts.get('when','')}",
                         f"Then: {ts.get('then','')}"]
                content = "\n".join(p for p in parts if p.split(": ",1)[1])
                linked = ts.get("linked_criteria") or []
            else:
                title = f"TS-{i+1}"
                content = str(ts)
                linked = []
            raw_parts.append(content)
            ts_cid = f"{prefix}_ts_{i}"
            result.nodes.append(EmittedNode(
                candidate_id=ts_cid,
                node_type="TestScenario",
                title=title,
                content=content,
                source_artifact_ref=_spec_child_ref(spec_id, "test_scenario", ts, i),
                source_confidence=1.0,
            ))
            _add_belongs_to(ts_cid, "ts", i)
            # Emit one `tests` edge per matched linked_criteria entry; missing
            # matches produce a missing_link_candidate so the cognitive agent
            # can propose the edge with capped confidence (fallback policy).
            if not linked:
                result.missing_link_candidates.append(MissingLinkCandidate(
                    edge_type="tests",
                    from_candidate_id=ts_cid,
                    from_candidate_title=title,
                    reason="no_criterion_match",
                    suggested_candidates=list(ac_by_text.values()),
                    artifact_ref=artifact_ref,
                ))
                continue
            for idx, link in enumerate(linked):
                target_cid = None
                if isinstance(link, int) and link in ac_by_index:
                    target_cid = ac_by_index[link]
                elif isinstance(link, str):
                    # Try exact text match first, then index lookup.
                    target_cid = ac_by_text.get(link.strip())
                    if target_cid is None:
                        try:
                            target_cid = ac_by_index.get(int(link))
                        except (ValueError, TypeError):
                            pass
                if target_cid is None:
                    result.missing_link_candidates.append(MissingLinkCandidate(
                        edge_type="tests",
                        from_candidate_id=ts_cid,
                        from_candidate_title=title,
                        reason="no_criterion_match",
                        suggested_candidates=list(ac_by_text.values()),
                        artifact_ref=artifact_ref,
                    ))
                    continue
                result.edges.append(EmittedEdge(
                    candidate_id=f"{prefix}_edge_ts{i}_to_ac{idx}",
                    edge_type="tests",
                    from_candidate_id=ts_cid,
                    to_candidate_id=target_cid,
                    confidence=1.0,
                    rule_id=f"tests/ac_match@{WORKER_VERSION}",
                ))

        # 7. APIContract + `implements` edges to Requirement/Constraint via linked_requirements.
        fr_text_to_cid = {text.strip(): cid for cid, text in fr_ids}
        tr_text_to_cid = {text.strip(): cid for cid, text in tr_ids}
        requirement_candidate_suggestions = [c for c, _ in fr_ids] + [c for c, _ in tr_ids]
        api_ids_by_id: dict[str, str] = {}
        for i, api in enumerate(spec.get("api_contracts") or []):
            if not isinstance(api, dict):
                continue
            method = api.get("method", "")
            path = api.get("path", "")
            title = f"{method} {path}".strip() or f"API-{i+1}"
            content = api.get("description") or json.dumps(api)
            raw_parts.append(content)
            api_cid = f"{prefix}_api_{i}"
            result.nodes.append(EmittedNode(
                candidate_id=api_cid,
                node_type="APIContract",
                title=title,
                content=content,
                source_artifact_ref=_spec_child_ref(spec_id, "api_contract", api, i),
                source_confidence=1.0,
            ))
            _add_belongs_to(api_cid, "api", i)
            api_id = api.get("id")
            if api_id:
                api_ids_by_id[str(api_id)] = api_cid
            linked = api.get("linked_requirements") or []
            if not linked:
                result.missing_link_candidates.append(MissingLinkCandidate(
                    edge_type="implements",
                    from_candidate_id=api_cid,
                    from_candidate_title=title,
                    reason="no_requirement_match",
                    suggested_candidates=requirement_candidate_suggestions,
                    artifact_ref=artifact_ref,
                ))
                continue
            for idx, link in enumerate(linked):
                if not isinstance(link, str):
                    continue
                # (a) Resolve linked_requirements entry: try canonical FR/TR
                # ids first, then fall back to full-text match for legacy refs.
                stripped_link = link.strip()
                target = (
                    fr_id_to_cid.get(stripped_link)
                    or tr_id_to_cid.get(stripped_link)
                    or fr_text_to_cid.get(stripped_link)
                    or tr_text_to_cid.get(stripped_link)
                )
                if target is None:
                    result.missing_link_candidates.append(MissingLinkCandidate(
                        edge_type="implements",
                        from_candidate_id=api_cid,
                        from_candidate_title=title,
                        reason="no_requirement_match",
                        suggested_candidates=requirement_candidate_suggestions,
                        artifact_ref=artifact_ref,
                    ))
                    continue
                result.edges.append(EmittedEdge(
                    candidate_id=f"{prefix}_edge_api{i}_to_fr{idx}",
                    edge_type="implements",
                    from_candidate_id=api_cid,
                    to_candidate_id=target,
                    confidence=1.0,
                    rule_id=f"implements/fr_match@{WORKER_VERSION}",
                ))

        # 7a. Integration Requirements → Requirement. IRs reuse the existing
        # Requirement node type by design: they are actionable requirements
        # about APIs, queues, stored procedures, events, files, and data
        # contracts. When linked_api_contracts is present, emit deterministic
        # APIContract → Requirement implements edges.
        ir_ids_by_id: dict[str, str] = {}
        for i, ir in enumerate(spec.get("integration_requirements") or []):
            if not isinstance(ir, dict) or ir.get("status", "active") != "active":
                continue
            title = ir.get("title") or f"IR-{i+1}"
            content_parts = [
                ir.get("description") or "",
                f"type={ir.get('integration_type')}" if ir.get("integration_type") else "",
                f"provider={ir.get('provider')}" if ir.get("provider") else "",
                f"consumer={ir.get('consumer')}" if ir.get("consumer") else "",
                f"endpoint={ir.get('endpoint')}" if ir.get("endpoint") else "",
                f"contract_ref={ir.get('contract_ref')}" if ir.get("contract_ref") else "",
            ]
            content = "\n".join(part for part in content_parts if part).strip() or title
            raw_parts.append(content)
            ir_cid = f"{prefix}_ir_{i}"
            ir_id = ir.get("id")
            if ir_id:
                ir_ids_by_id[str(ir_id)] = ir_cid
            result.nodes.append(EmittedNode(
                candidate_id=ir_cid,
                node_type="Requirement",
                title=title[:120],
                content=content,
                source_artifact_ref=_spec_child_ref(spec_id, "integration_requirement", ir, i),
                source_confidence=1.0,
            ))
            _add_belongs_to(ir_cid, "ir", i)
            for idx, contract_ref in enumerate(ir.get("linked_api_contracts") or []):
                api_cid = api_ids_by_id.get(str(contract_ref))
                if api_cid is None:
                    result.missing_link_candidates.append(MissingLinkCandidate(
                        edge_type="implements",
                        from_candidate_id=f"api_contract:{contract_ref}",
                        from_candidate_title=str(contract_ref),
                        reason="no_api_contract_match",
                        suggested_candidates=list(api_ids_by_id.values()),
                        artifact_ref=artifact_ref,
                    ))
                    continue
                result.edges.append(EmittedEdge(
                    candidate_id=f"{prefix}_edge_api_{idx}_implements_ir_{i}",
                    edge_type="implements",
                    from_candidate_id=api_cid,
                    to_candidate_id=ir_cid,
                    confidence=1.0,
                    rule_id=f"implements/ir_api_contract_link@{WORKER_VERSION}",
                ))

        # 7c. Observability Requirements → Constraint. ORs reuse the existing
        # Constraint node type because they constrain delivery with dashboards,
        # metrics, alerts, SLOs, thresholds, logs, or traces.
        for i, req in enumerate(spec.get("observability_requirements") or []):
            if not isinstance(req, dict) or req.get("status", "active") != "active":
                continue
            title = req.get("title") or f"OR-{i+1}"
            content_parts = [
                req.get("description") or "",
                f"signal={req.get('signal_type')}" if req.get("signal_type") else "",
                f"target={req.get('target')}" if req.get("target") else "",
                f"metric={req.get('metric_name')}" if req.get("metric_name") else "",
                f"threshold={req.get('threshold')}" if req.get("threshold") else "",
                f"severity={req.get('severity')}" if req.get("severity") else "",
            ]
            content = "\n".join(part for part in content_parts if part).strip() or title
            raw_parts.append(content)
            or_cid = f"{prefix}_or_{i}"
            result.nodes.append(EmittedNode(
                candidate_id=or_cid,
                node_type="Constraint",
                title=title[:120],
                content=content,
                source_artifact_ref=_spec_child_ref(spec_id, "observability_requirement", req, i),
                source_confidence=1.0,
            ))
            _add_belongs_to(or_cid, "or", i)

        # 7b. Architecture Design light KG projection. No new graph backend types are
        # needed: architecture envelope/entities map to Entity, while
        # interfaces/contracts map to APIContract.
        _append_architecture_designs(
            result,
            prefix=prefix,
            artifact_ref=artifact_ref,
            parent_candidate_id=spec_entity_id,
            raw_parts=raw_parts,
            architecture_designs=spec.get("architecture_designs") or [],
        )

        # 8a. Formalized decisions from spec.decisions[] (spec b66d2562) —
        #     structured entries win over the legacy markdown regex. Only
        #     `active` decisions are emitted; superseded/revoked keep their
        #     historical nodes from earlier commits (supersedence is written
        #     on-state via subsequent commits).
        formal_decisions = [
            d for d in (spec.get("decisions") or [])
            if isinstance(d, dict)
            and d.get("status", "active") == "active"
            and d.get("title")
        ]
        tech_whitelist_version = _load_tech_whitelist()[1]
        formal_titles: set[str] = {d["title"].strip() for d in formal_decisions}
        for i, dec in enumerate(formal_decisions):
            dec_title = dec["title"]
            dec_text = dec.get("rationale") or dec_title
            raw_parts.append(dec_text)
            dec_cid = f"{prefix}_fdec_{i}"
            result.nodes.append(EmittedNode(
                candidate_id=dec_cid,
                node_type="Decision",
                title=dec_title[:120],
                content=dec_text,
                context=dec.get("context") or "",
                source_artifact_ref=_spec_child_ref(spec_id, "decision", dec, i),
                source_confidence=1.0,
            ))
            _add_belongs_to(dec_cid, "fdec", i)
            # derives_from — link to linked_requirements when provided,
            # otherwise fall back to co-occurrence (all FRs, confidence 0.6).
            # (b) Resolve refs: try canonical fr_id first (IMPL-1 persists the
            # id field on FR dicts), then fall back to positional int index for
            # specs written before IMPL-1.  Unresolvable refs are silently
            # skipped so the co-occurrence fallback still fires when the
            # explicit_fr_cids set ends up empty.
            explicit_fr_cids: set[str] = set()
            for ref in (dec.get("linked_requirements") or []):
                ref_str = str(ref) if ref is not None else ""
                # Try fr_id lookup first.
                resolved = fr_id_to_cid.get(ref_str)
                if resolved is None:
                    # Legacy fallback: interpret ref as a positional int index.
                    try:
                        idx_int = int(ref_str)
                        if 0 <= idx_int < len(fr_ids):
                            resolved = fr_ids[idx_int][0]
                    except (TypeError, ValueError):
                        pass
                if resolved is not None:
                    explicit_fr_cids.add(resolved)
            for j, (fr_cid, _fr_text) in enumerate(fr_ids):
                is_explicit = fr_cid in explicit_fr_cids
                if explicit_fr_cids and not is_explicit:
                    continue
                result.edges.append(EmittedEdge(
                    candidate_id=f"{prefix}_edge_fdec{i}_derives_fr_{fr_cid}",
                    edge_type="derives_from",
                    from_candidate_id=dec_cid,
                    to_candidate_id=fr_cid,
                    confidence=1.0 if is_explicit else 0.6,
                    rule_id=(
                        f"derives_from/explicit_link@{WORKER_VERSION}"
                        if is_explicit
                        else f"derives_from/cooccurrence@{WORKER_VERSION}"
                    ),
                ))
            # mentions via tech whitelist — same as legacy path.
            for canonical in _extract_tech_mentions(dec_text):
                ent_cid = f"ent_{_canonical_slug(canonical)}"
                if not any(n.candidate_id == ent_cid for n in result.nodes):
                    result.nodes.append(EmittedNode(
                        candidate_id=ent_cid,
                        node_type="Entity",
                        title=canonical,
                        content=canonical,
                        source_artifact_ref="tech_entities.yml",
                        source_confidence=1.0,
                    ))
                result.edges.append(EmittedEdge(
                    candidate_id=f"{prefix}_edge_fdec{i}_mentions_{ent_cid}",
                    edge_type="mentions",
                    from_candidate_id=dec_cid,
                    to_candidate_id=ent_cid,
                    confidence=1.0,
                    rule_id=f"mentions/tech_whitelist@v{tech_whitelist_version}",
                ))

        # 8. Legacy fallback: "## Decisions" bullets in context → Decision nodes
        #    (backward-compat until the spec is migrated via
        #    okto_pulse_migrate_spec_decisions). Skips titles already emitted
        #    from the formalized path above to avoid duplicate candidates in
        #    the same session.
        decisions_text = (
            []
            if _inherits_refinement_decision_context(spec)
            else _extract_decisions_from_context(spec.get("context") or "")
        )
        decisions_text = [t for t in decisions_text if t.strip() not in formal_titles]
        for i, dec_text in enumerate(decisions_text):
            raw_parts.append(dec_text)
            dec_cid = f"{prefix}_dec_{i}"
            result.nodes.append(EmittedNode(
                candidate_id=dec_cid,
                node_type="Decision",
                title=dec_text[:120],
                content=dec_text,
                source_artifact_ref=_spec_child_ref(spec_id, "decision_legacy", dec_text, i),
                source_confidence=1.0,
            ))
            _add_belongs_to(dec_cid, "dec", i)
            # derives_from — low-confidence co-occurrence. Cognitive layer can
            # narrow this down to the specific FR if confidence <0.7.
            for fr_cid, _fr_text in fr_ids:
                result.edges.append(EmittedEdge(
                    candidate_id=f"{prefix}_edge_dec{i}_derives_fr_{fr_cid}",
                    edge_type="derives_from",
                    from_candidate_id=dec_cid,
                    to_candidate_id=fr_cid,
                    confidence=0.6,
                    rule_id=f"derives_from/cooccurrence@{WORKER_VERSION}",
                ))
            # mentions via tech whitelist. confidence=1.0 for exact canonical/
            # alias hit; we don't enable stemming for any entity yet so the
            # 0.85 stem path is unused (guarded for future extensions).
            for canonical in _extract_tech_mentions(dec_text):
                ent_cid = f"ent_{_canonical_slug(canonical)}"
                # Entity nodes for canonical techs are emitted once per spec
                # run; dedup happens at the commit layer, but we still guard
                # here so the session_count stays accurate.
                if not any(n.candidate_id == ent_cid for n in result.nodes):
                    result.nodes.append(EmittedNode(
                        candidate_id=ent_cid,
                        node_type="Entity",
                        title=canonical,
                        content=canonical,
                        source_artifact_ref="tech_entities.yml",
                        source_confidence=1.0,
                    ))
                result.edges.append(EmittedEdge(
                    candidate_id=f"{prefix}_edge_dec{i}_mentions_{ent_cid}",
                    edge_type="mentions",
                    from_candidate_id=dec_cid,
                    to_candidate_id=ent_cid,
                    confidence=1.0,
                    rule_id=f"mentions/tech_whitelist@v{tech_whitelist_version}",
                ))

        # 9. Content hash — used by BR `Idempotent Commit via content_hash`.
        raw = "\n---\n".join(p for p in raw_parts if p)
        result.raw_content = raw
        result.content_hash = _sha256(raw)

        graph_layer, maturity_status = _layer_attrs_for_artifact(
            "spec",
            spec.get("status"),
        )
        _apply_layer_to_result(
            result,
            graph_layer=graph_layer,
            maturity_status=maturity_status,
        )
        logger.info(
            "deterministic_worker.spec_processed spec=%s nodes=%d edges=%d "
            "missing=%d det_ratio=%.2f",
            spec_id, len(result.nodes), len(result.edges),
            len(result.missing_link_candidates), result.deterministic_edge_ratio(),
            extra={
                "event": "deterministic_worker.spec_processed",
                "spec_id": spec_id,
                "node_count": len(result.nodes),
                "edge_count": len(result.edges),
                "missing_count": len(result.missing_link_candidates),
                "deterministic_edge_ratio": result.deterministic_edge_ratio(),
                "content_hash": result.content_hash,
                "worker_version": WORKER_VERSION,
            },
        )
        return result


    # ------------------------------------------------------------------
    # Pre-spec artifact entry points — Story / Ideation / Refinement
    # ------------------------------------------------------------------

    def process_story(self, story: dict[str, Any]) -> WorkerResult:
        sid = story["id"]
        board_id = story.get("board_id")
        prefix = f"story_{sid[:8]}"
        artifact_ref = f"story:{sid}"
        result = WorkerResult()
        labels = story.get("labels") or []
        raw_parts = [
            story.get("title") or "",
            story.get("description") or "",
            story.get("actor") or "",
            story.get("goal") or "",
            story.get("benefit") or "",
            json.dumps(labels, ensure_ascii=False, sort_keys=True) if labels else "",
        ]
        context_parts = [
            f"Topic: {story.get('topic_id')}" if story.get("topic_id") else "",
            f"Status: {story.get('status')}" if story.get("status") else "",
        ]
        result.nodes.append(EmittedNode(
            candidate_id=f"{prefix}_entity",
            node_type="Entity",
            title=story.get("title") or f"Story {sid}",
            content=story.get("description") or story.get("title") or "",
            context="\n".join(p for p in context_parts if p),
            source_artifact_ref=artifact_ref,
            source_confidence=1.0,
        ))
        _attach_to_board_root(
            result,
            board_id=board_id,
            child_candidate_id=f"{prefix}_entity",
            rule_slot="story",
        )
        raw = "\n---\n".join(p for p in raw_parts if p)
        result.raw_content = raw
        result.content_hash = _sha256(raw)
        graph_layer, maturity_status = _layer_attrs_for_artifact(
            "story",
            story.get("status"),
        )
        _apply_layer_to_result(
            result,
            graph_layer=graph_layer,
            maturity_status=maturity_status,
        )
        logger.info(
            "deterministic_worker.story_processed story=%s nodes=%d edges=%d",
            sid, len(result.nodes), len(result.edges),
            extra={
                "event": "deterministic_worker.story_processed",
                "story_id": sid,
                "content_hash": result.content_hash,
                "worker_version": WORKER_VERSION,
            },
        )
        return result

    def process_ideation(self, ideation: dict[str, Any]) -> WorkerResult:
        iid = ideation["id"]
        board_id = ideation.get("board_id")
        prefix = f"ideation_{iid[:8]}"
        artifact_ref = f"ideation:{iid}"
        result = WorkerResult()
        quality_assessments = _projection_records(
            ideation.get("quality_assessments")
        )
        raw_parts = [
            ideation.get("title") or "",
            ideation.get("description") or "",
            ideation.get("problem_statement") or "",
            ideation.get("proposed_approach") or "",
            json.dumps(ideation.get("scope_assessment") or {}, ensure_ascii=False, sort_keys=True),
            json.dumps(ideation.get("labels") or [], ensure_ascii=False, sort_keys=True),
        ]
        if "quality_assessments" in ideation:
            raw_parts.append(
                _projection_raw(
                    "quality_assessments",
                    quality_assessments,
                )
            )
        content = "\n\n".join(
            p for p in (
                ideation.get("description"),
                ideation.get("problem_statement"),
                ideation.get("proposed_approach"),
            )
            if p
        )
        ideation_cid = f"{prefix}_entity"
        result.nodes.append(EmittedNode(
            candidate_id=ideation_cid,
            node_type="Entity",
            title=ideation.get("title") or f"Ideation {iid}",
            content=content or ideation.get("title") or "",
            context=_join_context(
                (
                    f"Status: {ideation.get('status') or ''}\n"
                    f"Complexity: {ideation.get('complexity') or ''}"
                ).strip(),
                _quality_context_block(quality_assessments),
            ),
            source_artifact_ref=artifact_ref,
            source_confidence=1.0,
        ))

        story_ids = ideation.get("story_ids") or []
        if isinstance(story_ids, str):
            story_ids = [story_ids]
        for idx, story_id in enumerate(str(s) for s in story_ids if s not in (None, "")):
            result.edges.append(EmittedEdge(
                candidate_id=f"{prefix}_belongs_story_{idx}",
                edge_type="belongs_to",
                from_candidate_id=f"story_{story_id[:8]}_entity",
                to_candidate_id=ideation_cid,
                confidence=1.0,
                rule_id=f"belongs_to/story_to_ideation@{WORKER_VERSION}",
            ))
        _attach_to_board_root(
            result,
            board_id=board_id,
            child_candidate_id=ideation_cid,
            rule_slot="ideation",
        )

        raw = "\n---\n".join(p for p in raw_parts if p and p not in ("{}", "[]"))
        result.raw_content = raw
        result.content_hash = _sha256(raw)
        graph_layer, maturity_status = _layer_attrs_for_artifact(
            "ideation",
            ideation.get("status"),
        )
        _apply_layer_to_result(
            result,
            graph_layer=graph_layer,
            maturity_status=maturity_status,
        )
        logger.info(
            "deterministic_worker.ideation_processed ideation=%s nodes=%d edges=%d",
            iid, len(result.nodes), len(result.edges),
            extra={
                "event": "deterministic_worker.ideation_processed",
                "ideation_id": iid,
                "content_hash": result.content_hash,
                "worker_version": WORKER_VERSION,
            },
        )
        return result

    def process_refinement(self, refinement: dict[str, Any]) -> WorkerResult:
        rid = refinement["id"]
        board_id = refinement.get("board_id")
        prefix = f"refinement_{rid[:8]}"
        artifact_ref = f"refinement:{rid}"
        result = WorkerResult()
        quality_assessments = _projection_records(
            refinement.get("quality_assessments")
        )
        research_decisions = _projection_records(
            refinement.get("research_decisions")
        )
        raw_parts = [
            refinement.get("title") or "",
            refinement.get("description") or "",
            "\n".join(refinement.get("in_scope") or []),
            "\n".join(refinement.get("out_of_scope") or []),
            refinement.get("analysis") or "",
            json.dumps(refinement.get("decisions") or [], ensure_ascii=False, sort_keys=True),
            json.dumps(refinement.get("labels") or [], ensure_ascii=False, sort_keys=True),
        ]
        if "quality_assessments" in refinement:
            raw_parts.append(
                _projection_raw(
                    "quality_assessments",
                    quality_assessments,
                )
            )
        if "research_decisions" in refinement:
            raw_parts.append(
                _projection_raw(
                    "research_decisions",
                    research_decisions,
                )
            )
        content = "\n\n".join(
            p for p in (
                refinement.get("description"),
                refinement.get("analysis"),
            )
            if p
        )
        refinement_cid = f"{prefix}_entity"
        result.nodes.append(EmittedNode(
            candidate_id=refinement_cid,
            node_type="Entity",
            title=refinement.get("title") or f"Refinement {rid}",
            content=content or refinement.get("title") or "",
            context=_join_context(
                f"Status: {refinement.get('status') or ''}",
                _quality_context_block(quality_assessments),
                _research_decision_context_block(research_decisions),
            ),
            source_artifact_ref=artifact_ref,
            source_confidence=1.0,
        ))
        ideation_id = refinement.get("ideation_id")
        if ideation_id:
            ideation_id = str(ideation_id)
            result.edges.append(EmittedEdge(
                candidate_id=f"{prefix}_belongs_to_ideation",
                edge_type="belongs_to",
                from_candidate_id=refinement_cid,
                to_candidate_id=f"ideation_{ideation_id[:8]}_entity",
                confidence=1.0,
                rule_id=f"belongs_to/refinement_to_ideation@{WORKER_VERSION}",
            ))
        _attach_to_board_root(
            result,
            board_id=board_id,
            child_candidate_id=refinement_cid,
            rule_slot="refinement",
        )
        if "research_decisions" in refinement:
            _project_research_decisions(
                refinement_id=rid,
                refinement_candidate_id=refinement_cid,
                records=research_decisions,
                result=result,
            )

        raw = "\n---\n".join(p for p in raw_parts if p and p not in ("[]",))
        result.raw_content = raw
        result.content_hash = _sha256(raw)
        graph_layer, maturity_status = _layer_attrs_for_artifact(
            "refinement",
            refinement.get("status"),
        )
        _apply_layer_to_result(
            result,
            graph_layer=graph_layer,
            maturity_status=maturity_status,
        )
        logger.info(
            "deterministic_worker.refinement_processed refinement=%s nodes=%d edges=%d",
            rid, len(result.nodes), len(result.edges),
            extra={
                "event": "deterministic_worker.refinement_processed",
                "refinement_id": rid,
                "content_hash": result.content_hash,
                "worker_version": WORKER_VERSION,
            },
        )
        return result


    # ------------------------------------------------------------------
    # Sprint entry point — lighter artifact, only Entity + outcome Criterion
    # ------------------------------------------------------------------

    def process_sprint(self, sprint: dict[str, Any]) -> WorkerResult:
        sid = sprint["id"]
        board_id = sprint.get("board_id")
        prefix = f"sprint_{sid[:8]}"
        artifact_ref = f"sprint:{sid}"
        result = WorkerResult()
        lane_type = sprint.get("lane_type") or "normal"
        origin_sprint_id = sprint.get("origin_sprint_id")
        origin_bug_id = sprint.get("origin_bug_id")
        normal_sprint_created = sprint.get("normal_sprint_created")
        if normal_sprint_created is None:
            normal_sprint_created = lane_type == "normal"
        lane_context = (
            f"lane_type={lane_type}\n"
            f"origin_sprint_id={origin_sprint_id or ''}\n"
            f"origin_bug_id={origin_bug_id or ''}\n"
            f"normal_sprint_created={str(bool(normal_sprint_created)).lower()}"
        )
        raw_parts = [sprint.get("title") or "",
                     sprint.get("description") or "",
                     sprint.get("objective") or "",
                     lane_context]

        sprint_cid = f"{prefix}_entity"
        result.nodes.append(EmittedNode(
            candidate_id=sprint_cid,
            node_type="Entity",
            title=sprint.get("title") or f"Sprint {sid}",
            content=sprint.get("description") or "",
            context="\n".join(p for p in [sprint.get("objective") or "", lane_context] if p),
            source_artifact_ref=artifact_ref,
            source_confidence=1.0,
        ))

        if sprint.get("expected_outcome"):
            raw_parts.append(sprint["expected_outcome"])
            oc_cid = f"{prefix}_outcome"
            result.nodes.append(EmittedNode(
                candidate_id=oc_cid,
                node_type="Criterion",
                title=f"Expected Outcome: {sprint.get('title','')}",
                content=sprint["expected_outcome"],
                source_artifact_ref=artifact_ref,
                source_confidence=1.0,
            ))
            # Outcome criterion belongs to the sprint entity itself.
            result.edges.append(EmittedEdge(
                candidate_id=f"{prefix}_belongs_outcome",
                edge_type="belongs_to",
                from_candidate_id=oc_cid,
                to_candidate_id=sprint_cid,
                confidence=1.0,
                rule_id=f"belongs_to/sprint_outcome@{WORKER_VERSION}",
            ))

        # Hierarchy edge: Sprint Entity → Spec Entity. The Spec entity is
        # written by `process_spec` with the deterministic id
        # `spec_<short>_entity`; we reference it via the cross-session
        # `kg:` prefix so the orchestrator resolves it as an existing node
        # without requiring it in this session.
        parent_spec_id = sprint.get("spec_id")
        if parent_spec_id:
            spec_entity_cand = f"spec_{parent_spec_id[:8]}_entity"
            result.edges.append(EmittedEdge(
                candidate_id=f"{prefix}_belongs_to_spec",
                edge_type="belongs_to",
                from_candidate_id=sprint_cid,
                to_candidate_id=spec_entity_cand,
                confidence=1.0,
                rule_id=f"belongs_to/sprint_to_spec@{WORKER_VERSION}",
            ))
        _attach_to_board_root(
            result,
            board_id=board_id,
            child_candidate_id=sprint_cid,
            rule_slot="sprint",
        )

        raw = "\n---\n".join(p for p in raw_parts if p)
        result.raw_content = raw
        result.content_hash = _sha256(raw)
        graph_layer, maturity_status = _layer_attrs_for_artifact(
            "sprint",
            sprint.get("status"),
        )
        _apply_layer_to_result(
            result,
            graph_layer=graph_layer,
            maturity_status=maturity_status,
        )
        logger.info(
            "deterministic_worker.sprint_processed sprint=%s nodes=%d edges=%d",
            sid, len(result.nodes), len(result.edges),
            extra={"event": "deterministic_worker.sprint_processed",
                   "sprint_id": sid, "content_hash": result.content_hash,
                   "worker_version": WORKER_VERSION},
        )
        return result

    # ------------------------------------------------------------------
    # Card entry point — normal/test/bug polymorphism
    # ------------------------------------------------------------------

    def process_card(self, card: dict[str, Any]) -> WorkerResult:
        """Extract a card into the KG. Bugs emit a Bug node + `violates`
        missing_link_candidate (resolution depends on the origin_task_id
        chain — the cognitive agent handles it via fallback)."""
        # Import here to avoid a scoring↔worker cycle at module load time;
        # scoring.py is a lightweight leaf so the indirect import is cheap.
        from okto_pulse.core.kg.scoring import (
            _resolve_priority_boost,
            _resolve_severity_boost,
        )

        cid = card["id"]
        board_id = card.get("board_id")
        prefix = f"card_{cid[:8]}"
        artifact_ref = f"card:{cid}"
        card_type = card.get("card_type") or "normal"
        result = WorkerResult()
        raw_parts = [card.get("title") or "", card.get("description") or ""]
        rework_signal = str(card.get("status") or "").lower() == "rejected"
        if rework_signal:
            raw_parts.append("Lifecycle: Rejected — rework required.")

        if card_type == "bug":
            node_type = "Bug"
        else:
            node_type = "Entity"

        # v0.3.1: resolve priority_boost from card.priority — only the root
        # node of the card carries the boost. Hierarchy/belongs_to nodes
        # (sprint/spec parents) stay at 0.0 per BR "Boost não herda".
        # v0.3.3 (Ideação #4, dec_27de54df): for Bug nodes, severity is the
        # second additive input. MAX preserves the strongest signal without
        # arbitrating which axis dominates.
        if card_type == "bug":
            boost = max(
                _resolve_priority_boost(card.get("priority")),
                _resolve_severity_boost(card.get("severity")),
            )
        else:
            boost = _resolve_priority_boost(card.get("priority"))
        card_cid = f"{prefix}_entity"
        result.nodes.append(EmittedNode(
            candidate_id=card_cid,
            node_type=node_type,
            title=card.get("title") or f"Card {cid}",
            content="\n\n".join(
                part
                for part in (
                    card.get("description") or "",
                    "Lifecycle: Rejected — rework required."
                    if rework_signal
                    else "",
                )
                if part
            ),
            source_artifact_ref=artifact_ref,
            source_confidence=1.0,
            priority_boost=boost,
        ))

        if card_type == "bug":
            # `violates` needs origin_task_id → linked BR/TR → Constraint; we
            # don't have that lookup here, so defer to fallback.
            origin = card.get("origin_task_id")
            if origin:
                raw_parts.append(f"Origin task: {origin}")
            if not origin:
                result.missing_link_candidates.append(MissingLinkCandidate(
                    edge_type="violates",
                    from_candidate_id=card_cid,
                    from_candidate_title=card.get("title") or f"Bug {cid}",
                    reason="no_origin_task",
                    suggested_candidates=[],
                    artifact_ref=artifact_ref,
                ))
            else:
                result.missing_link_candidates.append(MissingLinkCandidate(
                    edge_type="violates",
                    from_candidate_id=card_cid,
                    from_candidate_title=card.get("title") or f"Bug {cid}",
                    reason="origin_task_requires_cross_artifact_resolution",
                    suggested_candidates=[f"task:{origin}"],
                    artifact_ref=artifact_ref,
                ))
            linked_test_task_ids = card.get("linked_test_task_ids") or []
            if isinstance(linked_test_task_ids, str):
                linked_test_task_ids = [linked_test_task_ids]
            linked_test_task_ids = [
                str(test_task_id)
                for test_task_id in linked_test_task_ids
                if test_task_id not in (None, "")
            ]
            if linked_test_task_ids:
                raw_parts.append(
                    "Linked test tasks: "
                    + json.dumps(
                        linked_test_task_ids,
                        ensure_ascii=False,
                        sort_keys=True,
                    )
                )
            for test_task_id in linked_test_task_ids:
                result.missing_link_candidates.append(MissingLinkCandidate(
                    edge_type="tests",
                    from_candidate_id=card_cid,
                    from_candidate_title=card.get("title") or f"Bug {cid}",
                    reason="linked_test_task_requires_cross_artifact_resolution",
                    suggested_candidates=[f"test_task:{test_task_id}"],
                    artifact_ref=artifact_ref,
                ))

        # Hierarchy: Card → Sprint (preferred) or Card → Spec entity.
        # Parents come from FKs in pulse.db; we reference them by their
        # deterministic candidate ids (`spec_<short>_entity` /
        # `sprint_<short>_entity`) which the orchestrator resolves via
        # the prior session's writes.
        sprint_id = card.get("sprint_id")
        spec_id = card.get("spec_id")
        if sprint_id:
            result.edges.append(EmittedEdge(
                candidate_id=f"{prefix}_belongs_to_sprint",
                edge_type="belongs_to",
                from_candidate_id=card_cid,
                to_candidate_id=f"sprint_{sprint_id[:8]}_entity",
                confidence=1.0,
                rule_id=f"belongs_to/card_to_sprint@{WORKER_VERSION}",
            ))
        if spec_id:
            result.edges.append(EmittedEdge(
                candidate_id=f"{prefix}_belongs_to_spec",
                edge_type="belongs_to",
                from_candidate_id=card_cid,
                to_candidate_id=f"spec_{spec_id[:8]}_entity",
                confidence=1.0,
                rule_id=f"belongs_to/card_to_spec@{WORKER_VERSION}",
            ))
        _attach_to_board_root(
            result,
            board_id=board_id,
            child_candidate_id=card_cid,
            rule_slot="card",
        )

        _append_architecture_designs(
            result,
            prefix=prefix,
            artifact_ref=artifact_ref,
            parent_candidate_id=card_cid,
            raw_parts=raw_parts,
            architecture_designs=card.get("architecture_designs") or [],
        )

        raw = "\n---\n".join(p for p in raw_parts if p)
        result.raw_content = raw
        result.content_hash = _sha256(raw)
        graph_layer, maturity_status = _layer_attrs_for_artifact(
            _card_source_artifact_type(card_type),
            card.get("status"),
            has_minimal_evidence=bool(card.get("has_minimal_evidence", True)),
        )
        _apply_layer_to_result(
            result,
            graph_layer=graph_layer,
            maturity_status=maturity_status,
        )
        logger.info(
            "deterministic_worker.card_processed card=%s type=%s nodes=%d missing=%d",
            cid, card_type, len(result.nodes), len(result.missing_link_candidates),
            extra={"event": "deterministic_worker.card_processed",
                   "card_id": cid, "card_type": card_type,
                   "content_hash": result.content_hash,
                   "worker_version": WORKER_VERSION},
        )
        return result

    def process_amendment(self, amendment: dict[str, Any]) -> WorkerResult:
        """Materialize a Path B AmendmentHotfixRevision (spec 7ea1e4be, FR5).

        Emits a SEPARATE ``Entity`` node (never the original spec's node) with
        ``belongs_to`` edges — referencing the EXISTING deterministic candidate
        ids (no placeholders) — to the original spec, the origin bug and each
        regression test task, plus the board root for guaranteed provenance that
        resolves regardless of rebuild ordering. The original done/locked spec
        node is only an edge TARGET, so it is never re-emitted/recanonicalized
        (AC1). The layer is decided by the source-maturity guard via
        ``lineage_complete``: working-only before done, canonical only at
        done + complete lineage. Edge semantics live in the rule_id + node
        content (codex decision: reuse ``belongs_to``, no ``amends`` DDL).
        """
        aid = amendment["id"]
        board_id = amendment.get("board_id")
        prefix = f"amendment_{aid[:8]}"
        artifact_ref = f"amendment_hotfix_revision:{aid}"
        amendment_cid = f"{prefix}_entity"
        result = WorkerResult()

        original_spec_id = str(amendment.get("original_spec_id") or "")
        origin_bug_id = str(amendment.get("origin_bug_id") or "")
        regression_test_task_ids = [
            str(t) for t in (amendment.get("regression_test_task_ids") or []) if t
        ]
        regression_scenario_ids = [
            str(s) for s in (amendment.get("regression_scenario_ids") or []) if s
        ]
        automated_regression_refs = [
            str(r) for r in (amendment.get("automated_regression_refs") or []) if r
        ]

        # Searchable semantics on the node (codex condition 2): make it explicit
        # this is a Path B correction + carry the regression evidence pointers
        # that have no standalone deterministic node candidate id (scenario ids
        # and automated refs) as content rather than dangling placeholder edges.
        content_lines = [
            f"Path B amendment / hotfix revision correcting spec {original_spec_id}.",
            f"Origin bug: {origin_bug_id}.",
        ]
        if regression_scenario_ids:
            content_lines.append(
                "Regression scenarios: "
                + json.dumps(regression_scenario_ids, ensure_ascii=False, sort_keys=True)
            )
        if automated_regression_refs:
            content_lines.append(
                "Automated regression refs: "
                + json.dumps(
                    automated_regression_refs, ensure_ascii=False, sort_keys=True
                )
            )
        content = "\n".join(content_lines)

        result.nodes.append(EmittedNode(
            candidate_id=amendment_cid,
            node_type="Entity",
            title=f"Amendment for spec {original_spec_id[:8]}",
            content=content,
            source_artifact_ref=artifact_ref,
            source_confidence=1.0,
        ))

        if original_spec_id:
            result.edges.append(EmittedEdge(
                candidate_id=f"{prefix}_belongs_to_original_spec",
                edge_type="belongs_to",
                from_candidate_id=amendment_cid,
                to_candidate_id=f"spec_{original_spec_id[:8]}_entity",
                confidence=1.0,
                rule_id=f"belongs_to/amendment_to_original_spec@{WORKER_VERSION}",
            ))
        if origin_bug_id:
            result.edges.append(EmittedEdge(
                candidate_id=f"{prefix}_belongs_to_origin_bug",
                edge_type="belongs_to",
                from_candidate_id=amendment_cid,
                to_candidate_id=f"card_{origin_bug_id[:8]}_entity",
                confidence=1.0,
                rule_id=f"belongs_to/amendment_to_origin_bug@{WORKER_VERSION}",
            ))
        for idx, test_task_id in enumerate(regression_test_task_ids):
            result.edges.append(EmittedEdge(
                candidate_id=f"{prefix}_belongs_to_regression_test_task_{idx}",
                edge_type="belongs_to",
                from_candidate_id=amendment_cid,
                to_candidate_id=f"card_{test_task_id[:8]}_entity",
                confidence=1.0,
                rule_id=f"belongs_to/amendment_to_regression_test_task@{WORKER_VERSION}",
            ))
        # Guaranteed-resolvable provenance (board root is allowlisted + always
        # materialized) so the connectivity guard passes even if spec/bug nodes
        # are enqueued in the same rebuild and resolve later (ordering safety).
        _attach_to_board_root(
            result,
            board_id=board_id,
            child_candidate_id=amendment_cid,
            rule_slot="amendment",
        )

        result.raw_content = content
        result.content_hash = _sha256(content)
        graph_layer, maturity_status = _layer_attrs_for_artifact(
            "amendment_hotfix_revision",
            amendment.get("status"),
            lineage_complete=str(amendment.get("lineage_state") or "").strip().lower()
            == "complete",
        )
        _apply_layer_to_result(
            result,
            graph_layer=graph_layer,
            maturity_status=maturity_status,
        )
        logger.info(
            "deterministic_worker.amendment_processed amendment=%s status=%s "
            "layer=%s nodes=%d edges=%d",
            aid, amendment.get("status"), graph_layer,
            len(result.nodes), len(result.edges),
            extra={"event": "deterministic_worker.amendment_processed",
                   "amendment_id": aid, "content_hash": result.content_hash,
                   "graph_layer": graph_layer, "worker_version": WORKER_VERSION},
        )
        return result

    # ------------------------------------------------------------------
    # Code Traceability relational projections
    # ------------------------------------------------------------------

    def process_code_investigation_receipt(
        self,
        receipt: dict[str, Any],
    ) -> WorkerResult:
        """Materialize one persisted external-agent receipt as Entity."""

        _reject_traceability_unexpected_fields(
            receipt,
            frozenset(
                {
                    "id",
                    "board_id",
                    "status",
                    "acceptance_status",
                    "investigation_source_ref",
                    "attestor_actor_id",
                    "declared_revision",
                    "workspace_state_id",
                    "trust_level",
                    "outcome",
                    "generation",
                    "payload_sha256",
                    "content_hash",
                }
            ),
        )
        receipt_id = _required_traceability_string(receipt, "id", max_length=255)
        board_id = _required_traceability_string(
            receipt,
            "board_id",
            max_length=255,
        )
        investigation_source_ref = _required_traceability_string(
            receipt,
            "investigation_source_ref",
            max_length=512,
        )
        attestor_actor_id = _required_traceability_string(
            receipt,
            "attestor_actor_id",
            max_length=255,
        )
        outcome = _required_traceability_choice(
            receipt,
            "outcome",
            allowed=frozenset({"accessible", "partial", "unavailable"}),
        )
        trust_level = _required_traceability_choice(
            receipt,
            "trust_level",
            allowed=frozenset(
                {"single_attestation", "corroborated", "conflicted"}
            ),
        )
        status = CodeInvestigationReceiptKGStatus(
            _required_traceability_choice(
                receipt,
                "status",
                value=receipt.get("status") or receipt.get("acceptance_status"),
                allowed=CODE_INVESTIGATION_RECEIPT_KG_STATUSES,
            )
        )
        generation = int(receipt.get("generation") or 0)
        if generation < 1:
            raise ValueError("code_traceability_generation_invalid")
        payload_sha256 = _required_traceability_digest(receipt, "payload_sha256")
        content_hash = _optional_traceability_digest(receipt, "content_hash")
        safe_payload = {
            "id": receipt_id,
            "board_id": board_id,
            "status": status.value,
            "investigation_source_ref": investigation_source_ref,
            "attestor_actor_id": attestor_actor_id,
            "declared_revision": receipt.get("declared_revision"),
            "workspace_state_id": receipt.get("workspace_state_id"),
            "trust_level": trust_level,
            "outcome": outcome,
            "generation": generation,
            "payload_sha256": payload_sha256,
        }
        raw_content = _traceability_raw(safe_payload, tuple(safe_payload))
        candidate_id = f"code_receipt_{_ref_token(receipt_id)}_entity"
        result = WorkerResult(raw_content=raw_content)
        result.nodes.append(
            EmittedNode(
                candidate_id=candidate_id,
                node_type="Entity",
                title=f"Code investigation receipt {receipt_id}",
                content=f"outcome={outcome}\ntrust_level={trust_level}",
                source_artifact_ref=f"code_investigation_receipt:{receipt_id}",
                kind_of="code_investigation_receipt",
                investigation_receipt_id=receipt_id,
                source_ref=investigation_source_ref,
                attestor_actor_id=attestor_actor_id,
                declared_revision=_optional_traceability_string(
                    receipt,
                    "declared_revision",
                    max_length=255,
                ),
                workspace_state_id=_optional_traceability_string(
                    receipt,
                    "workspace_state_id",
                    max_length=255,
                ),
                source_content_hash=content_hash or payload_sha256,
            )
        )
        _attach_to_board_root(
            result,
            board_id=board_id,
            child_candidate_id=candidate_id,
            rule_slot="code_traceability_receipt",
        )
        graph_layer, maturity_status = _layer_attrs_for_artifact(
            "code_investigation_receipt",
            status.value,
        )
        _apply_layer_to_result(
            result,
            graph_layer=graph_layer,
            maturity_status=maturity_status,
        )
        result.content_hash = _sha256(raw_content)
        return result

    def process_code_evidence(self, evidence: dict[str, Any]) -> WorkerResult:
        """Materialize immutable Evidence plus persisted links/supersedence."""

        _reject_traceability_unexpected_fields(
            evidence,
            frozenset(
                {
                    "id",
                    "board_id",
                    "lifecycle_status",
                    "investigation_receipt_id",
                    "investigation_source_ref",
                    "declared_revision",
                    "workspace_state_id",
                    "relative_path",
                    "qualified_symbol",
                    "symbol_kind",
                    "selector_kind",
                    "snapshot_line_start",
                    "snapshot_line_end",
                    "declared_source_content_sha256",
                    "evidence_type",
                    "claim",
                    "supersedes_evidence_id",
                    "content_hash",
                    "spec_links",
                }
            ),
        )
        evidence_id = _required_traceability_string(evidence, "id", max_length=255)
        board_id = _required_traceability_string(
            evidence,
            "board_id",
            max_length=255,
        )
        receipt_id = _required_traceability_string(
            evidence,
            "investigation_receipt_id",
            max_length=255,
        )
        investigation_source_ref = _required_traceability_string(
            evidence,
            "investigation_source_ref",
            max_length=512,
        )
        claim = _required_traceability_string(evidence, "claim", max_length=16_384)
        evidence_type = _required_traceability_string(
            evidence,
            "evidence_type",
            max_length=64,
        )
        lifecycle_status = _required_traceability_string(
            evidence,
            "lifecycle_status",
            max_length=32,
        )
        selector_kind = _required_traceability_string(
            evidence,
            "selector_kind",
            max_length=32,
        )
        source_content_hash = _required_traceability_digest(
            evidence,
            "declared_source_content_sha256",
        )
        _optional_traceability_digest(evidence, "content_hash")
        spec_links = _traceability_sequence(evidence, "spec_links", limit=200)
        safe_links = tuple(
            {
                "id": link.get("id"),
                "spec_id": link.get("spec_id"),
                "entity_type": link.get("entity_type"),
                "entity_id": link.get("entity_id"),
                "relation_type": link.get("relation_type"),
            }
            for link in spec_links
        )
        safe_payload = {
            "id": evidence_id,
            "board_id": board_id,
            "investigation_receipt_id": receipt_id,
            "investigation_source_ref": investigation_source_ref,
            "declared_revision": evidence.get("declared_revision"),
            "workspace_state_id": evidence.get("workspace_state_id"),
            "relative_path": evidence.get("relative_path"),
            "qualified_symbol": evidence.get("qualified_symbol"),
            "symbol_kind": evidence.get("symbol_kind"),
            "selector_kind": selector_kind,
            "snapshot_line_start": evidence.get("snapshot_line_start"),
            "snapshot_line_end": evidence.get("snapshot_line_end"),
            "declared_source_content_sha256": source_content_hash,
            "evidence_type": evidence_type,
            "claim": claim,
            "lifecycle_status": lifecycle_status,
            "supersedes_evidence_id": evidence.get("supersedes_evidence_id"),
            "spec_links": safe_links,
        }
        raw_content = _traceability_raw(safe_payload, tuple(safe_payload))
        candidate_id = f"code_evidence_{_ref_token(evidence_id)}_entity"
        result = WorkerResult(raw_content=raw_content)
        line_start = evidence.get("snapshot_line_start")
        line_end = evidence.get("snapshot_line_end")
        if line_start is not None:
            line_start = int(line_start)
        if line_end is not None:
            line_end = int(line_end)
        if (line_start is None) != (line_end is None) or (
            line_start is not None and (line_start < 1 or line_end < line_start)
        ):
            raise ValueError("code_traceability_snapshot_span_invalid")
        result.nodes.append(
            EmittedNode(
                candidate_id=candidate_id,
                node_type="Entity",
                title=claim[:120],
                content=claim,
                source_artifact_ref=f"code_evidence:{evidence_id}",
                kind_of="code_evidence",
                investigation_receipt_id=receipt_id,
                source_ref=investigation_source_ref,
                declared_revision=_optional_traceability_string(
                    evidence,
                    "declared_revision",
                    max_length=255,
                ),
                workspace_state_id=_optional_traceability_string(
                    evidence,
                    "workspace_state_id",
                    max_length=255,
                ),
                code_path=_optional_traceability_string(
                    evidence,
                    "relative_path",
                    max_length=1024,
                ),
                symbol_qualified_name=_optional_traceability_string(
                    evidence,
                    "qualified_symbol",
                    max_length=2048,
                ),
                symbol_kind=_optional_traceability_string(
                    evidence,
                    "symbol_kind",
                    max_length=128,
                ),
                selector_kind=selector_kind,
                source_span_start=line_start,
                source_span_end=line_end,
                source_content_hash=source_content_hash,
            )
        )
        _attach_to_board_root(
            result,
            board_id=board_id,
            child_candidate_id=candidate_id,
            rule_slot="code_traceability_evidence",
        )
        for index, link in enumerate(safe_links):
            relation_type = str(link.get("relation_type") or "").strip()
            if relation_type != "supports":
                continue
            node_type, source_artifact_ref = _spec_link_endpoint(link)
            link_id = str(link.get("id") or index)
            result.edges.append(
                EmittedEdge(
                    candidate_id=(
                        f"code_evidence_{_ref_token(evidence_id)}_supports_"
                        f"{_ref_token(link_id)}"
                    ),
                    edge_type="supports",
                    from_candidate_id=candidate_id,
                    to_candidate_id=_source_ref_endpoint(
                        node_type,
                        source_artifact_ref,
                    ),
                    confidence=1.0,
                    rule_id=f"supports/code_traceability_spec_link@{WORKER_VERSION}",
                )
            )
        superseded_id = _optional_traceability_string(
            evidence,
            "supersedes_evidence_id",
            max_length=255,
        )
        if superseded_id is not None:
            result.edges.append(
                EmittedEdge(
                    candidate_id=(
                        f"code_evidence_{_ref_token(evidence_id)}_supersedes_"
                        f"{_ref_token(superseded_id)}"
                    ),
                    edge_type="supersedes",
                    from_candidate_id=candidate_id,
                    to_candidate_id=_source_ref_endpoint(
                        "Entity",
                        f"code_evidence:{superseded_id}",
                    ),
                    confidence=1.0,
                    rule_id=(
                        f"supersedes/code_traceability_evidence@{WORKER_VERSION}"
                    ),
                )
            )
        graph_layer, maturity_status = _layer_attrs_for_artifact(
            "code_evidence",
            lifecycle_status,
        )
        _apply_layer_to_result(
            result,
            graph_layer=graph_layer,
            maturity_status=maturity_status,
        )
        result.content_hash = _sha256(raw_content)
        return result

    def process_implementation_target(
        self,
        target: dict[str, Any],
    ) -> WorkerResult:
        """Materialize Target intent and its current relational resolution."""

        _reject_traceability_unexpected_fields(
            target,
            frozenset(
                {
                    "id",
                    "board_id",
                    "card_id",
                    "card_node_type",
                    "investigation_source_ref",
                    "selector_kind",
                    "relative_path_hint",
                    "qualified_symbol",
                    "symbol_kind",
                    "role",
                    "intent",
                    "lifecycle_status",
                    "revision",
                    "baseline_evidence_id",
                    "resolution_state",
                    "investigation_receipt_id",
                    "declared_revision",
                    "workspace_state_id",
                    "selector_fingerprint",
                    "resolved_relative_path",
                    "resolved_qualified_symbol",
                    "resolved_symbol_kind",
                    "resolved_line_start",
                    "resolved_line_end",
                    "payload_sha256",
                    "content_hash",
                    "evidence_links",
                    "overlap_target_ids",
                }
            ),
        )
        target_id = _required_traceability_string(target, "id", max_length=255)
        board_id = _required_traceability_string(target, "board_id", max_length=255)
        card_id = _required_traceability_string(target, "card_id", max_length=255)
        investigation_source_ref = _required_traceability_string(
            target,
            "investigation_source_ref",
            max_length=512,
        )
        intent = _required_traceability_string(target, "intent", max_length=16_384)
        role = _required_traceability_string(target, "role", max_length=32)
        lifecycle_status = _required_traceability_string(
            target,
            "lifecycle_status",
            max_length=32,
        )
        selector_kind = _required_traceability_string(
            target,
            "selector_kind",
            max_length=32,
        )
        selector_fingerprint = _optional_traceability_digest(
            target,
            "selector_fingerprint",
        )
        payload_sha256 = _optional_traceability_digest(target, "payload_sha256")
        content_hash = _optional_traceability_digest(target, "content_hash")
        card_node_type = str(target.get("card_node_type") or "Entity").strip()
        if card_node_type not in {"Entity", "Bug"}:
            raise ValueError("code_traceability_card_node_type_invalid")
        evidence_links = _traceability_sequence(target, "evidence_links", limit=200)
        overlap_target_ids = _traceability_id_sequence(
            target,
            "overlap_target_ids",
            limit=200,
        )
        safe_evidence_links = tuple(
            {
                "id": link.get("id"),
                "evidence_id": link.get("evidence_id"),
                "relation_type": link.get("relation_type"),
            }
            for link in evidence_links
        )
        safe_payload = {
            "id": target_id,
            "board_id": board_id,
            "card_id": card_id,
            "card_node_type": card_node_type,
            "investigation_source_ref": investigation_source_ref,
            "selector_kind": selector_kind,
            "relative_path_hint": target.get("relative_path_hint"),
            "qualified_symbol": target.get("qualified_symbol"),
            "symbol_kind": target.get("symbol_kind"),
            "role": role,
            "intent": intent,
            "lifecycle_status": lifecycle_status,
            "revision": target.get("revision"),
            "resolution_state": target.get("resolution_state"),
            "investigation_receipt_id": target.get("investigation_receipt_id"),
            "declared_revision": target.get("declared_revision"),
            "workspace_state_id": target.get("workspace_state_id"),
            "selector_fingerprint": target.get("selector_fingerprint"),
            "resolved_relative_path": target.get("resolved_relative_path"),
            "resolved_qualified_symbol": target.get(
                "resolved_qualified_symbol"
            ),
            "resolved_symbol_kind": target.get("resolved_symbol_kind"),
            "resolved_line_start": target.get("resolved_line_start"),
            "resolved_line_end": target.get("resolved_line_end"),
            "baseline_evidence_id": target.get("baseline_evidence_id"),
            "evidence_links": safe_evidence_links,
            "overlap_target_ids": overlap_target_ids,
        }
        raw_content = _traceability_raw(safe_payload, tuple(safe_payload))
        candidate_id = f"implementation_target_{_ref_token(target_id)}_entity"
        result = WorkerResult(raw_content=raw_content)
        resolved_line_start = target.get("resolved_line_start")
        resolved_line_end = target.get("resolved_line_end")
        if resolved_line_start is not None:
            resolved_line_start = int(resolved_line_start)
        if resolved_line_end is not None:
            resolved_line_end = int(resolved_line_end)
        if (resolved_line_start is None) != (resolved_line_end is None) or (
            resolved_line_start is not None
            and (
                resolved_line_start < 1
                or resolved_line_end < resolved_line_start
            )
        ):
            raise ValueError("code_traceability_resolution_span_invalid")
        result.nodes.append(
            EmittedNode(
                candidate_id=candidate_id,
                node_type="Entity",
                title=f"{role}: {intent}"[:120],
                content=intent,
                source_artifact_ref=f"implementation_target:{target_id}",
                kind_of="implementation_target",
                investigation_receipt_id=_optional_traceability_string(
                    target,
                    "investigation_receipt_id",
                    max_length=255,
                ),
                source_ref=investigation_source_ref,
                declared_revision=_optional_traceability_string(
                    target,
                    "declared_revision",
                    max_length=255,
                ),
                workspace_state_id=_optional_traceability_string(
                    target,
                    "workspace_state_id",
                    max_length=255,
                ),
                code_path=(
                    _optional_traceability_string(
                        target,
                        "resolved_relative_path",
                        max_length=1024,
                    )
                    or _optional_traceability_string(
                        target,
                        "relative_path_hint",
                        max_length=1024,
                    )
                ),
                symbol_qualified_name=(
                    _optional_traceability_string(
                        target,
                        "resolved_qualified_symbol",
                        max_length=2048,
                    )
                    or _optional_traceability_string(
                        target,
                        "qualified_symbol",
                        max_length=2048,
                    )
                ),
                symbol_kind=(
                    _optional_traceability_string(
                        target,
                        "resolved_symbol_kind",
                        max_length=128,
                    )
                    or _optional_traceability_string(
                        target,
                        "symbol_kind",
                        max_length=128,
                    )
                ),
                selector_kind=selector_kind,
                selector_fingerprint=selector_fingerprint,
                resolution_state=_optional_traceability_string(
                    target,
                    "resolution_state",
                    max_length=32,
                ),
                source_span_start=resolved_line_start,
                source_span_end=resolved_line_end,
                source_content_hash=payload_sha256 or content_hash,
            )
        )
        result.edges.append(
            EmittedEdge(
                candidate_id=(
                    f"implementation_target_{_ref_token(target_id)}_belongs_card"
                ),
                edge_type="belongs_to",
                from_candidate_id=candidate_id,
                to_candidate_id=_source_ref_endpoint(
                    card_node_type,
                    f"card:{card_id}",
                ),
                confidence=1.0,
                rule_id=f"belongs_to/code_traceability_target_card@{WORKER_VERSION}",
            )
        )
        evidence_ids: list[str] = []
        baseline_evidence_id = _optional_traceability_string(
            target,
            "baseline_evidence_id",
            max_length=255,
        )
        if baseline_evidence_id is not None:
            evidence_ids.append(baseline_evidence_id)
        for link in safe_evidence_links:
            if str(link.get("relation_type") or "") != "derived_from":
                continue
            evidence_id = _required_traceability_string(
                link,
                "evidence_id",
                max_length=255,
            )
            if evidence_id not in evidence_ids:
                evidence_ids.append(evidence_id)
        for index, evidence_id in enumerate(evidence_ids):
            result.edges.append(
                EmittedEdge(
                    candidate_id=(
                        f"implementation_target_{_ref_token(target_id)}_"
                        f"derives_evidence_{index}"
                    ),
                    edge_type="derives_from",
                    from_candidate_id=candidate_id,
                    to_candidate_id=_source_ref_endpoint(
                        "Entity",
                        f"code_evidence:{evidence_id}",
                    ),
                    confidence=1.0,
                    rule_id=(
                        f"derives_from/code_traceability_evidence@{WORKER_VERSION}"
                    ),
                )
            )
        for overlap_target_id in overlap_target_ids:
            if overlap_target_id == target_id:
                raise ValueError("code_traceability_overlap_self_reference")
            first_id, second_id = sorted((target_id, overlap_target_id))
            result.edges.append(
                EmittedEdge(
                    candidate_id=(
                        f"implementation_overlap_{_ref_token(first_id)}_"
                        f"{_ref_token(second_id)}"
                    ),
                    edge_type="overlaps",
                    from_candidate_id=(
                        candidate_id
                        if first_id == target_id
                        else _source_ref_endpoint(
                            "Entity",
                            f"implementation_target:{first_id}",
                        )
                    ),
                    to_candidate_id=(
                        candidate_id
                        if second_id == target_id
                        else _source_ref_endpoint(
                            "Entity",
                            f"implementation_target:{second_id}",
                        )
                    ),
                    confidence=1.0,
                    rule_id=f"overlaps/code_traceability_current@{WORKER_VERSION}",
                )
            )
        graph_layer, maturity_status = _layer_attrs_for_artifact(
            "implementation_target",
            lifecycle_status,
        )
        _apply_layer_to_result(
            result,
            graph_layer=graph_layer,
            maturity_status=maturity_status,
        )
        result.content_hash = _sha256(raw_content)
        return result

    # ------------------------------------------------------------------
    # Polymorphic dispatch
    # ------------------------------------------------------------------

    def process_artifact(
        self,
        artifact_type: str,
        artifact: dict[str, Any],
    ) -> WorkerResult:
        """Route to the right extractor by artifact_type.

        Public API for the ConsolidationQueue worker — keeps all dispatch
        in one place so queue code stays a thin wrapper.

        Pre-spec artifacts are materialised as Entity nodes so deterministic
        import can preserve lineage before cognitive consolidation.
        """
        if artifact_type == "story":
            return self.process_story(artifact)
        if artifact_type == "ideation":
            return self.process_ideation(artifact)
        if artifact_type == "refinement":
            return self.process_refinement(artifact)
        if artifact_type == "spec":
            return self.process_spec(artifact)
        if artifact_type == "sprint":
            return self.process_sprint(artifact)
        if artifact_type == "card":
            return self.process_card(artifact)
        if artifact_type == "amendment_hotfix_revision":
            return self.process_amendment(artifact)
        if artifact_type == "code_investigation_receipt":
            return self.process_code_investigation_receipt(artifact)
        if artifact_type == "code_evidence":
            return self.process_code_evidence(artifact)
        if artifact_type == "implementation_target":
            return self.process_implementation_target(artifact)
        raise ValueError(f"unknown artifact_type: {artifact_type}")


def _canonical_slug(name: str) -> str:
    """Render a tech canonical name into a safe candidate_id slug."""
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
