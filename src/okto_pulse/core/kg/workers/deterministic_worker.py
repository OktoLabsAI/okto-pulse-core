"""Layer 1 Deterministic Worker — KG Pipeline v2 (spec c48a5c33).

Reads structured fields from the pulse.db artifact (Spec/Sprint/Card) and
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

The worker is pure — it never touches Kùzu directly. The caller hands the
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
from pathlib import Path
from typing import Any

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
    """One node scheduled for Kùzu insertion by the Layer 1 worker."""

    candidate_id: str
    node_type: str  # matches schema.NODE_TYPES
    title: str
    content: str
    source_artifact_ref: str
    source_confidence: float = 1.0
    context: str = ""
    # v0.3.1: additive score boost resolved from the source card's priority.
    # Non-zero only on the root node emitted from a Card — belongs_to child
    # nodes (FR/TR/AC per Spec) stay at 0.0. Cap +0.2 (CRITICAL).
    priority_boost: float = 0.0


@dataclass
class EmittedEdge:
    """One edge scheduled for Kùzu insertion, carrying full v0.2.0 metadata."""

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
    """An edge the worker REFUSED to emit because a linked_* field was empty.

    The cognitive agent consumes these to propose a fallback edge with
    capped confidence (BR `Cognitive Fallback Confidence Cap`, ≤0.85).
    """

    edge_type: str
    from_candidate_id: str
    from_candidate_title: str  # denormalised for LLM prompt efficiency
    reason: str  # machine-readable, matches BR wording
    suggested_candidates: list[str] = field(default_factory=list)
    artifact_ref: str = ""


@dataclass
class WorkerResult:
    nodes: list[EmittedNode] = field(default_factory=list)
    edges: list[EmittedEdge] = field(default_factory=list)
    missing_link_candidates: list[MissingLinkCandidate] = field(default_factory=list)
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


_whitelist_cache: tuple[list[TechEntity], int] | None = None


def _load_tech_whitelist() -> tuple[list[TechEntity], int]:
    """Parse tech_entities.yml into TechEntity dataclasses.

    Uses PyYAML when available; falls back to a hand-rolled parser covering
    the narrow shape we emit. Cached for the life of the process — call
    reset_tech_whitelist_cache() in tests to force reload.
    """
    global _whitelist_cache
    if _whitelist_cache is not None:
        return _whitelist_cache

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
    _whitelist_cache = (entities, version)
    return _whitelist_cache


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
    global _whitelist_cache
    _whitelist_cache = None


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


# =====================================================================
# Core extractor
# =====================================================================


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


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
        prefix = f"spec_{spec_id[:8]}"
        artifact_ref = f"spec:{spec_id}"
        result = WorkerResult(raw_content="")
        raw_parts: list[str] = [
            spec.get("title") or "",
            spec.get("description") or "",
            spec.get("context") or "",
        ]

        # 1. Spec entity (anchor node) — used by hierarchy edges on caller.
        spec_entity_id = f"{prefix}_entity"
        result.nodes.append(EmittedNode(
            candidate_id=spec_entity_id,
            node_type="Entity",
            title=spec.get("title") or f"Spec {spec_id}",
            content=spec.get("description") or "",
            context=spec.get("context") or "",
            source_artifact_ref=artifact_ref,
            source_confidence=1.0,
        ))

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
        for i, req in enumerate(spec.get("functional_requirements") or []):
            text = req if isinstance(req, str) else (
                req.get("text") or req.get("description") or json.dumps(req)
            )
            raw_parts.append(text)
            cid = f"{prefix}_fr_{i}"
            fr_ids.append((cid, text))
            result.nodes.append(EmittedNode(
                candidate_id=cid,
                node_type="Requirement",
                title=text[:120],
                content=text,
                source_artifact_ref=artifact_ref,
                source_confidence=1.0,
            ))
            _add_belongs_to(cid, "fr", i)

        # 3. Technical requirements → Constraint
        tr_ids: list[tuple[str, str]] = []  # (candidate_id, text)
        for i, req in enumerate(spec.get("technical_requirements") or []):
            if isinstance(req, dict):
                text = req.get("text") or req.get("description") or json.dumps(req)
            else:
                text = str(req)
            raw_parts.append(text)
            cid = f"{prefix}_tr_{i}"
            tr_ids.append((cid, text))
            result.nodes.append(EmittedNode(
                candidate_id=cid,
                node_type="Constraint",
                title=text[:120],
                content=text,
                source_artifact_ref=artifact_ref,
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
                source_artifact_ref=artifact_ref,
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
                source_artifact_ref=artifact_ref,
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
                source_artifact_ref=artifact_ref,
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

        # 7. APIContract + `implements` edges to Requirement via linked_requirements.
        fr_text_to_cid = {text.strip(): cid for cid, text in fr_ids}
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
                source_artifact_ref=artifact_ref,
                source_confidence=1.0,
            ))
            _add_belongs_to(api_cid, "api", i)
            linked = api.get("linked_requirements") or []
            if not linked:
                result.missing_link_candidates.append(MissingLinkCandidate(
                    edge_type="implements",
                    from_candidate_id=api_cid,
                    from_candidate_title=title,
                    reason="no_requirement_match",
                    suggested_candidates=[c for c, _ in fr_ids],
                    artifact_ref=artifact_ref,
                ))
                continue
            for idx, link in enumerate(linked):
                if not isinstance(link, str):
                    continue
                target = fr_text_to_cid.get(link.strip())
                if target is None:
                    result.missing_link_candidates.append(MissingLinkCandidate(
                        edge_type="implements",
                        from_candidate_id=api_cid,
                        from_candidate_title=title,
                        reason="no_requirement_match",
                        suggested_candidates=[c for c, _ in fr_ids],
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

        # 8. Decisions from "## Decisions" section → Decision nodes + derives_from
        #    edges to EVERY FR (co-occurrence heuristic, confidence 0.6).
        decisions_text = _extract_decisions_from_context(spec.get("context") or "")
        tech_whitelist_version = _load_tech_whitelist()[1]
        for i, dec_text in enumerate(decisions_text):
            raw_parts.append(dec_text)
            dec_cid = f"{prefix}_dec_{i}"
            result.nodes.append(EmittedNode(
                candidate_id=dec_cid,
                node_type="Decision",
                title=dec_text[:120],
                content=dec_text,
                source_artifact_ref=artifact_ref,
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
    # Sprint entry point — lighter artifact, only Entity + outcome Criterion
    # ------------------------------------------------------------------

    def process_sprint(self, sprint: dict[str, Any]) -> WorkerResult:
        sid = sprint["id"]
        prefix = f"sprint_{sid[:8]}"
        artifact_ref = f"sprint:{sid}"
        result = WorkerResult()
        raw_parts = [sprint.get("title") or "",
                     sprint.get("description") or "",
                     sprint.get("objective") or ""]

        sprint_cid = f"{prefix}_entity"
        result.nodes.append(EmittedNode(
            candidate_id=sprint_cid,
            node_type="Entity",
            title=sprint.get("title") or f"Sprint {sid}",
            content=sprint.get("description") or "",
            context=sprint.get("objective") or "",
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

        raw = "\n---\n".join(p for p in raw_parts if p)
        result.raw_content = raw
        result.content_hash = _sha256(raw)
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
        from okto_pulse.core.kg.scoring import _resolve_priority_boost

        cid = card["id"]
        prefix = f"card_{cid[:8]}"
        artifact_ref = f"card:{cid}"
        card_type = card.get("card_type") or "normal"
        result = WorkerResult()
        raw_parts = [card.get("title") or "", card.get("description") or ""]

        if card_type == "bug":
            node_type = "Bug"
        else:
            node_type = "Entity"

        # v0.3.1: resolve priority_boost from card.priority — only the root
        # node of the card carries the boost. Hierarchy/belongs_to nodes
        # (sprint/spec parents) stay at 0.0 per BR "Boost não herda".
        boost = _resolve_priority_boost(card.get("priority"))
        card_cid = f"{prefix}_entity"
        result.nodes.append(EmittedNode(
            candidate_id=card_cid,
            node_type=node_type,
            title=card.get("title") or f"Card {cid}",
            content=card.get("description") or "",
            source_artifact_ref=artifact_ref,
            source_confidence=1.0,
            priority_boost=boost,
        ))

        if card_type == "bug":
            # `violates` needs origin_task_id → linked BR/TR → Constraint; we
            # don't have that lookup here, so defer to fallback.
            origin = card.get("origin_task_id")
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

        raw = "\n---\n".join(p for p in raw_parts if p)
        result.raw_content = raw
        result.content_hash = _sha256(raw)
        logger.info(
            "deterministic_worker.card_processed card=%s type=%s nodes=%d missing=%d",
            cid, card_type, len(result.nodes), len(result.missing_link_candidates),
            extra={"event": "deterministic_worker.card_processed",
                   "card_id": cid, "card_type": card_type,
                   "content_hash": result.content_hash,
                   "worker_version": WORKER_VERSION},
        )
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
        """
        if artifact_type == "spec":
            return self.process_spec(artifact)
        if artifact_type == "sprint":
            return self.process_sprint(artifact)
        if artifact_type == "card":
            return self.process_card(artifact)
        raise ValueError(f"unknown artifact_type: {artifact_type}")


def _canonical_slug(name: str) -> str:
    """Render a tech canonical name into a safe candidate_id slug."""
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
