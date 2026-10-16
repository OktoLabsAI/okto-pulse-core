"""Authoritative closeout diagnostics over the edition's public read port."""
from collections import Counter
from dataclasses import dataclass
import logging
import json

from okto_pulse.core.domain.missing_link_gate import (
    SPEC_LINK_FIELDS, MissingLinkSourceUnavailable, SemanticLinkFinding,
    missing_link_mode, spec_semantic_link_findings,
)
from okto_pulse.core.ports.application_persistence import (
    ApplicationFilter, ApplicationQuery, get_application_persistence_port,
)
from okto_pulse.core.services.gate_contracts import GateContractError


@dataclass(frozen=True)
class MissingLinkEvaluation:
    mode: str
    status: str
    findings: tuple[SemanticLinkFinding, ...] = ()

    @property
    def blocked(self) -> bool:
        return self.mode == "blocking" and (self.status != "available" or bool(self.findings))

    def to_payload(self) -> dict:
        items = []
        for finding in self.findings[:20]:
            item = finding.to_payload()
            # Preserve exact identities; omit complete entries, never truncate an ID.
            if len(json.dumps([*items, item], ensure_ascii=False).encode('utf-8')) > 3000:
                break
            items.append(item)
        return {"mode": self.mode, "status": self.status, "authority": "relational_source",
                "would_block_done": self.blocked,
                "finding_count": len(self.findings) if self.status == "available" else None,
                "findings": items,
                "truncated": len(self.findings) > len(items)}


def _refs(value) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, (list, tuple)) or any(type(v) is not str or not v for v in value):
        raise MissingLinkSourceUnavailable("missing_link_source_unavailable:reference_shape")
    return tuple(sorted(set(value)))


async def evaluate_missing_links(context, *, subject, entity_type: str, settings) -> MissingLinkEvaluation:
    """A fresh evaluation; neither graph state nor persisted findings are inputs.

    Cross-Spec scenarios are resolved in the same Board. Existing Path B gates
    retain responsibility for lineage and proof; this check does not prohibit a
    legitimate cross-Spec declaration or turn mere existence into coverage.
    """
    mode = missing_link_mode(settings)
    findings = []
    try:
        port = get_application_persistence_port()
        if entity_type not in {"spec", "card"}:
            raise MissingLinkSourceUnavailable("missing_link_source_unavailable:subject_type")
        current = await port.get(context, entity=entity_type, record_id=subject.id)
        if current is None or current.board_id != subject.board_id:
            raise MissingLinkSourceUnavailable("missing_link_source_unavailable:subject")
        card_refs = []
        if entity_type == "spec":
            fields = {name for origin, _, targets in SPEC_LINK_FIELDS for name in (origin, *targets)}
            collections = {}
            for field in sorted(fields):
                if not hasattr(current, field):
                    raise MissingLinkSourceUnavailable("missing_link_source_unavailable:" + field)
                value = getattr(current, field)
                collections[field] = [] if value is None else value
            findings.extend(spec_semantic_link_findings(spec_id=current.id, collections=collections))
            for collection, rows in collections.items():
                for row in rows:
                    if row.get("status") in {"revoked", "superseded", "retired", "cancelled"}:
                        continue
                    for ref in _refs(row.get("linked_task_ids")):
                        card_refs.append((f"spec:{current.id}:{collection}:{row['id']}", "linked_task_ids", ref))
        else:
            source = f"card:{current.id}"
            parent_id = getattr(current, "spec_id", None)
            if parent_id:
                parent = await port.get(context, entity="spec", record_id=parent_id)
                if parent is None or parent.board_id != current.board_id:
                    findings.append(SemanticLinkFinding(source, "spec_id", parent_id,
                        "target_absent", "update_card"))
            scenarios = _refs(current.test_scenario_ids)
            if scenarios:
                specs = await port.list(context, ApplicationQuery(entity="spec",
                    filters=(ApplicationFilter("board_id", "eq", current.board_id),),
                    select_fields=("id", "board_id", "test_scenarios")))
                counts = Counter()
                for spec in specs:
                    if spec.board_id != current.board_id or not hasattr(spec, "test_scenarios"):
                        raise MissingLinkSourceUnavailable("missing_link_source_unavailable:scenario_scope")
                    for scenario in spec.test_scenarios or []:
                        if not isinstance(scenario, dict) or not scenario.get("id"):
                            raise MissingLinkSourceUnavailable("missing_link_source_unavailable:scenario")
                        counts[scenario["id"]] += 1
                for ref in scenarios:
                    if counts[ref] != 1:
                        findings.append(SemanticLinkFinding(source, "test_scenario_ids", ref,
                            "target_absent" if counts[ref] == 0 else "target_ambiguous", "update_card"))
            for ref in _refs(getattr(current, "linked_test_task_ids", None)):
                card_refs.append((source, "linked_test_task_ids", ref))
            origin = getattr(current, "origin_task_id", None)
            if origin:
                card_refs.append((source, "origin_task_id", origin))
        if card_refs:
            cards = await port.list(context, ApplicationQuery(entity="card", filters=(
                ApplicationFilter("board_id", "eq", current.board_id),
                ApplicationFilter("id", "in", tuple(sorted({ref for _, _, ref in card_refs})))),
                select_fields=("id", "board_id")))
            valid = {card.id for card in cards if card.board_id == current.board_id}
            for source, field, ref in card_refs:
                if ref not in valid:
                    findings.append(SemanticLinkFinding(source, field, ref, "target_absent",
                        "update_card" if entity_type == "card" else "update_spec_entity"))
        return MissingLinkEvaluation(mode, "available", tuple(findings))
    except Exception:
        logging.getLogger(__name__).exception("missing_link_source_unavailable subject=%s:%s", entity_type, subject.id)
        return MissingLinkEvaluation(mode, "unavailable")


def require_missing_links_closed(evaluation: MissingLinkEvaluation, *, entity_type: str, subject) -> None:
    if not evaluation.blocked:
        return
    unavailable = evaluation.status != "available"
    raise GateContractError(
        code="missing_link_source_unavailable" if unavailable else "missing_links_open",
        message=("Authoritative references could not be checked." if unavailable
                 else "Current declared references must be corrected before completion."),
        gate_type="missing_link_gate", entity_type=entity_type, entity_id=subject.id,
        current_status=str(getattr(subject.status, "value", subject.status)),
        blocked_transition="done", enforcement_mode=evaluation.mode, enforcement_active=True,
        # Transition authority does not imply read access to every Spec section.
        # Exact child/target IDs are exposed by the separately authorized context.
        would_block_done=True, extra_details={
            "nature": "source_unavailable" if unavailable else "semantic_reference",
            "diagnostic_context": "missing_link_context",
        },
        next_action={"operation": "retry_authoritative_read" if unavailable else "correct_domain_reference",
                     "context_tool": "okto_pulse_get_task_context" if entity_type == "card" else "okto_pulse_get_spec_context",
                     "entity_id": subject.id},
    )
