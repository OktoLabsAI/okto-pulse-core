"""Compose current relational classifications into a replaceable KG edge set."""
import hashlib
import json

from okto_pulse.core.application.processors.deterministic_kg import (
    EmittedNode, EmittedEdge, _architecture_interface_content,
)
from okto_pulse.core.domain.architecture_projection import resolve_architecture_associations
from okto_pulse.core.ports.structured_spec import get_structured_spec_store
from okto_pulse.core.services.architecture_candidates import load_spec_architecture_candidates
from okto_pulse.core.application.processors.deterministic_kg import (
    RelationalProjectionActiveSetIntent, RelationalProjectionActiveEdgeRef,
)

NAMESPACE = "architecture_associations"
RULE = "implements/architecture_association@v1"


async def prepare_architecture_association_projection(context, *, board_id, spec_id, result):
    store = get_structured_spec_store()
    spec = await store.get(context, spec_id=spec_id)
    if spec is None or spec.board_id != board_id:
        raise ValueError("architecture_projection_scope_invalid")
    population = await load_spec_architecture_candidates(context, board_id=board_id, spec_id=spec_id)
    decisions = await store.list_architecture_decisions(context, spec_id=spec_id, spec_edition=spec.edition)
    associations = resolve_architecture_associations(
        board_id=board_id, spec_id=spec_id, spec_edition=spec.edition,
        spec_version=spec.version, population=population, decisions=decisions,
        integration_requirements=tuple(spec.integration_requirements or ()),
    )
    nodes = {node.source_artifact_ref: node for node in result.nodes}
    pairs = set()
    for association in associations:
        source_ref = f"architecture_design:{association.design_id}:interface:{association.interface_id}"
        target_ref = f"spec:{spec_id}:integration_requirement:{association.integration_requirement_id}"
        target = nodes.get(target_ref)
        if target is None or target.node_type != "Requirement":
            raise ValueError("architecture_projection_target_missing")
        source = nodes.get(source_ref)
        if source is None:
            contract = json.loads(association.contract_json)
            source = EmittedNode(
                candidate_id="architecture_interface_" + hashlib.sha256(source_ref.encode()).hexdigest(),
                node_type="APIContract", title=str(contract.get("name") or association.interface_id)[:120],
                content=_architecture_interface_content(contract), source_artifact_ref=source_ref,
                source_confidence=1.0,
            )
            result.nodes.append(source)
            nodes[source_ref] = source
        if source.node_type != "APIContract":
            raise ValueError("architecture_projection_source_invalid")
        pair = (source.candidate_id, target.candidate_id)
        if pair in pairs:
            continue
        pairs.add(pair)
        result.edges.append(EmittedEdge(
            candidate_id="architecture_association_" + hashlib.sha256(repr(pair).encode()).hexdigest(),
            edge_type="implements", from_candidate_id=pair[0], to_candidate_id=pair[1],
            confidence=1.0, rule_id=RULE,
        ))
    result.relational_projection_active_set_intents += (
        RelationalProjectionActiveSetIntent(
            owner_type="spec", owner_id=spec_id, namespace=NAMESPACE, active_refs=(),
            active_edges=tuple(RelationalProjectionActiveEdgeRef(
                candidate_id=edge.candidate_id, edge_type=edge.edge_type,
                from_candidate_id=edge.from_candidate_id, to_candidate_id=edge.to_candidate_id,
                rule_id=edge.rule_id,
            ) for edge in result.edges if edge.rule_id == RULE),
        ),
    )
    return result
