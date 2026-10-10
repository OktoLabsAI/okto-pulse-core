"""Pure resolution of current Decision observations and their material scope."""

from dataclasses import asdict, dataclass

from okto_pulse.core.domain.delivery_inventory import delivery_digest


@dataclass(frozen=True, slots=True)
class DecisionReviewFact:
    record_id: str
    decision_id: str
    scope_sha256: str
    result: str
    reconciles: tuple[str, ...] = ()
    revoked: bool = False
    authority_current: bool = True


@dataclass(frozen=True, slots=True)
class DecisionInspectionState:
    decision_id: str
    scope_sha256: str
    status: str
    record_ids: tuple[str, ...]


def resolve_decision_inspection(decision_id, scope_sha256, facts):
    current = [f for f in facts if f.decision_id == decision_id and f.scope_sha256 == scope_sha256]
    if len(facts) > 10000 or len({f.record_id for f in current}) != len(current):
        return DecisionInspectionState(decision_id, scope_sha256, "unavailable", ())
    # Reconciliation references are server-validated against the existing heads.
    # Arrival order alone can never make a failing observation disappear.
    replaced = {identity for f in current if not f.revoked and f.authority_current for identity in f.reconciles}
    heads = [f for f in current if f.record_id not in replaced]
    outcomes = {"revoked" if f.revoked else "unavailable" if not f.authority_current else f.result for f in heads}
    status = ("inspection_pending" if not outcomes else "conflict" if len(outcomes) > 1
              else "verified" if outcomes == {"passed"} else next(iter(outcomes)))
    return DecisionInspectionState(decision_id, scope_sha256, status, tuple(sorted(f.record_id for f in heads)))


def decision_inspection_basis(*, spec, snapshot, row, separation_mode):
    """Seal observed parts and their current proof, excluding reviews/comments.

    Sources are native, authorized relational projections. Their digests are
    supplied by the server; an arbitrary external URL/hash is not a source receipt.
    This authenticates the observation's base, not execution of the inspection.
    """
    plan = row.decision_plan
    if not plan or not plan.complete or not plan.verification.inspection:
        raise ValueError("decision_inspection_plan_required")
    context = snapshot.effective_context
    if context is None or not context.inventory.population_complete:
        raise ValueError("decision_inspection_scope_unavailable")
    sources = []
    for scope in plan.verification.inspection.scope_refs:
        full = scope.kind == "spec"
        selected = [item for item in context.inventory.rows if full or item.binding.obligation_ref == scope.id]
        if not selected and not full:
            raise ValueError("decision_inspection_scope_unavailable")
        refs = {item.binding.obligation_ref for item in selected}
        semantic = {
            "scope": asdict(snapshot.scope),
            "obligations": [{"binding": asdict(item.binding), "contributions": [asdict(c) for c in item.contributions],
                             "blockers": item.blockers} for item in sorted(selected, key=lambda i: i.binding.obligation_ref)],
            "implementations": [asdict(f) for f in sorted(snapshot.implementations, key=lambda i: i.id)
                                if any(b.obligation_ref in refs for b in f.bindings)],
            "tests": [asdict(f) for f in sorted(snapshot.tests, key=lambda i: i.id)
                      if any(b.obligation_ref in refs for b in f.bindings)],
        }
        if full:
            semantic["definition"] = {name: getattr(spec, name, None) for name in
                ("title", "description", "context", "project_structure", "architecture_adoption", "execution_contract")}
        digest = delivery_digest(semantic)
        sources.append({"reference": f"spec:{spec.id}" if full else scope.id,
                        "revision": f"edition:{spec.edition}", "sha256": digest})
    return {"scope_sha256": delivery_digest({"scope": asdict(snapshot.scope), "decision": asdict(row.binding),
                "sources": sources, "reviewer_separation_mode": separation_mode}),
            "sources": sources, "expected": plan.verification.inspection.condition}


def decision_authors(decision, histories):
    """Recover authors from complete native field diffs, never a client field."""
    authors = set()
    found = False
    for history in histories:
        for change in getattr(history, "changes", None) or ():
            if change.get("field") != "decisions":
                continue
            old = {d.get("id"): d for d in change.get("old") or () if isinstance(d, dict)}
            new = {d.get("id"): d for d in change.get("new") or () if isinstance(d, dict)}
            value = new.get(decision["id"])
            if value is not None and value != old.get(decision["id"]):
                actor_id = getattr(history, "actor_id", None)
                if not actor_id:
                    return (), False
                authors.add(actor_id)
                found = value == decision
    return tuple(sorted(authors)), found
