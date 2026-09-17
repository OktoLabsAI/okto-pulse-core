"""Provider-neutral inventory and the single delivery readiness gate."""

import hashlib
import json

from okto_pulse.core.domain.delivery_evidence import (
    CardDeliveryScope,
    DeliveryBinding,
    DeliveryObligation,
    DeliveryScope,
    evaluate_delivery_coverage,
)
from okto_pulse.core.ports.relational_application import (
    require_relational_application_adapter,
)

COLLECTIONS = (
    ("fr", "functional_requirements"),
    ("tr", "technical_requirements"),
    ("br", "business_rules"),
    ("ac", "acceptance_criteria"),
    ("api", "api_contracts"),
    ("ir", "integration_requirements"),
    ("or", "observability_requirements"),
    ("decision", "decisions"),
)
# Operational links and state do not change the obligation being implemented.
_OPERATIONAL = {"linked_task_ids", "status", "created_at", "updated_at"}


def delivery_digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
    ).hexdigest()


def _collection_obligations(
    prefix: str,
    values: list,
    *,
    linked_card_id: str | None = None,
) -> list[DeliveryObligation]:
    """Obligations of one spec collection, optionally filtered by card links.

    Digests are identical between the spec and card inventories by
    construction: both paths run the same semantic extraction, so the
    migration verdict-equivalence gate compares like with like. String items
    carry no ``linked_task_ids`` and are therefore spec-inventory only.
    """
    result: list[DeliveryObligation] = []
    for index, value in enumerate(values):
        if isinstance(value, dict):
            if value.get("status") in {
                "cancelled",
                "superseded",
                "deprecated",
                "revoked",
            }:
                continue
            if linked_card_id is not None and linked_card_id not in (
                value.get("linked_task_ids") or []
            ):
                continue
            identity = str(value.get("id") or f"index-{index}")
            semantic = {k: v for k, v in value.items() if k not in _OPERATIONAL}
            title = str(
                value.get("title")
                or value.get("text")
                or value.get("description")
                or value.get("rule")
                or identity
            )
        elif isinstance(value, str) and value.strip():
            if linked_card_id is not None:
                continue
            identity, semantic, title = f"index-{index}", value, value
        else:
            raise ValueError("delivery_obligation_inventory_invalid")
        result.append(
            DeliveryObligation(
                DeliveryBinding(f"{prefix}:{identity}", delivery_digest(semantic)),
                title,
            )
        )
    return result


def delivery_inventory(spec: object) -> tuple[DeliveryObligation, ...]:
    result = []
    for prefix, collection in COLLECTIONS:
        values = getattr(spec, collection, None) or []
        if not isinstance(values, list):
            raise ValueError("delivery_obligation_inventory_invalid")
        result.extend(_collection_obligations(prefix, values))
    # A spec without structured obligations still needs an explicit scope decision;
    # it is never silently counted as 100% delivered.
    if not result:
        semantic = {
            name: getattr(spec, name, None)
            for name in ("title", "description", "context")
        }
        result.append(
            DeliveryObligation(
                DeliveryBinding(f"spec:{spec.id}", delivery_digest(semantic)),
                str(spec.title),
            )
        )
    if len({item.binding.obligation_ref for item in result}) != len(result):
        raise ValueError("delivery_obligations_ambiguous")
    return tuple(result)


def card_delivery_inventory(spec: object, card: object) -> tuple[DeliveryObligation, ...]:
    """Obligations of one card, derived from its links in the spec collections.

    The join key is ``linked_task_ids`` on each structured entity (the same
    refs ``delivery_inventory`` produces), so a card sees exactly the
    obligations it is linked to. A card without links receives exactly the
    fallback obligation ``card:<card_id>`` — coverage is never vacuously
    satisfied (BR: nunca vacuamente satisfeito).
    """
    card_id = str(card.id)
    result: list[DeliveryObligation] = []
    for prefix, collection in COLLECTIONS:
        values = getattr(spec, collection, None) or []
        if not isinstance(values, list):
            raise ValueError("delivery_obligation_inventory_invalid")
        result.extend(_collection_obligations(prefix, values, linked_card_id=card_id))
    if not result:
        semantic = {"title": str(card.title)}
        result.append(
            DeliveryObligation(
                DeliveryBinding(f"card:{card_id}", delivery_digest(semantic)),
                str(card.title),
            )
        )
    if len({item.binding.obligation_ref for item in result}) != len(result):
        raise ValueError("delivery_obligations_ambiguous")
    return tuple(result)


def delivery_store(session: object):
    factory = getattr(
        require_relational_application_adapter(), "delivery_evidence", None
    )
    if factory is None:
        raise ValueError("delivery_evidence_adapter_unavailable")
    return factory(session)


def card_delivery_store(session: object):
    """Resolve the card-scoped seam of the delivery adapter, failing closed."""
    store = delivery_store(session)
    if not hasattr(store, "load_card_snapshot") or not hasattr(
        store, "record_card"
    ):
        raise ValueError("card_delivery_evidence_adapter_unavailable")
    return store


async def require_spec_delivery(
    session: object,
    spec: object,
    *,
    for_update: bool = False,
    board: object | None = None,
) -> None:
    """Spec→done delivery gate (FR-4).

    The gate consumes the card-ledger ROLLUP when the adapter exposes it —
    the spec-level projection becomes a derivation, not a recording surface.
    The same board setting (delivery_evidence_gate) governs this seam: in
    ``advisory`` the aggregated verdict is surfaced through the rollup read
    without blocking the transition.
    """
    if resolve_delivery_gate_mode(board) != "blocking":
        return
    if getattr(spec, "skip_delivery_evidence", False):
        # Spec-level override following the Tests-tab skip-flag pattern:
        # the transition is allowed while the rollup projection keeps
        # showing the truthful coverage verdict. The evaluator is untouched.
        return
    store = delivery_store(session)
    scope = DeliveryScope(spec.board_id, spec.id, int(spec.edition))
    if for_update:
        await store.lock_scope(scope)
    load_rollup = getattr(store, "load_rollup_snapshot", None)
    if load_rollup is not None:
        snapshot = (await load_rollup(spec.board_id, spec.id))[0]
    else:
        snapshot = await store.load_snapshot(scope)
    result = evaluate_delivery_coverage(snapshot)
    if not result.allowed:
        missing = [
            row.obligation.binding.obligation_ref
            for row in result.rows
            if not row.implementation_satisfied or not row.test_satisfied
        ]
        raise ValueError(
            "delivery_evidence_incomplete: "
            + ", ".join(result.blockers)
            + "; obligations="
            + ", ".join(missing[:20])
        )


def resolve_delivery_gate_mode(board: object | None) -> str:
    """Resolve the board's delivery-evidence gate mode.

    Default (and any persisted out-of-enum value) resolves to ``blocking``:
    the 0.3.3 spec-side gate was unconditional, so legacy boards keep their
    existing protection level (BR-8 — default blocking, fail-closed read).
    """
    settings = (getattr(board, "settings", None) or {}) if board is not None else {}
    if not isinstance(settings, dict):
        return "blocking"
    value = settings.get("delivery_evidence_gate")
    return value if value in {"advisory", "blocking"} else "blocking"


async def require_card_delivery(
    session: object,
    card: object,
    spec: object,
    *,
    board: object | None = None,
) -> None:
    """Card→done delivery gate (FR-3).

    In ``blocking`` mode, a normal/bug card cannot complete while any
    obligation derived from its links lacks accepted implementation proof.
    The task DoD covers the implementation phase only — the test-phase join
    (cross-card ``verified_implementation_ids``) lives at the spec rollup
    (BR-5), and test cards are never gated by this seam. ``advisory`` mode
    never raises: the verdict stays visible through the card snapshot.

    ``evaluate_delivery_coverage`` only credits implementation facts whose
    card is already DONE — a status the completing card cannot have while
    this gate runs (AC ac_c41b1fa3: record proof, then move). The gate
    therefore accepts chain-valid proof (``current_accepted_execution``)
    bound to the obligation; the evaluator itself stays pure and unchanged
    and re-runs with the final status at the spec rollup.
    """
    if resolve_delivery_gate_mode(board) != "blocking":
        return
    raw_type = getattr(card, "card_type", None)
    card_type = str(getattr(raw_type, "value", raw_type or "normal"))
    if card_type not in {"normal", "bug"}:
        return
    spec_id = getattr(card, "spec_id", None)
    if not spec_id or spec_id != getattr(spec, "id", None):
        return
    store = card_delivery_store(session)
    scope = CardDeliveryScope(
        card.board_id, card.id, spec_id, int(spec.edition)
    )
    snapshot = await store.load_card_snapshot(scope)
    if snapshot.complete is not True or not snapshot.obligations:
        raise ValueError(
            "delivery_evidence_incomplete: delivery_projection_incomplete"
        )
    evaluation = evaluate_delivery_coverage(snapshot)
    # The card DoD covers the implementation phase only — the evaluator's
    # test-phase blockers (delivery_test_result_missing) are rollup concerns
    # (BR-5). Evaluator-valid proof always satisfies; beyond that, chain-valid
    # proof (accepted committed execution) bound to the obligation satisfies
    # the DoD even before the DONE status lands (see docstring).
    missing = [
        row.obligation.binding.obligation_ref
        for row in evaluation.rows
        if not row.implementation_satisfied
        and not any(
            fact.current_accepted_execution
            and row.obligation.binding in fact.bindings
            for fact in snapshot.implementations
        )
    ]
    if missing:
        raise ValueError(
            "delivery_evidence_incomplete: obligations=" + ", ".join(missing[:20])
        )
