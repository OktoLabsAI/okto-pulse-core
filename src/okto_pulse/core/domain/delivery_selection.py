"""A report selects immutable records; it cannot hide current invalidators."""

from collections.abc import Mapping

from okto_pulse.core.domain.delivery_evidence import CardDeliveryScope
from okto_pulse.core.domain.delivery_inventory import delivery_digest
from okto_pulse.core.models.delivery_selection import DeliverySelectionManifest


def delivery_scope_digest(obligations) -> str:
    return delivery_digest(sorted(
        (row.binding.obligation_ref, row.binding.semantic_sha256) for row in obligations
    ))


def seal_delivery_selection(*, scope: CardDeliveryScope, card_version: int,
                            revision: int, records: list[dict], obligations,
                            impact: dict | None, impact_basis: list[dict] | None = None) -> dict:
    payload = dict(contract_version="card-delivery-selection/v1",
                   board_id=scope.board_id, card_id=scope.card_id, spec_id=scope.spec_id,
                   spec_edition=scope.spec_edition, card_version=card_version,
                   delivery_revision=revision, records=records,
                   scope_sha256=delivery_scope_digest(obligations),
                   impact_sha256=delivery_digest(impact))
    if impact_basis is not None:
        payload.update(contract_version="card-delivery-selection/v2", impact_basis=impact_basis)
    return DeliverySelectionManifest(**payload, sha256=delivery_digest(payload)).model_dump(mode="json")


def current_delivery_report(card: object) -> Mapping | None:
    status = getattr(card, "status", None)
    if getattr(status, "value", status) not in {"validation", "rejected", "done"}:
        return None
    reports = [row for row in (getattr(card, "conclusions", None) or [])
               if isinstance(row, Mapping) and row.get("source") in {"move_to_validation", "move_to_done"}]
    return reports[-1] if reports else None


def report_reuses_impact(report: Mapping) -> bool:
    manifest = report.get("delivery_manifest")
    return isinstance(manifest, Mapping) and (
        manifest.get("contract_version") != "card-delivery-selection/v1" or "impact_basis" in manifest
    )


def submitted_report_receipt(card: object, scope: CardDeliveryScope, record_ids: set[str], target_status: str) -> dict:
    """Locate the immutable report that atomically included this batch.

    The first matching report is the original submission, even after authorized
    rework. Do not return the Card's current status as the historical outcome.
    """
    for report in getattr(card, "conclusions", None) or ():
        if not isinstance(report, Mapping) or report.get("source") != "move_to_" + target_status:
            continue
        raw = report.get("delivery_manifest")
        if not isinstance(raw, Mapping):
            continue
        manifest = DeliverySelectionManifest.model_validate(raw)
        if not record_ids <= {row.id for row in manifest.records}:
            continue
        if ((manifest.board_id, manifest.card_id, manifest.spec_id, manifest.spec_edition)
            != (scope.board_id, scope.card_id, scope.spec_id, scope.spec_edition)
            or delivery_digest(manifest.model_dump(mode="json", exclude={"sha256"})) != manifest.sha256
            or delivery_digest(report.get("impact_evidence")) != manifest.impact_sha256):
            raise ValueError("delivery_report_receipt_invalid")
        return dict(status=target_status, manifest_sha256=manifest.sha256, delivery_revision=manifest.delivery_revision)
    raise ValueError("delivery_report_receipt_unavailable")


def current_delivery_selection(card: object, scope: CardDeliveryScope, *, obligations,
                               record_hashes: dict[str, str],
                               prospective_report: Mapping | None = None) -> set[str] | None:
    """Only a frozen executor report controls delivery selection.

    Rework reads the accumulated ledger again. Revocation, source/Target heads,
    material progress and latest test results must still be read independently
    of the returned ID set. Card version is recorded for provenance, not compared
    to a later version created by an authorized lifecycle transition.

    Completion may supply the server-built report before it is appended. Its
    manifest receives exactly the same integrity checks, without temporarily
    mutating status or historical conclusions to evaluate the proposed report.
    """
    report = prospective_report if prospective_report is not None else current_delivery_report(card)
    if report is None or "delivery_manifest" not in report:
        return None
    try:
        manifest = DeliverySelectionManifest.model_validate(report["delivery_manifest"])
        data = manifest.model_dump(mode="json", exclude={"sha256"})
        valid = (
            manifest.sha256 == delivery_digest(data)
            and (manifest.board_id, manifest.card_id, manifest.spec_id, manifest.spec_edition)
            == (scope.board_id, scope.card_id, scope.spec_id, scope.spec_edition)
            and manifest.scope_sha256 == delivery_scope_digest(obligations)
            and manifest.impact_sha256 == delivery_digest(report.get("impact_evidence"))
            and all(record_hashes.get(row.id) == row.sha256 for row in manifest.records)
        )
    except (TypeError, ValueError):
        valid = False
    if not valid:
        raise ValueError("delivery_selection_manifest_invalid")
    return {row.id for row in manifest.records}
