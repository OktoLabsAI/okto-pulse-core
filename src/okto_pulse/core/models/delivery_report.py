"""Optional atomic batch + existing Card report; no new handoff entity."""
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from okto_pulse.core.models.delivery_evidence import (
    CardDeliveryEvidenceBatchInput, CardDeliveryEvidenceBatchCommand,
    CardDeliveryEvidenceWriteInput, DeliveryEvidenceQuery, Identity,
)
from okto_pulse.core.models.schemas import CardMove


class DeliveryReportMove(CardMove):
    model_config = ConfigDict(extra="forbid")


class CardDeliveryReportInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    contract_version: Literal["card-delivery-report/v1"]
    expected_card_status: Literal["started", "in_progress"]
    batch: CardDeliveryEvidenceBatchInput
    report: DeliveryReportMove
    existing_record_ids: list[Identity] = Field(default_factory=list, max_length=200)
    reuse_impact: bool = False

    @property
    def entries(self):
        return self.batch.entries

    @model_validator(mode="after")
    def report_shape(self):
        if self.report.status not in {"validation", "done"} or self.report.delivery_selection is not None:
            raise ValueError("delivery_report_target_or_selection_invalid")
        if len(set(self.existing_record_ids)) != len(self.existing_record_ids):
            raise ValueError("delivery_selection_duplicate_record")
        if len(self.existing_record_ids) + len(self.entries) > 200 or len(self.model_dump_json().encode("utf-8")) > 128 * 1024:
            raise ValueError("delivery_report_payload_limit")
        return self


class CardDeliveryReportCommand(DeliveryEvidenceQuery, CardDeliveryReportInput):
    card_id: Identity

    def batch_command(self):
        return CardDeliveryEvidenceBatchCommand(board_id=self.board_id, card_id=self.card_id,
            spec_id=self.spec_id, **self.batch.model_dump())


CardDeliveryRecordInput = CardDeliveryEvidenceWriteInput | CardDeliveryReportInput


class DeliveryReportRejected(ValueError):
    """Keep the original report gate actionable in the canonical transport."""
    def __init__(self, error: ValueError):
        super().__init__(str(error))
        self.code = getattr(error, "code", None) or "delivery_report_rejected"
        serialize = getattr(error, "to_dict", None)
        self.gate = serialize() if callable(serialize) else getattr(error, "details", {})

    def to_error_dict(self):
        return dict(code=self.code, message=str(self), details={"report_gate": self.gate},
                    remediation=[dict(action="resolve_card_report_gate", tool="okto_pulse_get_card")])
