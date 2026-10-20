"""Typed JSON representation of graph compensation before-images.

The prepared SQLite journal must survive a process crash between graph writes
and native commit. Only these known record types are reconstructed; journal
contents never select an arbitrary Python class.
"""

from __future__ import annotations

import base64
import binascii
import math
from dataclasses import fields, is_dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any
from uuid import UUID

from okto_pulse.core.kg.interfaces.graph_transaction import (
    GraphNodePropertyBeforeImage,
    LearningBugAssociationReceipt,
    ProjectionActiveSetIntent,
    ProjectionActiveSetReceipt,
    ProjectionEdgeBeforeImage,
    ProjectionEdgeRef,
    ProjectionLogicalEdgeRef,
    ProjectionNodeBeforeImage,
    ProjectionNodeRef,
    ProjectionRemovalOnlyIntent,
    SpecLineageEdgeSnapshot,
    SpecLineageReconciliationReceipt,
)
from okto_pulse.core.kg.transaction import GraphWriteRecord


class ProjectionJournalRecordsError(ValueError):
    """The journal cannot faithfully represent compensation state."""


_RECORD_TYPES = {
    cls.__name__: cls
    for cls in (
        GraphWriteRecord,
        GraphNodePropertyBeforeImage,
        LearningBugAssociationReceipt,
        ProjectionActiveSetIntent,
        ProjectionActiveSetReceipt,
        ProjectionEdgeBeforeImage,
        ProjectionEdgeRef,
        ProjectionLogicalEdgeRef,
        ProjectionNodeBeforeImage,
        ProjectionNodeRef,
        ProjectionRemovalOnlyIntent,
        SpecLineageEdgeSnapshot,
        SpecLineageReconciliationReceipt,
    )
}


def _encode(value: Any, depth: int = 0) -> Any:
    if depth > 64:
        raise ProjectionJournalRecordsError("projection_record_depth_exceeded")
    if value is None or type(value) in (str, bool, int):
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise ProjectionJournalRecordsError("projection_record_nonfinite_float")
        return value
    if type(value) is datetime:
        return {"type": "datetime", "value": value.isoformat()}
    if type(value) is date:
        return {"type": "date", "value": value.isoformat()}
    if type(value) is time:
        return {"type": "time", "value": value.isoformat()}
    if type(value) is Decimal:
        if not value.is_finite():
            raise ProjectionJournalRecordsError("projection_record_nonfinite_decimal")
        return {"type": "decimal", "value": str(value)}
    if type(value) is UUID:
        return {"type": "uuid", "value": str(value)}
    if type(value) is bytes:
        return {"type": "bytes", "value": base64.b64encode(value).decode("ascii")}
    if type(value) is list:
        return {"type": "list", "items": [_encode(item, depth + 1) for item in value]}
    if type(value) is tuple:
        return {"type": "tuple", "items": [_encode(item, depth + 1) for item in value]}
    if type(value) is dict:
        return {"type": "dict", "items": [
            [_encode(key, depth + 1), _encode(item, depth + 1)]
            for key, item in value.items()
        ]}
    if type(value) in _RECORD_TYPES.values() and is_dataclass(value):
        return {"type": type(value).__name__, "fields": {
            field.name: _encode(getattr(value, field.name), depth + 1)
            for field in fields(value)
        }}
    raise ProjectionJournalRecordsError(
        f"projection_record_type_unsupported:{type(value).__name__}"
    )


def _decode(payload: Any, depth: int = 0) -> Any:
    if depth > 64:
        raise ProjectionJournalRecordsError("projection_record_depth_exceeded")
    if payload is None or type(payload) in (str, bool, int):
        return payload
    if type(payload) is float:
        if not math.isfinite(payload):
            raise ProjectionJournalRecordsError("projection_record_nonfinite_float")
        return payload
    if type(payload) is not dict or type(payload.get("type")) is not str:
        raise ProjectionJournalRecordsError("projection_record_payload_invalid")
    kind = payload["type"]
    if kind in {"datetime", "date", "time", "decimal", "uuid", "bytes"}:
        if set(payload) != {"type", "value"} or type(payload["value"]) is not str:
            raise ProjectionJournalRecordsError("projection_record_scalar_invalid")
        value = payload["value"]
        try:
            if kind == "datetime":
                return datetime.fromisoformat(value)
            if kind == "date":
                return date.fromisoformat(value)
            if kind == "time":
                return time.fromisoformat(value)
            if kind == "decimal":
                decimal_value = Decimal(value)
                if not decimal_value.is_finite():
                    raise ValueError("nonfinite decimal")
                return decimal_value
            if kind == "uuid":
                return UUID(value)
            return base64.b64decode(value, validate=True)
        except (ValueError, TypeError, binascii.Error) as exc:
            raise ProjectionJournalRecordsError("projection_record_scalar_invalid") from exc
    if kind in {"list", "tuple", "dict"}:
        if set(payload) != {"type", "items"} or type(payload["items"]) is not list:
            raise ProjectionJournalRecordsError("projection_record_collection_invalid")
        items = payload["items"]
        if kind == "dict":
            try:
                if any(type(pair) is not list or len(pair) != 2 for pair in items):
                    raise ProjectionJournalRecordsError("projection_record_mapping_invalid")
                return {
                    _decode(key, depth + 1): _decode(value, depth + 1)
                    for key, value in items
                }
            except TypeError as exc:
                raise ProjectionJournalRecordsError("projection_record_mapping_invalid") from exc
        decoded = [_decode(item, depth + 1) for item in items]
        return tuple(decoded) if kind == "tuple" else decoded
    cls = _RECORD_TYPES.get(kind)
    if cls is None or set(payload) != {"type", "fields"}:
        raise ProjectionJournalRecordsError("projection_record_class_invalid")
    data = payload["fields"]
    expected = {field.name for field in fields(cls)}
    if type(data) is not dict or set(data) != expected:
        raise ProjectionJournalRecordsError("projection_record_fields_invalid")
    try:
        return cls(**{
            name: _decode(value, depth + 1) for name, value in data.items()
        })
    except (TypeError, ValueError) as exc:
        raise ProjectionJournalRecordsError("projection_record_fields_invalid") from exc


def graph_write_records_to_payload(records: list[object]) -> list[dict[str, Any]]:
    """Serialize exact before-images before the prepared journal is committed."""

    if len(records) > 100_000 or any(type(item) is not GraphWriteRecord for item in records):
        raise ProjectionJournalRecordsError("projection_record_sequence_invalid")
    return [_encode(item) for item in records]


def graph_write_records_from_payload(payload: object) -> list[GraphWriteRecord]:
    """Decode a journal for absent-receipt compensation; fail closed on gaps."""

    if type(payload) is not list or len(payload) > 100_000:
        raise ProjectionJournalRecordsError("projection_record_sequence_invalid")
    records = [_decode(item) for item in payload]
    if any(
        type(item) is not GraphWriteRecord
        or item.kind not in {
            "node", "edge", "property", "projection", "learning_association"
        }
        or type(item.entity_type) is not str
        or type(item.entity_id) is not str
        for item in records
    ):
        raise ProjectionJournalRecordsError("projection_record_sequence_invalid")
    return records
