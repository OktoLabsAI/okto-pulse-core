"""Pydantic DTOs for the AuditRepository — decoupled from SQLAlchemy models."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class ConsolidationAuditData(BaseModel):
    session_id: str
    board_id: str
    artifact_id: str
    artifact_type: str
    agent_id: str
    started_at: datetime
    committed_at: datetime
    nodes_added: int = 0
    nodes_updated: int = 0
    nodes_superseded: int = 0
    edges_added: int = 0
    summary_text: str | None = None
    content_hash: str | None = None


class NodeRefData(BaseModel):
    session_id: str
    board_id: str
    kuzu_node_id: str
    kuzu_node_type: str
    operation: str


class OutboxEventData(BaseModel):
    event_id: str
    board_id: str
    session_id: str
    event_type: str
    payload: dict[str, Any]


class AuditRow(BaseModel):
    """Read-side DTO returned by AuditRepository queries."""

    session_id: str
    board_id: str
    artifact_id: str
    artifact_type: str
    agent_id: str
    started_at: datetime
    committed_at: datetime | None = None
    nodes_added: int = 0
    nodes_updated: int = 0
    nodes_superseded: int = 0
    edges_added: int = 0
    summary_text: str | None = None
    content_hash: str | None = None
    undo_status: str = "none"
