"""MCPQuickWinPayloadCompactor — local MCP payload compaction (spec R3, card
R3.2, FR6/FR8).

Drops null/empty NON-semantic fields from selected high-frequency MCP read
responses (get_card, list_cards_by_status, list_comments) and collapses a
duplicated ``updated_at`` (== ``created_at``). It does NOT touch authorization,
persistence, or query semantics — only the shape of the serialized response.

Semantic fields (ids, status, card/comment type, and any non-empty linkage /
comments / Q&A / workflow state) are preserved; only genuinely empty,
non-semantic keys are omitted. This is the *local* compactor — the reusable
global projection envelope is R5's concern, not this card's.

Observability (FR8 / or_f4159e58): ``compact_and_emit`` emits a safe diagnostic
under the ``mcp_quickwin_compaction_total`` metric name carrying only counts and
identifiers — ``tool_name``, ``profile``, ``omitted_count``, ``deduped_count``
and a ``truncated`` flag — and NEVER any payload body. This mirrors R3.1's
test-observable metadata approach; R5 may later consolidate the metric family.
"""

from __future__ import annotations

import logging
from typing import Any

# Keys whose value is meaningful even when falsy/empty — never dropped.
_SEMANTIC_ALWAYS = frozenset({"id", "status", "card_type", "comment_type"})

# Metric name for the safe compaction diagnostic (or_f4159e58). Emitted as the
# log event; R5 may consolidate the metric family later.
COMPACTION_METRIC = "mcp_quickwin_compaction_total"

_LOG = logging.getLogger("okto_pulse.mcp.compaction")


def _is_empty(value: Any) -> bool:
    """True for the non-semantic 'absent' values we drop: None and empties."""
    return value is None or value == "" or value == [] or value == {}


class MCPQuickWinPayloadCompactor:
    """Compact a response object by dropping null/empty non-semantic fields."""

    def compact(self, obj: Any) -> Any:
        """Return the compacted object (no stats); kept for callers/tests that
        only want the reshaped payload."""
        compacted, _ = self._walk(obj)
        return compacted

    def compact_with_stats(
        self,
        obj: Any,
        *,
        tool_name: str,
        profile: str = "compact",
        truncated: bool = False,
    ) -> tuple[Any, dict[str, Any]]:
        """Compact ``obj`` and return ``(compacted, stats)``.

        ``stats`` carries ONLY counts + identifiers (no payload body) so it is
        safe to log or export as the ``mcp_quickwin_compaction_total`` metric.
        """
        compacted, counters = self._walk(obj)
        stats = {
            "tool_name": tool_name,
            "profile": profile,
            "omitted_count": counters["omitted"],
            "deduped_count": counters["deduped"],
            "truncated": bool(truncated),
        }
        return compacted, stats

    def _walk(self, obj: Any) -> tuple[Any, dict[str, int]]:
        """Recursive compaction that also accumulates omitted/deduped counters."""
        if isinstance(obj, dict):
            created = obj.get("created_at")
            out: dict[Any, Any] = {}
            omitted = 0
            deduped = 0
            for key, value in obj.items():
                # Collapse a duplicated updated_at (no edit since creation).
                if key == "updated_at" and value is not None and value == created:
                    deduped += 1
                    continue
                compacted, sub = self._walk(value)
                omitted += sub["omitted"]
                deduped += sub["deduped"]
                if key in _SEMANTIC_ALWAYS or not _is_empty(compacted):
                    out[key] = compacted
                else:
                    omitted += 1
            return out, {"omitted": omitted, "deduped": deduped}
        if isinstance(obj, list):
            out_list = []
            omitted = 0
            deduped = 0
            for item in obj:
                compacted, sub = self._walk(item)
                omitted += sub["omitted"]
                deduped += sub["deduped"]
                out_list.append(compacted)
            return out_list, {"omitted": omitted, "deduped": deduped}
        return obj, {"omitted": 0, "deduped": 0}


_COMPACTOR = MCPQuickWinPayloadCompactor()


def compact_payload(obj: Any) -> Any:
    """Module-level convenience wrapper over a shared compactor instance."""
    return _COMPACTOR.compact(obj)


def compute_compaction_stats(
    obj: Any,
    *,
    tool_name: str,
    profile: str = "compact",
    truncated: bool = False,
) -> tuple[Any, dict[str, Any]]:
    """Compact ``obj`` and also return the safe stats payload (no emission)."""
    return _COMPACTOR.compact_with_stats(
        obj, tool_name=tool_name, profile=profile, truncated=truncated
    )


def emit_compaction_metric(
    *,
    tool_name: str,
    profile: str = "compact",
    omitted_count: int = 0,
    deduped_count: int = 0,
    truncated: bool = False,
) -> dict[str, Any]:
    """Emit the safe ``mcp_quickwin_compaction_total`` diagnostic and return the
    stats payload.

    Used both by ``compact_and_emit`` (compactor-based paths) and by the manual
    dedup paths (traceability / business-rule / API-contract lists) so EVERY
    compacted-or-deduped quick-win response records the metric (or_f4159e58).
    The payload carries ONLY counts + identifiers — never a body, FR text, or
    artifact content.
    """
    stats = {
        "tool_name": tool_name,
        "profile": profile,
        "omitted_count": int(omitted_count),
        "deduped_count": int(deduped_count),
        "truncated": bool(truncated),
    }
    _LOG.info(COMPACTION_METRIC, extra={"compaction": stats})
    return stats


def compact_and_emit(
    obj: Any,
    *,
    tool_name: str,
    profile: str = "compact",
    truncated: bool = False,
) -> Any:
    """Compact ``obj``, emit the safe ``mcp_quickwin_compaction_total`` diagnostic
    (counts only, never a payload body), and return the compacted object."""
    compacted, stats = _COMPACTOR.compact_with_stats(
        obj, tool_name=tool_name, profile=profile, truncated=truncated
    )
    # FR8 / or_f4159e58: counts + identifiers only — the response body never
    # travels through the diagnostic channel.
    _LOG.info(COMPACTION_METRIC, extra={"compaction": stats})
    return compacted
