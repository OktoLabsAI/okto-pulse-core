"""Shared REST/MCP construction contract for policy-keyset cursors.

The composition root supplies the same stable secret-backed settings snapshot
to both inbound transports.  This module intentionally has no environment
reader and no random/default key: missing configuration fails closed instead
of producing cursors that become invalid after a restart or differ by
transport process.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from okto_pulse.core.domain.guideline_compliance import PolicyCursorCodec


POLICY_CURSOR_SIGNING_KEY_SETTING = "guideline_policy_cursor_signing_key"


class GuidelinePolicyCursorConfigurationError(RuntimeError):
    """The edition did not provide a usable shared cursor signing secret."""

    code = "guideline_policy_cursor_unavailable"

    def __init__(self) -> None:
        super().__init__(self.code)


@runtime_checkable
class GuidelinePolicyCursorSettings(Protocol):
    """Minimal settings surface consumed by both REST and MCP composition."""

    guideline_policy_cursor_signing_key: object


def policy_cursor_codec_from_settings(
    settings: GuidelinePolicyCursorSettings | object,
) -> PolicyCursorCodec:
    """Build a codec from one explicitly injected stable settings value.

    ``SecretStr``-like values are supported without importing Pydantic into the
    contract.  The raw secret is never returned, logged, or included in an
    error.  Callers may cache the resulting codec at their composition root.
    """

    raw_value = getattr(settings, POLICY_CURSOR_SIGNING_KEY_SETTING, None)
    reveal = getattr(raw_value, "get_secret_value", None)
    if callable(reveal):
        raw_value = reveal()
    if isinstance(raw_value, str):
        raw_value = raw_value.encode("utf-8")
    if not isinstance(raw_value, bytes) or len(raw_value) < 32:
        raise GuidelinePolicyCursorConfigurationError
    return PolicyCursorCodec(raw_value)


__all__ = [
    "POLICY_CURSOR_SIGNING_KEY_SETTING",
    "GuidelinePolicyCursorConfigurationError",
    "GuidelinePolicyCursorSettings",
    "policy_cursor_codec_from_settings",
]
