"""Application-facing agent helpers.

Public facade for agent credential primitives that Community needs during
first-boot seed without importing the legacy ``services.main`` module.
"""

from __future__ import annotations


def hash_api_key(key: str) -> str:
    from okto_pulse.core.services.main import AgentService

    return AgentService.hash_api_key(key)


def credential_marker(key_hash: str) -> str:
    from okto_pulse.core.services.main import AgentService

    return AgentService.credential_marker(key_hash)


__all__ = [
    "credential_marker",
    "hash_api_key",
]
