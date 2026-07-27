"""LLMProvider Protocol — runtime-agnostic port for LLM calls in the core (R13-A).

The cognitive / query flows (HyDE, decompose, fusion, adaptive hops, LLM rerank,
context compression, retrieve critic, grounding, heuristics and cognitive
extraction) historically reached LLM behaviour through scattered callables and
protocols. R13-A consolidates the *contract* — request/response DTOs, normalized
failure reasons and budget/telemetry context — WITHOUT choosing a provider,
model, credential, billing or tenant. Concrete providers are wired by adapters /
composition and remain edition-local until #05.

This module deliberately:
  * does NOT import any vendor SDK, ``sentence_transformers``, concrete provider,
    ``requests`` or Community-local settings (conformance gate ts_06896ae7);
  * does NOT recreate ``EmbeddingProvider`` (interfaces/embedding.py) or
    ``Reranker`` (interfaces/reranker.py) — those remain the canonical ports and
    are only documented as compatible here.

Bridges (R13-B/C/D) adapt this port to the existing callables; absence of a
provider preserves the current per-flow behaviour by mapping ``provider_absent``
to either an explicit error or a fallback/no-op, per the consuming flow.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

# ---------------------------------------------------------------------------
# Normalized failure reasons (FR fr_r13a_failure_semantics).
#
# A bridge maps these to the behaviour the consuming flow already had:
#   - ``provider_absent``  -> explicit error where a callable is REQUIRED today
#                             (e.g. query_rewrite ``llm`` strategy), or no-op/
#                             fallback where the flow already degrades.
#   - ``timeout`` / ``provider_error`` -> the flow's existing error path.
#   - ``invalid_response`` -> malformed/unsable provider output.
# The contract never decides between raise vs fallback; the bridge does.
# ---------------------------------------------------------------------------
LLM_OK = "ok"
LLM_PROVIDER_ABSENT = "provider_absent"
LLM_TIMEOUT = "timeout"
LLM_PROVIDER_ERROR = "provider_error"
LLM_INVALID_RESPONSE = "invalid_response"

#: The set of normalized *failure* reasons (``LLM_OK`` excluded).
LLM_FAILURE_REASONS: frozenset[str] = frozenset(
    {LLM_PROVIDER_ABSENT, LLM_TIMEOUT, LLM_PROVIDER_ERROR, LLM_INVALID_RESPONSE}
)


@dataclass(frozen=True)
class LLMMessage:
    """A single chat message. ``role`` is provider-agnostic (e.g. ``system`` /
    ``user`` / ``assistant``); adapters map it to the concrete provider."""

    role: str
    content: str


@dataclass(frozen=True)
class LLMRequest:
    """A runtime-agnostic LLM request.

    ``purpose`` labels the calling flow (e.g. ``"hyde"``, ``"decompose"``,
    ``"rerank"``, ``"grounding"``, ``"cognitive_extraction"``) so adapters can
    route model/budget/telemetry without the core knowing the provider. Either
    ``input`` (single textual prompt) or ``messages`` (chat) is supplied; both
    may be empty when a bridge only needs failure semantics.

    ``board_id`` / ``actor_id`` carry optional tenant-neutral context. They do
    NOT imply realm/tenant isolation (out of scope for Phase 1) — they are
    passthrough labels for telemetry/budget attribution only.
    """

    purpose: str
    prompt_id: str | None = None
    input: str | None = None
    messages: tuple[LLMMessage, ...] = ()
    board_id: str | None = None
    actor_id: str | None = None
    timeout_seconds: float | None = None
    max_tokens: int | None = None
    budget_hint: dict[str, Any] | None = None
    rate_limit_hint: dict[str, Any] | None = None
    telemetry_labels: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class LLMUsage:
    """Optional token-usage accounting. All fields optional — a provider that
    does not report usage leaves them ``None``."""

    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None


@dataclass(frozen=True)
class LLMResponse:
    """A runtime-agnostic LLM response.

    ``status`` is ``LLM_OK`` on success or one of ``LLM_FAILURE_REASONS`` when
    the provider/bridge normalized a failure. ``text`` and/or ``json`` carry the
    payload; ``failure_reason`` carries a short, secret-free detail for the
    failure case (never the full prompt, credential material or raw provider
    error body — OR or_r13a_provider_failure_reasons).

    Field set matches the approved API contract ``api_r13a_llm_provider_protocol``:
    ``status, text, json, usage, finish_reason, failure_reason``.
    """

    status: str = LLM_OK
    text: str | None = None
    json: Any | None = None
    finish_reason: str | None = None
    usage: LLMUsage | None = None
    failure_reason: str | None = None

    @property
    def ok(self) -> bool:
        """True iff the response carries a successful completion."""
        return self.status == LLM_OK

    @property
    def is_failure(self) -> bool:
        """True iff the response normalized a known failure reason."""
        return self.status in LLM_FAILURE_REASONS

    @classmethod
    def failure(cls, status: str, failure_reason: str | None = None) -> "LLMResponse":
        """Build a normalized failure response. ``status`` MUST be one of
        ``LLM_FAILURE_REASONS``."""
        if status not in LLM_FAILURE_REASONS:
            raise ValueError(f"unknown LLM failure status: {status!r}")
        return cls(status=status, failure_reason=failure_reason)


class LLMProviderError(Exception):
    """Normalized LLM failure raised where a flow requires an explicit error
    (e.g. a strategy that REQUIRES a callable today). ``reason`` is one of
    ``LLM_FAILURE_REASONS``. The message is kept secret-free."""

    def __init__(self, reason: str, message: str = "") -> None:
        if reason not in LLM_FAILURE_REASONS:
            raise ValueError(f"unknown LLM failure reason: {reason!r}")
        self.reason = reason
        super().__init__(message or reason)


@runtime_checkable
class LLMProvider(Protocol):
    """Runtime-agnostic LLM port. Adapters/composition decide the concrete
    provider, model and credential; the core depends only on this contract.

    Implementations SHOULD normalize failures into ``LLMResponse.status`` (or
    raise ``LLMProviderError``) rather than leaking vendor-specific exceptions,
    so bridges can preserve each flow's existing error-vs-fallback behaviour.

    Selection/description of a provider on common surfaces MUST use
    ``capabilities`` metadata, never ``isinstance`` against a concrete class
    (BR br_r13a_metadata_over_type_checks). Implementations satisfy this
    Protocol by duck typing (PEP 544); they need not inherit.
    """

    #: Stable provider identifier (e.g. ``"noop"``, ``"openai"``). Used for
    #: telemetry/description without importing the concrete class.
    name: str

    def complete(self, request: LLMRequest) -> LLMResponse:
        """Execute an LLM completion and return a normalized response."""
        ...

    def capabilities(self) -> dict[str, Any]:
        """Return capability/metadata describing this provider (model family,
        purposes supported, whether it is a stub, etc.) so common surfaces can
        describe/select it via metadata instead of ``isinstance``."""
        ...
