"""Defensive sanitization for excerpts already submitted by an external agent.

This module is deliberately pure: it receives a bounded string and never reads
the claimed path, repository, provider, process environment or filesystem.
The external agent remains responsible for primary secret/binary screening;
Pulse applies this deterministic second pass before persistence.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import hmac
import re
from typing import TypeVar

from okto_pulse.core.domain.code_traceability import (
    CODE_EVIDENCE_EXCERPT_OMITTED_NOT_SUBMITTED,
    DEFAULT_CODE_TRACEABILITY_LIMITS,
    CodeEvidenceSubmissionFailed,
    CodeInvestigationPayloadDigestMismatch,
    CodeInvestigationSubmissionLimitExceeded,
)
from okto_pulse.core.models.code_traceability import CodeEvidenceSubmission


_PRIVATE_KEY_RE = re.compile(
    r"-----BEGIN (?:RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----.*?"
    r"(?:-----END (?:RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----|\Z)",
    re.IGNORECASE | re.DOTALL,
)
_BEARER_RE = re.compile(
    r"(?i)(\b(?:authorization\s*[:=]\s*)?(?:bearer|token)\s+)"
    r"[A-Za-z0-9._~+/=-]{8,}"
)
_CREDENTIAL_URI_RE = re.compile(
    r"(?i)\b([a-z][a-z0-9+.-]*://[^/\s:@]+:)([^@\s/]+)(@)"
)
_ASSIGNMENT_RE = re.compile(
    r"(?im)(\b(?:api[_-]?key|access[_-]?key|client[_-]?secret|password|passwd|"
    r"secret|auth[_-]?token|access[_-]?token|refresh[_-]?token)\b\s*[:=]\s*)"
    r"(?:(['\"])([^\r\n]*)\2|(['\"][^\r\n]*)|([^\s,;]+))"
)
_STANDALONE_SECRET_PATTERNS = (
    re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
    re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,}\b"),
    re.compile(r"\bglpat-[A-Za-z0-9_-]{20,}\b"),
    re.compile(r"\bnpm_[A-Za-z0-9]{20,}\b"),
    re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b"),
    re.compile(r"\bAIza[0-9A-Za-z_-]{20,}\b"),
    re.compile(r"\b(?:rk|sk)_(?:live|test)_[0-9A-Za-z]{16,}\b"),
    re.compile(r"\bsk-[A-Za-z0-9_-]{20,}\b"),
    re.compile(
        r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\."
        r"[A-Za-z0-9_-]{8,}\b"
    ),
)
_REDACTED = "[REDACTED]"


@dataclass(frozen=True, slots=True)
class SanitizedCodeEvidenceExcerpt:
    excerpt: str | None
    excerpt_sha256: str | None
    excerpt_omitted_reason: str | None
    redaction_count: int = 0


def _reject_binary_content(excerpt: str) -> None:
    if any(
        character == "\x00"
        or (ord(character) < 32 and character not in {"\t", "\n"})
        or ord(character) == 127
        for character in excerpt
    ):
        raise CodeEvidenceSubmissionFailed(
            details={"reason": "binary_content", "field": "excerpt"}
        )


def sanitize_code_evidence_excerpt(
    excerpt: str | None,
    *,
    claimed_sha256: str | None,
) -> SanitizedCodeEvidenceExcerpt:
    """Validate, redact and hash one received excerpt without leaking content."""

    if excerpt is None:
        if claimed_sha256 is not None:
            raise CodeInvestigationPayloadDigestMismatch(
                details={"field": "excerpt_sha256"}
            )
        return SanitizedCodeEvidenceExcerpt(
            excerpt=None,
            excerpt_sha256=None,
            excerpt_omitted_reason=(
                CODE_EVIDENCE_EXCERPT_OMITTED_NOT_SUBMITTED
            ),
        )
    encoded = excerpt.encode("utf-8")
    limit = DEFAULT_CODE_TRACEABILITY_LIMITS.evidence_excerpt_bytes
    if len(encoded) > limit:
        raise CodeInvestigationSubmissionLimitExceeded(
            details={"field": "excerpt", "max_bytes": limit}
        )
    expected = hashlib.sha256(encoded).hexdigest()
    if claimed_sha256 is None or not hmac.compare_digest(
        claimed_sha256.casefold(), expected
    ):
        raise CodeInvestigationPayloadDigestMismatch(
            details={"field": "excerpt_sha256"}
        )
    # The sender's digest authenticates the exact received bytes.  Persistence
    # then uses one canonical text form so CRLF/CR agents produce the same safe
    # accepted excerpt and content digest.
    normalized_excerpt = excerpt.replace("\r\n", "\n").replace("\r", "\n")
    _reject_binary_content(normalized_excerpt)

    redaction_count = 0

    def redact_private_key(_match: re.Match[str]) -> str:
        nonlocal redaction_count
        redaction_count += 1
        return "[REDACTED PRIVATE KEY]"

    def redact_bearer(match: re.Match[str]) -> str:
        nonlocal redaction_count
        redaction_count += 1
        return f"{match.group(1)}{_REDACTED}"

    def redact_assignment(match: re.Match[str]) -> str:
        nonlocal redaction_count
        redaction_count += 1
        quote = match.group(2)
        if quote:
            return f"{match.group(1)}{quote}{_REDACTED}{quote}"
        unclosed = match.group(4)
        if unclosed:
            return f"{match.group(1)}{unclosed[0]}{_REDACTED}"
        return f"{match.group(1)}{_REDACTED}"

    def redact_uri_credential(match: re.Match[str]) -> str:
        nonlocal redaction_count
        redaction_count += 1
        return f"{match.group(1)}{_REDACTED}{match.group(3)}"

    sanitized = _PRIVATE_KEY_RE.sub(redact_private_key, normalized_excerpt)
    sanitized = _BEARER_RE.sub(redact_bearer, sanitized)
    sanitized = _CREDENTIAL_URI_RE.sub(redact_uri_credential, sanitized)
    sanitized = _ASSIGNMENT_RE.sub(redact_assignment, sanitized)
    for pattern in _STANDALONE_SECRET_PATTERNS:
        sanitized, count = pattern.subn(_REDACTED, sanitized)
        redaction_count += count

    sanitized_hash = hashlib.sha256(sanitized.encode("utf-8")).hexdigest()
    return SanitizedCodeEvidenceExcerpt(
        excerpt=sanitized,
        excerpt_sha256=sanitized_hash,
        excerpt_omitted_reason=None,
        redaction_count=redaction_count,
    )


SubmissionT = TypeVar("SubmissionT", bound=CodeEvidenceSubmission)


def sanitize_code_evidence_submission(submission: SubmissionT) -> SubmissionT:
    sanitized = sanitize_code_evidence_excerpt(
        submission.excerpt,
        claimed_sha256=submission.excerpt_sha256,
    )
    return submission.model_copy(
        update={
            "excerpt": sanitized.excerpt,
            "excerpt_sha256": sanitized.excerpt_sha256,
        }
    )


__all__ = [
    "SanitizedCodeEvidenceExcerpt",
    "sanitize_code_evidence_excerpt",
    "sanitize_code_evidence_submission",
]
