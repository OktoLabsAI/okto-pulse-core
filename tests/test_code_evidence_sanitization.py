from __future__ import annotations

import hashlib

import pytest

from okto_pulse.core.domain.code_traceability import (
    CodeEvidenceSelectorKind,
    CodeEvidenceSubmissionFailed,
    CodeEvidenceType,
    CodeInvestigationSubmissionLimitExceeded,
    CodeInvestigationTrustLevel,
    CodeTraceabilitySubjectType,
)
from okto_pulse.core.models.code_traceability import (
    CodeEvidenceSelectorInput,
    CodeEvidenceSubmission,
)
from okto_pulse.core.services.code_evidence import CodeEvidenceService
from okto_pulse.core.services.code_evidence_sanitization import (
    sanitize_code_evidence_excerpt,
    sanitize_code_evidence_submission,
)
from okto_pulse.core.services.code_investigation import (
    CodeInvestigationService,
    required_capabilities_for_subject,
    selector_scope_digest_for_subject,
)
from test_code_traceability_application import (
    H1,
    FakeInvestigationStore,
    FakeTraceabilityStore,
    MutableClock,
    StableIds,
    accepted_receipt,
    challenge_policy,
)


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _submission(*, receipt_id: str, excerpt: str) -> CodeEvidenceSubmission:
    return CodeEvidenceSubmission(
        board_id="board-1",
        investigation_receipt_id=receipt_id,
        parent_type=CodeTraceabilitySubjectType.REFINEMENT,
        parent_id="refinement-1",
        evidence_type=CodeEvidenceType.BEHAVIOR,
        claim="The external agent submitted bounded supporting material.",
        selector=CodeEvidenceSelectorInput(
            kind=CodeEvidenceSelectorKind.FILE,
            relative_path="src/service.py",
        ),
        excerpt=excerpt,
        excerpt_sha256=_sha(excerpt),
        declared_source_content_sha256=H1,
        idempotency_key="evidence-secret-redaction",
    )


def test_excerpt_redaction_is_deterministic_and_rehashes_accepted_content() -> None:
    secret = "ghp_abcdefghijklmnopqrstuvwxyz0123456789"
    excerpt = (
        f'api_key="{secret}"\n'
        "Authorization: Bearer abcdefghijklmnopqrstuvwxyz\n"
        "safe_value = 42\n"
    )

    first = sanitize_code_evidence_excerpt(
        excerpt,
        claimed_sha256=_sha(excerpt),
    )
    second = sanitize_code_evidence_excerpt(
        excerpt,
        claimed_sha256=_sha(excerpt),
    )

    assert first == second
    assert first.redaction_count >= 2
    assert secret not in (first.excerpt or "")
    assert "abcdefghijklmnopqrstuvwxyz" not in (first.excerpt or "")
    assert "safe_value = 42" in (first.excerpt or "")
    assert first.excerpt_sha256 == _sha(first.excerpt or "")

    alternate_secret = "ghp_9876543210zyxwvutsrqponmlkjihgfedcba"
    alternate = excerpt.replace(secret, alternate_secret)
    accepted_first = sanitize_code_evidence_submission(
        _submission(receipt_id="receipt-1", excerpt=excerpt)
    )
    accepted_alternate = sanitize_code_evidence_submission(
        _submission(receipt_id="receipt-1", excerpt=alternate)
    )
    assert accepted_first.model_dump(mode="python") == (
        accepted_alternate.model_dump(mode="python")
    )


def test_uri_credentials_private_keys_and_known_tokens_are_redacted() -> None:
    slack_token = "-".join(("xoxb", "1234567890", "abcdefghijklmnop"))
    excerpt = "\n".join(
        (
            "postgresql://service:database-password@example.invalid/app",
            "-----BEGIN PRIVATE KEY-----\nvery-secret-material\n"
            "-----END PRIVATE KEY-----",
            slack_token,
            "eyJabcdefghijk.eyJmnopqrstuv.abcdefghijklmnop",
        )
    )
    sanitized = sanitize_code_evidence_excerpt(
        excerpt,
        claimed_sha256=_sha(excerpt),
    )
    rendered = sanitized.excerpt or ""
    for secret in (
        "database-password",
        "very-secret-material",
        slack_token,
        "eyJabcdefghijk.eyJmnopqrstuv.abcdefghijklmnop",
    ):
        assert secret not in rendered
    assert "postgresql://service:[REDACTED]@example.invalid/app" in rendered


def test_crlf_quoted_assignments_and_incomplete_private_keys_are_safe(
    caplog: pytest.LogCaptureFixture,
) -> None:
    windows_excerpt = 'password="my very secret"\r\nresult = True\r\n'
    unix_excerpt = windows_excerpt.replace("\r\n", "\n")

    windows = sanitize_code_evidence_excerpt(
        windows_excerpt,
        claimed_sha256=_sha(windows_excerpt),
    )
    unix = sanitize_code_evidence_excerpt(
        unix_excerpt,
        claimed_sha256=_sha(unix_excerpt),
    )

    assert windows == unix
    assert windows.excerpt == 'password="[REDACTED]"\nresult = True\n'
    assert "my very secret" not in (windows.excerpt or "")

    incomplete_secret = "private-material-that-must-never-escape"
    incomplete_key = (
        "-----BEGIN PRIVATE KEY-----\n"
        f"{incomplete_secret}\n"
        "still truncated"
    )
    private_key = sanitize_code_evidence_excerpt(
        incomplete_key,
        claimed_sha256=_sha(incomplete_key),
    )
    assert private_key.excerpt == "[REDACTED PRIVATE KEY]"
    assert incomplete_secret not in caplog.text
    assert "my very secret" not in caplog.text


def test_binary_and_size_rejections_expose_only_bounded_metadata() -> None:
    embedded_secret = "do-not-log-this-secret"
    binary = f"prefix\x00{embedded_secret}"
    with pytest.raises(CodeEvidenceSubmissionFailed) as captured:
        sanitize_code_evidence_excerpt(
            binary,
            claimed_sha256=_sha(binary),
        )
    rendered = str(captured.value.as_dict())
    assert captured.value.details == {"reason": "binary_content", "field": "excerpt"}
    assert embedded_secret not in rendered

    oversized = "x" * (8 * 1024 + 1)
    with pytest.raises(CodeInvestigationSubmissionLimitExceeded) as too_large:
        sanitize_code_evidence_excerpt(
            oversized,
            claimed_sha256=_sha(oversized),
        )
    assert too_large.value.details == {"field": "excerpt", "max_bytes": 8192}
    assert oversized[:100] not in str(too_large.value.as_dict())


@pytest.mark.asyncio
async def test_evidence_service_persists_only_defensively_sanitized_excerpt() -> None:
    clock = MutableClock()
    ids = StableIds()
    investigations = FakeInvestigationStore()
    traceability = FakeTraceabilityStore(investigations)
    investigation_service = CodeInvestigationService(
        challenge_policy=challenge_policy(),
        clock=clock,
        id_factory=ids,
    )
    scope = selector_scope_digest_for_subject(
        board_id="board-1",
        subject_type=CodeTraceabilitySubjectType.REFINEMENT,
        subject_id="refinement-1",
        subject_version=3,
    )
    accepted = await accepted_receipt(
        service=investigation_service,
        store=investigations,
        clock=clock,
        actor_id="agent-1",
        subject_type=CodeTraceabilitySubjectType.REFINEMENT,
        subject_id="refinement-1",
        subject_version=3,
        source_ref=None,
        selector_scope_digest=scope,
        capabilities=required_capabilities_for_subject(
            CodeTraceabilitySubjectType.REFINEMENT
        ),
        request_key="request-redaction",
        receipt_key="receipt-redaction",
    )
    raw_secret = "sk-abcdefghijklmnopqrstuvwxyz0123456789"
    result = await CodeEvidenceService(clock=clock, id_factory=ids).submit(
        _submission(
            receipt_id=accepted.receipt.id,
            excerpt=f"client_secret={raw_secret}\nresult = True\n",
        ),
        actor_id="agent-1",
        actor_kind="agent",
        current_parent_version=3,
        minimum_trust=CodeInvestigationTrustLevel.SINGLE_ATTESTATION,
        require_committed_state=False,
        investigation_service=investigation_service,
        investigation_store=investigations,
        store=traceability,
    )

    assert raw_secret not in (result.evidence.excerpt or "")
    assert "[REDACTED]" in (result.evidence.excerpt or "")
    assert result.evidence.excerpt_sha256 == _sha(result.evidence.excerpt or "")
    assert traceability.evidence[result.evidence.id] == result.evidence
