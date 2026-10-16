"""Native publish status, initial defaults and secret-redaction invariants."""

from __future__ import annotations

from pathlib import Path

import pytest

from okto_pulse.core.telemetry.failure_state import (
    CONSENT_BLOCKED,
    CONSENT_GRANTED,
    CONSENT_UNKNOWN,
    FAILURE_STATE_KEY,
    PUBLIC_FAILURE_STATE_FIELDS,
    STATUS_DEGRADED,
    STATUS_UNKNOWN,
    FailureState,
    is_secret_key,
    merge,
    public_status_projection,
    read_failure_state,
    redact_secret_keys,
    write_failure_state,
)
from okto_pulse.core.telemetry.settings import load_state, save_state

# A native state with explicit outcome and cursor fields.
_NATIVE_STATE = {
    "install_id": "11111111-2222-3333-4444-555555555555",
    "install_token": "super-secret-token-value",
    "token_hash": "deadbeefdeadbeef",
    "install_token_expires_at": "2026-07-01T00:00:00Z",
    "next_batch_seq": 7,
    "watermark": None,
    "watermark_event_id": None,
    FAILURE_STATE_KEY: FailureState().to_public_dict(),
}

_SECRET_VALUES = {"super-secret-token-value", "deadbeefdeadbeef"}




def _assert_no_secret(obj) -> None:
    """No secret key or secret value appears anywhere in a (nested) structure."""
    blob = repr(obj)
    for value in _SECRET_VALUES:
        assert value not in blob
    if isinstance(obj, dict):
        for key in obj:
            assert not is_secret_key(key), f"secret key leaked: {key}"








def test_public_projection_is_allowlisted_even_with_injected_secret(tmp_path: Path) -> None:
    """A secret accidentally written into the failure_state block is dropped on
    read — the projection is allowlist-based, so it is structurally secret-free."""
    metrics_dir = tmp_path / "metrics"
    save_state(
        metrics_dir,
        {
            "mode": "anonymous_beacon",
            FAILURE_STATE_KEY: {
                "status": STATUS_DEGRADED,
                "reason_code": "USAGE_503",
                "retry_count": 2,
                # Hostile/buggy extra keys that must NOT survive a read:
                "install_token": "leaked-token",
                "token_hash": "leaked-hash",
            },
        },
    )

    state = load_state(metrics_dir)
    fs = read_failure_state(state)
    assert fs.status == STATUS_DEGRADED
    assert fs.reason_code == "USAGE_503"
    assert fs.retry_count == 2

    projection = public_status_projection(state)
    assert set(projection) == set(PUBLIC_FAILURE_STATE_FIELDS)
    assert "install_token" not in projection
    assert "token_hash" not in projection
    assert "leaked-token" not in repr(projection)


def test_write_failure_state_preserves_other_keys_and_omits_secrets() -> None:
    # R-P2-08: the FS persistence roundtrip is a Community concern; the core keeps
    # the PURE projection — write_failure_state must not disturb other state keys.
    state = {**_NATIVE_STATE, "mode": "anonymous_beacon"}

    fs = merge(
        read_failure_state(state),
        status=STATUS_DEGRADED,
        reason_code="USAGE_500",
        http_status=500,
        last_failure_at="2026-06-15T17:00:00Z",
        next_retry_at="2026-06-15T17:15:00Z",
        retry_count=1,
    )
    updated = write_failure_state(state, fs)

    # Round-trips identically through the pure projection.
    assert read_failure_state(updated) == fs

    # Other keys preserved (token still there for publishing).
    assert updated["install_token"] == "super-secret-token-value"
    assert updated["next_batch_seq"] == 7
    # The persisted failure_state block carries only allowlisted, non-secret keys.
    block = updated[FAILURE_STATE_KEY]
    assert set(block) == set(PUBLIC_FAILURE_STATE_FIELDS)
    _assert_no_secret(block)


def test_is_secret_key_covers_credentials_not_timestamps() -> None:
    for secret in ("install_token", "token_hash", "signature", "token", "refresh_token"):
        assert is_secret_key(secret) is True
    for safe in (
        "install_token_expires_at",
        "install_id",
        "next_batch_seq",
        "last_send_at",
        "status",
        "reason_code",
    ):
        assert is_secret_key(safe) is False


def test_redact_secret_keys_strips_credentials_keeps_safe_fields() -> None:
    redacted = redact_secret_keys({**_NATIVE_STATE, "mode": "anonymous_beacon"})
    assert "install_token" not in redacted
    assert "token_hash" not in redacted
    assert redacted["install_token_expires_at"] == "2026-07-01T00:00:00Z"
    assert redacted["install_id"] == _NATIVE_STATE["install_id"]
    _assert_no_secret(redacted)


def test_merge_validates_status_and_consent() -> None:
    base = FailureState()
    ok = merge(base, status=STATUS_DEGRADED, retry_count=3)
    assert ok.status == STATUS_DEGRADED and ok.retry_count == 3
    assert base.retry_count == 0  # frozen: original untouched

    with pytest.raises(ValueError):
        merge(base, status="not-a-real-status")
    with pytest.raises(ValueError):
        merge(base, consent_state="maybe")


def test_write_failure_state_is_pure_and_allowlisted() -> None:
    state = {"install_token": "secret", "mode": "anonymous_beacon"}
    out = write_failure_state(state, FailureState(status=STATUS_UNKNOWN))
    # original untouched (pure)
    assert FAILURE_STATE_KEY not in state
    # block is allowlisted
    assert set(out[FAILURE_STATE_KEY]) == set(PUBLIC_FAILURE_STATE_FIELDS)
    # untouched sibling keys preserved
    assert out["install_token"] == "secret"


@pytest.mark.parametrize("mode,consent,enabled", [(None, CONSENT_UNKNOWN, False), ("disabled", CONSENT_BLOCKED, False), ("anonymous_beacon", CONSENT_GRANTED, True)])
def test_initial_status_does_not_invent_a_previous_outcome(mode, consent, enabled):
    state = {"mode": mode}
    result = read_failure_state(state)
    assert result.status == STATUS_UNKNOWN
    assert result.last_success_at is None
    assert result.consent_state == consent
    assert result.publish_enabled is enabled
    assert state == {"mode": mode}
    _assert_no_secret(public_status_projection(state))


@pytest.mark.parametrize("state", [
    {"last_send_at": "2026-06-12T20:15:00Z"},
    {"next_batch_seq": 7},
    {"failure_state": None},
])
def test_missing_or_invalid_status_is_not_reconstructed(state):
    with pytest.raises(ValueError):
        read_failure_state(state)
