"""B13 contract tests for revision keyset cursors shared by REST and MCP."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from pydantic import SecretStr

from okto_pulse.core.domain.guideline_compliance import (
    POLICY_KEYSET_CONTRACT_VERSION,
    PolicyCursorCodec,
    PolicyProjection,
)
from okto_pulse.core.domain.guideline_policy import (
    GUIDELINE_PAGE_LIMIT_MAX,
    GUIDELINE_REVISION_ORDERING,
    GuidelinePolicyContractError,
    GuidelineRevisionPageCursor,
)
from okto_pulse.core.inbound.guideline_policy_cursor import (
    GuidelinePolicyCursorConfigurationError,
    policy_cursor_codec_from_settings,
)
from okto_pulse.core.ports.guideline_policy import (
    GuidelinePolicyInvalidCursor,
    GuidelineRevisionListQuery,
)


SIGNING_KEY = b"b13-policy-cursor-shared-key-32-bytes"


def _cursor(
    query: GuidelineRevisionListQuery,
    *,
    revision_number: int = 17,
    item_id: str = "revision-17",
) -> GuidelineRevisionPageCursor:
    return GuidelineRevisionPageCursor(
        revision_number=revision_number,
        item_id=item_id,
        filter_digest=query.filter_digest,
        projection_digest=query.projection_digest,
    )


def test_revision_query_preserves_bounded_defaults_and_stable_order() -> None:
    query = GuidelineRevisionListQuery(guideline_id="guideline-1")

    assert query.limit == 50
    assert GUIDELINE_PAGE_LIMIT_MAX == 200
    assert (
        query.ordering
        == GUIDELINE_REVISION_ORDERING
        == (
            "revision_number DESC",
            "revision_id DESC",
        )
    )
    assert query.projection is PolicyProjection.SUMMARY
    assert len(query.filter_digest) == 64
    assert len(query.projection_digest) == 64
    GuidelineRevisionListQuery(guideline_id="guideline-1", limit=200)
    with pytest.raises(ValueError, match="guideline_page_limit_invalid"):
        GuidelineRevisionListQuery(guideline_id="guideline-1", limit=201)


def test_revision_cursor_round_trip_is_opaque_signed_and_kind_bound() -> None:
    query = GuidelineRevisionListQuery(
        guideline_id="guideline-private-id",
        projection=PolicyProjection.DETAIL,
    )
    source = _cursor(query)
    codec = PolicyCursorCodec(SIGNING_KEY)

    token = codec.encode(source)

    assert "guideline-private-id" not in token
    assert "revision-17" not in token
    assert codec.decode(token, expected_kind="revision") == source
    with pytest.raises(GuidelinePolicyContractError, match="invalid_cursor"):
        codec.decode(token, expected_kind="receipt")


def test_revision_cursor_rejects_tampering_and_noncanonical_tokens() -> None:
    query = GuidelineRevisionListQuery(guideline_id="guideline-1")
    codec = PolicyCursorCodec(SIGNING_KEY)
    token = codec.encode(_cursor(query))
    replacement = "A" if token[-1] != "A" else "B"

    with pytest.raises(GuidelinePolicyContractError, match="invalid_cursor"):
        codec.decode(token[:-1] + replacement, expected_kind="revision")
    with pytest.raises(GuidelinePolicyContractError, match="invalid_cursor"):
        codec.decode("not-a-policy-cursor", expected_kind="revision")


def test_revision_cursor_is_bound_to_guideline_and_arbitrary_filter_digest() -> None:
    original = GuidelineRevisionListQuery(guideline_id="guideline-1")
    cursor = _cursor(original)

    with pytest.raises(
        GuidelinePolicyInvalidCursor,
        match="guideline_revision_cursor_context_mismatch",
    ):
        GuidelineRevisionListQuery(
            guideline_id="guideline-2",
            cursor=cursor,
        )

    foreign_filter_cursor = GuidelineRevisionPageCursor(
        revision_number=cursor.revision_number,
        item_id=cursor.item_id,
        filter_digest="f" * 64,
        projection_digest=cursor.projection_digest,
    )
    with pytest.raises(
        GuidelinePolicyInvalidCursor,
        match="guideline_revision_cursor_context_mismatch",
    ):
        GuidelineRevisionListQuery(
            guideline_id="guideline-1",
            cursor=foreign_filter_cursor,
        )


def test_revision_cursor_is_bound_to_summary_or_detail_projection() -> None:
    summary = GuidelineRevisionListQuery(
        guideline_id="guideline-1",
        projection=PolicyProjection.SUMMARY,
    )
    detail = GuidelineRevisionListQuery(
        guideline_id="guideline-1",
        projection=PolicyProjection.DETAIL,
    )

    assert summary.filter_digest == detail.filter_digest
    assert summary.projection_digest != detail.projection_digest
    with pytest.raises(
        GuidelinePolicyInvalidCursor,
        match="guideline_revision_cursor_context_mismatch",
    ):
        GuidelineRevisionListQuery(
            guideline_id="guideline-1",
            projection=PolicyProjection.DETAIL,
            cursor=_cursor(summary),
        )


def test_revision_cursor_closes_schema_order_and_digest_contracts() -> None:
    query = GuidelineRevisionListQuery(guideline_id="guideline-1")
    kwargs = {
        "revision_number": 1,
        "item_id": "revision-1",
        "filter_digest": query.filter_digest,
        "projection_digest": query.projection_digest,
    }
    with pytest.raises(
        GuidelinePolicyContractError,
        match="policy_cursor_schema_version_invalid",
    ):
        GuidelineRevisionPageCursor(
            **kwargs,
            schema_version="policy-keyset/v0",
        )
    with pytest.raises(
        GuidelinePolicyContractError,
        match="guideline_revision_cursor_ordering_invalid",
    ):
        GuidelineRevisionPageCursor(
            **kwargs,
            ordering=("revision_number ASC", "revision_id ASC"),
        )
    assert _cursor(query).schema_version == POLICY_KEYSET_CONTRACT_VERSION


def test_rest_and_mcp_can_share_one_explicit_stable_secret() -> None:
    settings = SimpleNamespace(
        guideline_policy_cursor_signing_key=SecretStr(SIGNING_KEY.decode()),
    )
    rest_codec = policy_cursor_codec_from_settings(settings)
    mcp_codec = policy_cursor_codec_from_settings(settings)
    cursor = _cursor(GuidelineRevisionListQuery(guideline_id="guideline-1"))

    assert (
        mcp_codec.decode(
            rest_codec.encode(cursor),
            expected_kind="revision",
        )
        == cursor
    )


@pytest.mark.parametrize(
    "configured_value",
    [None, "", "short", b"also-short"],
)
def test_cursor_secret_configuration_fails_closed_without_fallback(
    configured_value: object,
) -> None:
    settings = SimpleNamespace(
        guideline_policy_cursor_signing_key=configured_value,
    )
    with pytest.raises(GuidelinePolicyCursorConfigurationError):
        policy_cursor_codec_from_settings(settings)
