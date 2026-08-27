"""Typed, backend-neutral failures for the logical transfer boundary.

Every refusal in this package is one of these.  Concrete backend exceptions
(Ladybug/Kùzu, Grafx, filesystem) must never reach a caller through the logical
transfer surface: an adapter that raises its own error type is expected to
translate it here, so orchestration can classify a failure without importing
the backend that produced it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


TransferPhase = Literal["write", "import", "checkpoint", "reopen"]
"""The finite set of phases a transfer failure can be attributed to."""

TRANSFER_PHASES: tuple[TransferPhase, ...] = (
    "write",
    "import",
    "checkpoint",
    "reopen",
)


@dataclass(slots=True)
class LogicalTransferError(Exception):
    """Structured failure raised anywhere on the logical transfer boundary."""

    code: str
    message: str
    detail: str | None = None

    def __str__(self) -> str:
        if self.detail:
            return f"{self.code}: {self.message} ({self.detail})"
        return f"{self.code}: {self.message}"


class LogicalFormatError(LogicalTransferError):
    """The artifact does not identify itself as a format this build can read."""

    def __init__(self, message: str, *, detail: str | None = None) -> None:
        super().__init__("logical_format", message, detail)


class UnsupportedFormatVersionError(LogicalFormatError):
    """The artifact names a format identifier this build does not implement."""

    def __init__(self, message: str, *, detail: str | None = None) -> None:
        LogicalTransferError.__init__(
            self, "unsupported_format_version", message, detail
        )


class UnsupportedFeatureError(LogicalFormatError):
    """The artifact requires a feature this build does not implement.

    Required features are refused rather than ignored.  A reader that skipped an
    unknown required feature would import a graph that silently means something
    other than what the writer recorded.
    """

    def __init__(self, message: str, *, detail: str | None = None) -> None:
        LogicalTransferError.__init__(self, "unsupported_feature", message, detail)


class LogicalArtifactError(LogicalTransferError):
    """The artifact is structurally unusable."""

    def __init__(self, message: str, *, detail: str | None = None) -> None:
        super().__init__("logical_artifact", message, detail)


class ArtifactMalformedError(LogicalArtifactError):
    """A record is not canonical, not decodable, or not shaped as declared."""

    def __init__(self, message: str, *, detail: str | None = None) -> None:
        LogicalTransferError.__init__(self, "artifact_malformed", message, detail)


class ArtifactSequenceError(LogicalArtifactError):
    """A record arrived outside the structural order the format fixes."""

    def __init__(self, message: str, *, detail: str | None = None) -> None:
        LogicalTransferError.__init__(self, "artifact_sequence", message, detail)


class ArtifactTruncatedError(LogicalArtifactError):
    """The stream ended before the terminal manifest.

    Truncation and a deliberate short write are indistinguishable without a
    terminal record, which is why the manifest is mandatory rather than
    advisory.
    """

    def __init__(self, message: str, *, detail: str | None = None) -> None:
        LogicalTransferError.__init__(self, "artifact_truncated", message, detail)


class ArtifactTrailingDataError(LogicalArtifactError):
    """Bytes followed the terminal manifest."""

    def __init__(self, message: str, *, detail: str | None = None) -> None:
        LogicalTransferError.__init__(self, "artifact_trailing_data", message, detail)


class ArtifactIntegrityError(LogicalArtifactError):
    """Declared counts or checksums disagree with what the stream carried."""

    def __init__(self, message: str, *, detail: str | None = None) -> None:
        LogicalTransferError.__init__(self, "artifact_integrity", message, detail)


class LogicalValueError(LogicalTransferError):
    """A value cannot be represented exactly in the logical value codec."""

    def __init__(self, message: str, *, detail: str | None = None) -> None:
        super().__init__("logical_value", message, detail)


class LogicalSchemaError(LogicalTransferError):
    """A logical schema or record contradicts the schema it is declared under."""

    def __init__(self, message: str, *, detail: str | None = None) -> None:
        super().__init__("logical_schema", message, detail)


@dataclass(slots=True)
class PhasedTransferError(LogicalTransferError):
    """A transfer failure that names the phase it happened in.

    The phase is what makes a failure actionable: the same underlying refusal
    means something different when it happens while reading the source than
    when it happens on the destination's cold reopen.

    Every failure a transfer can end on is one of these, so a caller classifies
    an outcome with a single ``isinstance`` instead of enumerating error types
    and hoping the list stayed complete.
    """

    phase: TransferPhase = "write"

    def __init__(
        self,
        code: str,
        message: str,
        *,
        phase: TransferPhase,
        detail: str | None = None,
    ) -> None:
        LogicalTransferError.__init__(self, code, message, detail)
        self.phase = phase

    def __str__(self) -> str:
        base = LogicalTransferError.__str__(self)
        return f"{base} [phase={self.phase}]"


class TransferFailedError(PhasedTransferError):
    """A transfer stopped in a named phase; the candidate was abandoned."""

    def __init__(
        self,
        message: str,
        *,
        phase: TransferPhase,
        detail: str | None = None,
    ) -> None:
        super().__init__("transfer_failed", message, phase=phase, detail=detail)


class CertificationRefusedError(PhasedTransferError):
    """The candidate was not certified, so the transfer does not report success.

    A missing certificate field is refused exactly like a divergent one.  An
    unproved claim and a disproved claim are the same thing to a caller deciding
    whether to trust a destination.

    It is a ``reopen`` failure: everything it judges is a claim about the
    candidate as re-read from cold, so a caller reading the phase learns where
    to look without having to special-case this type.
    """

    def __init__(
        self,
        message: str,
        *,
        detail: str | None = None,
        phase: TransferPhase = "reopen",
    ) -> None:
        super().__init__("certification_refused", message, phase=phase, detail=detail)


__all__ = [
    "TRANSFER_PHASES",
    "ArtifactIntegrityError",
    "ArtifactMalformedError",
    "ArtifactSequenceError",
    "ArtifactTrailingDataError",
    "ArtifactTruncatedError",
    "CertificationRefusedError",
    "LogicalArtifactError",
    "LogicalFormatError",
    "LogicalSchemaError",
    "LogicalTransferError",
    "LogicalValueError",
    "PhasedTransferError",
    "TransferFailedError",
    "TransferPhase",
    "UnsupportedFeatureError",
    "UnsupportedFormatVersionError",
]
