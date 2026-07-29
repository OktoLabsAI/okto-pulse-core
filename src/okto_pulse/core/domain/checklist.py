"""Pure A3 curated-checklist domain contracts.

The only template in v1 is the immutable ``/specify`` checklist.  The module
contains no template authoring surface, persistence dependency, or transport
concern.  Executions and receipts are bound to the exact Spec, template, and
board binding identities that produced them.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType
from typing import Final, Generic, TypeVar

from okto_pulse.core.domain.quality_canonicalization import (
    canonical_sha256,
    normative_manifest_digest_v1,
)

CHECKLIST_CONTRACT_VERSION = "checklist-execution/v1"
CHECKLIST_BINDING_CONTRACT_VERSION = "checklist-binding/v1"
SPECIFY_CHECKLIST_TEMPLATE_ID = "/specify"
SPECIFY_CHECKLIST_TEMPLATE_VERSION = "/specify/v1"
CHECKLIST_PAGE_LIMIT_MAX = 200

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class ChecklistContractError(ValueError):
    """A value violates the frozen A3 checklist contract."""

    def __init__(self, code: str, message: str | None = None) -> None:
        self.code = code
        self.message = message or code
        super().__init__(self.message)


def _required_text(value: str, code: str) -> str:
    if not isinstance(value, str):
        raise ChecklistContractError(code)
    normalized = value.strip()
    if not normalized:
        raise ChecklistContractError(code)
    return normalized


def _optional_text(value: str | None, code: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, code)


def _strict_positive_int(value: int, code: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ChecklistContractError(code)
    return value


def _strict_non_negative_int(value: int, code: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ChecklistContractError(code)
    return value


def _sha256(value: str, code: str) -> str:
    if not isinstance(value, str):
        raise ChecklistContractError(code)
    normalized = value.strip().lower()
    if not _SHA256_RE.fullmatch(normalized):
        raise ChecklistContractError(code)
    return normalized


def _aware_utc(value: datetime, code: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise ChecklistContractError(code)
    return value.astimezone(timezone.utc)


class ChecklistTargetType(str, Enum):
    SPEC = "spec"


class ChecklistPhase(str, Enum):
    SPEC_VALIDATION = "spec_validation"


class ChecklistMode(str, Enum):
    OFF = "off"
    ADVISORY = "advisory"
    BLOCKING = "blocking"


class ChecklistItemOutcome(str, Enum):
    PASS = "pass"
    FAIL = "fail"
    NOT_APPLICABLE = "not_applicable"


class ChecklistReceiptSource(str, Enum):
    NATIVE = "native"
    LEGACY_UNVERIFIED = "legacy_unverified"


class ChecklistExecutionStatus(str, Enum):
    OPEN = "open"
    SUBMITTED = "submitted"


class ChecklistStaleReason(str, Enum):
    SPEC_VERSION_CHANGED = "spec_version_changed"
    CONTENT_DIGEST_CHANGED = "content_digest_changed"
    INPUT_DIGEST_CHANGED = "input_digest_changed"
    TEMPLATE_VERSION_CHANGED = "template_version_changed"
    TEMPLATE_DIGEST_CHANGED = "template_digest_changed"
    BINDING_DIGEST_CHANGED = "binding_digest_changed"


CHECKLIST_STALE_REASON_ORDER: Final[tuple[ChecklistStaleReason, ...]] = (
    ChecklistStaleReason.SPEC_VERSION_CHANGED,
    ChecklistStaleReason.CONTENT_DIGEST_CHANGED,
    ChecklistStaleReason.INPUT_DIGEST_CHANGED,
    ChecklistStaleReason.TEMPLATE_VERSION_CHANGED,
    ChecklistStaleReason.TEMPLATE_DIGEST_CHANGED,
    ChecklistStaleReason.BINDING_DIGEST_CHANGED,
)


@dataclass(frozen=True, slots=True)
class ChecklistTemplateItem:
    item_id: str
    title_en: str
    title_pt: str
    description_en: str
    description_pt: str
    allow_na: bool

    def __post_init__(self) -> None:
        for field_name in (
            "item_id",
            "title_en",
            "title_pt",
            "description_en",
            "description_pt",
        ):
            value = getattr(self, field_name)
            normalized = _required_text(
                value,
                f"checklist_template_{field_name}_required",
            )
            if normalized != value:
                raise ChecklistContractError(
                    f"checklist_template_{field_name}_not_canonical"
                )
        if not isinstance(self.allow_na, bool):
            raise ChecklistContractError("checklist_template_allow_na_invalid")


SPECIFY_CHECKLIST_ITEMS_V1: Final[tuple[ChecklistTemplateItem, ...]] = (
    ChecklistTemplateItem(
        item_id="chk_scope_boundaries",
        title_en="Scope and boundaries",
        title_pt="Escopo e limites",
        description_en=(
            "The Spec explicitly defines in-scope behavior, out-of-scope "
            "behavior, and relevant boundaries."
        ),
        description_pt=(
            "A Spec define explicitamente comportamentos dentro e fora do "
            "escopo e seus limites relevantes."
        ),
        allow_na=False,
    ),
    ChecklistTemplateItem(
        item_id="chk_fr_value",
        title_en="Functional requirement value",
        title_pt="Valor dos requisitos funcionais",
        description_en=(
            "Each functional requirement states user or business value and "
            "observable behavior."
        ),
        description_pt=(
            "Cada requisito funcional declara valor para usuário ou negócio e "
            "comportamento observável."
        ),
        allow_na=False,
    ),
    ChecklistTemplateItem(
        item_id="chk_fr_ac_testable",
        title_en="Testable FR and AC coverage",
        title_pt="Cobertura testável de FR e AC",
        description_en=(
            "Functional requirements are covered by acceptance criteria that "
            "can be tested."
        ),
        description_pt=(
            "Os requisitos funcionais são cobertos por critérios de aceite que "
            "podem ser testados."
        ),
        allow_na=False,
    ),
    ChecklistTemplateItem(
        item_id="chk_ac_measurable",
        title_en="Measurable acceptance criteria",
        title_pt="Critérios de aceite mensuráveis",
        description_en=(
            "Acceptance criteria contain observable outcomes, explicit "
            "conditions, and measurable thresholds where relevant."
        ),
        description_pt=(
            "Os critérios de aceite contêm resultados observáveis, condições "
            "explícitas e limites mensuráveis quando pertinentes."
        ),
        allow_na=False,
    ),
    ChecklistTemplateItem(
        item_id="chk_edge_failures",
        title_en="Edge cases and failures",
        title_pt="Casos-limite e falhas",
        description_en=(
            "Relevant edge cases, invalid inputs, partial failures, and "
            "recovery behavior are specified."
        ),
        description_pt=(
            "Casos-limite, entradas inválidas, falhas parciais e comportamento "
            "de recuperação pertinentes estão especificados."
        ),
        allow_na=True,
    ),
    ChecklistTemplateItem(
        item_id="chk_dependencies_assumptions",
        title_en="Dependencies and assumptions",
        title_pt="Dependências e premissas",
        description_en=(
            "External dependencies, integration expectations, and assumptions "
            "are explicit and traceable."
        ),
        description_pt=(
            "Dependências externas, expectativas de integração e premissas "
            "estão explícitas e rastreáveis."
        ),
        allow_na=True,
    ),
    ChecklistTemplateItem(
        item_id="chk_no_placeholders",
        title_en="No unresolved placeholders",
        title_pt="Sem marcadores provisórios",
        description_en=(
            "Normative Spec content contains no unresolved placeholder such as "
            "TBD, TODO, TBC, or equivalent text."
        ),
        description_pt=(
            "O conteúdo normativo da Spec não contém marcadores não resolvidos "
            "como TBD, TODO, TBC ou texto equivalente."
        ),
        allow_na=False,
    ),
    ChecklistTemplateItem(
        item_id="chk_fr_tr_separation",
        title_en="FR and TR separation",
        title_pt="Separação entre FR e TR",
        description_en=(
            "Functional behavior and technical constraints are separated "
            "without losing their relationship."
        ),
        description_pt=(
            "Comportamento funcional e restrições técnicas estão separados sem "
            "perder sua relação."
        ),
        allow_na=True,
    ),
    ChecklistTemplateItem(
        item_id="chk_ids_traceability",
        title_en="Stable IDs and traceability",
        title_pt="IDs estáveis e rastreabilidade",
        description_en=(
            "Requirements, acceptance criteria, tests, and decisions use stable "
            "identities with complete traceability."
        ),
        description_pt=(
            "Requisitos, critérios de aceite, testes e decisões usam identidades "
            "estáveis com rastreabilidade completa."
        ),
        allow_na=False,
    ),
    ChecklistTemplateItem(
        item_id="chk_decisions_rationale",
        title_en="Decision rationale",
        title_pt="Justificativa das decisões",
        description_en=(
            "Material decisions record their rationale and relevant rejected "
            "alternatives."
        ),
        description_pt=(
            "Decisões materiais registram sua justificativa e alternativas "
            "rejeitadas pertinentes."
        ),
        allow_na=True,
    ),
)

SPECIFY_CHECKLIST_ITEM_IDS: Final[tuple[str, ...]] = tuple(
    item.item_id for item in SPECIFY_CHECKLIST_ITEMS_V1
)
SPECIFY_CHECKLIST_ITEM_ID_SET: Final[frozenset[str]] = frozenset(
    SPECIFY_CHECKLIST_ITEM_IDS
)
_SPECIFY_CHECKLIST_ITEM_BY_ID = MappingProxyType(
    {item.item_id: item for item in SPECIFY_CHECKLIST_ITEMS_V1}
)

SPECIFY_CHECKLIST_MANIFEST_V1 = MappingProxyType(
    {
        "template_id": SPECIFY_CHECKLIST_TEMPLATE_ID,
        "ordering": "normative_append_only",
        "items": tuple(
            MappingProxyType(
                {
                    "item_id": item.item_id,
                    "title_en": item.title_en,
                    "title_pt": item.title_pt,
                    "description_en": item.description_en,
                    "description_pt": item.description_pt,
                    "allow_na": item.allow_na,
                }
            )
            for item in SPECIFY_CHECKLIST_ITEMS_V1
        ),
    }
)
SPECIFY_CHECKLIST_TEMPLATE_DIGEST = normative_manifest_digest_v1(
    namespace="checklist_template",
    version=SPECIFY_CHECKLIST_TEMPLATE_VERSION,
    manifest=SPECIFY_CHECKLIST_MANIFEST_V1,
)


@dataclass(frozen=True, slots=True)
class ChecklistTemplate:
    template_id: str
    version: str
    digest: str
    items: tuple[ChecklistTemplateItem, ...]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "template_id",
            _required_text(
                self.template_id,
                "checklist_template_id_required",
            ),
        )
        object.__setattr__(
            self,
            "version",
            _required_text(
                self.version,
                "checklist_template_version_required",
            ),
        )
        object.__setattr__(
            self,
            "digest",
            _sha256(self.digest, "checklist_template_digest_invalid"),
        )
        if not isinstance(self.items, tuple | list):
            raise ChecklistContractError("checklist_template_items_invalid")
        items = tuple(self.items)
        if any(not isinstance(item, ChecklistTemplateItem) for item in items):
            raise ChecklistContractError("checklist_template_items_invalid")
        if len({item.item_id for item in items}) != len(items):
            raise ChecklistContractError("checklist_template_item_duplicate")
        object.__setattr__(self, "items", items)


SPECIFY_CHECKLIST_TEMPLATE_V1 = ChecklistTemplate(
    template_id=SPECIFY_CHECKLIST_TEMPLATE_ID,
    version=SPECIFY_CHECKLIST_TEMPLATE_VERSION,
    digest=SPECIFY_CHECKLIST_TEMPLATE_DIGEST,
    items=SPECIFY_CHECKLIST_ITEMS_V1,
)


def require_specify_checklist_item(item_id: str) -> ChecklistTemplateItem:
    """Resolve an exact v1 item ID without aliases or normalization."""

    if not isinstance(item_id, str) or not item_id or item_id != item_id.strip():
        raise ChecklistContractError("checklist_item_id_invalid")
    try:
        return _SPECIFY_CHECKLIST_ITEM_BY_ID[item_id]
    except KeyError as exc:
        raise ChecklistContractError("checklist_item_unknown") from exc


def checklist_binding_digest_v1(
    *,
    board_id: str,
    target_type: ChecklistTargetType,
    phase: ChecklistPhase,
    template_version: str,
) -> str:
    """Hash the executable identity selected by one board binding.

    ``mode`` and the monotonically increasing binding record version are
    governance/CAS state.  They deliberately do not participate in the
    executable identity: promoting an already-executed checklist from advisory
    to blocking must not invalidate its receipt.  Repinning the immutable
    template still changes this digest through ``template_version``.
    """

    return normative_manifest_digest_v1(
        namespace="checklist_binding",
        version=CHECKLIST_BINDING_CONTRACT_VERSION,
        manifest={
            "board_id": board_id,
            "target_type": target_type,
            "phase": phase,
            "template_version": template_version,
        },
    )


@dataclass(frozen=True, slots=True)
class ChecklistBinding:
    board_id: str
    mode: ChecklistMode
    version: int
    revision: int | None = None
    target_type: ChecklistTargetType = ChecklistTargetType.SPEC
    phase: ChecklistPhase = ChecklistPhase.SPEC_VALIDATION
    template_version: str = SPECIFY_CHECKLIST_TEMPLATE_VERSION
    digest: str | None = None

    def __post_init__(self) -> None:
        board_id = _required_text(
            self.board_id,
            "checklist_binding_board_id_required",
        )
        if not isinstance(self.mode, ChecklistMode):
            raise ChecklistContractError("checklist_binding_mode_invalid")
        version = _strict_positive_int(
            self.version,
            "checklist_binding_version_invalid",
        )
        revision = (
            version
            if self.revision is None
            else _strict_non_negative_int(
                self.revision,
                "checklist_binding_revision_invalid",
            )
        )
        if revision not in (0, version):
            raise ChecklistContractError("checklist_binding_revision_invalid")
        if revision == 0 and (
            self.mode is not ChecklistMode.OFF or version != 1
        ):
            raise ChecklistContractError("checklist_binding_synthetic_invalid")
        if self.target_type is not ChecklistTargetType.SPEC:
            raise ChecklistContractError("checklist_target_type_unsupported")
        if self.phase is not ChecklistPhase.SPEC_VALIDATION:
            raise ChecklistContractError("checklist_phase_unsupported")
        if self.template_version != SPECIFY_CHECKLIST_TEMPLATE_VERSION:
            raise ChecklistContractError("checklist_template_version_unsupported")
        expected_digest = checklist_binding_digest_v1(
            board_id=board_id,
            target_type=self.target_type,
            phase=self.phase,
            template_version=self.template_version,
        )
        if self.digest is not None:
            provided_digest = _sha256(
                self.digest,
                "checklist_binding_digest_invalid",
            )
            if provided_digest != expected_digest:
                raise ChecklistContractError("checklist_binding_digest_mismatch")
        object.__setattr__(self, "board_id", board_id)
        object.__setattr__(self, "version", version)
        object.__setattr__(self, "revision", revision)
        object.__setattr__(self, "digest", expected_digest)

    @classmethod
    def synthetic_off(cls, *, board_id: str) -> ChecklistBinding:
        """Project a legacy board with no persisted binding head.

        The effective contract remains OFF at binding version 1, while
        ``revision=0`` exposes the real initial CAS revision.  A persisted
        OFF/v1 row has ``revision=1`` and is therefore unambiguous.
        """

        return cls(
            board_id=board_id,
            mode=ChecklistMode.OFF,
            version=1,
            revision=0,
        )

    @property
    def is_synthetic(self) -> bool:
        return self.revision == 0


@dataclass(frozen=True, slots=True)
class ChecklistSpecSnapshot:
    board_id: str
    spec_id: str
    spec_version: int
    content_digest: str
    input_digest: str
    status: str
    archived: bool = False

    def __post_init__(self) -> None:
        for field_name in ("board_id", "spec_id", "status"):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"checklist_{field_name}_required",
                ),
            )
        object.__setattr__(
            self,
            "spec_version",
            _strict_positive_int(
                self.spec_version,
                "checklist_spec_version_invalid",
            ),
        )
        for field_name in ("content_digest", "input_digest"):
            object.__setattr__(
                self,
                field_name,
                _sha256(
                    getattr(self, field_name),
                    f"checklist_{field_name}_invalid",
                ),
            )
        if not isinstance(self.archived, bool):
            raise ChecklistContractError("checklist_spec_archived_invalid")


@dataclass(frozen=True, slots=True)
class ChecklistPreflight:
    subject: ChecklistSpecSnapshot
    binding: ChecklistBinding
    current_head_revision: int
    current_head_receipt_id: str | None

    def __post_init__(self) -> None:
        if not isinstance(self.subject, ChecklistSpecSnapshot):
            raise ChecklistContractError("checklist_preflight_subject_invalid")
        if not isinstance(self.binding, ChecklistBinding):
            raise ChecklistContractError("checklist_preflight_binding_invalid")
        if self.binding.board_id != self.subject.board_id:
            raise ChecklistContractError("checklist_preflight_board_mismatch")
        revision = _strict_non_negative_int(
            self.current_head_revision,
            "checklist_head_revision_invalid",
        )
        receipt_id = _optional_text(
            self.current_head_receipt_id,
            "checklist_head_receipt_id_invalid",
        )
        if (revision == 0) != (receipt_id is None):
            raise ChecklistContractError("checklist_head_identity_mismatch")
        object.__setattr__(self, "current_head_revision", revision)
        object.__setattr__(self, "current_head_receipt_id", receipt_id)


@dataclass(frozen=True, slots=True)
class ChecklistItemResult:
    item_id: str
    outcome: ChecklistItemOutcome
    anchor: str
    rationale: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "item_id",
            _required_text(self.item_id, "checklist_item_id_required"),
        )
        if not isinstance(self.outcome, ChecklistItemOutcome):
            raise ChecklistContractError("checklist_item_outcome_invalid")
        object.__setattr__(
            self,
            "anchor",
            _required_text(self.anchor, "checklist_item_anchor_required"),
        )
        rationale = _optional_text(
            self.rationale,
            "checklist_item_rationale_invalid",
        )
        if self.outcome is ChecklistItemOutcome.NOT_APPLICABLE and rationale is None:
            raise ChecklistContractError("checklist_item_na_rationale_required")
        object.__setattr__(self, "rationale", rationale)


@dataclass(frozen=True, slots=True)
class ChecklistExecution:
    """Frozen, server-issued A3 execution created before item submission."""

    id: str
    board_id: str
    spec_id: str
    spec_version: int
    content_digest: str
    input_digest: str
    template_version: str
    template_digest: str
    binding_version: int
    binding_digest: str
    binding_mode: ChecklistMode
    request_digest: str
    idempotency_key: str
    created_by: str
    created_at: datetime
    revision: int = 1
    status: ChecklistExecutionStatus = ChecklistExecutionStatus.OPEN
    receipt_id: str | None = None

    def __post_init__(self) -> None:
        for field_name in (
            "id",
            "board_id",
            "spec_id",
            "template_version",
            "idempotency_key",
            "created_by",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"checklist_execution_{field_name}_required",
                ),
            )
        object.__setattr__(
            self,
            "spec_version",
            _strict_positive_int(
                self.spec_version,
                "checklist_spec_version_invalid",
            ),
        )
        object.__setattr__(
            self,
            "binding_version",
            _strict_positive_int(
                self.binding_version,
                "checklist_binding_version_invalid",
            ),
        )
        object.__setattr__(
            self,
            "revision",
            _strict_positive_int(
                self.revision,
                "checklist_execution_revision_invalid",
            ),
        )
        for field_name in (
            "content_digest",
            "input_digest",
            "template_digest",
            "binding_digest",
            "request_digest",
        ):
            object.__setattr__(
                self,
                field_name,
                _sha256(
                    getattr(self, field_name),
                    f"checklist_{field_name}_invalid",
                ),
            )
        if not isinstance(self.binding_mode, ChecklistMode):
            raise ChecklistContractError("checklist_binding_mode_invalid")
        if not isinstance(self.status, ChecklistExecutionStatus):
            raise ChecklistContractError("checklist_execution_status_invalid")
        receipt_id = _optional_text(
            self.receipt_id,
            "checklist_execution_receipt_id_invalid",
        )
        if self.status is ChecklistExecutionStatus.OPEN and receipt_id is not None:
            raise ChecklistContractError("checklist_execution_open_receipt_forbidden")
        if self.status is ChecklistExecutionStatus.SUBMITTED and receipt_id is None:
            raise ChecklistContractError("checklist_execution_receipt_required")
        object.__setattr__(self, "receipt_id", receipt_id)
        object.__setattr__(
            self,
            "created_at",
            _aware_utc(
                self.created_at,
                "checklist_execution_created_at_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class ChecklistExecutionStartResult:
    execution: ChecklistExecution
    replayed: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.execution, ChecklistExecution):
            raise ChecklistContractError("checklist_execution_result_invalid")
        if not isinstance(self.replayed, bool):
            raise ChecklistContractError("checklist_replayed_flag_invalid")


@dataclass(frozen=True, slots=True)
class ChecklistSubmission:
    board_id: str
    spec_id: str
    spec_version: int
    content_digest: str
    input_digest: str
    template_version: str
    template_digest: str
    binding_version: int
    binding_digest: str
    expected_head_revision: int
    items: tuple[ChecklistItemResult, ...] = ()
    idempotency_key: str | None = None
    manual_checklist_ref: str | None = None

    def __post_init__(self) -> None:
        for field_name in ("board_id", "spec_id", "template_version"):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"checklist_{field_name}_required",
                ),
            )
        object.__setattr__(
            self,
            "spec_version",
            _strict_positive_int(
                self.spec_version,
                "checklist_spec_version_invalid",
            ),
        )
        object.__setattr__(
            self,
            "binding_version",
            _strict_positive_int(
                self.binding_version,
                "checklist_binding_version_invalid",
            ),
        )
        object.__setattr__(
            self,
            "expected_head_revision",
            _strict_non_negative_int(
                self.expected_head_revision,
                "checklist_head_revision_invalid",
            ),
        )
        for field_name in (
            "content_digest",
            "input_digest",
            "template_digest",
            "binding_digest",
        ):
            object.__setattr__(
                self,
                field_name,
                _sha256(
                    getattr(self, field_name),
                    f"checklist_{field_name}_invalid",
                ),
            )
        if not isinstance(self.items, tuple | list):
            raise ChecklistContractError("checklist_items_invalid")
        items = tuple(self.items)
        if any(not isinstance(item, ChecklistItemResult) for item in items):
            raise ChecklistContractError("checklist_items_invalid")
        object.__setattr__(self, "items", items)
        idempotency_key = _optional_text(
            self.idempotency_key,
            "checklist_idempotency_key_invalid",
        )
        manual_ref = _optional_text(
            self.manual_checklist_ref,
            "manual_checklist_ref_invalid",
        )
        if manual_ref is None and idempotency_key is None:
            raise ChecklistContractError("checklist_idempotency_key_required")
        if manual_ref is not None:
            if idempotency_key is not None:
                raise ChecklistContractError("manual_checklist_non_replayable")
            if items:
                raise ChecklistContractError("manual_checklist_items_forbidden")
        object.__setattr__(self, "idempotency_key", idempotency_key)
        object.__setattr__(self, "manual_checklist_ref", manual_ref)

    @property
    def is_legacy_manual(self) -> bool:
        return self.manual_checklist_ref is not None


@dataclass(frozen=True, slots=True)
class ChecklistReceipt:
    id: str
    board_id: str
    spec_id: str
    spec_version: int
    content_digest: str
    input_digest: str
    template_version: str
    template_digest: str
    binding_version: int
    binding_digest: str
    binding_mode: ChecklistMode
    items: tuple[ChecklistItemResult, ...]
    source: ChecklistReceiptSource
    request_digest: str
    created_by: str
    created_at: datetime
    head_revision: int
    idempotency_key: str | None = None
    manual_checklist_ref: str | None = None
    predecessor_receipt_id: str | None = None

    def __post_init__(self) -> None:
        for field_name in (
            "id",
            "board_id",
            "spec_id",
            "template_version",
            "created_by",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"checklist_receipt_{field_name}_required",
                ),
            )
        object.__setattr__(
            self,
            "spec_version",
            _strict_positive_int(
                self.spec_version,
                "checklist_spec_version_invalid",
            ),
        )
        object.__setattr__(
            self,
            "binding_version",
            _strict_positive_int(
                self.binding_version,
                "checklist_binding_version_invalid",
            ),
        )
        object.__setattr__(
            self,
            "head_revision",
            _strict_positive_int(
                self.head_revision,
                "checklist_head_revision_invalid",
            ),
        )
        for field_name in (
            "content_digest",
            "input_digest",
            "template_digest",
            "binding_digest",
            "request_digest",
        ):
            object.__setattr__(
                self,
                field_name,
                _sha256(
                    getattr(self, field_name),
                    f"checklist_{field_name}_invalid",
                ),
            )
        if not isinstance(self.binding_mode, ChecklistMode):
            raise ChecklistContractError("checklist_binding_mode_invalid")
        if not isinstance(self.source, ChecklistReceiptSource):
            raise ChecklistContractError("checklist_receipt_source_invalid")
        if not isinstance(self.items, tuple | list):
            raise ChecklistContractError("checklist_items_invalid")
        items = tuple(self.items)
        if any(not isinstance(item, ChecklistItemResult) for item in items):
            raise ChecklistContractError("checklist_items_invalid")
        idempotency_key = _optional_text(
            self.idempotency_key,
            "checklist_idempotency_key_invalid",
        )
        manual_ref = _optional_text(
            self.manual_checklist_ref,
            "manual_checklist_ref_invalid",
        )
        predecessor = _optional_text(
            self.predecessor_receipt_id,
            "checklist_predecessor_receipt_id_invalid",
        )
        if self.source is ChecklistReceiptSource.NATIVE:
            if tuple(item.item_id for item in items) != SPECIFY_CHECKLIST_ITEM_IDS:
                raise ChecklistContractError("checklist_items_incomplete")
            for result in items:
                template_item = require_specify_checklist_item(result.item_id)
                if (
                    result.outcome is ChecklistItemOutcome.NOT_APPLICABLE
                    and not template_item.allow_na
                ):
                    raise ChecklistContractError("checklist_item_na_not_allowed")
            if idempotency_key is None:
                raise ChecklistContractError("checklist_idempotency_key_required")
            if manual_ref is not None:
                raise ChecklistContractError("native_checklist_manual_ref_forbidden")
        else:
            if items:
                raise ChecklistContractError("manual_checklist_items_forbidden")
            if manual_ref is None:
                raise ChecklistContractError("manual_checklist_ref_required")
            if idempotency_key is not None:
                raise ChecklistContractError("manual_checklist_non_replayable")
        object.__setattr__(self, "items", items)
        object.__setattr__(self, "idempotency_key", idempotency_key)
        object.__setattr__(self, "manual_checklist_ref", manual_ref)
        object.__setattr__(self, "predecessor_receipt_id", predecessor)
        object.__setattr__(
            self,
            "created_at",
            _aware_utc(
                self.created_at,
                "checklist_receipt_created_at_invalid",
            ),
        )

    @property
    def verified(self) -> bool:
        return self.source is ChecklistReceiptSource.NATIVE

    @property
    def replayable(self) -> bool:
        return self.source is ChecklistReceiptSource.NATIVE

    @property
    def blocking_satisfied(self) -> bool:
        return self.verified and all(
            item.outcome is not ChecklistItemOutcome.FAIL for item in self.items
        )


@dataclass(frozen=True, slots=True)
class ChecklistExecutionHead:
    board_id: str
    spec_id: str
    phase: ChecklistPhase
    receipt_id: str
    revision: int
    updated_at: datetime

    def __post_init__(self) -> None:
        for field_name in ("board_id", "spec_id", "receipt_id"):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"checklist_head_{field_name}_required",
                ),
            )
        if self.phase is not ChecklistPhase.SPEC_VALIDATION:
            raise ChecklistContractError("checklist_phase_unsupported")
        object.__setattr__(
            self,
            "revision",
            _strict_positive_int(
                self.revision,
                "checklist_head_revision_invalid",
            ),
        )
        object.__setattr__(
            self,
            "updated_at",
            _aware_utc(
                self.updated_at,
                "checklist_head_updated_at_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class ChecklistWriteBundle:
    request_digest: str
    expected_spec_version: int
    expected_content_digest: str
    expected_input_digest: str
    expected_template_version: str
    expected_template_digest: str
    expected_binding_version: int
    expected_binding_digest: str
    expected_binding_mode: ChecklistMode
    expected_head_revision: int
    expected_head_receipt_id: str | None
    expected_spec_status: str
    expected_spec_archived: bool
    receipt: ChecklistReceipt
    next_head: ChecklistExecutionHead

    def __post_init__(self) -> None:
        if not isinstance(self.receipt, ChecklistReceipt):
            raise ChecklistContractError("checklist_bundle_receipt_invalid")
        if not isinstance(self.next_head, ChecklistExecutionHead):
            raise ChecklistContractError("checklist_bundle_head_invalid")
        request_digest = _sha256(
            self.request_digest,
            "checklist_request_digest_invalid",
        )
        for field_name in (
            "expected_content_digest",
            "expected_input_digest",
            "expected_template_digest",
            "expected_binding_digest",
        ):
            object.__setattr__(
                self,
                field_name,
                _sha256(
                    getattr(self, field_name),
                    f"checklist_{field_name}_invalid",
                ),
            )
        object.__setattr__(
            self,
            "expected_spec_version",
            _strict_positive_int(
                self.expected_spec_version,
                "checklist_spec_version_invalid",
            ),
        )
        object.__setattr__(
            self,
            "expected_binding_version",
            _strict_positive_int(
                self.expected_binding_version,
                "checklist_binding_version_invalid",
            ),
        )
        head_revision = _strict_non_negative_int(
            self.expected_head_revision,
            "checklist_head_revision_invalid",
        )
        head_receipt_id = _optional_text(
            self.expected_head_receipt_id,
            "checklist_head_receipt_id_invalid",
        )
        if (head_revision == 0) != (head_receipt_id is None):
            raise ChecklistContractError("checklist_head_identity_mismatch")
        template_version = _required_text(
            self.expected_template_version,
            "checklist_template_version_required",
        )
        status = _required_text(
            self.expected_spec_status,
            "checklist_spec_status_required",
        )
        if not isinstance(self.expected_binding_mode, ChecklistMode):
            raise ChecklistContractError("checklist_binding_mode_invalid")
        if not isinstance(self.expected_spec_archived, bool):
            raise ChecklistContractError("checklist_spec_archived_invalid")
        if self.expected_spec_archived:
            raise ChecklistContractError("checklist_archived_spec_forbidden")
        receipt = self.receipt
        head = self.next_head
        if (
            receipt.request_digest != request_digest
            or receipt.spec_version != self.expected_spec_version
            or receipt.content_digest != self.expected_content_digest
            or receipt.input_digest != self.expected_input_digest
            or receipt.template_version != template_version
            or receipt.template_digest != self.expected_template_digest
            or receipt.binding_version != self.expected_binding_version
            or receipt.binding_digest != self.expected_binding_digest
            or receipt.binding_mode is not self.expected_binding_mode
            or receipt.predecessor_receipt_id != head_receipt_id
        ):
            raise ChecklistContractError("checklist_bundle_fence_mismatch")
        if (
            head.board_id != receipt.board_id
            or head.spec_id != receipt.spec_id
            or head.receipt_id != receipt.id
            or head.revision != head_revision + 1
            or receipt.head_revision != head.revision
        ):
            raise ChecklistContractError("checklist_bundle_head_mismatch")
        object.__setattr__(self, "request_digest", request_digest)
        object.__setattr__(
            self,
            "expected_head_revision",
            head_revision,
        )
        object.__setattr__(
            self,
            "expected_head_receipt_id",
            head_receipt_id,
        )
        object.__setattr__(
            self,
            "expected_template_version",
            template_version,
        )
        object.__setattr__(self, "expected_spec_status", status)


@dataclass(frozen=True, slots=True)
class ChecklistCommitResult:
    board_id: str
    spec_id: str
    spec_version: int
    receipt_id: str
    request_digest: str
    head_revision: int
    replayed: bool = False

    def __post_init__(self) -> None:
        for field_name in ("board_id", "spec_id", "receipt_id"):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"checklist_commit_{field_name}_required",
                ),
            )
        object.__setattr__(
            self,
            "spec_version",
            _strict_positive_int(
                self.spec_version,
                "checklist_spec_version_invalid",
            ),
        )
        object.__setattr__(
            self,
            "request_digest",
            _sha256(
                self.request_digest,
                "checklist_request_digest_invalid",
            ),
        )
        object.__setattr__(
            self,
            "head_revision",
            _strict_positive_int(
                self.head_revision,
                "checklist_head_revision_invalid",
            ),
        )
        if not isinstance(self.replayed, bool):
            raise ChecklistContractError("checklist_replayed_flag_invalid")


@dataclass(frozen=True, slots=True)
class ChecklistCurrentness:
    current: bool
    stale_reasons: tuple[ChecklistStaleReason, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.current, bool):
            raise ChecklistContractError("checklist_current_flag_invalid")
        if not isinstance(self.stale_reasons, tuple | list):
            raise ChecklistContractError("checklist_stale_reasons_invalid")
        reasons = tuple(self.stale_reasons)
        if any(not isinstance(reason, ChecklistStaleReason) for reason in reasons):
            raise ChecklistContractError("checklist_stale_reasons_invalid")
        if len(set(reasons)) != len(reasons):
            raise ChecklistContractError("checklist_stale_reasons_duplicate")
        expected = tuple(
            reason for reason in CHECKLIST_STALE_REASON_ORDER if reason in reasons
        )
        if reasons != expected or self.current == bool(reasons):
            raise ChecklistContractError("checklist_currentness_inconsistent")
        object.__setattr__(self, "stale_reasons", reasons)


@dataclass(frozen=True, slots=True)
class ChecklistGateDecision:
    mode: ChecklistMode
    allowed: bool
    reason: str
    currentness: ChecklistCurrentness | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.mode, ChecklistMode):
            raise ChecklistContractError("checklist_binding_mode_invalid")
        if not isinstance(self.allowed, bool):
            raise ChecklistContractError("checklist_gate_allowed_invalid")
        object.__setattr__(
            self,
            "reason",
            _required_text(self.reason, "checklist_gate_reason_required"),
        )
        if self.currentness is not None and not isinstance(
            self.currentness,
            ChecklistCurrentness,
        ):
            raise ChecklistContractError("checklist_currentness_invalid")


@dataclass(frozen=True, slots=True)
class ChecklistReceiptView:
    receipt: ChecklistReceipt
    is_head: bool
    currentness: ChecklistCurrentness
    gate: ChecklistGateDecision

    def __post_init__(self) -> None:
        if not isinstance(self.receipt, ChecklistReceipt):
            raise ChecklistContractError("checklist_receipt_invalid")
        if not isinstance(self.is_head, bool):
            raise ChecklistContractError("checklist_is_head_invalid")
        if not isinstance(self.currentness, ChecklistCurrentness):
            raise ChecklistContractError("checklist_currentness_invalid")
        if not isinstance(self.gate, ChecklistGateDecision):
            raise ChecklistContractError("checklist_gate_invalid")


_PageItemT = TypeVar("_PageItemT")


@dataclass(frozen=True, slots=True)
class ChecklistPage(Generic[_PageItemT]):
    items: tuple[_PageItemT, ...]
    total: int
    offset: int
    limit: int

    def __post_init__(self) -> None:
        if not isinstance(self.items, tuple | list):
            raise ChecklistContractError("checklist_page_items_invalid")
        items = tuple(self.items)
        total = _strict_non_negative_int(
            self.total,
            "checklist_page_total_invalid",
        )
        offset = _strict_non_negative_int(
            self.offset,
            "checklist_page_offset_invalid",
        )
        limit = _strict_positive_int(
            self.limit,
            "checklist_page_limit_invalid",
        )
        if limit > CHECKLIST_PAGE_LIMIT_MAX:
            raise ChecklistContractError("checklist_page_limit_invalid")
        if len(items) > limit:
            raise ChecklistContractError("checklist_page_item_count_exceeds_limit")
        expected_count = min(limit, max(total - offset, 0))
        if len(items) != expected_count:
            raise ChecklistContractError("checklist_page_underfilled")
        object.__setattr__(self, "items", items)
        object.__setattr__(self, "total", total)
        object.__setattr__(self, "offset", offset)
        object.__setattr__(self, "limit", limit)


def checklist_submission_digest_v1(
    submission: ChecklistSubmission,
    *,
    actor_id: str,
) -> str:
    """Hash immutable inbound command data for native idempotency and audit."""

    if not isinstance(submission, ChecklistSubmission):
        raise ChecklistContractError("checklist_submission_invalid")
    actor = _required_text(actor_id, "checklist_actor_required")
    normative_index = {
        item_id: index for index, item_id in enumerate(SPECIFY_CHECKLIST_ITEM_IDS)
    }
    ordered_items = tuple(
        sorted(
            submission.items,
            key=lambda item: (
                normative_index.get(item.item_id, len(normative_index)),
                item.item_id,
            ),
        )
    )
    return canonical_sha256(
        {
            "contract_version": CHECKLIST_CONTRACT_VERSION,
            "actor_id": actor,
            "board_id": submission.board_id,
            "spec_id": submission.spec_id,
            "spec_version": submission.spec_version,
            "content_digest": submission.content_digest,
            "input_digest": submission.input_digest,
            "template_version": submission.template_version,
            "template_digest": submission.template_digest,
            "binding_digest": submission.binding_digest,
            "expected_head_revision": submission.expected_head_revision,
            "items": (
                tuple(
                    {
                        "item_id": item.item_id,
                        "outcome": item.outcome,
                        "anchor": item.anchor,
                        "rationale": item.rationale,
                    }
                    for item in ordered_items
                )
            ),
            "idempotency_key": submission.idempotency_key,
            "manual_checklist_ref": submission.manual_checklist_ref,
        }
    )


def checklist_execution_request_digest_v1(
    *,
    board_id: str,
    spec_id: str,
    spec_version: int,
    content_digest: str,
    input_digest: str,
    template_version: str,
    template_digest: str,
    binding_digest: str,
    actor_id: str,
) -> str:
    """Hash the semantic identity of one execution-start request.

    The immutable execution still records the observed binding version and
    mode for audit.  Neither is part of the idempotency identity because a
    governance-only mode revision does not change what the checklist executes.
    """

    return canonical_sha256(
        {
            "contract_version": CHECKLIST_CONTRACT_VERSION,
            "operation": "start",
            "board_id": board_id,
            "spec_id": spec_id,
            "spec_version": spec_version,
            "content_digest": content_digest,
            "input_digest": input_digest,
            "template_version": template_version,
            "template_digest": template_digest,
            "binding_digest": binding_digest,
            "actor_id": actor_id,
        }
    )


def evaluate_checklist_currentness(
    receipt: ChecklistReceipt,
    *,
    current_subject: ChecklistSpecSnapshot,
    current_binding: ChecklistBinding,
    current_template: ChecklistTemplate = SPECIFY_CHECKLIST_TEMPLATE_V1,
) -> ChecklistCurrentness:
    """Evaluate all version/digest fences required by the A3 receipt."""

    if not isinstance(receipt, ChecklistReceipt):
        raise ChecklistContractError("checklist_receipt_invalid")
    if not isinstance(current_subject, ChecklistSpecSnapshot):
        raise ChecklistContractError("checklist_subject_invalid")
    if not isinstance(current_binding, ChecklistBinding):
        raise ChecklistContractError("checklist_binding_invalid")
    if not isinstance(current_template, ChecklistTemplate):
        raise ChecklistContractError("checklist_template_invalid")
    if (
        receipt.board_id != current_subject.board_id
        or receipt.spec_id != current_subject.spec_id
        or current_binding.board_id != current_subject.board_id
    ):
        raise ChecklistContractError("checklist_subject_mismatch")

    reasons: list[ChecklistStaleReason] = []
    comparisons = (
        (
            receipt.spec_version,
            current_subject.spec_version,
            ChecklistStaleReason.SPEC_VERSION_CHANGED,
        ),
        (
            receipt.content_digest,
            current_subject.content_digest,
            ChecklistStaleReason.CONTENT_DIGEST_CHANGED,
        ),
        (
            receipt.input_digest,
            current_subject.input_digest,
            ChecklistStaleReason.INPUT_DIGEST_CHANGED,
        ),
        (
            receipt.template_version,
            current_template.version,
            ChecklistStaleReason.TEMPLATE_VERSION_CHANGED,
        ),
        (
            receipt.template_digest,
            current_template.digest,
            ChecklistStaleReason.TEMPLATE_DIGEST_CHANGED,
        ),
        (
            receipt.binding_digest,
            current_binding.digest,
            ChecklistStaleReason.BINDING_DIGEST_CHANGED,
        ),
    )
    reasons.extend(
        reason for previous, current, reason in comparisons if previous != current
    )
    ordered = tuple(
        reason for reason in CHECKLIST_STALE_REASON_ORDER if reason in reasons
    )
    return ChecklistCurrentness(current=not ordered, stale_reasons=ordered)


def evaluate_checklist_gate(
    *,
    binding: ChecklistBinding,
    current_subject: ChecklistSpecSnapshot,
    receipt: ChecklistReceipt | None,
    current_template: ChecklistTemplate = SPECIFY_CHECKLIST_TEMPLATE_V1,
) -> ChecklistGateDecision:
    """Project gate behavior without conflating advisory evidence with blocking."""

    if not isinstance(binding, ChecklistBinding):
        raise ChecklistContractError("checklist_binding_invalid")
    if not isinstance(current_subject, ChecklistSpecSnapshot):
        raise ChecklistContractError("checklist_subject_invalid")
    if receipt is not None and not isinstance(receipt, ChecklistReceipt):
        raise ChecklistContractError("checklist_receipt_invalid")

    currentness = (
        evaluate_checklist_currentness(
            receipt,
            current_subject=current_subject,
            current_binding=binding,
            current_template=current_template,
        )
        if receipt is not None
        else None
    )
    if binding.mode is ChecklistMode.OFF:
        return ChecklistGateDecision(
            mode=binding.mode,
            allowed=True,
            reason="checklist_off",
            currentness=currentness,
        )
    if binding.mode is ChecklistMode.ADVISORY:
        return ChecklistGateDecision(
            mode=binding.mode,
            allowed=True,
            reason="checklist_advisory",
            currentness=currentness,
        )
    if receipt is None:
        return ChecklistGateDecision(
            mode=binding.mode,
            allowed=False,
            reason="checklist_receipt_required",
        )
    assert currentness is not None
    if not currentness.current:
        return ChecklistGateDecision(
            mode=binding.mode,
            allowed=False,
            reason="checklist_receipt_stale",
            currentness=currentness,
        )
    if not receipt.verified:
        return ChecklistGateDecision(
            mode=binding.mode,
            allowed=False,
            reason="manual_checklist_legacy_unverified",
            currentness=currentness,
        )
    if not receipt.blocking_satisfied:
        return ChecklistGateDecision(
            mode=binding.mode,
            allowed=False,
            reason="checklist_item_failed",
            currentness=currentness,
        )
    return ChecklistGateDecision(
        mode=binding.mode,
        allowed=True,
        reason="checklist_satisfied",
        currentness=currentness,
    )


__all__ = [
    "CHECKLIST_BINDING_CONTRACT_VERSION",
    "CHECKLIST_CONTRACT_VERSION",
    "CHECKLIST_PAGE_LIMIT_MAX",
    "CHECKLIST_STALE_REASON_ORDER",
    "SPECIFY_CHECKLIST_ITEM_IDS",
    "SPECIFY_CHECKLIST_ITEM_ID_SET",
    "SPECIFY_CHECKLIST_ITEMS_V1",
    "SPECIFY_CHECKLIST_MANIFEST_V1",
    "SPECIFY_CHECKLIST_TEMPLATE_DIGEST",
    "SPECIFY_CHECKLIST_TEMPLATE_ID",
    "SPECIFY_CHECKLIST_TEMPLATE_V1",
    "SPECIFY_CHECKLIST_TEMPLATE_VERSION",
    "ChecklistBinding",
    "ChecklistCommitResult",
    "ChecklistContractError",
    "ChecklistCurrentness",
    "ChecklistExecution",
    "ChecklistExecutionHead",
    "ChecklistExecutionStartResult",
    "ChecklistExecutionStatus",
    "ChecklistGateDecision",
    "ChecklistItemOutcome",
    "ChecklistItemResult",
    "ChecklistMode",
    "ChecklistPage",
    "ChecklistPhase",
    "ChecklistPreflight",
    "ChecklistReceipt",
    "ChecklistReceiptSource",
    "ChecklistReceiptView",
    "ChecklistSpecSnapshot",
    "ChecklistStaleReason",
    "ChecklistSubmission",
    "ChecklistTargetType",
    "ChecklistTemplate",
    "ChecklistTemplateItem",
    "ChecklistWriteBundle",
    "checklist_binding_digest_v1",
    "checklist_execution_request_digest_v1",
    "checklist_submission_digest_v1",
    "evaluate_checklist_currentness",
    "evaluate_checklist_gate",
    "require_specify_checklist_item",
]
