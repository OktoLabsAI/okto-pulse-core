"""R05-E dependency conformance ledger (spec d9d30831, card 0c066d12).

A pure, versioned LEDGER of every NON-AGNOSTIC technical dependency / import that
the core either (a) ``removed``, (b) hands to the Community edition as
``community_owned``, (c) defers to a ``future_adapter`` (SaaS), or (d) keeps as
an explicit, gated ``temporary_exception``. It is the authority the R05-E
conformance auditor (``dependency_conformance``) consults to decide — fail-closed
— whether a concrete edition dependency/import that appears in the core is
allowed or a violation.

PURE (tr_73ba5de5 / ts_681ca6d4): this module imports ONLY stdlib
(``dataclasses`` / ``typing``). Every concrete dependency / import token is
referenced by STRING — it NEVER imports ``asyncpg`` / ``aiosqlite`` / ``ladybug``
/ ``numpy`` / ``sentence_transformers`` / ``apscheduler`` / any concrete
technology. Mirrors the style of ``adapter_readiness_inventory`` (R05-A), but
that ledger catalogues ADAPTERS while THIS one catalogues DEPENDENCIES — they are
complementary, never duplicated.

Each entry carries the bounded schema required by tr_8e7c6d7c:
``token`` / ``owner_wave`` / ``current_owner`` / ``reason`` /
``removal_criterion`` / ``validation_oracle`` (+ ``classification`` and ``kind``,
plus the direct-dep/no-import metadata that separates an auto-removable
dependency like ``asyncpg`` from a transitive-need exception like ``numpy`` /
``aiosqlite``).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

#: Bump when the ledger schema or its accepted-exception set changes.
LEDGER_VERSION = "R05-E.2"

#: Disposition of a non-agnostic technical token (br_913a9a9a / fr_4f86d9f0).
DependencyClassification = Literal[
    "removed",  # gone from the core default; must NOT reappear as dep or import
    "community_owned",  # owned/declared by the Community edition
    "future_adapter",  # deferred to a future (SaaS) adapter edition
    "temporary_exception",  # still required by the current runtime; ledger-gated
]

#: Whether the token is tracked as a manifest dependency, a source import, or both.
TokenKind = Literal["dependency", "import", "dependency_and_import"]

#: The bounded fields every entry MUST fill (fail-closed ledger integrity).
REQUIRED_ENTRY_FIELDS: tuple[str, ...] = (
    "token",
    "owner_wave",
    "current_owner",
    "reason",
    "removal_criterion",
    "validation_oracle",
)


def normalize_token(token: str) -> str:
    """Canonical comparison form: lowercased, ``-``/``_`` unified.

    Unifies the PyPI distribution name and the import root (e.g.
    ``sentence-transformers`` and ``sentence_transformers`` collapse to the same
    token) so the auditor matches a declared dependency against a source import.
    """
    return token.strip().lower().replace("-", "_")


@dataclass(frozen=True)
class LedgerEntry:
    """One non-agnostic technical token in the R05-E dependency ledger.

    Declarative only — ``token`` / ``aliases`` / ``expected_source_import_roots``
    are STRINGS; nothing is imported.
    """

    token: str
    classification: DependencyClassification
    kind: TokenKind
    owner_wave: str
    current_owner: str
    reason: str
    removal_criterion: str
    validation_oracle: str
    #: ``True`` when the token is a DIRECT manifest dependency that has NO runtime
    #: import in ``src/okto_pulse/core`` (the same shape as ``asyncpg``). The
    #: ledger is what decides whether such a token is auto-removable (``removed``)
    #: or a kept ``temporary_exception`` pinned by a transitive consumer.
    direct_dep_no_import: bool = False
    #: For a ``direct_dep_no_import`` exception: the transitive consumer that
    #: still pins it (e.g. SQLAlchemy's sqlite driver for ``aiosqlite``). This is
    #: what makes ``numpy``/``aiosqlite`` DIFFERENT from ``asyncpg`` even though
    #: all three are direct-dep + no-import.
    transitive_consumer: str | None = None
    #: Import roots this token is expected to appear as under
    #: ``src/okto_pulse/core`` (e.g. ``ladybug``). Empty for a direct-dep + no
    #: import token.
    expected_source_import_roots: tuple[str, ...] = ()
    #: Alternative spellings (PyPI name vs import root) that map to this entry.
    aliases: tuple[str, ...] = ()
    #: ``True`` when the token is declared as an optional-dependency extra rather
    #: than a default dependency.
    optional_extra: bool = False

    def matches(self, candidate: str) -> bool:
        """True if ``candidate`` (a dep name or import root) maps to this entry."""
        norm = normalize_token(candidate)
        return norm == normalize_token(self.token) or norm in {
            normalize_token(a) for a in self.aliases
        }


#: The temporary-exception tokens that R05-E MUST keep ledgered while they remain
#: in the core (ac_69911a08 / TS-R05E-03). ``requests`` + ``chardet`` are the
#: "requests/chardet" telemetry pair — six logical exceptions, seven tokens.
CANONICAL_TEMPORARY_EXCEPTION_TOKENS: tuple[str, ...] = (
    "aiosqlite",
    "numpy",
    "requests",
    "chardet",
    "apscheduler",
)


def build_dependency_ledger() -> tuple[LedgerEntry, ...]:
    """Build the canonical, immutable R05-E dependency ledger.

    Declarative — every token is a STRING; nothing concrete is imported.
    """
    entries: list[LedgerEntry] = [
        # --- removed in R05-E: future SaaS Postgres adapter, no consumer today --
        LedgerEntry(
            token="asyncpg",
            classification="removed",
            kind="dependency",
            owner_wave="R05-E",
            current_owner="future SaaS relational adapter (no current owner in core)",
            reason=(
                "Postgres async driver. ZERO import in src/okto_pulse/core and no "
                "core default requires it; it is a future SaaS relational adapter, "
                "not a Community-local need, so R05-E removes it from the core "
                "default rather than moving it to Community."
            ),
            removal_criterion=(
                "Removed now: asyncpg must NOT appear as a core direct dependency "
                "(pyproject / uv.lock okto-pulse-core package) nor be imported in "
                "src/okto_pulse/core. A future SaaS edition owns the Postgres "
                "adapter behind the relational repository/UoW port (#04) + "
                "migrations boundary (#16/#05-D)."
            ),
            validation_oracle=(
                "dependency_conformance auditor: removed asyncpg is absent from the "
                "manifest+lock direct deps and the source import scan "
                "(test_r05e_dependency_conformance::"
                "test_ts_c2f39229_asyncpg_removed_from_core_default)."
            ),
            direct_dep_no_import=True,
            transitive_consumer=None,
            expected_source_import_roots=(),
        ),
        LedgerEntry(
            token="aiofiles",
            classification="removed",
            kind="dependency",
            owner_wave="AF-05",
            current_owner="none (orphaned core dependency)",
            reason=(
                "Async filesystem helper declared by the core runtime manifest "
                "without any import consumer in src/okto_pulse/core or "
                "src/okto_pulse/community. It is not required by the common core "
                "package and is not moved to Community without a direct adapter use."
            ),
            removal_criterion=(
                "Removed now: aiofiles must NOT appear as a core direct "
                "dependency (pyproject / uv.lock okto-pulse-core package), wheel "
                "Requires-Dist entry, or runtime import under src/okto_pulse/core."
            ),
            validation_oracle=(
                "dependency_conformance auditor: removed aiofiles is absent from "
                "manifest+lock+source and explicit wheel metadata, and synthetic "
                "reintroduction fails closed on all four surfaces."
            ),
            direct_dep_no_import=True,
            transitive_consumer=None,
            expected_source_import_roots=(),
        ),
        # --- temporary exceptions: direct dep, NO import, pinned transitively ---
        LedgerEntry(
            token="aiosqlite",
            classification="temporary_exception",
            kind="dependency",
            owner_wave="#04_repository_uow + #16_schema_migrations (#05-D relational boundary)",
            current_owner="okto-pulse-core/infra",
            reason=(
                "Async SQLite driver loaded BY URL by SQLAlchemy for the default "
                "local database (sqlite+aiosqlite). Direct dependency with no "
                "explicit import in src/okto_pulse/core — SQLAlchemy resolves the "
                "dbapi from the connection URL."
            ),
            removal_criterion=(
                "Remove once the relational driver is owned by a Community/edition "
                "adapter behind the repository/UoW port (#04) and the migrations "
                "boundary (#16/#05-D). NOT removable while the core default DB URL "
                "is sqlite+aiosqlite (infra/config.py)."
            ),
            validation_oracle=(
                "dependency_conformance accepts aiosqlite as a ledgered "
                "direct-dep/no-import exception; #05-D relational boundary gate "
                "(test_r05d_data_ownership) governs the actual move."
            ),
            direct_dep_no_import=True,
            transitive_consumer="SQLAlchemy sqlite+aiosqlite async driver (default local DB URL)",
            expected_source_import_roots=(),
        ),
        LedgerEntry(
            token="numpy",
            classification="temporary_exception",
            kind="dependency",
            owner_wave="#13_llm_embedding_rerank",
            current_owner="okto-pulse-core/kg",
            reason=(
                "Numeric backing for the embedding / rerank vector stack. Direct "
                "dependency with NO import in src/okto_pulse/core (only a comment "
                "in kg/rerank/cross_encoder.py); pulled transitively by the ML "
                "stack (sentence-transformers / transformers / scikit-learn / "
                "scipy)."
            ),
            removal_criterion=(
                "Remove from the core default once the embedding/rerank adapters "
                "(#13) move to the Community edition and the vector stack is "
                "composition-owned; audit D3 (numpy domain-scoring usage) before "
                "removal."
            ),
            validation_oracle=(
                "dependency_conformance accepts numpy as a ledgered "
                "direct-dep/no-import exception pinned by the #13 embedding stack."
            ),
            direct_dep_no_import=True,
            transitive_consumer="sentence-transformers / transformers / scikit-learn / scipy",
            expected_source_import_roots=(),
        ),
        LedgerEntry(
            token="requests",
            classification="temporary_exception",
            kind="dependency",
            owner_wave="#10_telemetry",
            current_owner="okto-pulse-core (manifest only; transport owned by Community)",
            reason=(
                "HTTP transport for the telemetry beacon sender. Direct dependency "
                "with no import in src/okto_pulse/core — the telemetry_sender_"
                "ownership_gate already BLOCKS `import requests` inside "
                "core/telemetry, so the concrete sender lives in the Community "
                "edition."
            ),
            removal_criterion=(
                "Remove once #10 telemetry transport is fully Community-owned with a "
                "green telemetry oracle. tr_03abf5ab: R05-E may ledger/audit but "
                "must NOT change requests/chardet/telemetry without a green "
                "telemetry oracle (#10)."
            ),
            validation_oracle=(
                "telemetry_sender_ownership_gate (no requests import in "
                "core/telemetry) + #10 telemetry oracle; dependency_conformance "
                "accepts it as a ledgered direct-dep/no-import exception."
            ),
            direct_dep_no_import=True,
            transitive_consumer="telemetry beacon sender HTTP transport (Community-owned, governed by #10)",
            expected_source_import_roots=(),
        ),
        LedgerEntry(
            token="chardet",
            classification="temporary_exception",
            kind="dependency",
            owner_wave="#10_telemetry",
            current_owner="okto-pulse-core (manifest only; governed by #10)",
            reason=(
                "Charset detection companion of the telemetry/requests HTTP "
                "transport. Direct dependency with no import in src/okto_pulse/core."
            ),
            removal_criterion=(
                "Remove together with the telemetry transport once #10 telemetry is "
                "Community-owned with a green oracle. tr_03abf5ab: do not change "
                "without a green telemetry oracle (#10)."
            ),
            validation_oracle=(
                "dependency_conformance accepts chardet as a ledgered "
                "direct-dep/no-import exception governed by #10."
            ),
            direct_dep_no_import=True,
            transitive_consumer="telemetry/requests HTTP transport charset detection (governed by #10)",
            expected_source_import_roots=(),
        ),
        # --- transferred to Community: must not reappear in core --------------
        LedgerEntry(
            token="ladybug",
            classification="community_owned",
            kind="dependency_and_import",
            owner_wave="#06_kg_ports + #12_kg_runtime_extraction",
            current_owner="okto-pulse-community/adapters",
            reason=(
                "Embedded graph engine implementation. It is now owned by the "
                "Community KG runtime adapters; the core exposes only board graph "
                "runtime/transaction/store/lifecycle ports and schema contracts."
            ),
            removal_criterion=(
                "Already transferred: Ladybug must be absent from the core default "
                "manifest/lock and from runtime imports under src/okto_pulse/core. "
                "Any reintroduction is a core-contamination violation."
            ),
            validation_oracle=(
                "dependency_conformance reports ladybug as community_owned and "
                "blocks manifest, lock, wheel or source reintroduction in the core; "
                "the concrete implementation is tested through Community adapters."
            ),
            direct_dep_no_import=False,
            transitive_consumer=None,
            expected_source_import_roots=("ladybug",),
        ),
        LedgerEntry(
            token="apscheduler",
            classification="temporary_exception",
            kind="dependency_and_import",
            owner_wave="#03_lifecycle_composition (RuntimeComposition)",
            current_owner="okto-pulse-core/runtime",
            reason=(
                "Local scheduler for the KG decay tick. Still imported in "
                "core/app.py until the lifecycle cleanup moves scheduler boot "
                "ownership out of Core."
            ),
            removal_criterion=(
                "Remove once the SchedulerControl provider is composition-owned "
                "(#03 runtime ports) and the singleton scheduler bridge is gone; "
                "see adapter_readiness_inventory singleton_scheduler_control."
            ),
            validation_oracle=(
                "dependency_conformance accepts apscheduler while its import roots "
                "are ledgered; #03 RuntimeComposition governs the removal."
            ),
            direct_dep_no_import=False,
            transitive_consumer=None,
            expected_source_import_roots=("apscheduler",),
        ),
        LedgerEntry(
            token="sentence-transformers",
            classification="community_owned",
            kind="dependency_and_import",
            owner_wave="R-P2-07 (#13_llm_embedding_rerank)",
            current_owner="okto-pulse-community/adapters",
            reason=(
                "Concrete embedding and cross-encoder provider dependency. The "
                "core exposes EmbeddingProvider/Reranker ports and deterministic "
                "fallbacks only; Community owns the sentence-transformers runtime."
            ),
            removal_criterion=(
                "Already transferred by R-P2-07: sentence-transformers must be "
                "absent from the core manifest/lock and from runtime imports under "
                "src/okto_pulse/core. Any reintroduction is a core-contamination "
                "violation."
            ),
            validation_oracle=(
                "dependency_conformance reports sentence-transformers as "
                "community_owned and blocks manifest, lock, wheel or source "
                "reintroduction in the core; Community adapters provide the "
                "concrete providers."
            ),
            direct_dep_no_import=False,
            transitive_consumer=None,
            expected_source_import_roots=("sentence_transformers",),
            aliases=("sentence_transformers",),
            optional_extra=False,
        ),
    ]
    return tuple(entries)


def ledger_index(
    ledger: tuple[LedgerEntry, ...] | None = None,
) -> dict[str, LedgerEntry]:
    """Map every normalized token + alias to its entry (for O(1) lookup)."""
    ledger = ledger if ledger is not None else build_dependency_ledger()
    index: dict[str, LedgerEntry] = {}
    for entry in ledger:
        index[normalize_token(entry.token)] = entry
        for alias in entry.aliases:
            index[normalize_token(alias)] = entry
    return index


def entry_missing_fields(entry: LedgerEntry) -> tuple[str, ...]:
    """Required fields that are empty/blank on an entry (ledger integrity)."""
    missing: list[str] = []
    for field_name in REQUIRED_ENTRY_FIELDS:
        value = getattr(entry, field_name)
        if not (isinstance(value, str) and value.strip()):
            missing.append(field_name)
    return tuple(missing)


__all__ = [
    "LEDGER_VERSION",
    "DependencyClassification",
    "TokenKind",
    "REQUIRED_ENTRY_FIELDS",
    "CANONICAL_TEMPORARY_EXCEPTION_TOKENS",
    "LedgerEntry",
    "normalize_token",
    "build_dependency_ledger",
    "ledger_index",
    "entry_missing_fields",
]
