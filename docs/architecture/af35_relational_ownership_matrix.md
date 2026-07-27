# AF35 Relational Ownership Matrix

AF35-S5 makes relational ownership executable. The source of truth is
`run_af35_s5_relational_final_gate()` in
`okto_pulse.core.application.boundary.af35_s5_relational_final_gate`; this file
is the human-readable source map for that report.

Core application/use-case code must stay free of direct `AsyncSession`,
`Depends(get_db)`, `get_db_for_mcp`, `session.execute`, `select` and
`flag_modified` coupling. Concrete SQLAlchemy work belongs to edition adapters
and repository implementations. In the current local runtime that means the
Community edition supplies the SQLAlchemy oracle while core keeps the ports,
use cases, REST/MCP contracts and fail-closed gates.

Every temporary exception below is governed by an executable ledger row with
owner, rationale, public surface, evidence reference and removal criterion.
Missing metadata, new unowned findings or stale ledger rows make the S5 gate
blocking.

<!-- AF35-S5-RELATIONAL-OWNERSHIP:BEGIN -->
| Classification | Observed count | Owner boundary | Allowed locations | Removal rule |
| --- | ---: | --- | --- | --- |
| `adapter_owned` | 0 | Edition adapters and repository implementations | Concrete Community/SaaS adapters only; core must not import edition packages | Replace by satisfying the same core ports with another edition adapter |
| `migrated_clean` | 0 | Core application/use-case boundary | Rows that must remain at zero direct relational occurrences | Keep the count at zero; any observed occurrence is unowned |
| `non_productive_reference` | 0 | Documentation and false-positive references | Comments, docs or non-productive references only | Remove when the reference stops being useful |
| `temporary_core_exception` | 0 | AF35 temporary residue owners named per row | Governed KG, REST and MCP residues with owner/rationale/evidence/removal metadata | Retire each row through its row-level removal criterion; stale rows fail closed |
| `test_only` | 0 | Test fixtures | Tests and synthetic fixtures only | Do not promote test-only residue into productive core paths |
| `unowned` | 0 | No valid owner | None | Block the change, migrate behind UoW/ports, or add a governed temporary row |
| `uow_seam` | 0 | AF35 REST UoW request boundary | api/deps.py request UoW provider wrapper only | Retire when REST request UoW wiring no longer wraps get_db |
<!-- AF35-S5-RELATIONAL-OWNERSHIP:END -->

<!-- AF35-S5-RELATIONAL-SOURCES:BEGIN -->
| Source | Surface | Classification | Observed count | Ledger rows |
| --- | --- | --- | ---: | ---: |
| `af35-s1-use-cases` | `application_use_cases` | `migrated_clean` | 0 | 1 |
<!-- AF35-S5-RELATIONAL-SOURCES:END -->
