# okto-pulse-core

Domain and application engine for [Okto Pulse](https://github.com/OktoLabsAI/okto-pulse), with transport-neutral ports, contracts and MCP commands.

> **Ship with AI. Stay in control.**

> **You probably want to install [`okto-pulse`](https://pypi.org/project/okto-pulse/) instead.**
> This package is the internal engine. The `okto-pulse` package provides the CLI, frontend, and everything you need to get started.

## What's inside

- **0 SQLAlchemy models** — Core owns no concrete relational mappings. This is checked by scanning for `__tablename__` assignments anywhere under `core/`; the Community edition owns the SQLAlchemy model and repository adapters.
- **34 service classes** — Full business logic with governance rules, board agent governance, resource propagation + lineage, bug-regression workflow, archive/restore, traceability and board-level resource readiness. Source: classes ending in `Service` under `core/services`.
- **0 API route modules** — Core owns application contracts and use cases, not concrete FastAPI routers. The count scans `core/api/*.py`; Community owns the REST adapter and route modules.
- **17 governance gates** — Resource readiness, resource-to-task coverage, spec coverage, validation, evaluation, task completion, cognitive closeout, architecture-findings, evidence, bug traceability and sprint health controls.
- **281 MCP tools** — Complete Model Context Protocol command catalog for AI agent integration, counted from the transport-neutral Core catalog after importing the server, including:
  - Pipeline CRUD (Ideation, Refinement, Spec, Sprint, Card)
  - Q&A and choice questions across every entity
  - Mockups (HTML+Tailwind, sanitised) and Knowledge Bases at spec/refinement/card scope
  - Decisions with supersedence and coverage gates
  - Per-card Knowledge attachment lifecycle (`add_card_knowledge` and friends)
  - 62 Knowledge Graph tools (consolidation, query primary/power, health, dead-letter, schema-migrate, decay tick controllability, board rebuild and global discovery recovery preflight/confirm/run)
  - Community runtime exposure: 281 core MCP tools, 0 community-only MCP tools
- **Application composition contracts** — edition-neutral runtime, auth, storage, persistence, graph, telemetry and transport ports; concrete app construction belongs to the edition
- **Hexagonal backend ports** — runtime, telemetry, repository/UoW and KG provider seams, plus the adapter readiness ledger, documented in [`ARCHITECTURE.md`](./ARCHITECTURE.md)
- **Knowledge Graph contracts and orchestration** — graph schema vocabulary, query/consolidation semantics, deterministic + cognitive workers, 11 node types and **13 relationship types**. Source: `len(KGEdgeType)` in `core/kg/schemas.py`; the concrete LadybugDB/Kuzu board and global graph runtimes are supplied by the active edition
- **Bounded operational metric samples** — governance, architecture, bug-regression, resource-lineage and global-discovery observability keep capped diagnostic samples. Global-discovery count APIs remain monotonic totals and do not derive totals from the retained sample ring.

## Governance Gate Surface

Okto Pulse currently documents and enforces **17 named governance gates**:

| Gate family | Gates |
| --- | --- |
| Resource readiness | Resource readiness; resource-to-task coverage |
| Spec coverage | Scenario/test coverage; functional requirement/business rule coverage; technical requirement/task coverage; API contract/task coverage; active decision/task coverage |
| Validation and evaluation | Spec validation; spec qualitative evaluation; task validation |
| Execution quality | Task start/spec readiness; task conclusion; cognitive closeout; architecture-findings done; test evidence; bug test-first/traceability |
| Sprint health | Sprint closure/evaluation |

The two execution-quality additions introduced in 0.2.3 — **cognitive closeout** (a `done` transition is blocked while active cognitive-consolidation items remain) and the **architecture-findings done gate** (active architecture warnings block `spec`/card `done`) — remain enforced in the current release.

## Architecture

The core is a **ports-and-policy** package: it owns the SDLC domain, the governance gates and
the Knowledge Graph contracts, and delegates every concrete mechanism (database, graph runtime,
filesystem, HTTP, scheduler) to the active edition.

**→ [Architecture overview](docs/ARCHITECTURE-OVERVIEW.md)** — layering, boundary governance
(AF-20/21/22, AF-28/29/30/31), MCP composition helpers and the edition-ownership rules.
**→ [ARCHITECTURE.md](./ARCHITECTURE.md)** — the deep hexagonal reference (ports, adapter readiness
ledger, provider registry).

## Ports & Adapter Interfaces

Core defines **~100 `Protocol` contracts across 78 modules in `okto_pulse/core/ports/`** plus
**30 Knowledge-Graph interfaces in `okto_pulse/core/kg/interfaces/`**. Each one is a seam: core
declares *what* it needs; it never constructs a database, opens a file, resolves an environment
variable or speaks a wire protocol.

Unfilled slots **fail closed** (`R-P2-03A-D`) rather than falling back to a silent default.

**→ [Full port catalogue](docs/PORTS.md)** — every protocol with its method count and contract,
grouped by persistence, KG storage/runtime, KG governance, delivery/workers, MCP/inbound,
read models and telemetry.

## Docker

This repo has **no Dockerfile**. The deployable artifact is a single image built from the sibling [`okto-pulse`](https://github.com/OktoLabsAI/okto-pulse) repo:

- `okto-pulse/Dockerfile` target `local-runtime` builds wheels from this repo and `okto-pulse/` as siblings (used by `okto-pulse/docker-compose.yml` and the release pipeline's smoke build).
- `okto-pulse/Dockerfile` target `pypi-runtime` installs `okto-pulse==<version>` from PyPI, which transitively pulls this package off PyPI per the floor in `okto-pulse/pyproject.toml` (used by `okto-pulse/docker-compose.prod.yml`).

To run the published image:

```bash
docker run -d --name okto-pulse \
  -e HOST=0.0.0.0 -e MCP_HOST=0.0.0.0 \
  -p 8100:8100 -p 8101:8101 \
  -v okto-pulse-data:/data \
  ghcr.io/oktolabsai/okto-pulse:latest
```

See [`okto-pulse/README.md`](https://github.com/OktoLabsAI/okto-pulse#run-with-docker) for the full Docker quickstart, and [`okto-pulse/CLAUDE.md`](https://github.com/OktoLabsAI/okto-pulse/blob/main/CLAUDE.md) for the multi-stage build architecture.

## Release Notes

**Current: 0.3.0** — the hexagonal decontamination release. The core stopped owning any concrete
infrastructure and became a pure ports-and-policy package, while the Knowledge Graph gained
deterministic identity, atomic provenance and reversible curation. 90 commits over `v0.2.6`.

**→ [Full release notes](docs/RELEASE-NOTES.md)** — 0.3.0 changeset in detail, plus 0.2.6, 0.2.5,
0.2.3, 0.2.2, 0.2.1, 0.2.0 and the 0.1.x line.

## SaaS Closure Audit

The executable ownership matrix is generated by `okto-pulse-saas-closure`. Every transitional budget must remain zero; the command fails closed on import, dependency, adapter, wheel, or documentation drift.

<!-- F16-SAAS-CLOSURE:BEGIN -->
| F16 executable surface | Owner | Observed | Terminal target |
| --- | --- | ---: | ---: |
| Core import rows | Core | 5344 | classified |
| Community-to-Core import rows | Community | 724 | classified |
| Direct dependency rows | Distribution owner | 23 | classified |
| `import_boundary_baseline` budget | `675c43ee-7d91-4cc3-8f87-44eeb293f90c` | 0 | 0 |
| `singleton_baseline` budget | `675c43ee-7d91-4cc3-8f87-44eeb293f90c` | 0 | 0 |
| `dependency_temporary_exceptions` budget | `675c43ee-7d91-4cc3-8f87-44eeb293f90c` | 0 | 0 |
| `graph_runtime_compatibility` budget | `675c43ee-7d91-4cc3-8f87-44eeb293f90c` | 0 | 0 |
| `rebuild_artifact_compatibility` budget | `675c43ee-7d91-4cc3-8f87-44eeb293f90c` | 0 | 0 |
| `community_private_reach_ins` budget | `675c43ee-7d91-4cc3-8f87-44eeb293f90c` | 0 | 0 |
| `community_adapter_bridges` budget | `675c43ee-7d91-4cc3-8f87-44eeb293f90c` | 0 | 0 |
| `af35_relational_residue` budget | `675c43ee-7d91-4cc3-8f87-44eeb293f90c` | 0 | 0 |
<!-- F16-SAAS-CLOSURE:END -->

## License

[Elastic License 2.0](./LICENSE) — free for personal and commercial use. Cannot be offered as a hosted/managed service.
