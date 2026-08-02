# Glossary

This glossary provides concise definitions for domain terms used across `okto-pulse-core`.

---

### Adapter
An edition-owned component (like database, storage, or telemetry adapters) that implements a core port interface to connect external systems without leaking dependencies into core. See [ARCHITECTURE.md](../ARCHITECTURE.md).

### Architecture Findings
Warnings or findings derived from architectural constraints (such as boundary audits) that gate card or spec execution when active. See [README.md](../README.md).

### Cognitive Closeout
An execution quality governance gate that blocks a card or task transition to `done` while active cognitive-consolidation items remain. See [README.md](../README.md).

### Conformance Gate
A governance mechanism that validates execution quality, policy adherence, and structural rules across SDLC stages. See [ARCHITECTURE.md](../ARCHITECTURE.md).

### Domain Model
The core SDLC entities, business logic rules, and execution models owned by `okto-pulse-core`, independent of external frameworks or transport layers. See [ARCHITECTURE.md](../ARCHITECTURE.md).

### Governance Gate
A policy-enforcement boundary (17 named gates) evaluating resource readiness, spec coverage, validation, execution quality, and sprint health before state transitions. See [README.md](../README.md).

### Hexagonal Architecture
The architectural pattern (Ports & Adapters) separating domain logic from external mechanisms like databases, APIs, and graph runtimes. See [ARCHITECTURE.md](../ARCHITECTURE.md).

### Knowledge Graph
A structured representation of domain concepts and relationships (11 node types, 13 relationship types) providing contextual memory and orchestration. See [README.md](../README.md).

### Lineage
The verifiable record of resource propagation and history tracked across business services and entities. See [README.md](../README.md).

### MCP (Model Context Protocol)
A transport-neutral command catalog (281 tools) used for integrating AI agents with SDLC pipeline operations, decisions, and knowledge graphs. See [README.md](../README.md).

### Port
A pure `Protocol` contract declared by the core package specifying *what* functionality is needed without binding to concrete implementations. See [PORTS.md](./PORTS.md).

### Provider Registry
The system ledger used to track, register, and resolve adapter readiness and runtime dependencies across backend ports. See [ARCHITECTURE.md](../ARCHITECTURE.md).

### Resource Readiness
A governance check verifying that necessary resources, board dependencies, and task coverages are fully initialized prior to execution. See [README.md](../README.md).

### Spec Coverage
A set of governance gates ensuring scenario/test, functional requirement, technical requirement, API contract, and decision coverage for tasks. See [README.md](../README.md).

### Supersedence
The formal tracking and replacement mechanism by which active decisions or records overwrite earlier versions while maintaining history. See [README.md](../README.md).