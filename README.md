# okto-pulse-core

Core engine for [Okto Pulse](https://github.com/okto-labs/okto-pulse) — shared models, services, API routes, and MCP server.

> **You probably want to install [`okto-pulse`](https://pypi.org/project/okto-pulse/) instead.**
> This package is the internal engine. The `okto-pulse` package provides the CLI, frontend, and everything you need to get started.

## What's inside

- **27 SQLAlchemy models** — Boards, Cards, Specs, Ideations, Refinements, Agents, etc.
- **18 service classes** — Full business logic with governance rules
- **11 API route modules** — FastAPI REST endpoints
- **119+ MCP tools** — Complete Model Context Protocol server for AI agent integration
- **App factory** — `create_app()` with dependency injection for auth and storage providers

## License

[Elastic License 2.0](./LICENSE) — free for personal and commercial use. Cannot be offered as a hosted/managed service.
