# CLAUDE.md — okto-pulse-core

This file gives Claude (and humans) what they need to operate this repo.
Project overview lives in `README.md`; this file is for procedures.

This repo is the internal engine for `okto-pulse`: SQLAlchemy models,
FastAPI routes, MCP server, embedded knowledge graph (Ladybug). The
deployable Docker artifact lives in the sibling `okto-pulse` repo, NOT
here. This package is consumed as a build-time dependency.

---

## Releasing a new version

This repo's role in a release: **be tagged with the matching `vX.Y.Z` tag
BEFORE okto-pulse is tagged**, so the release pipeline's preflight check
passes. Tagging here does not trigger any CI by itself — tags exist purely
as anchors for the okto-pulse release workflow's checkout step.

The full pipeline lives in `okto-pulse/.github/workflows/release.yml`.
Read `okto-pulse/CLAUDE.md` for what the pipeline actually does, image
tags, recovery, and known caveats.

### Procedure for THIS repo

1. **Bump version** in `pyproject.toml`:

   ```bash
   sed -i '' 's/^version = "X.Y.Z"/version = "X.Y.Z+1"/' pyproject.toml
   git add pyproject.toml
   git commit -m "chore: bump version to X.Y.Z+1"
   git push origin <branch>
   ```

2. **Tag and push**. This must happen BEFORE you tag okto-pulse:

   ```bash
   git tag -a vX.Y.Z+1 -m "Release X.Y.Z+1"
   git push origin vX.Y.Z+1
   ```

3. **Then** go to `okto-pulse` and follow its release procedure (it does
   the build + smoke + GHCR push).

If you tag `okto-pulse` without first tagging this repo, its workflow
fails fast with this error:

```
::error::okto-pulse-core is not tagged vX.Y.Z.
::error::Tag and push core BEFORE pulse:
::error::  cd okto-pulse-core && git tag vX.Y.Z && git push origin vX.Y.Z
```

### Why this coupling exists

`okto-pulse/Dockerfile` (target `local-runtime`) builds wheels from BOTH
sibling sources. The release workflow does a sibling checkout of this repo
at the exact same `vX.Y.Z` tag as `okto-pulse`, so the resulting image is
reproducible and the tag is the single source of truth for the engine
version that ships.

The `okto-pulse/pyproject.toml` dep pin (`okto-pulse-core>=X.Y.Z,<1.0.0`)
is not what controls the build — the sibling checkout does. Keep both
repos at the same `pyproject.toml` version so the workflow's version-match
gate passes.

---

## How this repo ships in Docker

This repo has **no Dockerfile** of its own. It ships as a wheel built into the
`okto-pulse` image. There are two paths that emit the same runtime contract:

- **`local-runtime`** (`okto-pulse/Dockerfile`, target `local-runtime`,
  context = parent of `okto-pulse/`): the `wheel-builder` stage runs
  `python -m build --wheel /src/okto-pulse-core` AND
  `python -m build --wheel /src/okto-pulse`. Both wheels land in `/wheels/`
  and `local-install` does `uv pip install` against `okto-pulse/uv.lock` plus
  the two local wheels. This is what `okto-pulse/docker-compose.yml` and the
  release pipeline's smoke build use.
- **`pypi-runtime`** (target `pypi-runtime`, context = `okto-pulse/` only):
  resolves and installs `okto-pulse==<ARG OKTO_PULSE_VERSION>` from PyPI,
  which transitively pulls `okto-pulse-core` matching the floor in
  `okto-pulse/pyproject.toml`. This is what `okto-pulse/docker-compose.prod.yml`
  uses. **Implication:** to ship a `pypi-runtime` image of a given core
  version, that core version must already be published to PyPI; otherwise
  use `local-runtime`.

### Module-level state — single-process, dual-port

The Docker container runs `okto-pulse serve`, which spins up two
`uvicorn.Server` instances inside one Python process via `asyncio.gather`.
Why one process: the embedded LadybugDB is single-writer and module-level
state (the registered SQLAlchemy session factory, the `_global_db` cache,
the `_active_api_key` `ContextVar`) must be shared between the API listener
and the MCP listener. Two ports because the SPA fetches go to one
(`http://localhost:8100`) and the AI tool's MCP HTTP transport hits the other
(`http://localhost:8101/mcp`).

`build_mcp_asgi_app()` is the helper the community runner calls to construct
the MCP ASGI app for its second uvicorn server. `mount_mcp(app)` is an
alternative path that mounts the MCP sub-app onto an existing FastAPI app.
Either approach hits the same `register_session_factory` / `ContextVar`
state.

### MCP host binding

The MCP listener is constructed in
`okto-pulse/src/okto_pulse/community/main.py`, NOT in this repo's
`core/mcp/server.py:run_mcp_server()`. The standalone entry-point in this
repo (`python -m okto_pulse.core.mcp.server`) reads `MCP_HOST` from the
environment, but it is not the production code path. As of okto-pulse 0.1.12,
the production path also reads `MCP_HOST`. Pre-0.1.12 it didn't — be careful
with backports.

### Persistence

The container writes to `/data`:
- `/data/data/pulse.db` — SQLite (boards, agents, specs, sprints, cards, …)
- `/data/boards/<board-id>/graph.lbug` — per-board LadybugDB graph
- `/data/uploads/` — attachments

Mount `/data` to a named volume. Don't mount over `/opt/hf-cache` — the
sentence-transformers model is baked in at build time so the runtime is
offline-capable.

### Recovery — when the pipeline fails

Tags in this repo are NOT protected by a ruleset (only `okto-pulse` has
that). But because okto-pulse's tags ARE protected and immutable, **the
pragmatic answer is still to bump and re-tag both repos**, not to
fast-forward this one. Keeping the two repos' tags in lockstep is what
makes a release reproducible — diverging them creates ghosts.

Procedure on a failure:

1. Diagnose using `gh run view --log-failed --job=<id> -R OktoLabsAI/okto-pulse`
2. Fix in code or workflow
3. Bump patch version in BOTH repos
4. Re-tag in lockstep (this repo first, okto-pulse second)

---

## What runs on tag push to THIS repo

Nothing. `.github/workflows/ci.yml` triggers only on `push: main` and
`pull_request: main` — runs the pytest matrix (Linux + Windows × py3.11/12/13),
ruff, pyright, security audit, license check. Tag pushes are silent.

If you want core CI to run on tag pushes (e.g. for auditing what tagged
commits would look like under full matrix), extend `on.push.branches` to
include `'release/**'` or add `tags: ['v*.*.*']`. Not done by default
because `okto-pulse/release.yml` re-runs core's pytest as part of its
release gate.

---

## Local development

```bash
pip install -e ".[dev]"
pytest -q

# kg layer specifically
pytest tests/test_kg_file_handles.py        # currently red on Linux — see below
pytest tests/test_kg_schema_lifecycle.py    # has a flaky gc-counter test

# Code quality
ruff check src/
pyright src/
```

### Known issues in this repo

- **`test_kg_file_handles::test_close_releases_handles` fails reproducibly
  on Linux** post-Ladybug migration. `BoardConnection.close()` does not
  release `graph.kuzu` + `graph.kuzu.wal` file handles. This is a real
  bug worth tracking; it's why the okto-pulse release workflow currently
  has no pytest gate. Fixing it should restore the pytest gate in
  `okto-pulse/.github/workflows/release.yml`.

- **`test_kg_schema_lifecycle::test_exit_marks_closed_and_runs_gc` is
  flaky on CI runners** — patches `gc.collect` and asserts call count;
  Linux GC scheduling differs from local macOS dev. Pre-existing.

- **27 other pre-existing test failures** in the broader suite (stale
  schema strings, asyncio race conditions). These are tracked separately;
  they are NOT release blockers.

---

## CI workflows in this repo

- `.github/workflows/ci.yml` — full matrix on main + PRs; do not change
  the matrix without coordinating, the multi-OS coverage is intentional
- `.github/workflows/cla.yml` — CLA signing, do not touch
