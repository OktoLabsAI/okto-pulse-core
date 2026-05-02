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
