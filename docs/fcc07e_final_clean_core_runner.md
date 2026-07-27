# FCC-07E — Final clean-core smoke & conformance runner

The FCC-07E runner is the single, locally-callable entrypoint that aggregates the
FCC-07A/B/C/D boundary gates into one deterministic report and a fail-closed exit
code. It needs **no Pulse server** — every backend check is a pure function or a
local `pytest` invocation — so Codex, Claude and a human operator can all run
`quick` / `full` and attach the same report as release evidence.

All symbols below are public on
`okto_pulse.core.application.boundary` (`from okto_pulse.core.application.boundary import …`).

| Card | Module | Surface |
|---|---|---|
| FCC-07E-IMP1 | `final_clean_core_runner` | `run_final_clean_core`, `RunnerReport`, `render_final_clean_core_report` (the shell + report schema + exit code) |
| FCC-07E-IMP2 | `final_clean_core_orchestrator` | `orchestrate_final_clean_core` (maps each *supplied* A/B/C/D report → gate rows; no rule duplication; C→B feed) |
| FCC-07E-IMP3 | `final_clean_core_focused_suites` | `run_focused_suites`, `DEFAULT_FOCUSED_SUITES`, `run_final_clean_core_quick` (focused pytest + granular failure map) |
| FCC-07E-IMP4 | `final_clean_core_full_smoke` | `run_full_mode_smoke`, `detect_full_prerequisites`, `run_final_clean_core_full_mode` (full-mode install smoke + temp isolation) |

---

## 1. Prerequisites

| Mode | Needs | Notes |
|---|---|---|
| `quick` | a checked-out core repo + `uv` | Runs the four boundary gates (pure functions) and the focused `pytest` suites. No build, no venv, no network. |
| `full`  | `quick` prerequisites **plus** a built core wheel (`dist/*.whl`) and the stdlib `venv` module | Adds the install smoke gates inside a throw-away temporary venv. When the wheel/venv are absent the smoke gates are **skipped with an honest reason — never a fake success** (see §4). |

The `full`-mode prerequisite probe is explicit and fail-closed:

```python
from pathlib import Path
from okto_pulse.core.application.boundary import (
    FullModePrerequisites, detect_full_prerequisites,
)

prereqs = FullModePrerequisites(
    required_wheels=(Path("dist/okto_pulse_core-0.3.0-py3-none-any.whl"),),
    require_venv_module=True,
)
check = detect_full_prerequisites(prereqs)        # PrerequisiteCheck(available, reason)
# check.available is False (with an honest reason) when the wheel is not built,
# venv is unimportable, or the temp root is not writable.
```

---

## 2. Quick mode — commands

### 2a. Orchestrated run against the real gates

`orchestrate_final_clean_core` consumes whichever of the **canonical** FCC-07A/B/C/D
reports it is given (built or injected) and maps each verdict into a runner gate row.
It never re-derives a decision — it projects each gate's own `ok` / `overall_status`
/ `status`.

```python
from okto_pulse.core.application.boundary import (
    orchestrate_final_clean_core, render_final_clean_core_report,
)

report = orchestrate_final_clean_core(
    mode="quick",
    use_real_builders=True,        # builds the A and C reports from the live core
    adapter_evidence={},           # FCC-07B evidence map (see note)
    required_gate_ids=["FCC07D"],  # fail-close D, which is not auto-mapped (see note)
)
print(render_final_clean_core_report(report, fmt="markdown"))
raise SystemExit(report.exit_code)  # 0 only when every required gate passes
```

This recipe maps **FCC07A / FCC07B / FCC07C** rows. The two notes below say why D is
different and how B is fed.

> **FCC-07B evidence note.** FCC-07B (adapter readiness) is evidence-driven: pass the
> `{adapter_key: AdapterEvidence}` map (or `removal_bindings=` for the removal-binding
> path). FCC-07C's `dependency_audit_passed` projection is threaded into FCC-07B
> automatically (the C→B feed) — you never compute it yourself. An adapter with no
> evidence collapses to `deferred` → a **non-blocking** `skipped` row.

> **FCC-07D note.** Unlike A/B/C, FCC-07D (the runtime provider guard) has **no real
> builder** in this path — there is no live composition for the orchestrator to
> introspect on its own. With the recipe above it is therefore **not mapped** (the
> report carries `FCC07A` / `FCC07B` / `FCC07C` rows only). To include it, pass an
> explicit provider source: `runtime_composition=` (a live `RuntimeComposition`),
> `provider_guard=` (a pre-built `RuntimeCompositionGuardReport`), or
> `provider_observations=` (injected Community wiring). Listing it in
> `required_gate_ids=["FCC07D"]` (as above) fail-closes: a required gate that did not
> run is synthesised as a `blocked` row, so a missing D is never silently green.

### 2b. Focused pytest suites

`quick` also runs five focused `pytest` suites. The exact commands (stable, copy-pasteable):

```bash
# FCC07B — adapter readiness (owner: okto-pulse-core/boundary)
uv run pytest tests/test_fcc07b_readiness_aggregator.py tests/test_fcc07b_removal_binding.py tests/test_r05a_adapter_readiness_inventory.py -p no:cacheprovider -q

# FCC07A_C — dependency / conformance (owner: okto-pulse-core/architecture)
uv run pytest tests/test_fcc07a_conformance_matrix.py tests/test_fcc07c_packaging_gate.py tests/test_fcc07c_community_audit.py -p no:cacheprovider -q

# FCC07D — fake / registry guard (owner: okto-pulse-core/runtime)
uv run pytest tests/test_fcc07d_test_provider_policy.py tests/test_fcc07d_runtime_guard.py -p no:cacheprovider -q

# community_wiring (owner: okto-pulse-core/architecture)
uv run pytest tests/test_community_smoke_12b.py -p no:cacheprovider -q

# composition_smoke (owner: okto-pulse-core/runtime)
uv run pytest tests/test_runtime_composition_03.py tests/test_boundary_audit_12.py -p no:cacheprovider -q
```

To capture them as runner rows (with a granular per-suite failure map) rather than
running them by hand, drive them through the injectable runner. Pick **one** of these —
each runs the five suites exactly once:

```python
from okto_pulse.core.application.boundary import (
    run_focused_suites, run_final_clean_core_quick,
)

# Option A — just the focused-suite rows (a FocusedSuitesResult):
result = run_focused_suites()          # default real SubprocessCommandRunner
#   -> result.command_results / result.gate_results / result.all_passed

# Option B — the suites folded into a full RunnerReport (runs them once, internally):
report = run_final_clean_core_quick()  # focused suites -> the reused E-IMP1 shell
```

> Do **not** call both in the same run: `run_final_clean_core_quick` invokes
> `run_focused_suites` internally, so pairing them would execute every suite twice.

A failing suite produces a `blocked` gate whose `remediation` names the **exact
command, the failing test files, the gate id, the owner and the exit code** — never
a generic "pytest failed".

---

## 3. The report (`api_170877a6`)

`RunnerReport.as_dict()` emits exactly:

```jsonc
{
  "overall_status": "success" | "blocked",      // success IFF exit_code == 0
  "mode": "quick" | "full",
  "exit_code": 0,                                 // 0 only when no gate blocks and no command fails
  "ordering": ["gate_id", "spec_id", "adapter_key", "dependency_family", "provider_key"],
  "gates": [
    {
      "gate_id": "FCC07B",
      "spec_id": "FCC-07B",
      "status": "success" | "blocked" | "skipped",   // NOTE the enum (not passed/blocking)
      "owner": "okto-pulse-core/boundary",
      "adapter_key": "kuzu_graph_store",
      "dependency_family": null,
      "provider_key": null,
      "evidence_fields": { /* the six REQUIRED_EVIDENCE tri-state flags */ },
      "command": null,
      "test_file": null,
      "remediation": null
    }
  ],
  "commands": [ { "command": "...", "exit_code": 0, "test_files": ["..."], "owner": "..." } ],
  "summary": { "blocking": 0, "passed": 2, "skipped": 0 }
}
```

Two contract details worth internalising:

- **Gate status enum is `success | blocked | skipped`.** A passing gate is `success`,
  a blocking gate is `blocked`. The deliberate naming asymmetry is that `summary`
  counts those as `passed` / `blocking` / `skipped`.
- **`ordering` lists the five sort-key *names*.** The realised order is the `gates`
  array itself, sorted by exactly those keys, so a shuffled input is byte-identical.

`overall_status == "success"` **iff** `exit_code == 0` **iff** no gate is `blocked`
and no command has a non-zero exit code. A required gate that did not run is
synthesised as a `blocked` row (fail-closed) — a gate that never ran can never count
as success.

---

## 4. Full mode — environment strategy

```python
from pathlib import Path
from okto_pulse.core.application.boundary import (
    FullModePrerequisites, detect_full_prerequisites,
    SmokeInstallInput, CommunitySmokeInput,
    CoreSmokeInstallAdapter, CommunityRebuildReinstallSmokeAdapter,
    run_full_mode_smoke, run_final_clean_core_full_mode,
)

wheel = Path("dist/okto_pulse_core-0.3.0-py3-none-any.whl")
prereqs = FullModePrerequisites(required_wheels=(wheel,))

# The real adapters wrap the #12 smoke gates; each takes its #12 gate input.
smoke = run_full_mode_smoke(
    adapters=[
        CoreSmokeInstallAdapter(
            gate_input=SmokeInstallInput(wheel_path=wheel, expected_imports=("okto_pulse",)),
        ),
        CommunityRebuildReinstallSmokeAdapter(
            gate_input=CommunitySmokeInput(oracle=None),
        ),
    ],
    prerequisites=lambda: detect_full_prerequisites(prereqs),
)
report = run_final_clean_core_full_mode(smoke=smoke)
```

- **Isolation.** When the prerequisites are available the smoke runs inside a uniquely
  named `tempfile.TemporaryDirectory` (one sandbox sub-dir per gate). The sandbox — and
  only the sandbox — is removed on exit, **including on exception**; a user file outside
  the sandbox is never touched.
- **Honest skip, never fake success.** When the prerequisites are absent (no wheel,
  no `venv`, non-writable temp root) **no sandbox is created and no gate is run** — each
  gate is recorded as `skipped` with the honest reason in its remediation. This is the
  load-bearing anti-requirement: an absent prerequisite can never become a green gate.

---

## 5. Diagnosing a blocker per FCC / gate

Filter `report.gates` (or `report.as_dict()["gates"]`) to `status == "blocked"` and
read the identity + remediation columns:

| Gate | Identity columns to read | What a block means / remediation |
|---|---|---|
| **FCC07A** (conformance matrix) | `remediation` | The matrix has a blocking row or a missing required adapter key. The remediation lists the unmet adapter keys; resolve every blocking dependency/import finding. |
| **FCC07B** (adapter readiness) | `adapter_key`, `evidence_fields` | Evidence readiness blocked for that adapter. The `evidence_fields` tri-state flags show which of the six `REQUIRED_EVIDENCE` checks failed; `remediation` + `owner` point at who closes it. (`deferred` shows as a non-blocking `skipped`.) |
| **FCC07C** (packaging ownership) | `dependency_family` (the offending symbol/surface) | A core surface owns a Community-owned/removed/unknown dependency. One `blocked` row per finding names the dependency family + surface; move it to the Community edition or register an explicit, owned exception. |
| **FCC07D** (provider guard) | `provider_key` | A test-only provider is wired in a production composition. The `provider_key` (+ `composition_path` in the remediation) names the offending slot; remove it from the production wiring. |
| **focused pytest** (any suite) | `command`, `test_file` | A focused suite failed. The remediation quotes the exact `uv run pytest …` command and the failing files — re-run it locally and fix those tests. |

> **Real-state caveat.** Until the FCC decontamination trail is complete, a real
> `quick` run against the live core is expected to report `blocked` (FCC07C still
> surfaces the framework dependencies the refactor is removing, e.g. `sqlalchemy` /
> `fastapi`). That is the runner working as designed — it is the conformance signal,
> not a runner bug.

---

## 6. Release evidence (reproducible)

The report is the evidence. Render it in either format and attach it to the Pulse
validation / release record:

```python
md   = render_final_clean_core_report(report, fmt="markdown")  # human-readable, attachable
data = render_final_clean_core_report(report, fmt="json")      # machine-readable (sorted keys)
```

Both renders are pure functions of the report — deterministic, ordered by the five
sort keys, and stable across runs on the same inputs — so the same evidence is
reproducible by anyone running the recipe in §2 / §4. Record alongside it:

1. the git commit of `okto-pulse-core`,
2. the exact recipe used (`quick` orchestration and/or the five focused-suite
   commands; `full` with the wheel path),
3. the rendered markdown (and/or JSON) report,
4. the process exit code (`report.exit_code`).
