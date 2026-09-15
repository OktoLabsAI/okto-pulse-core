# Optional graph observation ports

The registry exposes three independent, optional contracts. An edition may
implement some, all or none; missing capabilities must be reported as unavailable,
never silently substituted with empty successful results. They are not required
registry bootstrap slots. Core imports no graph-engine implementation.

| Registry slot | Contract module | Operations |
| --- | --- | --- |
| `ranked_graph_search` | `core.kg.interfaces.ranked_graph_search` | `readiness`, explicit `prepare`, `search(RankedGraphQuery)` |
| `graph_history` | `core.kg.interfaces.graph_observations` | `activate`, `commits`, `as_of`, `diff`, `prune` |
| `graph_analytics` | `core.kg.interfaces.graph_observations` | `analyze`: components, cycles, dependency impact |

`RankedGraphQuery` describes textual/hybrid retrieval, result/candidate/filter
bounds and existing visibility policy. Implementations validate it before I/O.
Return business IDs qualified by node type, independent source scores and an
explicit ranking regime: fusion scores are not cosine scores or probabilities.
These ports do not replace current substring matching or Core's domain-specific
hybrid expansion/ranking pipeline.

Commit/history tokens and row lineages are opaque observations, not storage paths,
authorization or a claim of a shared relational/graph transaction. History starts
at explicit activation, can become unavailable after retention, and must not
fabricate pre-activation values. Providers report omitted properties explicitly.
Separate history pages need not share a snapshot; consumers must honor each
response's snapshot and continuation contract.

Edition/application callers enforce board access and Code Traceability visibility.
History additionally requires audit-read authority, since it may disclose retained
deleted values. Administrative operations require the existing writer policy and
durability lifecycle; in Community REST these are composed through
`guarded_board_write`. Adapters revalidate that authority, not create or bypass it.
An applied operation followed by a failed fence/lifecycle must not be automatically
replayed. Engine-specific activation, native value conversion, routing, handles,
index definitions and budgeting remain in the edition adapter.

Analytics declares its selected node/relationship scope, visibility and budgets.
Hidden nodes must not survive as path bridges. Budget refusal is distinct from an
empty graph or complete result. Cycle-blocked nodes can include descendants of a
cycle; they are not necessarily cycle members. Dependency reachability is bounded
and includes its source. Analytics is not implicitly run for ordinary node pages.

The concrete Community request/response schemas, current limits, examples and
operational cautions are documented in that edition's
`docs/GRAFX_ADVANCED_ADOPTION_0_0_6.md`; those choices are not Core requirements for
every graph engine.
