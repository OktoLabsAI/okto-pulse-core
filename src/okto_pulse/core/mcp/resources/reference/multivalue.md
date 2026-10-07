---
version: "2.0"
---

# Native multi-value parameters

Send string collections as native JSON arrays, for example
`labels=["bug, regression", "str | None"]`. String inputs, including encoded
JSON, delimited strings and bare single values, are rejected. Outer whitespace
is trimmed and empty string items are dropped; punctuation, order, duplicates
and literal backslashes inside each item are preserved.

Consult tools/list for required fields. Optional arrays accept omission/null.
An empty array follows the individual tool's update semantics; it is not a
universal clear operation. Test scenario updates use the native `clear` array
to explicitly request supported field resets.

Choice creation uses one required `options` array of objects:
`[{"label": "A, B | C", "recommended": true, "tradeoff": "Costs more"}]`.
Every option needs a non-empty string label. The optional recommended field
is a native boolean, default false. The optional tradeoff is a string or null,
default null. Unknown fields are refused. Answers use a native `selected`
array of option IDs. No alternate options parameter or string decoder exists.

Spec functional requirements, technical requirements and acceptance criteria
use native arrays of structured objects, as described by their tools.
Object-valued parameters have separate schemas; consult tools/list for each
field rather than encoding a string collection into them.
