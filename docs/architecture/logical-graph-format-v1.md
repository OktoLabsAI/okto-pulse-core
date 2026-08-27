# `okto-pulse-logical-graph/1`

The portable logical graph format used to move a Board or a Global Discovery
scope between storage engines, and the Core contracts that surround it.

Everything described here lives in `okto_pulse.core.kg.logical_transfer` and is
IO-neutral: the encoder yields lines, the decoder consumes lines, and no module
in the package opens a file, names a path, or imports a backend.

## What the format is for

A logical artifact has to be restorable into an engine that is not the one that
produced it. That single requirement is what excludes physical identity from
the format. Record ids, page numbers, filenames, WAL positions, vector space
ids and HNSW topology are all absent by design: any of them would silently bind
an artifact to the database that wrote it.

What is preserved instead is everything a reader can observe logically — the
schema, the keys, every property, the direction of every relation, and one
entry per occurrence.

## Framing

The artifact is a sequence of canonical JSON documents, one per line, UTF-8.
Canonical means sorted object keys and no insignificant whitespace, so the same
graph produces the same bytes on every machine and every run. A checksum or a
fingerprint over non-canonical bytes would mean nothing.

Records appear in a fixed structural order:

```
header
node*
relation*
manifest
```

The order is load-bearing, not stylistic:

- the **header** precedes every record, so a reader knows the schema, the
  declared census and the required features before it interprets anything;
- **nodes precede relations**, so a relation's endpoints are already known and
  a reader never has to buffer the artifact to resolve one;
- the **manifest is terminal and mandatory** — it is the only thing that
  distinguishes a complete artifact from a truncated one.

A `node` after the first `relation` is a sequence error, not a tolerated
reordering.

## Header

```json
{
  "record": "header",
  "format": "okto-pulse-logical-graph/1",
  "scope": "board",
  "features": {"required": ["vectors"], "optional": []},
  "schema": { "...": "..." },
  "counts": {"nodes": 2, "relations": 3, "properties": 11, "vectors": 1},
  "schema_digest": "<hex>"
}
```

`format` must match exactly; anything else is refused as an unsupported
version rather than parsed optimistically.

`features.required` names what a reader must implement. An unknown **required**
feature is refused, never skipped — a reader that ignored one would import a
graph that means something other than what the writer recorded. An unknown
**optional** feature is ignored, which is the entire point of the distinction.

`counts` is the census the source already knows, declared up front so a reader
can detect divergence early. It costs the writer nothing: a snapshot knows its
own census before it streams.

The stream checksum is deliberately **not** here. Putting it in the header
would force the writer either to buffer the whole artifact or to seek back and
patch it, and the codec must be able to stream a graph it never holds.

## Node and relation records

```json
{"record":"node","type":"Card","key":"c1","properties":{ "...": "..." }}
{"record":"relation","layout":"blocks","source":"c1","target":"c2","properties":{}}
```

Relations name their endpoints by **logical key**. A self-loop is
`source == target`. Two relation records that compare equal are two
occurrences; nothing deduplicates them.

## Manifest

```json
{
  "record": "manifest",
  "counts": {"nodes": 2, "relations": 3, "properties": 11, "vectors": 1},
  "fingerprint": "<hex>",
  "stream_checksum": "<hex>"
}
```

`stream_checksum` is SHA-256 over every preceding record line and its newline.
On decode, the manifest counts, the header counts, the fingerprint and the
checksum are all verified against what actually arrived.

## Value codec

Values are tagged explicitly. An untagged encoding would have to recover a type
from JSON's own type lattice, which cannot tell an int from a timestamp, cannot
represent a float exactly, and has no way at all to say *absent*.

| Tag | Encoding | Notes |
| --- | --- | --- |
| `null` | `["null"]` | an explicit null value |
| `bool` | `["bool", true]` | checked before `int64`, since `bool` is a subclass of `int` |
| `int64` | `["int64", "7"]` | canonical decimal string; out-of-range is refused |
| `float64` | `["float64", "0x1.999999999999ap-4"]` | `float.hex()`, exact and platform-independent |
| `string` | `["string", ""]` | the empty string is a value, not a null |
| `timestamp_us` | `["timestamp_us", "1717171717000000"]` | whole microseconds since the Unix epoch |
| `vector` | `["vector", {"space_name": "...", "dtype": "...", "components": ["..."]}]` | named by **logical** space |

Refused at encode time: NaN, infinity, int64 overflow, Python `None`, and any
type not in the table. `None` is refused specifically so the absent/null
distinction cannot be lost by accident.

### `absent` versus `NULL` versus `""`

Three different facts, three different encodings:

| Fact | Encoding |
| --- | --- |
| never set | the key is **omitted** from `properties` |
| set to null | the key is present with `["null"]` |
| set to the empty string | the key is present with `["string", ""]` |

## Logical fingerprint

The fingerprint proves that what arrived is what left, without depending on the
order either side walked its storage in.

Each record is digested on its own under its section's domain tag, and the
digests are combined by **addition modulo 2^256**. That combination is
commutative, so order does not matter, and it does not cancel, so multiplicity
survives.

XOR would satisfy the first property and destroy the second: two identical
parallel relations would XOR to zero and vanish from the digest — exactly the
multiplicity this format exists to preserve. A regression test pins that the
digests of zero, one and two identical relations are three different values.

The final digest commits to the schema digest and then, per section, to the
domain tag, the record count and the modular sum, plus the property and vector
totals. Committing to counts as well as sums means one section cannot be
confused with another that happens to sum alike.

The accumulator holds two counters and two integers, so fingerprinting an
artifact costs the same memory whether it carries ten records or ten million.

## Refusals

Every refusal is a typed Core error; no backend exception reaches a caller.

| Condition | Error |
| --- | --- |
| unknown `format` | `UnsupportedFormatVersionError` |
| unknown required feature | `UnsupportedFeatureError` |
| record out of structural order | `ArtifactSequenceError` |
| stream ends without a manifest | `ArtifactTruncatedError` |
| anything after the manifest | `ArtifactTrailingDataError` |
| counts, checksum or fingerprint disagree | `ArtifactIntegrityError` |
| unparseable or misshapen record | `ArtifactMalformedError` |
| value cannot be represented exactly | `LogicalValueError` |
| schema or record contradicts its schema | `LogicalSchemaError` |

## Transfer contracts

Two neutral ports. Core does not know whether a snapshot is an MVCC read
transaction or a frozen file, nor whether a candidate is a generation, a
directory or a namespace.

- **`LogicalSnapshotSource`** opens the single snapshot a transfer reads from.
- **`LogicalCandidateSink`** accepts records into a new, empty, unbound
  candidate and can certify it.

The orchestration in `service.py` opens exactly one snapshot, moves bounded
batches, and:

- **fails closed on the candidate** — any failure abandons it, including a
  failure inside `begin_candidate`, which may already have allocated one. There
  is deliberately no resume path in this milestone; an interrupted transfer is
  repeated, not continued.
- **certifies before finalizing** — `finalize` runs only after a typed
  certificate has been checked field by field.
- **treats absence as refusal** — a certificate that omits a claim is refused
  exactly like one that contradicts it, or a sink could earn success by staying
  quiet.

A certificate must make all six of these observable, and all six must match the
snapshot: cold reopen completed, schema equality, counts, vector spaces,
logical fingerprint, and a successful `verify()`.

Failures are classified into a finite matrix so the same refusal is actionable
depending on where it happened:

| Phase | Covers |
| --- | --- |
| `write` | opening and reading the snapshot, batch bound violations, a census the snapshot contradicts |
| `import` | beginning the candidate and writing batches into it |
| `checkpoint` | making imported records durable |
| `reopen` | certification and acceptance of the re-read candidate |

## Out of scope

The following are deliberately **not** here, and belong to other milestones:
filesystem access, paths, fsync and atomic replace; concrete adapters; provider,
router, binding and CAS; generation management; journal and outbox; shadow,
canary and cutover; and partial-candidate resume.
