# Iceberg federation — design obligations for v1

What the initial hoglake design must get right so the read-only Iceberg
REST facade ([README.md](README.md), Decisions) works — and keeps
working — as a federation participant. The theme: *mappability is a
property of the data model and the files, not of the facade code*, and
several pieces are unretrofittable once a lake exists.

Companions: [README.md](README.md) (architecture + decisions),
[trino-integration.md](trino-integration.md) (the first consumer of
this facade), [metadata-schema.md](metadata-schema.md) (what today's
model looks like).

## 1. Field IDs in the parquet files (unretrofittable)

Iceberg resolves projection by **field ID embedded in the parquet
schema** (`PARQUET:field_id` on each schema element), not by column
name. The hoglake writer contract must mandate embedding the catalog's
column IDs in every file from the first byte written; a lake without
them can only be served by name-mapping shims, and fixing it later
means rewriting every file.

This also formally kills the pyducklake field-ID fiction (sequential
IDs synthesized client-side from `ordinal_position` — see
[pyducklake-api-map.md](pyducklake-api-map.md)): the catalog's column
IDs are the one identity, in the metadata and in the files.

Registration-time enforcement is cheap: the footer the writer ships
already contains the schema; the commit endpoint verifies field IDs are
present and match the table schema. The async hydrator re-verifies for
deferred-stats registrations.

## 2. Type discipline

Every catalog type either has a defined Iceberg mapping or an explicit
`unmappable` marker with a defined facade behavior (error on table
access vs. column omitted — pick one, per type, at spec time). The
known trouble spots from the DuckLake type system: HUGEINT/UHUGEINT
(no 128-bit int in Iceberg), VARIANT (Iceberg V3 has variant — decide
whether we track V3 here), unsigned ints generally, and
sub-microsecond timestamps. The rule to adopt: **a type may not be
used in a hoglake table unless its facade story is defined** — the
decision happens at DDL time, not at read time in someone else's
engine.

## 3. Iceberg-identical partition transforms only

Facade pruning works only if partition specs translate exactly.

- `bucket(n)`: our `murmur3_32` is already bit-compatible with
  Iceberg's hash by design ([ducklake-api-map.md](ducklake-api-map.md)
  §1.2) — keep it that way, and fix the nested-type
  string-representation fallback rather than inheriting it.
- Date/time transforms: Iceberg defines `year/month/day/hour` as
  epoch-relative integers. DuckLake grew both calendar-flavored and
  `epoch_*` variants; hoglake supports **only the Iceberg-semantics
  set**. One definition, one name, no aliases.
- `identity`, `truncate(w)`: direct mappings; adopt Iceberg's
  truncation semantics verbatim if/when we add truncate.

## 4. One delete encoding, and it's one Iceberg understands

A table with live deletes is only servable through the facade if the
deletes are expressible as Iceberg V2 position-delete files or V3
deletion vectors (puffin). The fork already half-supports puffin DVs.
Position: **deletion vectors become THE hoglake delete representation**
— internal readers and the facade share one encoding, and we skip
carrying DuckLake's bespoke positional-parquet delete format into the
new world. This also pins the facade's Iceberg format-version target
(V3, with V2 position-delete generation as a fallback only if a
consumer forces it).

## 5. Stats bounds stored typed, not as text

Manifest entries carry `lower_bounds`/`upper_bounds` in Iceberg's
single-value binary serialization. DuckLake stores min/max as VARCHAR
text, which makes bound conversion a per-type parsing adventure with
ambiguity (float formatting, timestamp zones, binary values). Hoglake
stores bounds in a typed/binary form chosen so manifest generation is a
mechanical re-encode. Same rule for `value_counts`/`null_value_counts`
/`nan_value_counts` — they map 1:1 from the footer-shipped stats, so
the registration API's stats shape should be Iceberg-`Metrics`-shaped
from day one (see [trino-integration.md](trino-integration.md) §2 for
why this pays twice).

## 6. Metadata-artifact generation and bucket layout

The REST facade ultimately hands readers real objects: a
`metadata.json`, manifest lists, and manifest files (avro) in object
storage. Two v1 obligations:

- **Bucket layout reserves the space now**: a `metadata/` prefix per
  table alongside `data/`, so facade artifacts have a home that
  credential vending can scope to.
- **The manifest generator is a service component**: lazy per-snapshot
  generation with caching (a snapshot's manifests are immutable once
  written — generate once, serve forever, GC with snapshot expiry).
  Incremental generation falls out of the snapshot model: a new
  snapshot's manifest list reuses the previous manifests for untouched
  files, exactly like Iceberg's own writers do.

## 7. Identity and snapshot mapping (mostly free, keep it that way)

- `table_uuid` ↔ Iceberg `table-uuid`: already in the model; the
  changefeed carries it (README key moves) — the facade uses the same
  one.
- hoglake snapshot ↔ Iceberg snapshot is 1:1; the snapshot's
  change-summary must be mappable to Iceberg's snapshot `operation`
  (`append` / `overwrite` / `delete` / `replace`) — a constraint on
  how we encode the typed `snapshot_change` rows, cheap if considered
  now.
- Namespaces: hoglake schemas map to single-level Iceberg REST
  namespaces; don't invent nesting we'd have to flatten.

## 8. What stays out of scope (and why that's safe)

Write-path Iceberg compatibility remains out (Decisions) — but see
[trino-integration.md](trino-integration.md) for the commit-shape
choice that keeps an append-only write adapter a *translation* rather
than a redesign. Facade freshness bounds (relevant only if
Arrow-inline data files ever land) are covered in the README's
inlining decision.

## Checklist form (for the phase-2 spec)

| Obligation | Cost if skipped | When it must land |
|---|---|---|
| Field IDs in files | Lake rewrite | First file written |
| Per-type facade story | Read-time errors in foreign engines | DDL validation, v1 |
| Iceberg-exact transforms | No pruning through facade | Partition spec DDL, v1 |
| DV-only deletes | Delete-bearing tables unservable | First delete support |
| Typed stats bounds | Lossy/ambiguous manifests | Registration API, v1 |
| `metadata/` prefix + generator | Facade rework + layout migration | Bucket layout, v1 |
| Operation-mappable snapshot changes | Facade guesses `operation` | snapshot_change encoding, v1 |
