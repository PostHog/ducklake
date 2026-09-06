# Fuzzing hoglake

The strategy for finding bugs by generation rather than enumeration,
and where each piece lives. Status: Python side landing now; JVM side
next.

## Why fuzz this system

The bug classes that hurt us in the DuckLake era were exactly the kind
example-based tests miss: NULL-shaped rows crashing unguarded reads,
type-boundary values (UInt64 > 2^63), encoding mismatches between
components, and state machines driven into corners by concurrency. All
four are generator-friendly.

## The layers

### 1. Property-based tests (in the normal suites)

- **pyhoglake** (hypothesis, `tests/qe_*.py`): codec round-trips over
  full value domains (`decode(encode(x)) == x` per ColType — boundary
  ints, subnormal floats, ±0.0, precision-38 decimals, astral-plane
  unicode), arrow↔ColType mapping totality (every exotic arrow type
  either maps or raises `UnsupportedTypeError`, never passes silently),
  stats extraction vs independently computed ground truth, and
  wire-model parsing under mutated JSON (extra/missing/wrong-typed
  fields must fail cleanly, never leak `KeyError`).
- **server** (planned, kotest-property or jqwik): the same codec
  properties on `IcebergSingleValue`, expiry `newEarliest` math under
  generated snapshot/offset/time configurations (invariants: never >
  head, never > min consumer offset when floored, monotone across
  sweeps), commit row-id range tiling under generated concurrent
  request mixes.

### 2. Cross-language differential vectors

`pyhoglake/tests/vectors/bounds_vectors.json`: canonical
(type, value, hex) triples generated from the Python codec, consumed by
both suites. The Iceberg single-value encoding exists in Kotlin
(`stats/IcebergSingleValue.kt`) and Python (`pyhoglake/bounds.py`);
any divergence corrupts pruning silently — the vector file makes it a
test failure instead. JVM-side consumer test: planned alongside the
server property tests. When a fuzzer finds a nasty value, it gets
promoted into the vector file.

### 3. API fuzzing (live server)

Adversarial generation against a disposable stack: hostile identifiers
(unicode, injection strings, length boundaries), boundary numerics,
oversized payloads, duplicate registrations, storms of concurrent
commits/offsets/DDL. Lives in the QE test files (`qe-*` catalog
prefix); pathological but reproducible — seeds pinned on failure.

### 4. Coverage-guided fuzzing (JVM, planned)

[Jazzer](https://github.com/CodeIntelligenceTesting/jazzer) targets for
the pure parsing/derivation surfaces once they stabilize:
`IcebergSingleValue.encode/decode`, the alter-op wire discriminator,
and (highest value) the parquet-footer→stats path in the Hydrator fed
mutated footers — the exact class of the `TransformGlobalStatsRow`
NULL crash in the defect ledger. Not wired into CI yet; run as a
periodic soak.

## Rules

- A fuzzer-found bug becomes: a pinned regression test (exact input,
  not a seed), an entry in the ledger if it's a design-class defect,
  and a vector-file entry when cross-language.
- Property tests run in the normal suites (`just test-all`) with
  bounded example counts; deep runs (`--hypothesis-seed`, higher
  max_examples, Jazzer soaks) are manual/periodic.
