# Fuzzing hoglake

The strategy for finding bugs by generation rather than enumeration,
and where each piece lives. Status: Python side landed; JVM
coverage-guided layer landed (layer 4 below — corpus replay runs in the
normal server suite, deep runs via `./gradlew fuzz`).

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

### 4. Coverage-guided fuzzing (JVM, live)

[Jazzer](https://github.com/CodeIntelligenceTesting/jazzer) via
jazzer-junit `@FuzzTest`: the targets are ordinary JUnit tests in
`server/src/test/kotlin/com/posthog/hoglake/fuzz/`, hermetic (pure JVM,
no Docker/DB), so they run in BOTH modes:

- **Corpus replay (every `./gradlew :test` run)**: without `JAZZER_FUZZ`
  set, each `@FuzzTest` deterministically replays its committed seed
  corpus plus the empty input — fast regression tests, on in CI by
  construction.
- **Fuzzing (on demand)**: `cd server && flox activate -- ./gradlew
  fuzz -PfuzzSeconds=300` runs every target under libFuzzer for the
  given per-target budget (default 60s). jazzer-junit allows one fuzz
  test per JVM, so each target has its own task
  (`fuzzParquetFooterFuzzTest`, ...) — runnable individually, chained
  sequentially under the umbrella `fuzz` task.

Targets (one class, one `@FuzzTest` each):

| Target | Surface | Contract fuzzed |
|---|---|---|
| `IcebergSingleValueDecodeFuzzTest` | `IcebergSingleValue.decode` (arbitrary bytes × every ColType) | only `IllegalArgumentException` refusals; `encode(decode(x)) == x` for canonical encodings |
| `IcebergSingleValueCompareFuzzTest` | `encode`/`compareValues` over generated typed values | round-trip; comparator sign-antisymmetry, reflexivity, transitivity, equals-consistency |
| `ParquetFooterFuzzTest` | the hydrator's `ParquetFileReader.open(...).footer` path + `FooterStats` | typed/structural refusals only (parquet's own frames), `FooterStats.*` total over parsed footers; 1 MiB input cap |
| `PuffinDeletionVectorFuzzTest` | `PuffinDeletionVector.read` (highest value: fresh code on writer-supplied bytes) | loud typed refusals (IAE/ISE/IOException), deterministic decode, no silent mis-decode |
| `IdentifiersFuzzTest` | `Identifiers.validate` + the RequestId header shape | only `HoglakeException.Validation`; decisions stable and equal to a character-walk reference of the documented policy |
| `WireDtoParseFuzzTest` | `toModel()` on a successfully-parsed `CommitRequestDto` / polymorphic `AlterOp` | only the exceptions ErrorMapping turns into 4xx (`JacksonException`, `BadRequestException`, `HoglakeException`) |

**Phase discipline (learned the hard way, 2026-09-06.)** That last
target originally asserted over `readValue` *and* `toModel()` together,
and duly "found" a crash: a byte run Jackson's encoding detector reads
as UTF-32 raises `java.io.CharConversionException`, which is an
`IOException`, not a `JacksonException`. It was a harness artifact —
`readValue` runs inside ktor's ContentNegotiation, which wraps *any*
converter throw into a 400, so a parse-phase exception can never reach
the client as a 500. `toModel()` runs in the route handler, outside
that wrapping, and is the only phase where the exception class decides
the status code. The target now asserts on that phase alone; the wire
behavior it cannot observe is pinned end-to-end in
`api/WireParseErrorMappingTest` (hostile bytes -> 400 through the real
serialization + StatusPages stack). The crashing input stays in the
corpus — it is a genuinely interesting encoding trap, just not a bug.
General rule: a fuzz target that spans a framework boundary must assert
on the side of the boundary where the contract actually lives.

Corpus layout (jazzer-junit's inputs convention — the resource
directory doubles as seed corpus and replay fixture):

    server/src/test/resources/com/posthog/hoglake/fuzz/
        <TargetClass>Inputs/<fuzzMethod>/<seed files>

Seeds are generated by `./gradlew generateFuzzSeeds`
(`fuzz/FuzzSeedGenerator.kt`, manual, output committed): every vector
from `pyhoglake/tests/vectors/bounds_vectors.json` (type byte +
encoding) for the codec targets, real parquet-java-written footers
(with/without field ids, truncated), spec-conformant + CRC-corrupted
puffin DV blobs, boundary identifiers, and representative wire bodies.
Crashing inputs found while fuzzing are written back into the same
directories by jazzer-junit (instant regression tests); the growing
generated corpus lands in `server/.cifuzz-corpus/` (transient, never
committed).

Promotion rule (unchanged): a fuzzer-found nasty value becomes a
committed corpus entry here, a pinned regression test, and — when the
surface is cross-language (the bounds codec) — an entry promoted into
`bounds_vectors.json` by the Python side; the JVM side never edits the
vector file directly.

## Rules

- A fuzzer-found bug becomes: a pinned regression test (exact input,
  not a seed), an entry in the ledger if it's a design-class defect,
  and a vector-file entry when cross-language.
- Property tests run in the normal suites (`just test-all`) with
  bounded example counts; deep runs (`--hypothesis-seed`, higher
  max_examples, `./gradlew fuzz -PfuzzSeconds=...` soaks) are
  manual/periodic.
