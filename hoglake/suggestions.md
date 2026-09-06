# Kotlin Assessment & Suggestions (2026-09-05)

Verdict up front: Kotlin is a good-but-not-inevitable fit for the
server, and it was the correct choice **for this team and this
timeline** — the evidence is a well-tested M2 in ~6.8K LOC of legible
code with zero TODO/FIXME markers. The codebase's own evidence both
supports the choice and quietly indicts two of the stack choices
inside it.

## What the code says is working

- **Team fit** — the README's criterion ("strong (Jakob)") is the one
  that actually decides these things, and the code quality bears it
  out: typed exception taxonomy, clean service/repo separation, sealed
  hierarchies where they matter.
- **The domain model is a Kotlin sweet spot.** `Model.kt`'s sealed
  `HoglakeException` + `ChangeKind` enum mirroring a CHECK-constrained
  vocabulary, data-class DTOs with Jackson SNAKE_CASE/NON_NULL — the
  typed-OCC-vocabulary design *wants* exhaustive `when` and sealed
  hierarchies. Java would make this noisier; Go would make it anemic.
- **The escape hatch got used, and Kotlin absorbed it gracefully.**
  The README's language table predicted JVM parquet pain, and M4's
  decision — parquet-java quarantined in the compaction module,
  Hardwood for reads — is exactly the containment the doc described.
  Kotlin's interop makes that containment a module boundary, not a
  rewrite.

## What the code says is costing you

### 1. JDBI is the weakest link in the stack

The repos are `object`s with static SQL strings — no compile-time
safety, and drift evidence is already in the tree:
`OptionsService.LifecycleCatalog` duplicates catalog-row mapping
because "`CatalogRepo`'s `CatalogInfo` predates the V3 retention
columns." A hand-maintained mapping diverged within one milestone.
Exposed or jOOQ would have made that a compiler error; sqlx (Rust)
would have made it a compile-time checked query. A schema-as-law
system sits on top of an untyped SQL layer — the irony is not subtle.

**Suggestion**: before the schema grows more V3-style drift, either
evaluate a compile-checked mapping layer (Exposed/jOOQ) or, minimally,
generate row mappers from the schema. The schema-equivalence gate
already proves the DDL is canonical; the Kotlin↔row mapping is the
unverified half of the contract.

### 2. Background jobs as daemon threads + shutdown hooks

Hydrator/Expiry/Cleanup/CatalogMetrics are loops on raw threads with
`intervalMs <= 0` kill switches. Coroutines with supervisor scopes
would be the idiomatic Kotlin answer — cancellation, structured
concurrency, and test-clock control for free.

**Suggestion**: move the background loops to coroutines while there
are only four of them. It works today; it's the least-Kotlin thing in
the codebase, and each new loop entrenches the pattern.

### 3. No Gradle wrapper, flox-provided gradle

`just server build` does whatever the flox env's gradle version does —
a reproducibility hole between checkouts, CI runners, and agent
sessions.

**Suggestion**: check in the Gradle wrapper. The toolchain-is-flox
choice is deliberate and fine for the JDK; the wrapper is the standard
answer to "which gradle built this jar," and the two coexist
(`flox activate -- ./gradlew …`).

## The honest counterfactuals

| | Would have bought | Would have cost |
|---|---|---|
| **Rust** (axum/sqlx/arrow-rs) | Compile-time-checked SQL (the drift class above dies); best parquet/arrow story — no parquet-java quarantine ever; smaller footprint | An intermediate-Rust author writing the most correctness-sensitive service in the fleet; tokio's learning curve on the OCC tail; materially slower to M2 |
| **Go** (org momentum) | Simplest ops, fastest org onboarding | Weakest parquet of the three, sealed-class-less error taxonomy, more code for less type safety |
| **Java 21** | Same JVM | Verbatim more ceremony in exactly the files that are currently pleasant |

The README's own reasoning — "decide the commit protocol before the
language; if the server never touches parquet, the JVM's parquet
weakness stops mattering" — turned out half right. The server *did*
end up touching parquet (hydrator reads, M4 compaction writes), and
the JVM tax showed up precisely as predicted (Hardwood drops field_id
on write → parquet-java quarantine). But the quarantine contains it to
one module and pyarrow owns the client writer path, so the prediction
was right in cost, wrong in severity.

## Summary

| Priority | Change | Urgency |
|---|---|---|
| 1 | Compile-checked DB mapping (or generated mappers) | Before next schema growth |
| 2 | Coroutine-based background loops | While there are four loops |
| 3 | Check in the Gradle wrapper | Trivial |

None of these is urgent; all three get more expensive as the service
grows. The language choice itself: keep.
