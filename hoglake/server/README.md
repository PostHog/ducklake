# hoglake server

The hoglake control plane (see [../README.md](../README.md) for the
design). Kotlin/JVM, Ktor, Postgres via JDBI + Flyway, MinIO/S3 for the
data plane, parquet footers via Hardwood.

## Dev environment

Toolchain comes from [flox](https://flox.dev) (JDK 21); Gradle from the
host:

```sh
flox activate            # or prefix commands: flox activate -- gradle build
gradle build             # compile + unit tests
gradle test              # all tests; integration tests need Docker running
gradle test -PunitOnly   # skip Docker-gated tests
docker compose up -d     # local Postgres + MinIO for `gradle run`
gradle run
```

## Layout

- `src/main/resources/db/migration/` — Flyway migrations (plain SQL).
- `schema.sql` — canonical schema; CI asserts fold(migrations) == this.
- `src/main/resources/openapi/hoglake.yaml` — the REST contract
  (served at `/openapi.yaml`).
- `src/main/kotlin/com/posthog/hoglake/`
  - `model/` — domain types (mirror the OpenAPI schemas).
  - `persistence/` — JDBI repositories. All SQL lives here.
  - `commit/` — the append-commit service (OCC, advisory-lock tail,
    row-id range assignment).
  - `api/` — Ktor routes implementing the OpenAPI spec.
  - `hydrator/` — async stats hydration for `pending` files (S3 footer
    reads via Hardwood).

## Testing

Unit tests are plain JUnit 5. Integration tests use Testcontainers
(Postgres 16, MinIO) and are tagged `integration`; they require a
running Docker daemon.
