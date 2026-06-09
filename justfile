# DuckLake — PostHog fork

# Default recipe: list all available recipes
default:
    @just --list

# === Dev ===

# Initialize git submodules (duckdb, extension-ci-tools)
[group('dev')]
sync:
    git submodule update --init --recursive

# Update vendored DuckDB submodule to the pinned revision
[group('dev')]
pull:
    make pull

# Run the bundled duckdb shell from the release build
[group('dev')]
run:
    ./build/release/duckdb

# === Build ===

# Build release (multi-core via ninja)
[group('build')]
build: sync
    make GEN=ninja release

# Build debug
[group('build')]
debug: sync
    make debug

# Clean build artifacts
[group('build')]
clean:
    make clean

# === Test ===

# Run all DuckLake extension tests
[group('test')]
test:
    ./build/release/test/unittest

# Run a single test file (e.g. `just test-file test/sql/transaction/create_conflict.test`)
[group('test')]
test-file FILE:
    ./build/release/test/unittest "{{FILE}}"

# Run tests matching a pattern (e.g. `just test-match "test/sql/partitioning/*"`)
[group('test')]
test-match PATTERN:
    ./build/release/test/unittest "{{PATTERN}}"

# Run DuckDB core tests using DuckLake as storage backend
[group('test')]
test-attach:
    ./build/release/test/unittest --test-config test/configs/attach_ducklake.json --test-dir duckdb

# Run DuckLake tests using PostgreSQL as catalog database (requires running PostgreSQL)
[group('test')]
test-postgres:
    ./build/release/test/unittest --test-config test/configs/postgres.json

# Run DuckLake tests using SQLite as catalog database
[group('test')]
test-sqlite:
    ./build/release/test/unittest --test-config test/configs/sqlite.json

# Run tests with deletion vectors enabled
[group('test')]
test-deletion-vectors:
    ./build/release/test/unittest --test-config test/configs/deletion_vectors.json

# Run tests with an arbitrary test config (e.g. `just test-config test/configs/minio.json`)
[group('test')]
test-config CONFIG:
    ./build/release/test/unittest --test-config "{{CONFIG}}"

# Full CI check: build then run unit tests
[group('test')]
ci: build test
