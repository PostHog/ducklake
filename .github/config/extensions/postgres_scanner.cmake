# Source-build of postgres_scanner, used only for LOCAL/manual builds that set ENABLE_POSTGRES_SCANNER
# (see extension_config.cmake). CI does NOT build it from source -- the Postgres test job installs the
# released extension via `INSTALL postgres FROM core` (see .github/workflows/Catalogs.yml), because a
# source build streams multi-table scans, which DuckDB v1.5.3 rejects.
#
# When building from source, pin to the postgres_scanner shipped with the DuckDB v1.5.3 release
# (6b2b12c), which materializes those scans. postgres_scanner needs DONT_LINK (depends on libpq/OpenSSL).
if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(postgres_scanner
            DONT_LINK
            GIT_URL https://github.com/duckdb/duckdb-postgres
            GIT_TAG 6b2b12cad3afef61e8a4637e714e8a88895fed1a
            SUBMODULES database-connector
            )
endif()
