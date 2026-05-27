# postgres_scanner needs DONT_LINK because it depends on libpq/OpenSSL
if (NOT MINGW AND NOT ${WASM_ENABLED})
    # Pin to the postgres_scanner shipped with the DuckDB v1.5.3 release (extension network).
    # Older commits (c0e9256 / #5's dd71d196) stream multi-table scans, which DuckDB v1.5.3 rejects
    # ("Multiple streaming scans ... not currently supported"); 6b2b12c materializes them. This is
    # 29 commits ahead of dd71d196, so it keeps #5's connection-pool work.
    duckdb_extension_load(postgres_scanner
            DONT_LINK
            GIT_URL https://github.com/duckdb/duckdb-postgres
            GIT_TAG 6b2b12cad3afef61e8a4637e714e8a88895fed1a
            SUBMODULES database-connector
            )
endif()
