# postgres_scanner needs DONT_LINK because it depends on libpq/OpenSSL
if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(postgres_scanner
            DONT_LINK
            GIT_URL https://github.com/duckdb/duckdb-postgres
            GIT_TAG dd71d196cc1c512b3da508727e1e6ada308b6c4e
            SUBMODULES database-connector
            )
endif()
