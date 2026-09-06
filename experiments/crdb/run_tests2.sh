#!/usr/bin/env bash
cd "$(dirname "$0")/../.." || exit 1
DUCKDB=build/release/duckdb
ATTACH="LOAD postgres_scanner; SET GLOBAL pg_use_text_protocol = true; SET GLOBAL pg_use_ctid_scan = false; ATTACH 'ducklake:postgres:dbname=ducklakedb host=localhost port=26257 user=root' AS lake (DATA_PATH 'experiments/crdb/data/');"
RESULTS=experiments/crdb/results
mkdir -p "$RESULTS"

run_test() {
  local name="$1"; shift
  local sql="$1"
  local out="$RESULTS/$name.out"
  if $DUCKDB -unsigned -c "$ATTACH $sql" >"$out" 2>&1; then
    echo "PASS $name"
  else
    echo "FAIL $name"
  fi
}

run_test 10b_flush_inlined "
ATTACH 'ducklake:postgres:dbname=ducklakedb host=localhost port=26257 user=root' AS lake_inline (DATA_PATH 'experiments/crdb/data/', DATA_INLINING_ROW_LIMIT 10);
INSERT INTO lake_inline.inlined VALUES (2, 'flush-me');
CALL ducklake_flush_inlined_data('lake_inline');
SELECT * FROM lake_inline.inlined ORDER BY i;"

run_test 13b_expire_by_version "
CALL ducklake_expire_snapshots('lake', versions => [3]);"

run_test 15b_table_changes "
SELECT snapshot_id, rowid, change_type FROM lake.table_changes('t1', 1, 4) LIMIT 5;"

run_test 16b_table_info "
FROM ducklake_table_info('lake');"

run_test 17_list_files "
FROM ducklake_list_files('lake', 't1');"

run_test 18_delete_with_dv "
DELETE FROM lake.t1 WHERE id >= 100 AND id < 150;
SELECT count(*) FROM lake.t1;"

run_test 19_rewrite_dv "
CALL ducklake_rewrite_data_files('lake', 't1');
SELECT count(*) FROM lake.t1;"

echo '--- done ---'
