#!/usr/bin/env bash
# DuckLake + CockroachDB metadata store e2e test driver.
# Each test runs in its own duckdb process (also exercises re-attach).
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

run_test 01_update "UPDATE lake.t1 SET name = 'updated' WHERE id = 1; SELECT * FROM lake.t1 ORDER BY id;"

run_test 02_delete "DELETE FROM lake.t1 WHERE id = 2; SELECT count(*) FROM lake.t1;"

run_test 03_snapshots "FROM lake.snapshots();"

run_test 04_time_travel "
INSERT INTO lake.t1 VALUES (10, 'v-new');
SELECT count(*) AS now_count FROM lake.t1;
SELECT count(*) AS old_count FROM lake.t1 AT (VERSION => 2);"

run_test 05_schema_evolution "
ALTER TABLE lake.t1 ADD COLUMN score DOUBLE;
INSERT INTO lake.t1 VALUES (20, 'with-score', 1.5);
ALTER TABLE lake.t1 RENAME COLUMN name TO label;
SELECT * FROM lake.t1 ORDER BY id;
ALTER TABLE lake.t1 DROP COLUMN score;
SELECT * FROM lake.t1 ORDER BY id;"

run_test 06_type_promotion "
CREATE TABLE lake.promo (x INTEGER);
INSERT INTO lake.promo VALUES (1);
ALTER TABLE lake.promo ALTER COLUMN x SET DATA TYPE BIGINT;
INSERT INTO lake.promo VALUES (9223372036854775807);
SELECT * FROM lake.promo ORDER BY x;"

run_test 07_schemas_views "
CREATE SCHEMA lake.analytics;
CREATE TABLE lake.analytics.events (ts TIMESTAMP, event VARCHAR);
INSERT INTO lake.analytics.events VALUES ('2026-06-12 10:00:00', 'click');
CREATE VIEW lake.analytics.v_events AS SELECT event, count(*) AS n FROM lake.analytics.events GROUP BY event;
SELECT * FROM lake.analytics.v_events;"

run_test 08_transactions "
BEGIN;
CREATE TABLE lake.txn_test (i INTEGER);
INSERT INTO lake.txn_test VALUES (1), (2), (3);
COMMIT;
BEGIN;
INSERT INTO lake.txn_test VALUES (99);
ROLLBACK;
SELECT count(*) AS should_be_3 FROM lake.txn_test;"

run_test 09_types "
CREATE TABLE lake.types_test (
  a BOOLEAN, b TINYINT, c SMALLINT, d INTEGER, e BIGINT, f HUGEINT,
  g FLOAT, h DOUBLE, i DECIMAL(18,4), j VARCHAR, k BLOB,
  l DATE, m TIME, n TIMESTAMP, o TIMESTAMPTZ, p INTERVAL,
  q UUID, r INTEGER[], s STRUCT(x INTEGER, y VARCHAR), t MAP(VARCHAR, INTEGER));
INSERT INTO lake.types_test VALUES (
  true, 1, 2, 3, 4, 5, 1.5, 2.5, 123.4567, 'str', '\xDE\xAD'::BLOB,
  '2026-01-01', '12:34:56', '2026-01-01 12:34:56', '2026-01-01 12:34:56+00', INTERVAL 3 DAYS,
  'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', [1,2,3], {x: 1, y: 'two'}, MAP {'k': 1});
SELECT * FROM lake.types_test;"

run_test 10_inlining "
ATTACH 'ducklake:postgres:dbname=ducklakedb host=localhost port=26257 user=root' AS lake_inline (DATA_PATH 'experiments/crdb/data/', DATA_INLINING_ROW_LIMIT 10);
CREATE TABLE lake_inline.inlined (i INTEGER, s VARCHAR);
INSERT INTO lake_inline.inlined VALUES (1, 'inline-me');
SELECT * FROM lake_inline.inlined;
CALL lake_inline.flush_inlined_data();
SELECT * FROM lake_inline.inlined;"

run_test 11_partitioning "
CREATE TABLE lake.part_test (dt DATE, val INTEGER);
ALTER TABLE lake.part_test SET PARTITIONED BY (year(dt));
INSERT INTO lake.part_test VALUES ('2025-06-01', 1), ('2026-06-01', 2);
SELECT count(*) FROM lake.part_test WHERE year(dt) = 2026;"

run_test 12_compaction "
INSERT INTO lake.t1 (id, label) SELECT range, 'bulk' FROM range(100);
INSERT INTO lake.t1 (id, label) SELECT range + 100, 'bulk' FROM range(100);
CALL lake.merge_adjacent_files();
SELECT count(*) FROM lake.t1;"

run_test 13_expire_snapshots "
CALL ducklake_expire_snapshots('lake', older_than => now());
FROM lake.snapshots();"

run_test 14_cleanup "
CALL ducklake_cleanup_old_files('lake', cleanup_all => true);
CALL ducklake_delete_orphaned_files('lake', cleanup_all => true);
SELECT count(*) FROM lake.t1;"

run_test 15_table_changes "
INSERT INTO lake.t1 (id, label) VALUES (9999, 'change-tracked');
SELECT snapshot_id, rowid, change_type FROM lake.table_changes('t1', 0, 99999) WHERE change_type = 'insert' LIMIT 5;"

run_test 16_stats "
ANALYZE lake.t1;
SELECT * FROM lake.table_info('t1');"

echo "--- done ---"
