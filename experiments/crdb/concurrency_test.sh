#!/usr/bin/env bash
# Two concurrent writers appending to the same DuckLake table backed by CockroachDB.
cd "$(dirname "$0")/../.." || exit 1
DUCKDB=build/release/duckdb
ATTACH="LOAD postgres_scanner; SET GLOBAL pg_use_text_protocol = true; SET GLOBAL pg_use_ctid_scan = false; ATTACH 'ducklake:postgres:dbname=ducklakedb host=localhost port=26257 user=root' AS lake (DATA_PATH 'experiments/crdb/data/');"
RESULTS=experiments/crdb/results
N=${N:-15}

$DUCKDB -unsigned -c "$ATTACH CREATE TABLE IF NOT EXISTS lake.conc (writer INTEGER, seq INTEGER);" >/dev/null 2>&1

writer() {
  local wid=$1
  local fails=0
  for i in $(seq 1 "$N"); do
    if ! $DUCKDB -unsigned -c "$ATTACH INSERT INTO lake.conc VALUES ($wid, $i);" >>"$RESULTS/conc_w$wid.out" 2>&1; then
      fails=$((fails+1))
    fi
  done
  echo "writer $wid: $fails/$N failed"
}

writer 1 & writer 2 & writer 3 &
wait
$DUCKDB -unsigned -c "$ATTACH SELECT writer, count(*) FROM lake.conc GROUP BY writer ORDER BY writer; SELECT count(*) AS total FROM lake.conc;"
