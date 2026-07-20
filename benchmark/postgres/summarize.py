#!/usr/bin/env python3

import argparse
import json
import statistics
from collections import defaultdict
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("inputs", nargs="+", type=Path)
    parser.add_argument("--scale", default="xlarge")
    args = parser.parse_args()

    groups = defaultdict(list)
    for path in args.inputs:
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                row = json.loads(line)
                if row["scale"] == args.scale:
                    groups[(row.get("variant", row["adapter"]), row["operation"])].append(row)

    print(
        "operation\tfixture_rows\tresult_rows\tbase_s\tfeature_s\tchange\tspeedup"
    )
    operations = sorted({operation for _, operation in groups})
    for operation in operations:
        origin = groups.get(("v153_base", operation), [])
        generic = groups.get(("feature", operation), [])
        if len(origin) < 3 or len(generic) < 3:
            continue
        origin_s = statistics.median(row["elapsed_seconds"] for row in origin)
        generic_s = statistics.median(row["elapsed_seconds"] for row in generic)
        generic_change = 100 * (generic_s - origin_s) / origin_s
        speedup = origin_s / generic_s
        fixture_rows = int(statistics.median(row["fixture_rows"] for row in generic))
        result_rows = int(statistics.median(row["result_rows"] for row in generic))
        print(
            f"{operation}\t{fixture_rows}\t{result_rows}\t{origin_s:.6f}\t{generic_s:.6f}\t"
            f"{generic_change:+.2f}%\t{speedup:.2f}x"
        )


if __name__ == "__main__":
    main()
