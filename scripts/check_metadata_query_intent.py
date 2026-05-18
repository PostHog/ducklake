#!/usr/bin/env python3

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

QUERY_IMPLEMENTATION = re.compile(r"\b[A-Za-z_][A-Za-z0-9_:<>]*::Query\s*\(")
MEMBER_QUERY_CALL = re.compile(r"(?:\b[A-Za-z_][A-Za-z0-9_]*|\))\s*(?:->|\.)\s*Query\s*\(")

FORBIDDEN_DECLARATION = re.compile(
    r"^\s*(?:virtual\s+)?unique_ptr<QueryResult>\s+Query\s*\(",
    re.MULTILINE,
)

DECLARATION_HEADERS = [
    "src/include/storage/ducklake_transaction.hpp",
    "src/include/storage/ducklake_metadata_manager.hpp",
    "src/include/metadata_manager/postgres_metadata_manager.hpp",
]

SOURCE_ROOTS = [
    "src/include",
    "src/functions",
    "src/metadata_manager",
    "src/storage",
]

ALLOWED_MEMBER_QUERY_CALLS = {
    # Raw DuckDB Connection::Query call for checkpointing.
    "src/storage/ducklake_checkpoint.cpp": [
        re.compile(r"\bconn\s*->\s*Query\s*\(\s*checkpoint_query\s*\)"),
    ],
    # The single low-level DuckLake metadata runner. Higher-level metadata code
    # must go through Execute, SnapshotQuery, CurrentQuery, or RawQuery.
    "src/storage/ducklake_transaction.cpp": [
        re.compile(r"\bconnection\s*\.\s*Query\s*\(\s*query\s*\)"),
    ],
}


def iter_source_files():
    for root in SOURCE_ROOTS:
        base = ROOT / root
        if not base.exists():
            continue
        for path in base.rglob("*"):
            if path.suffix in {".cpp", ".hpp", ".h"}:
                yield path


def blank_preserving_newlines(text):
    return "".join("\n" if char == "\n" else " " for char in text)


def strip_comments_and_literals(text):
    result = []
    i = 0
    length = len(text)
    while i < length:
        char = text[i]
        nxt = text[i + 1] if i + 1 < length else ""

        if char == "/" and nxt == "/":
            end = text.find("\n", i + 2)
            if end == -1:
                result.append(" " * (length - i))
                break
            result.append(" " * (end - i))
            result.append("\n")
            i = end + 1
            continue

        if char == "/" and nxt == "*":
            end = text.find("*/", i + 2)
            if end == -1:
                result.append(blank_preserving_newlines(text[i:]))
                break
            result.append(blank_preserving_newlines(text[i : end + 2]))
            i = end + 2
            continue

        if char == "R" and nxt == '"':
            delimiter_start = i + 2
            delimiter_end = text.find("(", delimiter_start)
            if delimiter_end != -1:
                delimiter = text[delimiter_start:delimiter_end]
                terminator = ")" + delimiter + '"'
                end = text.find(terminator, delimiter_end + 1)
                if end != -1:
                    end += len(terminator)
                    result.append(blank_preserving_newlines(text[i:end]))
                    i = end
                    continue

        if char in {'"', "'"}:
            quote = char
            start = i
            i += 1
            escaped = False
            while i < length:
                current = text[i]
                if current == "\n":
                    i += 1
                    break
                if escaped:
                    escaped = False
                elif current == "\\":
                    escaped = True
                elif current == quote:
                    i += 1
                    break
                i += 1
            result.append(blank_preserving_newlines(text[start:i]))
            continue

        result.append(char)
        i += 1

    return "".join(result)


def is_allowed_member_query(path, line):
    rel_path = path.relative_to(ROOT).as_posix()
    return any(pattern.search(line) for pattern in ALLOWED_MEMBER_QUERY_CALLS.get(rel_path, []))


def add_matches(matches, path, pattern, reason, allow_member_queries=False):
    original_text = path.read_text(errors="replace")
    code_text = strip_comments_and_literals(original_text)
    original_lines = original_text.splitlines()
    code_lines = code_text.splitlines()
    for match in pattern.finditer(code_text):
        line_no = code_text.count("\n", 0, match.start()) + 1
        code_line = code_lines[line_no - 1] if line_no <= len(code_lines) else ""
        if allow_member_queries and is_allowed_member_query(path, code_line):
            continue
        original_line = original_lines[line_no - 1] if line_no <= len(original_lines) else code_line
        matches.append((path, line_no, reason, original_line.strip()))


def main():
    matches = []
    for rel_path in DECLARATION_HEADERS:
        path = ROOT / rel_path
        if path.exists():
            add_matches(matches, path, FORBIDDEN_DECLARATION, "ambiguous metadata Query declaration")

    for path in iter_source_files():
        add_matches(matches, path, QUERY_IMPLEMENTATION, "ambiguous Query implementation")
        add_matches(
            matches,
            path,
            MEMBER_QUERY_CALL,
            "direct Query call outside the low-level raw DuckDB execution allowlist",
            allow_member_queries=True,
        )

    if not matches:
        return 0

    print("Ambiguous DuckLake metadata Query API usage found.", file=sys.stderr)
    print("Use Execute, SnapshotQuery, CurrentQuery, or RawQuery instead.\n", file=sys.stderr)
    for path, line_no, reason, line in matches:
        rel_path = path.relative_to(ROOT)
        print(f"{rel_path}:{line_no}: {reason}: {line}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
