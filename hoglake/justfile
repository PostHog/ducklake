# hoglake — top-level recipes composing the per-component justfiles.
# `just server test`, `just pyhoglake unit`, `just test-all`, ...

mod server
mod pyhoglake
mod webui
mod hedgerow
mod bench

default:
    @just --list

# Full verification: server then client suites (client integration skips unless a server is up; `just dev` first for end-to-end)
test-all:
    just server test
    just pyhoglake test

# Style gates: ktlint (server) + ruff check/format (pyhoglake, hedgerow),
# every tool pinned. bench is not wired in yet — it passes `ruff check`
# but carries a formatting backlog; see AGENT.md open items.
lint-all:
    just server lint
    just pyhoglake lint
    just hedgerow lint

# Bring up the dev stack and the server (foreground).
dev:
    just server compose-up
    just server run
