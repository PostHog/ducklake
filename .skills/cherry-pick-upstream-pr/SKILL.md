---
name: cherry-pick-upstream-pr
description: Cherry-pick a specific upstream PR from duckdb/ducklake onto a new branch on this fork, then build and test to verify compatibility. Triggers when the user says things like "cherry-pick PR 1234 from upstream", "pull in ducklabs PR #X", "backport upstream #X", or supplies a github.com/duckdb/ducklake/pull/N URL.
---

# Cherry-pick an upstream ducklabs PR onto this fork

This repo is PostHog's fork of `duckdb/ducklake`. Upstream is registered as the git remote `ducklabs`. Upstream squash-merges PRs, so each merged PR is exactly **one commit** on `ducklabs/main`.

## Inputs

The user will supply one of:
- a PR number (e.g. `1234`)
- a PR URL (e.g. `https://github.com/duckdb/ducklake/pull/1234`)
- a commit SHA on `ducklabs/main`

If anything is ambiguous, ask before proceeding.

## Procedure

### 1. Sync upstream
```bash
git fetch ducklabs
```

### 2. Resolve the PR to a commit SHA on ducklabs/main

Prefer `gh` — it's authoritative:
```bash
gh pr view <N> --repo duckdb/ducklake --json mergeCommit,state,title,url
```

If `state` is not `MERGED`, stop and tell the user.

Use `mergeCommit.oid` as the SHA. Verify it exists on `ducklabs/main`:
```bash
git merge-base --is-ancestor <SHA> ducklabs/main && echo "on main"
```

If the merge commit isn't on `ducklabs/main` (rare — happens for PRs that went to a release branch like `v1.5-variegata`), search by PR number in the message instead:
```bash
git log --oneline ducklabs/main --grep "(#<N>)"
```

### 3. Check we don't already have it
```bash
git log --oneline HEAD --grep "(#<N>)"
git branch --contains <SHA> 2>/dev/null
```
If we already carry it, stop and tell the user.

### 4. Create a fresh branch off the current branch
```bash
git checkout -b cherry-pick-upstream-<N>
```
Branch off whatever the user is currently on (typically `main` or a feature branch). Do **not** branch off `ducklabs/main`.

### 5. Cherry-pick with provenance
```bash
git cherry-pick -x <SHA>
```
`-x` appends `(cherry picked from commit <SHA>)` so the upstream origin stays traceable.

### 6. Handle conflicts (if any)

If `git cherry-pick` reports conflicts:
1. Run `git status` — list the conflicted files.
2. **Do not auto-resolve.** Stop and report the conflict list to the user with a brief diagnosis (which upstream commits we're missing that the picked commit assumes). Ask whether to (a) resolve manually, (b) abort, or (c) cherry-pick prerequisite commits first.
3. Resume only with explicit user direction. If they resolve manually, run `git cherry-pick --continue` once they confirm.

To abort cleanly:
```bash
git cherry-pick --abort
git checkout -          # back to the original branch
git branch -D cherry-pick-upstream-<N>
```

### 7. Build
```bash
just build
```
If the build fails:
- If it's a source-level conflict the cherry-pick masked (e.g. a function signature changed upstream that we don't have), explain and stop.
- Do not paper over build failures by editing the picked code without flagging it.

### 8. Test
```bash
just test
```
Capture pass/fail counts. If any tests fail:
- Identify whether the failures are in the same area as the picked change (likely the picked change depends on other upstream work) or unrelated (likely a pre-existing flake — verify by running `just test` on the original branch before the pick).
- Report findings; **do not** modify tests or production code to make them pass without checking with the user.

### 9. Report back

Use this template:

```
Cherry-pick result for upstream #<N>: <PR title>
  Upstream SHA:  <SHA>
  New SHA:       <new SHA>
  Branch:        cherry-pick-upstream-<N>
  Conflicts:     <none | list>
  Build:         <pass | fail — details>
  Tests:         <X passed / Y failed | fail — details>

Next: review the branch, then `git push -u origin cherry-pick-upstream-<N>` and open a PR if it looks good.
```

## Hard rules

- **Never push and never open a PR** without explicit confirmation. Report and stop.
- **Never `--force` anything** (push, reset, checkout).
- **Never skip hooks** (`--no-verify`, `--no-gpg-sign`).
- **Never add AI attribution** to commit messages — the user has a global rule against it.
- Don't modify the picked commit's message except for the `-x` line that git adds automatically. If a conflict resolution requires extra context, put it in the commit message via `git commit --amend` only after asking.
- If a test was already failing on the base branch before the pick, surface that — don't silently absorb it into "the pick is fine".
