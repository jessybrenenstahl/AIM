# Handoff

Clean AIM baseline initialized April 3, 2026.

Prior repo: `jessybrenenstahl/FFR` branch `codex/splcw-harness-foundation`
Reason for clean slate: architectural drift toward GUI-first / native-Codex-engine framing

## What Is Already Done

- Clean wellspring crates copied verbatim: splcw-core, splcw-memory, splcw-computer-use, splcw-host
- Restored crates present: splcw-llm, splcw-orchestrator, splcw-operator-gui
- CLEANUP_GUIDE.md specifies exact code surgery needed
- Always-on memory surfaces created: artifacts/ultimentality-pilot/memory/os.md + memory.md
- Baseline truth document: artifacts/ultimentality-pilot/baseline/clean-splcw-harness-2026-04-03.md
- Stronger operator GUI/main from FFR migrated into AIM and scrubbed of obvious repo-drift references
- AIM now has its own checked-in launcher and packaging scripts:
  - `ultimentality-pilot/harness/scripts/package-operator-gui.ps1`
  - `ultimentality-pilot/harness/scripts/launch-operator-gui.ps1`
  - `ultimentality-pilot/harness/Launch AGRO Harness Operator.cmd`
- Verified in AIM:
  - `cargo check -p splcw-operator-gui`
  - `cargo test -p splcw-operator-gui` (`106` passing)
  - packaged smoke test from `artifacts/ultimentality-pilot/operator/dist/AGRO Harness Operator.exe`

## Immediate Next Action

Make the CLI-first operator path genuinely humane:

1. remove the remaining auth/profile mismatch that makes Codex CLI look unavailable when it is actually logged in
2. reshape the shell toward transcript/composer/proof workbench behavior
3. audit the CLI context injection so it always consumes AIM's canonical operating memory instead of falling back to generic replies
