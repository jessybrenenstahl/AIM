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
  - `cargo test -p splcw-operator-gui` (`110` passing)
  - packaged smoke test from `artifacts/ultimentality-pilot/operator/dist/AGRO Harness Operator.exe`
- top-level operator readiness now correctly treats a logged-in Codex CLI as ready even when no legacy fallback auth profile exists
- native/provider fallback readiness is now surfaced separately so the shell stops conflating primary CLI readiness with fallback profile state
- Codex CLI turns now receive an explicit operating-memory bundle built from:
  - `artifacts/ultimentality-pilot/memory/os.md`
  - `artifacts/ultimentality-pilot/memory/memory.md`
  - `artifacts/ultimentality-pilot/current-plan.md`
  - `offload/current/plan.md`
  - `offload/current/open-gaps.md`
  - `offload/current/handoff.md`
  - plus live repo/GitHub context when available
- the Operate page now surfaces that same grounding bundle in a visible `Prompt Grounding` pane

## Immediate Next Action

Make the CLI-first operator path genuinely humane:

1. reshape the shell toward transcript/composer/proof workbench behavior
2. run and inspect a real grounded Codex CLI turn to verify replies actually consume the operating-memory bundle
3. keep tightening the CLI proof surfaces so the GUI makes the live Codex session undeniable
