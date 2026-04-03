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

## Immediate Next Action

Apply CLEANUP_GUIDE.md. Start with splcw-operator-gui (smallest change), then splcw-llm, then splcw-orchestrator (lift patch modules in, wire memory injection).
