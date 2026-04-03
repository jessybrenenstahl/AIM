## Current Plan

AIM source-of-truth recovery — April 3, 2026.

### Phase: CLI-First Operator Recovery

Completed in this slice:

1. Migrated the strongest operator shell from `FFR` into `AIM` and scrubbed repo/branch drift markers.
2. Restored a checked-in packaging and launcher path under `ultimentality-pilot/harness/`.
3. Repointed the CLI context bundle at AIM canonical memory/context surfaces:
   - `artifacts/ultimentality-pilot/memory/os.md`
   - `artifacts/ultimentality-pilot/memory/memory.md`
   - `artifacts/ultimentality-pilot/baseline/clean-splcw-harness-2026-04-03.md`
   - `ultimentality-pilot/harness/ARCHITECTURE.md`
4. Verified the operator crate in AIM with:
   - `cargo check -p splcw-operator-gui`
   - `cargo test -p splcw-operator-gui`
   - packaged smoke test from the checked-in operator executable
5. Fixed the two real CLI execution blockers in the operator path:
   - grounded Codex prompts now flow through stdin instead of a giant Windows command-line argument
   - the live `main()` now actually honors `--run-turn` instead of leaving that path stranded in the dead legacy entrypoint
6. Verified a real grounded Codex CLI turn from both the debug operator binary and the packaged operator executable:
   - reply proved the model read the operating-memory bundle
   - session state persisted into `artifacts/ultimentality-pilot/operator/codex-cli-session.json`
   - reply/event history persisted into `artifacts/ultimentality-pilot/operator/sessions/operator-main/codex-cli-turn-log.jsonl`

### Immediate Next Work

1. Rework the GPUI shell toward a real workbench:
   - transcript-first layout
   - clearer CLI session proof
   - better compose/reply flow
   - less admin-dashboard framing
2. Reduce operator friction around the live Codex CLI warnings:
   - investigate `C:\Users\jessy\.codex\state_5.sqlite` migration drift
   - decide whether to repair, isolate, or deliberately ignore those warnings in the operator presentation
3. Only after the operator loop is humane and grounded, continue deeper continuity/substrate work.

### What Not To Do

- Do not reintroduce `FFR` repo assumptions or branch names.
- Do not grow fallback-provider UX into the primary path.
- Do not add more surface breadth if it hides whether Codex CLI is actually connected and grounded.
