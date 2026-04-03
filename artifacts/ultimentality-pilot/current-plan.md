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
7. Started the humane-shell correction:
   - widened the 4K work surface
   - improved reply rendering for quotes, numbered steps, and fenced blocks
   - pushed the Operate page further toward a transcript/proof workbench instead of a thin admin dashboard
8. Tightened interaction polish:
   - conversation now renders in chronological order instead of newest-first inversion
   - sidebar/header language now describe a CLI workbench instead of a generic control surface
   - top-level session chips now expose the live CLI session more directly
9. Merged the Operate page into a more continuous session workspace:
   - composer, run controls, and session settings now live in the same main workbench card
   - transcript sits directly beneath that workbench instead of feeling like a separate dashboard pane
   - recovery stays visible, but no longer competes with the session settings for primary attention
10. Replaced the transcript document box with a message-style session lane:
   - Codex replies now render as individual conversation cards
   - prompt drafts render as a distinct `You` message
   - each message can be copied directly from the session lane
11. Replaced the Codex CLI proof markdown block with a structured proof panel:
   - explicit availability/login/session evidence chips
   - artifact-backed path surface for the session file, turn log, and status snapshot
   - recent CLI event evidence stays visible without burying it in one big markdown slab
12. Added direct session interaction to the workbench:
   - `Ctrl+Enter` / `Cmd+Enter` sends the prompt
   - `Ctrl+Shift+Enter` / `Cmd+Shift+Enter` starts the loop
   - the composer now exposes a direct `Clear Draft` affordance and interaction hint inline

### Immediate Next Work

1. Keep reworking the GPUI shell toward a real workbench:
   - keep reducing the remaining stacked dashboard-card behavior
   - make the session lane more live and interactive beyond keyboard shortcuts and static message cards
   - make proof, grounding, and recovery read like an inspector lane instead of a second dashboard
2. Reduce operator friction around the live Codex CLI warnings:
   - investigate `C:\Users\jessy\.codex\state_5.sqlite` migration drift
   - decide whether to repair, isolate, or deliberately ignore those warnings in the operator presentation
3. Only after the operator loop is humane and grounded, continue deeper continuity/substrate work.

### What Not To Do

- Do not reintroduce `FFR` repo assumptions or branch names.
- Do not grow fallback-provider UX into the primary path.
- Do not add more surface breadth if it hides whether Codex CLI is actually connected and grounded.
