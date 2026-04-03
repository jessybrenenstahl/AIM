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
- grounded Codex CLI turns now complete successfully from both:
  - `ultimentality-pilot/harness/target/debug/splcw-operator-gui.exe --run-turn --engine-mode codex_cli ...`
  - `artifacts/ultimentality-pilot/operator/dist/AGRO Harness Operator.exe --run-turn --engine-mode codex_cli ...`
- the live CLI reply now proves the operating-memory bundle is being used and is persisted into:
  - `artifacts/ultimentality-pilot/operator/codex-cli-session.json`
  - `artifacts/ultimentality-pilot/operator/sessions/operator-main/codex-cli-turn-log.jsonl`
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
- the latest shell pass widened the main work surface and improved document rendering so replies with quotes, numbered steps, and fenced blocks survive the GUI more faithfully
- the next shell pass made the session chronology read naturally again and exposed the live CLI session more directly in the top-level workbench metadata
- the latest shell pass merged composer, run controls, and session settings into one continuous session workspace so Operate reads less like separate dashboard cards
- the newest shell pass replaced the transcript document box with message-style conversation cards so the session reads more like a real client conversation
- the latest shell pass replaced the Codex CLI proof markdown slab with a structured proof panel that surfaces availability, session, artifacts, and recent CLI events more directly
- the newest shell pass added direct keyboard interaction to the workbench: send prompt, start loop, and clear draft are now first-class operator actions
- the newest session-lane pass made assistant reply cards actionable: `Use as Draft` now feeds the composer directly from the transcript
- the newest session-lane pass expanded card actions further: assistant replies can now `Append`, and user cards can `Reuse Prompt`
- the newest dialogue pass upgraded the CLI session lane into real prompt/reply history using stored Codex CLI objectives, and historical user prompts can now `Run Again` directly from the lane

## Immediate Next Action

Make the CLI-first operator path genuinely humane:

1. keep reshaping the shell toward transcript/composer/proof workbench behavior until it feels like a real agent client instead of a dashboard
2. reduce or properly contextualize the remaining Codex-local warnings (`state_5.sqlite` migration drift and PowerShell shell snapshot warnings)
3. keep tightening the CLI proof surfaces so the GUI makes the live Codex session undeniable
