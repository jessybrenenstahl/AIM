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
- the newest streaming pass proved that packaged Codex CLI turns now emit a live artifact at:
  - `artifacts/ultimentality-pilot/operator/codex-cli-live-stream.json`
- that live stream now carries:
  - active objective
  - updated timestamp
  - live warning/event lines during execution
  - final `agent_message` text once the CLI emits it
- `status.json` now mirrors those live-stream fields so the GPUI shell can surface them directly on the Operate page
- resident session correction landed:
  - `splcw-orchestrator` now exposes a dedicated resident transport module
  - `splcw-operator-gui/src/resident_transport.rs` now owns a concrete `ResidentCodexTransport`
  - that transport now runs a real `codex app-server --listen stdio://` process inside a dedicated resident worker thread
  - the worker keeps the process and thread alive across turns instead of pretending `exec/resume` is the session runtime
  - app-server notifications now feed the live session lane through:
    - `thread/started`
    - `turn/started`
    - `item/agentMessage/delta`
    - tool-like item lifecycle notifications
    - `turn/completed`
  - the old `exec --json` lane still exists as a fallback bridge if resident app-server startup or execution fails
- verification update:
  - `cargo test -p splcw-operator-gui` now passes with the resident transport extraction in place
  - added an ignored real-transport probe:
    - `resident_transport::tests::resident_codex_transport_runs_real_app_server_turn`
  - that ignored probe passed locally against the installed logged-in Codex CLI app-server
- newest live-session pass:
  - the resident stream now leads the main conversation lane instead of being duplicated as the primary content of a side inspector card
  - live turns now render as a `Current prompt` user card followed by a `Codex is responding` assistant card with a streaming status pill
  - live event and warning details now appear as compact footer metadata under the streaming assistant card
  - the former `Active Turn Stream` card is now a supporting `Turn Activity` inspector surface rather than a second competing transcript
- important current boundary:
  - the resident app-server lane is now real
  - the main session lane now foregrounds the live resident stream more naturally
  - but the overall transcript/composer rhythm still does not yet feel as naturally polished as OpenClaw / OpenCode

## Immediate Next Action

Make the CLI-first operator path genuinely humane:

1. keep reshaping the shell toward transcript/composer/proof workbench behavior until it feels like a real agent client instead of a dashboard
2. deepen the live resident stream interaction so it feels like active typing/progression, not just better live cards
3. reduce or properly contextualize the remaining Codex-local warnings (`state_5.sqlite` migration drift and PowerShell shell snapshot warnings)
4. decide whether anything beyond the current `app-server` resident lane is still needed for truly first-class live text behavior
