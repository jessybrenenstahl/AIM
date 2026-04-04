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
13. Made the session lane directly reusable:
   - assistant reply cards now expose `Use as Draft`
   - session-card interaction now feeds the composer without leaving the transcript lane
   - the workbench is starting to behave more like a real client instead of a read-only conversation viewer
14. Expanded in-lane session actions:
   - assistant reply cards now support `Append` as well as `Use as Draft`
   - user cards now support `Reuse Prompt`
   - the transcript can now actively shape the composer in both directions instead of acting like static history
15. Upgraded the CLI session lane into real prompt/reply history:
   - Codex CLI conversation now renders stored historical objectives paired with their replies
   - the lane reads oldest-to-newest, with the current draft at the tail instead of at the top
   - historical user prompts can now `Run Again`, while the live draft can `Send Draft` directly from the lane
16. Added real active-turn stream state for packaged Codex CLI runs:
   - `artifacts/ultimentality-pilot/operator/codex-cli-live-stream.json` now appears during a live `--run-turn`
   - `status.json` now carries the current objective, updated timestamp, live text, live events, and live warnings from that stream
   - the Operate page now has an `Active Turn Stream` surface backed by the same live artifact
   - this was verified against the packaged `AGRO Harness Operator.exe`, not just the debug binary or unit tests
17. Captured the current streaming boundary explicitly:
   - the current Codex CLI `exec --json` path gives live warnings and coarse lifecycle events immediately
   - it also preserves the final `agent_message` into the live stream state
   - it does not currently appear to emit token-by-token text deltas on this path
18. Landed the first real resident Codex transport seam:
   - `splcw-orchestrator` now exposes a dedicated resident-session transport module instead of keeping the transport seam buried inline
   - `splcw-operator-gui/src/resident_transport.rs` now holds a GUI-side `ResidentCodexTransport` backed by a dedicated resident worker thread
   - that worker now keeps a real `codex app-server --listen stdio://` process alive across turns instead of pretending `exec/resume` is itself a session runtime
   - the new resident lane streams `thread/started`, `turn/started`, `item/agentMessage/delta`, tool-like item lifecycle, warnings, and `turn/completed` back into the live operator state
   - the old `exec --json` path is still retained as a fallback bridge if resident app-server startup or turn execution fails
19. Verified the resident lane against the real local Codex app-server:
   - regular `cargo test -p splcw-operator-gui` still passes
   - added an ignored real-transport probe that exercises `ResidentCodexTransport` directly against the installed logged-in Codex CLI app-server
   - that ignored probe passed locally, proving the resident transport is not just an abstraction sketch
20. Re-centered the live resident turn around the main session lane:
   - the Operate page now treats the resident reply stream as the primary conversation surface instead of duplicating it in a side diagnostic pane
   - live turns now show a `Current prompt` user card followed by a `Codex is responding` assistant card with a clear streaming pill
   - live event and warning lines now sit in compact footer metadata beneath the streaming reply instead of polluting the reply body itself
   - the right-rail `Turn Activity` panel is now explicitly supporting evidence, not the main text surface
21. Cleaned the session lane so historical Codex replies read like conversation instead of telemetry:
   - stored CLI reply cards now strip engine/session header bullets out of the main body and move them into compact footer metadata
   - raw `## CLI Warnings` sections are no longer dumped into the main conversation body
   - live resident cards now summarize warning presence instead of flooding the transcript with Codex-local warning text

### Immediate Next Work

1. Keep reworking the GPUI shell toward a real workbench:
   - keep reducing the remaining stacked dashboard-card behavior
   - make the session lane more live and interactive beyond historical prompt/reply controls
   - keep deepening the resident app-server stream so it feels like a first-class conversation, not just a better live card
   - continue separating proof/telemetry from reply content so the transcript stays humane under long-running use
   - make proof, grounding, and recovery read like an inspector lane instead of a second dashboard
2. Reduce operator friction around the live Codex CLI warnings:
   - investigate `C:\Users\jessy\.codex\state_5.sqlite` migration drift
   - decide whether to repair, isolate, or deliberately ignore those warnings in the operator presentation
3. Decide the next resident-session architecture step deliberately:
   - keep deepening the current `app-server` stdio lane
   - or move again toward a richer PTY/realtime lane only if `app-server` proves insufficient for truly first-class live text behavior
4. Only after the operator loop is humane and grounded, continue deeper continuity/substrate work.

### What Not To Do

- Do not reintroduce `FFR` repo assumptions or branch names.
- Do not grow fallback-provider UX into the primary path.
- Do not add more surface breadth if it hides whether Codex CLI is actually connected and grounded.
