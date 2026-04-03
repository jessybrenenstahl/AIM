# Architecture

## Doctrine Translation

The harness is structured around five recursive SPLCW units:

- Warden
- Captive
- Logician
- Poet
- Sculptor

Each unit participates in the recurrent dimensions of:

- perception
- action
- planning
- growth
- continuity

## Workspace Crates

### `splcw-core`

Shared doctrine-aligned types:

- plans
- receipts
- invariants
- contradictions
- capability gaps
- recodification records

### `splcw-memory`

Local durable truth:

- SQLite-backed metadata index
- append-only receipt/event logs
- capability registry
- offload adapters for GitHub-backed memory verification

### `splcw-computer-use`

Computer-use reasoning loop:

- observations
- screenshots
- OCR and structured perception hooks
- planner/action proposals
- post-action verification contracts

Current implementation status:

- richer observation frames for active window, OCR, clipboard, and structured signals
- broader action surface for click, drag, type, hotkey, scroll, launch, wait, and capture

### `splcw-host`

Host integration for the real body:

- window/process state
- screenshot capture
- input dispatch
- clipboard
- UIA/selector interfaces
- OCR/image anchoring

Current implementation status:

- `CommandHostBody` gives Rust a stable observe/enact/verify body contract over external control backends
- `splcw-host-backend` now provides a concrete Windows backend and an explicit macOS Apple Silicon path
- Windows focus verification now uses bounded stabilization plus explicit verification signals that the runtime contradiction path can trust
- Windows launch and pointer actions now also emit explicit post-action verification signals that the runtime can contradict on, with `LaunchProcess` now requiring a fresh process delta or spawned-child lineage from the anchored spawned PID instead of accepting pre-existing process-name presence and pointer actions requiring target reach plus visible or focus effect instead of only positional success
- macOS `FocusWindow` and `LaunchProcess` now emit the same structured verification contract the runtime already consumes on Windows, with bounded focus stabilization and launch proof gated on a fresh process delta or spawned-child lineage from the anchored spawned PID instead of raw observation fallback
- Windows keyboard actions now carry pre-input anchor evidence and emit explicit verification signals, with `TypeText` now requiring observable input effect such as focused-control value/text change, window change, or clipboard change, while `Hotkey` now requires an observable world-state effect such as window, clipboard, focused-control value, or screenshot change instead of counting bare focus preservation or control-shift-only evidence as proof; full shortcut-effect proof still remains beyond the current surface
- macOS keyboard actions now emit explicit verification signals too, and the Apple Silicon path can now use focused-control value or text evidence when Accessibility data is readable; when it is not, truth still falls back to observable window, clipboard, or viewport delta instead of pretending to prove exact input effect
- macOS pointer actions now use a dedicated Swift/Quartz sidecar while focus/keyboard/launch remain on the AppleScript control plane, and that bounded split is deliberate: `Click`, `DoubleClick`, `Drag`, and `Scroll` now emit explicit pointer/scroll verification signals with truthful success able to use focused-control shift proof when Accessibility data is readable, while capability advertising stays gated on local Swift toolchain availability instead of claiming pointer support everywhere; actual actuation still depends on the host granting the expected macOS accessibility/input permissions at runtime
- Windows scroll actions now carry pre-scroll anchor evidence and emit explicit verification signals, with truthful success now requiring observable viewport or focus change through screenshot delta, window change, or focused-control shift evidence instead of blind scroll success
- `WaitFor` now emits explicit post-action verification signals instead of relying on observation-only contradiction fallback, so matched waits and timed-out waits become explicit host/runtime facts rather than inference from summary text
- broader macOS exact-effect proof beyond the current focused-control/window/viewport pointer receipts and focused-control/window/clipboard/viewport keyboard receipts remains the next step

### `splcw-llm`

Provider and auth substrate:

- provider-neutral chat/message IR
- durable auth profile store
- OAuth/API-key capable provider descriptors
- future adapter seam for OpenAI Codex and other providers

Current implementation status:

- durable file-backed auth profile storage exists
- OpenAI Codex is modeled as an OAuth-capable provider descriptor
- live OpenAI-compatible Responses adapters now exist for `openai-codex` and `openai-api`
- stored OAuth bearer tokens or API keys can now drive real model calls through the provider seam
- controller cooldowns and last-success profile state are now durably persisted through the auth store and reloaded on client restart
- expired OAuth bearer state and refresh-only OAuth profiles are no longer only filtered before runtime provider calls: the client can now ask the provider seam to materialize fresh runtime auth from a stored refresh token, persist the updated profile, and continue with the materialized credential when that refresh succeeds
- still-valid OAuth profiles that are close to expiry now also get a best-effort proactive refresh pass during controller-side profile resolution, while failed proactive refresh falls back to the currently valid token instead of blocking the turn
- successful calls now also arm a process-local timer-driven refresh task per profile, with per-profile single-flight gating, one bounded scheduled retry, and stale-generation/drop guards so older timer tasks cannot overwrite newer schedule state
- OpenAI-compatible providers can now also begin and complete both browser-callback OAuth with PKCE and device-code OAuth, with durable pending authorization records, automatic callback-first browser completion plus pasted-redirect or raw-code fallback for browser flows, device verification URI/user code plus token-endpoint polling for device flows, and auth-store persistence for the completed profile; OpenAI Codex now carries a built-in interactive OAuth client id by default while `artifacts/ultimentality-pilot/operator/operator.env` remains available for overrides, OpenAI API client ids, and OpenClaw import-path hints so direct packaged launches do not depend on a custom launch shell
- the client can now also resume auth lifecycle ownership at startup: restore runtime health, clean expired pending OAuth entries, materialize refresh-only profiles before the first turn, and arm background refresh scheduling for ready profiles without waiting for a successful model call
- the client can now also explicitly start a resident auth-lifecycle loop that periodically re-runs lifecycle resume over time, cleaning newly expired pending OAuth entries and maintaining auth readiness without requiring turn traffic
- completed OAuth profiles now also get adopted into that resident lifecycle immediately, so browser/device authorization no longer has to wait for the next sweep or first successful model call before refresh scheduling is armed
- resident auth lifecycle control now also governs scheduled refresh ownership, so stopping the resident lifecycle aborts outstanding scheduled refresh tasks and prevents new background refresh arming until the lifecycle is started again
- resident auth start is now idempotent too, so repeated start calls become a true no-op while the loop is already running instead of re-sweeping auth state or churning scheduled refresh generations underneath that control path
- resident auth loop generations now also act as real lease tokens, so concurrent starts collapse onto one live resident loop generation, stale generations stand down without waiting for abort timing, and teardown invalidates resident loop ownership explicitly
- external refresh arming is now schedule-stable too, so repeated lifecycle resumes or repeated successful calls no longer rebuild a live per-profile refresh task when the target expiry is unchanged
- scheduled refresh tasks now also carry resident lifecycle generation, so stale refresh work stands down across resident lifecycle epoch changes instead of only checking per-profile scheduler generation
- resident lifecycle sweeps and direct refresh entrypoints now also hand off one captured resident epoch all the way into refresh arming, so the scheduler no longer re-reads mutable resident lifecycle state mid-path to decide task ownership
- resident lifecycle sweeps now also serialize through one single-flight gate, so manual resume, zero-interval start, and resident loop ticks cannot overlap pending cleanup, runtime materialization, or refresh arming work
- stale resident lifecycle sweeps now also stop before persisting freshly materialized profiles or continuing cleanup/arming work once they lose resident epoch ownership mid-flight
- refresh-task orchestration is now also unified across immediate arming, resident lifecycle sweeps, and timer-driven rescheduling, so those paths share one internal spawn-or-replace scheduler path instead of duplicating task orchestration logic
- resident loop continuation checks now also route through one internal helper, so the loop no longer duplicates its alive/background-enabled/generation gate inline at each breakpoint
- lifecycle-side eligible-and-arm flow now also routes through one internal helper, so resident lifecycle resume no longer open-codes its background-enabled plus refresh-eligibility routing before refresh arming
- scheduled refresh workers are now also background-auth-gated, so timer-driven refresh tasks recheck resident background-auth ownership before persisting refreshed profiles, retrying, or rearming the next schedule instead of only trusting arming-time state
- resident lifecycle resume now also reconciles scheduled refresh ownership per profile, so blocked or no-longer-refreshable profiles have stale scheduled refresh tasks cleared immediately instead of waiting for a later timer wake-up
- scheduled refresh spawn-time ownership checks now also route through the same helper family as timer-worker continuation, so background-auth, liveness, and resident-epoch ownership no longer diverge between scheduler entry and scheduler execution paths
- immediate refresh arming, lifecycle-side schedule reconciliation, and schedule-time spawning now also share one internal `cancel`/`noop`/`arm` schedule-decision helper, so refresh timing policy no longer partially diverges across those auth-control paths
- controller-side proactive refresh and scheduler-side “refresh now vs defer” decisions now also share one internal runtime refresh planner, so ready-profile refresh timing policy no longer diverges between the controller path and the timer path
- controller-side and scheduler-side paths now also classify ready/needs-refresh/blocked auth state through one internal runtime refresh-action planner, so auth readiness no longer branches through separate controller-vs-scheduler action logic before side effects are applied
- direct arming and timer-driven rescheduling now also share one internal scheduled-refresh replacement decision, so unchanged targets are reused and live timer generations are advanced through the same policy instead of carrying slightly different replacement semantics
- OpenAI-compatible providers now also surface typed provider/operation/category/status context on transport and HTTP failures, and the auth controller consumes that structure before falling back to legacy text heuristics
- further resident auth centralization beyond the current process-local but now generation-backed, background-auth-gated, schedule-stable, resident-epoch-aware, captured-epoch, single-flight, stale-sweep-guarded, internally unified refresh-task, helper-routed lifecycle/scheduler ownership, centralized refresh schedule-decision control plane, and shared scheduled-refresh replacement policy, plus broader provider-specific OAuth expansion, still remain the next step

### `splcw-orchestrator`

Task and worker coordination:

- task DAGs
- bounded child workers
- single-body actuation lease
- parent/child receipts
- capability-gap escalation into recodification

Current implementation status:

- the orchestrator can now hydrate from local truth and publish plan snapshots, receipts, gaps, and recodifications through the local store and mirror sink
- a bounded runtime turn is now implemented: hydrate -> observe -> model call -> host action or capability gap -> verify -> receipt/gap record
- the runtime now also carries a first `open-multi-agent`-style bounded inner tool loop, so one turn can continue through serialized tool-result rounds before surfacing back out
- host enact/verify failures inside that bounded turn now surface as capability gaps instead of aborting the turn outright, so continuity stays alive when the body misbehaves
- bounded runtime turns now also publish current-surface continuity and checkpoint manifests automatically
- the first continuity shell slice is now implemented: transcript write locking with same-process reentrancy, stale-lock reclaim, malformed-tail repair, transcript prewarm, runtime session journal events, and explicit runtime-owned stale-lock cleanup before bounded turns or orchestrator-owned compaction open a session
- the continuity shell now also supports first-open bootstrap repair: if `state.json` is missing or corrupt but transcript/turn-log continuity still exists, the session layer reconstructs durable state from those artifacts instead of starting blank
- the continuity-state foundation now persists compaction counters, memory-flush markers, startup-summary injection markers, and transcript rotation metadata so the later compaction path has durable state to stand on
- the executable compaction foundation now preserves a recent event tail, rotates the transcript into an archive, persists a queued post-compaction refresh, and injects that refresh into the next runtime turn
- compaction now also leaves behind a recoverable transaction snapshot while in flight, and the session layer repairs from that snapshot on the next open if compaction was interrupted mid-transaction
- compaction attempts now run under a bounded deadline and one retry, restoring from the durable snapshot before retry/failure so a timed-out compaction does not strand the session in a half-mutated state
- the session layer now rewrites its active transcript identity during compaction, keys same-process writer serialization to stable session identity rather than transcript canonical path, and can bootstrap from the rotated transcript if `state.json` is later lost
- open-time continuity repair now also performs a structured turn-log policy pass, so duplicate bounded-turn records are deduped and broken thread IDs are normalized after truncation rather than merely salvaging valid JSONL lines
- that turn-log policy pass now also repairs provider-level tool/result pairing inside stored request transcripts, removing orphaned tool results, deduping duplicate results by call id, and stripping unresolved tool calls after truncation or compaction
- the first thread-continuity foundation now persists foreground/background threads, compacts backgrounded threads into summaries, and injects current-thread plus cross-thread context server-side before model dispatch
- the session layer now also persists an atomic bounded-turn ledger with exact request/response/action records, and the runtime injects recent turn history server-side into the next model prompt
- the orchestrator now owns a compaction wrapper that republishes current-surface continuity and emits checkpoint manifests with the concrete compaction artifact paths after session compaction, so compaction outside a bounded runtime turn no longer leaves offloaded truth stale
- host/process and provider transport calls are now timeboxed, and bounded runtime turns now also wrap host observe/enact/verify with explicit deadlines, so a hung PowerShell/osascript/backend call, stuck host phase, or slow OpenAI-compatible Responses request cannot stall the runtime indefinitely
- the live provider seam now retries retryable transport failures once, classifies explicit HTTP status markers before fallback string heuristics, and converts provider-call failures into surfaced runtime capability gaps instead of aborting the turn
- session and auth durability now use atomic write/append paths, and the SQLite state store now wraps plan snapshot plus metadata persistence in one transaction
- continuity publication now also belongs to orchestrator mutations outside the bounded runtime turn, so adopt-plan, receipt, gap, and recodification writes refresh the current surface and checkpoint mirror automatically
- continuity publication now also reads back the latest mirrored checkpoint manifest after publish, verifies it against the just-generated watermark, and surfaces mirror drift as a durable capability gap without recursive publication loops
- contradiction detection is now part of the runtime turn, and the continuity shell now includes mirror-verification drift surfacing, but it is still shallower than OpenClaw’s full startup/lifecycle sweep discipline
- runtime verification is now also preserved as machine-decided turn state instead of raw host evidence only: bounded turns record verification `kind`, `ok`, `proof_level`, and summary into the turn ledger, receipts, and inner-loop tool results, explicitly mark unstable focus as non-progress, and still accept older host signals that do not yet emit a proof level
- bounded turns now also emit durable step-progress journal events, so each runtime round records start, action selection, execution, verification, and completion before the turn either continues, completes, or surfaces a gap
- bounded turns now also persist `pending-turn.json`, so compaction snapshot/recovery carries in-flight turn state, reopen can safely resume from `AwaitingProvider` or `AwaitingHostExecution`, and `HostEffectsUncertain` now fails closed instead of risking duplicate host action replay after a crash or relaunch
- `splcw-memory` offload checkpoint selection is now timestamp-correct and empty plan snapshot sections now render explicitly instead of collapsing silently, and the runtime/host pair now fail closed for required structured `FocusWindow`, `WaitFor`, and `CaptureObservation` verification instead of accepting legacy heuristic fallback. The next major certainty steps are deeper exact-effect proof plus richer continuity/publication behavior that stays correct across multi-step turns, compaction, and auth/profile rotation

### `splcw-operator-gui`

Direct operator launch and monitoring surface:

- native desktop window over the existing runtime crates
- bootstraps or updates a minimal runtime plan objective for direct operator testing
- runs a bounded runtime turn from one button instead of requiring a library harness
- monitors raw session files plus `offload/current/*` directly, so the UI does not mutate session state just by polling

Current implementation status:

- the first directly launchable GUI shell is now landed
- it now has a checked-in Windows launcher plus a packaging script that promotes a release exe into `artifacts/ultimentality-pilot/operator/dist/`, while still using the canonical repo, `offload/current`, and `artifacts/ultimentality-pilot/operator/` surfaces at runtime
- it discovers the canonical `FFR` repo root automatically
- it uses `artifacts/ultimentality-pilot/operator/` for state, auth, and session storage
- it now also owns first resident/background control files there, specifically `background-runner.json`, `background-stop.request`, `background-handoff.json`, and the derived `status.json` mirror, so detached loop ownership, pre-launch runner visibility, and replacement intent are visible outside the foreground window
- it reads `state.json`, `pending-turn.json`, `turn-log.jsonl`, and the live transcript event stream for monitoring
- it reads `offload/current/brief.md`, `plan.md`, `open-gaps.md`, and `handoff.md` for coarse continuity status
- it mirrors a derived operator `status.json` for out-of-band monitoring without reopening the runtime journal
- roadmap, autonomy, repo, branch, commit, and GitHub-oriented bounded turns now also receive an external context bundle from the canonical repo root, composed from current plan, roadmap, certainty superslice, current open gaps, current handoff, plus live git branch/head/upstream/origin/status/recent commits and, when the local `gh` CLI is authenticated, live remote GitHub repo summary plus recent PR and issue state, so the harness can reason from live local and remote project state during self-development turns instead of only from the window objective
- it now surfaces auth readiness and preflight over the durable auth store plus runtime-health state, so the operator shell can tell when the default profile is runnable before a turn starts, can separately report whether the currently selected interactive OAuth provider is configured in `operator.env` or only being satisfied by process env override, treats native OpenAI Codex sign-in as the primary path, and can auto-adopt or explicitly import an existing OpenClaw Codex auth store as fallback when the harness has no default local profile
- it now drives browser-callback and device-code OAuth initiation/completion through the existing `splcw-llm` substrate, including pending-flow display, automatic launch of browser or verification URLs when interactive OAuth is configured, explicit client-id readiness gating when it is not, in-window completion/poll actions, and automatic startup loading of `artifacts/ultimentality-pilot/operator/operator.env` before auth readiness is evaluated
- it can now bootstrap a plan objective, run one bounded turn, or drive a continuous bounded-turn loop with a stop path and configurable pause, while monitoring recent events, recent turn summaries, and clear auth gating in one window
- it can now also spawn and stop a detached background bounded-turn loop from the same packaged GUI binary, write a provisional runner record immediately after spawn so relaunch can see the worker before the child loop fully boots, persist richer runner metadata like stable runner id, launching shell identity, phase, last heartbeat, objective, pause, model, completed-turn count, and last detached-runner error, target stop requests to the specific active worker instance, preserve terminal or crashed runner records for later inspection, reconcile stale stop or obsolete handoff artifacts into an explicit crashed startup state instead of silently carrying old control files forever, clear stale runner truth automatically when heartbeat age proves the worker has died instead of only relying on tidy shutdown, also detect when the recorded detached-worker process has disappeared so crash reconciliation does not have to wait for heartbeat expiry alone, keep replacement handoff requests target-matched even after non-live runner exit so stale handoff intent does not become ready for the wrong worker, persist a replacement handoff request that a relaunched shell can inspect, adopt into its foreground launch form, and complete once the targeted runner is no longer live, relaunch a crashed or failed detached loop in one step from the preserved runner settings instead of forcing manual launch-form reconstruction first, and now require a relaunched shell to explicitly reattach before it controls a still-live worker whose original launching shell has disappeared
- that new external context path is now paired with a bounded supervised GitHub mutation lane: the runtime can request one operator-approved issue comment, issue assignee add, issue label add/remove, issue close, issue reopen, PR comment, PR assignee add, PR label add/remove, PR close, or PR reopen action at a time, the operator shell persists that request into `github-action-request.json`, records queued/applied/cleared/rejected transitions durably in `github-action-history.jsonl`, mirrors the current request plus latest lifecycle summary into `status.json`, keeps the latest settled request metadata visible after apply/reject/clear instead of collapsing back to generic history only, now also preserves a separate structured result excerpt or URL from the settled `gh` outcome, injects those operator surfaces back into future self-development context, suggests current-branch PRs plus keyword-ranked recent PRs or issues when the runtime still lacks a concrete target number, explains the manual fallback path when no trustworthy suggestion exists, and can apply the request through a tight `gh` allowlist even when the operator must first supply the concrete issue or PR number, but the harness still does not own unsupervised GitHub mutation, arbitrary repo-write authority, or full branch/PR workflow control from inside the runtime loop
- it is still a first operator surface: Windows packaging/launcher ergonomics plus a first detached background loop with pre-launch runner visibility, heartbeat-backed liveness, immediate missing-process crash detection, launch-shell ownership plus explicit reattach, runner-targeted ownership, target-scoped handoff ownership across abnormal exit, preserved live-vs-terminal-vs-crashed runner records, startup crash reconciliation, a first durable handoff request/completion path, and one-step relaunch from preserved crashed/failed runner state are now landed, but richer resident execution, deeper worker-kill or shell-kill recovery, cross-platform packaging parity, and a more polished multi-pane monitoring console still remain

## Core Invariants

- One authoritative local state core.
- One body actuator lease at a time.
- Every meaningful action produces a receipt.
- Every contradiction is preserved explicitly.
- Every obstacle becomes a capability-gap record before retry.
- GitHub mirrors continuity; it does not replace local truth.
