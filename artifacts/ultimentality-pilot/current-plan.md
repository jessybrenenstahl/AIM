## Current Plan

AIM clean-slate recovery — April 3, 2026.

### Phase: Clean Baseline → CLI/Gateway Seam

1. Apply `CLEANUP_GUIDE.md` to the three restored crates (`splcw-llm`, `splcw-orchestrator`, `splcw-operator-gui`).
2. Lift the three patch modules (`host_verify_retry`, `gap_task_emitter`, `compaction_publisher`) directly into `splcw-orchestrator`.
3. Wire always-on operating memory injection (`os.md`, `memory.md`) into `run_runtime_turn`.
4. Define the CLI/gateway-backed session seam in `splcw-orchestrator` (import OpenClaw lane serialization posture).
5. Verify: `cargo build` passes, `cargo test` passes, no `NATIVE_CODEX_ENGINE_LABEL` in any source.

### Next After Clean Baseline

- Deepen auth controller lifecycle (OpenClaw-style profile rotation, cooldown, refresh scheduling).
- Harden compaction continuity transactions (mirror publish, snapshot/retry/fallback).
- Strengthen fail-closed verification (stabilization retry now lifted from patches).
- Begin thread-first continuity beyond recent-turn summaries.

### What Not To Do

- Do not add GUI features before the session seam is defined.
- Do not expand macOS parity before continuity is harder.
- Do not add more operator surface breadth before the harness core is trustworthy.
