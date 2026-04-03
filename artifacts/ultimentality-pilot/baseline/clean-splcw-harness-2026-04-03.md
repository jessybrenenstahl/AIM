# Clean SPLCW Harness Baseline — April 3, 2026

Single source of truth for the AIM clean-slate recovery.

## What Is Clean

These crates are copied verbatim from FFR and are considered architecturally sound:

| Crate | Role | Status |
|---|---|---|
| `splcw-core` | Doctrine records (plan, receipt, gap, recodification) | ✅ Clean |
| `splcw-memory` | Durable local truth, SQLite store, GitHub mirror offload | ✅ Clean |
| `splcw-computer-use` | Observation/action contract (ObservationFrame, ProposedAction) | ✅ Clean |
| `splcw-host` | PC body: Windows backend + macOS path | ✅ Clean |

## What Is Restored (Needs Code Surgery)

These crates are present but must have drifted code removed before use. See `CLEANUP_GUIDE.md`.

| Crate | Keep | Remove |
|---|---|---|
| `splcw-llm` | Auth controller, auth profiles, OpenAI OAuth, provider seam | Native Codex endpoint as *primary* engine framing; `OpenAiCodex` variant as a first-class product identity rather than an auth path |
| `splcw-orchestrator` | Receipt, gap, compaction, continuity, session hardening logic | Any code that hard-wires the GUI as the only consumer; any code that references `patches/` |
| `splcw-operator-gui` | Monitoring panel, Run Turn, background loop control, auth readiness | Self-identification as "Native Codex runtime"; `NATIVE_CODEX_ENGINE_LABEL`; framing that treats the GUI as the engine rather than the frontend |

## What Is New

| File | Purpose |
|---|---|
| `artifacts/ultimentality-pilot/memory/os.md` | Always-on OS context injection |
| `artifacts/ultimentality-pilot/memory/memory.md` | Always-on working memory surface |
| `CLEANUP_GUIDE.md` | Precise code surgery instructions for Codex |
| This file | Baseline truth record |

## Corrected Center of Gravity

The session layer is **not** the GUI. It is a CLI/gateway/app-server seam that inherits behavior from OpenClaw and RustyClaw. The GUI is an operator frontend only.

The harness-native substrate — host body, verification, continuity, recodification — is the unique value layer. Everything else wraps or extends it.

## Next Immediate Moves (after cleanup)

1. Define the CLI/gateway-backed agent/session seam in `splcw-orchestrator`.
2. Wire always-on operating memory injection into the turn prompt.
3. Continue fail-closed verification hardening (stabilization retry from `patches/` scaffolding can be lifted into `splcw-orchestrator` directly now that `patches/` is gone).
4. Continue compaction continuity hardening.
5. Import OpenClaw-style lane serialization and auth controller lifecycle.
