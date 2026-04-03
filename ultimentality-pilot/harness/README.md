# SPLCW Pilot Harness

Rust workspace for the AGRO doctrine-aligned human-level PC pilot.

Course-corrected April 3, 2026. This is the clean AIM baseline.

## Corrected Center of Gravity

The harness is **not** a Codex GUI replacement. It is not a dashboard. The unique value is:

1. Full PC host control with fail-closed verification
2. Durable continuity through compaction and restarts
3. Contradiction/gap capture → permanent recodification

The session layer (CLI/gateway/app-server) inherits behavior from OpenClaw and RustyClaw. The GPUI shell is an operator frontend only — monitoring, Run Turn, background loop control.

## Crate Roles

| Crate | Role | Status |
|---|---|---|
| `splcw-core` | Doctrine records: SufficientPlan, Receipt, CapabilityGap, Recodification | ✅ Clean wellspring |
| `splcw-memory` | SQLite truth store, GitHub mirror offload, checkpoint manifests | ✅ Clean wellspring |
| `splcw-computer-use` | ObservationFrame, ProposedAction, ActionExecution contract | ✅ Clean wellspring |
| `splcw-host` | PC body: Windows Win32/UIA backend + macOS Quartz/Accessibility path | ✅ Clean wellspring |
| `splcw-llm` | Auth controller, OAuth lifecycle, provider seam, chat IR | 🔧 Restored — see CLEANUP_GUIDE.md |
| `splcw-orchestrator` | Runtime turn loop, receipt/gap/compaction, session hardening, continuity | 🔧 Restored — see CLEANUP_GUIDE.md |
| `splcw-operator-gui` | GPUI operator frontend — monitor, Run Turn, background control | 🔧 Restored — see CLEANUP_GUIDE.md |

## Always-On Operating Memory

Every runtime turn must inject:
- `artifacts/ultimentality-pilot/memory/os.md`
- `artifacts/ultimentality-pilot/memory/memory.md`

The orchestrator must read these files and prepend them to the system prompt before every model call.

## Build

```sh
cargo build --manifest-path ultimentality-pilot/harness/Cargo.toml
cargo test --manifest-path ultimentality-pilot/harness/Cargo.toml
```

## Key Docs

- `ARCHITECTURE.md` — crate boundaries and data flow
- `CLEANUP_GUIDE.md` (repo root) — exact code surgery for restored crates
- `artifacts/ultimentality-pilot/baseline/clean-splcw-harness-2026-04-03.md` — baseline truth
- `artifacts/ultimentality-pilot/roadmap.md` — phase roadmap
