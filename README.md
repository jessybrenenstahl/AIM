# AIM — AGRO In Motion

Clean-slate recovery of the SPLCW AGRO harness. Course-corrected April 3, 2026.

## What This Is

A Rust-native substrate for a persistent, human-level PC pilot. Not a dashboard. Not a Codex wrapper. Not a hand-rolled agent shell.

AGRO is:
- a durable operator body that can perceive, plan, act on, and verify a real desktop
- an always-on resident that survives compaction, restarts, and context limits
- a self-improving system where obstacles become permanent capability growth

## What Was Wrong (Course Correction)

The prior branch (FFR `codex/splcw-harness-foundation`) drifted into treating the native Codex GUI client and GPUI shell as the product center. That was the wrong center of gravity.

AGRO is **not**:
- a hand-rolled replacement for Codex, Claude Code, or OpenClaw
- a dashboard that tries to become an agent shell from scratch
- a GUI-first product whose visible breadth stands in for autonomy depth

## Corrected Architecture

```
CLI / gateway / app-server session layer  (inherit from OpenClaw / RustyClaw)
        ↓
harness-native substrate  (unique value layer)
  splcw-orchestrator  ← runtime turn loop, fail-closed verification, compaction
  splcw-host          ← full PC body (Windows + macOS)
  splcw-memory        ← durable local truth, GitHub mirror
  splcw-core          ← SPLCW doctrine records
  splcw-llm           ← auth controller, provider seam
  splcw-computer-use  ← observation/action contract
        ↓
GPUI shell  (operator frontend only — monitoring, Run Turn, background control)
```

The session layer must be inherited from OpenClaw / RustyClaw posture, not hand-rolled.

## Always-On Operating Memory

Every turn must inject:
- `artifacts/ultimentality-pilot/memory/os.md` — operating system context
- `artifacts/ultimentality-pilot/memory/memory.md` — working memory surfaces

## SPLCW Doctrine

Five units: Warden / Captive / Logician / Poet / Sculptor.
One body, serialized actuation.
Durable local truth → receipts → contradictions → gaps → permanent recodification.

## Key Docs

- `ultimentality-pilot/harness/ARCHITECTURE.md` — crate roles and boundaries
- `artifacts/ultimentality-pilot/baseline/clean-splcw-harness-2026-04-03.md` — single source of truth for this clean baseline
- `artifacts/ultimentality-pilot/roadmap.md` — phase roadmap
- `artifacts/ultimentality-pilot/results/goalscope-enactment-map.md` — reference repo to function map
- `CLEANUP_GUIDE.md` — exact code surgery Codex must apply to the restored crates
