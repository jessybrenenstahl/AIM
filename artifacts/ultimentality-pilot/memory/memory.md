# Working Memory

This file is injected into every runtime turn as part of always-on operating memory.

## Canonical Injected Surfaces (every turn)

The runtime turn prompt must include all of the following:

| Surface | Path | Injection method |
|---|---|---|
| OS context | `artifacts/ultimentality-pilot/memory/os.md` | Read and prepend to system prompt |
| This file | `artifacts/ultimentality-pilot/memory/memory.md` | Read and prepend to system prompt |
| Current plan | `offload/current/plan.md` | Read and inject as "Current Plan" section |
| Open gaps | `offload/current/open-gaps.md` | Read and inject as "Open Gaps" section |
| Handoff | `offload/current/handoff.md` | Read and inject as "Handoff" section |
| Recent receipts | SQLite (last 5) | Injected by `splcw-memory` hydration |
| Thread context | session-state.json threads | Injected by `splcw-orchestrator` journal |

## Compact Runtime Context Format

When context window is tight, collapse the above into:

```
[os] {platform} | {host_backend_status}
[plan] {active_module}: {objective_summary}
[gaps] {open_gap_count} open | lead: {lead_gap_title}
[last_receipt] {receipt_summary}
[thread] {foreground_thread_id}: {recent_turns_summary}
```

## Recodification Trigger

When a capability gap appears in 3 or more consecutive turns with the same `title`, it must be escalated to a `Recodification` record rather than treated as a one-off obstacle.

## Memory Flush Policy

Before each compaction:
1. Flush receipts, gaps, and recodifications from in-memory buffers to SQLite.
2. Write updated `offload/current/` surfaces.
3. Only then begin compaction.

## Session Identity

Session ID is stable across compactions. Transcript path rotates. The session ID is the continuity anchor, not the transcript path.
