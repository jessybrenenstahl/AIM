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

### Immediate Next Work

1. Make the Codex CLI lane the default usable path in AIM even when no legacy OAuth profile is present in operator state.
2. Rework the GPUI shell toward a real workbench:
   - transcript-first layout
   - clearer CLI session proof
   - better compose/reply flow
   - less admin-dashboard framing
3. Audit memory/tool grounding end-to-end so every CLI turn gets the canonical operating memory without relying on brittle heuristics.
4. Only after the operator loop is humane and grounded, continue deeper continuity/substrate work.

### What Not To Do

- Do not reintroduce `FFR` repo assumptions or branch names.
- Do not grow fallback-provider UX into the primary path.
- Do not add more surface breadth if it hides whether Codex CLI is actually connected and grounded.
