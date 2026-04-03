## Reference Synthesis

The earlier synthesis was directionally right but too architectural. The three reference repos are not just pattern banks; they are living harnesses with concrete runtime loops. The right reuse target is their enacted control behavior, not their labels.

## What Each Repo Actually Does

### `open-multi-agent`

Actual harness loop:

1. A coordinator agent decomposes a goal into task specs.
2. Specs are loaded into a dependency-aware queue.
3. A scheduler assigns work to agents.
4. Workers run their own LLM/tool loops.
5. Results are written into shared memory.
6. The coordinator synthesizes from completed task outputs.

Concrete anchors:

- `src/orchestrator/orchestrator.ts`
- `src/task/queue.ts`
- `src/orchestrator/scheduler.ts`
- `src/agent/runner.ts`
- `src/tool/executor.ts`
- `src/memory/shared.ts`

Keep:

- coordinator/meta-agent over a real task DAG
- queue as mutable execution truth, scheduler as assignment policy
- provider-neutral content-block IR
- explicit LLM -> tool -> tool-result loop in `AgentRunner`

Do not copy directly:

- prompt replay treated as memory
- duplicate orchestration surfaces (`Team` and `OpenMultiAgent`)
- weak dependency enforcement surviving into synthesis
- shared memory as mostly accumulated text instead of promoted durable artifacts

What it does not solve for our goalscope:

- no serious computer-use body
- no strong long-horizon continuity model
- no obstacle-to-capability promotion loop

### `openclaw`

Actual harness loop:

1. Ingress arrives through channels and gateway surfaces.
2. Sender identity is paired/authorized.
3. The message is routed to a session/agent context.
4. Auth profiles and model bindings are resolved for the run.
5. The embedded runner executes a turn with tools, compaction guards, fallbacks, and hook/context machinery.
6. Session state, memory state, and delivery context are persisted across turns.

Concrete anchors:

- `src/config/sessions/store.ts`
- `src/agents/pi-embedded-runner/run.ts`
- `src/agents/pi-embedded-runner/run/attempt.ts`
- `src/agents/model-auth.ts`
- `src/agents/auth-profiles/oauth.ts`
- `src/agents/pi-embedded-runner/run/auth-controller.ts`
- `extensions/memory-core/src/memory/manager.ts`
- `src/wizard/setup.ts`

Keep:

- local canonical truth with derived projections
- stable session identity distinct from route/index keys
- explicit auth-profile resolution and failure rotation
- compaction-aware continuity, not stateless reruns
- channel/gateway ingress as a real harness concern, not an afterthought

Do not copy directly:

- too many overlapping state planes
- process-local registries as hidden truth
- hook-heavy lifecycle coupling
- stringly session semantics and merge-heavy control flow

What it does not solve for our goalscope:

- it is not a full local computer-use pilot body
- desktop control is adjunctive, not the primary enacted substrate
- obstacle correction is not promoted into a first-class permanent-growth engine

### `RustyClaw`

Actual harness loop:

1. A Rust-native gateway/runtime hosts sessions, threads, tools, and background work.
2. Threads and subtasks are created as real operating units.
3. Runtime adapters and process/session substrate mediate execution capability.
4. Memory flush and compaction are explicit concerns.
5. Long-running daemons, channels, and tool/security layers keep the agent resident.

Concrete anchors:

- `crates/rustyclaw-core/src/runtime/traits.rs`
- `crates/rustyclaw-core/src/threads/manager.rs`
- `crates/rustyclaw-core/src/threads/model.rs`
- `crates/rustyclaw-core/src/threads/subtask.rs`
- `crates/rustyclaw-core/src/memory_flush.rs`
- `crates/rustyclaw-core/src/sessions.rs`
- `crates/rustyclaw-core/src/tools/helpers.rs`

Keep:

- Rust workspace split
- `RuntimeAdapter` seam as the capability boundary
- unified thread/subtask model
- explicit pre-compaction memory flush idea
- long-running process/session posture as part of the harness body

Do not copy directly:

- overlapping task/session/thread registries
- runtime abstraction that execution can bypass
- partial task-control wiring
- substrate claims that exceed real Windows strength

What it does not solve for our goalscope:

- no complete computer-use stack
- no fully unified single-body actuation discipline
- memory/continuity is promising but not yet the full doctrine of recodification

## The Real Combined Harness Target

The right synthesis is:

- `open-multi-agent` provides execution topology
- `openclaw` provides harness/control-plane behavior
- `RustyClaw` provides Rust-native runtime posture

The harness we are building should therefore enact this loop:

1. receive ingress or resume from durable state
2. hydrate the sufficient plan and current world state
3. resolve provider/auth binding for the next turn
4. ask the model for the next action under the current plan
5. execute through one serialized host body
6. verify the world-state change
7. record receipts, contradictions, and capability gaps durably
8. recodify when insufficiency is revealed
9. publish compact continuity artifacts into GitHub
10. continue rather than resetting

## What Our Current Branch Has vs Lacks

Already landed:

- durable truth core in `splcw-memory`
- broad action/observation contract in `splcw-computer-use`
- real Windows host backend and future macOS Apple Silicon path in `splcw-host`
- provider/auth substrate in `splcw-llm`
- live OpenAI-compatible Responses adapters in `splcw-llm` with stored bearer credential resolution
- GitHub mirror proof surface in `offload/`
- a first bounded runtime turn in `splcw-orchestrator` that hydrates, observes, calls the model, executes one host action, verifies, and records a receipt or capability gap

Still missing:

- OpenClaw-style lane serialization before model work
- OpenClaw-style auth controller behavior: profile ordering, refresh scheduling, failure rotation, cooldown state
- OpenClaw-style session repair/prewarm/lock pipeline
- OpenClaw-style compaction lifecycle with pre-compaction snapshot, retry/fallback, and post-compaction maintenance
- post-action verification strong enough to prove world-state, not just command success
- automated runtime publish/checkpoint flow into `offload/current`
- thread-first continuity patterns from RustyClaw where they are actually live
- first-class recodification loop that upgrades capability after failure

## Immediate Consequence

The next slice should stop adding only substrate and instead import the next live control behaviors:

- OpenClaw-style lane serialization and auth controller behavior
- OpenClaw-style session hardening and compaction lifecycle
- stronger contradiction-first verification receipts
- automated mirror publish/checkpoint emission from runtime execution
- explicit capability-gap promotion path
