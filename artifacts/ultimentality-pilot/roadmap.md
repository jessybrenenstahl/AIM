# AGRO Roadmap

## Objective

Turn the three mined source repos plus real computer control into AGRO:

- a Rust-native SPLCW harness
- a persistent human-level PC pilot substrate
- one that can perceive, plan, act, verify, persist, and recodify

This is not a “merge the repos” plan.

It is a synthesis plan:

- `open-multi-agent` contributes execution topology
- `openclaw` contributes control-plane/runtime continuity behavior
- `RustyClaw` contributes Rust-native resident posture
- computer-control contributes the actual body

## Relaunch Start Point

This roadmap was originally written from a colder starting point than the current branch.

The current branch already has:

- one-body lane serialization
- a bounded runtime loop with a bounded inner tool loop
- a real Windows host backend and explicit future Mac Studio path
- a live OpenAI-compatible provider seam
- durable local truth and GitHub offload
- first session/thread/compaction continuity
- a recent robustness pass across timeouts, retries, atomic writes, and transactional persistence

So the relaunch should begin from the remaining control-plane and continuity transaction work, not by pretending the project is still pre-runtime.

## Governing Reading

AGRO is not a collection of modules.

It is a recursively integrated participatory system where:

- perception changes planning
- planning changes action
- action changes the lived construct of the world
- changed world-state becomes new symbolic intake
- revealed insufficiency becomes recodification

So the roadmap is ordered by the next highest-value enacted behaviors, not by abstract software layers.

## Source-To-Role Map

### `open-multi-agent`

Use for:

- coordinator pattern
- task DAGs
- worker decomposition
- bounded synthesis
- provider-neutral message/tool IR instincts

Do not use as:

- continuity model
- persistent memory model
- computer-use body

### `openclaw`

Use for:

- ingress and session routing
- lane serialization
- auth controller behavior
- session lock/repair/prewarm discipline
- compaction lifecycle
- continuity maintenance

Do not use as:

- primary desktop/body substrate
- doctrine of permanent capability growth

### `RustyClaw`

Use for:

- Rust-native resident posture
- gateway/daemon stance
- thread-first continuity
- provider normalization
- process/runtime residency
- pre-compaction memory flush

Do not use as:

- authoritative runtime abstraction without verification
- fragmented registry model
- lossy background-process continuity

### Computer Control

Use for:

- screenshots
- OCR
- UIA/accessibility
- keyboard/mouse
- window/process/clipboard control
- browser tools when applicable

This is the actual body.

Without this, AGRO is still only a control plane.

## Target Enacted Loop

AGRO should ultimately enact this loop:

1. receive ingress or resume from durable state
2. serialize one-body authority before model work
3. hydrate sufficient plan and current world-state
4. resolve provider/auth identity through an auth controller
5. reason over the current state
6. act through the body
7. verify the changed world-state
8. write receipts, contradictions, and capability gaps
9. recodify revealed insufficiency into durable growth
10. republish compact continuity state
11. continue rather than reset

## Roadmap Phases

### Phase 1: Complete The Body

Primary inputs:

- current `splcw-host`
- computer-control requirements
- selective RustyClaw runtime/process posture

Build:

- screenshots
- OCR
- active window + window enumeration
- focus/move/resize
- mouse move/click/double-click/right-click/drag/scroll
- keyboard text/hotkeys
- clipboard read/write
- app launch/close/process state
- UIA tree inspection and selector support
- browser surface when relevant

Success condition:

- one bounded runtime turn can observe, act, and verify against a real desktop app on Windows

### Phase 2: Harden The Runtime Spine

Primary inputs:

- current `splcw-orchestrator`
- current `splcw-host`
- current `splcw-memory`

Build:

- one-body lease discipline
- contradiction-first receipts
- runtime-owned receipt creation
- runtime-owned continuity publication
- state-proof verification rather than command-success reporting

Success condition:

- every successful or failed bounded turn leaves durable local truth plus compact continuity artifacts

### Phase 3: Import OpenClaw Lane Serialization

Primary input:

- `openclaw`

Build:

- session lane
- global lane
- pre-model serialization policy
- explicit contention rules for one-body actuation

Success condition:

- planning may parallelize, but world action is serialized by policy before model work touches the body

### Phase 4: Build The Auth Controller

Primary input:

- `openclaw`

Build:

- profile ordering
- default/explicit profile resolution
- auth-failure rotation
- cooldown handling
- refresh scheduling
- runtime auth materialization
- browser/device OAuth initiation

Success condition:

- provider identity is managed as a live controller, not a static secret lookup

### Phase 5: Session Hardening

Primary input:

- `openclaw`

Build:

- lock discipline
- session repair
- prewarm/open pipeline
- first-turn persistence hardening
- session metadata durability

Success condition:

- the harness assumes disk/runtime state can drift and repairs it before continuing

### Phase 6: Compaction Lifecycle

Primary inputs:

- `openclaw`
- `RustyClaw`

Build:

- pre-compaction snapshot
- timeout grace
- compaction retry/fallback
- post-compaction maintenance
- memory flush before compaction
- local-vs-mirror contradiction verification

Success condition:

- compaction becomes a continuity event, not a summarization hack

### Phase 7: Thread-First Continuity

Primary input:

- `RustyClaw`

Build:

- thread summaries
- switched-away thread compaction
- background-summary injection
- resident gateway/daemon posture
- long-running process state as real session state

Success condition:

- AGRO can maintain multiple work threads without collapsing continuity into raw transcript replay

### Phase 8: Multi-Agent Planning Topology

Primary input:

- `open-multi-agent`

Build:

- coordinator
- dependency-aware task graph
- bounded worker planning lanes
- synthesis back into the main sufficient plan

Rule:

- many minds may plan
- one body acts

Success condition:

- AGRO can decompose work without violating one-body authority or continuity truth

### Phase 9: Recodification Engine

Primary inputs:

- SPLCW doctrine
- all prior runtime/body/continuity layers

Build:

- obstacle classification
- missing-capability mapping
- selector/playbook/tool/recovery promotion
- regression creation
- retry under repaired plan

Success condition:

- repeated obstacle classes stop recurring as unresolved classes

### Phase 10: Resident Operator Surface

Primary inputs:

- OpenClaw posture
- RustyClaw posture

Build:

- gateway/daemon
- operator ingress
- pause/resume/stop
- supervision and status
- long-running runtime ownership

Success condition:

- AGRO is not just a harness crate set, but an always-on operating substrate

## Current Priority Order

The relaunch sequence should now be:

1. complete session hardening into a true runtime-owned continuity transaction
2. operationalize the compaction lifecycle with snapshot, timeout, retry/fallback, and queue/session rewrites
3. deepen the auth controller into refresh/runtime-health persistence and browser/device OAuth initiation
4. strengthen verification, contradiction capture, and capability-promotion so turns become reliably truth-tracking
5. deepen thread continuity beyond recent-turn summaries and keep it stable across compaction/identity rotation

Deprioritize for now:

- broad macOS parity beyond what is required to keep the future Mac Studio path viable
- wider multi-agent planning expansion before continuity correctness is stronger
- resident operator surface / channel UX work before the harness shell is harder to break

## What To Avoid

- treating the repos as code to merge instead of behaviors to transplant
- building more substrate without importing the next live control behaviors
- letting multi-agent planning outrun one-body discipline
- treating compaction as mere summarization
- treating obstacles as one-off repair events instead of capability-growth events

## Current Repo Mapping

The current repository already has the right early shape:

- `ultimentality-pilot/harness/crates/splcw-host` = body
- `ultimentality-pilot/harness/crates/splcw-computer-use` = observation/action contract
- `ultimentality-pilot/harness/crates/splcw-llm` = provider/auth/controller substrate
- `ultimentality-pilot/harness/crates/splcw-orchestrator` = runtime turn and later coordination shell
- `ultimentality-pilot/harness/crates/splcw-memory` = durable local truth + mirror verification
- `ultimentality-pilot/harness/crates/splcw-core` = doctrine records

The next task is not to reorganize that shape.

The next task is to make that shape enact the next missing AGRO behaviors.
