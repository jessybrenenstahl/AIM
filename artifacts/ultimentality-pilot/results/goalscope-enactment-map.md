## Goalscope Enactment Map

This file maps the three reference repos against the actual goalscope:

- human-level PC pilot
- broad control surface
- high autonomy
- durable continuity through compaction and restarts
- recodification when insufficiency is revealed

The key correction is that a harness is not just architecture. It is the enacted runtime loop that carries ingress, identity, plan, execution, verification, memory, and continuation through time.

## Required Harness Functions

For this goalscope, the harness must do all of the following as a continuous system:

1. accept ingress and operator direction
2. preserve stable identity/session state
3. bind the correct model/provider/auth profile
4. load the current sufficient plan and active world state
5. run a model turn
6. execute tools or computer-use actions
7. verify the result in world-state terms
8. record receipts, contradictions, and open capability gaps
9. survive compaction, restart, and handoff
10. promote repeated insufficiencies into permanent capability

## Repo-to-Function Map

### `open-multi-agent`

What it strongly contributes:

- task decomposition and coordinator pattern
- explicit worker execution over a mutable DAG
- provider-normalized message/tool surface
- per-turn LLM -> tool -> tool-result loop

What it weakly contributes:

- durable memory
- long-running session identity
- compaction continuity
- grounded computer-use
- capability-growth after failure

Implication:

- This repo is strongest for the execution topology of multi-step work.
- It is not the right center of gravity for persistent operator identity or world-grounded pilot control.

### `openclaw`

What it strongly contributes:

- ingress surfaces
- pairing and authorization
- session routing
- auth profile selection and rotation
- compaction-aware session continuity
- long-running gateway/runtime behavior

What it weakly contributes:

- single-body computer-use control
- direct desktop pilot substrate
- first-class permanent capability growth

Implication:

- This repo is strongest for the harness/control-plane layer.
- It is the closest reference for how an authenticated always-on operator should actually be wired.

### `RustyClaw`

What it strongly contributes:

- Rust-native runtime posture
- threads/subtasks as real operating units
- runtime adapter seam
- daemon/session/process substrate
- explicit memory-flush/compaction concern

What it weakly contributes:

- complete live provider/auth runtime
- real computer-use body
- unified doctrine-level recodification loop

Implication:

- This repo is strongest for the Rust body and resident runtime stance.
- It is the right temperament for the implementation language, but not a finished answer to the pilot goalscope.

## Combined System Reading

The strongest combined reading is:

- `open-multi-agent` = execution topology
- `openclaw` = harness/control-plane behavior
- `RustyClaw` = Rust-native runtime posture

That means the target harness should not be modeled as “one of these repos, but in Rust.” It should be modeled as a synthesis whose enacted loop is:

1. ingress or wake
2. stable session/identity lookup
3. auth/provider resolution
4. plan + world-state hydration
5. model turn
6. host-body action
7. verification
8. receipt + contradiction capture
9. recodification when needed
10. continuation

## Current Branch Against Goalscope

What is already present in the branch:

- durable local truth
- compact GitHub proof surface
- broad action/observation contract
- real Windows host backend
- future Mac Studio path
- provider/auth substrate
- a first bounded runtime turn loop in `splcw-orchestrator`
- live OpenAI-compatible provider adapters in `splcw-llm`

What still prevents it from being a true harness:

- no OpenClaw-style lane serialization before model work
- no OpenClaw-style auth controller with refresh, rotation, and cooldown behavior
- no OpenClaw-style session repair/prewarm/lock pipeline
- no OpenClaw-style compaction lifecycle with snapshot fallback and post-compaction maintenance
- no first-class capability-promotion engine

## Immediate Build Consequence

The next slice should be judged by whether it closes the harness gap, not whether it adds more substrate.

Concretely, the next implemented loop must:

1. serialize access to the one-body runtime
2. resolve a default or explicit auth profile under a real auth controller
3. call a bound provider adapter
4. convert provider output into proposed actions
5. enact through the host body
6. verify world-state
7. write receipt and gap records
8. republish compact continuity state

If a slice does not advance that loop, it is probably still groundwork rather than harness enactment.
