# Cleanup Guide for Restored Crates

Codex: apply these changes to the restored crates. Each section names the file,
the exact symbol or block to remove, and the reason. Compile after each crate.

---

## `crates/splcw-llm`

### Remove: native Codex endpoint as primary engine identity

**File**: `src/providers.rs`

Remove or demote:
- `NATIVE_CODEX_ENGINE_LABEL` and `NATIVE_CODEX_ENGINE_SUMMARY` constants if present
- Any code path that treats `DEFAULT_OPENAI_CODEX_RESPONSES_URL`
  (`chatgpt.com/backend-api/codex/responses`) as the *default* or *primary*
  provider URL rather than an OAuth-gated option
- The `BUILTIN_OPENAI_CODEX_OAUTH_CLIENT_ID` constant is **fine to keep** —
  it is used for legitimate OAuth authentication, not engine impersonation

**Keep everything else**: `FileAuthProfileStore`, `ConfiguredLlmClient`,
`OpenAiResponsesProvider`, OAuth lifecycle, auth profile store, provider registry,
`ProviderKind`, all chat IR types.

**Reason**: The auth infrastructure is the unique value. The "native Codex engine"
framing is the drift. The harness authenticates *to* OpenAI Codex; it does not
*become* a Codex client.

---

## `crates/splcw-orchestrator`

### Remove: any reference to `patches/` directory

**Files**: search all `.rs` files for `"patches"` or `patches/`

Remove any hard-coded path that reads from or writes to `patches/`. The
`patches/self-optimization-scaffolding/` folder was in the old FFR repo and is
not present in AIM. The logic from those patch files should be lifted directly
into `splcw-orchestrator` proper (see below).

### Lift in: stabilization retry from patches

The module `patches/self-optimization-scaffolding/src/host_verify_retry.rs`
contains `stabilize_host_effect`. Lift it into
`crates/splcw-orchestrator/src/host_verify_retry.rs` and wire it in at the
`HostEffectsUncertain` write site in `runtime.rs`.

### Lift in: gap task emitter from patches

The module `patches/self-optimization-scaffolding/src/gap_task_emitter.rs`
contains `emit_gap_task` / `build_gap_task_context` / `mark_oldest_pending_task_resolved`.
Lift it into `crates/splcw-orchestrator/src/gap_task_emitter.rs` and wire in:
- `emit_gap_task` after every `surface_runtime_gap` call
- `build_gap_task_context` before building the system prompt

### Lift in: compaction publisher from patches

The module `patches/self-optimization-scaffolding/src/compaction_publisher.rs`
contains `publish_compaction_to_mirror`. Lift it into
`crates/splcw-orchestrator/src/compaction_publisher.rs` and call it just before
the `CompactionCompleted` event is appended in `session.rs`.

### Add: always-on memory injection

In `runtime.rs`, in `run_runtime_turn`, before building the system prompt:

```rust
// Read always-on memory surfaces
let repo_root = find_repo_root(&session_root).unwrap_or_else(|_| session_root.clone());
let os_md = tokio::fs::read_to_string(
    repo_root.join("artifacts/ultimentality-pilot/memory/os.md")
).await.unwrap_or_default();
let memory_md = tokio::fs::read_to_string(
    repo_root.join("artifacts/ultimentality-pilot/memory/memory.md")
).await.unwrap_or_default();

// Prepend to system prompt
let system_prompt = format!(
    "{os_md}\n\n{memory_md}\n\n{base_system_prompt}"
);
```

---

## `crates/splcw-operator-gui`

### Remove: native Codex engine self-framing

**File**: `src/shell.rs`

Remove:
```rust
const NATIVE_CODEX_ENGINE_LABEL: &str = "Native Codex runtime";
const NATIVE_CODEX_ENGINE_SUMMARY: &str =
    "This operator shell is a harness-native Codex client, not a wrapped Codex CLI session.";
```

And any UI element that displays either of these strings to the operator.
Replace with a neutral label such as `"AGRO Operator"` or `"Harness Operator"`.

Remove: `OPENAI_CODEX_ENDPOINT` and `OPENAI_API_ENDPOINT` constants from
`shell.rs` if they are used to self-describe the engine to the operator
(vs. just being used for URL validation or display).

**Keep everything else**: monitoring panel, Run Turn button, Start/Stop Loop,
auth readiness display, background runner control, supervised GitHub action
request display and approval UI. All of that is legitimate operator frontend.

### Keep: all GPUI rendering, all status monitoring, all background control

The GUI's job is to show the operator what the harness is doing and let them
approve or intervene. None of that needs to change. Only the self-identification
as "Native Codex runtime" is wrong.

---

## Verification Checklist

After applying all cleanup:

- [ ] `cargo build --manifest-path ultimentality-pilot/harness/Cargo.toml` passes
- [ ] `cargo test --manifest-path ultimentality-pilot/harness/Cargo.toml` passes
- [ ] No string `"Native Codex runtime"` or `"harness-native Codex client"` in any source file
- [ ] No path reference to `patches/` in any source file
- [ ] `os.md` and `memory.md` are injected into the system prompt in `run_runtime_turn`
- [ ] `emit_gap_task` is called after every `surface_runtime_gap`
- [ ] `publish_compaction_to_mirror` is called before `CompactionCompleted` is appended
