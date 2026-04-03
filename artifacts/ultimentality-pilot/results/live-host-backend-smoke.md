## Live Host Backend Smoke

Date:

- `2026-04-01`

Observed through the Rust backend binary:

- `cargo run -q -p splcw-host --bin splcw-host-backend -- observe`
- active window returned as `Task Manager`
- window enumeration returned multiple visible windows including `Codex`
- screenshot capture returned a real PNG path under `%TEMP%\splcw-host`

Actuation proof:

- `cargo run -q -p splcw-host --bin splcw-host-backend -- enact`
- action: `FocusWindow { title: "Task Manager" }`
- backend reported success through `windows-powershell`

Verification proof:

- `cargo run -q -p splcw-host --bin splcw-host-backend -- verify`
- returned a fresh observation frame
- foreground state had shifted to `Codex`, which reveals that focus success is not sufficient as a terminal proof

Revealed gap:

- focus commands need stabilization logic plus contradiction recording when verify disagrees with actuation success
