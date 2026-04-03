use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use std::path::Path;
use std::process::{Command, Output};
#[cfg(target_os = "macos")]
use std::sync::OnceLock;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use chrono::Utc;
use serde::Deserialize;
use serde_json::{Value, json};
#[cfg(any(target_os = "windows", target_os = "macos"))]
use splcw_computer_use::MouseButton;
use splcw_computer_use::{ActionExecution, ObservationFrame, ProposedAction, StructuredSignal};
use uuid::Uuid;

#[cfg(target_os = "windows")]
const WINDOWS_FOCUS_VERIFICATION_TIMEOUT: Duration = Duration::from_millis(1500);
#[cfg(target_os = "windows")]
const WINDOWS_FOCUS_VERIFICATION_POLL: Duration = Duration::from_millis(100);
#[cfg(target_os = "windows")]
const WINDOWS_FOCUS_VERIFICATION_STABLE_SAMPLES: usize = 2;
#[cfg(target_os = "windows")]
const WINDOWS_POST_ACTION_VERIFICATION_TIMEOUT: Duration = Duration::from_millis(2000);
#[cfg(target_os = "windows")]
const WINDOWS_POST_ACTION_VERIFICATION_POLL: Duration = Duration::from_millis(125);
#[cfg(target_os = "macos")]
const MACOS_FOCUS_VERIFICATION_TIMEOUT: Duration = Duration::from_millis(1500);
#[cfg(target_os = "macos")]
const MACOS_FOCUS_VERIFICATION_POLL: Duration = Duration::from_millis(100);
#[cfg(target_os = "macos")]
const MACOS_FOCUS_VERIFICATION_STABLE_SAMPLES: usize = 2;
#[cfg(target_os = "macos")]
const MACOS_POST_ACTION_VERIFICATION_TIMEOUT: Duration = Duration::from_millis(2000);
#[cfg(target_os = "macos")]
const MACOS_POST_ACTION_VERIFICATION_POLL: Duration = Duration::from_millis(125);
#[cfg(target_os = "macos")]
const SHELL_COMMAND_TIMEOUT: Duration = Duration::from_secs(15);
#[cfg(target_os = "macos")]
const MACOS_POINTER_COMMAND_TIMEOUT: Duration = Duration::from_secs(15);
const OBSERVATION_COMMAND_TIMEOUT: Duration = Duration::from_secs(20);
#[cfg(target_os = "macos")]
const CLIPBOARD_COMMAND_TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(target_os = "macos")]
static MACOS_POINTER_BACKEND_AVAILABLE: OnceLock<bool> = OnceLock::new();

#[cfg_attr(target_os = "macos", allow(dead_code))]
struct CommandCapture {
    stdout: String,
    stderr: String,
}

#[cfg(target_os = "windows")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct WindowsFocusState {
    active_window: Option<String>,
    window_titles: Vec<String>,
}

#[cfg(target_os = "windows")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct WindowsPointerState {
    x: i32,
    y: i32,
}

#[cfg(target_os = "windows")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct WindowsPointerActionAnchorState {
    active_window: Option<String>,
    focused_control_name: Option<String>,
    screenshot_fingerprint: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WindowsProcessState {
    pid: u32,
    name: String,
    parent_process_id: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WindowsLaunchState {
    processes: Vec<WindowsProcessState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WindowsLaunchAnchorState {
    baseline_process_ids: HashSet<u32>,
    spawned_pid: Option<u32>,
}

#[cfg(target_os = "macos")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct MacosFocusState {
    active_window: Option<String>,
    window_titles: Vec<String>,
}

#[cfg(target_os = "macos")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct MacosKeyboardAnchorState {
    active_window: Option<String>,
    clipboard_present: bool,
    clipboard_fingerprint: Option<u64>,
    screenshot_fingerprint: Option<u64>,
    focused_control_name: Option<String>,
    focused_control_value_fingerprint: Option<u64>,
}

#[cfg(target_os = "macos")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct MacosKeyboardVerificationStatus {
    before_window: Option<String>,
    observed_window: Option<String>,
    before_control_name: Option<String>,
    observed_control_name: Option<String>,
    focus_preserved: bool,
    window_changed: bool,
    clipboard_changed: bool,
    focused_control_changed: bool,
    focused_control_value_changed: bool,
    requested_text_observed: bool,
    screenshot_changed: bool,
    ok: bool,
    proof_level: &'static str,
}

#[cfg(target_os = "macos")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct MacosFocusedControlState {
    name: Option<String>,
    value: Option<String>,
}

#[cfg(target_os = "macos")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct MacosPointerState {
    x: i32,
    y: i32,
}

#[cfg(target_os = "macos")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct MacosPointerActionAnchorState {
    active_window: Option<String>,
    focused_control_name: Option<String>,
    screenshot_fingerprint: Option<u64>,
}

#[cfg(target_os = "macos")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct MacosPointerActionVerificationStatus {
    observed_x: i32,
    observed_y: i32,
    within_tolerance: bool,
    expected_x: i32,
    expected_y: i32,
    tolerance_px: i32,
    before_window: Option<String>,
    observed_window: Option<String>,
    window_changed: bool,
    focused_control_changed: bool,
    screenshot_changed: bool,
    ok: bool,
    proof_level: &'static str,
}

#[cfg(target_os = "macos")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct MacosScrollAnchorState {
    active_window: Option<String>,
    focused_control_name: Option<String>,
    screenshot_fingerprint: Option<u64>,
}

#[cfg(target_os = "macos")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct MacosScrollVerificationStatus {
    before_window: Option<String>,
    observed_window: Option<String>,
    window_changed: bool,
    focused_control_changed: bool,
    screenshot_changed: bool,
    ok: bool,
    proof_level: &'static str,
}

#[cfg(target_os = "windows")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct WindowsFocusedControlState {
    name: Option<String>,
    value: Option<String>,
}

#[cfg(target_os = "windows")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct WindowsKeyboardAnchorState {
    active_window: Option<String>,
    clipboard_present: bool,
    clipboard_fingerprint: Option<u64>,
    focused_control_name: Option<String>,
    focused_control_value_fingerprint: Option<u64>,
}

#[cfg(target_os = "windows")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct WindowsScrollAnchorState {
    active_window: Option<String>,
    focused_control_name: Option<String>,
    screenshot_fingerprint: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FocusVerificationStatus {
    observed: Option<String>,
    matched: bool,
    stable: bool,
    attempts: usize,
    elapsed_ms: u64,
    timed_out: bool,
    poll_ms: u64,
    stable_samples: usize,
    required_stable_samples: usize,
    window_present: bool,
}

#[cfg(target_os = "windows")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct PointerVerificationStatus {
    observed_x: i32,
    observed_y: i32,
    within_tolerance: bool,
    expected_x: i32,
    expected_y: i32,
    tolerance_px: i32,
}

#[cfg(target_os = "windows")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct PointerActionVerificationStatus {
    observed_x: i32,
    observed_y: i32,
    within_tolerance: bool,
    expected_x: i32,
    expected_y: i32,
    tolerance_px: i32,
    before_window: Option<String>,
    observed_window: Option<String>,
    before_control_name: Option<String>,
    observed_control_name: Option<String>,
    window_changed: bool,
    focused_control_changed: bool,
    screenshot_changed: bool,
    ok: bool,
    proof_level: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LaunchVerificationStatus {
    expected_process: String,
    observed_process: Option<String>,
    observed_process_id: Option<u32>,
    spawned_pid: Option<u32>,
    spawned_pid_present: bool,
    spawned_child_detected: bool,
    new_process_detected: bool,
    attempts: usize,
    elapsed_ms: u64,
    timed_out: bool,
    poll_ms: u64,
    proof_level: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WaitForVerificationStatus {
    signal: String,
    matched: bool,
    attempts: usize,
    elapsed_ms: u64,
    timed_out: bool,
    proof_level: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CaptureObservationVerificationStatus {
    screenshot_present: bool,
    screenshot_readable: bool,
    screenshot_fingerprint: Option<u64>,
    ok: bool,
    proof_level: &'static str,
}

#[cfg(target_os = "windows")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct KeyboardVerificationStatus {
    before_window: Option<String>,
    observed_window: Option<String>,
    before_control_name: Option<String>,
    observed_control_name: Option<String>,
    focus_preserved: bool,
    window_changed: bool,
    clipboard_changed: bool,
    focused_control_changed: bool,
    focused_control_value_changed: bool,
    requested_text_observed: bool,
    ok: bool,
    proof_level: &'static str,
}

#[cfg(target_os = "windows")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct ScrollVerificationStatus {
    before_window: Option<String>,
    observed_window: Option<String>,
    before_control_name: Option<String>,
    observed_control_name: Option<String>,
    window_changed: bool,
    focused_control_changed: bool,
    screenshot_changed: bool,
    ok: bool,
    proof_level: &'static str,
}

#[cfg(target_os = "windows")]
#[derive(Debug, Clone)]
struct FocusVerificationTracker {
    target: String,
    attempts: usize,
    consecutive_matches: usize,
    required_stable_samples: usize,
}

#[derive(Debug, Clone)]
struct LaunchVerificationTracker {
    expected_process: String,
    baseline_process_ids: HashSet<u32>,
    spawned_pid: Option<u32>,
    attempts: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendMode {
    Observe,
    Enact,
    Verify,
}

pub fn run_cli(mode: BackendMode) -> anyhow::Result<()> {
    match mode {
        BackendMode::Observe => {
            let frame = observe_current()?;
            print_json(&frame)
        }
        BackendMode::Enact => {
            let action: ProposedAction = read_json_stdin()?;
            let execution = enact_action(&action)?;
            print_json(&execution)
        }
        BackendMode::Verify => {
            let execution: ActionExecution = read_json_stdin()?;
            let frame = verify_post_action_frame(&execution)?;
            print_json(&frame)
        }
    }
}

pub fn observe_frame() -> anyhow::Result<ObservationFrame> {
    observe_current()
}

pub fn enact_action_direct(action: &ProposedAction) -> anyhow::Result<ActionExecution> {
    enact_action(action)
}

pub fn verify_post_action_direct(execution: &ActionExecution) -> anyhow::Result<ObservationFrame> {
    verify_post_action_frame(execution)
}

fn print_json<T: serde::Serialize>(value: &T) -> anyhow::Result<()> {
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();
    serde_json::to_writer(&mut handle, value)?;
    handle.write_all(b"\n")?;
    Ok(())
}

fn read_json_stdin<T: for<'de> Deserialize<'de>>() -> anyhow::Result<T> {
    let mut input = String::new();
    std::io::stdin()
        .read_to_string(&mut input)
        .context("read backend stdin")?;
    serde_json::from_str(&input).context("parse backend stdin json")
}

fn observe_current() -> anyhow::Result<ObservationFrame> {
    #[cfg(target_os = "windows")]
    {
        observe_windows()
    }
    #[cfg(target_os = "macos")]
    {
        observe_macos()
    }
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    {
        bail!("unsupported platform")
    }
}

fn enact_action(action: &ProposedAction) -> anyhow::Result<ActionExecution> {
    #[cfg(target_os = "windows")]
    {
        enact_windows(action)
    }
    #[cfg(target_os = "macos")]
    {
        enact_macos(action)
    }
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    {
        let _ = action;
        bail!("unsupported platform")
    }
}

fn verify_post_action_frame(execution: &ActionExecution) -> anyhow::Result<ObservationFrame> {
    #[cfg(target_os = "windows")]
    {
        verify_windows_post_action(execution)
    }
    #[cfg(target_os = "macos")]
    {
        verify_macos_post_action(execution)
    }
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    {
        let _ = execution;
        bail!("unsupported platform")
    }
}

#[cfg(target_os = "windows")]
fn observe_windows() -> anyhow::Result<ObservationFrame> {
    Ok(observe_windows_with_metadata()?.0)
}

#[cfg(target_os = "windows")]
fn observe_windows_with_metadata()
-> anyhow::Result<(ObservationFrame, Option<WindowsFocusedControlState>)> {
    let script = r#"
Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName System.Drawing
try {
  Add-Type -AssemblyName UIAutomationClient
  Add-Type -AssemblyName UIAutomationTypes
} catch {}
Add-Type @"
using System;
using System.Runtime.InteropServices;
using System.Text;
public static class Win32Observe {
  [DllImport("user32.dll")] public static extern IntPtr GetForegroundWindow();
  [DllImport("user32.dll", CharSet=CharSet.Unicode)] public static extern int GetWindowText(IntPtr hWnd, StringBuilder text, int maxCount);
}
"@
$hwnd = [Win32Observe]::GetForegroundWindow()
$sb = New-Object System.Text.StringBuilder 1024
[void][Win32Observe]::GetWindowText($hwnd, $sb, $sb.Capacity)
$active = $sb.ToString()
$windows = @(Get-Process | Where-Object { $_.MainWindowTitle } | Select-Object -ExpandProperty MainWindowTitle)
$clipboard = $null
try { $clipboard = Get-Clipboard -Raw } catch {}
$focusedControlName = $null
$focusedControlValue = $null
try {
  $focusedElement = [System.Windows.Automation.AutomationElement]::FocusedElement
  if ($focusedElement -ne $null) {
    try { $focusedControlName = $focusedElement.Current.Name } catch {}
    $valuePattern = $null
    try {
      if ($focusedElement.TryGetCurrentPattern([System.Windows.Automation.ValuePattern]::Pattern, [ref]$valuePattern)) {
        try { $focusedControlValue = $valuePattern.Current.Value } catch {}
      }
    } catch {}
  }
} catch {}
$tempDir = Join-Path $env:TEMP 'splcw-host'
New-Item -ItemType Directory -Force -Path $tempDir | Out-Null
$path = Join-Path $tempDir ('screen-' + [guid]::NewGuid().ToString() + '.png')
$bounds = [System.Windows.Forms.SystemInformation]::VirtualScreen
$bmp = New-Object System.Drawing.Bitmap $bounds.Width, $bounds.Height
$graphics = [System.Drawing.Graphics]::FromImage($bmp)
$graphics.CopyFromScreen($bounds.Left, $bounds.Top, 0, 0, $bmp.Size)
$bmp.Save($path, [System.Drawing.Imaging.ImageFormat]::Png)
$graphics.Dispose()
$bmp.Dispose()
[pscustomobject]@{
  active_window = $active
  window_titles = $windows
  clipboard_text = $clipboard
  focused_control_name = $focusedControlName
  focused_control_value = $focusedControlValue
  screenshot_path = $path
} | ConvertTo-Json -Compress
"#;

    let output = run_powershell(script)?;
    let raw: Value = serde_json::from_str(output.stdout.trim()).with_context(|| {
        format!(
            "parse windows observe json{}",
            if output.stderr.trim().is_empty() {
                String::new()
            } else {
                format!(" (stderr: {})", output.stderr.trim())
            }
        )
    })?;
    let active_window = raw
        .get("active_window")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let window_titles = raw
        .get("window_titles")
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(|s| s.to_string()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let clipboard_text = raw
        .get("clipboard_text")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let focused_control_name = raw
        .get("focused_control_name")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let focused_control_value = raw
        .get("focused_control_value")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let screenshot_path = raw
        .get("screenshot_path")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let focused_control = if focused_control_name.is_some() || focused_control_value.is_some() {
        Some(WindowsFocusedControlState {
            name: focused_control_name.clone(),
            value: focused_control_value.clone(),
        })
    } else {
        None
    };

    Ok((
        ObservationFrame {
            captured_at: Utc::now(),
            summary: format!(
                "windows observe: active={} windows={}",
                active_window.clone().unwrap_or_else(|| "unknown".into()),
                window_titles.len()
            ),
            screenshot_path,
            ocr_text: None,
            active_window,
            window_titles,
            clipboard_text,
            structured_signals: vec![
                StructuredSignal {
                    key: "platform".into(),
                    payload: json!("windows"),
                },
                StructuredSignal {
                    key: "capability_set".into(),
                    payload: json!([
                        "focus_window",
                        "type_text",
                        "hotkey",
                        "click",
                        "double_click",
                        "drag",
                        "scroll",
                        "launch_process",
                        "capture_observation"
                    ]),
                },
                StructuredSignal {
                    key: "focused_control_state".into(),
                    payload: json!({
                        "name": focused_control_name,
                        "value_present": focused_control_value.is_some(),
                        "value_fingerprint": focused_control_value.as_deref().map(fingerprint_text),
                        "value_length": focused_control_value.as_ref().map(|value| value.chars().count()),
                    }),
                },
            ],
        },
        focused_control,
    ))
}

#[cfg(target_os = "windows")]
impl FocusVerificationTracker {
    fn new(target: &str) -> Self {
        Self {
            target: target.to_string(),
            attempts: 0,
            consecutive_matches: 0,
            required_stable_samples: WINDOWS_FOCUS_VERIFICATION_STABLE_SAMPLES,
        }
    }

    fn record(
        &mut self,
        state: &WindowsFocusState,
        elapsed: Duration,
        timed_out: bool,
    ) -> FocusVerificationStatus {
        self.attempts += 1;
        let observed = state.active_window.clone();
        let matched = observed
            .as_deref()
            .map(|active| contains_case_insensitive(active, &self.target))
            .unwrap_or(false);
        if matched {
            self.consecutive_matches += 1;
        } else {
            self.consecutive_matches = 0;
        }
        let window_present = matched
            || state
                .window_titles
                .iter()
                .any(|title| contains_case_insensitive(title, &self.target));

        FocusVerificationStatus {
            observed,
            matched,
            stable: self.consecutive_matches >= self.required_stable_samples,
            attempts: self.attempts,
            elapsed_ms: elapsed.as_millis().min(u128::from(u64::MAX)) as u64,
            timed_out,
            poll_ms: WINDOWS_FOCUS_VERIFICATION_POLL.as_millis() as u64,
            stable_samples: self.consecutive_matches,
            required_stable_samples: self.required_stable_samples,
            window_present,
        }
    }
}

impl LaunchVerificationTracker {
    fn new(command: &str, before: Option<&WindowsLaunchAnchorState>) -> Self {
        Self {
            expected_process: launch_target_token(command),
            baseline_process_ids: before
                .map(|anchor| anchor.baseline_process_ids.clone())
                .unwrap_or_default(),
            spawned_pid: before.and_then(|anchor| anchor.spawned_pid),
            attempts: 0,
        }
    }

    fn record(
        &mut self,
        state: &WindowsLaunchState,
        elapsed: Duration,
        timed_out: bool,
        poll: Duration,
    ) -> LaunchVerificationStatus {
        self.attempts += 1;
        let matching_processes = state
            .processes
            .iter()
            .filter(|process| contains_case_insensitive(&process.name, &self.expected_process))
            .collect::<Vec<_>>();
        let spawned_process = self.spawned_pid.and_then(|spawned_pid| {
            state
                .processes
                .iter()
                .find(|process| process.pid == spawned_pid)
        });
        let spawned_child = self.spawned_pid.and_then(|spawned_pid| {
            state.processes.iter().find(|process| {
                process
                    .parent_process_id
                    .map(|parent_pid| parent_pid == spawned_pid)
                    .unwrap_or(false)
            })
        });
        let new_process = matching_processes
            .iter()
            .copied()
            .find(|process| !self.baseline_process_ids.contains(&process.pid));
        let preferred = spawned_process.or(spawned_child).or(new_process);
        let observed_process = preferred.map(|process| process.name.clone());
        let observed_process_id = preferred.map(|process| process.pid);
        let spawned_pid_present = spawned_process.is_some();
        let spawned_child_detected = spawned_child.is_some();
        let new_process_detected = new_process.is_some();
        let proof_level = if spawned_pid_present {
            "spawned_pid_still_present"
        } else if spawned_child_detected {
            "spawned_child_detected"
        } else if new_process_detected {
            "new_process_delta"
        } else if !matching_processes.is_empty() {
            "preexisting_process_only"
        } else {
            "none"
        };

        LaunchVerificationStatus {
            expected_process: self.expected_process.clone(),
            observed_process,
            observed_process_id,
            spawned_pid: self.spawned_pid,
            spawned_pid_present,
            spawned_child_detected,
            new_process_detected,
            attempts: self.attempts,
            elapsed_ms: elapsed.as_millis().min(u128::from(u64::MAX)) as u64,
            timed_out,
            poll_ms: poll.as_millis() as u64,
            proof_level,
        }
    }
}

#[cfg(target_os = "windows")]
fn verify_windows_post_action(execution: &ActionExecution) -> anyhow::Result<ObservationFrame> {
    match &execution.action {
        ProposedAction::FocusWindow { title } => verify_windows_focus_stability(title),
        ProposedAction::LaunchProcess { command, .. } => verify_windows_launch(execution, command),
        ProposedAction::Click { x, y, .. } | ProposedAction::DoubleClick { x, y, .. } => {
            verify_windows_pointer_action(execution, *x, *y, action_kind(&execution.action))
        }
        ProposedAction::Drag { to_x, to_y, .. } => {
            verify_windows_pointer_action(execution, *to_x, *to_y, action_kind(&execution.action))
        }
        ProposedAction::TypeText { .. } | ProposedAction::Hotkey { .. } => {
            verify_windows_keyboard_anchor(execution, action_kind(&execution.action))
        }
        ProposedAction::Scroll { .. } => verify_windows_scroll_effect(execution),
        ProposedAction::WaitFor { .. } => verify_wait_for_post_action(execution, observe_windows),
        ProposedAction::CaptureObservation => {
            verify_capture_observation_post_action(observe_windows)
        }
    }
}

#[cfg(target_os = "macos")]
fn verify_macos_post_action(execution: &ActionExecution) -> anyhow::Result<ObservationFrame> {
    match &execution.action {
        ProposedAction::FocusWindow { title } => verify_macos_focus_stability(title),
        ProposedAction::LaunchProcess { command, .. } => verify_macos_launch(execution, command),
        ProposedAction::Click { x, y, .. } | ProposedAction::DoubleClick { x, y, .. } => {
            verify_macos_pointer_action(execution, *x, *y, action_kind(&execution.action))
        }
        ProposedAction::Drag { to_x, to_y, .. } => {
            verify_macos_pointer_action(execution, *to_x, *to_y, action_kind(&execution.action))
        }
        ProposedAction::TypeText { .. } | ProposedAction::Hotkey { .. } => {
            verify_macos_keyboard_anchor(execution, action_kind(&execution.action))
        }
        ProposedAction::Scroll { .. } => verify_macos_scroll_action(execution),
        ProposedAction::WaitFor { .. } => verify_wait_for_post_action(execution, observe_macos),
        ProposedAction::CaptureObservation => verify_capture_observation_post_action(observe_macos),
    }
}

fn verify_wait_for_post_action(
    execution: &ActionExecution,
    observe: fn() -> anyhow::Result<ObservationFrame>,
) -> anyhow::Result<ObservationFrame> {
    let mut frame = observe()?;
    let Some(status) = wait_for_verification_status_from_execution(execution) else {
        append_generic_post_action_signal(
            &mut frame,
            "wait_for",
            false,
            json!({
                "reason": "missing_wait_for_result",
                "proof_level": "none",
            }),
            "wait verify missing wait-for result proof".into(),
        );
        return Ok(frame);
    };

    append_generic_post_action_signal(
        &mut frame,
        "wait_for",
        status.matched,
        json!({
            "signal": status.signal,
            "matched": status.matched,
            "attempts": status.attempts,
            "elapsed_ms": status.elapsed_ms,
            "timed_out": status.timed_out,
            "proof_level": status.proof_level,
        }),
        format!(
            "wait verify signal={} matched={} attempts={} elapsed_ms={} timed_out={} proof_level={}",
            status.signal,
            status.matched,
            status.attempts,
            status.elapsed_ms,
            status.timed_out,
            status.proof_level,
        ),
    );
    Ok(frame)
}

fn verify_capture_observation_post_action(
    observe: fn() -> anyhow::Result<ObservationFrame>,
) -> anyhow::Result<ObservationFrame> {
    let mut frame = observe()?;
    let status = capture_observation_verification_status(&frame);
    append_generic_post_action_signal(
        &mut frame,
        "capture_observation",
        status.ok,
        json!({
            "screenshot_present": status.screenshot_present,
            "screenshot_readable": status.screenshot_readable,
            "screenshot_fingerprint": status.screenshot_fingerprint,
            "proof_level": status.proof_level,
        }),
        format!(
            "capture observation verify screenshot_present={} screenshot_readable={} proof_level={}",
            status.screenshot_present, status.screenshot_readable, status.proof_level,
        ),
    );
    Ok(frame)
}

#[cfg(target_os = "windows")]
fn verify_windows_focus_stability(title: &str) -> anyhow::Result<ObservationFrame> {
    let started = Instant::now();
    let mut tracker = FocusVerificationTracker::new(title);

    loop {
        let state = observe_windows_focus_state()?;
        let status = tracker.record(&state, started.elapsed(), false);
        if status.stable || started.elapsed() >= WINDOWS_FOCUS_VERIFICATION_TIMEOUT {
            break;
        }
        thread::sleep(WINDOWS_FOCUS_VERIFICATION_POLL);
    }

    let mut frame = observe_windows()?;
    let state = WindowsFocusState {
        active_window: frame.active_window.clone(),
        window_titles: frame.window_titles.clone(),
    };
    let final_status = tracker.record(
        &state,
        started.elapsed(),
        started.elapsed() >= WINDOWS_FOCUS_VERIFICATION_TIMEOUT,
    );
    append_focus_verification_signal(&mut frame, title, &final_status);
    Ok(frame)
}

#[cfg(target_os = "windows")]
fn verify_windows_launch(
    execution: &ActionExecution,
    command: &str,
) -> anyhow::Result<ObservationFrame> {
    let Some(before) = launch_anchor_state_from_execution(execution) else {
        let mut frame = observe_windows()?;
        append_generic_post_action_signal(
            &mut frame,
            "launch_process",
            false,
            json!({
                "command": command,
                "reason": "missing_launch_anchor",
                "proof_level": "none",
            }),
            format!("launch verify missing launch anchor proof for {}", command),
        );
        return Ok(frame);
    };
    let started = Instant::now();
    let mut tracker = LaunchVerificationTracker::new(command, Some(&before));

    loop {
        let state = observe_windows_launch_state()?;
        let status = tracker.record(
            &state,
            started.elapsed(),
            false,
            WINDOWS_POST_ACTION_VERIFICATION_POLL,
        );
        if status.spawned_pid_present
            || status.spawned_child_detected
            || status.new_process_detected
            || started.elapsed() >= WINDOWS_POST_ACTION_VERIFICATION_TIMEOUT
        {
            break;
        }
        thread::sleep(WINDOWS_POST_ACTION_VERIFICATION_POLL);
    }

    let mut frame = observe_windows()?;
    let final_status = tracker.record(
        &observe_windows_launch_state()?,
        started.elapsed(),
        started.elapsed() >= WINDOWS_POST_ACTION_VERIFICATION_TIMEOUT,
        WINDOWS_POST_ACTION_VERIFICATION_POLL,
    );
    append_generic_post_action_signal(
        &mut frame,
        "launch_process",
        final_status.spawned_pid_present
            || final_status.spawned_child_detected
            || final_status.new_process_detected,
        json!({
            "command": command,
            "expected_process": final_status.expected_process,
            "observed_process": final_status.observed_process,
            "observed_process_id": final_status.observed_process_id,
            "spawned_pid": final_status.spawned_pid,
            "spawned_pid_present": final_status.spawned_pid_present,
            "spawned_child_detected": final_status.spawned_child_detected,
            "new_process_detected": final_status.new_process_detected,
            "attempts": final_status.attempts,
            "elapsed_ms": final_status.elapsed_ms,
            "timed_out": final_status.timed_out,
            "poll_ms": final_status.poll_ms,
            "proof_level": final_status.proof_level,
        }),
        format!(
            "launch verify expected_process={} observed_process={} observed_process_id={} spawned_pid={} spawned_pid_present={} spawned_child_detected={} new_process_detected={} attempts={} elapsed_ms={} timed_out={} proof_level={}",
            final_status.expected_process,
            final_status.observed_process.as_deref().unwrap_or("none"),
            final_status
                .observed_process_id
                .map(|value| value.to_string())
                .as_deref()
                .unwrap_or("none"),
            final_status
                .spawned_pid
                .map(|value| value.to_string())
                .as_deref()
                .unwrap_or("none"),
            final_status.spawned_pid_present,
            final_status.spawned_child_detected,
            final_status.new_process_detected,
            final_status.attempts,
            final_status.elapsed_ms,
            final_status.timed_out,
            final_status.proof_level,
        ),
    );
    Ok(frame)
}

#[cfg(target_os = "macos")]
fn verify_macos_focus_stability(title: &str) -> anyhow::Result<ObservationFrame> {
    let started = Instant::now();
    let mut attempts = 0usize;
    let mut consecutive_matches = 0usize;
    let final_status = loop {
        attempts += 1;
        let state = observe_macos_focus_state()?;
        let observed = state.active_window.clone();
        let matched = observed
            .as_deref()
            .map(|active| contains_case_insensitive(active, title))
            .unwrap_or(false);
        if matched {
            consecutive_matches += 1;
        } else {
            consecutive_matches = 0;
        }
        let timed_out = started.elapsed() >= MACOS_FOCUS_VERIFICATION_TIMEOUT;
        let status = FocusVerificationStatus {
            observed,
            matched,
            stable: consecutive_matches >= MACOS_FOCUS_VERIFICATION_STABLE_SAMPLES,
            attempts,
            elapsed_ms: started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
            timed_out,
            poll_ms: MACOS_FOCUS_VERIFICATION_POLL.as_millis() as u64,
            stable_samples: consecutive_matches,
            required_stable_samples: MACOS_FOCUS_VERIFICATION_STABLE_SAMPLES,
            window_present: matched
                || state
                    .window_titles
                    .iter()
                    .any(|window| contains_case_insensitive(window, title)),
        };
        if status.stable || timed_out {
            break status;
        }
        thread::sleep(MACOS_FOCUS_VERIFICATION_POLL);
    };

    let mut frame = observe_macos()?;
    append_focus_verification_signal(&mut frame, title, &final_status);
    Ok(frame)
}

#[cfg(target_os = "macos")]
fn verify_macos_launch(
    execution: &ActionExecution,
    command: &str,
) -> anyhow::Result<ObservationFrame> {
    let Some(before) = launch_anchor_state_from_execution(execution) else {
        let mut frame = observe_macos()?;
        append_generic_post_action_signal(
            &mut frame,
            "launch_process",
            false,
            json!({
                "command": command,
                "reason": "missing_launch_anchor",
                "proof_level": "none",
            }),
            format!("launch verify missing launch anchor proof for {}", command),
        );
        return Ok(frame);
    };
    let started = Instant::now();
    let mut tracker = LaunchVerificationTracker::new(command, Some(&before));

    loop {
        let state = observe_macos_launch_state()?;
        let status = tracker.record(
            &state,
            started.elapsed(),
            false,
            MACOS_POST_ACTION_VERIFICATION_POLL,
        );
        if status.spawned_pid_present
            || status.spawned_child_detected
            || status.new_process_detected
            || started.elapsed() >= MACOS_POST_ACTION_VERIFICATION_TIMEOUT
        {
            break;
        }
        thread::sleep(MACOS_POST_ACTION_VERIFICATION_POLL);
    }

    let mut frame = observe_macos()?;
    let final_status = tracker.record(
        &observe_macos_launch_state()?,
        started.elapsed(),
        started.elapsed() >= MACOS_POST_ACTION_VERIFICATION_TIMEOUT,
        MACOS_POST_ACTION_VERIFICATION_POLL,
    );
    append_generic_post_action_signal(
        &mut frame,
        "launch_process",
        final_status.spawned_pid_present
            || final_status.spawned_child_detected
            || final_status.new_process_detected,
        json!({
            "command": command,
            "expected_process": final_status.expected_process,
            "observed_process": final_status.observed_process,
            "observed_process_id": final_status.observed_process_id,
            "spawned_pid": final_status.spawned_pid,
            "spawned_pid_present": final_status.spawned_pid_present,
            "spawned_child_detected": final_status.spawned_child_detected,
            "new_process_detected": final_status.new_process_detected,
            "attempts": final_status.attempts,
            "elapsed_ms": final_status.elapsed_ms,
            "timed_out": final_status.timed_out,
            "poll_ms": final_status.poll_ms,
            "proof_level": final_status.proof_level,
        }),
        format!(
            "launch verify expected_process={} observed_process={} observed_process_id={} spawned_pid={} spawned_pid_present={} spawned_child_detected={} new_process_detected={} attempts={} elapsed_ms={} timed_out={} proof_level={}",
            final_status.expected_process,
            final_status.observed_process.as_deref().unwrap_or("none"),
            final_status
                .observed_process_id
                .map(|value| value.to_string())
                .as_deref()
                .unwrap_or("none"),
            final_status
                .spawned_pid
                .map(|value| value.to_string())
                .as_deref()
                .unwrap_or("none"),
            final_status.spawned_pid_present,
            final_status.spawned_child_detected,
            final_status.new_process_detected,
            final_status.attempts,
            final_status.elapsed_ms,
            final_status.timed_out,
            final_status.proof_level,
        ),
    );
    Ok(frame)
}

#[cfg(target_os = "macos")]
fn verify_macos_keyboard_anchor(
    execution: &ActionExecution,
    kind: &str,
) -> anyhow::Result<ObservationFrame> {
    let (mut frame, focused_control) = observe_macos_with_metadata()?;
    let Some(before) = macos_keyboard_anchor_state_from_execution(execution) else {
        append_generic_post_action_signal(
            &mut frame,
            kind,
            false,
            json!({
                "reason": "missing_input_anchor",
                "proof_level": "none",
            }),
            format!("{kind} verify missing input anchor proof"),
        );
        return Ok(frame);
    };

    let (
        requested_text,
        requested_text_present_before,
        require_observable_effect,
        accept_focused_control_shift_as_effect,
    ) = match &execution.action {
        ProposedAction::TypeText { text, .. } => (
            Some(text.as_str()),
            execution
                .evidence
                .as_ref()
                .and_then(|raw| raw.get("requested_text_present_before"))
                .and_then(|value| value.as_bool())
                .unwrap_or(false),
            true,
            false,
        ),
        ProposedAction::Hotkey { .. } => (None, false, true, false),
        _ => (None, false, false, false),
    };
    let status = macos_keyboard_verification_status(
        &before,
        &frame,
        focused_control.as_ref(),
        requested_text,
        requested_text_present_before,
        require_observable_effect,
        accept_focused_control_shift_as_effect,
    );
    append_generic_post_action_signal(
        &mut frame,
        kind,
        status.ok,
        json!({
            "before_window": status.before_window,
            "observed_window": status.observed_window,
            "before_control_name": status.before_control_name,
            "observed_control_name": status.observed_control_name,
            "focus_preserved": status.focus_preserved,
            "window_changed": status.window_changed,
            "clipboard_changed": status.clipboard_changed,
            "focused_control_changed": status.focused_control_changed,
            "focused_control_value_changed": status.focused_control_value_changed,
            "requested_text_observed": status.requested_text_observed,
            "screenshot_changed": status.screenshot_changed,
            "proof_level": status.proof_level,
        }),
        format!(
            "{kind} verify before_window={} observed_window={} before_control={} observed_control={} focus_preserved={} window_changed={} clipboard_changed={} focused_control_changed={} focused_control_value_changed={} requested_text_observed={} screenshot_changed={} proof_level={} ok={}",
            status.before_window.as_deref().unwrap_or("none"),
            status.observed_window.as_deref().unwrap_or("none"),
            status.before_control_name.as_deref().unwrap_or("none"),
            status.observed_control_name.as_deref().unwrap_or("none"),
            status.focus_preserved,
            status.window_changed,
            status.clipboard_changed,
            status.focused_control_changed,
            status.focused_control_value_changed,
            status.requested_text_observed,
            status.screenshot_changed,
            status.proof_level,
            status.ok
        ),
    );
    Ok(frame)
}

#[cfg(target_os = "macos")]
fn verify_macos_pointer_action(
    execution: &ActionExecution,
    expected_x: i32,
    expected_y: i32,
    kind: &str,
) -> anyhow::Result<ObservationFrame> {
    let (mut frame, focused_control) = observe_macos_with_metadata()?;
    let state = observe_macos_pointer_state()?;
    let Some(before) = macos_pointer_action_anchor_state_from_execution(execution) else {
        append_generic_post_action_signal(
            &mut frame,
            kind,
            false,
            json!({
                "reason": "missing_pointer_anchor",
                "proof_level": "none",
            }),
            format!("{kind} verify missing pointer anchor proof"),
        );
        return Ok(frame);
    };

    let after_screenshot_fingerprint = frame.screenshot_path.as_deref().and_then(file_fingerprint);
    let status = macos_pointer_action_verification_status(
        expected_x,
        expected_y,
        &state,
        &before,
        &frame,
        after_screenshot_fingerprint,
        focused_control.as_ref(),
    );
    let signal_control = focused_control_signal_state(&frame);
    let observed_control_name = focused_control
        .as_ref()
        .and_then(|state| state.name.clone())
        .or_else(|| signal_control.as_ref().and_then(|state| state.name.clone()));
    append_generic_post_action_signal(
        &mut frame,
        kind,
        status.ok,
        json!({
            "expected_x": status.expected_x,
            "expected_y": status.expected_y,
            "observed_x": status.observed_x,
            "observed_y": status.observed_y,
            "tolerance_px": status.tolerance_px,
            "before_window": status.before_window,
            "observed_window": status.observed_window,
            "before_control_name": before.focused_control_name.clone(),
            "observed_control_name": observed_control_name,
            "window_changed": status.window_changed,
            "focused_control_changed": status.focused_control_changed,
            "screenshot_changed": status.screenshot_changed,
            "proof_level": status.proof_level,
        }),
        format!(
            "{kind} verify expected=({}, {}) observed=({}, {}) tolerance_px={} before_window={} observed_window={} before_control={} observed_control={} window_changed={} focused_control_changed={} screenshot_changed={} proof_level={} ok={}",
            status.expected_x,
            status.expected_y,
            status.observed_x,
            status.observed_y,
            status.tolerance_px,
            status.before_window.as_deref().unwrap_or("none"),
            status.observed_window.as_deref().unwrap_or("none"),
            before.focused_control_name.as_deref().unwrap_or("none"),
            observed_control_name.as_deref().unwrap_or("none"),
            status.window_changed,
            status.focused_control_changed,
            status.screenshot_changed,
            status.proof_level,
            status.ok
        ),
    );
    Ok(frame)
}

#[cfg(target_os = "macos")]
fn verify_macos_scroll_action(execution: &ActionExecution) -> anyhow::Result<ObservationFrame> {
    let (mut frame, focused_control) = observe_macos_with_metadata()?;
    let Some(before) = macos_scroll_anchor_state_from_execution(execution) else {
        append_generic_post_action_signal(
            &mut frame,
            "scroll",
            false,
            json!({
                "reason": "missing_scroll_anchor",
                "proof_level": "none",
            }),
            "scroll verify missing scroll anchor proof".into(),
        );
        return Ok(frame);
    };

    let after_screenshot_fingerprint = frame.screenshot_path.as_deref().and_then(file_fingerprint);
    let status = macos_scroll_verification_status(
        &before,
        &frame,
        after_screenshot_fingerprint,
        focused_control.as_ref(),
    );
    let signal_control = focused_control_signal_state(&frame);
    let observed_control_name = focused_control
        .as_ref()
        .and_then(|state| state.name.clone())
        .or_else(|| signal_control.as_ref().and_then(|state| state.name.clone()));
    append_generic_post_action_signal(
        &mut frame,
        "scroll",
        status.ok,
        json!({
            "before_window": status.before_window,
            "observed_window": status.observed_window,
            "before_control_name": before.focused_control_name.clone(),
            "observed_control_name": observed_control_name,
            "window_changed": status.window_changed,
            "focused_control_changed": status.focused_control_changed,
            "screenshot_changed": status.screenshot_changed,
            "proof_level": status.proof_level,
        }),
        format!(
            "scroll verify before_window={} observed_window={} before_control={} observed_control={} window_changed={} focused_control_changed={} screenshot_changed={} proof_level={} ok={}",
            status.before_window.as_deref().unwrap_or("none"),
            status.observed_window.as_deref().unwrap_or("none"),
            before.focused_control_name.as_deref().unwrap_or("none"),
            observed_control_name.as_deref().unwrap_or("none"),
            status.window_changed,
            status.focused_control_changed,
            status.screenshot_changed,
            status.proof_level,
            status.ok
        ),
    );
    Ok(frame)
}

#[cfg(target_os = "windows")]
fn verify_windows_pointer_action(
    execution: &ActionExecution,
    expected_x: i32,
    expected_y: i32,
    kind: &str,
) -> anyhow::Result<ObservationFrame> {
    let (mut frame, focused_control) = observe_windows_with_metadata()?;
    let state = observe_windows_pointer_state()?;
    let Some(before) = pointer_action_anchor_state_from_execution(execution) else {
        append_generic_post_action_signal(
            &mut frame,
            kind,
            false,
            json!({
                "reason": "missing_pointer_anchor",
                "proof_level": "none",
            }),
            format!("{kind} verify missing pointer anchor proof"),
        );
        return Ok(frame);
    };
    let after_screenshot_fingerprint = frame.screenshot_path.as_deref().and_then(file_fingerprint);
    let status = pointer_action_verification_status(
        expected_x,
        expected_y,
        &state,
        &before,
        &frame,
        after_screenshot_fingerprint,
        focused_control.as_ref(),
    );
    append_generic_post_action_signal(
        &mut frame,
        kind,
        status.ok,
        json!({
            "expected_x": status.expected_x,
            "expected_y": status.expected_y,
            "observed_x": status.observed_x,
            "observed_y": status.observed_y,
            "tolerance_px": status.tolerance_px,
            "before_window": status.before_window,
            "observed_window": status.observed_window,
            "before_control_name": status.before_control_name,
            "observed_control_name": status.observed_control_name,
            "window_changed": status.window_changed,
            "focused_control_changed": status.focused_control_changed,
            "screenshot_changed": status.screenshot_changed,
            "proof_level": status.proof_level,
        }),
        format!(
            "{kind} verify expected=({}, {}) observed=({}, {}) tolerance_px={} before_window={} observed_window={} before_control={} observed_control={} window_changed={} focused_control_changed={} screenshot_changed={} proof_level={} ok={}",
            status.expected_x,
            status.expected_y,
            status.observed_x,
            status.observed_y,
            status.tolerance_px,
            status.before_window.as_deref().unwrap_or("none"),
            status.observed_window.as_deref().unwrap_or("none"),
            status.before_control_name.as_deref().unwrap_or("none"),
            status.observed_control_name.as_deref().unwrap_or("none"),
            status.window_changed,
            status.focused_control_changed,
            status.screenshot_changed,
            status.proof_level,
            status.ok
        ),
    );
    Ok(frame)
}

#[cfg(target_os = "windows")]
fn verify_windows_keyboard_anchor(
    execution: &ActionExecution,
    kind: &str,
) -> anyhow::Result<ObservationFrame> {
    let (mut frame, focused_control) = observe_windows_with_metadata()?;
    let Some(before) = keyboard_anchor_state_from_execution(execution) else {
        append_generic_post_action_signal(
            &mut frame,
            kind,
            false,
            json!({
                "reason": "missing_input_anchor",
                "proof_level": "none",
            }),
            format!("{kind} verify missing input anchor proof"),
        );
        return Ok(frame);
    };

    let (
        requested_text,
        requested_text_present_before,
        require_observable_effect,
        accept_focused_control_shift_as_effect,
    ) = match &execution.action {
        ProposedAction::TypeText { text, .. } => (
            Some(text.as_str()),
            execution
                .evidence
                .as_ref()
                .and_then(|raw| raw.get("requested_text_present_before"))
                .and_then(|value| value.as_bool())
                .unwrap_or(false),
            true,
            false,
        ),
        ProposedAction::Hotkey { .. } => (None, false, true, false),
        _ => (None, false, false, false),
    };
    let status = keyboard_verification_status(
        &before,
        &frame,
        focused_control.as_ref(),
        requested_text,
        requested_text_present_before,
        require_observable_effect,
        accept_focused_control_shift_as_effect,
    );
    append_generic_post_action_signal(
        &mut frame,
        kind,
        status.ok,
        json!({
            "before_window": status.before_window,
            "observed_window": status.observed_window,
            "before_control_name": status.before_control_name,
            "observed_control_name": status.observed_control_name,
            "focus_preserved": status.focus_preserved,
            "window_changed": status.window_changed,
            "clipboard_changed": status.clipboard_changed,
            "focused_control_changed": status.focused_control_changed,
            "focused_control_value_changed": status.focused_control_value_changed,
            "requested_text_observed": status.requested_text_observed,
            "requires_observable_effect": require_observable_effect,
            "accepts_focused_control_shift_as_effect": accept_focused_control_shift_as_effect,
            "proof_level": status.proof_level,
        }),
        format!(
            "{kind} verify before_window={} observed_window={} before_control={} observed_control={} focus_preserved={} window_changed={} clipboard_changed={} focused_control_changed={} focused_control_value_changed={} requested_text_observed={} requires_observable_effect={} accepts_focused_control_shift_as_effect={} proof_level={} ok={}",
            status.before_window.as_deref().unwrap_or("none"),
            status.observed_window.as_deref().unwrap_or("none"),
            status.before_control_name.as_deref().unwrap_or("none"),
            status.observed_control_name.as_deref().unwrap_or("none"),
            status.focus_preserved,
            status.window_changed,
            status.clipboard_changed,
            status.focused_control_changed,
            status.focused_control_value_changed,
            status.requested_text_observed,
            require_observable_effect,
            accept_focused_control_shift_as_effect,
            status.proof_level,
            status.ok
        ),
    );
    Ok(frame)
}

#[cfg(target_os = "windows")]
fn verify_windows_scroll_effect(execution: &ActionExecution) -> anyhow::Result<ObservationFrame> {
    let (mut frame, focused_control) = observe_windows_with_metadata()?;
    let Some(before) = scroll_anchor_state_from_execution(execution) else {
        append_generic_post_action_signal(
            &mut frame,
            "scroll",
            false,
            json!({
                "reason": "missing_scroll_anchor",
                "proof_level": "none",
            }),
            "scroll verify missing scroll anchor proof".into(),
        );
        return Ok(frame);
    };

    let after_screenshot_fingerprint = frame.screenshot_path.as_deref().and_then(file_fingerprint);
    let status = scroll_verification_status(
        &before,
        &frame,
        after_screenshot_fingerprint,
        focused_control.as_ref(),
    );
    append_generic_post_action_signal(
        &mut frame,
        "scroll",
        status.ok,
        json!({
            "before_window": status.before_window,
            "observed_window": status.observed_window,
            "before_control_name": status.before_control_name,
            "observed_control_name": status.observed_control_name,
            "window_changed": status.window_changed,
            "focused_control_changed": status.focused_control_changed,
            "screenshot_changed": status.screenshot_changed,
            "proof_level": status.proof_level,
        }),
        format!(
            "scroll verify before_window={} observed_window={} before_control={} observed_control={} window_changed={} focused_control_changed={} screenshot_changed={} proof_level={} ok={}",
            status.before_window.as_deref().unwrap_or("none"),
            status.observed_window.as_deref().unwrap_or("none"),
            status.before_control_name.as_deref().unwrap_or("none"),
            status.observed_control_name.as_deref().unwrap_or("none"),
            status.window_changed,
            status.focused_control_changed,
            status.screenshot_changed,
            status.proof_level,
            status.ok
        ),
    );
    Ok(frame)
}

#[cfg(target_os = "windows")]
fn observe_windows_focus_state() -> anyhow::Result<WindowsFocusState> {
    let script = r#"
Add-Type @"
using System;
using System.Runtime.InteropServices;
using System.Text;
public static class Win32Observe {
  [DllImport("user32.dll")] public static extern IntPtr GetForegroundWindow();
  [DllImport("user32.dll", CharSet=CharSet.Unicode)] public static extern int GetWindowText(IntPtr hWnd, StringBuilder text, int maxCount);
}
"@
$hwnd = [Win32Observe]::GetForegroundWindow()
$sb = New-Object System.Text.StringBuilder 1024
[void][Win32Observe]::GetWindowText($hwnd, $sb, $sb.Capacity)
$active = $sb.ToString()
$windows = @(Get-Process | Where-Object { $_.MainWindowTitle } | Select-Object -ExpandProperty MainWindowTitle)
[pscustomobject]@{
  active_window = $active
  window_titles = $windows
} | ConvertTo-Json -Compress
"#;

    let output = run_powershell(script)?;
    let raw: Value = serde_json::from_str(output.stdout.trim()).with_context(|| {
        format!(
            "parse windows focus-state json{}",
            if output.stderr.trim().is_empty() {
                String::new()
            } else {
                format!(" (stderr: {})", output.stderr.trim())
            }
        )
    })?;

    Ok(WindowsFocusState {
        active_window: raw
            .get("active_window")
            .and_then(|value| value.as_str())
            .map(|value| value.to_string()),
        window_titles: raw
            .get("window_titles")
            .and_then(|value| value.as_array())
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| item.as_str().map(|value| value.to_string()))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
    })
}

#[cfg(target_os = "windows")]
fn observe_windows_launch_state() -> anyhow::Result<WindowsLaunchState> {
    let script = r#"
$processes = @(
  Get-CimInstance Win32_Process | ForEach-Object {
    [pscustomobject]@{
      pid = [uint32]$_.ProcessId
      name = $_.Name
      parent_process_id = if ($_.ParentProcessId) { [uint32]$_.ParentProcessId } else { $null }
    }
  }
)
[pscustomobject]@{
  processes = $processes
} | ConvertTo-Json -Compress
"#;

    let output = run_powershell(script)?;
    let raw: Value = serde_json::from_str(output.stdout.trim()).with_context(|| {
        format!(
            "parse windows launch-state json{}",
            if output.stderr.trim().is_empty() {
                String::new()
            } else {
                format!(" (stderr: {})", output.stderr.trim())
            }
        )
    })?;

    Ok(WindowsLaunchState {
        processes: raw
            .get("processes")
            .and_then(|value| value.as_array())
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| {
                        let object = item.as_object()?;
                        Some(WindowsProcessState {
                            pid: object.get("pid")?.as_u64()? as u32,
                            name: object.get("name")?.as_str()?.to_string(),
                            parent_process_id: object
                                .get("parent_process_id")
                                .and_then(|value| value.as_u64())
                                .map(|value| value as u32),
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
    })
}

#[cfg(target_os = "windows")]
fn observe_windows_pointer_state() -> anyhow::Result<WindowsPointerState> {
    let script = r#"
Add-Type @"
using System;
using System.Runtime.InteropServices;
public static class PointerObserve {
  [StructLayout(LayoutKind.Sequential)]
  public struct POINT {
    public int X;
    public int Y;
  }
  [DllImport("user32.dll")] public static extern bool GetCursorPos(out POINT lpPoint);
}
"@
$point = New-Object PointerObserve+POINT
[PointerObserve]::GetCursorPos([ref]$point) | Out-Null
[pscustomobject]@{
  x = $point.X
  y = $point.Y
} | ConvertTo-Json -Compress
"#;

    let output = run_powershell(script)?;
    let raw: Value = serde_json::from_str(output.stdout.trim()).with_context(|| {
        format!(
            "parse windows pointer-state json{}",
            if output.stderr.trim().is_empty() {
                String::new()
            } else {
                format!(" (stderr: {})", output.stderr.trim())
            }
        )
    })?;

    Ok(WindowsPointerState {
        x: raw
            .get("x")
            .and_then(|value| value.as_i64())
            .unwrap_or_default() as i32,
        y: raw
            .get("y")
            .and_then(|value| value.as_i64())
            .unwrap_or_default() as i32,
    })
}

fn append_focus_verification_signal(
    frame: &mut ObservationFrame,
    expected: &str,
    status: &FocusVerificationStatus,
) {
    frame.summary = format!(
        "{} | focus verify expected={} observed={} matched={} stable={} attempts={} elapsed_ms={} timed_out={}",
        frame.summary,
        expected,
        status.observed.as_deref().unwrap_or("none"),
        status.matched,
        status.stable,
        status.attempts,
        status.elapsed_ms,
        status.timed_out
    );
    frame.structured_signals.push(StructuredSignal {
        key: "focus_verification".into(),
        payload: json!({
            "expected": expected,
            "observed": status.observed,
            "matched": status.matched,
            "stable": status.stable,
            "attempts": status.attempts,
            "elapsed_ms": status.elapsed_ms,
            "timed_out": status.timed_out,
            "poll_ms": status.poll_ms,
            "stable_samples": status.stable_samples,
            "required_stable_samples": status.required_stable_samples,
            "window_present": status.window_present,
        }),
    });
}

fn append_generic_post_action_signal(
    frame: &mut ObservationFrame,
    kind: &str,
    ok: bool,
    evidence: Value,
    summary_suffix: String,
) {
    frame.summary = format!("{} | {}", frame.summary, summary_suffix);
    frame.structured_signals.push(StructuredSignal {
        key: "post_action_verification".into(),
        payload: json!({
            "kind": kind,
            "ok": ok,
            "evidence": evidence,
        }),
    });
}

fn capture_observation_verification_status(
    frame: &ObservationFrame,
) -> CaptureObservationVerificationStatus {
    let screenshot_fingerprint = frame.screenshot_path.as_deref().and_then(file_fingerprint);
    let screenshot_present = frame.screenshot_path.is_some();
    let screenshot_readable = screenshot_fingerprint.is_some();
    let proof_level = if !screenshot_present {
        "missing_screenshot"
    } else if screenshot_readable {
        "screenshot_readable"
    } else {
        "screenshot_unreadable"
    };

    CaptureObservationVerificationStatus {
        screenshot_present,
        screenshot_readable,
        screenshot_fingerprint,
        ok: screenshot_present && screenshot_readable,
        proof_level,
    }
}

#[cfg(target_os = "windows")]
fn enact_windows(action: &ProposedAction) -> anyhow::Result<ActionExecution> {
    let recorded_at = Utc::now();
    let backend = "windows-powershell".to_string();

    match action {
        ProposedAction::FocusWindow { title } => {
            let script = format!(
                "$wshell = New-Object -ComObject WScript.Shell; if (-not $wshell.AppActivate({})) {{ throw 'Unable to focus window' }}",
                ps_literal(title)
            );
            run_powershell(&script)?;
            Ok(ActionExecution {
                id: Uuid::new_v4(),
                action: action.clone(),
                backend,
                summary: format!("focused window matching {}", title),
                evidence: None,
                recorded_at,
            })
        }
        ProposedAction::TypeText { text, submit } => {
            let (before_frame, before_focused_control) = observe_windows_with_metadata()?;
            let before = keyboard_anchor_state_from_frame(&before_frame);
            let requested_text_present_before = before_focused_control
                .as_ref()
                .and_then(|state| state.value.as_deref())
                .map(|value| !text.is_empty() && value.contains(text))
                .unwrap_or(false);
            let mut script = String::from("Add-Type -AssemblyName System.Windows.Forms; ");
            script.push_str(&format!(
                "[System.Windows.Forms.SendKeys]::SendWait({}); ",
                ps_literal(&escape_windows_sendkeys_text(text))
            ));
            if *submit {
                script.push_str("[System.Windows.Forms.SendKeys]::SendWait('{ENTER}');");
            }
            run_powershell(&script)?;
            Ok(ActionExecution {
                id: Uuid::new_v4(),
                action: action.clone(),
                backend,
                summary: format!("typed {} characters", text.chars().count()),
                evidence: Some(json!({
                    "input_anchor_before": {
                        "active_window": before.active_window,
                        "clipboard_present": before.clipboard_present,
                        "clipboard_fingerprint": before.clipboard_fingerprint,
                        "focused_control_name": before.focused_control_name,
                        "focused_control_value_fingerprint": before.focused_control_value_fingerprint,
                    },
                    "requested_text_present_before": requested_text_present_before,
                    "submit": submit,
                })),
                recorded_at,
            })
        }
        ProposedAction::Hotkey { chord } => {
            let (before_frame, _before_focused_control) = observe_windows_with_metadata()?;
            let before = keyboard_anchor_state_from_frame(&before_frame);
            let sendkeys = windows_hotkey_to_sendkeys(chord)?;
            let script = format!(
                "Add-Type -AssemblyName System.Windows.Forms; [System.Windows.Forms.SendKeys]::SendWait({});",
                ps_literal(&sendkeys)
            );
            run_powershell(&script)?;
            Ok(ActionExecution {
                id: Uuid::new_v4(),
                action: action.clone(),
                backend,
                summary: format!("sent hotkey {}", chord),
                evidence: Some(json!({
                    "input_anchor_before": {
                        "active_window": before.active_window,
                        "clipboard_present": before.clipboard_present,
                        "clipboard_fingerprint": before.clipboard_fingerprint,
                        "focused_control_name": before.focused_control_name,
                        "focused_control_value_fingerprint": before.focused_control_value_fingerprint,
                    },
                    "chord": chord,
                })),
                recorded_at,
            })
        }
        ProposedAction::Click { x, y, button } => {
            let (before_frame, before_focused_control) = observe_windows_with_metadata()?;
            run_powershell(&windows_click_script(*x, *y, button, false))?;
            Ok(ActionExecution {
                id: Uuid::new_v4(),
                action: action.clone(),
                backend,
                summary: format!("clicked at {},{}", x, y),
                evidence: Some(json!({
                    "pointer_anchor_before": {
                        "active_window": before_frame.active_window,
                        "focused_control_name": before_focused_control.and_then(|state| state.name),
                        "screenshot_fingerprint": before_frame.screenshot_path.as_deref().and_then(file_fingerprint),
                    }
                })),
                recorded_at,
            })
        }
        ProposedAction::DoubleClick { x, y, button } => {
            let (before_frame, before_focused_control) = observe_windows_with_metadata()?;
            run_powershell(&windows_click_script(*x, *y, button, true))?;
            Ok(ActionExecution {
                id: Uuid::new_v4(),
                action: action.clone(),
                backend,
                summary: format!("double-clicked at {},{}", x, y),
                evidence: Some(json!({
                    "pointer_anchor_before": {
                        "active_window": before_frame.active_window,
                        "focused_control_name": before_focused_control.and_then(|state| state.name),
                        "screenshot_fingerprint": before_frame.screenshot_path.as_deref().and_then(file_fingerprint),
                    }
                })),
                recorded_at,
            })
        }
        ProposedAction::Drag {
            from_x,
            from_y,
            to_x,
            to_y,
        } => {
            let (before_frame, before_focused_control) = observe_windows_with_metadata()?;
            run_powershell(&windows_drag_script(*from_x, *from_y, *to_x, *to_y))?;
            Ok(ActionExecution {
                id: Uuid::new_v4(),
                action: action.clone(),
                backend,
                summary: format!("dragged from {},{} to {},{}", from_x, from_y, to_x, to_y),
                evidence: Some(json!({
                    "pointer_anchor_before": {
                        "active_window": before_frame.active_window,
                        "focused_control_name": before_focused_control.and_then(|state| state.name),
                        "screenshot_fingerprint": before_frame.screenshot_path.as_deref().and_then(file_fingerprint),
                    }
                })),
                recorded_at,
            })
        }
        ProposedAction::Scroll { delta } => {
            let (before_frame, before_focused_control) = observe_windows_with_metadata()?;
            run_powershell(&windows_scroll_script(*delta))?;
            Ok(ActionExecution {
                id: Uuid::new_v4(),
                action: action.clone(),
                backend,
                summary: format!("scrolled {}", delta),
                evidence: Some(json!({
                    "scroll_anchor_before": {
                        "active_window": before_frame.active_window,
                        "focused_control_name": before_focused_control.and_then(|state| state.name),
                        "screenshot_fingerprint": before_frame.screenshot_path.as_deref().and_then(file_fingerprint),
                    },
                    "delta": delta,
                })),
                recorded_at,
            })
        }
        ProposedAction::LaunchProcess { command, args } => {
            let before = observe_windows_launch_state()?;
            let child = Command::new(command)
                .args(args)
                .spawn()
                .with_context(|| format!("launch process {}", command))?;
            Ok(ActionExecution {
                id: Uuid::new_v4(),
                action: action.clone(),
                backend,
                summary: format!("launched {}", command),
                evidence: Some(json!({
                    "launch_anchor_before": {
                        "baseline_process_ids": before
                            .processes
                            .iter()
                            .map(|process| process.pid)
                            .collect::<Vec<_>>(),
                        "spawned_pid": child.id(),
                    }
                })),
                recorded_at,
            })
        }
        ProposedAction::WaitFor { signal, timeout_ms } => {
            let started = Instant::now();
            let mut attempts = 0usize;
            let mut matched = false;
            let deadline = Instant::now() + Duration::from_millis(*timeout_ms);
            while Instant::now() <= deadline {
                attempts += 1;
                let frame = observe_windows()?;
                if signal_matches(&frame, signal) {
                    matched = true;
                    break;
                }
                thread::sleep(Duration::from_millis(250));
            }
            let elapsed_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
            Ok(ActionExecution {
                id: Uuid::new_v4(),
                action: action.clone(),
                backend,
                summary: if matched {
                    format!("wait condition satisfied: {}", signal)
                } else {
                    format!("wait condition timed out: {}", signal)
                },
                evidence: Some(json!({
                    "wait_for_result": {
                        "signal": signal,
                        "matched": matched,
                        "attempts": attempts,
                        "elapsed_ms": elapsed_ms,
                        "timed_out": !matched,
                    }
                })),
                recorded_at,
            })
        }
        ProposedAction::CaptureObservation => Ok(ActionExecution {
            id: Uuid::new_v4(),
            action: action.clone(),
            backend,
            summary: "capture observation requested".into(),
            evidence: None,
            recorded_at,
        }),
    }
}

#[cfg(target_os = "macos")]
fn observe_macos() -> anyhow::Result<ObservationFrame> {
    Ok(observe_macos_with_metadata()?.0)
}

#[cfg(target_os = "macos")]
fn observe_macos_with_metadata()
-> anyhow::Result<(ObservationFrame, Option<MacosFocusedControlState>)> {
    let front_app = run_osascript(
        r#"tell application "System Events" to get name of first application process whose frontmost is true"#,
    )?;
    let window_titles = run_osascript(
        r#"tell application "System Events"
set frontApp to name of first application process whose frontmost is true
try
  tell process frontApp
    set titleList to name of every window
  end tell
  return titleList as string
on error
  return ""
end try
end tell"#,
    )?;
    let screenshot_path = {
        let path = std::env::temp_dir()
            .join("splcw-host")
            .join(format!("screen-{}.png", Uuid::new_v4()));
        std::fs::create_dir_all(path.parent().unwrap())?;
        let output = run_command_capture_with_timeout(
            "screencapture",
            &["-x", path.to_str().unwrap()],
            OBSERVATION_COMMAND_TIMEOUT,
            "run screencapture",
        )?;
        if !output.status.success() {
            bail!("screencapture failed")
        }
        Some(path.to_string_lossy().to_string())
    };
    let clipboard_text =
        run_command_capture_with_timeout("pbpaste", &[], CLIPBOARD_COMMAND_TIMEOUT, "run pbpaste")
            .ok()
            .and_then(|out| String::from_utf8(out.stdout).ok());
    let focused_control = observe_macos_focused_control().ok().flatten();

    let mut capability_set = vec![
        "focus_window",
        "type_text",
        "hotkey",
        "launch_process",
        "capture_observation",
    ];
    if macos_pointer_backend_available() {
        capability_set.extend(["click", "double_click", "drag", "scroll"]);
    }

    let frame = ObservationFrame {
        captured_at: Utc::now(),
        summary: format!("macos observe: active={}", front_app.stdout.trim()),
        screenshot_path,
        ocr_text: None,
        active_window: Some(front_app.stdout.trim().to_string()),
        window_titles: if window_titles.stdout.trim().is_empty() {
            Vec::new()
        } else {
            window_titles
                .stdout
                .split(", ")
                .map(|s| s.trim().to_string())
                .collect()
        },
        clipboard_text,
        structured_signals: vec![
            StructuredSignal {
                key: "platform".into(),
                payload: json!("macos-apple-silicon"),
            },
            StructuredSignal {
                key: "capability_set".into(),
                payload: json!(capability_set),
            },
            StructuredSignal {
                key: "focused_control_state".into(),
                payload: json!({
                    "name": focused_control.as_ref().and_then(|state| state.name.clone()),
                    "value_present": focused_control.as_ref().and_then(|state| state.value.as_ref()).is_some(),
                    "value_fingerprint": focused_control.as_ref().and_then(|state| state.value.as_deref()).map(fingerprint_text),
                    "value_length": focused_control.as_ref().and_then(|state| state.value.as_ref()).map(|value| value.chars().count()),
                }),
            },
            StructuredSignal {
                key: "pointer_backend".into(),
                payload: json!({
                    "available": macos_pointer_backend_available(),
                    "implementation": if macos_pointer_backend_available() {
                        "swift-quartz"
                    } else {
                        "unavailable"
                    },
                }),
            },
        ],
    };

    Ok((frame, focused_control))
}

#[cfg(target_os = "macos")]
fn observe_macos_focus_state() -> anyhow::Result<MacosFocusState> {
    let output = run_osascript(
        r#"tell application "System Events"
set frontApp to ""
set titleList to ""
try
  set frontApp to name of first application process whose frontmost is true
on error
  set frontApp to ""
end try
try
  if frontApp is not "" then
    tell process frontApp
      set titleList to name of every window as string
    end tell
  end if
on error
  set titleList to ""
end try
return frontApp & linefeed & titleList
end tell"#,
    )?;
    let mut lines = output.stdout.lines();
    let active_window = lines
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string());
    let window_titles = lines
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .split(", ")
                .map(|entry| entry.trim().to_string())
                .filter(|entry| !entry.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    Ok(MacosFocusState {
        active_window,
        window_titles,
    })
}

#[cfg(target_os = "macos")]
fn observe_macos_launch_state() -> anyhow::Result<WindowsLaunchState> {
    let output = run_command_capture_with_timeout(
        "ps",
        &["-axo", "pid=,ppid=,comm="],
        OBSERVATION_COMMAND_TIMEOUT,
        "run ps for macos launch-state",
    )?;
    if !output.status.success() {
        bail!(
            "ps launch-state failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let stdout = String::from_utf8(output.stdout)?;
    let processes = stdout
        .lines()
        .filter_map(|line| parse_macos_process_state(line))
        .collect::<Vec<_>>();

    Ok(WindowsLaunchState { processes })
}

#[cfg(target_os = "macos")]
fn observe_macos_pointer_state() -> anyhow::Result<MacosPointerState> {
    let output = run_macos_swift(
        macos_pointer_state_script(),
        MACOS_POINTER_COMMAND_TIMEOUT,
        "observe macos pointer-state",
    )?;
    let raw: Value = serde_json::from_str(output.stdout.trim()).with_context(|| {
        format!(
            "parse macos pointer-state json{}",
            if output.stderr.trim().is_empty() {
                String::new()
            } else {
                format!(" (stderr: {})", output.stderr.trim())
            }
        )
    })?;

    Ok(MacosPointerState {
        x: raw
            .get("x")
            .and_then(|value| value.as_i64())
            .unwrap_or_default() as i32,
        y: raw
            .get("y")
            .and_then(|value| value.as_i64())
            .unwrap_or_default() as i32,
    })
}

#[cfg(target_os = "macos")]
fn observe_macos_focused_control() -> anyhow::Result<Option<MacosFocusedControlState>> {
    if !macos_pointer_backend_available() {
        return Ok(None);
    }

    let output = match run_macos_swift(
        macos_focused_control_signal_script(),
        MACOS_POINTER_COMMAND_TIMEOUT,
        "observe macos focused-control",
    ) {
        Ok(output) => output,
        Err(_) => return Ok(None),
    };
    let raw: Value = serde_json::from_str(output.stdout.trim()).with_context(|| {
        format!(
            "parse macos focused-control json{}",
            if output.stderr.trim().is_empty() {
                String::new()
            } else {
                format!(" (stderr: {})", output.stderr.trim())
            }
        )
    })?;

    Ok(Some(MacosFocusedControlState {
        name: raw
            .get("name")
            .and_then(|value| value.as_str())
            .map(|value| value.to_string()),
        value: raw
            .get("value")
            .and_then(|value| value.as_str())
            .map(|value| value.to_string()),
    }))
}

#[cfg(target_os = "macos")]
fn parse_macos_process_state(line: &str) -> Option<WindowsProcessState> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return None;
    }
    let mut parts = trimmed.split_whitespace();
    let pid = parts.next()?.parse::<u32>().ok()?;
    let parent_process_id = parts.next()?.parse::<u32>().ok()?;
    let name = parts.collect::<Vec<_>>().join(" ");
    if name.is_empty() {
        return None;
    }

    Some(WindowsProcessState {
        pid,
        name,
        parent_process_id: Some(parent_process_id),
    })
}

#[cfg(target_os = "macos")]
fn macos_keyboard_anchor_state_from_frame(
    frame: &ObservationFrame,
    focused_control: Option<&MacosFocusedControlState>,
) -> MacosKeyboardAnchorState {
    let focused_control_signal = focused_control_signal_state(frame);
    MacosKeyboardAnchorState {
        active_window: frame.active_window.clone(),
        clipboard_present: frame.clipboard_text.is_some(),
        clipboard_fingerprint: frame.clipboard_text.as_deref().map(fingerprint_text),
        screenshot_fingerprint: frame.screenshot_path.as_deref().and_then(file_fingerprint),
        focused_control_name: focused_control
            .and_then(|state| state.name.clone())
            .or_else(|| {
                focused_control_signal
                    .as_ref()
                    .and_then(|state| state.name.clone())
            }),
        focused_control_value_fingerprint: focused_control
            .and_then(|state| state.value.as_deref())
            .map(fingerprint_text)
            .or_else(|| focused_control_signal.and_then(|state| state.value_fingerprint)),
    }
}

#[cfg(target_os = "macos")]
fn macos_keyboard_anchor_state_from_execution(
    execution: &ActionExecution,
) -> Option<MacosKeyboardAnchorState> {
    let raw = execution.evidence.as_ref()?.get("input_anchor_before")?;
    Some(MacosKeyboardAnchorState {
        active_window: raw
            .get("active_window")
            .and_then(|value| value.as_str())
            .map(|value| value.to_string()),
        clipboard_present: raw
            .get("clipboard_present")
            .and_then(|value| value.as_bool())
            .unwrap_or(false),
        clipboard_fingerprint: raw
            .get("clipboard_fingerprint")
            .and_then(|value| value.as_u64()),
        screenshot_fingerprint: raw
            .get("screenshot_fingerprint")
            .and_then(|value| value.as_u64()),
        focused_control_name: raw
            .get("focused_control_name")
            .and_then(|value| value.as_str())
            .map(|value| value.to_string()),
        focused_control_value_fingerprint: raw
            .get("focused_control_value_fingerprint")
            .and_then(|value| value.as_u64()),
    })
}

#[cfg(target_os = "macos")]
fn macos_keyboard_verification_status(
    before: &MacosKeyboardAnchorState,
    frame: &ObservationFrame,
    focused_control: Option<&MacosFocusedControlState>,
    requested_text: Option<&str>,
    requested_text_present_before: bool,
    require_observable_effect: bool,
    accept_focused_control_shift_as_effect: bool,
) -> MacosKeyboardVerificationStatus {
    let focused_control_signal = focused_control_signal_state(frame);
    let observed_window = frame.active_window.clone();
    let observed_control_name = focused_control
        .and_then(|state| state.name.clone())
        .or_else(|| {
            focused_control_signal
                .as_ref()
                .and_then(|state| state.name.clone())
        });
    let focus_preserved = before
        .active_window
        .as_deref()
        .zip(observed_window.as_deref())
        .map(|(before, after)| before.eq_ignore_ascii_case(after))
        .unwrap_or(false);
    let window_changed = before.active_window != observed_window;
    let observed_clipboard_present = frame.clipboard_text.is_some();
    let observed_clipboard_fingerprint = frame.clipboard_text.as_deref().map(fingerprint_text);
    let clipboard_changed = before.clipboard_present != observed_clipboard_present
        || before.clipboard_fingerprint != observed_clipboard_fingerprint;
    let focused_control_changed = before
        .focused_control_name
        .as_deref()
        .zip(observed_control_name.as_deref())
        .map(|(before, after)| !before.eq_ignore_ascii_case(after))
        .unwrap_or(false);
    let observed_control_value_fingerprint = focused_control
        .and_then(|state| state.value.as_deref())
        .map(fingerprint_text)
        .or_else(|| focused_control_signal.and_then(|state| state.value_fingerprint));
    let focused_control_value_changed =
        before.focused_control_value_fingerprint != observed_control_value_fingerprint;
    let requested_text_observed = requested_text
        .filter(|text| !text.is_empty())
        .map(|text| {
            focused_control
                .and_then(|state| state.value.as_deref())
                .map(|value| {
                    value.contains(text)
                        && (!requested_text_present_before || focused_control_value_changed)
                })
                .unwrap_or(false)
        })
        .unwrap_or(false);
    let screenshot_changed = before
        .screenshot_fingerprint
        .zip(frame.screenshot_path.as_deref().and_then(file_fingerprint))
        .map(|(before, after)| before != after)
        .unwrap_or(false);
    let observable_effect = window_changed
        || clipboard_changed
        || (accept_focused_control_shift_as_effect && focused_control_changed)
        || focused_control_value_changed
        || requested_text_observed
        || screenshot_changed;
    let proof_level = if requested_text_observed {
        "focused_control_text_match"
    } else if focused_control_value_changed {
        "focused_control_value_delta"
    } else if screenshot_changed {
        "viewport_delta"
    } else if window_changed || clipboard_changed {
        "observable_delta"
    } else if focused_control_changed {
        "focused_control_focus_shift"
    } else if focus_preserved {
        "focus_anchor"
    } else {
        "none"
    };

    MacosKeyboardVerificationStatus {
        before_window: before.active_window.clone(),
        observed_window,
        before_control_name: before.focused_control_name.clone(),
        observed_control_name,
        focus_preserved,
        window_changed,
        clipboard_changed,
        focused_control_changed,
        focused_control_value_changed,
        requested_text_observed,
        screenshot_changed,
        ok: if require_observable_effect {
            observable_effect
        } else {
            focus_preserved || observable_effect
        },
        proof_level,
    }
}

#[cfg(target_os = "macos")]
fn macos_pointer_action_anchor_state_from_frame(
    frame: &ObservationFrame,
    focused_control: Option<&MacosFocusedControlState>,
) -> MacosPointerActionAnchorState {
    let focused_control_signal = focused_control_signal_state(frame);
    MacosPointerActionAnchorState {
        active_window: frame.active_window.clone(),
        focused_control_name: focused_control
            .and_then(|state| state.name.clone())
            .or_else(|| {
                focused_control_signal
                    .as_ref()
                    .and_then(|state| state.name.clone())
            }),
        screenshot_fingerprint: frame.screenshot_path.as_deref().and_then(file_fingerprint),
    }
}

#[cfg(target_os = "macos")]
fn macos_pointer_action_anchor_state_from_execution(
    execution: &ActionExecution,
) -> Option<MacosPointerActionAnchorState> {
    let raw = execution
        .evidence
        .as_ref()?
        .get("pointer_anchor_before")?
        .as_object()?;
    Some(MacosPointerActionAnchorState {
        active_window: raw
            .get("active_window")
            .and_then(|value| value.as_str())
            .map(|value| value.to_string()),
        focused_control_name: raw
            .get("focused_control_name")
            .and_then(|value| value.as_str())
            .map(|value| value.to_string()),
        screenshot_fingerprint: raw
            .get("screenshot_fingerprint")
            .and_then(|value| value.as_u64()),
    })
}

#[cfg(target_os = "macos")]
fn macos_pointer_action_verification_status(
    expected_x: i32,
    expected_y: i32,
    observed: &MacosPointerState,
    before: &MacosPointerActionAnchorState,
    frame: &ObservationFrame,
    observed_screenshot_fingerprint: Option<u64>,
    focused_control: Option<&MacosFocusedControlState>,
) -> MacosPointerActionVerificationStatus {
    let tolerance_px = 4;
    let within_tolerance = (observed.x - expected_x).abs() <= tolerance_px
        && (observed.y - expected_y).abs() <= tolerance_px;
    let focused_control_signal = focused_control_signal_state(frame);
    let observed_window = frame.active_window.clone();
    let window_changed = before.active_window != observed_window;
    let observed_control_name = focused_control
        .and_then(|state| state.name.clone())
        .or_else(|| {
            focused_control_signal
                .as_ref()
                .and_then(|state| state.name.clone())
        });
    let focused_control_changed = before
        .focused_control_name
        .as_deref()
        .zip(observed_control_name.as_deref())
        .map(|(before, after)| !before.eq_ignore_ascii_case(after))
        .unwrap_or(false);
    let screenshot_changed = before
        .screenshot_fingerprint
        .zip(observed_screenshot_fingerprint)
        .map(|(before, after)| before != after)
        .unwrap_or(false);
    let proof_level = if screenshot_changed {
        "viewport_delta"
    } else if window_changed {
        "observable_delta"
    } else if focused_control_changed {
        "focused_control_focus_shift"
    } else if within_tolerance {
        "pointer_target_only"
    } else {
        "none"
    };

    MacosPointerActionVerificationStatus {
        observed_x: observed.x,
        observed_y: observed.y,
        within_tolerance,
        expected_x,
        expected_y,
        tolerance_px,
        before_window: before.active_window.clone(),
        observed_window,
        window_changed,
        focused_control_changed,
        screenshot_changed,
        ok: within_tolerance && (window_changed || screenshot_changed),
        proof_level,
    }
}

#[cfg(target_os = "macos")]
fn macos_scroll_anchor_state_from_execution(
    execution: &ActionExecution,
) -> Option<MacosScrollAnchorState> {
    let raw = execution
        .evidence
        .as_ref()?
        .get("scroll_anchor_before")?
        .as_object()?;
    Some(MacosScrollAnchorState {
        active_window: raw
            .get("active_window")
            .and_then(|value| value.as_str())
            .map(|value| value.to_string()),
        focused_control_name: raw
            .get("focused_control_name")
            .and_then(|value| value.as_str())
            .map(|value| value.to_string()),
        screenshot_fingerprint: raw
            .get("screenshot_fingerprint")
            .and_then(|value| value.as_u64()),
    })
}

#[cfg(target_os = "macos")]
fn macos_scroll_verification_status(
    before: &MacosScrollAnchorState,
    frame: &ObservationFrame,
    observed_screenshot_fingerprint: Option<u64>,
    focused_control: Option<&MacosFocusedControlState>,
) -> MacosScrollVerificationStatus {
    let focused_control_signal = focused_control_signal_state(frame);
    let observed_window = frame.active_window.clone();
    let window_changed = before.active_window != observed_window;
    let observed_control_name = focused_control
        .and_then(|state| state.name.clone())
        .or_else(|| {
            focused_control_signal
                .as_ref()
                .and_then(|state| state.name.clone())
        });
    let focused_control_changed = before
        .focused_control_name
        .as_deref()
        .zip(observed_control_name.as_deref())
        .map(|(before, after)| !before.eq_ignore_ascii_case(after))
        .unwrap_or(false);
    let screenshot_changed = before
        .screenshot_fingerprint
        .zip(observed_screenshot_fingerprint)
        .map(|(before, after)| before != after)
        .unwrap_or(false);
    let proof_level = if screenshot_changed {
        "viewport_delta"
    } else if window_changed {
        "observable_delta"
    } else if focused_control_changed {
        "focused_control_focus_shift"
    } else {
        "none"
    };

    MacosScrollVerificationStatus {
        before_window: before.active_window.clone(),
        observed_window,
        window_changed,
        focused_control_changed,
        screenshot_changed,
        ok: window_changed || screenshot_changed,
        proof_level,
    }
}

#[cfg(target_os = "macos")]
fn enact_macos(action: &ProposedAction) -> anyhow::Result<ActionExecution> {
    let recorded_at = Utc::now();

    match action {
        ProposedAction::FocusWindow { title } => {
            let script = format!(
                r#"tell application "{}" to activate"#,
                title.replace('"', "\\\"")
            );
            run_osascript(&script)?;
            Ok(ActionExecution {
                id: Uuid::new_v4(),
                action: action.clone(),
                backend: "macos-osascript".to_string(),
                summary: format!("focused application {}", title),
                evidence: None,
                recorded_at,
            })
        }
        ProposedAction::TypeText { text, submit } => {
            let (before_frame, before_focused_control) = observe_macos_with_metadata()?;
            let before = macos_keyboard_anchor_state_from_frame(
                &before_frame,
                before_focused_control.as_ref(),
            );
            let requested_text_present_before = before_focused_control
                .as_ref()
                .and_then(|state| state.value.as_deref())
                .map(|value| !text.is_empty() && value.contains(text))
                .unwrap_or(false);
            let mut script = format!(
                r#"tell application "System Events" to keystroke "{}""#,
                apple_script_escape(text)
            );
            if *submit {
                script.push_str(
                    r#"
tell application "System Events" to key code 36"#,
                );
            }
            run_osascript(&script)?;
            Ok(ActionExecution {
                id: Uuid::new_v4(),
                action: action.clone(),
                backend: "macos-osascript".to_string(),
                summary: format!("typed {} characters", text.chars().count()),
                evidence: Some(json!({
                    "input_anchor_before": {
                        "active_window": before.active_window,
                        "clipboard_present": before.clipboard_present,
                        "clipboard_fingerprint": before.clipboard_fingerprint,
                        "screenshot_fingerprint": before.screenshot_fingerprint,
                        "focused_control_name": before.focused_control_name,
                        "focused_control_value_fingerprint": before.focused_control_value_fingerprint,
                    },
                    "requested_text_present_before": requested_text_present_before,
                    "submit": submit,
                })),
                recorded_at,
            })
        }
        ProposedAction::Hotkey { chord } => {
            let (before_frame, before_focused_control) = observe_macos_with_metadata()?;
            let before = macos_keyboard_anchor_state_from_frame(
                &before_frame,
                before_focused_control.as_ref(),
            );
            let script = macos_hotkey_script(chord)?;
            run_osascript(&script)?;
            Ok(ActionExecution {
                id: Uuid::new_v4(),
                action: action.clone(),
                backend: "macos-osascript".to_string(),
                summary: format!("sent hotkey {}", chord),
                evidence: Some(json!({
                    "input_anchor_before": {
                        "active_window": before.active_window,
                        "clipboard_present": before.clipboard_present,
                        "clipboard_fingerprint": before.clipboard_fingerprint,
                        "screenshot_fingerprint": before.screenshot_fingerprint,
                        "focused_control_name": before.focused_control_name,
                        "focused_control_value_fingerprint": before.focused_control_value_fingerprint,
                    },
                    "chord": chord,
                })),
                recorded_at,
            })
        }
        ProposedAction::LaunchProcess { command, args } => {
            let before = observe_macos_launch_state()?;
            let child = Command::new(command)
                .args(args)
                .spawn()
                .with_context(|| format!("launch process {}", command))?;
            Ok(ActionExecution {
                id: Uuid::new_v4(),
                action: action.clone(),
                backend: "macos-osascript".to_string(),
                summary: format!("launched {}", command),
                evidence: Some(json!({
                    "launch_anchor_before": {
                        "baseline_process_ids": before
                            .processes
                            .iter()
                            .map(|process| process.pid)
                            .collect::<Vec<_>>(),
                        "spawned_pid": child.id(),
                    }
                })),
                recorded_at,
            })
        }
        ProposedAction::WaitFor { signal, timeout_ms } => {
            let started = Instant::now();
            let mut attempts = 0usize;
            let mut matched = false;
            let deadline = Instant::now() + Duration::from_millis(*timeout_ms);
            while Instant::now() <= deadline {
                attempts += 1;
                let frame = observe_macos()?;
                if signal_matches(&frame, signal) {
                    matched = true;
                    break;
                }
                thread::sleep(Duration::from_millis(250));
            }
            let elapsed_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
            Ok(ActionExecution {
                id: Uuid::new_v4(),
                action: action.clone(),
                backend: "macos-osascript".to_string(),
                summary: if matched {
                    format!("wait condition satisfied: {}", signal)
                } else {
                    format!("wait condition timed out: {}", signal)
                },
                evidence: Some(json!({
                    "wait_for_result": {
                        "signal": signal,
                        "matched": matched,
                        "attempts": attempts,
                        "elapsed_ms": elapsed_ms,
                        "timed_out": !matched,
                    }
                })),
                recorded_at,
            })
        }
        ProposedAction::CaptureObservation => Ok(ActionExecution {
            id: Uuid::new_v4(),
            action: action.clone(),
            backend: "macos-osascript".to_string(),
            summary: "capture observation requested".into(),
            evidence: None,
            recorded_at,
        }),
        ProposedAction::Click { x, y, button } => {
            ensure_macos_pointer_backend_available()?;
            let (before_frame, before_focused_control) = observe_macos_with_metadata()?;
            let before = macos_pointer_action_anchor_state_from_frame(
                &before_frame,
                before_focused_control.as_ref(),
            );
            run_macos_swift(
                &macos_click_script(*x, *y, button, false),
                MACOS_POINTER_COMMAND_TIMEOUT,
                "run macos click",
            )?;
            Ok(ActionExecution {
                id: Uuid::new_v4(),
                action: action.clone(),
                backend: "macos-swift-quartz".to_string(),
                summary: format!("clicked at {},{}", x, y),
                evidence: Some(json!({
                    "pointer_anchor_before": {
                        "active_window": before.active_window,
                        "focused_control_name": before.focused_control_name,
                        "screenshot_fingerprint": before.screenshot_fingerprint,
                    }
                })),
                recorded_at,
            })
        }
        ProposedAction::DoubleClick { x, y, button } => {
            ensure_macos_pointer_backend_available()?;
            let (before_frame, before_focused_control) = observe_macos_with_metadata()?;
            let before = macos_pointer_action_anchor_state_from_frame(
                &before_frame,
                before_focused_control.as_ref(),
            );
            run_macos_swift(
                &macos_click_script(*x, *y, button, true),
                MACOS_POINTER_COMMAND_TIMEOUT,
                "run macos double click",
            )?;
            Ok(ActionExecution {
                id: Uuid::new_v4(),
                action: action.clone(),
                backend: "macos-swift-quartz".to_string(),
                summary: format!("double-clicked at {},{}", x, y),
                evidence: Some(json!({
                    "pointer_anchor_before": {
                        "active_window": before.active_window,
                        "focused_control_name": before.focused_control_name,
                        "screenshot_fingerprint": before.screenshot_fingerprint,
                    }
                })),
                recorded_at,
            })
        }
        ProposedAction::Drag {
            from_x,
            from_y,
            to_x,
            to_y,
        } => {
            ensure_macos_pointer_backend_available()?;
            let (before_frame, before_focused_control) = observe_macos_with_metadata()?;
            let before = macos_pointer_action_anchor_state_from_frame(
                &before_frame,
                before_focused_control.as_ref(),
            );
            run_macos_swift(
                &macos_drag_script(*from_x, *from_y, *to_x, *to_y),
                MACOS_POINTER_COMMAND_TIMEOUT,
                "run macos drag",
            )?;
            Ok(ActionExecution {
                id: Uuid::new_v4(),
                action: action.clone(),
                backend: "macos-swift-quartz".to_string(),
                summary: format!("dragged from {},{} to {},{}", from_x, from_y, to_x, to_y),
                evidence: Some(json!({
                    "pointer_anchor_before": {
                        "active_window": before.active_window,
                        "focused_control_name": before.focused_control_name,
                        "screenshot_fingerprint": before.screenshot_fingerprint,
                    }
                })),
                recorded_at,
            })
        }
        ProposedAction::Scroll { delta } => {
            ensure_macos_pointer_backend_available()?;
            let (before_frame, before_focused_control) = observe_macos_with_metadata()?;
            run_macos_swift(
                &macos_scroll_script(*delta),
                MACOS_POINTER_COMMAND_TIMEOUT,
                "run macos scroll",
            )?;
            Ok(ActionExecution {
                id: Uuid::new_v4(),
                action: action.clone(),
                backend: "macos-swift-quartz".to_string(),
                summary: format!("scrolled {}", delta),
                evidence: Some(json!({
                    "scroll_anchor_before": {
                        "active_window": before_frame.active_window,
                        "focused_control_name": before_focused_control.and_then(|state| state.name),
                        "screenshot_fingerprint": before_frame.screenshot_path.as_deref().and_then(file_fingerprint),
                    }
                })),
                recorded_at,
            })
        }
    }
}

#[cfg(target_os = "windows")]
fn run_powershell(script: &str) -> anyhow::Result<CommandCapture> {
    let candidates = ["powershell", "pwsh"];
    let mut not_found = Vec::new();

    for candidate in candidates {
        match run_command_capture_with_timeout(
            candidate,
            &["-NoProfile", "-NonInteractive", "-Command", script],
            OBSERVATION_COMMAND_TIMEOUT,
            "run PowerShell",
        ) {
            Ok(output) => {
                if !output.status.success() {
                    bail!(
                        "{} failed: {}",
                        candidate,
                        String::from_utf8_lossy(&output.stderr)
                    );
                }
                return Ok(CommandCapture {
                    stdout: String::from_utf8(output.stdout)?,
                    stderr: String::from_utf8(output.stderr)?,
                });
            }
            Err(error) => {
                let text = format!("{error:#}");
                if text.contains("not found") {
                    not_found.push(candidate.to_string());
                    continue;
                }
                return Err(error);
            }
        }
    }

    bail!(
        "no PowerShell executable is available (tried: {})",
        not_found.join(", ")
    )
}

#[cfg(target_os = "macos")]
fn run_osascript(script: &str) -> anyhow::Result<CommandCapture> {
    let output = run_command_capture_with_timeout(
        "osascript",
        &["-e", script],
        SHELL_COMMAND_TIMEOUT,
        "run osascript",
    )?;
    if !output.status.success() {
        bail!(
            "osascript failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(CommandCapture {
        stdout: String::from_utf8(output.stdout)?,
        stderr: String::from_utf8(output.stderr)?,
    })
}

#[cfg(target_os = "macos")]
fn macos_pointer_backend_available() -> bool {
    *MACOS_POINTER_BACKEND_AVAILABLE.get_or_init(|| {
        run_command_capture_with_timeout(
            "xcrun",
            &["--find", "swift"],
            SHELL_COMMAND_TIMEOUT,
            "locate swift via xcrun",
        )
        .map(|output| output.status.success())
        .unwrap_or(false)
            || run_command_capture_with_timeout(
                "swift",
                &["-version"],
                SHELL_COMMAND_TIMEOUT,
                "probe swift",
            )
            .map(|output| output.status.success())
            .unwrap_or(false)
    })
}

#[cfg(target_os = "macos")]
fn ensure_macos_pointer_backend_available() -> anyhow::Result<()> {
    if macos_pointer_backend_available() {
        return Ok(());
    }
    bail!("macOS pointer backend requires Swift/Quartz tooling and is not available on this host")
}

#[cfg(target_os = "macos")]
fn run_macos_swift(
    script: &str,
    timeout: Duration,
    context_label: &str,
) -> anyhow::Result<CommandCapture> {
    let candidates = [
        ("xcrun", vec!["swift", "-e", script]),
        ("swift", vec!["-e", script]),
    ];
    let mut not_found = Vec::new();
    let mut last_error = None;

    for (program, args) in candidates {
        match run_command_capture_with_timeout(program, &args, timeout, context_label) {
            Ok(output) if output.status.success() => {
                return Ok(CommandCapture {
                    stdout: String::from_utf8(output.stdout)?,
                    stderr: String::from_utf8(output.stderr)?,
                });
            }
            Ok(output) => {
                let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
                let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
                let detail = if !stderr.is_empty() {
                    stderr
                } else if !stdout.is_empty() {
                    stdout
                } else {
                    format!("{program} exited with status {}", output.status)
                };
                last_error = Some(anyhow::anyhow!("{program} failed: {detail}"));
            }
            Err(error) => {
                let text = format!("{error:#}");
                if text.contains("not found") {
                    not_found.push(program.to_string());
                    continue;
                }
                last_error = Some(error);
            }
        }
    }

    if let Some(error) = last_error {
        return Err(error);
    }

    bail!(
        "no Swift executable is available for macOS pointer backend (tried: {})",
        not_found.join(", ")
    )
}

fn run_command_capture_with_timeout(
    program: &str,
    args: &[&str],
    timeout: Duration,
    context_label: &str,
) -> anyhow::Result<Output> {
    let mut child = Command::new(program)
        .args(args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .with_context(|| format!("{context_label}: spawn {}", program))?;

    let started = Instant::now();
    loop {
        if let Some(_status) = child
            .try_wait()
            .with_context(|| format!("{context_label}: poll {}", program))?
        {
            return child
                .wait_with_output()
                .with_context(|| format!("{context_label}: collect {}", program));
        }
        if started.elapsed() >= timeout {
            let _ = child.kill();
            let output = child
                .wait_with_output()
                .with_context(|| format!("{context_label}: collect killed {}", program))?;
            bail!(
                "{} timed out after {:?} while running {}{}",
                context_label,
                timeout,
                program,
                if output.stderr.is_empty() {
                    String::new()
                } else {
                    format!(" ({})", String::from_utf8_lossy(&output.stderr).trim())
                }
            );
        }
        thread::sleep(Duration::from_millis(25));
    }
}

fn signal_matches(frame: &ObservationFrame, signal: &str) -> bool {
    if let Some(expected) = signal.strip_prefix("window:") {
        let expected = expected.to_ascii_lowercase();
        return frame
            .window_titles
            .iter()
            .any(|title| title.to_ascii_lowercase().contains(&expected))
            || frame
                .active_window
                .as_ref()
                .map(|title| title.to_ascii_lowercase().contains(&expected))
                .unwrap_or(false);
    }

    if let Some(expected) = signal.strip_prefix("clipboard:") {
        return frame
            .clipboard_text
            .as_ref()
            .map(|text| text.contains(expected))
            .unwrap_or(false);
    }

    frame.summary.contains(signal)
        || frame
            .ocr_text
            .as_ref()
            .map(|text| text.contains(signal))
            .unwrap_or(false)
}

fn wait_for_verification_status_from_execution(
    execution: &ActionExecution,
) -> Option<WaitForVerificationStatus> {
    let raw = execution
        .evidence
        .as_ref()?
        .get("wait_for_result")?
        .as_object()?;
    let matched = raw.get("matched")?.as_bool()?;
    Some(WaitForVerificationStatus {
        signal: raw.get("signal")?.as_str()?.to_string(),
        matched,
        attempts: raw.get("attempts")?.as_u64()? as usize,
        elapsed_ms: raw.get("elapsed_ms")?.as_u64()?,
        timed_out: raw
            .get("timed_out")
            .and_then(|value| value.as_bool())
            .unwrap_or(!matched),
        proof_level: if matched {
            "wait_signal_match"
        } else {
            "wait_signal_timeout"
        },
    })
}

#[cfg_attr(target_os = "macos", allow(dead_code))]
fn action_kind(action: &ProposedAction) -> &'static str {
    match action {
        ProposedAction::FocusWindow { .. } => "focus_window",
        ProposedAction::Click { .. } => "click",
        ProposedAction::DoubleClick { .. } => "double_click",
        ProposedAction::Drag { .. } => "drag",
        ProposedAction::TypeText { .. } => "type_text",
        ProposedAction::Hotkey { .. } => "hotkey",
        ProposedAction::Scroll { .. } => "scroll",
        ProposedAction::LaunchProcess { .. } => "launch_process",
        ProposedAction::WaitFor { .. } => "wait_for",
        ProposedAction::CaptureObservation => "capture_observation",
    }
}

fn contains_case_insensitive(haystack: &str, needle: &str) -> bool {
    haystack
        .to_ascii_lowercase()
        .contains(&needle.to_ascii_lowercase())
}

#[cfg(target_os = "windows")]
fn pointer_verification_status(
    expected_x: i32,
    expected_y: i32,
    observed: &WindowsPointerState,
) -> PointerVerificationStatus {
    let tolerance_px = 4;
    PointerVerificationStatus {
        observed_x: observed.x,
        observed_y: observed.y,
        within_tolerance: (observed.x - expected_x).abs() <= tolerance_px
            && (observed.y - expected_y).abs() <= tolerance_px,
        expected_x,
        expected_y,
        tolerance_px,
    }
}

#[cfg(target_os = "windows")]
fn pointer_action_anchor_state_from_execution(
    execution: &ActionExecution,
) -> Option<WindowsPointerActionAnchorState> {
    let raw = execution
        .evidence
        .as_ref()?
        .get("pointer_anchor_before")?
        .as_object()?;
    Some(WindowsPointerActionAnchorState {
        active_window: raw
            .get("active_window")
            .and_then(|value| value.as_str())
            .map(|value| value.to_string()),
        focused_control_name: raw
            .get("focused_control_name")
            .and_then(|value| value.as_str())
            .map(|value| value.to_string()),
        screenshot_fingerprint: raw
            .get("screenshot_fingerprint")
            .and_then(|value| value.as_u64()),
    })
}

fn launch_anchor_state_from_execution(
    execution: &ActionExecution,
) -> Option<WindowsLaunchAnchorState> {
    let raw = execution
        .evidence
        .as_ref()?
        .get("launch_anchor_before")?
        .as_object()?;
    let baseline_process_ids = raw
        .get("baseline_process_ids")
        .and_then(|value| value.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_u64().map(|value| value as u32))
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default();
    Some(WindowsLaunchAnchorState {
        baseline_process_ids,
        spawned_pid: raw
            .get("spawned_pid")
            .and_then(|value| value.as_u64())
            .map(|value| value as u32),
    })
}

#[cfg(target_os = "windows")]
fn pointer_action_verification_status(
    expected_x: i32,
    expected_y: i32,
    observed: &WindowsPointerState,
    before: &WindowsPointerActionAnchorState,
    frame: &ObservationFrame,
    observed_screenshot_fingerprint: Option<u64>,
    focused_control: Option<&WindowsFocusedControlState>,
) -> PointerActionVerificationStatus {
    let base = pointer_verification_status(expected_x, expected_y, observed);
    let focused_control_signal = focused_control_signal_state(frame);
    let observed_control_name = focused_control
        .and_then(|state| state.name.clone())
        .or_else(|| {
            focused_control_signal
                .as_ref()
                .and_then(|state| state.name.clone())
        });
    let observed_window = frame.active_window.clone();
    let window_changed = before.active_window != observed_window;
    let focused_control_changed = before
        .focused_control_name
        .as_deref()
        .zip(observed_control_name.as_deref())
        .map(|(before, after)| !before.eq_ignore_ascii_case(after))
        .unwrap_or(false);
    let screenshot_changed = before
        .screenshot_fingerprint
        .zip(observed_screenshot_fingerprint)
        .map(|(before, after)| before != after)
        .unwrap_or(false);
    let proof_level = if screenshot_changed {
        "viewport_delta"
    } else if window_changed {
        "observable_delta"
    } else if focused_control_changed {
        "focused_control_focus_shift"
    } else if base.within_tolerance {
        "pointer_target_only"
    } else {
        "none"
    };

    PointerActionVerificationStatus {
        observed_x: base.observed_x,
        observed_y: base.observed_y,
        within_tolerance: base.within_tolerance,
        expected_x: base.expected_x,
        expected_y: base.expected_y,
        tolerance_px: base.tolerance_px,
        before_window: before.active_window.clone(),
        observed_window,
        before_control_name: before.focused_control_name.clone(),
        observed_control_name,
        window_changed,
        focused_control_changed,
        screenshot_changed,
        ok: base.within_tolerance
            && (screenshot_changed || window_changed || focused_control_changed),
        proof_level,
    }
}

#[cfg(target_os = "windows")]
fn keyboard_anchor_state_from_frame(frame: &ObservationFrame) -> WindowsKeyboardAnchorState {
    let focused_control = focused_control_signal_state(frame);
    WindowsKeyboardAnchorState {
        active_window: frame.active_window.clone(),
        clipboard_present: frame.clipboard_text.is_some(),
        clipboard_fingerprint: frame.clipboard_text.as_deref().map(fingerprint_text),
        focused_control_name: focused_control
            .as_ref()
            .and_then(|state| state.name.clone()),
        focused_control_value_fingerprint: focused_control
            .and_then(|state| state.value_fingerprint),
    }
}

#[cfg(target_os = "windows")]
fn keyboard_anchor_state_from_execution(
    execution: &ActionExecution,
) -> Option<WindowsKeyboardAnchorState> {
    let raw = execution.evidence.as_ref()?.get("input_anchor_before")?;
    Some(WindowsKeyboardAnchorState {
        active_window: raw
            .get("active_window")
            .and_then(|value| value.as_str())
            .map(|value| value.to_string()),
        clipboard_present: raw
            .get("clipboard_present")
            .and_then(|value| value.as_bool())
            .unwrap_or(false),
        clipboard_fingerprint: raw
            .get("clipboard_fingerprint")
            .and_then(|value| value.as_u64()),
        focused_control_name: raw
            .get("focused_control_name")
            .and_then(|value| value.as_str())
            .map(|value| value.to_string()),
        focused_control_value_fingerprint: raw
            .get("focused_control_value_fingerprint")
            .and_then(|value| value.as_u64()),
    })
}

#[cfg(target_os = "windows")]
fn keyboard_verification_status(
    before: &WindowsKeyboardAnchorState,
    frame: &ObservationFrame,
    focused_control: Option<&WindowsFocusedControlState>,
    requested_text: Option<&str>,
    requested_text_present_before: bool,
    require_observable_effect: bool,
    accept_focused_control_shift_as_effect: bool,
) -> KeyboardVerificationStatus {
    let observed_window = frame.active_window.clone();
    let focused_control_signal = focused_control_signal_state(frame);
    let observed_control_name = focused_control
        .and_then(|state| state.name.clone())
        .or_else(|| {
            focused_control_signal
                .as_ref()
                .and_then(|state| state.name.clone())
        });
    let focused_control_changed = before
        .focused_control_name
        .as_deref()
        .zip(observed_control_name.as_deref())
        .map(|(before, after)| !before.eq_ignore_ascii_case(after))
        .unwrap_or(false);
    let focus_preserved = before
        .active_window
        .as_deref()
        .zip(observed_window.as_deref())
        .map(|(before, after)| before.eq_ignore_ascii_case(after))
        .unwrap_or(false);
    let window_changed = before.active_window != observed_window;
    let observed_clipboard_present = frame.clipboard_text.is_some();
    let observed_clipboard_fingerprint = frame.clipboard_text.as_deref().map(fingerprint_text);
    let clipboard_changed = before.clipboard_present != observed_clipboard_present
        || before.clipboard_fingerprint != observed_clipboard_fingerprint;
    let observed_control_value_fingerprint = focused_control
        .and_then(|state| state.value.as_deref())
        .map(fingerprint_text);
    let observed_control_value_fingerprint = observed_control_value_fingerprint
        .or_else(|| focused_control_signal.and_then(|state| state.value_fingerprint));
    let focused_control_value_changed =
        before.focused_control_value_fingerprint != observed_control_value_fingerprint;
    let requested_text_observed = requested_text
        .filter(|text| !text.is_empty())
        .map(|text| {
            focused_control
                .and_then(|state| state.value.as_deref())
                .map(|value| {
                    value.contains(text)
                        && (!requested_text_present_before || focused_control_value_changed)
                })
                .unwrap_or(false)
        })
        .unwrap_or(false);
    let observable_effect = window_changed
        || clipboard_changed
        || (accept_focused_control_shift_as_effect && focused_control_changed)
        || focused_control_value_changed;
    let proof_level = if requested_text_observed {
        "focused_control_text_match"
    } else if clipboard_changed || window_changed {
        "observable_delta"
    } else if focused_control_changed {
        "focused_control_focus_shift"
    } else if focused_control_value_changed {
        "focused_control_value_delta"
    } else if focus_preserved {
        "focus_anchor"
    } else {
        "none"
    };

    KeyboardVerificationStatus {
        before_window: before.active_window.clone(),
        observed_window,
        before_control_name: before.focused_control_name.clone(),
        observed_control_name,
        focus_preserved,
        window_changed,
        clipboard_changed,
        focused_control_changed,
        focused_control_value_changed,
        requested_text_observed,
        ok: if require_observable_effect {
            observable_effect
        } else {
            focus_preserved || observable_effect
        },
        proof_level,
    }
}

#[cfg(target_os = "windows")]
fn scroll_anchor_state_from_execution(
    execution: &ActionExecution,
) -> Option<WindowsScrollAnchorState> {
    let raw = execution
        .evidence
        .as_ref()?
        .get("scroll_anchor_before")?
        .as_object()?;
    Some(WindowsScrollAnchorState {
        active_window: raw
            .get("active_window")
            .and_then(|value| value.as_str())
            .map(|value| value.to_string()),
        focused_control_name: raw
            .get("focused_control_name")
            .and_then(|value| value.as_str())
            .map(|value| value.to_string()),
        screenshot_fingerprint: raw
            .get("screenshot_fingerprint")
            .and_then(|value| value.as_u64()),
    })
}

#[cfg(target_os = "windows")]
fn scroll_verification_status(
    before: &WindowsScrollAnchorState,
    frame: &ObservationFrame,
    observed_screenshot_fingerprint: Option<u64>,
    focused_control: Option<&WindowsFocusedControlState>,
) -> ScrollVerificationStatus {
    let observed_window = frame.active_window.clone();
    let focused_control_signal = focused_control_signal_state(frame);
    let observed_control_name = focused_control
        .and_then(|state| state.name.clone())
        .or_else(|| {
            focused_control_signal
                .as_ref()
                .and_then(|state| state.name.clone())
        });
    let window_changed = before.active_window != observed_window;
    let focused_control_changed = before
        .focused_control_name
        .as_deref()
        .zip(observed_control_name.as_deref())
        .map(|(before, after)| !before.eq_ignore_ascii_case(after))
        .unwrap_or(false);
    let screenshot_changed = before
        .screenshot_fingerprint
        .zip(observed_screenshot_fingerprint)
        .map(|(before, after)| before != after)
        .unwrap_or(false);
    let proof_level = if screenshot_changed {
        "viewport_delta"
    } else if window_changed {
        "observable_delta"
    } else if focused_control_changed {
        "focused_control_focus_shift"
    } else {
        "none"
    };

    ScrollVerificationStatus {
        before_window: before.active_window.clone(),
        observed_window,
        before_control_name: before.focused_control_name.clone(),
        observed_control_name,
        window_changed,
        focused_control_changed,
        screenshot_changed,
        ok: screenshot_changed || window_changed || focused_control_changed,
        proof_level,
    }
}

fn fingerprint_text(text: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    text.hash(&mut hasher);
    hasher.finish()
}

#[cfg_attr(target_os = "macos", allow(dead_code))]
fn file_fingerprint(path: &str) -> Option<u64> {
    let mut file = std::fs::File::open(path).ok()?;
    let mut buffer = [0u8; 8192];
    let mut hasher = DefaultHasher::new();
    loop {
        let read = file.read(&mut buffer).ok()?;
        if read == 0 {
            break;
        }
        buffer[..read].hash(&mut hasher);
    }
    Some(hasher.finish())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FocusedControlSignalState {
    name: Option<String>,
    value_fingerprint: Option<u64>,
}

fn focused_control_signal_state(frame: &ObservationFrame) -> Option<FocusedControlSignalState> {
    let raw = frame
        .structured_signals
        .iter()
        .find(|entry| entry.key == "focused_control_state")?
        .payload
        .clone();
    Some(FocusedControlSignalState {
        name: raw
            .get("name")
            .and_then(|value| value.as_str())
            .map(|value| value.to_string()),
        value_fingerprint: raw
            .get("value_fingerprint")
            .and_then(|value| value.as_u64()),
    })
}

fn launch_target_token(command: &str) -> String {
    let path = Path::new(command);
    path.file_stem()
        .or_else(|| path.file_name())
        .map(|value| value.to_string_lossy().to_string())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| command.to_string())
}

#[cfg(target_os = "windows")]
fn ps_literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

#[cfg(target_os = "windows")]
fn escape_windows_sendkeys_text(text: &str) -> String {
    let mut out = String::new();
    for ch in text.chars() {
        match ch {
            '+' => out.push_str("{+}"),
            '^' => out.push_str("{^}"),
            '%' => out.push_str("{%}"),
            '~' => out.push_str("{~}"),
            '(' => out.push_str("{(}"),
            ')' => out.push_str("{)}"),
            '[' => out.push_str("{[}"),
            ']' => out.push_str("{]}"),
            '{' => out.push_str("{{}"),
            '}' => out.push_str("{}}"),
            '\n' => out.push_str("{ENTER}"),
            _ => out.push(ch),
        }
    }
    out
}

#[cfg(target_os = "windows")]
fn windows_hotkey_to_sendkeys(chord: &str) -> anyhow::Result<String> {
    let mut modifiers = String::new();
    let mut key = None;
    for part in chord.split('+').map(|s| s.trim()).filter(|s| !s.is_empty()) {
        match part.to_ascii_lowercase().as_str() {
            "ctrl" | "control" => modifiers.push('^'),
            "alt" => modifiers.push('%'),
            "shift" => modifiers.push('+'),
            "cmd" | "win" | "meta" => modifiers.push_str("^{ESC}"),
            other => key = Some(other.to_string()),
        }
    }
    let key = key.context("hotkey missing terminal key")?;
    let key = match key.as_str() {
        "enter" => "{ENTER}".to_string(),
        "tab" => "{TAB}".to_string(),
        "esc" | "escape" => "{ESC}".to_string(),
        "up" => "{UP}".to_string(),
        "down" => "{DOWN}".to_string(),
        "left" => "{LEFT}".to_string(),
        "right" => "{RIGHT}".to_string(),
        "delete" => "{DEL}".to_string(),
        "backspace" => "{BACKSPACE}".to_string(),
        single if single.chars().count() == 1 => single.to_string(),
        other => format!("{{{}}}", other.to_ascii_uppercase()),
    };
    Ok(format!("{}{}", modifiers, key))
}

#[cfg(target_os = "windows")]
fn windows_mouse_flags(button: &MouseButton) -> (u32, u32) {
    match button {
        MouseButton::Left => (0x0002, 0x0004),
        MouseButton::Right => (0x0008, 0x0010),
        MouseButton::Middle => (0x0020, 0x0040),
    }
}

#[cfg(target_os = "windows")]
fn windows_mouse_preamble() -> &'static str {
    r#"
Add-Type @"
using System;
using System.Runtime.InteropServices;
public static class MouseOps {
  [DllImport("user32.dll")] public static extern bool SetCursorPos(int X, int Y);
  [DllImport("user32.dll")] public static extern void mouse_event(uint dwFlags, uint dx, uint dy, uint dwData, UIntPtr dwExtraInfo);
}
"@
"#
}

#[cfg(target_os = "windows")]
fn windows_click_script(x: i32, y: i32, button: &MouseButton, double: bool) -> String {
    let (down, up) = windows_mouse_flags(button);
    let repeat = if double { 2 } else { 1 };
    format!(
        r#"{preamble}
[MouseOps]::SetCursorPos({x}, {y}) | Out-Null
for ($i = 0; $i -lt {repeat}; $i++) {{
  [MouseOps]::mouse_event({down}, 0, 0, 0, [UIntPtr]::Zero)
  Start-Sleep -Milliseconds 30
  [MouseOps]::mouse_event({up}, 0, 0, 0, [UIntPtr]::Zero)
  Start-Sleep -Milliseconds 60
}}
"#,
        preamble = windows_mouse_preamble(),
        x = x,
        y = y,
        repeat = repeat,
        down = down,
        up = up
    )
}

#[cfg(target_os = "windows")]
fn windows_drag_script(from_x: i32, from_y: i32, to_x: i32, to_y: i32) -> String {
    format!(
        r#"{preamble}
[MouseOps]::SetCursorPos({from_x}, {from_y}) | Out-Null
[MouseOps]::mouse_event(0x0002, 0, 0, 0, [UIntPtr]::Zero)
Start-Sleep -Milliseconds 50
[MouseOps]::SetCursorPos({to_x}, {to_y}) | Out-Null
Start-Sleep -Milliseconds 50
[MouseOps]::mouse_event(0x0004, 0, 0, 0, [UIntPtr]::Zero)
"#,
        preamble = windows_mouse_preamble(),
        from_x = from_x,
        from_y = from_y,
        to_x = to_x,
        to_y = to_y
    )
}

#[cfg(target_os = "windows")]
fn windows_scroll_script(delta: i32) -> String {
    format!(
        r#"{preamble}
[MouseOps]::mouse_event(0x0800, 0, 0, {delta}, [UIntPtr]::Zero)
"#,
        preamble = windows_mouse_preamble(),
        delta = delta
    )
}

#[cfg(target_os = "macos")]
fn apple_script_escape(text: &str) -> String {
    text.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(target_os = "macos")]
fn macos_hotkey_script(chord: &str) -> anyhow::Result<String> {
    let mut modifiers = Vec::new();
    let mut key = None;
    for part in chord.split('+').map(|s| s.trim()).filter(|s| !s.is_empty()) {
        match part.to_ascii_lowercase().as_str() {
            "cmd" | "command" | "meta" => modifiers.push("command down"),
            "shift" => modifiers.push("shift down"),
            "ctrl" | "control" => modifiers.push("control down"),
            "alt" | "option" => modifiers.push("option down"),
            other => key = Some(other.to_string()),
        }
    }
    let key = key.context("hotkey missing terminal key")?;
    Ok(format!(
        r#"tell application "System Events" to keystroke "{}" using {{{}}}"#,
        apple_script_escape(&key),
        modifiers.join(", ")
    ))
}

#[cfg(target_os = "macos")]
fn macos_pointer_state_script() -> &'static str {
    r#"
import Foundation
import ApplicationServices

guard let event = CGEvent(source: nil) else {
    FileHandle.standardError.write(Data("unable to read pointer location\n".utf8))
    exit(1)
}

let payload: [String: Any] = [
    "x": Int(event.location.x.rounded()),
    "y": Int(event.location.y.rounded())
]
let data = try JSONSerialization.data(withJSONObject: payload, options: [])
FileHandle.standardOutput.write(data)
"#
}

#[cfg(target_os = "macos")]
fn macos_focused_control_signal_script() -> &'static str {
    r#"
import Foundation
import ApplicationServices

func stringValue(from value: CFTypeRef?) -> String? {
    guard let value else { return nil }
    if CFGetTypeID(value) == CFStringGetTypeID() {
        return value as? String
    }
    if CFGetTypeID(value) == CFNumberGetTypeID() {
        return (value as? NSNumber)?.stringValue
    }
    return nil
}

func attribute(_ element: AXUIElement, _ name: CFString) -> CFTypeRef? {
    var value: CFTypeRef?
    let result = AXUIElementCopyAttributeValue(element, name, &value)
    guard result == .success else { return nil }
    return value
}

let system = AXUIElementCreateSystemWide()
var focusedRaw: CFTypeRef?
var payload: [String: Any] = [
    "name": NSNull(),
    "value": NSNull()
]

if AXUIElementCopyAttributeValue(system, kAXFocusedUIElementAttribute as CFString, &focusedRaw) == .success,
   let focusedRaw,
   CFGetTypeID(focusedRaw) == AXUIElementGetTypeID() {
    let focusedElement = unsafeBitCast(focusedRaw, to: AXUIElement.self)
    let name =
        stringValue(from: attribute(focusedElement, kAXTitleAttribute as CFString)) ??
        stringValue(from: attribute(focusedElement, kAXDescriptionAttribute as CFString)) ??
        stringValue(from: attribute(focusedElement, kAXRoleDescriptionAttribute as CFString)) ??
        stringValue(from: attribute(focusedElement, kAXRoleAttribute as CFString))
    let value =
        stringValue(from: attribute(focusedElement, kAXValueAttribute as CFString)) ??
        stringValue(from: attribute(focusedElement, kAXHelpAttribute as CFString))
    payload["name"] = name ?? NSNull()
    payload["value"] = value ?? NSNull()
}

let data = try JSONSerialization.data(withJSONObject: payload, options: [])
FileHandle.standardOutput.write(data)
"#
}

#[cfg(target_os = "macos")]
fn macos_mouse_button_spec(
    button: &MouseButton,
) -> (&'static str, &'static str, &'static str, &'static str) {
    match button {
        MouseButton::Left => (
            ".left",
            ".leftMouseDown",
            ".leftMouseUp",
            ".leftMouseDragged",
        ),
        MouseButton::Right => (
            ".right",
            ".rightMouseDown",
            ".rightMouseUp",
            ".rightMouseDragged",
        ),
        MouseButton::Middle => (
            ".center",
            ".otherMouseDown",
            ".otherMouseUp",
            ".otherMouseDragged",
        ),
    }
}

#[cfg(target_os = "macos")]
fn macos_click_script(x: i32, y: i32, button: &MouseButton, double: bool) -> String {
    let (mouse_button, down_type, up_type, _drag_type) = macos_mouse_button_spec(button);
    let repeat = if double { 2 } else { 1 };
    format!(
        r#"
import Foundation
import ApplicationServices

func fail(_ message: String) -> Never {{
    FileHandle.standardError.write(Data((message + "\n").utf8))
    exit(1)
}}

guard let source = CGEventSource(stateID: .hidSystemState) else {{
    fail("unable to create event source")
}}

let point = CGPoint(x: Double({x}), y: Double({y}))
let warpResult = CGWarpMouseCursorPosition(point)
guard warpResult == .success else {{
    fail("failed to move pointer to target")
}}
Thread.sleep(forTimeInterval: 0.02)

func post(_ type: CGEventType, clickState: Int64) {{
    guard let event = CGEvent(
        mouseEventSource: source,
        mouseType: type,
        mouseCursorPosition: point,
        mouseButton: {mouse_button}
    ) else {{
        fail("failed to create mouse event")
    }}
    event.setIntegerValueField(.mouseEventClickState, value: clickState)
    event.post(tap: .cghidEventTap)
}}

for step in 1...{repeat} {{
    post({down_type}, clickState: Int64(step))
    Thread.sleep(forTimeInterval: 0.03)
    post({up_type}, clickState: Int64(step))
    Thread.sleep(forTimeInterval: 0.06)
}}

let payload: [String: Any] = [
    "ok": true,
    "x": Int(point.x.rounded()),
    "y": Int(point.y.rounded()),
    "repeat": {repeat}
]
let data = try JSONSerialization.data(withJSONObject: payload, options: [])
FileHandle.standardOutput.write(data)
"#,
        x = x,
        y = y,
        repeat = repeat,
        mouse_button = mouse_button,
        down_type = down_type,
        up_type = up_type,
    )
}

#[cfg(target_os = "macos")]
fn macos_drag_script(from_x: i32, from_y: i32, to_x: i32, to_y: i32) -> String {
    let (mouse_button, down_type, up_type, drag_type) = macos_mouse_button_spec(&MouseButton::Left);
    format!(
        r#"
import Foundation
import ApplicationServices

func fail(_ message: String) -> Never {{
    FileHandle.standardError.write(Data((message + "\n").utf8))
    exit(1)
}}

guard let source = CGEventSource(stateID: .hidSystemState) else {{
    fail("unable to create event source")
}}

let fromPoint = CGPoint(x: Double({from_x}), y: Double({from_y}))
let toPoint = CGPoint(x: Double({to_x}), y: Double({to_y}))
guard CGWarpMouseCursorPosition(fromPoint) == .success else {{
    fail("failed to move pointer to drag origin")
}}
Thread.sleep(forTimeInterval: 0.02)

func post(_ type: CGEventType, point: CGPoint) {{
    guard let event = CGEvent(
        mouseEventSource: source,
        mouseType: type,
        mouseCursorPosition: point,
        mouseButton: {mouse_button}
    ) else {{
        fail("failed to create drag event")
    }}
    event.post(tap: .cghidEventTap)
}}

post({down_type}, point: fromPoint)
Thread.sleep(forTimeInterval: 0.04)
for step in 1...12 {{
    let progress = Double(step) / 12.0
    let point = CGPoint(
        x: fromPoint.x + ((toPoint.x - fromPoint.x) * progress),
        y: fromPoint.y + ((toPoint.y - fromPoint.y) * progress)
    )
    post({drag_type}, point: point)
    Thread.sleep(forTimeInterval: 0.01)
}}
post({up_type}, point: toPoint)

let payload: [String: Any] = [
    "ok": true,
    "to_x": Int(toPoint.x.rounded()),
    "to_y": Int(toPoint.y.rounded())
]
let data = try JSONSerialization.data(withJSONObject: payload, options: [])
FileHandle.standardOutput.write(data)
"#,
        from_x = from_x,
        from_y = from_y,
        to_x = to_x,
        to_y = to_y,
        mouse_button = mouse_button,
        down_type = down_type,
        drag_type = drag_type,
        up_type = up_type,
    )
}

#[cfg(target_os = "macos")]
fn macos_scroll_script(delta: i32) -> String {
    format!(
        r#"
import Foundation
import ApplicationServices

func fail(_ message: String) -> Never {{
    FileHandle.standardError.write(Data((message + "\n").utf8))
    exit(1)
}}

guard let source = CGEventSource(stateID: .hidSystemState) else {{
    fail("unable to create event source")
}}

guard let event = CGEvent(
    scrollWheelEvent2Source: source,
    units: .line,
    wheelCount: 1,
    wheel1: Int32({delta}),
    wheel2: 0,
    wheel3: 0
) else {{
    fail("failed to create scroll event")
}}

event.post(tap: .cghidEventTap)

let payload: [String: Any] = [
    "ok": true,
    "delta": {delta}
]
let data = try JSONSerialization.data(withJSONObject: payload, options: [])
FileHandle.standardOutput.write(data)
"#,
        delta = delta
    )
}

#[cfg(all(test, target_os = "windows"))]
mod tests {
    use super::*;

    fn sample_focused_control_signal(name: Option<&str>, value: Option<&str>) -> StructuredSignal {
        StructuredSignal {
            key: "focused_control_state".into(),
            payload: json!({
                "name": name,
                "value_present": value.is_some(),
                "value_fingerprint": value.map(fingerprint_text),
                "value_length": value.map(|text| text.chars().count()),
            }),
        }
    }

    fn sample_focus_state(
        active_window: Option<&str>,
        window_titles: &[&str],
    ) -> WindowsFocusState {
        WindowsFocusState {
            active_window: active_window.map(|value| value.to_string()),
            window_titles: window_titles
                .iter()
                .map(|value| value.to_string())
                .collect(),
        }
    }

    fn sample_launch_state(processes: &[(&str, u32, Option<u32>)]) -> WindowsLaunchState {
        WindowsLaunchState {
            processes: processes
                .iter()
                .map(|(name, pid, parent_process_id)| WindowsProcessState {
                    pid: *pid,
                    name: (*name).to_string(),
                    parent_process_id: *parent_process_id,
                })
                .collect(),
        }
    }

    #[test]
    fn focus_tracker_requires_stable_consecutive_matches() {
        let mut tracker = FocusVerificationTracker::new("Codex");

        let first = tracker.record(
            &sample_focus_state(Some("Codex"), &["Codex", "Browser"]),
            Duration::from_millis(100),
            false,
        );
        assert!(first.matched);
        assert!(!first.stable);
        assert_eq!(first.stable_samples, 1);

        let second = tracker.record(
            &sample_focus_state(Some("Codex"), &["Codex", "Browser"]),
            Duration::from_millis(200),
            false,
        );
        assert!(second.matched);
        assert!(second.stable);
        assert_eq!(second.attempts, 2);
        assert!(!second.timed_out);
    }

    #[test]
    fn focus_tracker_marks_timeout_when_stability_never_holds() {
        let mut tracker = FocusVerificationTracker::new("Codex");

        let first = tracker.record(
            &sample_focus_state(Some("Other App"), &["Codex", "Other App"]),
            Duration::from_millis(100),
            false,
        );
        assert!(!first.matched);
        assert!(first.window_present);

        let second = tracker.record(
            &sample_focus_state(Some("Other App"), &["Codex", "Other App"]),
            WINDOWS_FOCUS_VERIFICATION_TIMEOUT,
            true,
        );
        assert!(!second.matched);
        assert!(!second.stable);
        assert!(second.timed_out);
        assert_eq!(second.attempts, 2);
    }

    #[test]
    fn focus_tracker_matches_case_insensitively() {
        let mut tracker = FocusVerificationTracker::new("tooling-verification");

        let status = tracker.record(
            &sample_focus_state(Some("Tooling-Verification - Codex"), &["Browser"]),
            Duration::from_millis(100),
            false,
        );
        assert!(status.matched);
        assert_eq!(
            status.observed.as_deref(),
            Some("Tooling-Verification - Codex")
        );
    }

    #[test]
    fn launch_tracker_requires_new_process_delta_from_executable_stem() {
        let before = WindowsLaunchAnchorState {
            baseline_process_ids: HashSet::from([111]),
            spawned_pid: Some(222),
        };
        let mut tracker =
            LaunchVerificationTracker::new("C:\\Program Files\\Codex\\Codex.exe", Some(&before));

        let status = tracker.record(
            &sample_launch_state(&[
                ("Explorer", 100, None),
                ("Codex", 111, None),
                ("Codex", 222, None),
            ]),
            Duration::from_millis(150),
            false,
            WINDOWS_POST_ACTION_VERIFICATION_POLL,
        );

        assert!(status.spawned_pid_present);
        assert!(status.new_process_detected);
        assert_eq!(status.observed_process.as_deref(), Some("Codex"));
        assert_eq!(status.observed_process_id, Some(222));
        assert_eq!(status.expected_process, "Codex");
        assert_eq!(status.proof_level, "spawned_pid_still_present");
    }

    #[test]
    fn launch_tracker_rejects_preexisting_process_without_delta() {
        let before = WindowsLaunchAnchorState {
            baseline_process_ids: HashSet::from([700]),
            spawned_pid: None,
        };
        let mut tracker = LaunchVerificationTracker::new("code", Some(&before));

        let status = tracker.record(
            &sample_launch_state(&[("Code.exe", 700, None), ("Browser.exe", 701, None)]),
            WINDOWS_POST_ACTION_VERIFICATION_TIMEOUT,
            true,
            WINDOWS_POST_ACTION_VERIFICATION_POLL,
        );

        assert!(!status.spawned_pid_present);
        assert!(!status.new_process_detected);
        assert_eq!(status.observed_process_id, None);
        assert_eq!(status.proof_level, "preexisting_process_only");
        assert!(status.timed_out);
        assert_eq!(status.attempts, 1);
    }

    #[test]
    fn launch_tracker_marks_timeout_when_process_never_appears() {
        let before = WindowsLaunchAnchorState {
            baseline_process_ids: HashSet::from([11, 12]),
            spawned_pid: None,
        };
        let mut tracker = LaunchVerificationTracker::new("code", Some(&before));

        let status = tracker.record(
            &sample_launch_state(&[("Explorer", 11, None), ("Browser", 12, None)]),
            WINDOWS_POST_ACTION_VERIFICATION_TIMEOUT,
            true,
            WINDOWS_POST_ACTION_VERIFICATION_POLL,
        );

        assert!(!status.spawned_pid_present);
        assert!(!status.new_process_detected);
        assert!(status.timed_out);
        assert_eq!(status.attempts, 1);
        assert_eq!(status.proof_level, "none");
    }

    #[test]
    fn launch_tracker_accepts_spawned_child_lineage_when_launcher_exits() {
        let before = WindowsLaunchAnchorState {
            baseline_process_ids: HashSet::from([111]),
            spawned_pid: Some(222),
        };
        let mut tracker = LaunchVerificationTracker::new("launcher.exe", Some(&before));

        let status = tracker.record(
            &sample_launch_state(&[("Explorer", 111, None), ("FinalApp.exe", 333, Some(222))]),
            Duration::from_millis(200),
            false,
            WINDOWS_POST_ACTION_VERIFICATION_POLL,
        );

        assert!(!status.spawned_pid_present);
        assert!(status.spawned_child_detected);
        assert!(!status.new_process_detected);
        assert_eq!(status.observed_process.as_deref(), Some("FinalApp.exe"));
        assert_eq!(status.observed_process_id, Some(333));
        assert_eq!(status.proof_level, "spawned_child_detected");
    }

    #[test]
    fn wait_for_verification_status_reads_successful_wait_result() {
        let execution = ActionExecution {
            id: Uuid::new_v4(),
            action: ProposedAction::WaitFor {
                signal: "window:Browser".into(),
                timeout_ms: 1_000,
            },
            backend: "mock".into(),
            summary: "wait condition satisfied".into(),
            evidence: Some(json!({
                "wait_for_result": {
                    "signal": "window:Browser",
                    "matched": true,
                    "attempts": 3,
                    "elapsed_ms": 450,
                    "timed_out": false,
                }
            })),
            recorded_at: Utc::now(),
        };

        let status = wait_for_verification_status_from_execution(&execution).unwrap();
        assert_eq!(status.signal, "window:Browser");
        assert!(status.matched);
        assert_eq!(status.attempts, 3);
        assert_eq!(status.elapsed_ms, 450);
        assert!(!status.timed_out);
        assert_eq!(status.proof_level, "wait_signal_match");
    }

    #[test]
    fn wait_for_verification_status_reads_timed_out_wait_result() {
        let execution = ActionExecution {
            id: Uuid::new_v4(),
            action: ProposedAction::WaitFor {
                signal: "clipboard:done".into(),
                timeout_ms: 1_000,
            },
            backend: "mock".into(),
            summary: "wait condition timed out".into(),
            evidence: Some(json!({
                "wait_for_result": {
                    "signal": "clipboard:done",
                    "matched": false,
                    "attempts": 4,
                    "elapsed_ms": 1_000,
                    "timed_out": true,
                }
            })),
            recorded_at: Utc::now(),
        };

        let status = wait_for_verification_status_from_execution(&execution).unwrap();
        assert_eq!(status.signal, "clipboard:done");
        assert!(!status.matched);
        assert!(status.timed_out);
        assert_eq!(status.proof_level, "wait_signal_timeout");
    }

    #[test]
    fn capture_observation_verification_status_rejects_missing_screenshot() {
        let frame = ObservationFrame {
            captured_at: Utc::now(),
            summary: "capture".into(),
            screenshot_path: None,
            ocr_text: None,
            active_window: None,
            window_titles: Vec::new(),
            clipboard_text: None,
            structured_signals: Vec::new(),
        };

        let status = capture_observation_verification_status(&frame);
        assert!(!status.ok);
        assert!(!status.screenshot_present);
        assert!(!status.screenshot_readable);
        assert_eq!(status.proof_level, "missing_screenshot");
    }

    #[test]
    fn capture_observation_verification_status_accepts_readable_screenshot() {
        let screenshot_path = std::env::temp_dir()
            .join("splcw-host")
            .join(format!("capture-{}.png", Uuid::new_v4()));
        std::fs::create_dir_all(screenshot_path.parent().unwrap()).unwrap();
        std::fs::write(&screenshot_path, b"png-bytes").unwrap();

        let frame = ObservationFrame {
            captured_at: Utc::now(),
            summary: "capture".into(),
            screenshot_path: Some(screenshot_path.to_string_lossy().to_string()),
            ocr_text: None,
            active_window: None,
            window_titles: Vec::new(),
            clipboard_text: None,
            structured_signals: Vec::new(),
        };

        let status = capture_observation_verification_status(&frame);
        assert!(status.ok);
        assert!(status.screenshot_present);
        assert!(status.screenshot_readable);
        assert_eq!(status.proof_level, "screenshot_readable");

        let _ = std::fs::remove_file(screenshot_path);
    }

    #[test]
    fn pointer_verification_uses_small_tolerance() {
        let status = pointer_verification_status(100, 200, &WindowsPointerState { x: 103, y: 197 });
        assert!(status.within_tolerance);

        let failed = pointer_verification_status(100, 200, &WindowsPointerState { x: 110, y: 197 });
        assert!(!failed.within_tolerance);
    }

    #[test]
    fn keyboard_verification_reports_focus_anchor_without_observable_delta() {
        let before = WindowsKeyboardAnchorState {
            active_window: Some("Codex".into()),
            clipboard_present: true,
            clipboard_fingerprint: Some(fingerprint_text("clipboard")),
            focused_control_name: Some("Composer".into()),
            focused_control_value_fingerprint: Some(fingerprint_text("hello")),
        };
        let frame = ObservationFrame {
            captured_at: Utc::now(),
            summary: "post keyboard observation".into(),
            screenshot_path: None,
            ocr_text: None,
            active_window: Some("Codex".into()),
            window_titles: vec!["Codex".into()],
            clipboard_text: Some("clipboard".into()),
            structured_signals: vec![sample_focused_control_signal(
                Some("Composer"),
                Some("hello"),
            )],
        };

        let status = keyboard_verification_status(&before, &frame, None, None, false, false, false);

        assert!(status.ok);
        assert!(status.focus_preserved);
        assert!(!status.window_changed);
        assert!(!status.clipboard_changed);
        assert_eq!(status.proof_level, "focus_anchor");
    }

    #[test]
    fn keyboard_verification_reports_observable_delta_when_window_changes() {
        let before = WindowsKeyboardAnchorState {
            active_window: Some("Codex".into()),
            clipboard_present: false,
            clipboard_fingerprint: None,
            focused_control_name: Some("Composer".into()),
            focused_control_value_fingerprint: Some(fingerprint_text("hello")),
        };
        let frame = ObservationFrame {
            captured_at: Utc::now(),
            summary: "post hotkey observation".into(),
            screenshot_path: None,
            ocr_text: None,
            active_window: Some("Run".into()),
            window_titles: vec!["Run".into()],
            clipboard_text: None,
            structured_signals: vec![sample_focused_control_signal(Some("RunInput"), Some(""))],
        };

        let status = keyboard_verification_status(&before, &frame, None, None, false, false, false);

        assert!(status.ok);
        assert!(!status.focus_preserved);
        assert!(status.window_changed);
        assert_eq!(status.proof_level, "observable_delta");
    }

    #[test]
    fn keyboard_verification_reports_focused_control_text_match() {
        let before = WindowsKeyboardAnchorState {
            active_window: Some("Codex".into()),
            clipboard_present: false,
            clipboard_fingerprint: None,
            focused_control_name: Some("Composer".into()),
            focused_control_value_fingerprint: Some(fingerprint_text("hel")),
        };
        let focused_control = WindowsFocusedControlState {
            name: Some("Composer".into()),
            value: Some("hello".into()),
        };
        let frame = ObservationFrame {
            captured_at: Utc::now(),
            summary: "post typing observation".into(),
            screenshot_path: None,
            ocr_text: None,
            active_window: Some("Codex".into()),
            window_titles: vec!["Codex".into()],
            clipboard_text: None,
            structured_signals: vec![sample_focused_control_signal(
                Some("Composer"),
                Some("hello"),
            )],
        };

        let status = keyboard_verification_status(
            &before,
            &frame,
            Some(&focused_control),
            Some("lo"),
            false,
            false,
            false,
        );

        assert!(status.ok);
        assert!(status.focus_preserved);
        assert!(status.focused_control_value_changed);
        assert!(status.requested_text_observed);
        assert_eq!(status.proof_level, "focused_control_text_match");
    }

    #[test]
    fn type_text_verification_does_not_accept_focus_anchor_without_effect() {
        let before = WindowsKeyboardAnchorState {
            active_window: Some("Codex".into()),
            clipboard_present: true,
            clipboard_fingerprint: Some(fingerprint_text("clipboard")),
            focused_control_name: Some("Composer".into()),
            focused_control_value_fingerprint: Some(fingerprint_text("hello")),
        };
        let frame = ObservationFrame {
            captured_at: Utc::now(),
            summary: "post typing observation".into(),
            screenshot_path: None,
            ocr_text: None,
            active_window: Some("Codex".into()),
            window_titles: vec!["Codex".into()],
            clipboard_text: Some("clipboard".into()),
            structured_signals: vec![sample_focused_control_signal(
                Some("Composer"),
                Some("hello"),
            )],
        };

        let status =
            keyboard_verification_status(&before, &frame, None, Some("hello"), true, true, false);

        assert!(!status.ok);
        assert!(status.focus_preserved);
        assert!(!status.requested_text_observed);
        assert_eq!(status.proof_level, "focus_anchor");
    }

    #[test]
    fn type_text_verification_does_not_accept_control_shift_without_input_delta() {
        let before = WindowsKeyboardAnchorState {
            active_window: Some("Codex".into()),
            clipboard_present: false,
            clipboard_fingerprint: None,
            focused_control_name: Some("Composer".into()),
            focused_control_value_fingerprint: Some(fingerprint_text("hello")),
        };
        let frame = ObservationFrame {
            captured_at: Utc::now(),
            summary: "post typing observation".into(),
            screenshot_path: None,
            ocr_text: None,
            active_window: Some("Codex".into()),
            window_titles: vec!["Codex".into()],
            clipboard_text: None,
            structured_signals: vec![sample_focused_control_signal(
                Some("Sidebar Search"),
                Some("hello"),
            )],
        };

        let status =
            keyboard_verification_status(&before, &frame, None, Some("hello"), true, true, false);

        assert!(!status.ok);
        assert!(status.focus_preserved);
        assert!(status.focused_control_changed);
        assert_eq!(status.proof_level, "focused_control_focus_shift");
    }

    #[test]
    fn hotkey_verification_does_not_accept_focus_anchor_without_effect() {
        let before = WindowsKeyboardAnchorState {
            active_window: Some("Codex".into()),
            clipboard_present: true,
            clipboard_fingerprint: Some(fingerprint_text("clipboard")),
            focused_control_name: Some("Composer".into()),
            focused_control_value_fingerprint: Some(fingerprint_text("hello")),
        };
        let frame = ObservationFrame {
            captured_at: Utc::now(),
            summary: "post hotkey observation".into(),
            screenshot_path: None,
            ocr_text: None,
            active_window: Some("Codex".into()),
            window_titles: vec!["Codex".into()],
            clipboard_text: Some("clipboard".into()),
            structured_signals: vec![sample_focused_control_signal(
                Some("Composer"),
                Some("hello"),
            )],
        };

        let status = keyboard_verification_status(&before, &frame, None, None, false, true, true);

        assert!(!status.ok);
        assert!(status.focus_preserved);
        assert_eq!(status.proof_level, "focus_anchor");
    }

    #[test]
    fn hotkey_verification_rejects_focused_control_shift_without_observable_delta() {
        let before = WindowsKeyboardAnchorState {
            active_window: Some("Codex".into()),
            clipboard_present: false,
            clipboard_fingerprint: None,
            focused_control_name: Some("Composer".into()),
            focused_control_value_fingerprint: Some(fingerprint_text("hello")),
        };
        let frame = ObservationFrame {
            captured_at: Utc::now(),
            summary: "post hotkey observation".into(),
            screenshot_path: None,
            ocr_text: None,
            active_window: Some("Codex".into()),
            window_titles: vec!["Codex".into()],
            clipboard_text: None,
            structured_signals: vec![sample_focused_control_signal(
                Some("Sidebar Search"),
                Some("hello"),
            )],
        };

        let status = keyboard_verification_status(&before, &frame, None, None, false, true, false);

        assert!(!status.ok);
        assert!(status.focus_preserved);
        assert!(status.focused_control_changed);
        assert_eq!(status.proof_level, "focused_control_focus_shift");
    }

    #[test]
    fn scroll_verification_rejects_missing_viewport_or_focus_effect() {
        let before = WindowsScrollAnchorState {
            active_window: Some("Codex".into()),
            focused_control_name: Some("Editor".into()),
            screenshot_fingerprint: Some(11),
        };
        let frame = ObservationFrame {
            captured_at: Utc::now(),
            summary: "post scroll observation".into(),
            screenshot_path: None,
            ocr_text: None,
            active_window: Some("Codex".into()),
            window_titles: vec!["Codex".into()],
            clipboard_text: None,
            structured_signals: vec![sample_focused_control_signal(Some("Editor"), Some("same"))],
        };

        let status = scroll_verification_status(&before, &frame, Some(11), None);

        assert!(!status.ok);
        assert!(!status.window_changed);
        assert!(!status.focused_control_changed);
        assert!(!status.screenshot_changed);
        assert_eq!(status.proof_level, "none");
    }

    #[test]
    fn scroll_verification_accepts_viewport_delta() {
        let before = WindowsScrollAnchorState {
            active_window: Some("Codex".into()),
            focused_control_name: Some("Editor".into()),
            screenshot_fingerprint: Some(11),
        };
        let frame = ObservationFrame {
            captured_at: Utc::now(),
            summary: "post scroll observation".into(),
            screenshot_path: None,
            ocr_text: None,
            active_window: Some("Codex".into()),
            window_titles: vec!["Codex".into()],
            clipboard_text: None,
            structured_signals: vec![sample_focused_control_signal(Some("Editor"), Some("same"))],
        };

        let status = scroll_verification_status(&before, &frame, Some(22), None);

        assert!(status.ok);
        assert!(status.screenshot_changed);
        assert_eq!(status.proof_level, "viewport_delta");
    }
}
