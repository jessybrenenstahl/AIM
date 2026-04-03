use std::collections::HashMap;
use std::env;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
#[cfg(target_os = "windows")]
use std::os::windows::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow};
use chrono::{DateTime, Utc};
use gpui::{App, AppContext, Application, Bounds, WindowBounds, WindowOptions, px, size};
use gpui_component::Root;
use serde::{Deserialize, Serialize};
use splcw_core::{Invariant, PlanModule, PlanSnapshot, SufficientPlan};
use splcw_host::EmbeddedHostBody;
use splcw_llm::{
    AuthProfile, AuthProfileStore, ChatResponse, ConfiguredLlmClient, ContentBlock,
    FileAuthProfileStore, OAuthAuthorizationKind, OAuthInitiationMode, PendingOAuthAuthorization,
    ProviderKind, ProviderRegistry, RuntimeAuthReadiness, builtin_interactive_oauth_client_id,
    inspect_runtime_auth, register_openai_responses_providers,
};
use splcw_memory::{FilesystemOffloadSink, SqliteStateStore};
use splcw_orchestrator::{
    OrchestratorState, PersistentOrchestrator, RuntimePendingTurnCheckpoint, RuntimeSessionEvent,
    RuntimeSessionState, RuntimeTurnOptions, RuntimeTurnRecord, SupervisedGithubActionKind,
    SupervisedGithubActionRequest,
};
use sysinfo::{Pid, ProcessesToUpdate, System};
use tokio::runtime::Builder;
use uuid::Uuid;

mod shell;

use shell::{OperatorShell, init as init_operator_shell};

const DEFAULT_MODEL: &str = "gpt-5.4";
const DEFAULT_OBJECTIVE: &str =
    "Safely operate the current desktop and make one verifiable step toward the active goal.";
const DEFAULT_SESSION_ID: &str = "operator-main";
const DEFAULT_THREAD_ID: &str = "main";
const DEFAULT_THREAD_LABEL: &str = "Main";
const DEFAULT_LOOP_PAUSE_SECONDS: f32 = 2.0;
const REFRESH_INTERVAL: Duration = Duration::from_millis(1500);
const RECENT_TURN_LIMIT: usize = 5;
const RECENT_EVENT_LIMIT: usize = 8;
const RECENT_LIVE_STREAM_LINE_LIMIT: usize = 12;
const RECENT_GITHUB_ACTION_LIMIT: usize = 5;
const BACKGROUND_RUNNER_STALE_AFTER: Duration = Duration::from_secs(120);
const BACKGROUND_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(250);
const SUPPORTED_OPERATOR_ENV_NAMES: &[&str] = &[
    "SPLCW_OPENAI_CODEX_OAUTH_CLIENT_ID",
    "OPENAI_CODEX_OAUTH_CLIENT_ID",
    "SPLCW_OPENAI_API_OAUTH_CLIENT_ID",
    "OPENAI_API_OAUTH_CLIENT_ID",
    "SPLCW_OPENAI_OAUTH_CLIENT_ID",
    "OPENAI_OAUTH_CLIENT_ID",
    "CODEX_BIN",
    "OPENCLAW_BIN",
    "OPENCLAW_STATE_DIR",
    "OPENCLAW_AGENT_DIR",
    "PI_CODING_AGENT_DIR",
];

#[derive(Clone)]
struct OperatorPaths {
    repo_root: PathBuf,
    harness_root: PathBuf,
    operator_root: PathBuf,
    operator_env_path: PathBuf,
    status_path: PathBuf,
    github_action_request_path: PathBuf,
    github_action_history_path: PathBuf,
    background_runner_path: PathBuf,
    background_stop_path: PathBuf,
    background_handoff_path: PathBuf,
    codex_cli_session_path: PathBuf,
    codex_cli_live_stream_path: PathBuf,
    session_root: PathBuf,
    session_id: String,
    state_db_path: PathBuf,
    auth_store_path: PathBuf,
}

impl OperatorPaths {
    fn discover() -> anyhow::Result<Self> {
        let repo_root = discover_repo_root(
            std::env::current_dir().ok().as_deref(),
            std::env::current_exe().ok().as_deref(),
        )
        .context("locate AIM repo root from current directory or executable path")?;
        let harness_root = repo_root.join("ultimentality-pilot").join("harness");
        let operator_root = repo_root
            .join("artifacts")
            .join("ultimentality-pilot")
            .join("operator");
        Ok(Self {
            repo_root,
            harness_root,
            operator_env_path: operator_root.join("operator.env"),
            session_root: operator_root.join("sessions"),
            session_id: DEFAULT_SESSION_ID.into(),
            state_db_path: operator_root.join("state.sqlite"),
            auth_store_path: operator_root.join("auth-profiles.json"),
            status_path: operator_root.join("status.json"),
            github_action_request_path: operator_root.join("github-action-request.json"),
            github_action_history_path: operator_root.join("github-action-history.jsonl"),
            background_runner_path: operator_root.join("background-runner.json"),
            background_stop_path: operator_root.join("background-stop.request"),
            background_handoff_path: operator_root.join("background-handoff.json"),
            codex_cli_session_path: operator_root.join("codex-cli-session.json"),
            codex_cli_live_stream_path: operator_root.join("codex-cli-live-stream.json"),
            operator_root,
        })
    }
}

fn discover_repo_root(current_dir: Option<&Path>, current_exe: Option<&Path>) -> Option<PathBuf> {
    current_exe
        .and_then(|path| path.parent().and_then(find_repo_root_from))
        .or_else(|| current_dir.and_then(find_repo_root_from))
}

fn find_repo_root_from(start: &Path) -> Option<PathBuf> {
    for candidate in start.ancestors() {
        let harness_root = candidate.join("ultimentality-pilot").join("harness");
        if harness_root.join("Cargo.toml").exists() {
            return Some(candidate.to_path_buf());
        }
    }
    None
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RunSettings {
    objective: String,
    model: String,
    thread_id: String,
    thread_label: String,
    #[serde(default = "default_native_engine_mode")]
    engine_mode: OperatorEngineMode,
}

impl Default for RunSettings {
    fn default() -> Self {
        Self {
            objective: DEFAULT_OBJECTIVE.into(),
            model: DEFAULT_MODEL.into(),
            thread_id: DEFAULT_THREAD_ID.into(),
            thread_label: DEFAULT_THREAD_LABEL.into(),
            engine_mode: OperatorEngineMode::CodexCli,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OperatorEngineMode {
    CodexCli,
    NativeHarness,
}

impl OperatorEngineMode {
    fn as_label(self) -> &'static str {
        match self {
            Self::CodexCli => "Codex CLI",
            Self::NativeHarness => "Native Harness",
        }
    }
}

fn default_native_engine_mode() -> OperatorEngineMode {
    OperatorEngineMode::NativeHarness
}

#[derive(Debug, Clone, Default)]
struct OperatorCommand {
    smoke_test: bool,
    run_turn: bool,
    background_loop: bool,
    settings: RunSettings,
    loop_pause_seconds: f32,
    background_runner_id: Option<String>,
    background_owner_shell_id: Option<String>,
    background_owner_shell_pid: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OperatorRunMode {
    Idle,
    SingleTurn,
    Continuous,
}

impl OperatorRunMode {
    fn as_label(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::SingleTurn => "single_turn",
            Self::Continuous => "continuous",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperatorTurnTerminal {
    Completed,
    SurfacedGap,
    GithubActionRequested,
}

#[derive(Debug, Clone)]
struct OperatorTurnResult {
    terminal: OperatorTurnTerminal,
    summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OperatorGithubActionRequestRecord {
    requested_at: DateTime<Utc>,
    thread_id: String,
    thread_label: String,
    request: SupervisedGithubActionRequest,
    summary: String,
    narrative: Option<String>,
    #[serde(default)]
    target_suggestions: Vec<OperatorGithubTargetSuggestion>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct OperatorGithubTargetSuggestion {
    number: u64,
    title: String,
    url: Option<String>,
    source: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OperatorGithubActionLifecycleState {
    Queued,
    Applied,
    Cleared,
    Rejected,
}

impl OperatorGithubActionLifecycleState {
    fn as_label(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Applied => "applied",
            Self::Cleared => "cleared",
            Self::Rejected => "rejected",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OperatorGithubActionLifecycleRecord {
    recorded_at: DateTime<Utc>,
    state: OperatorGithubActionLifecycleState,
    thread_id: String,
    thread_label: String,
    request: SupervisedGithubActionRequest,
    summary: String,
    detail: Option<String>,
    result_excerpt: Option<String>,
    result_url: Option<String>,
}

#[derive(Debug, Clone)]
struct BackgroundLoopExitState {
    run_state: String,
    run_mode: OperatorRunMode,
    summary: String,
    last_error: Option<String>,
    completed_turn_count: u64,
}

#[derive(Debug, Clone)]
struct BackgroundSpawnResult {
    pid: u32,
    runner_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperatorAuthProvider {
    OpenAiCodex,
    OpenAiApi,
}

impl OperatorAuthProvider {
    fn as_provider_kind(self) -> ProviderKind {
        match self {
            Self::OpenAiCodex => ProviderKind::OpenAiCodex,
            Self::OpenAiApi => ProviderKind::OpenAiApi,
        }
    }

    fn as_label(self) -> &'static str {
        match self {
            Self::OpenAiCodex => "OpenAI Codex",
            Self::OpenAiApi => "OpenAI API",
        }
    }

    fn oauth_client_id_env_names(self) -> &'static [&'static str] {
        match self {
            Self::OpenAiCodex => &[
                "SPLCW_OPENAI_CODEX_OAUTH_CLIENT_ID",
                "OPENAI_CODEX_OAUTH_CLIENT_ID",
                "SPLCW_OPENAI_OAUTH_CLIENT_ID",
                "OPENAI_OAUTH_CLIENT_ID",
            ],
            Self::OpenAiApi => &[
                "SPLCW_OPENAI_API_OAUTH_CLIENT_ID",
                "OPENAI_API_OAUTH_CLIENT_ID",
                "SPLCW_OPENAI_OAUTH_CLIENT_ID",
                "OPENAI_OAUTH_CLIENT_ID",
            ],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InteractiveOAuthLaunchStatus {
    ready: bool,
    summary: String,
    env_name: Option<&'static str>,
    built_in: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OperatorEnvConfigStatus {
    configured: bool,
    summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OpenClawImportStatus {
    available: bool,
    summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OpenClawCliStatus {
    available: bool,
    summary: String,
    command_path: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CodexCliStatus {
    available: bool,
    logged_in: bool,
    summary: String,
    command_path: Option<PathBuf>,
    account_summary: Option<String>,
}

#[derive(Debug, Clone)]
struct OpenClawImportPlan {
    source_path: PathBuf,
    profiles: Vec<OpenClawImportProfile>,
    preferred_index: usize,
}

#[derive(Debug, Clone)]
struct OpenClawImportOutcome {
    source_path: PathBuf,
    imported_count: usize,
    default_label: String,
    default_source_profile_id: String,
}

#[derive(Debug, Clone)]
struct OpenClawImportProfile {
    source_profile_id: String,
    provider: ProviderKind,
    label: String,
    account_label: Option<String>,
    access_token: Option<String>,
    refresh_token: Option<String>,
    expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Deserialize)]
struct OpenClawAuthProfileStoreFile {
    #[serde(default)]
    profiles: HashMap<String, OpenClawRawCredential>,
    #[serde(default, rename = "lastGood")]
    last_good: HashMap<String, String>,
    #[serde(default, rename = "usageStats")]
    usage_stats: HashMap<String, OpenClawUsageStats>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct OpenClawRawCredential {
    #[serde(default, rename = "type")]
    kind: Option<String>,
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    provider: Option<String>,
    #[serde(default)]
    access: Option<String>,
    #[serde(default)]
    refresh: Option<String>,
    #[serde(default)]
    expires: Option<i64>,
    #[serde(default)]
    email: Option<String>,
    #[serde(default, rename = "displayName")]
    display_name: Option<String>,
    #[serde(default, rename = "accountId")]
    account_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct OpenClawUsageStats {
    #[serde(default, rename = "lastUsed")]
    last_used: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OperatorPendingOAuthView {
    id: String,
    provider: String,
    label: String,
    kind: String,
    started_at: String,
    expires_at: Option<String>,
    authorization_url: Option<String>,
    redirect_uri: Option<String>,
    callback_prompt: Option<String>,
    verification_uri: Option<String>,
    user_code: Option<String>,
    action_hint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OperatorBackgroundRunnerState {
    runner_id: String,
    pid: u32,
    owner_shell_id: Option<String>,
    owner_shell_pid: Option<u32>,
    started_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    phase: String,
    loop_pause_seconds: f32,
    objective: String,
    model: String,
    thread_id: String,
    thread_label: String,
    #[serde(default = "default_native_engine_mode")]
    engine_mode: OperatorEngineMode,
    completed_turn_count: u64,
    last_summary: Option<String>,
    last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CodexCliSessionState {
    session_id: String,
    updated_at: DateTime<Utc>,
    model: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CodexCliTurnRecord {
    recorded_at: DateTime<Utc>,
    session_id: Option<String>,
    model: String,
    objective: String,
    reply: String,
    summary: String,
    event_lines: Vec<String>,
    warning_lines: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CodexCliLiveStreamState {
    started_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    active: bool,
    objective: String,
    session_id: Option<String>,
    latest_text: String,
    event_lines: Vec<String>,
    warning_lines: Vec<String>,
}

struct CapturedCommandOutput {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

struct BackgroundRunnerRead {
    state: Option<OperatorBackgroundRunnerState>,
    disposition: Option<BackgroundRunnerDisposition>,
    notice: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackgroundRunnerDisposition {
    Live,
    TerminalIdle,
    TerminalError,
    Crashed,
}

impl BackgroundRunnerDisposition {
    fn as_label(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::TerminalIdle => "terminal_idle",
            Self::TerminalError => "terminal_error",
            Self::Crashed => "crashed",
        }
    }

    fn is_active(self) -> bool {
        matches!(self, Self::Live)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackgroundHandoffDisposition {
    WaitingForTarget,
    ReadyToLaunch,
    Obsolete,
}

impl BackgroundHandoffDisposition {
    fn as_label(self) -> &'static str {
        match self {
            Self::WaitingForTarget => "waiting_for_target",
            Self::ReadyToLaunch => "ready_to_launch",
            Self::Obsolete => "obsolete",
        }
    }

    fn is_ready(self) -> bool {
        matches!(self, Self::ReadyToLaunch)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OperatorBackgroundStopRequest {
    runner_id: Option<String>,
    requested_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OperatorBackgroundHandoffRequest {
    target_runner_id: Option<String>,
    requested_at: DateTime<Utc>,
    settings: RunSettings,
    loop_pause_seconds: f32,
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct OperatorSnapshot {
    refreshed_at: Option<DateTime<Utc>>,
    run_state: String,
    run_mode: String,
    summary: String,
    last_error: Option<String>,
    auth_ready: bool,
    auth_readiness: String,
    auth_summary: String,
    native_auth_ready: bool,
    native_auth_readiness: String,
    native_auth_summary: String,
    auth_notice: Option<String>,
    codex_cli_available: bool,
    codex_cli_logged_in: bool,
    codex_cli_summary: String,
    codex_cli_command_path: Option<String>,
    codex_cli_account_summary: Option<String>,
    codex_cli_session_id: Option<String>,
    codex_cli_last_turn_summary: Option<String>,
    codex_cli_last_turn_reply: Option<String>,
    codex_cli_live_stream_active: bool,
    codex_cli_live_stream_objective: Option<String>,
    codex_cli_live_stream_updated_at: Option<DateTime<Utc>>,
    codex_cli_live_stream_text: Option<String>,
    codex_cli_live_stream_events: Vec<String>,
    codex_cli_live_stream_warnings: Vec<String>,
    #[serde(default)]
    codex_cli_recent_turn_objectives: Vec<String>,
    codex_cli_recent_turn_replies: Vec<String>,
    codex_cli_recent_events: Vec<String>,
    operator_env_configured_keys: Vec<String>,
    github_action_pending: bool,
    github_action_state: Option<String>,
    github_action_updated_at: Option<DateTime<Utc>>,
    github_action_requested_at: Option<DateTime<Utc>>,
    github_action_kind: Option<String>,
    github_action_repository: Option<String>,
    github_action_target: Option<String>,
    github_action_requires_target: bool,
    github_action_target_kind: Option<String>,
    github_action_target_suggestions: Vec<OperatorGithubTargetSuggestion>,
    github_action_target_guidance: Option<String>,
    github_action_summary: Option<String>,
    github_action_latest_summary: Option<String>,
    github_action_detail: Option<String>,
    github_action_result_excerpt: Option<String>,
    github_action_result_url: Option<String>,
    github_action_body: Option<String>,
    github_action_justification: Option<String>,
    github_action_recent_events: Vec<String>,
    background_runner_active: bool,
    background_runner_status: Option<String>,
    background_runner_id: Option<String>,
    background_runner_pid: Option<u32>,
    background_runner_owner_shell_id: Option<String>,
    background_runner_owner_shell_pid: Option<u32>,
    background_runner_owner_shell_alive: Option<bool>,
    background_runner_phase: Option<String>,
    background_runner_started_at: Option<DateTime<Utc>>,
    background_runner_updated_at: Option<DateTime<Utc>>,
    background_runner_thread_id: Option<String>,
    background_runner_thread_label: Option<String>,
    background_runner_model: Option<String>,
    background_runner_objective: Option<String>,
    background_runner_engine_mode: Option<String>,
    background_runner_loop_pause_seconds: Option<f32>,
    background_runner_turn_count: Option<u64>,
    background_stop_requested: bool,
    background_runner_summary: Option<String>,
    background_runner_last_error: Option<String>,
    background_recovery_recommendation: Option<String>,
    background_reattach_required: bool,
    background_reattach_recommendation: Option<String>,
    background_handoff_pending: bool,
    background_handoff_ready: bool,
    background_handoff_status: Option<String>,
    background_handoff_target_runner_id: Option<String>,
    background_handoff_requested_at: Option<DateTime<Utc>>,
    background_handoff_objective: Option<String>,
    background_handoff_model: Option<String>,
    background_handoff_thread_id: Option<String>,
    background_handoff_thread_label: Option<String>,
    background_handoff_engine_mode: Option<String>,
    background_handoff_loop_pause_seconds: Option<f32>,
    foreground_thread_id: Option<String>,
    compaction_count: Option<u64>,
    pending_turn_phase: Option<String>,
    pending_turn_action: Option<String>,
    completed_turn_count: u64,
    last_turn_summary: Option<String>,
    last_turn_reply: Option<String>,
    recent_turns: Vec<String>,
    recent_turn_replies: Vec<String>,
    recent_events: Vec<String>,
    pending_oauth: Vec<OperatorPendingOAuthView>,
    current_brief: Option<String>,
    current_plan: Option<String>,
    current_open_gaps: Option<String>,
    current_handoff: Option<String>,
    runtime_grounding_bundle: Option<String>,
}

struct HarnessController {
    paths: OperatorPaths,
}

impl HarnessController {
    fn new(paths: OperatorPaths) -> anyhow::Result<Self> {
        fs::create_dir_all(&paths.operator_root)
            .with_context(|| format!("create operator root {}", paths.operator_root.display()))?;
        fs::create_dir_all(&paths.session_root)
            .with_context(|| format!("create session root {}", paths.session_root.display()))?;
        ensure_operator_env_template(&paths.operator_env_path)?;
        load_operator_env_overlay(&paths.operator_env_path)?;
        Ok(Self { paths })
    }

    fn persist_snapshot(&self, snapshot: &OperatorSnapshot) -> anyhow::Result<()> {
        write_json_atomic(&self.paths.status_path, snapshot)
    }

    fn read_github_action_request(
        &self,
    ) -> anyhow::Result<Option<OperatorGithubActionRequestRecord>> {
        read_json_file(self.paths.github_action_request_path.clone())
    }

    fn read_recent_github_action_history(
        &self,
        limit: usize,
    ) -> anyhow::Result<Vec<OperatorGithubActionLifecycleRecord>> {
        read_recent_jsonl_entries(self.paths.github_action_history_path.clone(), limit)
    }

    fn write_github_action_request(
        &self,
        request: &OperatorGithubActionRequestRecord,
    ) -> anyhow::Result<()> {
        write_json_atomic(&self.paths.github_action_request_path, request)
    }

    fn append_github_action_history(
        &self,
        record: &OperatorGithubActionLifecycleRecord,
    ) -> anyhow::Result<()> {
        append_jsonl_entry(&self.paths.github_action_history_path, record)
    }

    fn remove_github_action_request(&self) -> anyhow::Result<()> {
        remove_file_if_exists(&self.paths.github_action_request_path)
    }

    fn record_pending_github_action_request(
        &self,
        request: &OperatorGithubActionRequestRecord,
    ) -> anyhow::Result<()> {
        let request = self.enrich_github_action_request_record(request.clone());
        self.write_github_action_request(&request)?;
        self.append_github_action_history(&queued_github_action_history_record(&request))
    }

    fn enrich_github_action_request_record(
        &self,
        mut request: OperatorGithubActionRequestRecord,
    ) -> OperatorGithubActionRequestRecord {
        if request.request.requires_operator_target() && request.target_suggestions.is_empty() {
            request.target_suggestions =
                discover_github_target_suggestions(&self.paths, &request.request);
        }
        request
    }

    fn build_auth_store(&self) -> FileAuthProfileStore {
        FileAuthProfileStore::new(&self.paths.auth_store_path)
    }

    fn apply_github_action_request(
        &self,
        target_override: Option<String>,
    ) -> anyhow::Result<String> {
        self.apply_github_action_request_with_runner(target_override, |request| {
            let command = build_github_action_command(request)?;
            run_repo_command_checked(&self.paths.repo_root, "gh", &command.args)
        })
    }

    fn apply_github_action_request_with_runner<F>(
        &self,
        target_override: Option<String>,
        run_request: F,
    ) -> anyhow::Result<String>
    where
        F: FnOnce(&SupervisedGithubActionRequest) -> anyhow::Result<String>,
    {
        let request = self
            .read_github_action_request()?
            .context("no pending supervised GitHub request is available")?;
        let target_override = parse_optional_github_target_override(target_override.as_deref())?;
        let effective_request = apply_github_target_override(&request.request, target_override)?;
        let output = run_request(&effective_request)?;
        let (result_excerpt, result_url) = summarize_github_action_result(output.as_str());
        self.remove_github_action_request()?;
        let output_summary = result_excerpt
            .as_deref()
            .map(|line| format!(" | gh: {line}"))
            .unwrap_or_default();
        let summary = format!(
            "applied GitHub request: {}{}",
            effective_request.summary(),
            output_summary
        );
        self.append_github_action_history(&OperatorGithubActionLifecycleRecord {
            recorded_at: Utc::now(),
            state: OperatorGithubActionLifecycleState::Applied,
            thread_id: request.thread_id,
            thread_label: request.thread_label,
            request: effective_request,
            summary: summary.clone(),
            detail: request.narrative,
            result_excerpt,
            result_url,
        })?;
        Ok(summary)
    }

    fn clear_github_action_request(&self) -> anyhow::Result<String> {
        let request = self
            .read_github_action_request()?
            .context("no pending supervised GitHub request is available")?;
        self.remove_github_action_request()?;
        let summary = format!("cleared GitHub request: {}", request.summary);
        self.append_github_action_history(&OperatorGithubActionLifecycleRecord {
            recorded_at: Utc::now(),
            state: OperatorGithubActionLifecycleState::Cleared,
            thread_id: request.thread_id,
            thread_label: request.thread_label,
            request: request.request,
            summary: summary.clone(),
            detail: request.narrative,
            result_excerpt: Some("operator cleared pending GitHub request before apply".into()),
            result_url: None,
        })?;
        Ok(summary)
    }

    fn reject_github_action_request(&self) -> anyhow::Result<String> {
        let request = self
            .read_github_action_request()?
            .context("no pending supervised GitHub request is available")?;
        self.remove_github_action_request()?;
        let summary = format!("rejected GitHub request: {}", request.summary);
        self.append_github_action_history(&OperatorGithubActionLifecycleRecord {
            recorded_at: Utc::now(),
            state: OperatorGithubActionLifecycleState::Rejected,
            thread_id: request.thread_id,
            thread_label: request.thread_label,
            request: request.request,
            summary: summary.clone(),
            detail: request.narrative,
            result_excerpt: Some("operator rejected pending GitHub request".into()),
            result_url: None,
        })?;
        Ok(summary)
    }

    async fn maybe_bootstrap_openclaw_codex_oauth(&self) -> anyhow::Result<Option<String>> {
        let auth_store = self.build_auth_store();
        let outcome = maybe_bootstrap_openclaw_codex_auth_into_store(
            &auth_store,
            discover_openclaw_import_plan()?,
        )
        .await?;
        Ok(outcome.map(|outcome| {
            format!(
                "adopted {} OpenClaw Codex oauth profile{} from {} | default={} ({})",
                outcome.imported_count,
                if outcome.imported_count == 1 { "" } else { "s" },
                outcome.source_path.display(),
                outcome.default_label,
                outcome.default_source_profile_id
            )
        }))
    }

    fn read_background_runner_state(&self) -> anyhow::Result<BackgroundRunnerRead> {
        let state = read_json_file::<OperatorBackgroundRunnerState>(
            self.paths.background_runner_path.clone(),
        )?;
        let Some(state) = state else {
            return Ok(BackgroundRunnerRead {
                state: None,
                disposition: None,
                notice: None,
            });
        };

        let disposition = classify_background_runner(&state);
        let notice = match disposition {
            BackgroundRunnerDisposition::Crashed => Some(format!(
                "background runner {} heartbeat expired; treating record as crashed after {}",
                state.runner_id,
                format_background_runner_age(state.updated_at)
            )),
            _ => None,
        };

        Ok(BackgroundRunnerRead {
            state: Some(state),
            disposition: Some(disposition),
            notice,
        })
    }

    fn reconcile_background_control_artifacts(&self) -> anyhow::Result<Option<String>> {
        self.reconcile_background_control_artifacts_with(background_runner_process_is_alive)
    }

    fn reconcile_background_control_artifacts_with<F>(
        &self,
        is_process_alive: F,
    ) -> anyhow::Result<Option<String>>
    where
        F: Fn(u32) -> bool,
    {
        let mut notices = Vec::new();
        let mut background_runner = read_json_file::<OperatorBackgroundRunnerState>(
            self.paths.background_runner_path.clone(),
        )?;

        if let Some(mut runner) = background_runner.clone() {
            if let Some((summary, last_error, notice)) =
                reconcile_crashed_background_runner(&runner, &is_process_alive)
            {
                runner.phase = "crashed".into();
                runner.updated_at = Utc::now();
                runner.last_summary = Some(summary);
                if runner.last_error.is_none() {
                    runner.last_error = Some(last_error);
                }
                self.write_background_runner_state(&runner)?;
                notices.push(notice);
                background_runner = Some(runner);
            }
        }

        let background_runner_disposition =
            background_runner.as_ref().map(classify_background_runner);
        let live_runner_id = match (&background_runner, background_runner_disposition) {
            (Some(runner), Some(BackgroundRunnerDisposition::Live)) => {
                Some(runner.runner_id.as_str())
            }
            _ => None,
        };

        if let Some(stop_request) = self.read_background_stop_request()? {
            let stop_request_is_stale = match (stop_request.runner_id.as_deref(), live_runner_id) {
                (Some(target), Some(current)) => target != current,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => true,
            };
            if stop_request_is_stale {
                self.clear_background_stop_request()?;
                notices.push("cleared stale background stop request".into());
            }
        }

        if let Some(handoff_request) = self.read_background_handoff_request()? {
            if matches!(
                classify_background_handoff(
                    &handoff_request,
                    background_runner.as_ref(),
                    background_runner_disposition
                ),
                BackgroundHandoffDisposition::Obsolete
            ) {
                self.clear_background_handoff_request()?;
                notices.push("cleared obsolete background handoff request".into());
            }
        }

        Ok(combine_optional_notices(notices.into_iter().map(Some)))
    }

    fn write_background_runner_state(
        &self,
        state: &OperatorBackgroundRunnerState,
    ) -> anyhow::Result<()> {
        write_json_atomic(&self.paths.background_runner_path, state)
    }

    fn update_background_runner_state(
        &self,
        phase: &str,
        completed_turn_count: u64,
        summary: Option<String>,
        last_error: Option<String>,
    ) -> anyhow::Result<()> {
        let state = read_json_file::<OperatorBackgroundRunnerState>(
            self.paths.background_runner_path.clone(),
        )?;
        if let Some(mut state) = state {
            state.updated_at = Utc::now();
            state.phase = phase.into();
            state.completed_turn_count = completed_turn_count;
            if let Some(summary) = summary {
                state.last_summary = Some(summary);
            }
            state.last_error = last_error;
            self.write_background_runner_state(&state)?;
        }
        Ok(())
    }

    fn touch_background_runner(&self, phase: &str) -> anyhow::Result<()> {
        let state = read_json_file::<OperatorBackgroundRunnerState>(
            self.paths.background_runner_path.clone(),
        )?;
        if let Some(mut state) = state {
            state.updated_at = Utc::now();
            state.phase = phase.into();
            self.write_background_runner_state(&state)?;
        }
        Ok(())
    }

    fn clear_background_runner_state(&self) -> anyhow::Result<()> {
        remove_file_if_exists(&self.paths.background_runner_path)?;
        remove_file_if_exists(&self.paths.background_stop_path)?;
        remove_file_if_exists(&self.paths.background_handoff_path)?;
        Ok(())
    }

    fn read_background_stop_request(
        &self,
    ) -> anyhow::Result<Option<OperatorBackgroundStopRequest>> {
        if !self.paths.background_stop_path.exists() {
            return Ok(None);
        }
        let body = fs::read_to_string(&self.paths.background_stop_path)
            .with_context(|| format!("read {}", self.paths.background_stop_path.display()))?;
        match serde_json::from_str::<OperatorBackgroundStopRequest>(&body) {
            Ok(request) => Ok(Some(request)),
            Err(_) => Ok(Some(OperatorBackgroundStopRequest {
                runner_id: None,
                requested_at: Utc::now(),
            })),
        }
    }

    fn request_background_stop(&self, runner_id: Option<&str>) -> anyhow::Result<()> {
        let request = OperatorBackgroundStopRequest {
            runner_id: runner_id.map(str::to_owned),
            requested_at: Utc::now(),
        };
        write_json_atomic(&self.paths.background_stop_path, &request)
    }

    fn clear_background_stop_request(&self) -> anyhow::Result<()> {
        remove_file_if_exists(&self.paths.background_stop_path)
    }

    fn read_background_handoff_request(
        &self,
    ) -> anyhow::Result<Option<OperatorBackgroundHandoffRequest>> {
        read_json_file(self.paths.background_handoff_path.clone())
    }

    fn write_background_handoff_request(
        &self,
        request: &OperatorBackgroundHandoffRequest,
    ) -> anyhow::Result<()> {
        write_json_atomic(&self.paths.background_handoff_path, request)
    }

    fn request_background_handoff(
        &self,
        target_runner_id: Option<&str>,
        settings: &RunSettings,
        loop_pause_seconds: f32,
    ) -> anyhow::Result<()> {
        let request = OperatorBackgroundHandoffRequest {
            target_runner_id: target_runner_id.map(str::to_owned),
            requested_at: Utc::now(),
            settings: settings.clone(),
            loop_pause_seconds: loop_pause_seconds.max(0.0),
        };
        self.write_background_handoff_request(&request)
    }

    fn clear_background_handoff_request(&self) -> anyhow::Result<()> {
        remove_file_if_exists(&self.paths.background_handoff_path)
    }

    fn attach_background_runner(
        &self,
        runner_id: &str,
        owner_shell_id: &str,
        owner_shell_pid: u32,
    ) -> anyhow::Result<String> {
        let mut read = self.read_background_runner_state()?;
        let disposition = read
            .disposition
            .context("no detached background runner is currently recorded")?;
        let mut runner = read
            .state
            .take()
            .context("no detached background runner is currently recorded")?;
        if runner.runner_id != runner_id {
            return Err(anyhow!(
                "background runner changed while trying to reattach (expected {runner_id}, found {})",
                runner.runner_id
            ));
        }
        if !matches!(disposition, BackgroundRunnerDisposition::Live) {
            return Err(anyhow!(
                "background runner {} is no longer live; refresh the shell before reattaching",
                runner.runner_id
            ));
        }
        runner.owner_shell_id = Some(owner_shell_id.to_string());
        runner.owner_shell_pid = Some(owner_shell_pid);
        self.write_background_runner_state(&runner)?;
        Ok(format!(
            "reattached this shell to detached runner {} (pid={})",
            runner.runner_id, runner.pid
        ))
    }

    fn background_stop_requested_for(&self, runner_id: Option<&str>) -> anyhow::Result<bool> {
        let Some(request) = self.read_background_stop_request()? else {
            return Ok(false);
        };
        Ok(match (request.runner_id.as_deref(), runner_id) {
            (Some(target), Some(current)) => target == current,
            (Some(_), None) => false,
            (None, _) => true,
        })
    }

    fn spawn_background_loop_process(
        &self,
        settings: &RunSettings,
        loop_pause_seconds: f32,
        owner_shell_id: &str,
        owner_shell_pid: u32,
    ) -> anyhow::Result<BackgroundSpawnResult> {
        let _ = self.reconcile_background_control_artifacts()?;
        let existing = self.read_background_runner_state()?;
        match (existing.state.as_ref(), existing.disposition) {
            (Some(existing), disposition) if !background_runner_allows_spawn(disposition) => {
                return Err(anyhow!(
                    "background loop already appears active (pid={} id={}); stop or clear it before starting another",
                    existing.pid,
                    existing.runner_id
                ));
            }
            (
                Some(_),
                Some(
                    BackgroundRunnerDisposition::TerminalIdle
                    | BackgroundRunnerDisposition::TerminalError
                    | BackgroundRunnerDisposition::Crashed,
                ),
            ) => {
                self.clear_background_runner_state()?;
            }
            _ => {}
        }

        self.clear_background_stop_request()?;
        let runner_id = Uuid::new_v4().to_string();
        let exe_path = env::current_exe().context("resolve current operator executable")?;
        let child = Command::new(&exe_path)
            .arg("--background-loop")
            .arg("--background-runner-id")
            .arg(runner_id.as_str())
            .arg("--background-owner-shell-id")
            .arg(owner_shell_id)
            .arg("--background-owner-shell-pid")
            .arg(owner_shell_pid.to_string())
            .arg("--objective")
            .arg(settings.objective.as_str())
            .arg("--model")
            .arg(settings.model.as_str())
            .arg("--thread-id")
            .arg(settings.thread_id.as_str())
            .arg("--thread-label")
            .arg(settings.thread_label.as_str())
            .arg("--engine-mode")
            .arg(match settings.engine_mode {
                OperatorEngineMode::CodexCli => "codex_cli",
                OperatorEngineMode::NativeHarness => "native_harness",
            })
            .arg("--loop-pause-seconds")
            .arg(format!("{:.2}", loop_pause_seconds.max(0.0)))
            .current_dir(&self.paths.repo_root)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .with_context(|| format!("spawn {}", exe_path.display()))?;
        let pid = child.id();
        let runner_state = provisional_background_runner_state(
            runner_id.as_str(),
            pid,
            Some(owner_shell_id),
            Some(owner_shell_pid),
            settings,
            loop_pause_seconds,
        );
        self.write_background_runner_state(&runner_state)?;
        Ok(BackgroundSpawnResult { pid, runner_id })
    }

    fn build_llm_client(&self) -> anyhow::Result<ConfiguredLlmClient<FileAuthProfileStore>> {
        let auth_store = Arc::new(self.build_auth_store());
        let mut registry = ProviderRegistry::new();
        register_openai_responses_providers(&mut registry)?;
        Ok(ConfiguredLlmClient::new(auth_store, registry))
    }

    async fn run_turn(&self, settings: &RunSettings) -> anyhow::Result<OperatorTurnResult> {
        match settings.engine_mode {
            OperatorEngineMode::CodexCli => self.run_codex_cli_turn(settings).await,
            OperatorEngineMode::NativeHarness => self.run_native_turn(settings).await,
        }
    }

    async fn run_native_turn(&self, settings: &RunSettings) -> anyhow::Result<OperatorTurnResult> {
        let store = Arc::new(SqliteStateStore::connect(&self.paths.state_db_path).await?);
        let offload = Arc::new(FilesystemOffloadSink::new(&self.paths.repo_root));
        let orchestrator = PersistentOrchestrator::new(store, offload);
        let _ = self.maybe_bootstrap_openclaw_codex_oauth().await?;
        let llm = self.build_llm_client()?;
        llm.resume_auth_lifecycle().await?;
        let host = EmbeddedHostBody;
        let external_context = build_project_artifact_context(&self.paths, &settings.objective)?;

        ensure_plan(&orchestrator, settings.objective.trim()).await?;
        let outcome = orchestrator
            .run_runtime_turn(
                &llm,
                &host,
                RuntimeTurnOptions {
                    model: Some(normalize_text(&settings.model, DEFAULT_MODEL)),
                    session: Some(splcw_orchestrator::RuntimeSessionConfig::new(
                        &self.paths.session_root,
                        &self.paths.session_id,
                    )),
                    thread_id: Some(normalize_text(&settings.thread_id, DEFAULT_THREAD_ID)),
                    thread_label: Some(normalize_text(
                        &settings.thread_label,
                        DEFAULT_THREAD_LABEL,
                    )),
                    external_context,
                    ..RuntimeTurnOptions::default()
                },
            )
            .await?;

        if let Some(request) = outcome.github_action_request {
            let record = OperatorGithubActionRequestRecord {
                requested_at: Utc::now(),
                thread_id: normalize_text(&settings.thread_id, DEFAULT_THREAD_ID),
                thread_label: normalize_text(&settings.thread_label, DEFAULT_THREAD_LABEL),
                summary: request.summary(),
                narrative: (!outcome.narrative.trim().is_empty()).then_some(outcome.narrative),
                request,
                target_suggestions: Vec::new(),
            };
            self.record_pending_github_action_request(&record)?;
            return Ok(OperatorTurnResult {
                terminal: OperatorTurnTerminal::GithubActionRequested,
                summary: format!("supervised GitHub request: {}", record.summary),
            });
        }

        if let Some(gap) = outcome.surfaced_gap {
            return Ok(OperatorTurnResult {
                terminal: OperatorTurnTerminal::SurfacedGap,
                summary: format!("gap: {} -> {}", gap.title, gap.permanent_fix_target),
            });
        }
        if let Some(receipt) = outcome.receipt {
            return Ok(OperatorTurnResult {
                terminal: OperatorTurnTerminal::Completed,
                summary: format!("receipt: {}", receipt.changed),
            });
        }
        if !outcome.narrative.trim().is_empty() {
            return Ok(OperatorTurnResult {
                terminal: OperatorTurnTerminal::Completed,
                summary: outcome.narrative,
            });
        }
        Ok(OperatorTurnResult {
            terminal: OperatorTurnTerminal::Completed,
            summary: "turn completed without receipt or surfaced gap".into(),
        })
    }

    async fn run_codex_cli_turn(
        &self,
        settings: &RunSettings,
    ) -> anyhow::Result<OperatorTurnResult> {
        let cli_status = codex_cli_status(&self.paths);
        if !cli_status.available {
            anyhow::bail!(cli_status.summary);
        }
        if !cli_status.logged_in {
            anyhow::bail!(
                "Codex CLI is installed but not logged in yet. Use the Auth page to launch `codex login` first."
            );
        }
        let command_path = cli_status
            .command_path
            .as_ref()
            .context("Codex CLI command path was not recorded")?;
        let session_dir = self.paths.session_root.join(&self.paths.session_id);
        fs::create_dir_all(&session_dir)
            .with_context(|| format!("create session dir {}", session_dir.display()))?;
        let grounding_bundle = build_runtime_grounding_bundle(&self.paths)?;
        let prompt =
            build_codex_cli_context_prompt(&self.paths, settings.objective.as_str(), grounding_bundle.as_deref());
        let previous_session =
            read_json_file::<CodexCliSessionState>(self.paths.codex_cli_session_path.clone())?;
        let args = if let Some(previous_session) = previous_session.as_ref() {
            vec![
                "exec".to_string(),
                "resume".to_string(),
                "--json".to_string(),
                "-m".to_string(),
                normalize_text(&settings.model, DEFAULT_MODEL),
                previous_session.session_id.clone(),
                "-".to_string(),
            ]
        } else {
            vec![
                "exec".to_string(),
                "--json".to_string(),
                "--color".to_string(),
                "never".to_string(),
                "-m".to_string(),
                normalize_text(&settings.model, DEFAULT_MODEL),
                "-C".to_string(),
                self.paths.repo_root.display().to_string(),
                "-".to_string(),
            ]
        };
        let output = run_command_stream_with_stdin(
            command_path,
            &self.paths.repo_root,
            &args,
            prompt.as_bytes(),
            &self.paths.codex_cli_live_stream_path,
            settings.objective.as_str(),
        )?;
        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let parsed = parse_codex_cli_exec_output(&stdout, &stderr);
        let session_id = parsed.session_id.clone().or_else(|| {
            previous_session
                .as_ref()
                .map(|state| state.session_id.clone())
        });
        if let Some(session_id) = session_id.as_ref() {
            write_json_atomic(
                &self.paths.codex_cli_session_path,
                &CodexCliSessionState {
                    session_id: session_id.clone(),
                    updated_at: Utc::now(),
                    model: normalize_text(&settings.model, DEFAULT_MODEL),
                },
            )?;
        }
        let warning_lines = parsed.warning_lines.clone();
        let summary = if !warning_lines.is_empty() {
            format!(
                "{} | warning: {}",
                parsed.summary,
                truncate_for_summary(&warning_lines[0], 120)
            )
        } else {
            parsed.summary.clone()
        };
        append_jsonl_entry(
            &session_dir.join("codex-cli-turn-log.jsonl"),
            &CodexCliTurnRecord {
                recorded_at: Utc::now(),
                session_id,
                model: normalize_text(&settings.model, DEFAULT_MODEL),
                objective: normalize_text(&settings.objective, DEFAULT_OBJECTIVE),
                reply: parsed.reply.clone(),
                summary: summary.clone(),
                event_lines: parsed.event_lines,
                warning_lines,
            },
        )?;
        if !output.status.success() {
            anyhow::bail!(
                "Codex CLI turn failed: {}",
                if parsed.reply.trim().is_empty() {
                    stderr.trim()
                } else {
                    parsed.reply.trim()
                }
            );
        }
        Ok(OperatorTurnResult {
            terminal: OperatorTurnTerminal::Completed,
            summary: format!("Codex CLI: {summary}"),
        })
    }

    async fn read_snapshot(
        &self,
        run_state: &str,
        run_mode: OperatorRunMode,
        summary: &str,
        last_error: Option<String>,
        completed_turn_count: u64,
        auth_notice: Option<String>,
    ) -> anyhow::Result<OperatorSnapshot> {
        let background_reconciliation_notice = self.reconcile_background_control_artifacts()?;
        let session_dir = self.paths.session_root.join(&self.paths.session_id);
        let session_state = read_json_file::<RuntimeSessionState>(session_dir.join("state.json"))?;
        let pending_turn =
            read_json_file::<RuntimePendingTurnCheckpoint>(session_dir.join("pending-turn.json"))?;
        let recent_turn_records = read_recent_jsonl_entries::<RuntimeTurnRecord>(
            session_dir.join("turn-log.jsonl"),
            RECENT_TURN_LIMIT,
        )?;
        let recent_codex_cli_turn_records = read_recent_jsonl_entries::<CodexCliTurnRecord>(
            session_dir.join("codex-cli-turn-log.jsonl"),
            RECENT_TURN_LIMIT,
        )?;
        let recent_event_lines = match session_state.as_ref() {
            Some(state) => read_recent_jsonl_entries::<RuntimeSessionEvent>(
                session_dir.join(&state.current_transcript_path),
                RECENT_EVENT_LIMIT,
            )?
            .into_iter()
            .map(|event| format_session_event(&event))
            .collect(),
            None => Vec::new(),
        };
        let recent_turns = recent_turn_records
            .iter()
            .map(RuntimeTurnRecord::summarize)
            .collect::<Vec<_>>();
        let last_turn_summary = recent_turn_records.last().map(RuntimeTurnRecord::summarize);
        let recent_turn_replies = recent_turn_records
            .iter()
            .rev()
            .map(format_runtime_turn_reply)
            .collect::<Vec<_>>();
        let last_turn_reply = recent_turn_records.last().map(format_runtime_turn_reply);
        let codex_cli_session =
            read_json_file::<CodexCliSessionState>(self.paths.codex_cli_session_path.clone())?;
        let codex_cli_status = codex_cli_status(&self.paths);
        let codex_cli_live_stream =
            read_json_file::<CodexCliLiveStreamState>(self.paths.codex_cli_live_stream_path.clone())?;
        let codex_cli_recent_turn_objectives = recent_codex_cli_turn_records
            .iter()
            .map(|record| record.objective.clone())
            .collect::<Vec<_>>();
        let codex_cli_recent_turn_replies = recent_codex_cli_turn_records
            .iter()
            .map(format_codex_cli_turn_reply)
            .collect::<Vec<_>>();
        let codex_cli_last_turn_reply = recent_codex_cli_turn_records
            .last()
            .map(format_codex_cli_turn_reply);
        let codex_cli_last_turn_summary = recent_codex_cli_turn_records
            .last()
            .map(|record| record.summary.clone());
        let codex_cli_recent_events = recent_codex_cli_turn_records
            .last()
            .map(|record| {
                let mut events = record.event_lines.clone();
                events.extend(
                    record
                        .warning_lines
                        .iter()
                        .map(|line| format!("warning {line}")),
                );
                events
            })
            .unwrap_or_default();
        let mut github_action_request = self.read_github_action_request()?;
        if let Some(request) = github_action_request.as_mut() {
            let enriched = self.enrich_github_action_request_record(request.clone());
            if enriched.target_suggestions != request.target_suggestions {
                self.write_github_action_request(&enriched)?;
                *request = enriched;
            }
        }
        let mut github_action_history =
            self.read_recent_github_action_history(RECENT_GITHUB_ACTION_LIMIT)?;
        if let Some(request) = github_action_request.as_ref() {
            let expected_queued = queued_github_action_history_record(request);
            let latest_matches_pending = github_action_history
                .last()
                .map(|record| {
                    record.state == OperatorGithubActionLifecycleState::Queued
                        && record.thread_id == expected_queued.thread_id
                        && record.thread_label == expected_queued.thread_label
                        && record.request == expected_queued.request
                        && record.summary == expected_queued.summary
                })
                .unwrap_or(false);
            if !latest_matches_pending {
                self.append_github_action_history(&expected_queued)?;
                github_action_history.push(expected_queued);
                if github_action_history.len() > RECENT_GITHUB_ACTION_LIMIT {
                    let keep_from = github_action_history.len() - RECENT_GITHUB_ACTION_LIMIT;
                    github_action_history = github_action_history.split_off(keep_from);
                }
            }
        }
        let latest_settled_github_action_history = github_action_history
            .iter()
            .rev()
            .find(|record| record.state != OperatorGithubActionLifecycleState::Queued);
        let latest_github_action_request = github_action_request
            .as_ref()
            .map(|request| &request.request)
            .or_else(|| latest_settled_github_action_history.map(|record| &record.request));
        let background_runner_read = self.read_background_runner_state()?;
        let background_runner = background_runner_read.state;
        let background_handoff = self.read_background_handoff_request()?;
        let background_handoff_disposition = background_handoff.as_ref().map(|request| {
            classify_background_handoff(
                request,
                background_runner.as_ref(),
                background_runner_read.disposition,
            )
        });
        let (effective_run_state, effective_run_mode, effective_summary) =
            effective_operator_status(
                run_state,
                run_mode,
                summary,
                background_runner.as_ref(),
                background_runner_read.disposition,
            );
        let imported_openclaw_notice = self.maybe_bootstrap_openclaw_codex_oauth().await?;
        let auth_store = self.build_auth_store();
        let default_profile = auth_store.load_default_profile().await?;
        let pending_oauth = auth_store.list_pending_oauth().await?;
        let (native_auth_ready, native_auth_readiness, native_auth_summary) =
            describe_auth_state(default_profile.as_ref(), &pending_oauth);
        let (auth_ready, auth_readiness, auth_summary) = describe_operator_auth_state(
            &codex_cli_status,
            native_auth_ready,
            native_auth_readiness.as_str(),
            native_auth_summary.as_str(),
        );
        let operator_env_assignments =
            read_operator_env_assignments(&self.paths.operator_env_path)?;
        let background_recovery_recommendation = background_runner.as_ref().and_then(|state| {
            background_recovery_recommendation(state, background_runner_read.disposition)
        });
        let background_runner_owner_shell_alive = background_runner
            .as_ref()
            .and_then(background_runner_owner_shell_alive);
        let background_reattach_recommendation = background_runner.as_ref().and_then(|state| {
            background_reattach_recommendation(
                state,
                background_runner_read.disposition,
                background_runner_owner_shell_alive,
            )
        });

        let auth_notice = combine_optional_notices([
            background_reconciliation_notice,
            background_runner_read.notice,
            auth_notice,
            imported_openclaw_notice,
        ]);
        let background_runner_active = background_runner_read
            .disposition
            .is_some_and(BackgroundRunnerDisposition::is_active);
        let background_stop_requested = if background_runner_active {
            self.background_stop_requested_for(
                background_runner
                    .as_ref()
                    .map(|state| state.runner_id.as_str()),
            )?
        } else {
            false
        };

        let snapshot = OperatorSnapshot {
            refreshed_at: Some(Utc::now()),
            run_state: effective_run_state,
            run_mode: effective_run_mode.as_label().into(),
            summary: effective_summary,
            last_error,
            auth_ready,
            auth_readiness,
            auth_summary,
            native_auth_ready,
            native_auth_readiness,
            native_auth_summary,
            auth_notice,
            codex_cli_available: codex_cli_status.available,
            codex_cli_logged_in: codex_cli_status.logged_in,
            codex_cli_summary: codex_cli_status.summary,
            codex_cli_command_path: codex_cli_status
                .command_path
                .as_ref()
                .map(|path| path.display().to_string()),
            codex_cli_account_summary: codex_cli_status.account_summary,
            codex_cli_session_id: codex_cli_session
                .as_ref()
                .map(|state| state.session_id.clone()),
            codex_cli_last_turn_summary,
            codex_cli_last_turn_reply,
            codex_cli_live_stream_active: codex_cli_live_stream
                .as_ref()
                .map(|stream| stream.active)
                .unwrap_or(false),
            codex_cli_live_stream_objective: codex_cli_live_stream
                .as_ref()
                .map(|stream| stream.objective.clone()),
            codex_cli_live_stream_updated_at: codex_cli_live_stream
                .as_ref()
                .map(|stream| stream.updated_at),
            codex_cli_live_stream_text: codex_cli_live_stream.as_ref().and_then(|stream| {
                let text = stream.latest_text.trim();
                (!text.is_empty()).then(|| text.to_string())
            }),
            codex_cli_live_stream_events: codex_cli_live_stream
                .as_ref()
                .map(|stream| stream.event_lines.clone())
                .unwrap_or_default(),
            codex_cli_live_stream_warnings: codex_cli_live_stream
                .as_ref()
                .map(|stream| stream.warning_lines.clone())
                .unwrap_or_default(),
            codex_cli_recent_turn_objectives,
            codex_cli_recent_turn_replies,
            codex_cli_recent_events,
            operator_env_configured_keys: operator_env_assignments
                .iter()
                .map(|(key, _)| key.clone())
                .collect(),
            github_action_pending: github_action_request.is_some(),
            github_action_state: github_action_request
                .as_ref()
                .map(|_| {
                    OperatorGithubActionLifecycleState::Queued
                        .as_label()
                        .to_string()
                })
                .or_else(|| {
                    latest_settled_github_action_history
                        .map(|record| record.state.as_label().to_string())
                }),
            github_action_updated_at: github_action_request
                .as_ref()
                .map(|request| request.requested_at)
                .or_else(|| latest_settled_github_action_history.map(|record| record.recorded_at)),
            github_action_requested_at: github_action_request
                .as_ref()
                .map(|request| request.requested_at),
            github_action_kind: latest_github_action_request
                .map(|request| request.kind.as_label().to_string()),
            github_action_repository: latest_github_action_request
                .and_then(|request| request.repository.clone()),
            github_action_target: latest_github_action_request
                .map(|request| request.target_summary()),
            github_action_requires_target: github_action_request
                .as_ref()
                .map(|request| request.request.requires_operator_target())
                .unwrap_or(false),
            github_action_target_kind: latest_github_action_request
                .map(|request| request.operator_target_kind().to_string()),
            github_action_target_suggestions: github_action_request
                .as_ref()
                .map(|request| request.target_suggestions.clone())
                .unwrap_or_default(),
            github_action_target_guidance: github_action_request.as_ref().and_then(|request| {
                build_github_target_guidance(
                    &self.paths,
                    &request.request,
                    &request.target_suggestions,
                )
            }),
            github_action_summary: github_action_request
                .as_ref()
                .map(|request| request.summary.clone()),
            github_action_latest_summary: github_action_request
                .as_ref()
                .map(|request| request.summary.clone())
                .or_else(|| {
                    latest_settled_github_action_history.map(|record| record.summary.clone())
                }),
            github_action_detail: github_action_request
                .as_ref()
                .and_then(|request| request.narrative.clone())
                .or_else(|| {
                    latest_settled_github_action_history.and_then(|record| record.detail.clone())
                }),
            github_action_result_excerpt: latest_settled_github_action_history
                .and_then(|record| record.result_excerpt.clone()),
            github_action_result_url: latest_settled_github_action_history
                .and_then(|record| record.result_url.clone()),
            github_action_body: latest_github_action_request
                .and_then(|request| request.body.clone()),
            github_action_justification: latest_github_action_request
                .and_then(|request| request.justification.clone()),
            github_action_recent_events: github_action_history
                .iter()
                .map(format_github_action_history_entry)
                .collect(),
            background_runner_active,
            background_runner_status: background_runner_read
                .disposition
                .map(BackgroundRunnerDisposition::as_label)
                .map(str::to_owned),
            background_runner_id: background_runner
                .as_ref()
                .map(|state| state.runner_id.clone()),
            background_runner_pid: background_runner.as_ref().map(|state| state.pid),
            background_runner_owner_shell_id: background_runner
                .as_ref()
                .and_then(|state| state.owner_shell_id.clone()),
            background_runner_owner_shell_pid: background_runner
                .as_ref()
                .and_then(|state| state.owner_shell_pid),
            background_runner_owner_shell_alive,
            background_runner_phase: background_runner.as_ref().map(|state| state.phase.clone()),
            background_runner_started_at: background_runner.as_ref().map(|state| state.started_at),
            background_runner_updated_at: background_runner.as_ref().map(|state| state.updated_at),
            background_runner_thread_id: background_runner
                .as_ref()
                .map(|state| state.thread_id.clone()),
            background_runner_thread_label: background_runner
                .as_ref()
                .map(|state| state.thread_label.clone()),
            background_runner_model: background_runner.as_ref().map(|state| state.model.clone()),
            background_runner_objective: background_runner
                .as_ref()
                .map(|state| state.objective.clone()),
            background_runner_engine_mode: background_runner
                .as_ref()
                .map(|state| state.engine_mode.as_label().to_string()),
            background_runner_loop_pause_seconds: background_runner
                .as_ref()
                .map(|state| state.loop_pause_seconds),
            background_runner_turn_count: background_runner
                .as_ref()
                .map(|state| state.completed_turn_count),
            background_stop_requested,
            background_runner_summary: background_runner
                .as_ref()
                .and_then(|state| state.last_summary.clone()),
            background_runner_last_error: background_runner
                .as_ref()
                .and_then(|state| state.last_error.clone()),
            background_recovery_recommendation,
            background_reattach_required: background_reattach_recommendation.is_some(),
            background_reattach_recommendation,
            background_handoff_pending: background_handoff.is_some(),
            background_handoff_ready: background_handoff_disposition
                .is_some_and(BackgroundHandoffDisposition::is_ready),
            background_handoff_status: background_handoff_disposition
                .map(BackgroundHandoffDisposition::as_label)
                .map(str::to_owned),
            background_handoff_target_runner_id: background_handoff
                .as_ref()
                .and_then(|request| request.target_runner_id.clone()),
            background_handoff_requested_at: background_handoff
                .as_ref()
                .map(|request| request.requested_at),
            background_handoff_objective: background_handoff
                .as_ref()
                .map(|request| request.settings.objective.clone()),
            background_handoff_model: background_handoff
                .as_ref()
                .map(|request| request.settings.model.clone()),
            background_handoff_thread_id: background_handoff
                .as_ref()
                .map(|request| request.settings.thread_id.clone()),
            background_handoff_thread_label: background_handoff
                .as_ref()
                .map(|request| request.settings.thread_label.clone()),
            background_handoff_engine_mode: background_handoff
                .as_ref()
                .map(|request| request.settings.engine_mode.as_label().to_string()),
            background_handoff_loop_pause_seconds: background_handoff
                .as_ref()
                .map(|request| request.loop_pause_seconds),
            foreground_thread_id: session_state
                .as_ref()
                .map(|state| state.foreground_thread_id.clone()),
            compaction_count: session_state.as_ref().map(|state| state.compaction_count),
            pending_turn_phase: pending_turn
                .as_ref()
                .map(|checkpoint| format!("{:?}", checkpoint.phase)),
            pending_turn_action: pending_turn
                .as_ref()
                .and_then(|checkpoint| checkpoint.pending_action.as_ref())
                .map(|action| format!("{action:?}")),
            completed_turn_count,
            last_turn_summary,
            last_turn_reply,
            recent_turns,
            recent_turn_replies,
            recent_events: recent_event_lines,
            pending_oauth: pending_oauth.iter().map(build_pending_oauth_view).collect(),
            current_brief: read_text_if_exists(
                self.paths
                    .repo_root
                    .join("offload")
                    .join("current")
                    .join("brief.md"),
            )?,
            current_plan: read_text_if_exists(
                self.paths
                    .repo_root
                    .join("offload")
                    .join("current")
                    .join("plan.md"),
            )?,
            current_open_gaps: read_text_if_exists(
                self.paths
                    .repo_root
                    .join("offload")
                    .join("current")
                    .join("open-gaps.md"),
            )?,
            current_handoff: read_text_if_exists(
                self.paths
                    .repo_root
                    .join("offload")
                    .join("current")
                    .join("handoff.md"),
            )?,
            runtime_grounding_bundle: build_runtime_grounding_bundle(&self.paths)?,
        };
        self.persist_snapshot(&snapshot)?;
        Ok(snapshot)
    }

    async fn auth_preflight(&self) -> anyhow::Result<String> {
        let imported_openclaw_notice = self.maybe_bootstrap_openclaw_codex_oauth().await?;
        let llm = self.build_llm_client()?;
        let report = llm.resume_auth_lifecycle().await?;
        Ok(match imported_openclaw_notice {
            Some(notice) => format!("{notice} | {}", format_auth_preflight_report(&report)),
            None => format_auth_preflight_report(&report),
        })
    }

    async fn import_openclaw_codex_oauth(&self) -> anyhow::Result<String> {
        let Some(plan) = discover_openclaw_import_plan()? else {
            anyhow::bail!(
                "no OpenClaw Codex auth store found; set OPENCLAW_STATE_DIR / OPENCLAW_AGENT_DIR in operator.env or sign in via OpenClaw first"
            );
        };
        let auth_store = self.build_auth_store();
        let (imported_count, default_label, default_source_profile_id) =
            import_openclaw_plan_into_auth_store(&auth_store, &plan).await?;
        let llm = self.build_llm_client()?;
        let report = llm.resume_auth_lifecycle().await?;
        Ok(format!(
            "imported {imported_count} OpenClaw Codex oauth profile{} from {} | default={} | {}",
            if imported_count == 1 { "" } else { "s" },
            plan.source_path.display(),
            format!("{default_label} ({default_source_profile_id})"),
            format_auth_preflight_report(&report)
        ))
    }

    fn launch_codex_cli_login(&self) -> anyhow::Result<String> {
        let command_path = discover_codex_command()
            .context("no Codex CLI found; install Codex or set CODEX_BIN in operator.env")?;
        let launch_target = command_path.display().to_string();
        let args = ["login"];

        #[cfg(target_os = "windows")]
        {
            const CREATE_NEW_CONSOLE: u32 = 0x00000010;
            let script = format!(
                "& '{}' {}",
                launch_target.replace('\'', "''"),
                args.join(" ")
            );
            let child = Command::new("powershell.exe")
                .arg("-NoExit")
                .arg("-NoProfile")
                .arg("-ExecutionPolicy")
                .arg("Bypass")
                .arg("-Command")
                .arg(script)
                .current_dir(&self.paths.repo_root)
                .creation_flags(CREATE_NEW_CONSOLE)
                .spawn()
                .context("launch Codex CLI login terminal")?;
            return Ok(format!(
                "launched Codex CLI login in a new terminal (pid={}) via {} | finish sign-in there, then refresh this shell",
                child.id(),
                launch_target
            ));
        }

        #[cfg(not(target_os = "windows"))]
        {
            let child = Command::new(&command_path)
                .args(args)
                .current_dir(&self.paths.repo_root)
                .spawn()
                .with_context(|| format!("launch {}", command_path.display()))?;
            Ok(format!(
                "launched Codex CLI login (pid={}) via {} | finish sign-in, then refresh this shell",
                child.id(),
                launch_target
            ))
        }
    }

    fn launch_openclaw_codex_login(&self) -> anyhow::Result<String> {
        let command_path = discover_openclaw_command().context(
            "no OpenClaw CLI found; install OpenClaw or set OPENCLAW_BIN in operator.env",
        )?;
        let launch_target = command_path.display().to_string();
        let args = [
            "models",
            "auth",
            "login",
            "--provider",
            "openai-codex",
            "--set-default",
        ];

        #[cfg(target_os = "windows")]
        {
            const CREATE_NEW_CONSOLE: u32 = 0x00000010;
            let script = format!(
                "& '{}' {}",
                launch_target.replace('\'', "''"),
                args.join(" ")
            );
            let child = Command::new("powershell.exe")
                .arg("-NoExit")
                .arg("-NoProfile")
                .arg("-ExecutionPolicy")
                .arg("Bypass")
                .arg("-Command")
                .arg(script)
                .current_dir(&self.paths.repo_root)
                .creation_flags(CREATE_NEW_CONSOLE)
                .spawn()
                .context("launch OpenClaw Codex login terminal")?;
            return Ok(format!(
                "launched OpenClaw Codex login in a new terminal (pid={}) via {} | finish sign-in there, then click Import OpenClaw Codex OAuth",
                child.id(),
                launch_target
            ));
        }

        #[cfg(not(target_os = "windows"))]
        {
            let child = Command::new(&command_path)
                .args(args)
                .current_dir(&self.paths.repo_root)
                .spawn()
                .with_context(|| format!("launch {}", command_path.display()))?;
            Ok(format!(
                "launched OpenClaw Codex login (pid={}) via {} | finish sign-in, then import the resulting OpenClaw auth",
                child.id(),
                launch_target
            ))
        }
    }

    async fn begin_oauth_authorization(
        &self,
        provider: ProviderKind,
        mode: OAuthInitiationMode,
        label: Option<String>,
    ) -> anyhow::Result<PendingOAuthAuthorization> {
        let llm = self.build_llm_client()?;
        llm.begin_oauth_authorization_with_mode(provider, label, mode)
            .await
    }

    async fn complete_oauth_authorization(
        &self,
        pending_id: Uuid,
        callback_input: &str,
    ) -> anyhow::Result<String> {
        let llm = self.build_llm_client()?;
        let profile = llm
            .complete_oauth_authorization(pending_id, callback_input, true)
            .await?;
        Ok(format!(
            "completed browser oauth for {} ({:?})",
            profile.label, profile.provider
        ))
    }

    async fn complete_device_oauth_authorization(
        &self,
        pending_id: Uuid,
    ) -> anyhow::Result<String> {
        let llm = self.build_llm_client()?;
        let profile = llm
            .complete_device_oauth_authorization(pending_id, true)
            .await?;
        Ok(format!(
            "completed device oauth for {} ({:?})",
            profile.label, profile.provider
        ))
    }
}

struct OperatorApp {
    controller: Arc<HarnessController>,
    shell_instance_id: String,
    shell_pid: u32,
    settings: RunSettings,
    engine_mode: OperatorEngineMode,
    auth_provider: OperatorAuthProvider,
    auth_label: String,
    auth_callback_input: String,
    github_target_input: String,
    snapshot: Arc<Mutex<OperatorSnapshot>>,
    running: Arc<AtomicBool>,
    loop_requested: Arc<AtomicBool>,
    refreshing: Arc<AtomicBool>,
    auth_working: Arc<AtomicBool>,
    loop_pause_seconds: f32,
    last_refresh: Instant,
}

impl OperatorApp {
    fn new(controller: Arc<HarnessController>) -> Self {
        Self {
            controller,
            shell_instance_id: Uuid::new_v4().to_string(),
            shell_pid: std::process::id(),
            settings: RunSettings::default(),
            engine_mode: OperatorEngineMode::CodexCli,
            auth_provider: OperatorAuthProvider::OpenAiCodex,
            auth_label: "codex-gui".into(),
            auth_callback_input: String::new(),
            github_target_input: String::new(),
            snapshot: Arc::new(Mutex::new(OperatorSnapshot {
                run_state: "idle".into(),
                run_mode: OperatorRunMode::Idle.as_label().into(),
                summary: "operator ready".into(),
                ..OperatorSnapshot::default()
            })),
            running: Arc::new(AtomicBool::new(false)),
            loop_requested: Arc::new(AtomicBool::new(false)),
            refreshing: Arc::new(AtomicBool::new(false)),
            auth_working: Arc::new(AtomicBool::new(false)),
            loop_pause_seconds: DEFAULT_LOOP_PAUSE_SECONDS,
            last_refresh: Instant::now() - REFRESH_INTERVAL,
        }
    }

    fn selected_engine_ready(&self, snapshot: &OperatorSnapshot) -> bool {
        match self.engine_mode {
            OperatorEngineMode::CodexCli => {
                snapshot.codex_cli_available && snapshot.codex_cli_logged_in
            }
            OperatorEngineMode::NativeHarness => snapshot.native_auth_ready,
        }
    }

    fn background_runner_owned_by_this_shell(&self, snapshot: &OperatorSnapshot) -> bool {
        snapshot.background_runner_active
            && snapshot.background_runner_owner_shell_id.as_deref()
                == Some(self.shell_instance_id.as_str())
            && snapshot.background_runner_owner_shell_pid == Some(self.shell_pid)
    }

    fn background_runner_attached_to_other_live_shell(&self, snapshot: &OperatorSnapshot) -> bool {
        snapshot.background_runner_active
            && !self.background_runner_owned_by_this_shell(snapshot)
            && snapshot.background_runner_owner_shell_alive == Some(true)
    }

    fn background_runner_can_reattach(&self, snapshot: &OperatorSnapshot) -> bool {
        snapshot.background_runner_active
            && !self.background_runner_owned_by_this_shell(snapshot)
            && snapshot.background_runner_owner_shell_alive != Some(true)
    }

    fn spawn_refresh(
        &mut self,
        run_state: String,
        run_mode: OperatorRunMode,
        summary: String,
        last_error: Option<String>,
        completed_turn_count: u64,
        auth_notice: Option<String>,
    ) {
        if self.refreshing.swap(true, Ordering::SeqCst) {
            return;
        }
        let controller = self.controller.clone();
        let snapshot = self.snapshot.clone();
        let refreshing = self.refreshing.clone();
        self.last_refresh = Instant::now();
        let refresh_state = run_state.clone();
        let refresh_mode = run_mode;
        let refresh_summary = summary.clone();
        let refresh_error = last_error.clone();
        let auth_notice_for_refresh = auth_notice.clone();
        std::thread::spawn(move || {
            let next = run_async(move || async move {
                controller
                    .read_snapshot(
                        &refresh_state,
                        refresh_mode,
                        &refresh_summary,
                        refresh_error.clone(),
                        completed_turn_count,
                        auth_notice_for_refresh.clone(),
                    )
                    .await
            })
            .unwrap_or(OperatorSnapshot {
                refreshed_at: Some(Utc::now()),
                run_state,
                run_mode: run_mode.as_label().into(),
                summary,
                last_error: Some(last_error.unwrap_or_else(|| "status refresh failed".into())),
                completed_turn_count,
                auth_notice,
                ..OperatorSnapshot::default()
            });
            *snapshot.lock().expect("operator snapshot poisoned") = next;
            refreshing.store(false, Ordering::SeqCst);
        });
    }

    fn begin_run(&mut self, run_mode: OperatorRunMode) {
        if self.running.swap(true, Ordering::SeqCst) {
            return;
        }
        let loop_requested = matches!(run_mode, OperatorRunMode::Continuous);
        self.loop_requested.store(loop_requested, Ordering::SeqCst);
        let next_snapshot = {
            let mut snapshot = self.snapshot.lock().expect("operator snapshot poisoned");
            snapshot.run_state = if loop_requested {
                "looping".into()
            } else {
                "running".into()
            };
            snapshot.run_mode = run_mode.as_label().into();
            snapshot.summary = if loop_requested {
                "running continuous bounded turns".into()
            } else {
                "running bounded turn".into()
            };
            snapshot.last_error = None;
            if !loop_requested {
                snapshot.completed_turn_count = 0;
            }
            snapshot.clone()
        };
        if let Err(error) = self.controller.persist_snapshot(&next_snapshot) {
            let mut snapshot = self.snapshot.lock().expect("operator snapshot poisoned");
            snapshot.last_error = Some(format!("persist operator status: {error:#}"));
        }
        let controller = self.controller.clone();
        let snapshot = self.snapshot.clone();
        let running = self.running.clone();
        let loop_flag = self.loop_requested.clone();
        let refreshing = self.refreshing.clone();
        let settings = self.settings.clone();
        let pause_duration = Duration::from_secs_f32(self.loop_pause_seconds.max(0.0));
        std::thread::spawn(move || {
            let mut completed_turn_count = 0_u64;
            loop {
                let controller_for_turn = controller.clone();
                let turn_settings = settings.clone();
                let result =
                    run_async(
                        move || async move { controller_for_turn.run_turn(&turn_settings).await },
                    );
                match result {
                    Ok(turn) => {
                        completed_turn_count += 1;
                        let should_continue = should_continue_run_loop(
                            run_mode,
                            loop_flag.load(Ordering::SeqCst),
                            turn.terminal,
                        );
                        if should_continue {
                            let summary = format!(
                                "completed turn {} | {} | next turn in {:.1}s",
                                completed_turn_count,
                                turn.summary,
                                pause_duration.as_secs_f32()
                            );
                            let controller_for_snapshot = controller.clone();
                            let summary_for_snapshot = summary.clone();
                            let next = run_async(move || async move {
                                controller_for_snapshot
                                    .read_snapshot(
                                        "looping",
                                        run_mode,
                                        &summary_for_snapshot,
                                        None,
                                        completed_turn_count,
                                        None,
                                    )
                                    .await
                            })
                            .unwrap_or(OperatorSnapshot {
                                refreshed_at: Some(Utc::now()),
                                run_state: "looping".into(),
                                run_mode: run_mode.as_label().into(),
                                summary,
                                completed_turn_count,
                                ..OperatorSnapshot::default()
                            });
                            *snapshot.lock().expect("operator snapshot poisoned") = next;
                            refreshing.store(false, Ordering::SeqCst);
                            if !sleep_while_requested(&loop_flag, pause_duration) {
                                break;
                            }
                            continue;
                        }

                        let summary = match turn.terminal {
                            OperatorTurnTerminal::Completed => {
                                if matches!(run_mode, OperatorRunMode::Continuous) {
                                    format!(
                                        "loop stopped after {} turn(s) | {}",
                                        completed_turn_count, turn.summary
                                    )
                                } else {
                                    turn.summary
                                }
                            }
                            OperatorTurnTerminal::SurfacedGap => format!(
                                "loop stopped on surfaced gap after {} turn(s) | {}",
                                completed_turn_count, turn.summary
                            ),
                            OperatorTurnTerminal::GithubActionRequested => format!(
                                "loop stopped on supervised GitHub request after {} turn(s) | {}",
                                completed_turn_count, turn.summary
                            ),
                        };
                        let controller_for_snapshot = controller.clone();
                        let summary_for_snapshot = summary.clone();
                        let next = run_async(move || async move {
                            controller_for_snapshot
                                .read_snapshot(
                                    "idle",
                                    OperatorRunMode::Idle,
                                    &summary_for_snapshot,
                                    None,
                                    completed_turn_count,
                                    None,
                                )
                                .await
                        })
                        .unwrap_or(OperatorSnapshot {
                            refreshed_at: Some(Utc::now()),
                            run_state: "idle".into(),
                            run_mode: OperatorRunMode::Idle.as_label().into(),
                            summary,
                            completed_turn_count,
                            ..OperatorSnapshot::default()
                        });
                        *snapshot.lock().expect("operator snapshot poisoned") = next;
                        break;
                    }
                    Err(error) => {
                        let last_error = Some(format!("{error:#}"));
                        let summary = if matches!(run_mode, OperatorRunMode::Continuous) {
                            format!(
                                "continuous run failed after {} turn(s)",
                                completed_turn_count
                            )
                        } else {
                            "runtime turn failed".into()
                        };
                        let controller_for_snapshot = controller.clone();
                        let refresh_error = last_error.clone();
                        let summary_for_snapshot = summary.clone();
                        let next = run_async(move || async move {
                            controller_for_snapshot
                                .read_snapshot(
                                    "error",
                                    OperatorRunMode::Idle,
                                    &summary_for_snapshot,
                                    refresh_error.clone(),
                                    completed_turn_count,
                                    None,
                                )
                                .await
                        })
                        .unwrap_or(OperatorSnapshot {
                            refreshed_at: Some(Utc::now()),
                            run_state: "error".into(),
                            run_mode: OperatorRunMode::Idle.as_label().into(),
                            summary,
                            last_error,
                            completed_turn_count,
                            ..OperatorSnapshot::default()
                        });
                        *snapshot.lock().expect("operator snapshot poisoned") = next;
                        break;
                    }
                }
            }
            loop_flag.store(false, Ordering::SeqCst);
            refreshing.store(false, Ordering::SeqCst);
            running.store(false, Ordering::SeqCst);
        });
    }

    fn stop_loop(&self) {
        self.loop_requested.store(false, Ordering::SeqCst);
    }

    fn background_settings_match_form(&self, snapshot: &OperatorSnapshot) -> bool {
        snapshot.background_runner_objective.as_deref() == Some(self.settings.objective.as_str())
            && snapshot.background_runner_model.as_deref() == Some(self.settings.model.as_str())
            && snapshot.background_runner_thread_id.as_deref()
                == Some(self.settings.thread_id.as_str())
            && snapshot.background_runner_thread_label.as_deref()
                == Some(self.settings.thread_label.as_str())
            && snapshot.background_runner_engine_mode.as_deref()
                == Some(self.settings.engine_mode.as_label())
            && snapshot
                .background_runner_loop_pause_seconds
                .map(|value| (value - self.loop_pause_seconds).abs() < 0.05)
                .unwrap_or(false)
    }

    fn adopt_background_settings(&mut self, snapshot: &OperatorSnapshot) {
        if let Some(objective) = snapshot.background_runner_objective.as_deref() {
            self.settings.objective = objective.to_string();
        }
        if let Some(model) = snapshot.background_runner_model.as_deref() {
            self.settings.model = model.to_string();
        }
        if let Some(thread_id) = snapshot.background_runner_thread_id.as_deref() {
            self.settings.thread_id = thread_id.to_string();
        }
        if let Some(thread_label) = snapshot.background_runner_thread_label.as_deref() {
            self.settings.thread_label = thread_label.to_string();
        }
        if let Some(engine_mode) = snapshot.background_runner_engine_mode.as_deref() {
            self.settings.engine_mode = match engine_mode {
                "Codex CLI" => OperatorEngineMode::CodexCli,
                _ => OperatorEngineMode::NativeHarness,
            };
            self.engine_mode = self.settings.engine_mode;
        }
        if let Some(loop_pause_seconds) = snapshot.background_runner_loop_pause_seconds {
            self.loop_pause_seconds = loop_pause_seconds;
        }
        let runner_id = snapshot
            .background_runner_id
            .as_deref()
            .unwrap_or("unknown");
        let mut next_snapshot = snapshot.clone();
        next_snapshot.auth_notice = Some(format!(
            "adopted launch settings from background runner {runner_id}"
        ));
        if let Err(error) = self.controller.persist_snapshot(&next_snapshot) {
            next_snapshot.last_error = Some(format!("persist operator status: {error:#}"));
        }
        *self.snapshot.lock().expect("operator snapshot poisoned") = next_snapshot;
    }

    fn handoff_settings_match_form(&self, snapshot: &OperatorSnapshot) -> bool {
        snapshot.background_handoff_objective.as_deref() == Some(self.settings.objective.as_str())
            && snapshot.background_handoff_model.as_deref() == Some(self.settings.model.as_str())
            && snapshot.background_handoff_thread_id.as_deref()
                == Some(self.settings.thread_id.as_str())
            && snapshot.background_handoff_thread_label.as_deref()
                == Some(self.settings.thread_label.as_str())
            && snapshot.background_handoff_engine_mode.as_deref()
                == Some(self.settings.engine_mode.as_label())
            && snapshot
                .background_handoff_loop_pause_seconds
                .map(|value| (value - self.loop_pause_seconds).abs() < 0.05)
                .unwrap_or(false)
    }

    fn adopt_background_handoff(&mut self, snapshot: &OperatorSnapshot) {
        if let Some(objective) = snapshot.background_handoff_objective.as_deref() {
            self.settings.objective = objective.to_string();
        }
        if let Some(model) = snapshot.background_handoff_model.as_deref() {
            self.settings.model = model.to_string();
        }
        if let Some(thread_id) = snapshot.background_handoff_thread_id.as_deref() {
            self.settings.thread_id = thread_id.to_string();
        }
        if let Some(thread_label) = snapshot.background_handoff_thread_label.as_deref() {
            self.settings.thread_label = thread_label.to_string();
        }
        if let Some(engine_mode) = snapshot.background_handoff_engine_mode.as_deref() {
            self.settings.engine_mode = match engine_mode {
                "Codex CLI" => OperatorEngineMode::CodexCli,
                _ => OperatorEngineMode::NativeHarness,
            };
            self.engine_mode = self.settings.engine_mode;
        }
        if let Some(loop_pause_seconds) = snapshot.background_handoff_loop_pause_seconds {
            self.loop_pause_seconds = loop_pause_seconds;
        }
        let runner_id = snapshot
            .background_handoff_target_runner_id
            .as_deref()
            .unwrap_or("unknown");
        let mut next_snapshot = snapshot.clone();
        next_snapshot.auth_notice = Some(format!(
            "adopted requested replacement settings for background runner {runner_id}"
        ));
        if let Err(error) = self.controller.persist_snapshot(&next_snapshot) {
            next_snapshot.last_error = Some(format!("persist operator status: {error:#}"));
        }
        *self.snapshot.lock().expect("operator snapshot poisoned") = next_snapshot;
    }

    fn spawn_background_loop(&mut self) {
        if self.auth_working.swap(true, Ordering::SeqCst) {
            return;
        }
        let controller = self.controller.clone();
        let shell_instance_id = self.shell_instance_id.clone();
        let shell_pid = self.shell_pid;
        let snapshot = self.snapshot.clone();
        let auth_working = self.auth_working.clone();
        let settings = self.settings.clone();
        let loop_pause_seconds = self.loop_pause_seconds.max(0.0);
        std::thread::spawn(move || {
            let current = snapshot.lock().expect("operator snapshot poisoned").clone();
            let result = controller.spawn_background_loop_process(
                &settings,
                loop_pause_seconds,
                shell_instance_id.as_str(),
                shell_pid,
            );
            let auth_notice = match result {
                Ok(background) => {
                    let _ = controller.clear_background_handoff_request();
                    Some(format!(
                        "started background loop pid={} id={} with {:.1}s pause",
                        background.pid, background.runner_id, loop_pause_seconds
                    ))
                }
                Err(error) => Some(format!("background launch failed: {error:#}")),
            };
            let current_for_snapshot = current.clone();
            let auth_notice_for_snapshot = auth_notice.clone();
            let controller_for_snapshot = controller.clone();
            let next = run_async(move || async move {
                controller_for_snapshot
                    .read_snapshot(
                        &current_for_snapshot.run_state,
                        parse_run_mode(current_for_snapshot.run_mode.as_str()),
                        &current_for_snapshot.summary,
                        current_for_snapshot.last_error.clone(),
                        current_for_snapshot.completed_turn_count,
                        auth_notice_for_snapshot.clone(),
                    )
                    .await
            })
            .unwrap_or(OperatorSnapshot {
                refreshed_at: Some(Utc::now()),
                run_state: current.run_state,
                run_mode: current.run_mode,
                summary: current.summary,
                last_error: current.last_error,
                auth_notice,
                background_runner_active: current.background_runner_active,
                background_runner_status: current.background_runner_status,
                background_runner_id: current.background_runner_id,
                background_runner_pid: current.background_runner_pid,
                background_runner_phase: current.background_runner_phase,
                background_runner_started_at: current.background_runner_started_at,
                background_runner_updated_at: current.background_runner_updated_at,
                background_runner_thread_id: current.background_runner_thread_id,
                background_runner_thread_label: current.background_runner_thread_label,
                background_runner_model: current.background_runner_model,
                background_runner_objective: current.background_runner_objective,
                background_runner_loop_pause_seconds: current.background_runner_loop_pause_seconds,
                background_runner_turn_count: current.background_runner_turn_count,
                background_stop_requested: current.background_stop_requested,
                background_runner_summary: current.background_runner_summary,
                background_runner_last_error: current.background_runner_last_error,
                background_recovery_recommendation: current.background_recovery_recommendation,
                background_handoff_pending: current.background_handoff_pending,
                background_handoff_ready: current.background_handoff_ready,
                background_handoff_status: current.background_handoff_status,
                background_handoff_target_runner_id: current.background_handoff_target_runner_id,
                background_handoff_requested_at: current.background_handoff_requested_at,
                background_handoff_objective: current.background_handoff_objective,
                background_handoff_model: current.background_handoff_model,
                background_handoff_thread_id: current.background_handoff_thread_id,
                background_handoff_thread_label: current.background_handoff_thread_label,
                background_handoff_loop_pause_seconds: current
                    .background_handoff_loop_pause_seconds,
                completed_turn_count: current.completed_turn_count,
                ..OperatorSnapshot::default()
            });
            *snapshot.lock().expect("operator snapshot poisoned") = next;
            auth_working.store(false, Ordering::SeqCst);
        });
    }

    fn spawn_reattach_background_runner(&mut self) {
        if self.auth_working.swap(true, Ordering::SeqCst) {
            return;
        }
        let controller = self.controller.clone();
        let shell_instance_id = self.shell_instance_id.clone();
        let shell_pid = self.shell_pid;
        let snapshot = self.snapshot.clone();
        let auth_working = self.auth_working.clone();
        std::thread::spawn(move || {
            let current = snapshot.lock().expect("operator snapshot poisoned").clone();
            let result = current
                .background_runner_id
                .as_deref()
                .context("no detached background runner is currently recorded")
                .and_then(|runner_id| {
                    controller.attach_background_runner(
                        runner_id,
                        shell_instance_id.as_str(),
                        shell_pid,
                    )
                });
            let auth_notice = match result {
                Ok(summary) => Some(summary),
                Err(error) => Some(format!("background reattach failed: {error:#}")),
            };
            let current_for_snapshot = current.clone();
            let auth_notice_for_snapshot = auth_notice.clone();
            let controller_for_snapshot = controller.clone();
            let next = run_async(move || async move {
                controller_for_snapshot
                    .read_snapshot(
                        &current_for_snapshot.run_state,
                        parse_run_mode(current_for_snapshot.run_mode.as_str()),
                        &current_for_snapshot.summary,
                        current_for_snapshot.last_error.clone(),
                        current_for_snapshot.completed_turn_count,
                        auth_notice_for_snapshot.clone(),
                    )
                    .await
            })
            .unwrap_or(OperatorSnapshot {
                refreshed_at: Some(Utc::now()),
                run_state: current.run_state,
                run_mode: current.run_mode,
                summary: current.summary,
                last_error: current.last_error,
                auth_notice,
                background_runner_active: current.background_runner_active,
                background_runner_status: current.background_runner_status,
                background_runner_id: current.background_runner_id,
                background_runner_pid: current.background_runner_pid,
                background_runner_owner_shell_id: current.background_runner_owner_shell_id,
                background_runner_owner_shell_pid: current.background_runner_owner_shell_pid,
                background_runner_owner_shell_alive: current.background_runner_owner_shell_alive,
                background_runner_phase: current.background_runner_phase,
                background_runner_started_at: current.background_runner_started_at,
                background_runner_updated_at: current.background_runner_updated_at,
                background_runner_thread_id: current.background_runner_thread_id,
                background_runner_thread_label: current.background_runner_thread_label,
                background_runner_model: current.background_runner_model,
                background_runner_objective: current.background_runner_objective,
                background_runner_loop_pause_seconds: current.background_runner_loop_pause_seconds,
                background_runner_turn_count: current.background_runner_turn_count,
                background_stop_requested: current.background_stop_requested,
                background_runner_summary: current.background_runner_summary,
                background_runner_last_error: current.background_runner_last_error,
                background_recovery_recommendation: current.background_recovery_recommendation,
                background_reattach_required: current.background_reattach_required,
                background_reattach_recommendation: current.background_reattach_recommendation,
                background_handoff_pending: current.background_handoff_pending,
                background_handoff_ready: current.background_handoff_ready,
                background_handoff_status: current.background_handoff_status,
                background_handoff_target_runner_id: current.background_handoff_target_runner_id,
                background_handoff_requested_at: current.background_handoff_requested_at,
                background_handoff_objective: current.background_handoff_objective,
                background_handoff_model: current.background_handoff_model,
                background_handoff_thread_id: current.background_handoff_thread_id,
                background_handoff_thread_label: current.background_handoff_thread_label,
                background_handoff_loop_pause_seconds: current
                    .background_handoff_loop_pause_seconds,
                completed_turn_count: current.completed_turn_count,
                ..OperatorSnapshot::default()
            });
            *snapshot.lock().expect("operator snapshot poisoned") = next;
            auth_working.store(false, Ordering::SeqCst);
        });
    }

    fn spawn_request_background_handoff(&mut self) {
        if self.auth_working.swap(true, Ordering::SeqCst) {
            return;
        }
        let controller = self.controller.clone();
        let snapshot = self.snapshot.clone();
        let auth_working = self.auth_working.clone();
        let settings = self.settings.clone();
        let loop_pause_seconds = self.loop_pause_seconds.max(0.0);
        std::thread::spawn(move || {
            let current = snapshot.lock().expect("operator snapshot poisoned").clone();
            let result = if current.background_runner_active {
                controller
                    .request_background_handoff(
                        current.background_runner_id.as_deref(),
                        &settings,
                        loop_pause_seconds,
                    )
                    .and_then(|_| {
                        controller.request_background_stop(current.background_runner_id.as_deref())
                    })
                    .map(|_| {
                        let runner_id = current
                            .background_runner_id
                            .as_deref()
                            .unwrap_or("unknown");
                        format!(
                            "requested background handoff from {runner_id} using launch-form settings"
                        )
                    })
            } else {
                Err(anyhow!(
                    "no live background runner is available to hand off"
                ))
            };
            let auth_notice = match result {
                Ok(summary) => Some(summary),
                Err(error) => Some(format!("background handoff request failed: {error:#}")),
            };
            let current_for_snapshot = current.clone();
            let auth_notice_for_snapshot = auth_notice.clone();
            let controller_for_snapshot = controller.clone();
            let next = run_async(move || async move {
                controller_for_snapshot
                    .read_snapshot(
                        &current_for_snapshot.run_state,
                        parse_run_mode(current_for_snapshot.run_mode.as_str()),
                        &current_for_snapshot.summary,
                        current_for_snapshot.last_error.clone(),
                        current_for_snapshot.completed_turn_count,
                        auth_notice_for_snapshot.clone(),
                    )
                    .await
            })
            .unwrap_or(OperatorSnapshot {
                refreshed_at: Some(Utc::now()),
                run_state: current.run_state,
                run_mode: current.run_mode,
                summary: current.summary,
                last_error: current.last_error,
                auth_notice,
                background_runner_active: current.background_runner_active,
                background_runner_status: current.background_runner_status,
                background_runner_id: current.background_runner_id,
                background_runner_pid: current.background_runner_pid,
                background_runner_phase: current.background_runner_phase,
                background_runner_started_at: current.background_runner_started_at,
                background_runner_updated_at: current.background_runner_updated_at,
                background_runner_thread_id: current.background_runner_thread_id,
                background_runner_thread_label: current.background_runner_thread_label,
                background_runner_model: current.background_runner_model,
                background_runner_objective: current.background_runner_objective,
                background_runner_loop_pause_seconds: current.background_runner_loop_pause_seconds,
                background_runner_turn_count: current.background_runner_turn_count,
                background_stop_requested: current.background_stop_requested,
                background_runner_summary: current.background_runner_summary,
                background_runner_last_error: current.background_runner_last_error,
                background_recovery_recommendation: current.background_recovery_recommendation,
                background_handoff_pending: current.background_handoff_pending,
                background_handoff_ready: current.background_handoff_ready,
                background_handoff_status: current.background_handoff_status,
                background_handoff_target_runner_id: current.background_handoff_target_runner_id,
                background_handoff_requested_at: current.background_handoff_requested_at,
                background_handoff_objective: current.background_handoff_objective,
                background_handoff_model: current.background_handoff_model,
                background_handoff_thread_id: current.background_handoff_thread_id,
                background_handoff_thread_label: current.background_handoff_thread_label,
                background_handoff_loop_pause_seconds: current
                    .background_handoff_loop_pause_seconds,
                completed_turn_count: current.completed_turn_count,
                ..OperatorSnapshot::default()
            });
            *snapshot.lock().expect("operator snapshot poisoned") = next;
            auth_working.store(false, Ordering::SeqCst);
        });
    }

    fn spawn_complete_background_handoff(&mut self) {
        if self.auth_working.swap(true, Ordering::SeqCst) {
            return;
        }
        let controller = self.controller.clone();
        let shell_instance_id = self.shell_instance_id.clone();
        let shell_pid = self.shell_pid;
        let snapshot = self.snapshot.clone();
        let auth_working = self.auth_working.clone();
        std::thread::spawn(move || {
            let current = snapshot.lock().expect("operator snapshot poisoned").clone();
            let result = controller
                .read_background_handoff_request()
                .and_then(|request| {
                    let request = request.context("no pending background handoff request")?;
                    if !current.background_handoff_ready {
                        return Err(anyhow!(
                            "background handoff is not ready while the targeted runner is still active"
                        ));
                    }
                    controller
                        .spawn_background_loop_process(
                            &request.settings,
                            request.loop_pause_seconds.max(0.0),
                            shell_instance_id.as_str(),
                            shell_pid,
                        )
                        .and_then(|background| {
                            controller.clear_background_handoff_request()?;
                            Ok((request, background))
                        })
                })
                .map(|(request, background)| {
                    let target = request
                        .target_runner_id
                        .as_deref()
                        .unwrap_or("unknown");
                    format!(
                        "completed background handoff from {target} with replacement id={} pid={}",
                        background.runner_id, background.pid
                    )
                });
            let auth_notice = match result {
                Ok(summary) => Some(summary),
                Err(error) => Some(format!("background handoff completion failed: {error:#}")),
            };
            let current_for_snapshot = current.clone();
            let auth_notice_for_snapshot = auth_notice.clone();
            let controller_for_snapshot = controller.clone();
            let next = run_async(move || async move {
                controller_for_snapshot
                    .read_snapshot(
                        &current_for_snapshot.run_state,
                        parse_run_mode(current_for_snapshot.run_mode.as_str()),
                        &current_for_snapshot.summary,
                        current_for_snapshot.last_error.clone(),
                        current_for_snapshot.completed_turn_count,
                        auth_notice_for_snapshot.clone(),
                    )
                    .await
            })
            .unwrap_or(OperatorSnapshot {
                refreshed_at: Some(Utc::now()),
                run_state: current.run_state,
                run_mode: current.run_mode,
                summary: current.summary,
                last_error: current.last_error,
                auth_notice,
                background_runner_active: current.background_runner_active,
                background_runner_status: current.background_runner_status,
                background_runner_id: current.background_runner_id,
                background_runner_pid: current.background_runner_pid,
                background_runner_phase: current.background_runner_phase,
                background_runner_started_at: current.background_runner_started_at,
                background_runner_updated_at: current.background_runner_updated_at,
                background_runner_thread_id: current.background_runner_thread_id,
                background_runner_thread_label: current.background_runner_thread_label,
                background_runner_model: current.background_runner_model,
                background_runner_objective: current.background_runner_objective,
                background_runner_loop_pause_seconds: current.background_runner_loop_pause_seconds,
                background_runner_turn_count: current.background_runner_turn_count,
                background_stop_requested: current.background_stop_requested,
                background_runner_summary: current.background_runner_summary,
                background_runner_last_error: current.background_runner_last_error,
                background_recovery_recommendation: current.background_recovery_recommendation,
                background_handoff_pending: current.background_handoff_pending,
                background_handoff_ready: current.background_handoff_ready,
                background_handoff_status: current.background_handoff_status,
                background_handoff_target_runner_id: current.background_handoff_target_runner_id,
                background_handoff_requested_at: current.background_handoff_requested_at,
                background_handoff_objective: current.background_handoff_objective,
                background_handoff_model: current.background_handoff_model,
                background_handoff_thread_id: current.background_handoff_thread_id,
                background_handoff_thread_label: current.background_handoff_thread_label,
                background_handoff_loop_pause_seconds: current
                    .background_handoff_loop_pause_seconds,
                completed_turn_count: current.completed_turn_count,
                ..OperatorSnapshot::default()
            });
            *snapshot.lock().expect("operator snapshot poisoned") = next;
            auth_working.store(false, Ordering::SeqCst);
        });
    }

    fn spawn_clear_background_handoff(&mut self) {
        if self.auth_working.swap(true, Ordering::SeqCst) {
            return;
        }
        let controller = self.controller.clone();
        let snapshot = self.snapshot.clone();
        let auth_working = self.auth_working.clone();
        std::thread::spawn(move || {
            let current = snapshot.lock().expect("operator snapshot poisoned").clone();
            let result = controller
                .clear_background_handoff_request()
                .map(|_| "cleared pending background handoff".to_string());
            let auth_notice = match result {
                Ok(summary) => Some(summary),
                Err(error) => Some(format!("clear background handoff failed: {error:#}")),
            };
            let current_for_snapshot = current.clone();
            let auth_notice_for_snapshot = auth_notice.clone();
            let controller_for_snapshot = controller.clone();
            let next = run_async(move || async move {
                controller_for_snapshot
                    .read_snapshot(
                        &current_for_snapshot.run_state,
                        parse_run_mode(current_for_snapshot.run_mode.as_str()),
                        &current_for_snapshot.summary,
                        current_for_snapshot.last_error.clone(),
                        current_for_snapshot.completed_turn_count,
                        auth_notice_for_snapshot.clone(),
                    )
                    .await
            })
            .unwrap_or(OperatorSnapshot {
                refreshed_at: Some(Utc::now()),
                run_state: current.run_state,
                run_mode: current.run_mode,
                summary: current.summary,
                last_error: current.last_error,
                auth_notice,
                background_runner_active: current.background_runner_active,
                background_runner_status: current.background_runner_status,
                background_runner_id: current.background_runner_id,
                background_runner_pid: current.background_runner_pid,
                background_runner_phase: current.background_runner_phase,
                background_runner_started_at: current.background_runner_started_at,
                background_runner_updated_at: current.background_runner_updated_at,
                background_runner_thread_id: current.background_runner_thread_id,
                background_runner_thread_label: current.background_runner_thread_label,
                background_runner_model: current.background_runner_model,
                background_runner_objective: current.background_runner_objective,
                background_runner_loop_pause_seconds: current.background_runner_loop_pause_seconds,
                background_runner_turn_count: current.background_runner_turn_count,
                background_stop_requested: current.background_stop_requested,
                background_runner_summary: current.background_runner_summary,
                background_runner_last_error: current.background_runner_last_error,
                background_recovery_recommendation: current.background_recovery_recommendation,
                background_handoff_pending: current.background_handoff_pending,
                background_handoff_ready: current.background_handoff_ready,
                background_handoff_status: current.background_handoff_status,
                background_handoff_target_runner_id: current.background_handoff_target_runner_id,
                background_handoff_requested_at: current.background_handoff_requested_at,
                background_handoff_objective: current.background_handoff_objective,
                background_handoff_model: current.background_handoff_model,
                background_handoff_thread_id: current.background_handoff_thread_id,
                background_handoff_thread_label: current.background_handoff_thread_label,
                background_handoff_loop_pause_seconds: current
                    .background_handoff_loop_pause_seconds,
                completed_turn_count: current.completed_turn_count,
                ..OperatorSnapshot::default()
            });
            *snapshot.lock().expect("operator snapshot poisoned") = next;
            auth_working.store(false, Ordering::SeqCst);
        });
    }

    fn spawn_apply_github_action_request(&mut self) {
        if self.auth_working.swap(true, Ordering::SeqCst) {
            return;
        }
        let github_target_input = self.github_target_input.trim().to_string();
        let controller = self.controller.clone();
        let snapshot = self.snapshot.clone();
        let auth_working = self.auth_working.clone();
        std::thread::spawn(move || {
            let current = snapshot.lock().expect("operator snapshot poisoned").clone();
            let result = controller.apply_github_action_request(Some(github_target_input));
            let (summary, last_error) = match result {
                Ok(summary) => (summary, None),
                Err(error) => (
                    current.summary.clone(),
                    Some(format!("apply GitHub request failed: {error:#}")),
                ),
            };
            let current_for_snapshot = current.clone();
            let summary_for_snapshot = summary.clone();
            let last_error_for_snapshot = last_error.clone();
            let controller_for_snapshot = controller.clone();
            let next = run_async(move || async move {
                controller_for_snapshot
                    .read_snapshot(
                        &current_for_snapshot.run_state,
                        parse_run_mode(current_for_snapshot.run_mode.as_str()),
                        &summary_for_snapshot,
                        last_error_for_snapshot.clone(),
                        current_for_snapshot.completed_turn_count,
                        None,
                    )
                    .await
            })
            .unwrap_or(OperatorSnapshot {
                refreshed_at: Some(Utc::now()),
                run_state: current.run_state,
                run_mode: current.run_mode,
                summary,
                last_error,
                completed_turn_count: current.completed_turn_count,
                ..OperatorSnapshot::default()
            });
            *snapshot.lock().expect("operator snapshot poisoned") = next;
            auth_working.store(false, Ordering::SeqCst);
        });
    }

    fn spawn_clear_github_action_request(&mut self) {
        if self.auth_working.swap(true, Ordering::SeqCst) {
            return;
        }
        let controller = self.controller.clone();
        let snapshot = self.snapshot.clone();
        let auth_working = self.auth_working.clone();
        std::thread::spawn(move || {
            let current = snapshot.lock().expect("operator snapshot poisoned").clone();
            let result = controller.clear_github_action_request();
            let (summary, last_error) = match result {
                Ok(summary) => (summary, None),
                Err(error) => (
                    current.summary.clone(),
                    Some(format!("clear GitHub request failed: {error:#}")),
                ),
            };
            let current_for_snapshot = current.clone();
            let summary_for_snapshot = summary.clone();
            let last_error_for_snapshot = last_error.clone();
            let controller_for_snapshot = controller.clone();
            let next = run_async(move || async move {
                controller_for_snapshot
                    .read_snapshot(
                        &current_for_snapshot.run_state,
                        parse_run_mode(current_for_snapshot.run_mode.as_str()),
                        &summary_for_snapshot,
                        last_error_for_snapshot.clone(),
                        current_for_snapshot.completed_turn_count,
                        None,
                    )
                    .await
            })
            .unwrap_or(OperatorSnapshot {
                refreshed_at: Some(Utc::now()),
                run_state: current.run_state,
                run_mode: current.run_mode,
                summary,
                last_error,
                completed_turn_count: current.completed_turn_count,
                ..OperatorSnapshot::default()
            });
            *snapshot.lock().expect("operator snapshot poisoned") = next;
            auth_working.store(false, Ordering::SeqCst);
        });
    }

    fn spawn_reject_github_action_request(&mut self) {
        if self.auth_working.swap(true, Ordering::SeqCst) {
            return;
        }
        let controller = self.controller.clone();
        let snapshot = self.snapshot.clone();
        let auth_working = self.auth_working.clone();
        std::thread::spawn(move || {
            let current = snapshot.lock().expect("operator snapshot poisoned").clone();
            let result = controller.reject_github_action_request();
            let (summary, last_error) = match result {
                Ok(summary) => (summary, None),
                Err(error) => (
                    current.summary.clone(),
                    Some(format!("reject GitHub request failed: {error:#}")),
                ),
            };
            let current_for_snapshot = current.clone();
            let summary_for_snapshot = summary.clone();
            let last_error_for_snapshot = last_error.clone();
            let controller_for_snapshot = controller.clone();
            let next = run_async(move || async move {
                controller_for_snapshot
                    .read_snapshot(
                        &current_for_snapshot.run_state,
                        parse_run_mode(current_for_snapshot.run_mode.as_str()),
                        &summary_for_snapshot,
                        last_error_for_snapshot.clone(),
                        current_for_snapshot.completed_turn_count,
                        None,
                    )
                    .await
            })
            .unwrap_or(OperatorSnapshot {
                refreshed_at: Some(Utc::now()),
                run_state: current.run_state,
                run_mode: current.run_mode,
                summary,
                last_error,
                completed_turn_count: current.completed_turn_count,
                ..OperatorSnapshot::default()
            });
            *snapshot.lock().expect("operator snapshot poisoned") = next;
            auth_working.store(false, Ordering::SeqCst);
        });
    }

    fn spawn_stop_background_loop(&mut self) {
        if self.auth_working.swap(true, Ordering::SeqCst) {
            return;
        }
        let controller = self.controller.clone();
        let snapshot = self.snapshot.clone();
        let auth_working = self.auth_working.clone();
        std::thread::spawn(move || {
            let current = snapshot.lock().expect("operator snapshot poisoned").clone();
            let result = controller
                .request_background_stop(current.background_runner_id.as_deref())
                .map(|_| match current.background_runner_id.as_deref() {
                    Some(runner_id) => format!("requested background loop stop for {runner_id}"),
                    None => "requested background loop stop".to_string(),
                });
            let auth_notice = match result {
                Ok(summary) => Some(summary),
                Err(error) => Some(format!("background stop request failed: {error:#}")),
            };
            let current_for_snapshot = current.clone();
            let auth_notice_for_snapshot = auth_notice.clone();
            let controller_for_snapshot = controller.clone();
            let next = run_async(move || async move {
                controller_for_snapshot
                    .read_snapshot(
                        &current_for_snapshot.run_state,
                        parse_run_mode(current_for_snapshot.run_mode.as_str()),
                        &current_for_snapshot.summary,
                        current_for_snapshot.last_error.clone(),
                        current_for_snapshot.completed_turn_count,
                        auth_notice_for_snapshot.clone(),
                    )
                    .await
            })
            .unwrap_or(OperatorSnapshot {
                refreshed_at: Some(Utc::now()),
                run_state: current.run_state,
                run_mode: current.run_mode,
                summary: current.summary,
                last_error: current.last_error,
                auth_notice,
                background_runner_active: current.background_runner_active,
                background_runner_status: current.background_runner_status,
                background_runner_id: current.background_runner_id,
                background_runner_pid: current.background_runner_pid,
                background_runner_phase: current.background_runner_phase,
                background_runner_started_at: current.background_runner_started_at,
                background_runner_updated_at: current.background_runner_updated_at,
                background_runner_thread_id: current.background_runner_thread_id,
                background_runner_thread_label: current.background_runner_thread_label,
                background_runner_model: current.background_runner_model,
                background_runner_objective: current.background_runner_objective,
                background_runner_loop_pause_seconds: current.background_runner_loop_pause_seconds,
                background_runner_turn_count: current.background_runner_turn_count,
                background_stop_requested: current.background_stop_requested,
                background_runner_summary: current.background_runner_summary,
                background_runner_last_error: current.background_runner_last_error,
                background_recovery_recommendation: current.background_recovery_recommendation,
                background_handoff_pending: current.background_handoff_pending,
                background_handoff_ready: current.background_handoff_ready,
                background_handoff_status: current.background_handoff_status,
                background_handoff_target_runner_id: current.background_handoff_target_runner_id,
                background_handoff_requested_at: current.background_handoff_requested_at,
                background_handoff_objective: current.background_handoff_objective,
                background_handoff_model: current.background_handoff_model,
                background_handoff_thread_id: current.background_handoff_thread_id,
                background_handoff_thread_label: current.background_handoff_thread_label,
                background_handoff_loop_pause_seconds: current
                    .background_handoff_loop_pause_seconds,
                completed_turn_count: current.completed_turn_count,
                ..OperatorSnapshot::default()
            });
            *snapshot.lock().expect("operator snapshot poisoned") = next;
            auth_working.store(false, Ordering::SeqCst);
        });
    }

    fn spawn_clear_background_state(&mut self) {
        if self.auth_working.swap(true, Ordering::SeqCst) {
            return;
        }
        let controller = self.controller.clone();
        let snapshot = self.snapshot.clone();
        let auth_working = self.auth_working.clone();
        std::thread::spawn(move || {
            let current = snapshot.lock().expect("operator snapshot poisoned").clone();
            let result = controller
                .clear_background_runner_state()
                .map(|_| "cleared background runner state".to_string());
            let auth_notice = match result {
                Ok(summary) => Some(summary),
                Err(error) => Some(format!("clear background state failed: {error:#}")),
            };
            let current_for_snapshot = current.clone();
            let auth_notice_for_snapshot = auth_notice.clone();
            let controller_for_snapshot = controller.clone();
            let next = run_async(move || async move {
                controller_for_snapshot
                    .read_snapshot(
                        &current_for_snapshot.run_state,
                        parse_run_mode(current_for_snapshot.run_mode.as_str()),
                        &current_for_snapshot.summary,
                        current_for_snapshot.last_error.clone(),
                        current_for_snapshot.completed_turn_count,
                        auth_notice_for_snapshot.clone(),
                    )
                    .await
            })
            .unwrap_or(OperatorSnapshot {
                refreshed_at: Some(Utc::now()),
                run_state: current.run_state,
                run_mode: current.run_mode,
                summary: current.summary,
                last_error: current.last_error,
                auth_notice,
                background_runner_active: current.background_runner_active,
                background_runner_status: current.background_runner_status,
                background_runner_id: current.background_runner_id,
                background_runner_pid: current.background_runner_pid,
                background_runner_phase: current.background_runner_phase,
                background_runner_started_at: current.background_runner_started_at,
                background_runner_updated_at: current.background_runner_updated_at,
                background_runner_thread_id: current.background_runner_thread_id,
                background_runner_thread_label: current.background_runner_thread_label,
                background_runner_model: current.background_runner_model,
                background_runner_objective: current.background_runner_objective,
                background_runner_loop_pause_seconds: current.background_runner_loop_pause_seconds,
                background_runner_turn_count: current.background_runner_turn_count,
                background_stop_requested: current.background_stop_requested,
                background_runner_summary: current.background_runner_summary,
                background_runner_last_error: current.background_runner_last_error,
                background_recovery_recommendation: current.background_recovery_recommendation,
                background_handoff_pending: current.background_handoff_pending,
                background_handoff_ready: current.background_handoff_ready,
                background_handoff_status: current.background_handoff_status,
                background_handoff_target_runner_id: current.background_handoff_target_runner_id,
                background_handoff_requested_at: current.background_handoff_requested_at,
                background_handoff_objective: current.background_handoff_objective,
                background_handoff_model: current.background_handoff_model,
                background_handoff_thread_id: current.background_handoff_thread_id,
                background_handoff_thread_label: current.background_handoff_thread_label,
                background_handoff_loop_pause_seconds: current
                    .background_handoff_loop_pause_seconds,
                completed_turn_count: current.completed_turn_count,
                ..OperatorSnapshot::default()
            });
            *snapshot.lock().expect("operator snapshot poisoned") = next;
            auth_working.store(false, Ordering::SeqCst);
        });
    }

    fn spawn_auth_preflight(&mut self) {
        if self.auth_working.swap(true, Ordering::SeqCst) {
            return;
        }
        let controller = self.controller.clone();
        let snapshot = self.snapshot.clone();
        let auth_working = self.auth_working.clone();
        std::thread::spawn(move || {
            let current = snapshot.lock().expect("operator snapshot poisoned").clone();
            let controller_for_action = controller.clone();
            let result =
                run_async(move || async move { controller_for_action.auth_preflight().await });
            let auth_notice = match result {
                Ok(summary) => Some(summary),
                Err(error) => Some(format!("auth preflight failed: {error:#}")),
            };
            let current_for_snapshot = current.clone();
            let auth_notice_for_snapshot = auth_notice.clone();
            let controller_for_snapshot = controller.clone();
            let next = run_async(move || async move {
                controller_for_snapshot
                    .read_snapshot(
                        &current_for_snapshot.run_state,
                        parse_run_mode(current_for_snapshot.run_mode.as_str()),
                        &current_for_snapshot.summary,
                        current_for_snapshot.last_error.clone(),
                        current_for_snapshot.completed_turn_count,
                        auth_notice_for_snapshot.clone(),
                    )
                    .await
            })
            .unwrap_or(OperatorSnapshot {
                refreshed_at: Some(Utc::now()),
                run_state: current.run_state,
                run_mode: current.run_mode,
                summary: current.summary,
                last_error: current.last_error,
                completed_turn_count: current.completed_turn_count,
                auth_notice,
                ..OperatorSnapshot::default()
            });
            *snapshot.lock().expect("operator snapshot poisoned") = next;
            auth_working.store(false, Ordering::SeqCst);
        });
    }

    fn spawn_import_openclaw_codex_oauth(&mut self) {
        if self.auth_working.swap(true, Ordering::SeqCst) {
            return;
        }
        let controller = self.controller.clone();
        let snapshot = self.snapshot.clone();
        let auth_working = self.auth_working.clone();
        std::thread::spawn(move || {
            let current = snapshot.lock().expect("operator snapshot poisoned").clone();
            let controller_for_action = controller.clone();
            let result = run_async(move || async move {
                controller_for_action.import_openclaw_codex_oauth().await
            });
            let auth_notice = match result {
                Ok(summary) => Some(summary),
                Err(error) => Some(format!("OpenClaw auth import failed: {error:#}")),
            };
            let current_for_snapshot = current.clone();
            let auth_notice_for_snapshot = auth_notice.clone();
            let controller_for_snapshot = controller.clone();
            let next = run_async(move || async move {
                controller_for_snapshot
                    .read_snapshot(
                        &current_for_snapshot.run_state,
                        parse_run_mode(current_for_snapshot.run_mode.as_str()),
                        &current_for_snapshot.summary,
                        current_for_snapshot.last_error.clone(),
                        current_for_snapshot.completed_turn_count,
                        auth_notice_for_snapshot.clone(),
                    )
                    .await
            })
            .unwrap_or(OperatorSnapshot {
                refreshed_at: Some(Utc::now()),
                run_state: current.run_state,
                run_mode: current.run_mode,
                summary: current.summary,
                last_error: current.last_error,
                completed_turn_count: current.completed_turn_count,
                auth_notice,
                ..OperatorSnapshot::default()
            });
            *snapshot.lock().expect("operator snapshot poisoned") = next;
            auth_working.store(false, Ordering::SeqCst);
        });
    }

    fn spawn_launch_codex_cli_login(&mut self) {
        if self.auth_working.swap(true, Ordering::SeqCst) {
            return;
        }
        let controller = self.controller.clone();
        let snapshot = self.snapshot.clone();
        let auth_working = self.auth_working.clone();
        std::thread::spawn(move || {
            let current = snapshot.lock().expect("operator snapshot poisoned").clone();
            let result = controller.launch_codex_cli_login();
            let auth_notice = match result {
                Ok(summary) => Some(summary),
                Err(error) => Some(format!("Codex CLI login launch failed: {error:#}")),
            };
            let current_for_snapshot = current.clone();
            let auth_notice_for_snapshot = auth_notice.clone();
            let controller_for_snapshot = controller.clone();
            let next = run_async(move || async move {
                controller_for_snapshot
                    .read_snapshot(
                        &current_for_snapshot.run_state,
                        parse_run_mode(current_for_snapshot.run_mode.as_str()),
                        &current_for_snapshot.summary,
                        current_for_snapshot.last_error.clone(),
                        current_for_snapshot.completed_turn_count,
                        auth_notice_for_snapshot.clone(),
                    )
                    .await
            })
            .unwrap_or(OperatorSnapshot {
                refreshed_at: Some(Utc::now()),
                run_state: current.run_state,
                run_mode: current.run_mode,
                summary: current.summary,
                last_error: current.last_error,
                completed_turn_count: current.completed_turn_count,
                auth_notice,
                ..OperatorSnapshot::default()
            });
            *snapshot.lock().expect("operator snapshot poisoned") = next;
            auth_working.store(false, Ordering::SeqCst);
        });
    }

    fn spawn_launch_openclaw_codex_login(&mut self) {
        if self.auth_working.swap(true, Ordering::SeqCst) {
            return;
        }
        let controller = self.controller.clone();
        let snapshot = self.snapshot.clone();
        let auth_working = self.auth_working.clone();
        std::thread::spawn(move || {
            let current = snapshot.lock().expect("operator snapshot poisoned").clone();
            let result = controller.launch_openclaw_codex_login();
            let auth_notice = match result {
                Ok(summary) => Some(summary),
                Err(error) => Some(format!("OpenClaw login launch failed: {error:#}")),
            };
            let current_for_snapshot = current.clone();
            let auth_notice_for_snapshot = auth_notice.clone();
            let controller_for_snapshot = controller.clone();
            let next = run_async(move || async move {
                controller_for_snapshot
                    .read_snapshot(
                        &current_for_snapshot.run_state,
                        parse_run_mode(current_for_snapshot.run_mode.as_str()),
                        &current_for_snapshot.summary,
                        current_for_snapshot.last_error.clone(),
                        current_for_snapshot.completed_turn_count,
                        auth_notice_for_snapshot.clone(),
                    )
                    .await
            })
            .unwrap_or(OperatorSnapshot {
                refreshed_at: Some(Utc::now()),
                run_state: current.run_state,
                run_mode: current.run_mode,
                summary: current.summary,
                last_error: current.last_error,
                completed_turn_count: current.completed_turn_count,
                auth_notice,
                ..OperatorSnapshot::default()
            });
            *snapshot.lock().expect("operator snapshot poisoned") = next;
            auth_working.store(false, Ordering::SeqCst);
        });
    }

    fn spawn_begin_oauth(&mut self, mode: OAuthInitiationMode) {
        if self.auth_working.swap(true, Ordering::SeqCst) {
            return;
        }
        let provider = self.auth_provider.as_provider_kind();
        let label = normalize_optional_text(&self.auth_label);
        let controller = self.controller.clone();
        let snapshot = self.snapshot.clone();
        let auth_working = self.auth_working.clone();
        std::thread::spawn(move || {
            let current = snapshot.lock().expect("operator snapshot poisoned").clone();
            let controller_for_action = controller.clone();
            let result = run_async(move || async move {
                controller_for_action
                    .begin_oauth_authorization(provider, mode, label.clone())
                    .await
            });
            let auth_notice = match result {
                Ok(pending) => {
                    let callback_note = match maybe_spawn_browser_oauth_callback_listener(
                        controller.clone(),
                        snapshot.clone(),
                        pending.clone(),
                    ) {
                        Ok(Some(note)) => Some(note),
                        Ok(None) => None,
                        Err(error) => Some(format!(
                            "auto callback listener unavailable ({error:#}); use the pending OAuth link and paste fallback"
                        )),
                    };
                    let base = format!(
                        "started {:?} oauth for {} ({:?})",
                        mode, pending.label, pending.provider
                    );
                    let launch_note = match pending_oauth_launch_url(&pending) {
                        Some((url, label)) => match open_external_url(url) {
                            Ok(()) => format!("opened {label} automatically"),
                            Err(error) => {
                                format!(
                                    "could not open {label} automatically ({error:#}); use the pending OAuth link below"
                                )
                            }
                        },
                        None => "pending oauth does not expose a launch URL".into(),
                    };
                    match callback_note {
                        Some(callback_note) => {
                            Some(format!("{base} | {launch_note} | {callback_note}"))
                        }
                        None => Some(format!("{base} | {launch_note}")),
                    }
                }
                Err(error) => Some(format!("oauth initiation failed: {error:#}")),
            };
            let current_for_snapshot = current.clone();
            let auth_notice_for_snapshot = auth_notice.clone();
            let controller_for_snapshot = controller.clone();
            let next = run_async(move || async move {
                controller_for_snapshot
                    .read_snapshot(
                        &current_for_snapshot.run_state,
                        parse_run_mode(current_for_snapshot.run_mode.as_str()),
                        &current_for_snapshot.summary,
                        current_for_snapshot.last_error.clone(),
                        current_for_snapshot.completed_turn_count,
                        auth_notice_for_snapshot.clone(),
                    )
                    .await
            })
            .unwrap_or(OperatorSnapshot {
                refreshed_at: Some(Utc::now()),
                run_state: current.run_state,
                run_mode: current.run_mode,
                summary: current.summary,
                last_error: current.last_error,
                completed_turn_count: current.completed_turn_count,
                auth_notice,
                ..OperatorSnapshot::default()
            });
            *snapshot.lock().expect("operator snapshot poisoned") = next;
            auth_working.store(false, Ordering::SeqCst);
        });
    }

    fn spawn_complete_browser_oauth(&mut self, pending_id: Uuid) {
        if self.auth_working.swap(true, Ordering::SeqCst) {
            return;
        }
        let callback_input = self.auth_callback_input.trim().to_string();
        let controller = self.controller.clone();
        let snapshot = self.snapshot.clone();
        let auth_working = self.auth_working.clone();
        std::thread::spawn(move || {
            let current = snapshot.lock().expect("operator snapshot poisoned").clone();
            let controller_for_action = controller.clone();
            let result = run_async(move || async move {
                controller_for_action
                    .complete_oauth_authorization(pending_id, &callback_input)
                    .await
            });
            let auth_notice = match result {
                Ok(summary) => Some(summary),
                Err(error) => Some(format!("oauth completion failed: {error:#}")),
            };
            let current_for_snapshot = current.clone();
            let auth_notice_for_snapshot = auth_notice.clone();
            let controller_for_snapshot = controller.clone();
            let next = run_async(move || async move {
                controller_for_snapshot
                    .read_snapshot(
                        &current_for_snapshot.run_state,
                        parse_run_mode(current_for_snapshot.run_mode.as_str()),
                        &current_for_snapshot.summary,
                        current_for_snapshot.last_error.clone(),
                        current_for_snapshot.completed_turn_count,
                        auth_notice_for_snapshot.clone(),
                    )
                    .await
            })
            .unwrap_or(OperatorSnapshot {
                refreshed_at: Some(Utc::now()),
                run_state: current.run_state,
                run_mode: current.run_mode,
                summary: current.summary,
                last_error: current.last_error,
                completed_turn_count: current.completed_turn_count,
                auth_notice,
                ..OperatorSnapshot::default()
            });
            *snapshot.lock().expect("operator snapshot poisoned") = next;
            auth_working.store(false, Ordering::SeqCst);
        });
    }

    fn spawn_complete_device_oauth(&mut self, pending_id: Uuid) {
        if self.auth_working.swap(true, Ordering::SeqCst) {
            return;
        }
        let controller = self.controller.clone();
        let snapshot = self.snapshot.clone();
        let auth_working = self.auth_working.clone();
        std::thread::spawn(move || {
            let current = snapshot.lock().expect("operator snapshot poisoned").clone();
            let controller_for_action = controller.clone();
            let result = run_async(move || async move {
                controller_for_action
                    .complete_device_oauth_authorization(pending_id)
                    .await
            });
            let auth_notice = match result {
                Ok(summary) => Some(summary),
                Err(error) => Some(format!("device oauth completion failed: {error:#}")),
            };
            let current_for_snapshot = current.clone();
            let auth_notice_for_snapshot = auth_notice.clone();
            let controller_for_snapshot = controller.clone();
            let next = run_async(move || async move {
                controller_for_snapshot
                    .read_snapshot(
                        &current_for_snapshot.run_state,
                        parse_run_mode(current_for_snapshot.run_mode.as_str()),
                        &current_for_snapshot.summary,
                        current_for_snapshot.last_error.clone(),
                        current_for_snapshot.completed_turn_count,
                        auth_notice_for_snapshot.clone(),
                    )
                    .await
            })
            .unwrap_or(OperatorSnapshot {
                refreshed_at: Some(Utc::now()),
                run_state: current.run_state,
                run_mode: current.run_mode,
                summary: current.summary,
                last_error: current.last_error,
                completed_turn_count: current.completed_turn_count,
                auth_notice,
                ..OperatorSnapshot::default()
            });
            *snapshot.lock().expect("operator snapshot poisoned") = next;
            auth_working.store(false, Ordering::SeqCst);
        });
    }
}

#[cfg(any())]
impl eframe::App for OperatorApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        if self.last_refresh.elapsed() >= REFRESH_INTERVAL {
            let snapshot = self
                .snapshot
                .lock()
                .expect("operator snapshot poisoned")
                .clone();
            self.spawn_refresh(
                snapshot.run_state,
                parse_run_mode(snapshot.run_mode.as_str()),
                snapshot.summary,
                snapshot.last_error,
                snapshot.completed_turn_count,
                snapshot.auth_notice,
            );
        }
        ctx.request_repaint_after(Duration::from_millis(250));

        let snapshot = self
            .snapshot
            .lock()
            .expect("operator snapshot poisoned")
            .clone();
        let auth_busy = self.auth_working.load(Ordering::SeqCst);
        let background_active = snapshot.background_runner_active;
        let background_recorded = snapshot.background_runner_id.is_some();
        let background_owned_by_this_shell = self.background_runner_owned_by_this_shell(&snapshot);
        let background_attached_to_other_live_shell =
            self.background_runner_attached_to_other_live_shell(&snapshot);
        let background_can_reattach = self.background_runner_can_reattach(&snapshot);
        let background_settings_match =
            background_recorded && self.background_settings_match_form(&snapshot);
        let background_recovery_label = background_recovery_action_label(&snapshot);
        let handoff_pending = snapshot.background_handoff_pending;
        let handoff_ready = snapshot.background_handoff_ready;
        let handoff_settings_match = handoff_pending && self.handoff_settings_match_form(&snapshot);
        let can_run = self.selected_engine_ready(&snapshot)
            && !self.running.load(Ordering::SeqCst)
            && !auth_busy
            && !background_active;
        egui::TopBottomPanel::top("toolbar").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.heading("AGRO Harness Operator");
                ui.separator();
                ui.label(format!("state: {}", snapshot.run_state));
                ui.label(format!("mode: {}", snapshot.run_mode));
                if background_active {
                    let pid = snapshot
                        .background_runner_pid
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "unknown".into());
                    let phase = snapshot
                        .background_runner_phase
                        .as_deref()
                        .unwrap_or("active");
                    let runner_id = snapshot
                        .background_runner_id
                        .as_deref()
                        .unwrap_or("unknown");
                    ui.label(format!("background: {phase} ({pid} / {runner_id})"));
                } else if background_recorded {
                    let status = snapshot
                        .background_runner_status
                        .as_deref()
                        .unwrap_or("inactive");
                    let runner_id = snapshot
                        .background_runner_id
                        .as_deref()
                        .unwrap_or("unknown");
                    ui.label(format!("background: {status} ({runner_id})"));
                } else {
                    ui.label("background: idle");
                }
                if self.running.load(Ordering::SeqCst) {
                    ui.spinner();
                }
                if auth_busy {
                    ui.label("auth busy");
                }
                if ui.button("Refresh").clicked() {
                    self.spawn_refresh(
                        snapshot.run_state.clone(),
                        parse_run_mode(snapshot.run_mode.as_str()),
                        snapshot.summary.clone(),
                        snapshot.last_error.clone(),
                        snapshot.completed_turn_count,
                        snapshot.auth_notice.clone(),
                    );
                }
                if ui
                    .add_enabled(can_run, egui::Button::new("Run Turn"))
                    .clicked()
                {
                    self.begin_run(OperatorRunMode::SingleTurn);
                }
                if ui
                    .add_enabled(can_run, egui::Button::new("Start Loop"))
                    .clicked()
                {
                    self.begin_run(OperatorRunMode::Continuous);
                }
                if ui
                    .add_enabled(
                        self.loop_requested.load(Ordering::SeqCst),
                        egui::Button::new("Stop Loop"),
                    )
                    .clicked()
                {
                    self.stop_loop();
                }
                if ui
                    .add_enabled(can_run, egui::Button::new("Start Background Loop"))
                    .clicked()
                {
                    self.spawn_background_loop();
                }
                if ui
                    .add_enabled(
                        background_active && background_owned_by_this_shell && !auth_busy,
                        egui::Button::new("Stop Background"),
                    )
                    .clicked()
                {
                    self.spawn_stop_background_loop();
                }
                if ui
                    .add_enabled(
                        background_active
                            && background_owned_by_this_shell
                            && !background_settings_match
                            && !auth_busy,
                        egui::Button::new("Request Handoff"),
                    )
                    .clicked()
                {
                    self.spawn_request_background_handoff();
                }
                if ui
                    .add_enabled(
                        background_recorded && !auth_busy,
                        egui::Button::new("Clear Background State"),
                    )
                    .clicked()
                {
                    self.spawn_clear_background_state();
                }
            });
        });

        egui::SidePanel::left("launch")
            .resizable(true)
            .default_width(320.0)
            .show(ctx, |ui| {
                ui.heading("Launch");
                ui.label("This window launches the harness directly and monitors the live continuity files.");
                ui.separator();
                ui.label("Objective");
                ui.text_edit_multiline(&mut self.settings.objective);
                ui.label("Model");
                ui.text_edit_singleline(&mut self.settings.model);
                ui.label("Thread Id");
                ui.text_edit_singleline(&mut self.settings.thread_id);
                ui.label("Thread Label");
                ui.text_edit_singleline(&mut self.settings.thread_label);
                ui.label("Loop pause (seconds)");
                ui.add(
                    egui::DragValue::new(&mut self.loop_pause_seconds)
                        .speed(0.25)
                        .range(0.0..=60.0),
                );
                ui.separator();
                ui.heading("Paths");
                ui.monospace(format!("repo: {}", self.controller.paths.repo_root.display()));
                ui.monospace(format!("harness: {}", self.controller.paths.harness_root.display()));
                ui.monospace(format!("status: {}", self.controller.paths.status_path.display()));
                ui.monospace(format!("session root: {}", self.controller.paths.session_root.display()));
                ui.monospace(format!("state db: {}", self.controller.paths.state_db_path.display()));
                ui.monospace(format!("auth store: {}", self.controller.paths.auth_store_path.display()));
                ui.monospace(format!(
                    "oauth env: {}",
                    self.controller.paths.operator_env_path.display()
                ));
                ui.separator();
                ui.heading("Auth");
                let auth_color = if self.selected_engine_ready(&snapshot) {
                    egui::Color32::LIGHT_GREEN
                } else {
                    egui::Color32::LIGHT_RED
                };
                ui.colored_label(auth_color, format!("readiness: {}", snapshot.auth_readiness));
                ui.label(&snapshot.auth_summary);
                if let Some(auth_notice) = snapshot.auth_notice.as_deref() {
                    ui.label(auth_notice);
                }
                if let Some(background_summary) = snapshot.background_runner_summary.as_deref() {
                    ui.label(format!("background: {background_summary}"));
                }
                if let Some(background_recovery) =
                    snapshot.background_recovery_recommendation.as_deref()
                {
                    ui.label(format!("recovery: {background_recovery}"));
                }
                if !self.selected_engine_ready(&snapshot) {
                    ui.label("Turn execution is gated until auth is ready.");
                }
                let oauth_launch_status = interactive_oauth_launch_status(self.auth_provider);
                let operator_env_status = operator_env_config_status(
                    self.auth_provider,
                    &snapshot.operator_env_configured_keys,
                    &oauth_launch_status,
                );
                let openclaw_status = if self.auth_provider == OperatorAuthProvider::OpenAiCodex {
                    Some(openclaw_import_status())
                } else {
                    None
                };
                let openclaw_cli = if self.auth_provider == OperatorAuthProvider::OpenAiCodex {
                    Some(openclaw_cli_status())
                } else {
                    None
                };
                let oauth_launch_color = if oauth_launch_status.ready {
                    egui::Color32::LIGHT_GREEN
                } else {
                    egui::Color32::LIGHT_RED
                };
                let operator_env_color = if operator_env_status.configured {
                    egui::Color32::LIGHT_GREEN
                } else if oauth_launch_status.ready {
                    egui::Color32::YELLOW
                } else {
                    egui::Color32::LIGHT_RED
                };
                ui.colored_label(
                    oauth_launch_color,
                    format!("interactive oauth: {}", oauth_launch_status.summary),
                );
                ui.colored_label(
                    operator_env_color,
                    format!("operator env: {}", operator_env_status.summary),
                );
                if let Some(openclaw_status) = openclaw_status.as_ref() {
                    ui.colored_label(
                        if openclaw_status.available {
                            egui::Color32::LIGHT_GREEN
                        } else {
                            egui::Color32::YELLOW
                        },
                        format!("openclaw fallback: {}", openclaw_status.summary),
                    );
                }
                if let Some(openclaw_cli) = openclaw_cli.as_ref() {
                    ui.colored_label(
                        if openclaw_cli.available {
                            egui::Color32::LIGHT_GREEN
                        } else {
                            egui::Color32::YELLOW
                        },
                        format!("openclaw cli: {}", openclaw_cli.summary),
                    );
                }
                ui.label(format!(
                    "oauth config file: {}",
                    self.controller.paths.operator_env_path.display()
                ));
                if self.auth_provider == OperatorAuthProvider::OpenAiCodex {
                    ui.label(
                        "Native harness Codex sign-in is the primary path here. OpenClaw login/import is only a fallback or migration path.",
                    );
                }
                if background_active {
                    ui.label(
                        "Foreground runs and auth mutations are gated while background control is active.",
                    );
                }
                ui.separator();
                ui.heading("Background");
                if background_recorded {
                    if let Some(status) = snapshot.background_runner_status.as_deref() {
                        ui.label(format!("status: {status}"));
                    }
                    if let Some(runner_id) = snapshot.background_runner_id.as_deref() {
                        ui.label(format!("runner id: {runner_id}"));
                    }
                    if let Some(pid) = snapshot.background_runner_pid {
                        ui.label(format!("pid: {pid}"));
                    }
                    if let Some(owner_shell_id) = snapshot.background_runner_owner_shell_id.as_deref()
                    {
                        ui.label(format!("launch shell id: {owner_shell_id}"));
                    }
                    if let Some(owner_shell_pid) = snapshot.background_runner_owner_shell_pid {
                        ui.label(format!("launch shell pid: {owner_shell_pid}"));
                    }
                    if let Some(owner_shell_alive) = snapshot.background_runner_owner_shell_alive {
                        ui.label(format!(
                            "launch shell alive: {}",
                            if owner_shell_alive { "yes" } else { "no" }
                        ));
                    }
                    if let Some(phase) = snapshot.background_runner_phase.as_deref() {
                        ui.label(format!("phase: {phase}"));
                    }
                    if let Some(turns) = snapshot.background_runner_turn_count {
                        ui.label(format!("completed turns: {turns}"));
                    }
                    if let Some(thread_id) = snapshot.background_runner_thread_id.as_deref() {
                        ui.label(format!("thread id: {thread_id}"));
                    }
                    if let Some(thread_label) = snapshot.background_runner_thread_label.as_deref() {
                        ui.label(format!("thread: {thread_label}"));
                    }
                    if let Some(model) = snapshot.background_runner_model.as_deref() {
                        ui.label(format!("model: {model}"));
                    }
                    if let Some(loop_pause_seconds) = snapshot.background_runner_loop_pause_seconds {
                        ui.label(format!("pause: {:.1}s", loop_pause_seconds));
                    }
                    if let Some(started_at) = snapshot.background_runner_started_at {
                        ui.label(format!("started: {}", started_at.to_rfc3339()));
                    }
                    if let Some(updated_at) = snapshot.background_runner_updated_at {
                        ui.label(format!(
                            "last heartbeat: {} ago ({})",
                            format_background_runner_age(updated_at),
                            updated_at.to_rfc3339()
                        ));
                    }
                    if snapshot.background_stop_requested {
                        ui.label("stop request: pending");
                    }
                    if let Some(objective) = snapshot.background_runner_objective.as_deref() {
                        ui.label("background objective:");
                        ui.label(objective);
                    }
                    if let Some(last_error) = snapshot.background_runner_last_error.as_deref() {
                        ui.label("last detached-runner error:");
                        ui.label(last_error);
                    }
                    if let Some(recovery) = snapshot.background_recovery_recommendation.as_deref() {
                        ui.label("recovery recommendation:");
                        ui.label(recovery);
                    }
                    if let Some(reattach) = snapshot.background_reattach_recommendation.as_deref() {
                        ui.label("reattach recommendation:");
                        ui.label(reattach);
                    }
                    if let Some(label) = background_recovery_label {
                        if ui
                            .add_enabled(
                                !background_active && !auth_busy,
                                egui::Button::new(label),
                            )
                            .clicked()
                        {
                            self.adopt_background_settings(&snapshot);
                            self.spawn_background_loop();
                        }
                    }
                    if background_owned_by_this_shell {
                        ui.label("this shell owns the detached runner");
                    } else if background_attached_to_other_live_shell {
                        ui.label(
                            "the detached runner is still attached to another live shell",
                        );
                    } else if background_can_reattach {
                        ui.label(
                            "the detached runner can be reattached from this shell before you control it",
                        );
                    }
                    ui.label(if background_settings_match {
                        "launch form matches the detached runner"
                    } else {
                        "launch form differs from the detached runner"
                    });
                    if ui.button("Adopt Background Settings").clicked() {
                        self.adopt_background_settings(&snapshot);
                    }
                    if ui
                        .add_enabled(
                            background_can_reattach && !auth_busy,
                            egui::Button::new("Reattach Background Runner"),
                        )
                        .clicked()
                    {
                        self.spawn_reattach_background_runner();
                    }
                } else {
                    ui.label("No detached background runner record is currently available.");
                }
                if handoff_pending {
                    ui.separator();
                    ui.label("pending handoff request:");
                    if let Some(status) = snapshot.background_handoff_status.as_deref() {
                        ui.label(format!("handoff status: {status}"));
                    }
                    if let Some(target_runner_id) =
                        snapshot.background_handoff_target_runner_id.as_deref()
                    {
                        ui.label(format!("target runner: {target_runner_id}"));
                    }
                    if let Some(requested_at) = snapshot.background_handoff_requested_at {
                        ui.label(format!("requested: {}", requested_at.to_rfc3339()));
                    }
                    if let Some(thread_id) = snapshot.background_handoff_thread_id.as_deref() {
                        ui.label(format!("handoff thread id: {thread_id}"));
                    }
                    if let Some(thread_label) =
                        snapshot.background_handoff_thread_label.as_deref()
                    {
                        ui.label(format!("handoff thread: {thread_label}"));
                    }
                    if let Some(model) = snapshot.background_handoff_model.as_deref() {
                        ui.label(format!("handoff model: {model}"));
                    }
                    if let Some(loop_pause_seconds) = snapshot.background_handoff_loop_pause_seconds
                    {
                        ui.label(format!("handoff pause: {:.1}s", loop_pause_seconds));
                    }
                    if let Some(objective) = snapshot.background_handoff_objective.as_deref() {
                        ui.label("handoff objective:");
                        ui.label(objective);
                    }
                    ui.label(if handoff_settings_match {
                        "launch form matches the pending handoff request"
                    } else {
                        "launch form differs from the pending handoff request"
                    });
                    ui.horizontal(|ui| {
                        if ui.button("Adopt Handoff Settings").clicked() {
                            self.adopt_background_handoff(&snapshot);
                        }
                        if ui
                            .add_enabled(
                                handoff_ready && !background_active && !auth_busy,
                                egui::Button::new("Complete Handoff"),
                            )
                            .clicked()
                        {
                            self.spawn_complete_background_handoff();
                        }
                        if ui.button("Clear Handoff").clicked() {
                            self.spawn_clear_background_handoff();
                        }
                    });
                }
                ui.horizontal(|ui| {
                    ui.label("Provider");
                    egui::ComboBox::from_id_salt("auth-provider")
                        .selected_text(self.auth_provider.as_label())
                        .show_ui(ui, |ui| {
                            ui.selectable_value(
                                &mut self.auth_provider,
                                OperatorAuthProvider::OpenAiCodex,
                                OperatorAuthProvider::OpenAiCodex.as_label(),
                            );
                            ui.selectable_value(
                                &mut self.auth_provider,
                                OperatorAuthProvider::OpenAiApi,
                                OperatorAuthProvider::OpenAiApi.as_label(),
                            );
                        });
                });
                ui.label("Auth label");
                ui.text_edit_singleline(&mut self.auth_label);
                ui.horizontal(|ui| {
                    if ui
                        .add_enabled(!auth_busy && !background_active, egui::Button::new("Auth Preflight"))
                        .clicked()
                    {
                        self.spawn_auth_preflight();
                    }
                    if self.auth_provider == OperatorAuthProvider::OpenAiCodex
                        && ui
                            .add_enabled(
                                !auth_busy
                                    && !background_active
                                    && openclaw_cli
                                        .as_ref()
                                        .map(|status| status.available)
                                        .unwrap_or(false),
                                egui::Button::new("Launch OpenClaw Login (Fallback)"),
                            )
                            .clicked()
                    {
                        self.spawn_launch_openclaw_codex_login();
                    }
                    if self.auth_provider == OperatorAuthProvider::OpenAiCodex
                        && ui
                            .add_enabled(
                                !auth_busy
                                    && !background_active
                                    && openclaw_status
                                        .as_ref()
                                        .map(|status| status.available)
                                        .unwrap_or(false),
                                egui::Button::new("Import OpenClaw Codex OAuth (Fallback)"),
                            )
                            .clicked()
                    {
                        self.spawn_import_openclaw_codex_oauth();
                    }
                    if ui
                        .add_enabled(
                            !auth_busy && !background_active && oauth_launch_status.ready,
                            egui::Button::new(if self.auth_provider
                                == OperatorAuthProvider::OpenAiCodex
                            {
                                "Sign In With OpenAI Codex"
                            } else {
                                "Begin Browser OAuth"
                            }),
                        )
                        .clicked()
                    {
                        self.spawn_begin_oauth(OAuthInitiationMode::BrowserCallback);
                    }
                    if self.auth_provider != OperatorAuthProvider::OpenAiCodex
                        && ui
                            .add_enabled(
                                !auth_busy && !background_active && oauth_launch_status.ready,
                                egui::Button::new("Begin Device OAuth"),
                            )
                            .clicked()
                    {
                        self.spawn_begin_oauth(OAuthInitiationMode::DeviceCode);
                    }
                });
                ui.label("Browser callback URL or auth code");
                ui.text_edit_multiline(&mut self.auth_callback_input);
                if snapshot.pending_oauth.is_empty() {
                    ui.label("pending oauth: none");
                } else {
                    ui.separator();
                    ui.heading("Pending OAuth");
                    let pending_views = snapshot.pending_oauth.clone();
                    for pending in pending_views {
                        ui.group(|ui| {
                            ui.label(format!(
                                "{} | {} | {}",
                                pending.label, pending.provider, pending.kind
                            ));
                            ui.label(format!("started: {}", pending.started_at));
                            if let Some(expires_at) = pending.expires_at.as_deref() {
                                ui.label(format!("expires: {expires_at}"));
                            }
                            if let Some(url) = pending.authorization_url.as_deref() {
                                ui.hyperlink_to("Open authorization URL", url);
                            }
                            if let Some(redirect_uri) = pending.redirect_uri.as_deref() {
                                ui.label(format!("redirect: {redirect_uri}"));
                            }
                            if let Some(prompt) = pending.callback_prompt.as_deref() {
                                ui.label(prompt);
                            }
                            if let Some(verification_uri) = pending.verification_uri.as_deref() {
                                ui.hyperlink_to("Open verification URL", verification_uri);
                            }
                            if let Some(user_code) = pending.user_code.as_deref() {
                                ui.label(format!("user code: {user_code}"));
                            }
                            ui.label(&pending.action_hint);
                            match Uuid::parse_str(&pending.id) {
                                Ok(pending_id) => {
                                    if pending.authorization_url.is_some() {
                                        if ui
                                            .add_enabled(
                                                !auth_busy && !background_active,
                                                egui::Button::new("Complete Browser OAuth"),
                                            )
                                            .clicked()
                                        {
                                            self.spawn_complete_browser_oauth(pending_id);
                                        }
                                    } else if ui
                                        .add_enabled(
                                            !auth_busy && !background_active,
                                            egui::Button::new("Poll / Complete Device OAuth"),
                                        )
                                        .clicked()
                                    {
                                        self.spawn_complete_device_oauth(pending_id);
                                    }
                                }
                                Err(_) => {
                                    ui.colored_label(
                                        egui::Color32::LIGHT_RED,
                                        "pending oauth id is invalid",
                                    );
                                }
                            }
                        });
                    }
                }
                ui.separator();
                ui.heading("GitHub Request");
                if let Some(state) = snapshot.github_action_state.as_deref() {
                    ui.label(format!("state: {state}"));
                }
                if let Some(updated_at) = snapshot.github_action_updated_at {
                    ui.label(format!("updated: {}", updated_at.to_rfc3339()));
                }
                if let Some(latest_summary) = snapshot.github_action_latest_summary.as_deref() {
                    ui.label(format!("latest: {latest_summary}"));
                }
                if snapshot.github_action_pending {
                    let github_target_ready = if snapshot.github_action_requires_target {
                        parse_optional_github_target_override(Some(self.github_target_input.as_str()))
                            .ok()
                            .flatten()
                            .is_some()
                    } else {
                        true
                    };
                    if let Some(summary) = snapshot.github_action_summary.as_deref() {
                        ui.label(summary);
                    }
                    if let Some(kind) = snapshot.github_action_kind.as_deref() {
                        ui.label(format!("kind: {kind}"));
                    }
                    if let Some(target) = snapshot.github_action_target.as_deref() {
                        ui.label(format!("target: {target}"));
                    }
                    if let Some(repository) = snapshot.github_action_repository.as_deref() {
                        ui.label(format!("repository: {repository}"));
                    }
                    if snapshot.github_action_requires_target {
                        let target_kind = snapshot
                            .github_action_target_kind
                            .as_deref()
                            .unwrap_or("target");
                        ui.label(format!(
                            "{target_kind} number required before this request can be applied"
                        ));
                        ui.horizontal(|ui| {
                            ui.label(format!("{target_kind} number:"));
                            ui.text_edit_singleline(&mut self.github_target_input);
                        });
                        if snapshot.github_action_target_suggestions.is_empty() {
                            ui.label(format!("no live {target_kind} suggestions are available"));
                        } else {
                            ui.label("suggested targets:");
                            for suggestion in &snapshot.github_action_target_suggestions {
                                ui.horizontal(|ui| {
                                    if ui
                                        .button(format!(
                                            "Use #{}",
                                            suggestion.number
                                        ))
                                        .clicked()
                                    {
                                        self.github_target_input = suggestion.number.to_string();
                                    }
                                    ui.label(format!(
                                        "#{} {} ({})",
                                        suggestion.number, suggestion.title, suggestion.source
                                    ));
                                });
                                if let Some(url) = suggestion.url.as_deref() {
                                    ui.hyperlink_to(url, url);
                                }
                            }
                        }
                        if let Some(guidance) =
                            snapshot.github_action_target_guidance.as_deref()
                        {
                            ui.label(guidance);
                        }
                    }
                    if let Some(requested_at) = snapshot.github_action_requested_at {
                        ui.label(format!("requested: {}", requested_at.to_rfc3339()));
                    }
                    if let Some(justification) = snapshot.github_action_justification.as_deref() {
                        ui.label("justification:");
                        ui.label(justification);
                    }
                    if let Some(body) = snapshot.github_action_body.as_deref() {
                        ui.label("body:");
                        ui.label(body);
                    }
                    ui.horizontal(|ui| {
                        if ui
                            .add_enabled(
                                !auth_busy
                                    && !background_active
                                    && !self.running.load(Ordering::SeqCst)
                                    && github_target_ready,
                                egui::Button::new("Apply GitHub Request"),
                            )
                            .clicked()
                        {
                            self.spawn_apply_github_action_request();
                        }
                        if ui
                            .add_enabled(
                                !auth_busy && !self.running.load(Ordering::SeqCst),
                                egui::Button::new("Reject GitHub Request"),
                            )
                            .clicked()
                        {
                            self.spawn_reject_github_action_request();
                        }
                        if ui
                            .add_enabled(
                                !auth_busy && !self.running.load(Ordering::SeqCst),
                                egui::Button::new("Clear GitHub Request"),
                            )
                            .clicked()
                        {
                            self.spawn_clear_github_action_request();
                        }
                    });
                } else {
                    self.github_target_input.clear();
                    ui.label("pending GitHub request: none");
                    if snapshot.github_action_state.is_some() {
                        ui.label("latest settled GitHub request:");
                        if let Some(kind) = snapshot.github_action_kind.as_deref() {
                            ui.label(format!("kind: {kind}"));
                        }
                        if let Some(target) = snapshot.github_action_target.as_deref() {
                            ui.label(format!("target: {target}"));
                        }
                        if let Some(repository) = snapshot.github_action_repository.as_deref() {
                            ui.label(format!("repository: {repository}"));
                        }
                        if let Some(detail) = snapshot.github_action_detail.as_deref() {
                            ui.label("detail:");
                            ui.label(detail);
                        }
                        if let Some(result_excerpt) = snapshot.github_action_result_excerpt.as_deref() {
                            ui.label("result:");
                            ui.label(result_excerpt);
                        }
                        if let Some(result_url) = snapshot.github_action_result_url.as_deref() {
                            ui.label("result url:");
                            ui.hyperlink_to(result_url, result_url);
                        }
                        if let Some(justification) =
                            snapshot.github_action_justification.as_deref()
                        {
                            ui.label("justification:");
                            ui.label(justification);
                        }
                        if let Some(body) = snapshot.github_action_body.as_deref() {
                            ui.label("body:");
                            ui.label(body);
                        }
                    }
                }
                if !snapshot.github_action_recent_events.is_empty() {
                    ui.label("recent GitHub request events:");
                    for event in &snapshot.github_action_recent_events {
                        ui.label(event);
                    }
                }
            });

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("Monitor");
            ui.label(format!("summary: {}", snapshot.summary));
            if let Some(refreshed_at) = snapshot.refreshed_at {
                ui.label(format!("refreshed: {}", refreshed_at.to_rfc3339()));
            }
            if let Some(error) = snapshot.last_error.as_deref() {
                ui.colored_label(egui::Color32::LIGHT_RED, error);
            }
            ui.separator();
            ui.columns(2, |columns| {
                columns[0].group(|ui| {
                    ui.heading("Session");
                    ui.label(format!(
                        "foreground thread: {}",
                        snapshot.foreground_thread_id.as_deref().unwrap_or("none")
                    ));
                    ui.label(format!(
                        "compactions: {}",
                        snapshot.compaction_count.unwrap_or_default()
                    ));
                    ui.label(format!(
                        "completed turns: {}",
                        snapshot.completed_turn_count
                    ));
                    if let Some(phase) = snapshot.pending_turn_phase.as_deref() {
                        ui.label(format!("pending phase: {phase}"));
                    } else {
                        ui.label("pending phase: none");
                    }
                    if let Some(action) = snapshot.pending_turn_action.as_deref() {
                        ui.label(format!("pending action: {action}"));
                    }
                    if let Some(turn) = snapshot.last_turn_summary.as_deref() {
                        ui.separator();
                        ui.label("Last turn");
                        ui.label(turn);
                    }
                    ui.separator();
                    draw_read_only_lines(ui, "Recent turns", &snapshot.recent_turns, 8);
                });
                columns[1].group(|ui| {
                    ui.heading("Current Surface");
                    draw_read_only_text(ui, "Brief", snapshot.current_brief.as_deref());
                    draw_read_only_text(ui, "Plan", snapshot.current_plan.as_deref());
                });
            });
            ui.separator();
            ui.columns(2, |columns| {
                columns[0].group(|ui| {
                    ui.heading("Recent Session Events");
                    draw_read_only_lines(ui, "Events", &snapshot.recent_events, 12);
                });
                columns[1].group(|ui| {
                    ui.heading("Open Gaps");
                    draw_read_only_text(ui, "Open gaps", snapshot.current_open_gaps.as_deref());
                    ui.separator();
                    ui.heading("Handoff");
                    draw_read_only_text(ui, "Handoff", snapshot.current_handoff.as_deref());
                });
            });
        });
    }
}

#[cfg(any())]
fn draw_read_only_text(ui: &mut egui::Ui, label: &str, body: Option<&str>) {
    ui.label(label);
    let mut text = body.unwrap_or("not available yet").to_string();
    ui.add(
        egui::TextEdit::multiline(&mut text)
            .desired_rows(10)
            .interactive(false)
            .code_editor(),
    );
}

#[cfg(any())]
fn draw_read_only_lines(ui: &mut egui::Ui, label: &str, lines: &[String], rows: usize) {
    let body = if lines.is_empty() {
        "not available yet".to_string()
    } else {
        lines.join("\n")
    };
    draw_read_only_text_with_rows(ui, label, &body, rows);
}

#[cfg(any())]
fn draw_read_only_text_with_rows(ui: &mut egui::Ui, label: &str, body: &str, rows: usize) {
    ui.label(label);
    let mut text = body.to_string();
    ui.add(
        egui::TextEdit::multiline(&mut text)
            .desired_rows(rows)
            .interactive(false)
            .code_editor(),
    );
}

fn normalize_text(value: &str, fallback: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        fallback.into()
    } else {
        trimmed.into()
    }
}

fn read_text_if_exists(path: PathBuf) -> anyhow::Result<Option<String>> {
    if !path.exists() {
        return Ok(None);
    }
    Ok(Some(
        fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?,
    ))
}

fn objective_needs_project_artifact_context(objective: &str) -> bool {
    let lower = objective.to_ascii_lowercase();
    [
        "roadmap",
        "current-plan",
        "open-gaps",
        "implementation slice",
        "self-development",
        "self development",
        "selfdev",
        "repo",
        "github",
        "branch",
        "commit",
        "pull request",
        "issue",
        "review",
        "autonomy",
        "autonomous",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn run_repo_command_capture(repo_root: &Path, program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program)
        .args(args)
        .current_dir(repo_root)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let body = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if body.is_empty() { None } else { Some(body) }
}

fn build_repo_git_context_from(
    repo_root: &Path,
    branch: Option<&str>,
    head: Option<&str>,
    upstream: Option<&str>,
    origin_url: Option<&str>,
    status_output: Option<&str>,
    recent_commits: Option<&str>,
) -> Option<String> {
    let mut sections = Vec::new();
    sections.push(format!("source: live git state in {}", repo_root.display()));

    if let Some(branch) = branch {
        sections.push(format!("- branch: {branch}"));
    }
    if let Some(head) = head {
        sections.push(format!("- head: {head}"));
    }
    if let Some(upstream) = upstream {
        sections.push(format!("- upstream: {upstream}"));
    }
    if let Some(origin_url) = origin_url {
        sections.push(format!("- origin: {origin_url}"));
    }

    if let Some(status_output) = status_output {
        let mut header = None;
        let mut tracked_entries = Vec::new();
        let mut untracked_entries = Vec::new();
        for line in status_output.lines() {
            if header.is_none() && line.starts_with("## ") {
                header = Some(line.trim().to_string());
                continue;
            }
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            if trimmed.starts_with("??") {
                untracked_entries.push(trimmed.to_string());
            } else {
                tracked_entries.push(trimmed.to_string());
            }
        }

        if let Some(header) = header {
            sections.push(format!("- status summary: {header}"));
        }
        sections.push(format!(
            "- tracked worktree: {}",
            if tracked_entries.is_empty() {
                "clean".to_string()
            } else {
                format!("{} tracked change(s)", tracked_entries.len())
            }
        ));
        if !untracked_entries.is_empty() {
            sections.push(format!("- untracked entries: {}", untracked_entries.len()));
        }
        if !tracked_entries.is_empty() {
            sections.push(format!(
                "### Tracked Worktree Entries\n```text\n{}\n```",
                tracked_entries.join("\n")
            ));
        }
        if !untracked_entries.is_empty() {
            sections.push(format!(
                "### Untracked Entries\n```text\n{}\n```",
                untracked_entries.join("\n")
            ));
        }
    }

    if let Some(recent_commits) = recent_commits {
        sections.push(format!(
            "### Recent Commits\n```text\n{}\n```",
            recent_commits.trim()
        ));
    }

    if sections.len() <= 1 {
        return None;
    }

    Some(format!("## Repo / GitHub Context\n{}", sections.join("\n")))
}

fn build_repo_git_context(paths: &OperatorPaths) -> Option<String> {
    let repo_root = paths.repo_root.as_path();
    build_repo_git_context_from(
        repo_root,
        run_repo_command_capture(repo_root, "git", &["branch", "--show-current"]).as_deref(),
        run_repo_command_capture(repo_root, "git", &["rev-parse", "HEAD"]).as_deref(),
        run_repo_command_capture(
            repo_root,
            "git",
            &["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"],
        )
        .as_deref(),
        run_repo_command_capture(repo_root, "git", &["remote", "get-url", "origin"]).as_deref(),
        run_repo_command_capture(repo_root, "git", &["status", "--short", "--branch"]).as_deref(),
        run_repo_command_capture(repo_root, "git", &["log", "--oneline", "-5"]).as_deref(),
    )
}

fn build_github_cli_context_from(
    repo_summary: Option<&str>,
    branch_pull_requests: Option<&str>,
    open_pull_requests: Option<&str>,
    open_issues: Option<&str>,
) -> Option<String> {
    let mut sections = vec!["source: live GitHub CLI state for the current repo".to_string()];

    if let Some(repo_summary) = repo_summary {
        sections.push(repo_summary.trim().to_string());
    }
    if let Some(branch_pull_requests) = branch_pull_requests {
        let body = branch_pull_requests.trim();
        if !body.is_empty() {
            sections.push(format!(
                "### Pull Requests For Current Branch\n```text\n{}\n```",
                body
            ));
        }
    }
    if let Some(open_pull_requests) = open_pull_requests {
        let body = open_pull_requests.trim();
        if !body.is_empty() {
            sections.push(format!(
                "### Recent Open Pull Requests\n```text\n{}\n```",
                body
            ));
        }
    }
    if let Some(open_issues) = open_issues {
        let body = open_issues.trim();
        if !body.is_empty() {
            sections.push(format!("### Recent Open Issues\n```text\n{}\n```", body));
        }
    }

    if sections.len() <= 1 {
        return None;
    }

    Some(format!("## GitHub Remote Context\n{}", sections.join("\n")))
}

fn build_github_cli_context(paths: &OperatorPaths) -> Option<String> {
    let repo_root = paths.repo_root.as_path();
    let branch = run_repo_command_capture(repo_root, "git", &["branch", "--show-current"]);
    let repo_summary = run_repo_command_capture(
        repo_root,
        "gh",
        &[
            "repo",
            "view",
            "--json",
            "nameWithOwner,url,defaultBranchRef,description",
            "--template",
            "- repo: {{.nameWithOwner}}\n- url: {{.url}}\n- default branch: {{.defaultBranchRef.name}}\n{{if .description}}- description: {{.description}}\n{{end}}",
        ],
    );
    let branch_pull_requests = branch.as_deref().and_then(|branch| {
        run_repo_command_capture(
            repo_root,
            "gh",
            &[
                "pr",
                "list",
                "--state",
                "all",
                "--head",
                branch,
                "--limit",
                "3",
                "--json",
                "number,title,state,url,isDraft,baseRefName,headRefName",
                "--template",
                "{{range .}}- #{{.number}} {{.title}} [{{.state}}] {{if .isDraft}}draft {{end}}head={{.headRefName}} base={{.baseRefName}} {{.url}}\n{{end}}",
            ],
        )
    });
    let open_pull_requests = run_repo_command_capture(
        repo_root,
        "gh",
        &[
            "pr",
            "list",
            "--state",
            "open",
            "--limit",
            "5",
            "--json",
            "number,title,state,url,isDraft,baseRefName,headRefName",
            "--template",
            "{{range .}}- #{{.number}} {{.title}} [{{.state}}] {{if .isDraft}}draft {{end}}head={{.headRefName}} base={{.baseRefName}} {{.url}}\n{{end}}",
        ],
    );
    let open_issues = run_repo_command_capture(
        repo_root,
        "gh",
        &[
            "issue",
            "list",
            "--state",
            "open",
            "--limit",
            "5",
            "--json",
            "number,title,state,url",
            "--template",
            "{{range .}}- #{{.number}} {{.title}} [{{.state}}] {{.url}}\n{{end}}",
        ],
    );

    build_github_cli_context_from(
        repo_summary.as_deref(),
        branch_pull_requests.as_deref(),
        open_pull_requests.as_deref(),
        open_issues.as_deref(),
    )
}

#[derive(Debug, Deserialize)]
struct GithubPullRequestSuggestionJson {
    number: u64,
    title: String,
    #[serde(default)]
    url: Option<String>,
    #[serde(default, rename = "headRefName")]
    head_ref_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GithubIssueSuggestionJson {
    number: u64,
    title: String,
    #[serde(default)]
    url: Option<String>,
}

fn parse_github_target_suggestions_json<T>(body: Option<&str>) -> Vec<T>
where
    T: for<'de> Deserialize<'de>,
{
    body.and_then(|body| serde_json::from_str::<Vec<T>>(body).ok())
        .unwrap_or_default()
}

fn github_target_search_terms(request: &SupervisedGithubActionRequest) -> Vec<String> {
    const STOP_WORDS: &[&str] = &[
        "about",
        "after",
        "before",
        "being",
        "between",
        "bound",
        "comment",
        "current",
        "draft",
        "explicit",
        "github",
        "harness",
        "issue",
        "label",
        "manual",
        "operator",
        "pending",
        "request",
        "supervised",
        "target",
        "their",
        "there",
        "these",
        "those",
        "through",
        "widen",
        "would",
    ];

    let mut seen = std::collections::HashSet::new();
    let mut terms = Vec::new();
    for text in [
        request.body.as_deref(),
        request.justification.as_deref(),
        request.label.as_deref(),
    ] {
        let Some(text) = text else {
            continue;
        };
        for raw_term in text.split(|character: char| !character.is_ascii_alphanumeric()) {
            let normalized = raw_term.trim().to_ascii_lowercase();
            if normalized.len() < 4
                || STOP_WORDS.contains(&normalized.as_str())
                || normalized
                    .chars()
                    .all(|character| character.is_ascii_digit())
                || !seen.insert(normalized.clone())
            {
                continue;
            }
            terms.push(normalized);
            if terms.len() >= 8 {
                return terms;
            }
        }
    }
    terms
}

fn github_target_keyword_score<'a>(
    texts: impl IntoIterator<Item = &'a str>,
    search_terms: &[String],
) -> usize {
    if search_terms.is_empty() {
        return 0;
    }
    let combined = texts
        .into_iter()
        .map(|text| text.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join(" ");
    search_terms
        .iter()
        .filter(|term| combined.contains(term.as_str()))
        .count()
}

fn build_github_target_suggestions_from(
    request: &SupervisedGithubActionRequest,
    kind: SupervisedGithubActionKind,
    current_branch: Option<&str>,
    branch_pull_requests_json: Option<&str>,
    open_pull_requests_json: Option<&str>,
    open_issues_json: Option<&str>,
) -> Vec<OperatorGithubTargetSuggestion> {
    let search_terms = github_target_search_terms(request);
    match kind {
        SupervisedGithubActionKind::CommentIssue
        | SupervisedGithubActionKind::AssignIssue
        | SupervisedGithubActionKind::CloseIssue
        | SupervisedGithubActionKind::ReopenIssue
        | SupervisedGithubActionKind::LabelIssue
        | SupervisedGithubActionKind::RemoveLabelIssue => {
            let mut matched = Vec::new();
            let mut unmatched = Vec::new();
            for issue in
                parse_github_target_suggestions_json::<GithubIssueSuggestionJson>(open_issues_json)
            {
                let score = github_target_keyword_score([issue.title.as_str()], &search_terms);
                let suggestion = OperatorGithubTargetSuggestion {
                    number: issue.number,
                    title: issue.title,
                    url: issue.url,
                    source: if score > 0 {
                        format!("keyword-matched open issue ({score} hits)")
                    } else {
                        "recent open issue".into()
                    },
                };
                if score > 0 {
                    matched.push((score, suggestion));
                } else {
                    unmatched.push(suggestion);
                }
            }
            matched.sort_by(|left, right| {
                right
                    .0
                    .cmp(&left.0)
                    .then_with(|| left.1.number.cmp(&right.1.number))
            });
            matched
                .into_iter()
                .map(|(_, suggestion)| suggestion)
                .chain(unmatched)
                .collect()
        }
        SupervisedGithubActionKind::CommentPullRequest
        | SupervisedGithubActionKind::AssignPullRequest
        | SupervisedGithubActionKind::ClosePullRequest
        | SupervisedGithubActionKind::ReopenPullRequest
        | SupervisedGithubActionKind::LabelPullRequest
        | SupervisedGithubActionKind::RemoveLabelPullRequest => {
            let mut suggestions = Vec::new();
            let mut seen_numbers = std::collections::HashSet::new();
            let current_branch_label = current_branch
                .map(str::trim)
                .filter(|branch| !branch.is_empty())
                .unwrap_or("current branch")
                .to_string();
            for pull_request in parse_github_target_suggestions_json::<
                GithubPullRequestSuggestionJson,
            >(branch_pull_requests_json)
            {
                if seen_numbers.insert(pull_request.number) {
                    suggestions.push(OperatorGithubTargetSuggestion {
                        number: pull_request.number,
                        title: pull_request.title,
                        url: pull_request.url,
                        source: format!("open PR for {current_branch_label}"),
                    });
                }
            }
            let mut matched = Vec::new();
            let mut unmatched = Vec::new();
            for pull_request in parse_github_target_suggestions_json::<
                GithubPullRequestSuggestionJson,
            >(open_pull_requests_json)
            {
                if seen_numbers.insert(pull_request.number) {
                    let score = github_target_keyword_score(
                        [
                            pull_request.title.as_str(),
                            pull_request.head_ref_name.as_deref().unwrap_or_default(),
                        ],
                        &search_terms,
                    );
                    let base_source = pull_request
                        .head_ref_name
                        .as_deref()
                        .filter(|head| !head.trim().is_empty())
                        .map(|head| format!("recent open PR from {head}"))
                        .unwrap_or_else(|| "recent open PR".into());
                    let suggestion = OperatorGithubTargetSuggestion {
                        number: pull_request.number,
                        title: pull_request.title,
                        url: pull_request.url,
                        source: if score > 0 {
                            base_source.replacen("recent", "keyword-matched", 1)
                        } else {
                            base_source
                        },
                    };
                    if score > 0 {
                        matched.push((score, suggestion));
                    } else {
                        unmatched.push(suggestion);
                    }
                }
            }
            matched.sort_by(|left, right| {
                right
                    .0
                    .cmp(&left.0)
                    .then_with(|| left.1.number.cmp(&right.1.number))
            });
            suggestions.extend(matched.into_iter().map(|(_, suggestion)| suggestion));
            suggestions.extend(unmatched);
            suggestions
        }
    }
}

fn build_github_target_guidance(
    paths: &OperatorPaths,
    request: &SupervisedGithubActionRequest,
    suggestions: &[OperatorGithubTargetSuggestion],
) -> Option<String> {
    if !request.requires_operator_target() {
        return None;
    }

    let target_kind = request.operator_target_kind();
    if !suggestions.is_empty() {
        return Some(format!(
            "Use one of the suggested {target_kind}s below, or enter a different {target_kind} number manually if this draft belongs somewhere else."
        ));
    }

    match request.kind {
        SupervisedGithubActionKind::CommentIssue
        | SupervisedGithubActionKind::AssignIssue
        | SupervisedGithubActionKind::CloseIssue
        | SupervisedGithubActionKind::ReopenIssue
        | SupervisedGithubActionKind::LabelIssue
        | SupervisedGithubActionKind::RemoveLabelIssue => Some(
            "No recent open issue suggestion is available right now. Enter the intended issue number manually, or reject/clear this draft if it should not be applied.".into(),
        ),
        SupervisedGithubActionKind::CommentPullRequest
        | SupervisedGithubActionKind::AssignPullRequest
        | SupervisedGithubActionKind::ClosePullRequest
        | SupervisedGithubActionKind::ReopenPullRequest
        | SupervisedGithubActionKind::LabelPullRequest
        | SupervisedGithubActionKind::RemoveLabelPullRequest => {
            let current_branch = run_repo_command_capture(
                paths.repo_root.as_path(),
                "git",
                &["branch", "--show-current"],
            );
            let branch_hint = current_branch
                .as_deref()
                .map(str::trim)
                .filter(|branch| !branch.is_empty())
                .map(|branch| format!(" for current branch `{branch}`"))
                .unwrap_or_default();
            Some(format!(
                "No open pull request suggestion is available{branch_hint}. Open or locate the intended pull request, then enter its number manually. This panel refreshes automatically, so a new matching pull request should appear here once GitHub reports it."
            ))
        }
    }
}

fn discover_github_target_suggestions(
    paths: &OperatorPaths,
    request: &SupervisedGithubActionRequest,
) -> Vec<OperatorGithubTargetSuggestion> {
    if !request.requires_operator_target() {
        return Vec::new();
    }

    let repo_root = paths.repo_root.as_path();
    let current_branch = run_repo_command_capture(repo_root, "git", &["branch", "--show-current"]);
    let branch_pull_requests_json = match request.kind {
        SupervisedGithubActionKind::CommentPullRequest
        | SupervisedGithubActionKind::AssignPullRequest
        | SupervisedGithubActionKind::ClosePullRequest
        | SupervisedGithubActionKind::ReopenPullRequest
        | SupervisedGithubActionKind::LabelPullRequest
        | SupervisedGithubActionKind::RemoveLabelPullRequest => {
            current_branch.as_deref().and_then(|branch| {
                run_repo_command_capture(
                    repo_root,
                    "gh",
                    &[
                        "pr",
                        "list",
                        "--state",
                        "open",
                        "--head",
                        branch,
                        "--limit",
                        "5",
                        "--json",
                        "number,title,url,headRefName",
                    ],
                )
            })
        }
        SupervisedGithubActionKind::CommentIssue
        | SupervisedGithubActionKind::AssignIssue
        | SupervisedGithubActionKind::CloseIssue
        | SupervisedGithubActionKind::ReopenIssue
        | SupervisedGithubActionKind::LabelIssue
        | SupervisedGithubActionKind::RemoveLabelIssue => None,
    };
    let open_pull_requests_json = match request.kind {
        SupervisedGithubActionKind::CommentPullRequest
        | SupervisedGithubActionKind::AssignPullRequest
        | SupervisedGithubActionKind::ClosePullRequest
        | SupervisedGithubActionKind::ReopenPullRequest
        | SupervisedGithubActionKind::LabelPullRequest
        | SupervisedGithubActionKind::RemoveLabelPullRequest => run_repo_command_capture(
            repo_root,
            "gh",
            &[
                "pr",
                "list",
                "--state",
                "open",
                "--limit",
                "20",
                "--json",
                "number,title,url,headRefName",
            ],
        ),
        SupervisedGithubActionKind::CommentIssue
        | SupervisedGithubActionKind::AssignIssue
        | SupervisedGithubActionKind::CloseIssue
        | SupervisedGithubActionKind::ReopenIssue
        | SupervisedGithubActionKind::LabelIssue
        | SupervisedGithubActionKind::RemoveLabelIssue => None,
    };
    let open_issues_json = match request.kind {
        SupervisedGithubActionKind::CommentIssue
        | SupervisedGithubActionKind::AssignIssue
        | SupervisedGithubActionKind::CloseIssue
        | SupervisedGithubActionKind::ReopenIssue
        | SupervisedGithubActionKind::LabelIssue
        | SupervisedGithubActionKind::RemoveLabelIssue => run_repo_command_capture(
            repo_root,
            "gh",
            &[
                "issue",
                "list",
                "--state",
                "open",
                "--limit",
                "20",
                "--json",
                "number,title,url",
            ],
        ),
        SupervisedGithubActionKind::CommentPullRequest
        | SupervisedGithubActionKind::AssignPullRequest
        | SupervisedGithubActionKind::ClosePullRequest
        | SupervisedGithubActionKind::ReopenPullRequest
        | SupervisedGithubActionKind::LabelPullRequest
        | SupervisedGithubActionKind::RemoveLabelPullRequest => None,
    };

    build_github_target_suggestions_from(
        request,
        request.kind,
        current_branch.as_deref(),
        branch_pull_requests_json.as_deref(),
        open_pull_requests_json.as_deref(),
        open_issues_json.as_deref(),
    )
}

fn build_project_artifact_context(
    paths: &OperatorPaths,
    objective: &str,
) -> anyhow::Result<Option<String>> {
    if !objective_needs_project_artifact_context(objective) {
        return Ok(None);
    }

    let sections = [
        (
            "Project Current Plan",
            paths
                .repo_root
                .join("artifacts")
                .join("ultimentality-pilot")
                .join("current-plan.md"),
        ),
        (
            "Project Roadmap",
            paths
                .repo_root
                .join("artifacts")
                .join("ultimentality-pilot")
                .join("roadmap.md"),
        ),
        (
            "Certainty Superslice",
            paths
                .repo_root
                .join("artifacts")
                .join("analysis")
                .join("certainty-superslice.md"),
        ),
        (
            "Current Open Gaps",
            paths
                .repo_root
                .join("offload")
                .join("current")
                .join("open-gaps.md"),
        ),
        (
            "Current Handoff",
            paths
                .repo_root
                .join("offload")
                .join("current")
                .join("handoff.md"),
        ),
        ("Operator Status", paths.status_path.clone()),
        (
            "Pending GitHub Request",
            paths.github_action_request_path.clone(),
        ),
        (
            "GitHub Request History",
            paths.github_action_history_path.clone(),
        ),
    ];

    let mut bundle = Vec::new();
    for (label, path) in sections {
        if let Some(body) = read_text_if_exists(path.clone())? {
            let trimmed = body.trim();
            if !trimmed.is_empty() {
                bundle.push(format!(
                    "## {label}\nsource: {}\n\n{}",
                    path.display(),
                    trimmed
                ));
            }
        }
    }

    if let Some(repo_context) = build_repo_git_context(paths) {
        bundle.push(repo_context);
    }
    if let Some(github_context) = build_github_cli_context(paths) {
        bundle.push(github_context);
    }

    if bundle.is_empty() {
        return Ok(None);
    }

    Ok(Some(bundle.join("\n\n")))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GithubCliCommand {
    args: Vec<String>,
}

fn parse_optional_github_target_override(raw: Option<&str>) -> anyhow::Result<Option<u64>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    trimmed
        .parse::<u64>()
        .with_context(|| format!("parse GitHub target number from `{trimmed}`"))
        .map(Some)
}

fn apply_github_target_override(
    request: &SupervisedGithubActionRequest,
    target_override: Option<u64>,
) -> anyhow::Result<SupervisedGithubActionRequest> {
    if request.requires_operator_target() {
        let target_override = target_override.ok_or_else(|| {
            anyhow!(
                "GitHub request needs a {} number before it can be applied",
                request.operator_target_kind()
            )
        })?;
        return Ok(request.with_operator_target_number(target_override));
    }

    Ok(request.clone())
}

fn build_github_action_command(
    request: &SupervisedGithubActionRequest,
) -> anyhow::Result<GithubCliCommand> {
    let mut args = Vec::new();
    match request.kind {
        splcw_orchestrator::SupervisedGithubActionKind::CommentIssue => {
            args.extend(["issue".into(), "comment".into()]);
            args.push(
                request
                    .issue_number
                    .context("comment_issue request is missing issue_number")?
                    .to_string(),
            );
        }
        splcw_orchestrator::SupervisedGithubActionKind::AssignIssue => {
            args.extend(["issue".into(), "edit".into()]);
            args.push(
                request
                    .issue_number
                    .context("assign_issue request is missing issue_number")?
                    .to_string(),
            );
        }
        splcw_orchestrator::SupervisedGithubActionKind::CloseIssue => {
            args.extend(["issue".into(), "close".into()]);
            args.push(
                request
                    .issue_number
                    .context("close_issue request is missing issue_number")?
                    .to_string(),
            );
        }
        splcw_orchestrator::SupervisedGithubActionKind::ReopenIssue => {
            args.extend(["issue".into(), "reopen".into()]);
            args.push(
                request
                    .issue_number
                    .context("reopen_issue request is missing issue_number")?
                    .to_string(),
            );
        }
        splcw_orchestrator::SupervisedGithubActionKind::LabelIssue => {
            args.extend(["issue".into(), "edit".into()]);
            args.push(
                request
                    .issue_number
                    .context("label_issue request is missing issue_number")?
                    .to_string(),
            );
        }
        splcw_orchestrator::SupervisedGithubActionKind::RemoveLabelIssue => {
            args.extend(["issue".into(), "edit".into()]);
            args.push(
                request
                    .issue_number
                    .context("remove_label_issue request is missing issue_number")?
                    .to_string(),
            );
        }
        splcw_orchestrator::SupervisedGithubActionKind::CommentPullRequest => {
            args.extend(["pr".into(), "comment".into()]);
            args.push(
                request
                    .pull_request_number
                    .context("comment_pull_request request is missing pull_request_number")?
                    .to_string(),
            );
        }
        splcw_orchestrator::SupervisedGithubActionKind::AssignPullRequest => {
            args.extend(["pr".into(), "edit".into()]);
            args.push(
                request
                    .pull_request_number
                    .context("assign_pull_request request is missing pull_request_number")?
                    .to_string(),
            );
        }
        splcw_orchestrator::SupervisedGithubActionKind::ClosePullRequest => {
            args.extend(["pr".into(), "close".into()]);
            args.push(
                request
                    .pull_request_number
                    .context("close_pull_request request is missing pull_request_number")?
                    .to_string(),
            );
        }
        splcw_orchestrator::SupervisedGithubActionKind::ReopenPullRequest => {
            args.extend(["pr".into(), "reopen".into()]);
            args.push(
                request
                    .pull_request_number
                    .context("reopen_pull_request request is missing pull_request_number")?
                    .to_string(),
            );
        }
        splcw_orchestrator::SupervisedGithubActionKind::LabelPullRequest => {
            args.extend(["pr".into(), "edit".into()]);
            args.push(
                request
                    .pull_request_number
                    .context("label_pull_request request is missing pull_request_number")?
                    .to_string(),
            );
        }
        splcw_orchestrator::SupervisedGithubActionKind::RemoveLabelPullRequest => {
            args.extend(["pr".into(), "edit".into()]);
            args.push(
                request
                    .pull_request_number
                    .context("remove_label_pull_request request is missing pull_request_number")?
                    .to_string(),
            );
        }
    }

    if let Some(repository) = request.repository.as_deref() {
        args.push("--repo".into());
        args.push(repository.into());
    }

    match request.kind {
        splcw_orchestrator::SupervisedGithubActionKind::CommentIssue
        | splcw_orchestrator::SupervisedGithubActionKind::CommentPullRequest => {
            args.push("--body".into());
            args.push(
                request
                    .body
                    .clone()
                    .context("comment request is missing body")?,
            );
        }
        splcw_orchestrator::SupervisedGithubActionKind::AssignIssue
        | splcw_orchestrator::SupervisedGithubActionKind::AssignPullRequest => {
            args.push("--add-assignee".into());
            args.push(
                request
                    .assignee
                    .clone()
                    .context("assign request is missing assignee")?,
            );
        }
        splcw_orchestrator::SupervisedGithubActionKind::CloseIssue => {
            if let Some(body) = request.body.clone() {
                args.push("--comment".into());
                args.push(body);
            }
        }
        splcw_orchestrator::SupervisedGithubActionKind::ReopenIssue => {
            if let Some(body) = request.body.clone() {
                args.push("--comment".into());
                args.push(body);
            }
        }
        splcw_orchestrator::SupervisedGithubActionKind::ClosePullRequest => {
            if let Some(body) = request.body.clone() {
                args.push("--comment".into());
                args.push(body);
            }
        }
        splcw_orchestrator::SupervisedGithubActionKind::ReopenPullRequest => {
            if let Some(body) = request.body.clone() {
                args.push("--comment".into());
                args.push(body);
            }
        }
        splcw_orchestrator::SupervisedGithubActionKind::LabelIssue
        | splcw_orchestrator::SupervisedGithubActionKind::LabelPullRequest => {
            args.push("--add-label".into());
            args.push(
                request
                    .label
                    .clone()
                    .context("label request is missing label")?,
            );
        }
        splcw_orchestrator::SupervisedGithubActionKind::RemoveLabelIssue
        | splcw_orchestrator::SupervisedGithubActionKind::RemoveLabelPullRequest => {
            args.push("--remove-label".into());
            args.push(
                request
                    .label
                    .clone()
                    .context("remove label request is missing label")?,
            );
        }
    }

    Ok(GithubCliCommand { args })
}

fn run_repo_command_checked(
    repo_root: &Path,
    program: &str,
    args: &[String],
) -> anyhow::Result<String> {
    let output = Command::new(program)
        .args(args)
        .current_dir(repo_root)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| {
            format!(
                "run {} {} in {}",
                program,
                args.join(" "),
                repo_root.display()
            )
        })?;

    if output.status.success() {
        return Ok(String::from_utf8_lossy(&output.stdout).trim().to_string());
    }

    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let detail = if !stderr.is_empty() { stderr } else { stdout };
    Err(anyhow!(
        "{} {} failed: {}",
        program,
        args.join(" "),
        if detail.is_empty() {
            format!("exit code {:?}", output.status.code())
        } else {
            detail
        }
    ))
}

fn build_output_command(command_path: &Path, cwd: &Path, args: &[String]) -> Command {
    #[cfg(target_os = "windows")]
    {
        let extension = command_path
            .extension()
            .and_then(|value| value.to_str())
            .map(|value| value.to_ascii_lowercase());
        if let Some((program, bootstrap_args)) = windows_codex_npm_shim_command(command_path) {
            let mut command = Command::new(program);
            command.args(bootstrap_args);
            command.args(args);
            command.current_dir(cwd);
            command.stdin(Stdio::null());
            command.stdout(Stdio::piped());
            command.stderr(Stdio::piped());
            return command;
        }
        if matches!(extension.as_deref(), Some("cmd" | "bat")) {
            let mut command = Command::new("cmd.exe");
            command.arg("/d").arg("/c").arg(command_path);
            command.args(args);
            command.current_dir(cwd);
            command.stdin(Stdio::null());
            command.stdout(Stdio::piped());
            command.stderr(Stdio::piped());
            return command;
        }
    }

    let mut command = Command::new(command_path);
    command.args(args);
    command.current_dir(cwd);
    command.stdin(Stdio::null());
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    command
}

#[cfg(target_os = "windows")]
fn windows_codex_npm_shim_command(command_path: &Path) -> Option<(PathBuf, Vec<String>)> {
    let file_stem = command_path
        .file_stem()
        .and_then(|value| value.to_str())?
        .to_ascii_lowercase();
    if file_stem != "codex" {
        return None;
    }
    let extension = command_path
        .extension()
        .and_then(|value| value.to_str())?
        .to_ascii_lowercase();
    if !matches!(extension.as_str(), "cmd" | "bat") {
        return None;
    }
    let shim_dir = command_path.parent()?;
    let script_path = shim_dir
        .join("node_modules")
        .join("@openai")
        .join("codex")
        .join("bin")
        .join("codex.js");
    if !script_path.exists() {
        return None;
    }
    let node_path = if shim_dir.join("node.exe").exists() {
        shim_dir.join("node.exe")
    } else {
        PathBuf::from("node")
    };
    Some((node_path, vec![script_path.display().to_string()]))
}

fn run_command_checked(command_path: &Path, cwd: &Path, args: &[String]) -> anyhow::Result<String> {
    let output = build_output_command(command_path, cwd, args)
        .output()
        .with_context(|| {
            format!(
                "run {} {} in {}",
                command_path.display(),
                args.join(" "),
                cwd.display()
            )
        })?;

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Ok(if stdout.is_empty() { stderr } else { stdout });
    }

    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let detail = if !stderr.is_empty() { stderr } else { stdout };
    Err(anyhow!(
        "{} {} failed: {}",
        command_path.display(),
        args.join(" "),
        if detail.is_empty() {
            format!("exit code {:?}", output.status.code())
        } else {
            detail
        }
    ))
}

fn run_command_stream_with_stdin(
    command_path: &Path,
    cwd: &Path,
    args: &[String],
    stdin_bytes: &[u8],
    live_stream_path: &Path,
    objective: &str,
) -> anyhow::Result<CapturedCommandOutput> {
    let mut child = build_output_command(command_path, cwd, args)
        .stdin(Stdio::piped())
        .spawn()
        .with_context(|| {
            format!(
                "run {} {} in {}",
                command_path.display(),
                args.join(" "),
                cwd.display()
            )
        })?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(stdin_bytes)
            .with_context(|| format!("write stdin for {}", command_path.display()))?;
    }

    let stdout = child
        .stdout
        .take()
        .context("capture Codex CLI stdout stream")?;
    let stderr = child
        .stderr
        .take()
        .context("capture Codex CLI stderr stream")?;

    let now = Utc::now();
    let live_state = Arc::new(Mutex::new(CodexCliLiveStreamState {
        started_at: now,
        updated_at: now,
        active: true,
        objective: normalize_text(objective, DEFAULT_OBJECTIVE),
        session_id: None,
        latest_text: String::new(),
        event_lines: Vec::new(),
        warning_lines: Vec::new(),
    }));
    write_json_atomic(
        live_stream_path,
        &live_state.lock().expect("live stream poisoned").clone(),
    )?;

    let stdout_state = Arc::clone(&live_state);
    let stdout_path = live_stream_path.to_path_buf();
    let stdout_handle = std::thread::spawn(move || -> anyhow::Result<Vec<u8>> {
        let mut reader = BufReader::new(stdout);
        let mut buffer = Vec::new();
        let mut line = String::new();
        loop {
            line.clear();
            let read = reader.read_line(&mut line)?;
            if read == 0 {
                break;
            }
            buffer.extend_from_slice(line.as_bytes());
            update_codex_cli_live_stream_from_stdout_line(&stdout_state, &stdout_path, &line);
        }
        Ok(buffer)
    });

    let stderr_state = Arc::clone(&live_state);
    let stderr_path = live_stream_path.to_path_buf();
    let stderr_handle = std::thread::spawn(move || -> anyhow::Result<Vec<u8>> {
        let mut reader = BufReader::new(stderr);
        let mut buffer = Vec::new();
        let mut line = String::new();
        loop {
            line.clear();
            let read = reader.read_line(&mut line)?;
            if read == 0 {
                break;
            }
            buffer.extend_from_slice(line.as_bytes());
            update_codex_cli_live_stream_from_stderr_line(&stderr_state, &stderr_path, &line);
        }
        Ok(buffer)
    });

    let status = child.wait().with_context(|| {
        format!(
            "wait for {} {} in {}",
            command_path.display(),
            args.join(" "),
            cwd.display()
        )
    })?;

    let stdout = stdout_handle
        .join()
        .map_err(|_| anyhow!("join stdout reader thread"))??;
    let stderr = stderr_handle
        .join()
        .map_err(|_| anyhow!("join stderr reader thread"))??;

    let final_state = {
        let mut stream = live_state.lock().expect("live stream poisoned");
        stream.active = false;
        stream.updated_at = Utc::now();
        stream.clone()
    };
    write_json_atomic(live_stream_path, &final_state)?;

    Ok(CapturedCommandOutput {
        status,
        stdout,
        stderr,
    })
}

#[derive(Debug)]
struct ParsedCodexCliExecOutput {
    session_id: Option<String>,
    reply: String,
    summary: String,
    event_lines: Vec<String>,
    warning_lines: Vec<String>,
}

fn append_grounding_section(
    sections: &mut Vec<String>,
    label: &str,
    path: PathBuf,
) -> anyhow::Result<()> {
    if let Some(body) = read_text_if_exists(path.clone())? {
        let trimmed = body.trim();
        if !trimmed.is_empty() {
            sections.push(format!(
                "## {label}\nsource: {}\n\n{}",
                path.display(),
                trimmed
            ));
        }
    }
    Ok(())
}

fn build_runtime_grounding_bundle(paths: &OperatorPaths) -> anyhow::Result<Option<String>> {
    let mut sections = Vec::new();

    append_grounding_section(
        &mut sections,
        "Operating System Context",
        paths.repo_root
            .join("artifacts")
            .join("ultimentality-pilot")
            .join("memory")
            .join("os.md"),
    )?;
    append_grounding_section(
        &mut sections,
        "Working Memory",
        paths.repo_root
            .join("artifacts")
            .join("ultimentality-pilot")
            .join("memory")
            .join("memory.md"),
    )?;
    append_grounding_section(
        &mut sections,
        "Baseline Recovery Context",
        paths.repo_root
            .join("artifacts")
            .join("ultimentality-pilot")
            .join("baseline")
            .join("clean-splcw-harness-2026-04-03.md"),
    )?;
    append_grounding_section(
        &mut sections,
        "Current Plan",
        paths.repo_root
            .join("artifacts")
            .join("ultimentality-pilot")
            .join("current-plan.md"),
    )?;
    append_grounding_section(
        &mut sections,
        "Current Brief",
        paths.repo_root.join("offload").join("current").join("brief.md"),
    )?;
    append_grounding_section(
        &mut sections,
        "Current Plan Mirror",
        paths.repo_root.join("offload").join("current").join("plan.md"),
    )?;
    append_grounding_section(
        &mut sections,
        "Current Open Gaps",
        paths.repo_root.join("offload").join("current").join("open-gaps.md"),
    )?;
    append_grounding_section(
        &mut sections,
        "Current Handoff",
        paths.repo_root.join("offload").join("current").join("handoff.md"),
    )?;

    if let Some(repo_context) = build_repo_git_context(paths) {
        sections.push(repo_context);
    }
    if let Some(github_context) = build_github_cli_context(paths) {
        sections.push(github_context);
    }

    if sections.is_empty() {
        Ok(None)
    } else {
        Ok(Some(sections.join("\n\n")))
    }
}

fn build_codex_cli_context_prompt(
    paths: &OperatorPaths,
    objective: &str,
    grounding_bundle: Option<&str>,
) -> String {
    let grounding = grounding_bundle.unwrap_or(
        "Grounding bundle unavailable. Rehydrate manually from artifacts/ultimentality-pilot/memory/, artifacts/ultimentality-pilot/current-plan.md, and offload/current/ before acting.",
    );
    format!(
        "You are Codex running inside the AGRO / AIM repo at `{}`.\n\
\n\
Treat the following operating-memory bundle as already loaded for this turn. Use it before replying, and prefer grounded use of the existing host, verification, memory, and orchestrator tools over generic repo chatter.\n\
\n\
# Operating Memory Bundle\n\
\n\
{}\n\
\n\
# Additional Reference Files\n\
\n\
- `README.md`\n\
- `ultimentality-pilot/harness/ARCHITECTURE.md`\n\
- `artifacts/ultimentality-pilot/roadmap.md`\n\
\n\
Current objective:\n\
{}\n",
        paths.repo_root.display(),
        grounding,
        normalize_text(objective, DEFAULT_OBJECTIVE)
    )
}

fn summarize_codex_cli_reply(reply: &str) -> String {
    let first_line = reply
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("Codex CLI completed without a visible reply.");
    truncate_for_summary(first_line, 180)
}

fn truncate_for_summary(text: &str, max_chars: usize) -> String {
    let mut chars = text.chars();
    let truncated: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

fn append_limited(lines: &mut Vec<String>, line: String, limit: usize) {
    lines.push(line);
    if lines.len() > limit {
        let overflow = lines.len() - limit;
        lines.drain(0..overflow);
    }
}

enum CodexCliTextUpdate {
    Replace(String),
    Append(String),
}

fn codex_cli_text_update_from_event(event: &serde_json::Value) -> Option<CodexCliTextUpdate> {
    let kind = event.get("type")?.as_str()?;
    if kind == "item.completed" {
        let item = event.get("item")?;
        if item.get("type").and_then(|value| value.as_str()) == Some("agent_message") {
            if let Some(text) = item.get("text").and_then(|value| value.as_str()) {
                return Some(CodexCliTextUpdate::Replace(text.to_string()));
            }
        }
    }

    if kind.contains("delta") {
        let delta = event
            .get("delta")
            .and_then(|value| value.as_str())
            .or_else(|| event.get("text").and_then(|value| value.as_str()))
            .or_else(|| {
                event.get("item")
                    .and_then(|item| item.get("delta"))
                    .and_then(|value| value.as_str())
            })
            .or_else(|| {
                event.get("item")
                    .and_then(|item| item.get("text"))
                    .and_then(|value| value.as_str())
            });
        if let Some(delta) = delta {
            if !delta.trim().is_empty() {
                return Some(CodexCliTextUpdate::Append(delta.to_string()));
            }
        }
    }

    None
}

fn format_codex_cli_event_line(event: &serde_json::Value) -> Option<String> {
    let kind = event.get("type")?.as_str()?;
    match kind {
        "thread.started" => Some(format!(
            "thread.started {}",
            event.get("thread_id")?.as_str().unwrap_or("unknown")
        )),
        "turn.started" => Some("turn.started".into()),
        "turn.completed" => Some("turn.completed".into()),
        "item.completed" => {
            let item = event.get("item")?;
            match item.get("type")?.as_str()? {
                "agent_message" => Some(format!(
                    "agent_message {}",
                    truncate_for_summary(item.get("text")?.as_str().unwrap_or_default(), 120)
                )),
                "command_execution" => Some(format!(
                    "command_execution {}",
                    truncate_for_summary(item.get("command")?.as_str().unwrap_or_default(), 120)
                )),
                other => Some(format!("item.completed {other}")),
            }
        }
        "item.started" => {
            let item = event.get("item")?;
            let item_type = item.get("type")?.as_str().unwrap_or("unknown");
            Some(format!("item.started {item_type}"))
        }
        other => Some(other.to_string()),
    }
}

fn parse_codex_cli_exec_output(stdout: &str, stderr: &str) -> ParsedCodexCliExecOutput {
    let mut session_id = None;
    let mut replies = Vec::new();
    let mut event_lines = Vec::new();
    for line in stdout.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<serde_json::Value>(trimmed) {
            Ok(event) => {
                if let Some(event_type) = event.get("type").and_then(|value| value.as_str()) {
                    if event_type == "thread.started" {
                        session_id = event
                            .get("thread_id")
                            .and_then(|value| value.as_str())
                            .map(str::to_string);
                    }
                    if event_type == "item.completed" {
                        let item = event.get("item").cloned().unwrap_or_default();
                        if item.get("type").and_then(|value| value.as_str())
                            == Some("agent_message")
                        {
                            if let Some(text) = item.get("text").and_then(|value| value.as_str()) {
                                replies.push(text.to_string());
                            }
                        }
                    }
                }
                if let Some(formatted) = format_codex_cli_event_line(&event) {
                    event_lines.push(formatted);
                }
            }
            Err(_) => event_lines.push(format!("stdout {}", truncate_for_summary(trimmed, 160))),
        }
    }

    let warning_lines = stderr
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    let reply = replies.join("\n\n");
    let summary = summarize_codex_cli_reply(&reply);
    ParsedCodexCliExecOutput {
        session_id,
        reply,
        summary,
        event_lines,
        warning_lines,
    }
}

fn update_codex_cli_live_stream_from_stdout_line(
    state: &Arc<Mutex<CodexCliLiveStreamState>>,
    live_stream_path: &Path,
    line: &str,
) {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return;
    }

    let snapshot = {
        let mut stream = state.lock().expect("live stream poisoned");
        stream.updated_at = Utc::now();
        match serde_json::from_str::<serde_json::Value>(trimmed) {
            Ok(event) => {
                if stream.session_id.is_none() {
                    if event.get("type").and_then(|value| value.as_str()) == Some("thread.started")
                    {
                        stream.session_id = event
                            .get("thread_id")
                            .and_then(|value| value.as_str())
                            .map(str::to_string);
                    }
                }
                if let Some(formatted) = format_codex_cli_event_line(&event) {
                    append_limited(
                        &mut stream.event_lines,
                        formatted,
                        RECENT_LIVE_STREAM_LINE_LIMIT,
                    );
                }
                if let Some(update) = codex_cli_text_update_from_event(&event) {
                    match update {
                        CodexCliTextUpdate::Replace(text) => stream.latest_text = text,
                        CodexCliTextUpdate::Append(delta) => stream.latest_text.push_str(&delta),
                    }
                }
            }
            Err(_) => append_limited(
                &mut stream.event_lines,
                format!("stdout {}", truncate_for_summary(trimmed, 160)),
                RECENT_LIVE_STREAM_LINE_LIMIT,
            ),
        }
        stream.clone()
    };
    let _ = write_json_atomic(live_stream_path, &snapshot);
}

fn update_codex_cli_live_stream_from_stderr_line(
    state: &Arc<Mutex<CodexCliLiveStreamState>>,
    live_stream_path: &Path,
    line: &str,
) {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return;
    }
    let snapshot = {
        let mut stream = state.lock().expect("live stream poisoned");
        stream.updated_at = Utc::now();
        append_limited(
            &mut stream.warning_lines,
            trimmed.to_string(),
            RECENT_LIVE_STREAM_LINE_LIMIT,
        );
        stream.clone()
    };
    let _ = write_json_atomic(live_stream_path, &snapshot);
}

fn format_codex_cli_turn_reply(turn: &CodexCliTurnRecord) -> String {
    let mut sections = vec![
        format!("# Reply · {}", turn.recorded_at.to_rfc3339()),
        format!("- **Engine:** `Codex CLI`"),
        format!("- **Model:** `{}`", turn.model),
    ];
    if let Some(session_id) = turn.session_id.as_deref() {
        sections.push(format!("- **Session:** `{session_id}`"));
    }
    sections.push("\n## Response Content".into());
    if turn.reply.trim().is_empty() {
        sections.push("\nNo agent_message reply was recorded for this CLI turn.".into());
    } else {
        sections.push(format!("\n{}", turn.reply.trim()));
    }
    if !turn.warning_lines.is_empty() {
        sections.push("\n## CLI Warnings".into());
        sections.extend(turn.warning_lines.iter().map(|line| format!("- {line}")));
    }
    sections.join("\n")
}

fn parse_codex_login_status(output: &str) -> (bool, Option<String>) {
    let trimmed = output.trim();
    if trimmed.is_empty() {
        return (false, None);
    }
    let logged_in = trimmed.contains("Logged in");
    (logged_in, Some(trimmed.to_string()))
}

fn read_json_file<T>(path: PathBuf) -> anyhow::Result<Option<T>>
where
    T: for<'de> Deserialize<'de>,
{
    if !path.exists() {
        return Ok(None);
    }
    let body = fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    let parsed = serde_json::from_str(&body)
        .with_context(|| format!("parse json from {}", path.display()))?;
    Ok(Some(parsed))
}

fn read_recent_jsonl_entries<T>(path: PathBuf, limit: usize) -> anyhow::Result<Vec<T>>
where
    T: for<'de> Deserialize<'de>,
{
    if !path.exists() {
        return Ok(Vec::new());
    }
    let body = fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    let mut entries = Vec::new();
    for line in body.lines().rev() {
        if line.trim().is_empty() {
            continue;
        }
        let parsed = serde_json::from_str(line)
            .with_context(|| format!("parse jsonl entry from {}", path.display()))?;
        entries.push(parsed);
        if entries.len() >= limit {
            break;
        }
    }
    entries.reverse();
    Ok(entries)
}

fn append_jsonl_entry<T>(path: &Path, value: &T) -> anyhow::Result<()>
where
    T: Serialize,
{
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create parent for {}", path.display()))?;
    }
    let line = format!(
        "{}\n",
        serde_json::to_string(value)
            .with_context(|| format!("serialize jsonl for {}", path.display()))?
    );
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open {}", path.display()))?;
    file.write_all(line.as_bytes())
        .with_context(|| format!("append {}", path.display()))?;
    file.flush()
        .with_context(|| format!("flush {}", path.display()))?;
    Ok(())
}

fn format_session_event(event: &RuntimeSessionEvent) -> String {
    format!(
        "{} | {:?} | {}",
        event.recorded_at.to_rfc3339(),
        event.kind,
        event.summary
    )
}

fn format_github_action_history_entry(record: &OperatorGithubActionLifecycleRecord) -> String {
    format!(
        "{} | {} | {}",
        record.recorded_at.to_rfc3339(),
        record.state.as_label(),
        record.summary
    )
}

fn extract_first_url(text: &str) -> Option<String> {
    text.split_whitespace().find_map(|token| {
        let trimmed = token.trim_matches(|character: char| {
            matches!(
                character,
                '"' | '\'' | '(' | ')' | '[' | ']' | '{' | '}' | ',' | ';'
            )
        });
        (trimmed.starts_with("http://") || trimmed.starts_with("https://"))
            .then(|| trimmed.to_string())
    })
}

fn summarize_github_action_result(output: &str) -> (Option<String>, Option<String>) {
    let excerpt = output
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(str::to_string);
    let url = extract_first_url(output);
    (excerpt, url)
}

fn queued_github_action_history_record(
    request: &OperatorGithubActionRequestRecord,
) -> OperatorGithubActionLifecycleRecord {
    OperatorGithubActionLifecycleRecord {
        recorded_at: request.requested_at,
        state: OperatorGithubActionLifecycleState::Queued,
        thread_id: request.thread_id.clone(),
        thread_label: request.thread_label.clone(),
        request: request.request.clone(),
        summary: format!("queued GitHub request: {}", request.summary),
        detail: request.narrative.clone(),
        result_excerpt: None,
        result_url: None,
    }
}

fn format_runtime_turn_reply(turn: &RuntimeTurnRecord) -> String {
    let mut sections = vec![
        format!("# Reply · {}", turn.recorded_at.to_rfc3339()),
        format!("- **Provider:** `{}`", turn.provider_id),
        format!("- **Model:** `{}`", turn.model),
        format!("- **Thread:** `{}`", turn.thread_id),
        format!("- **Turn ID:** `{}`", turn.turn_id),
    ];

    if !turn.narrative.trim().is_empty() {
        sections.push(format!("## Narrative\n\n{}", turn.narrative.trim()));
    }

    sections.push(format!(
        "## Response Content\n\n{}",
        format_response_content(&turn.response)
    ));

    if let Some(outcome) = &turn.tool_outcome {
        sections.push(format!(
            "## Recorded Tool Outcome\n\n{}",
            format_serialized_value(outcome)
        ));
    }

    if let Some(gap) = &turn.surfaced_gap {
        sections.push(format!(
            "## Surfaced Capability Gap\n\n{}",
            format_serialized_value(gap)
        ));
    }

    sections.join("\n\n")
}

fn format_response_content(response: &ChatResponse) -> String {
    if response.content.is_empty() {
        return "No response blocks were recorded for this provider round.".into();
    }

    response
        .content
        .iter()
        .enumerate()
        .map(|(index, block)| match block {
            ContentBlock::Text { text } => {
                let body = if text.trim().is_empty() {
                    "The provider returned an empty text block.".to_string()
                } else {
                    text.trim().to_string()
                };
                format!("## Text Block {}\n\n{}", index + 1, body)
            }
            ContentBlock::ImagePath { path } => {
                format!("## Image Block {}\n\n- **Path:** `{path}`", index + 1)
            }
            ContentBlock::ToolCall {
                id,
                name,
                arguments,
            } => format!(
                "## Tool Call {} · `{}`\n\n- **Call ID:** `{}`\n\n{}",
                index + 1,
                name,
                id,
                format_serialized_value(arguments)
            ),
            ContentBlock::ToolResult { id, content } => format!(
                "## Tool Result {}\n\n- **Call ID:** `{}`\n\n{}",
                index + 1,
                id,
                format_serialized_value(content)
            ),
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn format_serialized_value<T>(value: &T) -> String
where
    T: Serialize,
{
    serde_json::to_string_pretty(value)
        .unwrap_or_else(|error| format!("{{\"serialization_error\":\"{}\"}}", error))
}

fn write_json_atomic<T>(path: &Path, value: &T) -> anyhow::Result<()>
where
    T: Serialize,
{
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create parent for {}", path.display()))?;
    }
    let body = format!(
        "{}\n",
        serde_json::to_string_pretty(value)
            .with_context(|| format!("serialize json for {}", path.display()))?
    );
    let temp_path = path.with_extension(format!("{}.tmp", Uuid::new_v4()));
    fs::write(&temp_path, body).with_context(|| format!("write {}", temp_path.display()))?;
    if path.exists() {
        fs::remove_file(path).with_context(|| format!("remove {}", path.display()))?;
    }
    fs::rename(&temp_path, path)
        .with_context(|| format!("replace {} with {}", path.display(), temp_path.display()))?;
    Ok(())
}

fn sleep_while_requested(loop_requested: &AtomicBool, duration: Duration) -> bool {
    if duration.is_zero() {
        return loop_requested.load(Ordering::SeqCst);
    }
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        if !loop_requested.load(Ordering::SeqCst) {
            return false;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        std::thread::sleep(remaining.min(Duration::from_millis(100)));
    }
    loop_requested.load(Ordering::SeqCst)
}

fn should_continue_run_loop(
    run_mode: OperatorRunMode,
    loop_requested: bool,
    terminal: OperatorTurnTerminal,
) -> bool {
    matches!(run_mode, OperatorRunMode::Continuous)
        && loop_requested
        && matches!(terminal, OperatorTurnTerminal::Completed)
}

fn parse_run_mode(value: &str) -> OperatorRunMode {
    match value {
        "single_turn" => OperatorRunMode::SingleTurn,
        "continuous" => OperatorRunMode::Continuous,
        _ => OperatorRunMode::Idle,
    }
}

fn effective_operator_status(
    run_state: &str,
    run_mode: OperatorRunMode,
    summary: &str,
    background_runner: Option<&OperatorBackgroundRunnerState>,
    background_runner_disposition: Option<BackgroundRunnerDisposition>,
) -> (String, OperatorRunMode, String) {
    match (background_runner, background_runner_disposition) {
        (Some(runner), Some(BackgroundRunnerDisposition::Live)) => (
            "background_looping".into(),
            OperatorRunMode::Continuous,
            runner
                .last_summary
                .clone()
                .unwrap_or_else(|| "background loop active".into()),
        ),
        (Some(runner), Some(BackgroundRunnerDisposition::TerminalIdle)) => (
            "idle".into(),
            OperatorRunMode::Idle,
            runner
                .last_summary
                .clone()
                .unwrap_or_else(|| format!("background runner {} ended", runner.runner_id)),
        ),
        (Some(runner), Some(BackgroundRunnerDisposition::TerminalError)) => (
            "error".into(),
            OperatorRunMode::Idle,
            runner
                .last_summary
                .clone()
                .unwrap_or_else(|| format!("background runner {} failed", runner.runner_id)),
        ),
        (Some(runner), Some(BackgroundRunnerDisposition::Crashed)) => (
            "idle".into(),
            OperatorRunMode::Idle,
            if runner.phase == "crashed" {
                runner.last_summary.clone().unwrap_or_else(|| {
                    format!(
                        "background runner {} became stale during {} after {}",
                        runner.runner_id,
                        runner.phase,
                        format_background_runner_age(runner.updated_at)
                    )
                })
            } else {
                format!(
                    "background runner {} became stale during {} after {}",
                    runner.runner_id,
                    runner.phase,
                    format_background_runner_age(runner.updated_at)
                )
            },
        ),
        (None, _) if run_state == "background_looping" => {
            ("idle".into(), OperatorRunMode::Idle, summary.into())
        }
        _ => (run_state.into(), run_mode, summary.into()),
    }
}

fn background_runner_is_stale(state: &OperatorBackgroundRunnerState) -> bool {
    let age = Utc::now()
        .signed_duration_since(state.updated_at)
        .to_std()
        .unwrap_or_default();
    age > BACKGROUND_RUNNER_STALE_AFTER
}

fn background_runner_is_terminal(state: &OperatorBackgroundRunnerState) -> bool {
    matches!(state.phase.as_str(), "stop_requested" | "surfaced_gap")
}

fn background_runner_process_is_alive(pid: u32) -> bool {
    let pid = Pid::from_u32(pid);
    let mut system = System::new();
    system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
    system.process(pid).is_some()
}

fn background_runner_owner_shell_alive(state: &OperatorBackgroundRunnerState) -> Option<bool> {
    state
        .owner_shell_pid
        .map(background_runner_process_is_alive)
}

fn reconcile_crashed_background_runner<F>(
    runner: &OperatorBackgroundRunnerState,
    is_process_alive: F,
) -> Option<(String, String, String)>
where
    F: Fn(u32) -> bool,
{
    if runner.phase == "crashed" {
        return None;
    }

    if !background_runner_is_terminal(runner)
        && runner.phase != "failed"
        && !is_process_alive(runner.pid)
    {
        return Some((
            format!(
                "background runner {} process {} exited or disappeared during {}",
                runner.runner_id, runner.pid, runner.phase
            ),
            format!(
                "background runner process {} was no longer running during {}",
                runner.pid, runner.phase
            ),
            format!(
                "reconciled background runner {} into explicit crashed state after process {} disappeared",
                runner.runner_id, runner.pid
            ),
        ));
    }

    if matches!(
        classify_background_runner(runner),
        BackgroundRunnerDisposition::Crashed
    ) {
        let stale_for = format_background_runner_age(runner.updated_at);
        return Some((
            format!(
                "background runner {} crashed or stopped reporting during {} after {}",
                runner.runner_id, runner.phase, stale_for
            ),
            format!(
                "background runner stopped reporting during {} after {}",
                runner.phase, stale_for
            ),
            format!(
                "reconciled stale background runner {} into explicit crashed state",
                runner.runner_id
            ),
        ));
    }

    None
}

fn classify_background_runner(
    state: &OperatorBackgroundRunnerState,
) -> BackgroundRunnerDisposition {
    if matches!(state.phase.as_str(), "failed") {
        BackgroundRunnerDisposition::TerminalError
    } else if matches!(state.phase.as_str(), "crashed") {
        BackgroundRunnerDisposition::Crashed
    } else if background_runner_is_terminal(state) {
        BackgroundRunnerDisposition::TerminalIdle
    } else if background_runner_is_stale(state) {
        BackgroundRunnerDisposition::Crashed
    } else {
        BackgroundRunnerDisposition::Live
    }
}

fn background_recovery_recommendation(
    state: &OperatorBackgroundRunnerState,
    disposition: Option<BackgroundRunnerDisposition>,
) -> Option<String> {
    match disposition {
        Some(BackgroundRunnerDisposition::TerminalError) => Some(format!(
            "Inspect the preserved detached-runner error for {} and relaunch the background loop once the failure cause is fixed.",
            state.runner_id
        )),
        Some(BackgroundRunnerDisposition::Crashed) => Some(format!(
            "Review the preserved detached-runner state for {}, reconcile any stale stop or handoff artifacts, then relaunch or replace the background loop.",
            state.runner_id
        )),
        _ => None,
    }
}

fn background_reattach_recommendation(
    state: &OperatorBackgroundRunnerState,
    disposition: Option<BackgroundRunnerDisposition>,
    owner_shell_alive: Option<bool>,
) -> Option<String> {
    if !matches!(disposition, Some(BackgroundRunnerDisposition::Live)) {
        return None;
    }

    match (state.owner_shell_id.as_deref(), owner_shell_alive) {
        (Some(_), Some(false)) => Some(format!(
            "The detached runner {} is still live but its launching shell is gone. Reattach from this shell before stopping, handing off, or replacing it.",
            state.runner_id
        )),
        (None, _) | (_, None) => Some(format!(
            "The detached runner {} is live but no launch-shell ownership is recorded. Reattach from this shell before controlling it.",
            state.runner_id
        )),
        _ => None,
    }
}

fn recoverable_background_settings(snapshot: &OperatorSnapshot) -> Option<(RunSettings, f32)> {
    let status = snapshot.background_runner_status.as_deref()?;
    if !matches!(status, "crashed" | "terminal_error") {
        return None;
    }
    let engine_mode = match snapshot.background_runner_engine_mode.as_deref() {
        Some("Codex CLI") => OperatorEngineMode::CodexCli,
        _ => OperatorEngineMode::NativeHarness,
    };
    Some((
        RunSettings {
            objective: snapshot.background_runner_objective.clone()?,
            model: snapshot.background_runner_model.clone()?,
            thread_id: snapshot.background_runner_thread_id.clone()?,
            thread_label: snapshot.background_runner_thread_label.clone()?,
            engine_mode,
        },
        snapshot
            .background_runner_loop_pause_seconds
            .unwrap_or(DEFAULT_LOOP_PAUSE_SECONDS)
            .max(0.0),
    ))
}

fn background_recovery_action_label(snapshot: &OperatorSnapshot) -> Option<&'static str> {
    match snapshot.background_runner_status.as_deref() {
        Some("crashed") if recoverable_background_settings(snapshot).is_some() => {
            Some("Recover Crashed Background Loop")
        }
        Some("terminal_error") if recoverable_background_settings(snapshot).is_some() => {
            Some("Retry Failed Background Loop")
        }
        _ => None,
    }
}

fn background_runner_allows_spawn(disposition: Option<BackgroundRunnerDisposition>) -> bool {
    !matches!(disposition, Some(BackgroundRunnerDisposition::Live))
}

fn classify_background_handoff(
    request: &OperatorBackgroundHandoffRequest,
    background_runner: Option<&OperatorBackgroundRunnerState>,
    background_runner_disposition: Option<BackgroundRunnerDisposition>,
) -> BackgroundHandoffDisposition {
    match (
        request.target_runner_id.as_deref(),
        background_runner.map(|runner| runner.runner_id.as_str()),
    ) {
        (Some(target), Some(current)) if target != current => {
            return BackgroundHandoffDisposition::Obsolete;
        }
        _ => {}
    }

    if !matches!(
        background_runner_disposition,
        Some(BackgroundRunnerDisposition::Live)
    ) {
        return BackgroundHandoffDisposition::ReadyToLaunch;
    }

    BackgroundHandoffDisposition::WaitingForTarget
}

fn build_background_runner_state(
    runner_id: &str,
    pid: u32,
    owner_shell_id: Option<&str>,
    owner_shell_pid: Option<u32>,
    started_at: DateTime<Utc>,
    phase: &str,
    settings: &RunSettings,
    loop_pause_seconds: f32,
    completed_turn_count: u64,
    last_summary: Option<String>,
    last_error: Option<String>,
) -> OperatorBackgroundRunnerState {
    OperatorBackgroundRunnerState {
        runner_id: runner_id.to_string(),
        pid,
        owner_shell_id: owner_shell_id.map(ToOwned::to_owned),
        owner_shell_pid,
        started_at,
        updated_at: Utc::now(),
        phase: phase.into(),
        loop_pause_seconds: loop_pause_seconds.max(0.0),
        objective: settings.objective.clone(),
        model: settings.model.clone(),
        thread_id: settings.thread_id.clone(),
        thread_label: settings.thread_label.clone(),
        engine_mode: settings.engine_mode,
        completed_turn_count,
        last_summary,
        last_error,
    }
}

fn provisional_background_runner_state(
    runner_id: &str,
    pid: u32,
    owner_shell_id: Option<&str>,
    owner_shell_pid: Option<u32>,
    settings: &RunSettings,
    loop_pause_seconds: f32,
) -> OperatorBackgroundRunnerState {
    build_background_runner_state(
        runner_id,
        pid,
        owner_shell_id,
        owner_shell_pid,
        Utc::now(),
        "launching",
        settings,
        loop_pause_seconds,
        0,
        Some("background loop launching".into()),
        None,
    )
}

fn bootstrap_background_runner_state(
    existing: Option<&OperatorBackgroundRunnerState>,
    runner_id: &str,
    pid: u32,
    owner_shell_id: Option<&str>,
    owner_shell_pid: Option<u32>,
    settings: &RunSettings,
    loop_pause_seconds: f32,
) -> OperatorBackgroundRunnerState {
    let existing = existing.filter(|state| state.runner_id == runner_id);
    let started_at = existing
        .map(|state| state.started_at)
        .unwrap_or_else(Utc::now);
    let owner_shell_id = existing
        .and_then(|state| state.owner_shell_id.as_deref())
        .or(owner_shell_id);
    let owner_shell_pid = existing
        .and_then(|state| state.owner_shell_pid)
        .or(owner_shell_pid);
    build_background_runner_state(
        runner_id,
        pid,
        owner_shell_id,
        owner_shell_pid,
        started_at,
        "starting",
        settings,
        loop_pause_seconds,
        0,
        Some("background loop starting".into()),
        None,
    )
}

fn resolve_interactive_oauth_client_id_with<F>(
    provider: OperatorAuthProvider,
    get_env: F,
) -> Option<(&'static str, String)>
where
    F: Fn(&str) -> Option<String>,
{
    provider
        .oauth_client_id_env_names()
        .iter()
        .find_map(|name| {
            get_env(name)
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .map(|value| (*name, value))
        })
}

fn interactive_oauth_launch_status_with<F>(
    provider: OperatorAuthProvider,
    get_env: F,
) -> InteractiveOAuthLaunchStatus
where
    F: Fn(&str) -> Option<String>,
{
    match resolve_interactive_oauth_client_id_with(provider, get_env) {
        Some((name, _)) => InteractiveOAuthLaunchStatus {
            ready: true,
            summary: format!("ready via {name}"),
            env_name: Some(name),
            built_in: false,
        },
        None if builtin_interactive_oauth_client_id(&provider.as_provider_kind()).is_some() => {
            InteractiveOAuthLaunchStatus {
                ready: true,
                summary: "ready via built-in OpenAI Codex client id".into(),
                env_name: None,
                built_in: true,
            }
        }
        None => InteractiveOAuthLaunchStatus {
            ready: false,
            summary: format!(
                "blocked: set one of {}",
                provider.oauth_client_id_env_names().join(", ")
            ),
            env_name: None,
            built_in: false,
        },
    }
}

fn interactive_oauth_launch_status(provider: OperatorAuthProvider) -> InteractiveOAuthLaunchStatus {
    interactive_oauth_launch_status_with(provider, |name| env::var(name).ok())
}

fn discover_openclaw_command_with<F>(get_env: F, allow_shell_probe: bool) -> Option<PathBuf>
where
    F: Fn(&str) -> Option<String>,
{
    if let Some(configured) = get_env("OPENCLAW_BIN")
        .as_deref()
        .and_then(normalize_optional_text)
        .map(PathBuf::from)
    {
        if configured.exists() {
            return Some(configured);
        }
    }

    let appdata = get_env("APPDATA").or_else(|| env::var("APPDATA").ok());
    if let Some(appdata) = appdata
        .as_deref()
        .and_then(normalize_optional_text)
        .map(PathBuf::from)
    {
        for candidate in [
            appdata.join("npm").join("openclaw.cmd"),
            appdata.join("npm").join("openclaw.ps1"),
            appdata.join("npm").join("openclaw"),
        ] {
            if candidate.exists() {
                return Some(candidate);
            }
        }
    }

    if allow_shell_probe {
        #[cfg(target_os = "windows")]
        {
            if let Ok(output) = Command::new("where.exe")
                .arg("openclaw")
                .stdin(Stdio::null())
                .stderr(Stdio::null())
                .output()
            {
                if output.status.success() {
                    for line in String::from_utf8_lossy(&output.stdout).lines() {
                        let candidate = PathBuf::from(line.trim());
                        if candidate.exists() {
                            return Some(candidate);
                        }
                    }
                }
            }
        }
    }

    None
}

fn discover_codex_command_with<F>(get_env: F, allow_shell_probe: bool) -> Option<PathBuf>
where
    F: Fn(&str) -> Option<String>,
{
    if let Some(path) = get_env("CODEX_BIN")
        .as_deref()
        .and_then(normalize_optional_text)
        .map(PathBuf::from)
    {
        if path.exists() {
            return Some(path);
        }
    }

    let appdata = get_env("APPDATA").or_else(|| env::var("APPDATA").ok());
    if let Some(appdata) = appdata
        .as_deref()
        .and_then(normalize_optional_text)
        .map(PathBuf::from)
    {
        for candidate in [
            appdata.join("npm").join("codex.cmd"),
            appdata.join("npm").join("codex"),
            appdata.join("npm").join("codex.ps1"),
        ] {
            if candidate.exists() {
                return Some(candidate);
            }
        }
    }

    for candidate in [
        env::current_exe()
            .ok()
            .and_then(|path| path.parent().map(|parent| parent.join("codex"))),
        env::current_exe()
            .ok()
            .and_then(|path| path.parent().map(|parent| parent.join("codex.exe"))),
    ]
    .into_iter()
    .flatten()
    {
        if candidate.exists() {
            return Some(candidate);
        }
    }

    if allow_shell_probe {
        #[cfg(target_os = "windows")]
        {
            if let Ok(output) = Command::new("where.exe")
                .arg("codex")
                .stdin(Stdio::null())
                .stderr(Stdio::null())
                .output()
            {
                if output.status.success() {
                    for line in String::from_utf8_lossy(&output.stdout).lines() {
                        let candidate = PathBuf::from(line.trim());
                        if candidate.exists() {
                            return Some(candidate);
                        }
                    }
                }
            }
        }
    }

    None
}

fn discover_codex_command() -> Option<PathBuf> {
    discover_codex_command_with(|name| env::var(name).ok(), true)
}

fn codex_cli_status_with<F>(get_env: F, cwd: &Path) -> CodexCliStatus
where
    F: Fn(&str) -> Option<String>,
{
    let Some(path) = discover_codex_command_with(get_env, true) else {
        return CodexCliStatus {
            available: false,
            logged_in: false,
            summary: "blocked: install Codex CLI or set CODEX_BIN in operator.env".into(),
            command_path: None,
            account_summary: None,
        };
    };

    let args = vec!["login".into(), "status".into()];
    match run_command_checked(&path, cwd, &args) {
        Ok(output) => {
            let (logged_in, account_summary) = parse_codex_login_status(&output);
            CodexCliStatus {
                available: true,
                logged_in,
                summary: if logged_in {
                    format!("ready via {} | {}", path.display(), output.trim())
                } else {
                    format!("available via {} | {}", path.display(), output.trim())
                },
                command_path: Some(path),
                account_summary,
            }
        }
        Err(error) => CodexCliStatus {
            available: true,
            logged_in: false,
            summary: format!(
                "login status unavailable via {} ({error:#})",
                path.display()
            ),
            command_path: Some(path),
            account_summary: None,
        },
    }
}

fn codex_cli_status(paths: &OperatorPaths) -> CodexCliStatus {
    codex_cli_status_with(|name| env::var(name).ok(), &paths.repo_root)
}

fn discover_openclaw_command() -> Option<PathBuf> {
    discover_openclaw_command_with(|name| env::var(name).ok(), true)
}

fn openclaw_cli_status_with<F>(get_env: F) -> OpenClawCliStatus
where
    F: Fn(&str) -> Option<String>,
{
    match discover_openclaw_command_with(get_env, false) {
        Some(path) => OpenClawCliStatus {
            available: true,
            summary: format!("ready via {}", path.display()),
            command_path: Some(path),
        },
        None => OpenClawCliStatus {
            available: false,
            summary: "blocked: install OpenClaw or set OPENCLAW_BIN in operator.env".into(),
            command_path: None,
        },
    }
}

fn openclaw_cli_status() -> OpenClawCliStatus {
    openclaw_cli_status_with(|name| env::var(name).ok())
}

fn resolve_openclaw_agent_dir_with<F>(get_env: F) -> Option<PathBuf>
where
    F: Fn(&str) -> Option<String>,
{
    ["OPENCLAW_AGENT_DIR", "PI_CODING_AGENT_DIR"]
        .into_iter()
        .find_map(|name| {
            get_env(name)
                .as_deref()
                .and_then(normalize_optional_text)
                .map(PathBuf::from)
        })
}

fn resolve_openclaw_state_dir_with<F>(get_env: F) -> Option<PathBuf>
where
    F: Fn(&str) -> Option<String>,
{
    get_env("OPENCLAW_STATE_DIR")
        .as_deref()
        .and_then(normalize_optional_text)
        .map(PathBuf::from)
        .or_else(|| {
            env::var("USERPROFILE")
                .ok()
                .map(|value| PathBuf::from(value).join(".openclaw"))
        })
}

fn discover_openclaw_auth_store_paths(
    agent_dir: Option<&Path>,
    state_dir: Option<&Path>,
) -> anyhow::Result<Vec<PathBuf>> {
    let mut candidates = Vec::new();
    if let Some(agent_dir) = agent_dir {
        candidates.push(agent_dir.join("auth-profiles.json"));
    }
    if let Some(state_dir) = state_dir {
        candidates.push(state_dir.join("agent").join("auth-profiles.json"));
        let main_agent = state_dir
            .join("agents")
            .join("main")
            .join("agent")
            .join("auth-profiles.json");
        if !candidates.iter().any(|candidate| candidate == &main_agent) {
            candidates.push(main_agent);
        }
        let agents_root = state_dir.join("agents");
        if agents_root.exists() {
            for entry in fs::read_dir(&agents_root)
                .with_context(|| format!("read {}", agents_root.display()))?
            {
                let entry = entry?;
                let path = entry.path().join("agent").join("auth-profiles.json");
                if !candidates.iter().any(|candidate| candidate == &path) {
                    candidates.push(path);
                }
            }
        }
    }
    Ok(candidates
        .into_iter()
        .filter(|path| path.exists())
        .collect::<Vec<_>>())
}

fn openclaw_profile_rank(
    profile_id: &str,
    credential: &OpenClawRawCredential,
    last_good: &HashMap<String, String>,
    usage_stats: &HashMap<String, OpenClawUsageStats>,
) -> (i32, i64, i64, String) {
    let provider = credential
        .provider
        .as_deref()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let provider_last_good = last_good
        .get(provider.as_str())
        .map(|value| value == profile_id)
        .unwrap_or(false);
    let is_default = profile_id.eq_ignore_ascii_case("openai-codex:default");
    let last_used = usage_stats
        .get(profile_id)
        .and_then(|stats| stats.last_used)
        .unwrap_or(0);
    let expires = credential.expires.unwrap_or(0);
    (
        i32::from(provider_last_good) + i32::from(is_default),
        last_used,
        expires,
        profile_id.to_string(),
    )
}

fn build_openclaw_import_profile(
    profile_id: &str,
    credential: &OpenClawRawCredential,
) -> Option<OpenClawImportProfile> {
    let kind = credential
        .kind
        .as_deref()
        .or(credential.mode.as_deref())
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if kind != "oauth" {
        return None;
    }
    let provider = credential
        .provider
        .as_deref()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if provider != "openai-codex" {
        return None;
    }
    let access_token = credential
        .access
        .as_deref()
        .and_then(normalize_optional_text);
    let refresh_token = credential
        .refresh
        .as_deref()
        .and_then(normalize_optional_text);
    if access_token.is_none() && refresh_token.is_none() {
        return None;
    }
    let expires_at = credential
        .expires
        .filter(|value| *value > 0)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    let account_label = credential
        .display_name
        .as_deref()
        .and_then(normalize_optional_text)
        .or_else(|| {
            credential
                .email
                .as_deref()
                .and_then(normalize_optional_text)
        })
        .or_else(|| {
            credential
                .account_id
                .as_deref()
                .and_then(normalize_optional_text)
        });
    Some(OpenClawImportProfile {
        source_profile_id: profile_id.to_string(),
        provider: ProviderKind::OpenAiCodex,
        label: format!("OpenClaw Codex [{profile_id}]"),
        account_label,
        access_token,
        refresh_token,
        expires_at,
    })
}

fn load_openclaw_import_plan_from_paths(
    paths: &[PathBuf],
) -> anyhow::Result<Option<OpenClawImportPlan>> {
    let mut best_plan: Option<(i32, i64, i64, bool, String, OpenClawImportPlan)> = None;
    for path in paths {
        let body = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        let store: OpenClawAuthProfileStoreFile = serde_json::from_str(&body)
            .with_context(|| format!("deserialize {}", path.display()))?;
        let mut importable = store
            .profiles
            .iter()
            .filter_map(|(profile_id, credential)| {
                build_openclaw_import_profile(profile_id, credential).map(|profile| {
                    (
                        openclaw_profile_rank(
                            profile_id,
                            credential,
                            &store.last_good,
                            &store.usage_stats,
                        ),
                        profile,
                    )
                })
            })
            .collect::<Vec<_>>();
        if importable.is_empty() {
            continue;
        }
        importable.sort_by(|left, right| right.0.cmp(&left.0));
        let preferred_profile = importable[0].0.clone();
        let is_main = path
            .to_string_lossy()
            .replace('\\', "/")
            .contains("/agents/main/agent/auth-profiles.json");
        let plan = OpenClawImportPlan {
            source_path: path.clone(),
            profiles: importable.into_iter().map(|(_, profile)| profile).collect(),
            preferred_index: 0,
        };
        let candidate = (
            preferred_profile.0,
            preferred_profile.1,
            preferred_profile.2,
            is_main,
            preferred_profile.3,
            plan,
        );
        if best_plan
            .as_ref()
            .map(|current| {
                candidate.0 > current.0
                    || (candidate.0 == current.0 && candidate.1 > current.1)
                    || (candidate.0 == current.0
                        && candidate.1 == current.1
                        && candidate.2 > current.2)
                    || (candidate.0 == current.0
                        && candidate.1 == current.1
                        && candidate.2 == current.2
                        && candidate.3
                        && !current.3)
                    || (candidate.0 == current.0
                        && candidate.1 == current.1
                        && candidate.2 == current.2
                        && candidate.3 == current.3
                        && candidate.4 < current.4)
            })
            .unwrap_or(true)
        {
            best_plan = Some(candidate);
        }
    }
    Ok(best_plan.map(|(_, _, _, _, _, plan)| plan))
}

fn discover_openclaw_import_plan() -> anyhow::Result<Option<OpenClawImportPlan>> {
    let agent_dir = resolve_openclaw_agent_dir_with(|name| env::var(name).ok());
    let state_dir = resolve_openclaw_state_dir_with(|name| env::var(name).ok());
    let paths = discover_openclaw_auth_store_paths(agent_dir.as_deref(), state_dir.as_deref())?;
    load_openclaw_import_plan_from_paths(&paths)
}

fn openclaw_import_status() -> OpenClawImportStatus {
    match discover_openclaw_import_plan() {
        Ok(Some(plan)) => OpenClawImportStatus {
            available: true,
            summary: format!(
                "ready from {} ({} Codex profile{})",
                plan.source_path.display(),
                plan.profiles.len(),
                if plan.profiles.len() == 1 { "" } else { "s" }
            ),
        },
        Ok(None) => OpenClawImportStatus {
            available: false,
            summary: "blocked: no OpenClaw Codex auth store found; set OPENCLAW_STATE_DIR / OPENCLAW_AGENT_DIR or sign in via OpenClaw first".into(),
        },
        Err(error) => OpenClawImportStatus {
            available: false,
            summary: format!("blocked: could not inspect OpenClaw auth store ({error:#})"),
        },
    }
}

async fn maybe_bootstrap_openclaw_codex_auth_into_store(
    auth_store: &FileAuthProfileStore,
    plan: Option<OpenClawImportPlan>,
) -> anyhow::Result<Option<OpenClawImportOutcome>> {
    if auth_store.load_default_profile().await?.is_some() {
        return Ok(None);
    }
    let Some(plan) = plan else {
        return Ok(None);
    };
    let source_path = plan.source_path.clone();
    let (imported_count, default_label, default_source_profile_id) =
        import_openclaw_plan_into_auth_store(auth_store, &plan).await?;
    Ok(Some(OpenClawImportOutcome {
        source_path,
        imported_count,
        default_label,
        default_source_profile_id,
    }))
}

async fn import_openclaw_plan_into_auth_store(
    auth_store: &FileAuthProfileStore,
    plan: &OpenClawImportPlan,
) -> anyhow::Result<(usize, String, String)> {
    let existing_profiles = auth_store.list_profiles().await?;
    let mut imported_count = 0usize;
    let mut preferred_profile_id = None;
    for (index, imported) in plan.profiles.iter().enumerate() {
        let profile_id = existing_profiles
            .iter()
            .find(|profile| {
                profile.provider == imported.provider
                    && profile.mode == splcw_llm::AuthMode::OAuth
                    && profile.label == imported.label
            })
            .map(|profile| profile.id)
            .unwrap_or_else(Uuid::new_v4);
        let profile = AuthProfile {
            id: profile_id,
            provider: imported.provider.clone(),
            mode: splcw_llm::AuthMode::OAuth,
            label: imported.label.clone(),
            oauth: Some(splcw_llm::OAuthState {
                issuer: "openai".into(),
                account_label: imported.account_label.clone(),
                access_token: imported.access_token.clone(),
                refresh_token: imported.refresh_token.clone(),
                expires_at: imported.expires_at,
            }),
            api_key: None,
            updated_at: Utc::now(),
        };
        auth_store.upsert_profile(&profile).await?;
        if index == plan.preferred_index {
            preferred_profile_id = Some(profile_id);
        }
        imported_count += 1;
    }
    let preferred_profile_id =
        preferred_profile_id.context("OpenClaw import plan did not include a preferred profile")?;
    auth_store.set_default_profile(preferred_profile_id).await?;
    Ok((
        imported_count,
        plan.profiles[plan.preferred_index].label.clone(),
        plan.profiles[plan.preferred_index]
            .source_profile_id
            .clone(),
    ))
}

fn ensure_operator_env_template(path: &Path) -> anyhow::Result<()> {
    if path.exists() {
        return Ok(());
    }
    let template = [
        "# AGRO Harness operator environment",
        "# Fill the overrides you need, then relaunch the operator.",
        "# This file is loaded automatically for direct EXE and packaged launches.",
        "# CODEX_BIN can point the GUI at an installed official Codex CLI and is the primary engine override.",
        "# OpenAI Codex can launch OAuth with the built-in client id by default.",
        "# OPENCLAW_BIN can point the GUI at an installed OpenClaw CLI for native Codex login.",
        "# OPENCLAW_STATE_DIR or OPENCLAW_AGENT_DIR can point the GUI at an existing OpenClaw auth store.",
        "",
        "# CODEX_BIN=",
        "# SPLCW_OPENAI_CODEX_OAUTH_CLIENT_ID=",
        "# SPLCW_OPENAI_API_OAUTH_CLIENT_ID=",
        "# SPLCW_OPENAI_OAUTH_CLIENT_ID=",
        "# OPENAI_OAUTH_CLIENT_ID=",
        "# OPENCLAW_BIN=",
        "# OPENCLAW_STATE_DIR=",
        "# OPENCLAW_AGENT_DIR=",
        "",
    ]
    .join("\n");
    fs::write(path, template).with_context(|| format!("write {}", path.display()))
}

fn parse_operator_env_assignments(contents: &str) -> Vec<(String, String)> {
    contents
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                return None;
            }
            let line = trimmed.strip_prefix("export ").unwrap_or(trimmed);
            let (key, value) = line.split_once('=')?;
            let key = key.trim();
            if !SUPPORTED_OPERATOR_ENV_NAMES.contains(&key) {
                return None;
            }
            let value = value.trim().trim_matches('"').trim_matches('\'').trim();
            if value.is_empty() {
                return None;
            }
            Some((key.to_string(), value.to_string()))
        })
        .collect()
}

fn read_operator_env_assignments(path: &Path) -> anyhow::Result<Vec<(String, String)>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let contents = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    Ok(parse_operator_env_assignments(&contents))
}

fn operator_env_key_for_provider<'a>(
    provider: OperatorAuthProvider,
    configured_keys: &'a [String],
) -> Option<&'a str> {
    provider
        .oauth_client_id_env_names()
        .iter()
        .find_map(|candidate| {
            configured_keys
                .iter()
                .find(|key| key.as_str() == *candidate)
                .map(|key| key.as_str())
        })
}

fn operator_env_config_status(
    provider: OperatorAuthProvider,
    configured_keys: &[String],
    launch_status: &InteractiveOAuthLaunchStatus,
) -> OperatorEnvConfigStatus {
    if let Some(key) = operator_env_key_for_provider(provider, configured_keys) {
        return OperatorEnvConfigStatus {
            configured: true,
            summary: format!("configured for selected provider via {key}"),
        };
    }
    if let Some(env_name) = launch_status.env_name {
        return OperatorEnvConfigStatus {
            configured: false,
            summary: format!(
                "missing selected-provider client id in operator.env; current readiness comes from process env via {env_name}"
            ),
        };
    }
    if launch_status.built_in {
        return OperatorEnvConfigStatus {
            configured: false,
            summary:
                "selected provider is using the built-in OpenAI Codex client id; operator.env override is optional".into(),
        };
    }
    OperatorEnvConfigStatus {
        configured: false,
        summary: format!(
            "missing selected-provider client id; add one of {}",
            provider.oauth_client_id_env_names().join(", ")
        ),
    }
}

fn apply_operator_env_assignments_with<GetEnv, SetEnv>(
    assignments: &[(String, String)],
    get_env: GetEnv,
    mut set_env: SetEnv,
) -> Vec<String>
where
    GetEnv: Fn(&str) -> Option<String>,
    SetEnv: FnMut(&str, &str),
{
    let mut applied = Vec::new();
    for (key, value) in assignments {
        if get_env(key.as_str())
            .map(|current| !current.trim().is_empty())
            .unwrap_or(false)
        {
            continue;
        }
        set_env(key.as_str(), value.as_str());
        applied.push(key.clone());
    }
    applied
}

fn load_operator_env_overlay(path: &Path) -> anyhow::Result<Vec<String>> {
    let assignments = read_operator_env_assignments(path)?;
    Ok(apply_operator_env_assignments_with(
        &assignments,
        |name| env::var(name).ok(),
        |key, value| {
            // SAFETY: this runs during controller construction before the operator starts
            // background workers or UI-triggered threads that consult environment variables.
            unsafe { env::set_var(key, value) };
        },
    ))
}

fn pending_oauth_launch_url(pending: &PendingOAuthAuthorization) -> Option<(&str, &'static str)> {
    match &pending.kind {
        OAuthAuthorizationKind::BrowserCallback {
            authorization_url, ..
        } => Some((authorization_url.as_str(), "authorization URL")),
        OAuthAuthorizationKind::DeviceCode {
            verification_uri, ..
        } => Some((verification_uri.as_str(), "verification URL")),
    }
}

fn open_external_url(url: &str) -> anyhow::Result<()> {
    #[cfg(target_os = "windows")]
    {
        Command::new("rundll32")
            .arg("url.dll,FileProtocolHandler")
            .arg(url)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .with_context(|| format!("open {url} with the default browser"))?;
        return Ok(());
    }

    #[cfg(target_os = "macos")]
    {
        Command::new("open")
            .arg(url)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .with_context(|| format!("open {url} with the default browser"))?;
        return Ok(());
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    {
        Command::new("xdg-open")
            .arg(url)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .with_context(|| format!("open {url} with the default browser"))?;
        return Ok(());
    }

    #[allow(unreachable_code)]
    Err(anyhow!(
        "opening external URLs is not supported on this platform"
    ))
}

fn format_background_runner_age(updated_at: DateTime<Utc>) -> String {
    let age = Utc::now()
        .signed_duration_since(updated_at)
        .to_std()
        .unwrap_or_default();
    if age.as_secs() < 60 {
        format!("{}s", age.as_secs())
    } else {
        format!("{}m {}s", age.as_secs() / 60, age.as_secs() % 60)
    }
}

fn normalize_optional_text(value: &str) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn combine_optional_notices<I>(notices: I) -> Option<String>
where
    I: IntoIterator<Item = Option<String>>,
{
    let collected = notices
        .into_iter()
        .flatten()
        .filter(|notice| !notice.trim().is_empty())
        .collect::<Vec<_>>();
    if collected.is_empty() {
        None
    } else {
        Some(collected.join(" | "))
    }
}

fn describe_auth_state(
    default_profile: Option<&AuthProfile>,
    pending_oauth: &[PendingOAuthAuthorization],
) -> (bool, String, String) {
    match default_profile {
        Some(profile) => {
            let readiness = inspect_runtime_auth(profile);
            let readiness_text = format_auth_readiness(&readiness);
            let summary = match &profile.oauth {
                Some(oauth) => format!(
                    "default profile={} provider={:?} mode={:?} expires_at={} pending_oauth={}",
                    profile.label,
                    profile.provider,
                    profile.mode,
                    oauth
                        .expires_at
                        .map(|value| value.to_rfc3339())
                        .unwrap_or_else(|| "none".into()),
                    pending_oauth.len()
                ),
                None => format!(
                    "default profile={} provider={:?} mode={:?} pending_oauth={}",
                    profile.label,
                    profile.provider,
                    profile.mode,
                    pending_oauth.len()
                ),
            };
            (
                matches!(readiness, RuntimeAuthReadiness::Ready),
                readiness_text,
                summary,
            )
        }
        None => (
            false,
            "blocked: no default auth profile configured".into(),
            format!(
                "no default auth profile configured | pending_oauth={}",
                pending_oauth.len()
            ),
        ),
    }
}

fn describe_operator_auth_state(
    codex_cli_status: &CodexCliStatus,
    native_auth_ready: bool,
    native_auth_readiness: &str,
    native_auth_summary: &str,
) -> (bool, String, String) {
    match (codex_cli_status.available, codex_cli_status.logged_in, native_auth_ready) {
        (true, true, true) => (
            true,
            "ready via Codex CLI (provider fallback also ready)".into(),
            format!(
                "primary=Codex CLI ready ({}) | provider_fallback={}",
                codex_cli_status.summary, native_auth_summary
            ),
        ),
        (true, true, false) => (
            true,
            "ready via Codex CLI".into(),
            format!(
                "primary=Codex CLI ready ({}) | provider_fallback={}",
                codex_cli_status.summary, native_auth_readiness
            ),
        ),
        (true, false, true) => (
            true,
            "ready via provider fallback (Codex CLI login required for primary lane)".into(),
            format!(
                "primary=Codex CLI login required ({}) | provider_fallback={}",
                codex_cli_status.summary, native_auth_summary
            ),
        ),
        (false, _, true) => (
            true,
            "ready via provider fallback (Codex CLI unavailable)".into(),
            format!(
                "primary=Codex CLI unavailable ({}) | provider_fallback={}",
                codex_cli_status.summary, native_auth_summary
            ),
        ),
        (true, false, false) => (
            false,
            "blocked: Codex CLI login required and provider fallback is not ready".into(),
            format!(
                "primary=Codex CLI login required ({}) | provider_fallback={}",
                codex_cli_status.summary, native_auth_readiness
            ),
        ),
        (false, _, false) => (
            false,
            "blocked: Codex CLI unavailable and provider fallback is not ready".into(),
            format!(
                "primary=Codex CLI unavailable ({}) | provider_fallback={}",
                codex_cli_status.summary, native_auth_readiness
            ),
        ),
    }
}

fn format_auth_readiness(readiness: &RuntimeAuthReadiness) -> String {
    match readiness {
        RuntimeAuthReadiness::Ready => "ready".into(),
        RuntimeAuthReadiness::NeedsRefresh => "needs_refresh".into(),
        RuntimeAuthReadiness::Blocked(reason) => format!("blocked: {reason}"),
    }
}

fn format_auth_preflight_report(report: &splcw_llm::AuthLifecycleResumeReport) -> String {
    let mut parts = Vec::new();
    if !report.cleaned_pending_oauth.is_empty() {
        parts.push(format!(
            "cleaned_pending_oauth={}",
            report.cleaned_pending_oauth.len()
        ));
    }
    if !report.materialized_profiles.is_empty() {
        parts.push(format!(
            "materialized_profiles={}",
            report.materialized_profiles.len()
        ));
    }
    if !report.armed_profiles.is_empty() {
        parts.push(format!("armed_profiles={}", report.armed_profiles.len()));
    }
    if !report.blocked_profiles.is_empty() {
        parts.push(format!(
            "blocked_profiles={}",
            report.blocked_profiles.len()
        ));
    }
    if parts.is_empty() {
        "auth preflight completed with no state changes".into()
    } else {
        format!("auth preflight: {}", parts.join(" | "))
    }
}

fn build_pending_oauth_view(pending: &PendingOAuthAuthorization) -> OperatorPendingOAuthView {
    let (
        kind,
        authorization_url,
        redirect_uri,
        callback_prompt,
        verification_uri,
        user_code,
        action_hint,
    ) = match &pending.kind {
        OAuthAuthorizationKind::BrowserCallback {
            authorization_url,
            redirect_uri,
            paste_prompt,
            ..
        } => (
            "browser_callback".into(),
            Some(authorization_url.clone()),
            Some(redirect_uri.clone()),
            Some(paste_prompt.clone()),
            None,
            None,
            "Paste the callback URL or authorization code, then complete the browser flow.".into(),
        ),
        OAuthAuthorizationKind::DeviceCode {
            verification_uri,
            user_code,
            ..
        } => (
            "device_code".into(),
            None,
            None,
            None,
            Some(verification_uri.clone()),
            Some(user_code.clone()),
            "Open the verification URL, enter the user code, then poll/complete the device flow."
                .into(),
        ),
    };
    OperatorPendingOAuthView {
        id: pending.id.to_string(),
        provider: format!("{:?}", pending.provider),
        label: pending.label.clone(),
        kind,
        started_at: pending.started_at.to_rfc3339(),
        expires_at: pending.expires_at.map(|value| value.to_rfc3339()),
        authorization_url,
        redirect_uri,
        callback_prompt,
        verification_uri,
        user_code,
        action_hint,
    }
}

fn run_async<F, Fut, T>(factory: F) -> anyhow::Result<T>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .context("build current-thread operator runtime")?;
    runtime.block_on(factory())
}

fn parse_operator_command() -> anyhow::Result<OperatorCommand> {
    let mut command = OperatorCommand {
        loop_pause_seconds: DEFAULT_LOOP_PAUSE_SECONDS,
        ..OperatorCommand::default()
    };
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--smoke-test" => command.smoke_test = true,
            "--run-turn" => command.run_turn = true,
            "--background-loop" => command.background_loop = true,
            "--background-runner-id" => {
                command.background_runner_id =
                    Some(next_arg_value(&mut args, "--background-runner-id")?)
            }
            "--background-owner-shell-id" => {
                command.background_owner_shell_id =
                    Some(next_arg_value(&mut args, "--background-owner-shell-id")?)
            }
            "--background-owner-shell-pid" => {
                let raw = next_arg_value(&mut args, "--background-owner-shell-pid")?;
                command.background_owner_shell_pid = Some(
                    raw.parse::<u32>()
                        .with_context(|| format!("parse --background-owner-shell-pid '{raw}'"))?,
                );
            }
            "--objective" => command.settings.objective = next_arg_value(&mut args, "--objective")?,
            "--model" => command.settings.model = next_arg_value(&mut args, "--model")?,
            "--thread-id" => command.settings.thread_id = next_arg_value(&mut args, "--thread-id")?,
            "--thread-label" => {
                command.settings.thread_label = next_arg_value(&mut args, "--thread-label")?
            }
            "--engine-mode" => {
                command.settings.engine_mode =
                    match next_arg_value(&mut args, "--engine-mode")?.as_str() {
                        "codex_cli" => OperatorEngineMode::CodexCli,
                        "native_harness" => OperatorEngineMode::NativeHarness,
                        other => anyhow::bail!("unknown --engine-mode value '{other}'"),
                    };
            }
            "--loop-pause-seconds" => {
                let raw = next_arg_value(&mut args, "--loop-pause-seconds")?;
                command.loop_pause_seconds = raw
                    .parse::<f32>()
                    .with_context(|| format!("parse --loop-pause-seconds value '{raw}'"))?;
            }
            other => return Err(anyhow!("unknown operator argument: {other}")),
        }
    }

    if command.settings.objective.trim().is_empty() {
        command.settings.objective = DEFAULT_OBJECTIVE.into();
    }
    if command.settings.model.trim().is_empty() {
        command.settings.model = DEFAULT_MODEL.into();
    }
    if command.settings.thread_id.trim().is_empty() {
        command.settings.thread_id = DEFAULT_THREAD_ID.into();
    }
    if command.settings.thread_label.trim().is_empty() {
        command.settings.thread_label = DEFAULT_THREAD_LABEL.into();
    }

    Ok(command)
}

fn next_arg_value<I>(args: &mut I, flag: &str) -> anyhow::Result<String>
where
    I: Iterator<Item = String>,
{
    args.next()
        .ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn remove_file_if_exists(path: &Path) -> anyhow::Result<()> {
    if path.exists() {
        fs::remove_file(path).with_context(|| format!("remove {}", path.display()))?;
    }
    Ok(())
}

fn browser_callback_bind_target(redirect_uri: &str) -> anyhow::Result<String> {
    let trimmed = redirect_uri
        .strip_prefix("http://")
        .or_else(|| redirect_uri.strip_prefix("https://"))
        .with_context(|| format!("unsupported redirect uri scheme in {redirect_uri}"))?;
    let host_port = trimmed
        .split('/')
        .next()
        .and_then(normalize_optional_text)
        .context("redirect uri did not include a host/port")?;
    Ok(host_port.to_string())
}

fn extract_http_request_target(request: &str) -> Option<&str> {
    let line = request.lines().next()?;
    let mut parts = line.split_whitespace();
    let _method = parts.next()?;
    parts.next()
}

fn request_target_matches_callback_path(request_target: &str, callback_path: &str) -> bool {
    request_target
        .split('?')
        .next()
        .map(|path| path == callback_path)
        .unwrap_or(false)
}

fn build_browser_callback_url(redirect_uri: &str, request_target: &str) -> String {
    if request_target.starts_with("http://") || request_target.starts_with("https://") {
        return request_target.to_string();
    }
    let trimmed = redirect_uri.trim_end_matches('/');
    let origin = trimmed
        .find("://")
        .and_then(|scheme_index| {
            let after_scheme = &trimmed[scheme_index + 3..];
            after_scheme.find('/').map(|path_index| {
                let absolute_index = scheme_index + 3 + path_index;
                &trimmed[..absolute_index]
            })
        })
        .unwrap_or(trimmed);
    format!("{origin}{request_target}")
}

fn maybe_spawn_browser_oauth_callback_listener(
    controller: Arc<HarnessController>,
    snapshot: Arc<Mutex<OperatorSnapshot>>,
    pending: PendingOAuthAuthorization,
) -> anyhow::Result<Option<String>> {
    let OAuthAuthorizationKind::BrowserCallback {
        redirect_uri,
        callback_path,
        ..
    } = &pending.kind
    else {
        return Ok(None);
    };
    let bind_target = browser_callback_bind_target(redirect_uri)?;
    let bind_target_for_notice = bind_target.clone();
    let redirect_uri = redirect_uri.clone();
    let callback_path = callback_path.clone();
    std::thread::spawn(move || {
        let listener = match TcpListener::bind(bind_target.as_str()) {
            Ok(listener) => listener,
            Err(_) => return,
        };
        if listener.set_nonblocking(true).is_err() {
            return;
        }
        let deadline = Instant::now() + Duration::from_secs(300);
        while Instant::now() < deadline {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let mut buffer = [0u8; 4096];
                    let size = match stream.read(&mut buffer) {
                        Ok(size) => size,
                        Err(_) => 0,
                    };
                    let request = String::from_utf8_lossy(&buffer[..size]).to_string();
                    let Some(request_target) = extract_http_request_target(&request) else {
                        let _ = stream.write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\nInvalid OAuth callback request.");
                        break;
                    };
                    if !request_target_matches_callback_path(request_target, callback_path.as_str())
                    {
                        let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\nUnexpected OAuth callback path.");
                        break;
                    }
                    let callback_url =
                        build_browser_callback_url(redirect_uri.as_str(), request_target);
                    let controller_for_action = controller.clone();
                    let result = run_async(move || async move {
                        controller_for_action
                            .complete_oauth_authorization(pending.id, callback_url.as_str())
                            .await
                    });
                    let message = match result {
                        Ok(summary) => {
                            let current =
                                snapshot.lock().expect("operator snapshot poisoned").clone();
                            let current_for_snapshot = current.clone();
                            let controller_for_snapshot = controller.clone();
                            let summary_for_snapshot = summary.clone();
                            let next = run_async(move || async move {
                                controller_for_snapshot
                                    .read_snapshot(
                                        &current_for_snapshot.run_state,
                                        parse_run_mode(current_for_snapshot.run_mode.as_str()),
                                        &current_for_snapshot.summary,
                                        current_for_snapshot.last_error.clone(),
                                        current_for_snapshot.completed_turn_count,
                                        Some(summary_for_snapshot.clone()),
                                    )
                                    .await
                            })
                            .unwrap_or(OperatorSnapshot {
                                refreshed_at: Some(Utc::now()),
                                run_state: current.run_state,
                                run_mode: current.run_mode,
                                summary: current.summary,
                                last_error: current.last_error,
                                completed_turn_count: current.completed_turn_count,
                                auth_notice: Some(summary.clone()),
                                ..OperatorSnapshot::default()
                            });
                            *snapshot.lock().expect("operator snapshot poisoned") = next;
                            format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nConnection: close\r\n\r\n<html><body><h1>AGRO OAuth complete</h1><p>{}</p><p>You can return to the operator window.</p></body></html>",
                                summary
                            )
                        }
                        Err(error) => format!(
                            "HTTP/1.1 500 Internal Server Error\r\nContent-Type: text/html; charset=utf-8\r\nConnection: close\r\n\r\n<html><body><h1>AGRO OAuth failed</h1><p>{}</p><p>You can still use paste fallback in the operator window.</p></body></html>",
                            error
                        ),
                    };
                    let _ = stream.write_all(message.as_bytes());
                    let _ = stream.flush();
                    break;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(_) => break,
            }
        }
    });
    Ok(Some(format!(
        "listening for browser callback on {}",
        bind_target_for_notice
    )))
}

fn sleep_while_background_running(
    controller: &HarnessController,
    runner_id: &str,
    duration: Duration,
) -> anyhow::Result<bool> {
    if duration.is_zero() {
        return Ok(!controller.background_stop_requested_for(Some(runner_id))?);
    }
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        if controller.background_stop_requested_for(Some(runner_id))? {
            return Ok(false);
        }
        controller.touch_background_runner("sleeping")?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        std::thread::sleep(remaining.min(BACKGROUND_HEARTBEAT_INTERVAL));
    }
    Ok(!controller.background_stop_requested_for(Some(runner_id))?)
}

async fn run_background_loop(
    controller: Arc<HarnessController>,
    settings: RunSettings,
    pause_duration: Duration,
    runner_id: String,
    owner_shell_id: Option<String>,
    owner_shell_pid: Option<u32>,
) -> anyhow::Result<()> {
    controller.clear_background_stop_request()?;
    let existing_runner_state = controller.read_background_runner_state()?.state;
    let runner_state = bootstrap_background_runner_state(
        existing_runner_state.as_ref(),
        runner_id.as_str(),
        std::process::id(),
        owner_shell_id.as_deref(),
        owner_shell_pid,
        &settings,
        pause_duration.as_secs_f32(),
    );
    controller.write_background_runner_state(&runner_state)?;

    let result = async {
        let mut completed_turn_count = 0_u64;
        loop {
            if controller.background_stop_requested_for(Some(runner_id.as_str()))? {
                let summary = "background stop requested; exiting before next turn".to_string();
                controller.update_background_runner_state(
                    "stop_requested",
                    completed_turn_count,
                    Some(summary.clone()),
                    None,
                )?;
                break Ok(BackgroundLoopExitState {
                    run_state: "idle".into(),
                    run_mode: OperatorRunMode::Idle,
                    summary,
                    last_error: None,
                    completed_turn_count,
                });
            }

            controller.update_background_runner_state(
                "running_turn",
                completed_turn_count,
                Some(format!(
                    "starting background turn {}",
                    completed_turn_count + 1
                )),
                None,
            )?;
            match controller.run_turn(&settings).await {
                Ok(turn) => {
                    completed_turn_count += 1;
                    match turn.terminal {
                        OperatorTurnTerminal::Completed => {
                            let summary = format!(
                                "background turn {} completed | next turn in {:.1}s | {}",
                                completed_turn_count,
                                pause_duration.as_secs_f32(),
                                turn.summary
                            );
                            controller.update_background_runner_state(
                                "sleeping",
                                completed_turn_count,
                                Some(summary.clone()),
                                None,
                            )?;
                            controller
                                .read_snapshot(
                                    "background_looping",
                                    OperatorRunMode::Continuous,
                                    &summary,
                                    None,
                                    completed_turn_count,
                                    None,
                                )
                                .await?;
                            if !sleep_while_background_running(
                                &controller,
                                runner_id.as_str(),
                                pause_duration,
                            )? {
                                let summary = "background loop stop requested".to_string();
                                controller.update_background_runner_state(
                                    "stop_requested",
                                    completed_turn_count,
                                    Some(summary.clone()),
                                    None,
                                )?;
                                break Ok(BackgroundLoopExitState {
                                    run_state: "idle".into(),
                                    run_mode: OperatorRunMode::Idle,
                                    summary,
                                    last_error: None,
                                    completed_turn_count,
                                });
                            }
                        }
                        OperatorTurnTerminal::SurfacedGap => {
                            let summary = format!(
                                "background loop stopped on surfaced gap after {} turn(s) | {}",
                                completed_turn_count, turn.summary
                            );
                            controller.update_background_runner_state(
                                "surfaced_gap",
                                completed_turn_count,
                                Some(summary.clone()),
                                None,
                            )?;
                            break Ok(BackgroundLoopExitState {
                                run_state: "idle".into(),
                                run_mode: OperatorRunMode::Idle,
                                summary,
                                last_error: None,
                                completed_turn_count,
                            });
                        }
                        OperatorTurnTerminal::GithubActionRequested => {
                            let summary = format!(
                                "background loop stopped on supervised GitHub request after {} turn(s) | {}",
                                completed_turn_count, turn.summary
                            );
                            controller.update_background_runner_state(
                                "github_request_pending",
                                completed_turn_count,
                                Some(summary.clone()),
                                None,
                            )?;
                            break Ok(BackgroundLoopExitState {
                                run_state: "idle".into(),
                                run_mode: OperatorRunMode::Idle,
                                summary,
                                last_error: None,
                                completed_turn_count,
                            });
                        }
                    }
                }
                Err(error) => {
                    let last_error = format!("{error:#}");
                    let summary = format!(
                        "background loop failed after {} turn(s)",
                        completed_turn_count
                    );
                    controller.update_background_runner_state(
                        "failed",
                        completed_turn_count,
                        Some(summary.clone()),
                        Some(last_error.clone()),
                    )?;
                    break Ok(BackgroundLoopExitState {
                        run_state: "error".into(),
                        run_mode: OperatorRunMode::Idle,
                        summary,
                        last_error: Some(last_error),
                        completed_turn_count,
                    });
                }
            }
        }
    }
    .await;

    controller.clear_background_stop_request()?;

    match result {
        Ok(exit_state) => {
            controller
                .read_snapshot(
                    &exit_state.run_state,
                    exit_state.run_mode,
                    &exit_state.summary,
                    exit_state.last_error,
                    exit_state.completed_turn_count,
                    None,
                )
                .await?;
            Ok(())
        }
        Err(error) => Err(error),
    }
}

async fn ensure_plan(
    orchestrator: &PersistentOrchestrator<SqliteStateStore, FilesystemOffloadSink>,
    objective: &str,
) -> anyhow::Result<()> {
    let objective = normalize_text(objective, DEFAULT_OBJECTIVE);
    let existing = orchestrator.hydrate(1).await?;
    if let Some(state) = existing {
        if state.plan.objective == objective {
            return Ok(());
        }
        let snapshot = build_operator_plan(&state, objective);
        orchestrator.adopt_plan(&snapshot).await?;
        return Ok(());
    }

    let snapshot = build_initial_operator_plan(objective);
    orchestrator.adopt_plan(&snapshot).await
}

fn build_initial_operator_plan(objective: String) -> PlanSnapshot {
    let now = Utc::now();
    PlanSnapshot {
        snapshot_id: Uuid::new_v4(),
        plan: SufficientPlan {
            id: Uuid::new_v4(),
            version: 1,
            objective,
            constraints: default_constraints(),
            invariants: default_invariants(),
            modules: default_modules(),
            active_module: "operate".into(),
            recodification_rule: default_recodification_rule(),
            updated_at: now,
        },
        rationale: "Bootstrap operator plan from GUI objective".into(),
        source_gap_id: None,
        recorded_at: now,
    }
}

fn build_operator_plan(existing: &OrchestratorState, objective: String) -> PlanSnapshot {
    let now = Utc::now();
    PlanSnapshot {
        snapshot_id: Uuid::new_v4(),
        plan: SufficientPlan {
            id: existing.plan.id,
            version: existing.plan.version + 1,
            objective,
            constraints: default_constraints(),
            invariants: default_invariants(),
            modules: default_modules(),
            active_module: "operate".into(),
            recodification_rule: default_recodification_rule(),
            updated_at: now,
        },
        rationale: "Update operator objective from GUI".into(),
        source_gap_id: None,
        recorded_at: now,
    }
}

fn default_constraints() -> Vec<String> {
    vec![
        "Use one serialized host action at a time.".into(),
        "Prefer capability gaps over unverifiable success.".into(),
        "Keep continuity artifacts current after each bounded turn.".into(),
    ]
}

fn default_invariants() -> Vec<Invariant> {
    vec![
        Invariant {
            key: "body_truth".into(),
            description: "Only count actions as success when host verification proves the effect."
                .into(),
        },
        Invariant {
            key: "continuity".into(),
            description:
                "Crash and relaunch must preserve enough turn state to resume or fail closed."
                    .into(),
        },
    ]
}

fn default_modules() -> Vec<PlanModule> {
    vec![PlanModule {
        key: "operate".into(),
        description: "Observe the desktop, choose one action, verify it, and record the result."
            .into(),
        success_checks: vec![
            "The chosen action is bounded and verified.".into(),
            "Receipts, gaps, and current-surface artifacts remain coherent.".into(),
        ],
        reveal_response:
            "If the turn cannot proceed safely, surface a capability gap instead of guessing."
                .into(),
    }]
}

fn default_recodification_rule() -> String {
    "When a revealed insufficiency blocks safe progress, encode it durably into the next plan."
        .into()
}

#[cfg(any())]
fn main() -> anyhow::Result<()> {
    let command = parse_operator_command()?;
    let app_controller = Arc::new(HarnessController::new(OperatorPaths::discover()?)?);
    if command.smoke_test {
        let snapshot = run_async({
            let controller = app_controller.clone();
            move || async move {
                controller
                    .read_snapshot("idle", OperatorRunMode::Idle, "smoke-test", None, 0, None)
                    .await
            }
        })?;
        println!("{}", serde_json::to_string_pretty(&snapshot)?);
        return Ok(());
    }
    if command.run_turn {
        let settings = command.settings.clone();
        let snapshot = run_async({
            let controller = app_controller.clone();
            move || async move {
                match controller.run_turn(&settings).await {
                    Ok(outcome) => {
                        controller
                            .read_snapshot(
                                "idle",
                                OperatorRunMode::SingleTurn,
                                &outcome.summary,
                                None,
                                0,
                                None,
                            )
                            .await
                    }
                    Err(error) => {
                        let message = error.to_string();
                        let snapshot = controller
                            .read_snapshot(
                                "error",
                                OperatorRunMode::SingleTurn,
                                "run-turn failed",
                                Some(message.clone()),
                                0,
                                None,
                            )
                            .await?;
                        println!("{}", serde_json::to_string_pretty(&snapshot)?);
                        Err(anyhow!(message))
                    }
                }
            }
        })?;
        println!("{}", serde_json::to_string_pretty(&snapshot)?);
        return Ok(());
    }
    if command.run_turn {
        let settings = command.settings.clone();
        let snapshot = run_async({
            let controller = app_controller.clone();
            move || async move {
                match controller.run_turn(&settings).await {
                    Ok(outcome) => {
                        controller
                            .read_snapshot(
                                "idle",
                                OperatorRunMode::SingleTurn,
                                &outcome.summary,
                                None,
                                0,
                                None,
                            )
                            .await
                    }
                    Err(error) => {
                        let message = error.to_string();
                        let snapshot = controller
                            .read_snapshot(
                                "error",
                                OperatorRunMode::SingleTurn,
                                "run-turn failed",
                                Some(message.clone()),
                                0,
                                None,
                            )
                            .await?;
                        println!("{}", serde_json::to_string_pretty(&snapshot)?);
                        Err(anyhow!(message))
                    }
                }
            }
        })?;
        println!("{}", serde_json::to_string_pretty(&snapshot)?);
        return Ok(());
    }
    if command.background_loop {
        run_async({
            let controller = app_controller.clone();
            let settings = command.settings.clone();
            let runner_id = command
                .background_runner_id
                .clone()
                .unwrap_or_else(|| Uuid::new_v4().to_string());
            move || async move {
                run_background_loop(
                    controller,
                    settings,
                    Duration::from_secs_f32(command.loop_pause_seconds.max(0.0)),
                    runner_id,
                    command.background_owner_shell_id.clone(),
                    command.background_owner_shell_pid,
                )
                .await
            }
        })?;
        return Ok(());
    }
    eframe::run_native(
        "AGRO Harness Operator",
        eframe::NativeOptions::default(),
        Box::new(move |_cc| {
            let mut app = OperatorApp::new(app_controller.clone());
            app.spawn_refresh(
                "idle".into(),
                OperatorRunMode::Idle,
                "operator launched".into(),
                None,
                0,
                None,
            );
            Ok(Box::new(app))
        }),
    )
    .map_err(|error| anyhow!(error.to_string()))
}

fn main() -> anyhow::Result<()> {
    let command = parse_operator_command()?;
    let paths = OperatorPaths::discover()?;
    let app_controller = Arc::new(HarnessController::new(paths)?);
    if command.smoke_test {
        let snapshot = run_async({
            let controller = app_controller.clone();
            move || async move {
                controller
                    .read_snapshot("idle", OperatorRunMode::Idle, "smoke-test", None, 0, None)
                    .await
            }
        })?;
        println!("{}", serde_json::to_string_pretty(&snapshot)?);
        return Ok(());
    }
    if command.run_turn {
        let settings = command.settings.clone();
        let snapshot = run_async({
            let controller = app_controller.clone();
            move || async move {
                match controller.run_turn(&settings).await {
                    Ok(outcome) => {
                        controller
                            .read_snapshot(
                                "idle",
                                OperatorRunMode::SingleTurn,
                                &outcome.summary,
                                None,
                                0,
                                None,
                            )
                            .await
                    }
                    Err(error) => {
                        let message = error.to_string();
                        let snapshot = controller
                            .read_snapshot(
                                "error",
                                OperatorRunMode::SingleTurn,
                                "run-turn failed",
                                Some(message.clone()),
                                0,
                                None,
                            )
                            .await?;
                        println!("{}", serde_json::to_string_pretty(&snapshot)?);
                        Err(anyhow!(message))
                    }
                }
            }
        })?;
        println!("{}", serde_json::to_string_pretty(&snapshot)?);
        return Ok(());
    }
    if command.background_loop {
        run_async({
            let controller = app_controller.clone();
            let settings = command.settings.clone();
            let runner_id = command
                .background_runner_id
                .clone()
                .unwrap_or_else(|| Uuid::new_v4().to_string());
            move || async move {
                run_background_loop(
                    controller,
                    settings,
                    Duration::from_secs_f32(command.loop_pause_seconds.max(0.0)),
                    runner_id,
                    command.background_owner_shell_id.clone(),
                    command.background_owner_shell_pid,
                )
                .await
            }
        })?;
        return Ok(());
    }

    Application::new().run(move |cx: &mut App| {
        gpui_component::init(cx);
        init_operator_shell(cx);
        let bounds = Bounds::centered(None, size(px(1540.0), px(980.0)), cx);
        cx.open_window(
            WindowOptions {
                window_bounds: Some(WindowBounds::Windowed(bounds)),
                ..Default::default()
            },
            |window, cx| {
                let view = cx.new(|cx| OperatorShell::new(app_controller.clone(), window, cx));
                cx.new(|cx| Root::new(view, window, cx))
            },
        )
        .expect("open AGRO Harness Operator window");
        cx.activate(true);
    });

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        BackgroundHandoffDisposition, BackgroundRunnerDisposition, DEFAULT_LOOP_PAUSE_SECONDS,
        DEFAULT_MODEL, DEFAULT_OBJECTIVE, DEFAULT_SESSION_ID, DEFAULT_THREAD_ID,
        DEFAULT_THREAD_LABEL, HarnessController, InteractiveOAuthLaunchStatus, OperatorApp,
        OperatorAuthProvider, OperatorBackgroundHandoffRequest, OperatorBackgroundRunnerState,
        OperatorCommand, OperatorEngineMode, OperatorGithubActionLifecycleState,
        OperatorGithubActionRequestRecord, OperatorGithubTargetSuggestion, OperatorPaths,
        OperatorRunMode, OperatorSnapshot, OperatorTurnTerminal, RunSettings, CodexCliStatus,
        apply_github_target_override, apply_operator_env_assignments_with,
        background_reattach_recommendation, background_recovery_action_label,
        background_runner_allows_spawn, background_runner_owner_shell_alive,
        background_runner_process_is_alive, bootstrap_background_runner_state,
        browser_callback_bind_target, build_browser_callback_url, build_github_action_command,
        build_codex_cli_context_prompt, build_github_cli_context_from,
        build_github_target_guidance, build_github_target_suggestions_from,
        build_pending_oauth_view, build_project_artifact_context,
        build_repo_git_context_from, build_runtime_grounding_bundle,
        classify_background_handoff,
        classify_background_runner, combine_optional_notices, describe_auth_state,
        describe_operator_auth_state,
        discover_codex_command_with, discover_openclaw_command_with, discover_repo_root,
        effective_operator_status, ensure_operator_env_template, extract_http_request_target,
        format_background_runner_age, format_runtime_turn_reply,
        import_openclaw_plan_into_auth_store, interactive_oauth_launch_status_with,
        load_openclaw_import_plan_from_paths, maybe_bootstrap_openclaw_codex_auth_into_store,
        objective_needs_project_artifact_context, openclaw_cli_status_with,
        operator_env_config_status, parse_codex_cli_exec_output, parse_codex_login_status,
        parse_operator_env_assignments, pending_oauth_launch_url,
        provisional_background_runner_state, read_recent_jsonl_entries,
        reconcile_crashed_background_runner, recoverable_background_settings,
        request_target_matches_callback_path, resolve_interactive_oauth_client_id_with, run_async,
        should_continue_run_loop, sleep_while_background_running, summarize_github_action_result,
        windows_codex_npm_shim_command, write_json_atomic,
    };
    use anyhow::Context;
    use chrono::{Duration, Utc};
    use serde::{Deserialize, Serialize};
    use splcw_llm::{
        AuthMode, AuthProfile, AuthProfileStore, ChatRequest, ChatResponse, ContentBlock,
        OAuthAuthorizationKind, OAuthState, PendingOAuthAuthorization, ProviderKind,
    };
    use splcw_orchestrator::{
        RuntimeTurnRecord, SupervisedGithubActionKind, SupervisedGithubActionRequest,
    };
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use tempfile::tempdir;
    use uuid::Uuid;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestEntry {
        index: usize,
        label: String,
    }

    fn test_operator_paths(root: &Path) -> OperatorPaths {
        let operator_root = root.join("operator");
        OperatorPaths {
            repo_root: root.to_path_buf(),
            harness_root: root.join("ultimentality-pilot").join("harness"),
            operator_root: operator_root.clone(),
            operator_env_path: operator_root.join("operator.env"),
            status_path: operator_root.join("status.json"),
            github_action_request_path: operator_root.join("github-action-request.json"),
            github_action_history_path: operator_root.join("github-action-history.jsonl"),
            background_runner_path: operator_root.join("background-runner.json"),
            background_stop_path: operator_root.join("background-stop.request"),
            background_handoff_path: operator_root.join("background-handoff.json"),
            codex_cli_session_path: operator_root.join("codex-cli-session.json"),
            codex_cli_live_stream_path: operator_root.join("codex-cli-live-stream.json"),
            session_root: operator_root.join("sessions"),
            session_id: DEFAULT_SESSION_ID.into(),
            state_db_path: operator_root.join("state.sqlite"),
            auth_store_path: operator_root.join("auth-profiles.json"),
        }
    }

    #[test]
    fn discover_repo_root_prefers_executable_path_over_launch_shell() -> anyhow::Result<()> {
        let root = tempdir()?;
        let repo_root = root.path().join("AIM");
        let exe_root = repo_root
            .join("artifacts")
            .join("ultimentality-pilot")
            .join("operator");
        let shell_root = root.path().join("Codex");
        std::fs::create_dir_all(repo_root.join("ultimentality-pilot").join("harness"))?;
        std::fs::write(
            repo_root
                .join("ultimentality-pilot")
                .join("harness")
                .join("Cargo.toml"),
            "[workspace]",
        )?;
        std::fs::create_dir_all(&exe_root)?;
        std::fs::create_dir_all(&shell_root)?;

        let discovered = discover_repo_root(
            Some(shell_root.as_path()),
            Some(exe_root.join("AGRO Harness Operator.exe").as_path()),
        );

        assert_eq!(discovered.as_deref(), Some(repo_root.as_path()));
        Ok(())
    }

    #[test]
    fn read_recent_jsonl_entries_returns_tail_in_order() -> anyhow::Result<()> {
        let root = tempdir()?;
        let path = root.path().join("events.jsonl");
        std::fs::write(
            &path,
            [
                serde_json::to_string(&TestEntry {
                    index: 1,
                    label: "one".into(),
                })?,
                String::new(),
                serde_json::to_string(&TestEntry {
                    index: 2,
                    label: "two".into(),
                })?,
                serde_json::to_string(&TestEntry {
                    index: 3,
                    label: "three".into(),
                })?,
            ]
            .join("\n"),
        )?;

        let entries = read_recent_jsonl_entries::<TestEntry>(path, 2)?;
        assert_eq!(
            entries,
            vec![
                TestEntry {
                    index: 2,
                    label: "two".into()
                },
                TestEntry {
                    index: 3,
                    label: "three".into()
                }
            ]
        );
        Ok(())
    }

    #[test]
    fn write_json_atomic_replaces_existing_status() -> anyhow::Result<()> {
        let root = tempdir()?;
        let path = root.path().join("status.json");
        write_json_atomic(
            &path,
            &TestEntry {
                index: 1,
                label: "before".into(),
            },
        )?;
        write_json_atomic(
            &path,
            &TestEntry {
                index: 2,
                label: "after".into(),
            },
        )?;

        let body =
            std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        let entry: TestEntry = serde_json::from_str(&body)?;
        assert_eq!(
            entry,
            TestEntry {
                index: 2,
                label: "after".into()
            }
        );
        Ok(())
    }

    #[test]
    fn format_runtime_turn_reply_preserves_text_and_tool_blocks() {
        let reply = format_runtime_turn_reply(&RuntimeTurnRecord {
            recorded_at: Utc::now(),
            turn_id: Uuid::new_v4(),
            thread_id: "main".into(),
            provider_id: "openai-codex".into(),
            model: "gpt-5.4".into(),
            request: ChatRequest {
                model: "gpt-5.4".into(),
                system_prompt: None,
                messages: Vec::new(),
                tools: Vec::new(),
            },
            response: ChatResponse {
                provider_id: "openai-codex".into(),
                model: "gpt-5.4".into(),
                content: vec![
                    ContentBlock::Text {
                        text: "The model answered in prose.".into(),
                    },
                    ContentBlock::ToolCall {
                        id: "call_123".into(),
                        name: "capability_gap".into(),
                        arguments: serde_json::json!({
                            "title": "Need clearer target",
                            "notes": ["one", "two"]
                        }),
                    },
                ],
            },
            narrative: "The runtime recorded a narrative too.".into(),
            tool_outcome: None,
            surfaced_gap: None,
        });

        assert!(reply.contains("The model answered in prose."));
        assert!(reply.contains("capability_gap"));
        assert!(reply.contains("\"title\": \"Need clearer target\""));
        assert!(reply.contains("The runtime recorded a narrative too."));
    }

    #[test]
    fn continuous_run_loop_only_continues_when_requested_and_safe() {
        assert!(should_continue_run_loop(
            OperatorRunMode::Continuous,
            true,
            OperatorTurnTerminal::Completed
        ));
        assert!(!should_continue_run_loop(
            OperatorRunMode::Continuous,
            false,
            OperatorTurnTerminal::Completed
        ));
        assert!(!should_continue_run_loop(
            OperatorRunMode::Continuous,
            true,
            OperatorTurnTerminal::SurfacedGap
        ));
        assert!(!should_continue_run_loop(
            OperatorRunMode::Continuous,
            true,
            OperatorTurnTerminal::GithubActionRequested
        ));
        assert!(!should_continue_run_loop(
            OperatorRunMode::SingleTurn,
            true,
            OperatorTurnTerminal::Completed
        ));
    }

    #[test]
    fn build_github_action_command_maps_comment_issue_requests() {
        let command = build_github_action_command(&SupervisedGithubActionRequest {
            kind: SupervisedGithubActionKind::CommentIssue,
            repository: Some("jessybrenenstahl/AIM".into()),
            issue_number: Some(77),
            pull_request_number: None,
            body: Some("Operator-approved GitHub comment.".into()),
            label: None,
            assignee: None,
            justification: Some("Keep the harness supervised.".into()),
        })
        .expect("complete issue comment request should map");

        assert_eq!(
            command.args,
            vec![
                "issue",
                "comment",
                "77",
                "--repo",
                "jessybrenenstahl/AIM",
                "--body",
                "Operator-approved GitHub comment."
            ]
        );
    }

    #[test]
    fn build_github_action_command_maps_assign_issue_requests() {
        let command = build_github_action_command(&SupervisedGithubActionRequest {
            kind: SupervisedGithubActionKind::AssignIssue,
            repository: Some("jessybrenenstahl/AIM".into()),
            issue_number: Some(77),
            pull_request_number: None,
            body: None,
            label: None,
            assignee: Some("@me".into()),
            justification: None,
        })
        .expect("complete issue assign request should map");

        assert_eq!(
            command.args,
            vec![
                "issue",
                "edit",
                "77",
                "--repo",
                "jessybrenenstahl/AIM",
                "--add-assignee",
                "@me"
            ]
        );
    }

    #[test]
    fn build_github_action_command_maps_label_pull_request_requests() {
        let command = build_github_action_command(&SupervisedGithubActionRequest {
            kind: SupervisedGithubActionKind::LabelPullRequest,
            repository: None,
            issue_number: None,
            pull_request_number: Some(101),
            body: None,
            label: Some("needs-review".into()),
            assignee: None,
            justification: None,
        })
        .expect("complete label request should map");

        assert_eq!(
            command.args,
            vec!["pr", "edit", "101", "--add-label", "needs-review"]
        );
    }

    #[test]
    fn build_github_action_command_maps_label_issue_requests() {
        let command = build_github_action_command(&SupervisedGithubActionRequest {
            kind: SupervisedGithubActionKind::LabelIssue,
            repository: Some("jessybrenenstahl/AIM".into()),
            issue_number: Some(77),
            pull_request_number: None,
            body: None,
            label: Some("needs-repro".into()),
            assignee: None,
            justification: None,
        })
        .expect("complete issue label request should map");

        assert_eq!(
            command.args,
            vec![
                "issue",
                "edit",
                "77",
                "--repo",
                "jessybrenenstahl/AIM",
                "--add-label",
                "needs-repro"
            ]
        );
    }

    #[test]
    fn build_github_action_command_maps_remove_label_issue_requests() {
        let command = build_github_action_command(&SupervisedGithubActionRequest {
            kind: SupervisedGithubActionKind::RemoveLabelIssue,
            repository: Some("jessybrenenstahl/AIM".into()),
            issue_number: Some(77),
            pull_request_number: None,
            body: None,
            label: Some("needs-repro".into()),
            assignee: None,
            justification: None,
        })
        .expect("complete issue remove label request should map");

        assert_eq!(
            command.args,
            vec![
                "issue",
                "edit",
                "77",
                "--repo",
                "jessybrenenstahl/AIM",
                "--remove-label",
                "needs-repro"
            ]
        );
    }

    #[test]
    fn build_github_action_command_maps_close_issue_requests() {
        let command = build_github_action_command(&SupervisedGithubActionRequest {
            kind: SupervisedGithubActionKind::CloseIssue,
            repository: Some("jessybrenenstahl/AIM".into()),
            issue_number: Some(88),
            pull_request_number: None,
            body: Some("Closing this now that the supervised lane landed.".into()),
            label: None,
            assignee: None,
            justification: Some("Keep the tracked work aligned with shipped behavior.".into()),
        })
        .expect("complete close issue request should map");

        assert_eq!(
            command.args,
            vec![
                "issue",
                "close",
                "88",
                "--repo",
                "jessybrenenstahl/AIM",
                "--comment",
                "Closing this now that the supervised lane landed."
            ]
        );
    }

    #[test]
    fn build_github_action_command_maps_close_pull_request_requests() {
        let command = build_github_action_command(&SupervisedGithubActionRequest {
            kind: SupervisedGithubActionKind::ClosePullRequest,
            repository: Some("jessybrenenstahl/AIM".into()),
            issue_number: None,
            pull_request_number: Some(101),
            body: Some("Closing this PR now that the supervised lane landed.".into()),
            label: None,
            assignee: None,
            justification: Some("Keep the tracked work aligned with shipped behavior.".into()),
        })
        .expect("complete close pull request request should map");

        assert_eq!(
            command.args,
            vec![
                "pr",
                "close",
                "101",
                "--repo",
                "jessybrenenstahl/AIM",
                "--comment",
                "Closing this PR now that the supervised lane landed."
            ]
        );
    }

    #[test]
    fn build_github_action_command_maps_reopen_issue_requests() {
        let command = build_github_action_command(&SupervisedGithubActionRequest {
            kind: SupervisedGithubActionKind::ReopenIssue,
            repository: Some("jessybrenenstahl/AIM".into()),
            issue_number: Some(88),
            pull_request_number: None,
            body: Some("Reopening this issue so follow-up work stays visible.".into()),
            label: None,
            assignee: None,
            justification: Some("Keep the tracked work aligned with shipped behavior.".into()),
        })
        .expect("complete reopen issue request should map");

        assert_eq!(
            command.args,
            vec![
                "issue",
                "reopen",
                "88",
                "--repo",
                "jessybrenenstahl/AIM",
                "--comment",
                "Reopening this issue so follow-up work stays visible."
            ]
        );
    }

    #[test]
    fn build_github_action_command_maps_reopen_pull_request_requests() {
        let command = build_github_action_command(&SupervisedGithubActionRequest {
            kind: SupervisedGithubActionKind::ReopenPullRequest,
            repository: Some("jessybrenenstahl/AIM".into()),
            issue_number: None,
            pull_request_number: Some(101),
            body: Some("Reopening this PR so review can continue.".into()),
            label: None,
            assignee: None,
            justification: Some("Keep the tracked work aligned with shipped behavior.".into()),
        })
        .expect("complete reopen pull request request should map");

        assert_eq!(
            command.args,
            vec![
                "pr",
                "reopen",
                "101",
                "--repo",
                "jessybrenenstahl/AIM",
                "--comment",
                "Reopening this PR so review can continue."
            ]
        );
    }

    #[test]
    fn build_github_action_command_maps_assign_pull_request_requests() {
        let command = build_github_action_command(&SupervisedGithubActionRequest {
            kind: SupervisedGithubActionKind::AssignPullRequest,
            repository: Some("jessybrenenstahl/AIM".into()),
            issue_number: None,
            pull_request_number: Some(101),
            body: None,
            label: None,
            assignee: Some("@copilot".into()),
            justification: None,
        })
        .expect("complete pull request assign request should map");

        assert_eq!(
            command.args,
            vec![
                "pr",
                "edit",
                "101",
                "--repo",
                "jessybrenenstahl/AIM",
                "--add-assignee",
                "@copilot"
            ]
        );
    }

    #[test]
    fn build_github_action_command_maps_remove_label_pull_request_requests() {
        let command = build_github_action_command(&SupervisedGithubActionRequest {
            kind: SupervisedGithubActionKind::RemoveLabelPullRequest,
            repository: Some("jessybrenenstahl/AIM".into()),
            issue_number: None,
            pull_request_number: Some(101),
            body: None,
            label: Some("needs-review".into()),
            assignee: None,
            justification: None,
        })
        .expect("complete pull request remove label request should map");

        assert_eq!(
            command.args,
            vec![
                "pr",
                "edit",
                "101",
                "--repo",
                "jessybrenenstahl/AIM",
                "--remove-label",
                "needs-review"
            ]
        );
    }

    #[test]
    fn apply_github_target_override_fills_missing_pull_request_target() {
        let request = SupervisedGithubActionRequest {
            kind: SupervisedGithubActionKind::CommentPullRequest,
            repository: Some("jessybrenenstahl/AIM".into()),
            issue_number: None,
            pull_request_number: None,
            body: Some("Operator-approved GitHub comment.".into()),
            label: None,
            assignee: None,
            justification: Some("Keep the harness supervised.".into()),
        };

        let effective =
            apply_github_target_override(&request, Some(42)).expect("target override should apply");

        assert_eq!(effective.pull_request_number, Some(42));
        assert!(!effective.requires_operator_target());
    }

    #[test]
    fn build_github_action_command_rejects_missing_target() {
        let error = build_github_action_command(&SupervisedGithubActionRequest {
            kind: SupervisedGithubActionKind::CommentPullRequest,
            repository: Some("jessybrenenstahl/AIM".into()),
            issue_number: None,
            pull_request_number: None,
            body: Some("Operator-approved GitHub comment.".into()),
            label: None,
            assignee: None,
            justification: Some("Keep the harness supervised.".into()),
        })
        .expect_err("missing target should fail closed");

        assert!(error.to_string().contains("missing pull_request_number"));
    }

    fn pending_github_request_record(
        request: SupervisedGithubActionRequest,
    ) -> OperatorGithubActionRequestRecord {
        OperatorGithubActionRequestRecord {
            requested_at: Utc::now(),
            thread_id: "thread-1".into(),
            thread_label: "Thread 1".into(),
            summary: request.summary(),
            narrative: Some("narrative".into()),
            request,
            target_suggestions: Vec::new(),
        }
    }

    #[test]
    fn build_github_target_suggestions_prefers_current_branch_prs_and_dedupes() {
        let suggestions = build_github_target_suggestions_from(
            &SupervisedGithubActionRequest {
                kind: SupervisedGithubActionKind::CommentPullRequest,
                repository: Some("jessybrenenstahl/AIM".into()),
                issue_number: None,
                pull_request_number: None,
                body: Some("body".into()),
                label: None,
                assignee: None,
                justification: None,
            },
            SupervisedGithubActionKind::CommentPullRequest,
            Some("main"),
            Some(
                r#"[{"number":17,"title":"Current branch PR","url":"https://example.com/pr/17","headRefName":"main"}]"#,
            ),
            Some(
                r#"[{"number":17,"title":"Current branch PR","url":"https://example.com/pr/17","headRefName":"main"},{"number":11,"title":"Other PR","url":"https://example.com/pr/11","headRefName":"codex/other"}]"#,
            ),
            None,
        );

        assert_eq!(suggestions.len(), 2);
        assert_eq!(suggestions[0].number, 17);
        assert!(
            suggestions[0]
                .source
                .contains("main")
        );
        assert_eq!(suggestions[1].number, 11);
        assert!(suggestions[1].source.contains("recent open PR"));
    }

    #[test]
    fn build_github_target_suggestions_maps_open_issues_for_issue_requests() {
        let suggestions = build_github_target_suggestions_from(
            &SupervisedGithubActionRequest {
                kind: SupervisedGithubActionKind::CommentIssue,
                repository: Some("jessybrenenstahl/AIM".into()),
                issue_number: None,
                pull_request_number: None,
                body: Some("body".into()),
                label: None,
                assignee: None,
                justification: None,
            },
            SupervisedGithubActionKind::CommentIssue,
            Some("main"),
            None,
            None,
            Some(
                r#"[{"number":91,"title":"Track lifecycle state","url":"https://example.com/issues/91"}]"#,
            ),
        );

        assert_eq!(suggestions.len(), 1);
        assert_eq!(suggestions[0].number, 91);
        assert_eq!(suggestions[0].title, "Track lifecycle state");
        assert_eq!(suggestions[0].source, "recent open issue");
    }

    #[test]
    fn build_github_target_suggestions_maps_open_issues_for_issue_label_requests() {
        let suggestions = build_github_target_suggestions_from(
            &SupervisedGithubActionRequest {
                kind: SupervisedGithubActionKind::LabelIssue,
                repository: Some("jessybrenenstahl/AIM".into()),
                issue_number: None,
                pull_request_number: None,
                body: None,
                label: Some("needs-repro".into()),
                assignee: None,
                justification: None,
            },
            SupervisedGithubActionKind::LabelIssue,
            Some("main"),
            None,
            None,
            Some(r#"[{"number":17,"title":"Label me","url":"https://example.com/issues/17"}]"#),
        );

        assert_eq!(suggestions.len(), 1);
        assert_eq!(suggestions[0].number, 17);
        assert_eq!(suggestions[0].source, "recent open issue");
    }

    #[test]
    fn build_github_target_suggestions_maps_open_issues_for_issue_remove_label_requests() {
        let suggestions = build_github_target_suggestions_from(
            &SupervisedGithubActionRequest {
                kind: SupervisedGithubActionKind::RemoveLabelIssue,
                repository: Some("jessybrenenstahl/AIM".into()),
                issue_number: None,
                pull_request_number: None,
                body: None,
                label: Some("needs-repro".into()),
                assignee: None,
                justification: None,
            },
            SupervisedGithubActionKind::RemoveLabelIssue,
            Some("main"),
            None,
            None,
            Some(
                r#"[{"number":27,"title":"Remove stale issue label","url":"https://example.com/issues/27"}]"#,
            ),
        );

        assert_eq!(suggestions.len(), 1);
        assert_eq!(suggestions[0].number, 27);
        assert_eq!(suggestions[0].source, "recent open issue");
    }

    #[test]
    fn build_github_target_suggestions_maps_open_issues_for_assign_issue_requests() {
        let suggestions = build_github_target_suggestions_from(
            &SupervisedGithubActionRequest {
                kind: SupervisedGithubActionKind::AssignIssue,
                repository: Some("jessybrenenstahl/AIM".into()),
                issue_number: None,
                pull_request_number: None,
                body: None,
                label: None,
                assignee: Some("@me".into()),
                justification: Some("Assign the crash recovery issue.".into()),
            },
            SupervisedGithubActionKind::AssignIssue,
            Some("main"),
            None,
            None,
            Some(
                r#"[{"number":29,"title":"Assign crash recovery follow-up","url":"https://example.com/issues/29"}]"#,
            ),
        );

        assert_eq!(suggestions.len(), 1);
        assert_eq!(suggestions[0].number, 29);
        assert!(suggestions[0].source.contains("keyword-matched"));
    }

    #[test]
    fn build_github_target_suggestions_maps_open_issues_for_close_issue_requests() {
        let suggestions = build_github_target_suggestions_from(
            &SupervisedGithubActionRequest {
                kind: SupervisedGithubActionKind::CloseIssue,
                repository: Some("jessybrenenstahl/AIM".into()),
                issue_number: None,
                pull_request_number: None,
                body: Some("Close the shipped GitHub supervision gap.".into()),
                label: None,
                assignee: None,
                justification: Some("The shipped lane should close the tracking issue.".into()),
            },
            SupervisedGithubActionKind::CloseIssue,
            Some("main"),
            None,
            None,
            Some(
                r#"[{"number":24,"title":"Close the GitHub supervision tracking issue","url":"https://example.com/issues/24"}]"#,
            ),
        );

        assert_eq!(suggestions.len(), 1);
        assert_eq!(suggestions[0].number, 24);
        assert!(suggestions[0].source.contains("keyword-matched"));
    }

    #[test]
    fn build_github_target_suggestions_maps_open_prs_for_close_pull_request_requests() {
        let suggestions = build_github_target_suggestions_from(
            &SupervisedGithubActionRequest {
                kind: SupervisedGithubActionKind::ClosePullRequest,
                repository: Some("jessybrenenstahl/AIM".into()),
                issue_number: None,
                pull_request_number: None,
                body: Some("Close the crash recovery PR now that the replacement landed.".into()),
                label: None,
                assignee: None,
                justification: Some("Crash recovery PR cleanup should be supervised.".into()),
            },
            SupervisedGithubActionKind::ClosePullRequest,
            Some("main"),
            None,
            Some(
                r#"[{"number":11,"title":"Misc cleanup","url":"https://example.com/pr/11","headRefName":"codex/misc"},{"number":24,"title":"Close crash recovery PR after replacement","url":"https://example.com/pr/24","headRefName":"codex/recovery"}]"#,
            ),
            None,
        );

        assert_eq!(suggestions.len(), 2);
        assert_eq!(suggestions[0].number, 24);
        assert!(suggestions[0].source.contains("keyword-matched"));
    }

    #[test]
    fn build_github_target_suggestions_maps_open_issues_for_reopen_issue_requests() {
        let suggestions = build_github_target_suggestions_from(
            &SupervisedGithubActionRequest {
                kind: SupervisedGithubActionKind::ReopenIssue,
                repository: Some("jessybrenenstahl/AIM".into()),
                issue_number: None,
                pull_request_number: None,
                body: Some("Reopen the crash recovery tracking issue.".into()),
                label: None,
                assignee: None,
                justification: Some("The tracking issue still needs follow-up.".into()),
            },
            SupervisedGithubActionKind::ReopenIssue,
            Some("main"),
            None,
            None,
            Some(
                r#"[{"number":28,"title":"Crash recovery tracking issue","url":"https://example.com/issues/28"}]"#,
            ),
        );

        assert_eq!(suggestions.len(), 1);
        assert_eq!(suggestions[0].number, 28);
        assert!(suggestions[0].source.contains("keyword-matched"));
    }

    #[test]
    fn build_github_target_suggestions_maps_open_prs_for_reopen_pull_request_requests() {
        let suggestions = build_github_target_suggestions_from(
            &SupervisedGithubActionRequest {
                kind: SupervisedGithubActionKind::ReopenPullRequest,
                repository: Some("jessybrenenstahl/AIM".into()),
                issue_number: None,
                pull_request_number: None,
                body: Some("Reopen the crash recovery PR for follow-up review.".into()),
                label: None,
                assignee: None,
                justification: Some("The pull request still needs supervised follow-up.".into()),
            },
            SupervisedGithubActionKind::ReopenPullRequest,
            Some("main"),
            None,
            Some(
                r#"[{"number":12,"title":"Misc cleanup","url":"https://example.com/pr/12","headRefName":"codex/misc"},{"number":36,"title":"Reopen crash recovery PR for follow-up","url":"https://example.com/pr/36","headRefName":"codex/recovery"}]"#,
            ),
            None,
        );

        assert_eq!(suggestions.len(), 2);
        assert_eq!(suggestions[0].number, 36);
        assert!(suggestions[0].source.contains("keyword-matched"));
    }

    #[test]
    fn build_github_target_suggestions_maps_open_prs_for_remove_label_pull_request_requests() {
        let suggestions = build_github_target_suggestions_from(
            &SupervisedGithubActionRequest {
                kind: SupervisedGithubActionKind::RemoveLabelPullRequest,
                repository: Some("jessybrenenstahl/AIM".into()),
                issue_number: None,
                pull_request_number: None,
                body: None,
                label: Some("needs-review".into()),
                assignee: None,
                justification: Some("Remove the stale label after supervised review.".into()),
            },
            SupervisedGithubActionKind::RemoveLabelPullRequest,
            Some("main"),
            None,
            Some(
                r#"[{"number":15,"title":"Misc cleanup","url":"https://example.com/pr/15","headRefName":"codex/misc"},{"number":41,"title":"Remove stale review label guidance","url":"https://example.com/pr/41","headRefName":"codex/review"}]"#,
            ),
            None,
        );

        assert_eq!(suggestions.len(), 2);
        assert_eq!(suggestions[0].number, 41);
        assert!(suggestions[0].source.contains("keyword-matched"));
    }

    #[test]
    fn build_github_target_suggestions_maps_open_prs_for_assign_pull_request_requests() {
        let suggestions = build_github_target_suggestions_from(
            &SupervisedGithubActionRequest {
                kind: SupervisedGithubActionKind::AssignPullRequest,
                repository: Some("jessybrenenstahl/AIM".into()),
                issue_number: None,
                pull_request_number: None,
                body: None,
                label: None,
                assignee: Some("@copilot".into()),
                justification: Some("Assign the review follow-up PR.".into()),
            },
            SupervisedGithubActionKind::AssignPullRequest,
            Some("main"),
            None,
            Some(
                r#"[{"number":18,"title":"Misc cleanup","url":"https://example.com/pr/18","headRefName":"codex/misc"},{"number":45,"title":"Assign review follow-up PR","url":"https://example.com/pr/45","headRefName":"codex/review"}]"#,
            ),
            None,
        );

        assert_eq!(suggestions.len(), 2);
        assert_eq!(suggestions[0].number, 45);
        assert!(suggestions[0].source.contains("keyword-matched"));
    }

    #[test]
    fn build_github_target_suggestions_promotes_keyword_matched_pull_requests() {
        let suggestions = build_github_target_suggestions_from(
            &SupervisedGithubActionRequest {
                kind: SupervisedGithubActionKind::CommentPullRequest,
                repository: Some("jessybrenenstahl/AIM".into()),
                issue_number: None,
                pull_request_number: None,
                body: Some("Please comment on the crash recovery relaunch path.".into()),
                label: None,
                assignee: None,
                justification: Some("Crash recovery relaunch hardening is the focus.".into()),
            },
            SupervisedGithubActionKind::CommentPullRequest,
            Some("main"),
            None,
            Some(
                r#"[{"number":22,"title":"Misc cleanup","url":"https://example.com/pr/22","headRefName":"codex/misc"},{"number":31,"title":"Harden crash recovery relaunch flow","url":"https://example.com/pr/31","headRefName":"codex/recovery"}]"#,
            ),
            None,
        );

        assert_eq!(suggestions.len(), 2);
        assert_eq!(suggestions[0].number, 31);
        assert!(suggestions[0].source.contains("keyword-matched"));
    }

    #[test]
    fn build_github_target_suggestions_promotes_keyword_matched_issues() {
        let suggestions = build_github_target_suggestions_from(
            &SupervisedGithubActionRequest {
                kind: SupervisedGithubActionKind::CommentIssue,
                repository: Some("jessybrenenstahl/AIM".into()),
                issue_number: None,
                pull_request_number: None,
                body: Some("Need an issue about crash recovery relaunch guidance.".into()),
                label: None,
                assignee: None,
                justification: Some("Crash recovery guidance should be tracked.".into()),
            },
            SupervisedGithubActionKind::CommentIssue,
            Some("main"),
            None,
            None,
            Some(
                r#"[{"number":8,"title":"Misc operator cleanup","url":"https://example.com/issues/8"},{"number":44,"title":"Crash recovery relaunch guidance","url":"https://example.com/issues/44"}]"#,
            ),
        );

        assert_eq!(suggestions.len(), 2);
        assert_eq!(suggestions[0].number, 44);
        assert!(suggestions[0].source.contains("keyword-matched"));
    }

    #[test]
    fn build_github_target_guidance_for_pull_requests_mentions_manual_fallback() {
        let root = tempdir().expect("tempdir");
        let guidance = build_github_target_guidance(
            &test_operator_paths(root.path()),
            &SupervisedGithubActionRequest {
                kind: SupervisedGithubActionKind::CommentPullRequest,
                repository: Some("jessybrenenstahl/AIM".into()),
                issue_number: None,
                pull_request_number: None,
                body: Some("body".into()),
                label: None,
                assignee: None,
                justification: None,
            },
            &[],
        )
        .expect("guidance");

        assert!(guidance.contains("Open or locate the intended pull request"));
        assert!(guidance.contains("enter its number manually"));
        assert!(guidance.contains("refreshes automatically"));
    }

    #[test]
    fn build_github_target_guidance_for_issue_without_suggestions_mentions_manual_fallback() {
        let root = tempdir().expect("tempdir");
        let guidance = build_github_target_guidance(
            &test_operator_paths(root.path()),
            &SupervisedGithubActionRequest {
                kind: SupervisedGithubActionKind::CommentIssue,
                repository: Some("jessybrenenstahl/AIM".into()),
                issue_number: None,
                pull_request_number: None,
                body: Some("body".into()),
                label: None,
                assignee: None,
                justification: None,
            },
            &[],
        )
        .expect("guidance");

        assert!(guidance.contains("Enter the intended issue number manually"));
        assert!(guidance.contains("reject/clear"));
    }

    #[test]
    fn build_github_target_guidance_with_suggestions_supports_operator_choice() {
        let root = tempdir().expect("tempdir");
        let guidance = build_github_target_guidance(
            &test_operator_paths(root.path()),
            &SupervisedGithubActionRequest {
                kind: SupervisedGithubActionKind::CommentPullRequest,
                repository: Some("jessybrenenstahl/AIM".into()),
                issue_number: None,
                pull_request_number: None,
                body: Some("body".into()),
                label: None,
                assignee: None,
                justification: None,
            },
            &[OperatorGithubTargetSuggestion {
                number: 17,
                title: "Current branch PR".into(),
                url: Some("https://example.com/pr/17".into()),
                source: "open PR for main".into(),
            }],
        )
        .expect("guidance");

        assert!(guidance.contains("suggested pull requests"));
        assert!(guidance.contains("different pull request number manually"));
    }

    #[test]
    fn record_pending_github_action_request_persists_queued_history() -> anyhow::Result<()> {
        let root = tempdir()?;
        let controller = HarnessController::new(test_operator_paths(root.path()))?;
        let record = pending_github_request_record(SupervisedGithubActionRequest {
            kind: SupervisedGithubActionKind::CommentIssue,
            repository: Some("owner/repo".into()),
            issue_number: Some(7),
            pull_request_number: None,
            body: Some("hello".into()),
            label: None,
            assignee: None,
            justification: Some("because".into()),
        });

        controller.record_pending_github_action_request(&record)?;

        let pending = controller
            .read_github_action_request()?
            .context("expected pending request")?;
        assert_eq!(pending.summary, record.summary);
        let history = controller.read_recent_github_action_history(5)?;
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].state, OperatorGithubActionLifecycleState::Queued);
        assert_eq!(history[0].request.issue_number, Some(7));
        assert!(history[0].summary.contains("queued GitHub request"));
        Ok(())
    }

    #[test]
    fn read_snapshot_ignores_stale_queued_github_history_without_pending_request()
    -> anyhow::Result<()> {
        let root = tempdir()?;
        let controller = HarnessController::new(test_operator_paths(root.path()))?;
        let record = pending_github_request_record(SupervisedGithubActionRequest {
            kind: SupervisedGithubActionKind::CommentIssue,
            repository: Some("owner/repo".into()),
            issue_number: Some(7),
            pull_request_number: None,
            body: Some("hello".into()),
            label: None,
            assignee: None,
            justification: Some("because".into()),
        });

        controller.record_pending_github_action_request(&record)?;
        fs::remove_file(&controller.paths.github_action_request_path)?;

        let snapshot = run_async(|| async {
            controller
                .read_snapshot("idle", OperatorRunMode::Idle, "foreground", None, 0, None)
                .await
        })?;

        assert!(!snapshot.github_action_pending);
        assert_eq!(snapshot.github_action_state, None);
        assert_eq!(snapshot.github_action_latest_summary, None);
        assert_eq!(snapshot.github_action_kind, None);
        Ok(())
    }

    #[test]
    fn clear_and_reject_github_action_request_record_distinct_history() -> anyhow::Result<()> {
        let root = tempdir()?;
        let controller = HarnessController::new(test_operator_paths(root.path()))?;
        let record = pending_github_request_record(SupervisedGithubActionRequest {
            kind: SupervisedGithubActionKind::CommentIssue,
            repository: Some("owner/repo".into()),
            issue_number: Some(9),
            pull_request_number: None,
            body: Some("hello".into()),
            label: None,
            assignee: None,
            justification: None,
        });

        controller.record_pending_github_action_request(&record)?;
        let clear_summary = controller.clear_github_action_request()?;
        assert!(clear_summary.contains("cleared GitHub request"));
        let history = controller.read_recent_github_action_history(5)?;
        assert_eq!(
            history.last().map(|record| record.state),
            Some(OperatorGithubActionLifecycleState::Cleared)
        );

        controller.record_pending_github_action_request(&record)?;
        let reject_summary = controller.reject_github_action_request()?;
        assert!(reject_summary.contains("rejected GitHub request"));
        let history = controller.read_recent_github_action_history(10)?;
        assert_eq!(
            history.last().map(|record| record.state),
            Some(OperatorGithubActionLifecycleState::Rejected)
        );
        assert!(controller.read_github_action_request()?.is_none());
        Ok(())
    }

    #[test]
    fn apply_github_action_request_records_applied_history_with_target_override()
    -> anyhow::Result<()> {
        let root = tempdir()?;
        let controller = HarnessController::new(test_operator_paths(root.path()))?;
        let record = pending_github_request_record(SupervisedGithubActionRequest {
            kind: SupervisedGithubActionKind::CommentPullRequest,
            repository: Some("owner/repo".into()),
            issue_number: None,
            pull_request_number: None,
            body: Some("ship it".into()),
            label: None,
            assignee: None,
            justification: Some("because".into()),
        });

        controller.record_pending_github_action_request(&record)?;
        let summary =
            controller.apply_github_action_request_with_runner(Some("42".into()), |request| {
                assert_eq!(request.pull_request_number, Some(42));
                Ok("https://github.com/owner/repo/pull/42#issuecomment-1".into())
            })?;

        assert!(summary.contains("applied GitHub request"));
        assert!(controller.read_github_action_request()?.is_none());
        let history = controller.read_recent_github_action_history(5)?;
        let applied = history.last().context("expected applied record")?;
        assert_eq!(applied.state, OperatorGithubActionLifecycleState::Applied);
        assert_eq!(applied.request.pull_request_number, Some(42));
        assert!(applied.summary.contains("issuecomment-1"));
        assert_eq!(
            applied.result_excerpt.as_deref(),
            Some("https://github.com/owner/repo/pull/42#issuecomment-1")
        );
        assert_eq!(
            applied.result_url.as_deref(),
            Some("https://github.com/owner/repo/pull/42#issuecomment-1")
        );
        Ok(())
    }

    #[test]
    fn read_snapshot_surfaces_latest_applied_github_action_after_pending_clears()
    -> anyhow::Result<()> {
        let root = tempdir()?;
        let controller = HarnessController::new(test_operator_paths(root.path()))?;
        let record = pending_github_request_record(SupervisedGithubActionRequest {
            kind: SupervisedGithubActionKind::CommentPullRequest,
            repository: Some("owner/repo".into()),
            issue_number: None,
            pull_request_number: None,
            body: Some("ship it".into()),
            label: None,
            assignee: None,
            justification: Some("because".into()),
        });

        controller.record_pending_github_action_request(&record)?;
        controller.apply_github_action_request_with_runner(Some("42".into()), |_request| {
            Ok("https://github.com/owner/repo/pull/42#issuecomment-1".into())
        })?;

        let snapshot = run_async(|| async {
            controller
                .read_snapshot("idle", OperatorRunMode::Idle, "foreground", None, 0, None)
                .await
        })?;

        assert!(!snapshot.github_action_pending);
        assert_eq!(snapshot.github_action_state.as_deref(), Some("applied"));
        assert_eq!(
            snapshot.github_action_kind.as_deref(),
            Some("comment_pull_request")
        );
        assert_eq!(
            snapshot.github_action_repository.as_deref(),
            Some("owner/repo")
        );
        assert_eq!(
            snapshot.github_action_target.as_deref(),
            Some("owner/repo pull request #42")
        );
        assert_eq!(snapshot.github_action_body.as_deref(), Some("ship it"));
        assert_eq!(
            snapshot.github_action_justification.as_deref(),
            Some("because")
        );
        assert_eq!(snapshot.github_action_detail.as_deref(), Some("narrative"));
        assert_eq!(
            snapshot.github_action_result_excerpt.as_deref(),
            Some("https://github.com/owner/repo/pull/42#issuecomment-1")
        );
        assert_eq!(
            snapshot.github_action_result_url.as_deref(),
            Some("https://github.com/owner/repo/pull/42#issuecomment-1")
        );
        Ok(())
    }

    #[test]
    fn read_snapshot_surfaces_latest_rejected_github_action_after_pending_clears()
    -> anyhow::Result<()> {
        let root = tempdir()?;
        let controller = HarnessController::new(test_operator_paths(root.path()))?;
        let record = pending_github_request_record(SupervisedGithubActionRequest {
            kind: SupervisedGithubActionKind::CommentIssue,
            repository: Some("owner/repo".into()),
            issue_number: Some(9),
            pull_request_number: None,
            body: Some("not now".into()),
            label: None,
            assignee: None,
            justification: Some("wait".into()),
        });

        controller.record_pending_github_action_request(&record)?;
        controller.reject_github_action_request()?;

        let snapshot = run_async(|| async {
            controller
                .read_snapshot("idle", OperatorRunMode::Idle, "foreground", None, 0, None)
                .await
        })?;

        assert!(!snapshot.github_action_pending);
        assert_eq!(snapshot.github_action_state.as_deref(), Some("rejected"));
        assert_eq!(
            snapshot.github_action_kind.as_deref(),
            Some("comment_issue")
        );
        assert_eq!(
            snapshot.github_action_repository.as_deref(),
            Some("owner/repo")
        );
        assert_eq!(
            snapshot.github_action_target.as_deref(),
            Some("owner/repo issue #9")
        );
        assert_eq!(snapshot.github_action_body.as_deref(), Some("not now"));
        assert_eq!(
            snapshot.github_action_justification.as_deref(),
            Some("wait")
        );
        assert_eq!(snapshot.github_action_detail.as_deref(), Some("narrative"));
        assert_eq!(
            snapshot.github_action_result_excerpt.as_deref(),
            Some("operator rejected pending GitHub request")
        );
        assert_eq!(snapshot.github_action_result_url, None);
        Ok(())
    }

    #[test]
    fn clear_github_action_request_records_explicit_outcome_detail() -> anyhow::Result<()> {
        let root = tempdir()?;
        let controller = HarnessController::new(test_operator_paths(root.path()))?;
        let record = pending_github_request_record(SupervisedGithubActionRequest {
            kind: SupervisedGithubActionKind::CommentIssue,
            repository: Some("owner/repo".into()),
            issue_number: Some(13),
            pull_request_number: None,
            body: Some("later".into()),
            label: None,
            assignee: None,
            justification: Some("pause".into()),
        });

        controller.record_pending_github_action_request(&record)?;
        controller.clear_github_action_request()?;

        let history = controller.read_recent_github_action_history(5)?;
        let cleared = history.last().context("expected cleared record")?;
        assert_eq!(cleared.state, OperatorGithubActionLifecycleState::Cleared);
        assert_eq!(
            cleared.result_excerpt.as_deref(),
            Some("operator cleared pending GitHub request before apply")
        );
        assert_eq!(cleared.result_url, None);
        Ok(())
    }

    #[test]
    fn summarize_github_action_result_extracts_excerpt_and_url() {
        let (excerpt, url) = summarize_github_action_result(
            "updated issue successfully\nhttps://github.com/owner/repo/issues/12\n",
        );

        assert_eq!(excerpt.as_deref(), Some("updated issue successfully"));
        assert_eq!(
            url.as_deref(),
            Some("https://github.com/owner/repo/issues/12")
        );
    }

    #[test]
    fn describe_auth_state_marks_ready_profile_as_launchable() {
        let now = Utc::now();
        let profile = AuthProfile {
            id: Uuid::new_v4(),
            provider: ProviderKind::OpenAiCodex,
            mode: AuthMode::OAuth,
            label: "codex-gui".into(),
            oauth: Some(OAuthState {
                issuer: "https://auth.openai.com".into(),
                account_label: Some("operator@example.com".into()),
                access_token: Some("token".into()),
                refresh_token: Some("refresh".into()),
                expires_at: Some(now + Duration::minutes(45)),
            }),
            api_key: None,
            updated_at: now,
        };

        let (ready, readiness, summary) = describe_auth_state(Some(&profile), &[]);
        assert!(ready);
        assert_eq!(readiness, "ready");
        assert!(summary.contains("default profile=codex-gui"));
        assert!(summary.contains("pending_oauth=0"));
    }

    #[test]
    fn describe_operator_auth_state_prefers_logged_in_codex_cli() {
        let cli = CodexCliStatus {
            available: true,
            logged_in: true,
            summary: "ready via C:\\Users\\jessy\\AppData\\Roaming\\npm\\codex.cmd | Logged in using ChatGPT".into(),
            command_path: Some(PathBuf::from(r"C:\Users\jessy\AppData\Roaming\npm\codex.cmd")),
            account_summary: Some("Logged in using ChatGPT".into()),
        };

        let (ready, readiness, summary) = describe_operator_auth_state(
            &cli,
            false,
            "blocked: no default auth profile configured",
            "no default auth profile configured | pending_oauth=0",
        );

        assert!(ready);
        assert_eq!(readiness, "ready via Codex CLI");
        assert!(summary.contains("primary=Codex CLI ready"));
        assert!(summary.contains("provider_fallback=blocked: no default auth profile configured"));
    }

    #[test]
    fn describe_operator_auth_state_uses_fallback_when_cli_needs_login() {
        let cli = CodexCliStatus {
            available: true,
            logged_in: false,
            summary: "codex login required".into(),
            command_path: Some(PathBuf::from(r"C:\Users\jessy\AppData\Roaming\npm\codex.cmd")),
            account_summary: None,
        };

        let (ready, readiness, summary) = describe_operator_auth_state(
            &cli,
            true,
            "ready",
            "default profile=codex-gui provider=OpenAiCodex mode=OAuth pending_oauth=0",
        );

        assert!(ready);
        assert_eq!(
            readiness,
            "ready via provider fallback (Codex CLI login required for primary lane)"
        );
        assert!(summary.contains("primary=Codex CLI login required"));
        assert!(summary.contains("provider_fallback=default profile=codex-gui"));
    }

    #[test]
    fn build_pending_oauth_view_maps_device_code_details() {
        let pending = PendingOAuthAuthorization {
            id: Uuid::new_v4(),
            profile_id: Uuid::new_v4(),
            provider: ProviderKind::OpenAiApi,
            label: "api-device".into(),
            issuer: "https://auth.openai.com".into(),
            started_at: Utc::now(),
            expires_at: Some(Utc::now() + Duration::minutes(10)),
            client_id: "client-id".into(),
            token_endpoint: "https://auth.openai.com/oauth/token".into(),
            scopes: vec!["responses.read".into()],
            state: None,
            pkce_verifier: None,
            kind: OAuthAuthorizationKind::DeviceCode {
                verification_uri: "https://auth.openai.com/activate".into(),
                user_code: "ABCD-EFGH".into(),
                device_code: "device-code".into(),
                poll_interval_seconds: 5,
            },
        };

        let view = build_pending_oauth_view(&pending);
        assert_eq!(view.kind, "device_code");
        assert_eq!(
            view.verification_uri.as_deref(),
            Some("https://auth.openai.com/activate")
        );
        assert_eq!(view.user_code.as_deref(), Some("ABCD-EFGH"));
        assert!(view.authorization_url.is_none());
        assert!(view.action_hint.contains("Open the verification URL"));
    }

    #[test]
    fn browser_callback_helpers_extract_listener_and_callback_url() -> anyhow::Result<()> {
        assert_eq!(
            browser_callback_bind_target("http://127.0.0.1:1455/auth/callback")?,
            "127.0.0.1:1455"
        );
        let request =
            "GET /auth/callback?code=test-code&state=abc HTTP/1.1\r\nHost: 127.0.0.1:1455\r\n\r\n";
        let target = extract_http_request_target(request).context("expected request target")?;
        assert_eq!(target, "/auth/callback?code=test-code&state=abc");
        assert!(request_target_matches_callback_path(
            target,
            "/auth/callback"
        ));
        assert_eq!(
            build_browser_callback_url("http://127.0.0.1:1455/auth/callback", target),
            "http://127.0.0.1:1455/auth/callback?code=test-code&state=abc"
        );
        Ok(())
    }

    #[test]
    fn interactive_oauth_launch_status_reports_builtin_codex_client() {
        let status =
            interactive_oauth_launch_status_with(OperatorAuthProvider::OpenAiCodex, |_| None);
        assert!(status.ready);
        assert!(status.env_name.is_none());
        assert!(status.built_in);
        assert!(status.summary.contains("built-in OpenAI Codex client id"));
    }

    #[test]
    fn interactive_oauth_launch_status_reports_missing_client_id_for_openai_api() {
        let status =
            interactive_oauth_launch_status_with(OperatorAuthProvider::OpenAiApi, |_| None);
        assert!(!status.ready);
        assert!(status.env_name.is_none());
        assert!(!status.built_in);
        assert!(status.summary.contains("SPLCW_OPENAI_API_OAUTH_CLIENT_ID"));
    }

    #[test]
    fn resolve_interactive_oauth_client_id_prefers_first_matching_env_name() {
        let resolved =
            resolve_interactive_oauth_client_id_with(OperatorAuthProvider::OpenAiApi, |name| {
                match name {
                    "SPLCW_OPENAI_API_OAUTH_CLIENT_ID" => Some(" api-client ".into()),
                    "OPENAI_API_OAUTH_CLIENT_ID" => Some("backup-client".into()),
                    _ => None,
                }
            });

        assert_eq!(
            resolved,
            Some(("SPLCW_OPENAI_API_OAUTH_CLIENT_ID", "api-client".into()))
        );
    }

    #[test]
    fn operator_env_status_reports_selected_provider_configured_from_file() {
        let launch_status = InteractiveOAuthLaunchStatus {
            ready: true,
            summary: "ready via SPLCW_OPENAI_API_OAUTH_CLIENT_ID".into(),
            env_name: Some("SPLCW_OPENAI_API_OAUTH_CLIENT_ID"),
            built_in: false,
        };
        let status = operator_env_config_status(
            OperatorAuthProvider::OpenAiApi,
            &["SPLCW_OPENAI_API_OAUTH_CLIENT_ID".into()],
            &launch_status,
        );
        assert!(status.configured);
        assert!(status.summary.contains("configured for selected provider"));
        assert!(status.summary.contains("SPLCW_OPENAI_API_OAUTH_CLIENT_ID"));
    }

    #[test]
    fn operator_env_status_reports_process_env_fallback_when_file_missing() {
        let launch_status = InteractiveOAuthLaunchStatus {
            ready: true,
            summary: "ready via OPENAI_CODEX_OAUTH_CLIENT_ID".into(),
            env_name: Some("OPENAI_CODEX_OAUTH_CLIENT_ID"),
            built_in: false,
        };
        let status =
            operator_env_config_status(OperatorAuthProvider::OpenAiCodex, &[], &launch_status);
        assert!(!status.configured);
        assert!(status.summary.contains("process env"));
        assert!(status.summary.contains("OPENAI_CODEX_OAUTH_CLIENT_ID"));
    }

    #[test]
    fn operator_env_status_reports_builtin_codex_default_when_file_missing() {
        let launch_status = InteractiveOAuthLaunchStatus {
            ready: true,
            summary: "ready via built-in OpenAI Codex client id".into(),
            env_name: None,
            built_in: true,
        };
        let status =
            operator_env_config_status(OperatorAuthProvider::OpenAiCodex, &[], &launch_status);
        assert!(!status.configured);
        assert!(status.summary.contains("built-in OpenAI Codex client id"));
    }

    #[test]
    fn operator_env_status_reports_missing_selected_provider_config_for_openai_api() {
        let launch_status = InteractiveOAuthLaunchStatus {
            ready: false,
            summary: "blocked".into(),
            env_name: None,
            built_in: false,
        };
        let status =
            operator_env_config_status(OperatorAuthProvider::OpenAiApi, &[], &launch_status);
        assert!(!status.configured);
        assert!(
            status
                .summary
                .contains("missing selected-provider client id")
        );
        assert!(status.summary.contains("SPLCW_OPENAI_API_OAUTH_CLIENT_ID"));
    }

    #[test]
    fn discover_openclaw_command_prefers_operator_env_override() -> anyhow::Result<()> {
        let root = tempdir()?;
        let openclaw = root.path().join("custom-openclaw.cmd");
        std::fs::write(&openclaw, "@echo off\r\n")?;

        let resolved = discover_openclaw_command_with(
            |name| match name {
                "OPENCLAW_BIN" => Some(openclaw.display().to_string()),
                _ => None,
            },
            false,
        );

        assert_eq!(resolved, Some(openclaw));
        Ok(())
    }

    #[test]
    fn discover_codex_command_finds_appdata_npm_shim() -> anyhow::Result<()> {
        let root = tempdir()?;
        let npm_dir = root.path().join("npm");
        std::fs::create_dir_all(&npm_dir)?;
        let codex = npm_dir.join("codex.cmd");
        std::fs::write(&codex, "@echo off\r\n")?;

        let resolved = discover_codex_command_with(
            |name| match name {
                "APPDATA" => Some(root.path().display().to_string()),
                _ => None,
            },
            false,
        );

        assert_eq!(resolved, Some(codex));
        Ok(())
    }

    #[test]
    fn windows_codex_npm_shim_command_prefers_js_entrypoint() -> anyhow::Result<()> {
        let root = tempdir()?;
        let npm_dir = root.path().join("npm");
        let script_dir = npm_dir
            .join("node_modules")
            .join("@openai")
            .join("codex")
            .join("bin");
        std::fs::create_dir_all(&script_dir)?;
        let codex = npm_dir.join("codex.cmd");
        let script = script_dir.join("codex.js");
        std::fs::write(&codex, "@echo off\r\n")?;
        std::fs::write(&script, "console.log('codex')\n")?;

        let (program, bootstrap_args) = windows_codex_npm_shim_command(&codex)
            .context("expected npm shim bypass")?;

        assert_eq!(program, PathBuf::from("node"));
        assert_eq!(bootstrap_args, vec![script.display().to_string()]);
        Ok(())
    }

    #[test]
    fn openclaw_cli_status_reports_appdata_npm_shim() -> anyhow::Result<()> {
        let root = tempdir()?;
        let npm_dir = root.path().join("npm");
        std::fs::create_dir_all(&npm_dir)?;
        let openclaw = npm_dir.join("openclaw.cmd");
        std::fs::write(&openclaw, "@echo off\r\n")?;

        let status = openclaw_cli_status_with(|name| match name {
            "APPDATA" => Some(root.path().display().to_string()),
            _ => None,
        });

        assert!(status.available);
        assert_eq!(status.command_path, Some(openclaw.clone()));
        assert!(status.summary.contains(&openclaw.display().to_string()));
        Ok(())
    }

    #[test]
    fn parse_codex_login_status_detects_logged_in_cli() {
        let (logged_in, summary) =
            parse_codex_login_status("Logged in using ChatGPT\nAccount: test-user");
        assert!(logged_in);
        assert_eq!(
            summary.as_deref(),
            Some("Logged in using ChatGPT\nAccount: test-user")
        );
    }

    #[test]
    fn parse_codex_cli_exec_output_collects_session_reply_and_events() {
        let stdout = r#"{"type":"thread.started","thread_id":"thread-123"}
{"type":"turn.started"}
{"type":"item.completed","item":{"type":"agent_message","text":"CLI reply body"}}
{"type":"turn.completed"}"#;
        let stderr = "warning: cli warning";
        let parsed = parse_codex_cli_exec_output(stdout, stderr);
        assert_eq!(parsed.session_id.as_deref(), Some("thread-123"));
        assert_eq!(parsed.reply, "CLI reply body");
        assert!(parsed.summary.contains("CLI reply body"));
        assert!(
            parsed
                .event_lines
                .iter()
                .any(|line| line == "thread.started thread-123")
        );
        assert!(
            parsed
                .event_lines
                .iter()
                .any(|line| line == "turn.completed")
        );
        assert_eq!(
            parsed.warning_lines,
            vec!["warning: cli warning".to_string()]
        );
    }

    #[test]
    fn load_openclaw_import_plan_prefers_main_store_and_last_good_profile() -> anyhow::Result<()> {
        let root = tempdir()?;
        let main_store = root
            .path()
            .join("agents")
            .join("main")
            .join("agent")
            .join("auth-profiles.json");
        let secondary_store = root
            .path()
            .join("agents")
            .join("scratch")
            .join("agent")
            .join("auth-profiles.json");
        std::fs::create_dir_all(main_store.parent().unwrap())?;
        std::fs::create_dir_all(secondary_store.parent().unwrap())?;
        std::fs::write(
            &secondary_store,
            serde_json::json!({
                "version": 1,
                "profiles": {
                    "openai-codex:default": {
                        "type": "oauth",
                        "provider": "openai-codex",
                        "access": "secondary-access",
                        "refresh": "secondary-refresh",
                        "expires": 1000
                    }
                }
            })
            .to_string(),
        )?;
        std::fs::write(
            &main_store,
            serde_json::json!({
                "version": 1,
                "profiles": {
                    "openai-codex:default": {
                        "type": "oauth",
                        "provider": "openai-codex",
                        "access": "default-access",
                        "refresh": "default-refresh",
                        "expires": 1000
                    },
                    "openai-codex:preferred": {
                        "type": "oauth",
                        "provider": "openai-codex",
                        "access": "preferred-access",
                        "refresh": "preferred-refresh",
                        "expires": 2000,
                        "email": "operator@example.com"
                    }
                },
                "lastGood": {
                    "openai-codex": "openai-codex:preferred"
                },
                "usageStats": {
                    "openai-codex:preferred": {
                        "lastUsed": 2000
                    }
                }
            })
            .to_string(),
        )?;

        let plan = load_openclaw_import_plan_from_paths(&[secondary_store, main_store.clone()])?
            .context("expected an import plan")?;
        assert_eq!(plan.source_path, main_store);
        assert_eq!(
            plan.profiles[plan.preferred_index].source_profile_id,
            "openai-codex:preferred"
        );
        assert_eq!(
            plan.profiles[plan.preferred_index].account_label.as_deref(),
            Some("operator@example.com")
        );
        Ok(())
    }

    #[test]
    fn load_openclaw_import_plan_ignores_non_codex_profiles() -> anyhow::Result<()> {
        let root = tempdir()?;
        let store_path = root.path().join("auth-profiles.json");
        std::fs::write(
            &store_path,
            serde_json::json!({
                "version": 1,
                "profiles": {
                    "openai:default": {
                        "type": "oauth",
                        "provider": "openai",
                        "access": "api-access",
                        "refresh": "api-refresh",
                        "expires": 1234
                    },
                    "openai-codex:default": {
                        "mode": "oauth",
                        "provider": "openai-codex",
                        "access": "codex-access",
                        "refresh": "codex-refresh",
                        "expires": 5678
                    }
                }
            })
            .to_string(),
        )?;

        let plan =
            load_openclaw_import_plan_from_paths(&[store_path])?.context("expected import plan")?;
        assert_eq!(plan.profiles.len(), 1);
        assert_eq!(plan.profiles[0].source_profile_id, "openai-codex:default");
        assert_eq!(plan.profiles[0].provider, ProviderKind::OpenAiCodex);
        Ok(())
    }

    #[test]
    fn import_openclaw_plan_into_auth_store_sets_default_profile() -> anyhow::Result<()> {
        let root = tempdir()?;
        let auth_store_path = root.path().join("auth-profiles.json");
        let store = splcw_llm::FileAuthProfileStore::new(&auth_store_path);
        let plan = super::OpenClawImportPlan {
            source_path: root.path().join("openclaw").join("auth-profiles.json"),
            profiles: vec![super::OpenClawImportProfile {
                source_profile_id: "openai-codex:default".into(),
                provider: ProviderKind::OpenAiCodex,
                label: "OpenClaw Codex [openai-codex:default]".into(),
                account_label: Some("operator@example.com".into()),
                access_token: Some("access-token".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::minutes(30)),
            }],
            preferred_index: 0,
        };

        let (count, label, source_profile_id) =
            run_async(|| async { import_openclaw_plan_into_auth_store(&store, &plan).await })?;
        assert_eq!(count, 1);
        assert_eq!(label, "OpenClaw Codex [openai-codex:default]");
        assert_eq!(source_profile_id, "openai-codex:default");

        let default_profile = run_async(|| async { store.load_default_profile().await })?
            .context("expected imported default profile")?;
        assert_eq!(
            default_profile.label,
            "OpenClaw Codex [openai-codex:default]"
        );
        assert_eq!(default_profile.provider, ProviderKind::OpenAiCodex);
        assert_eq!(
            default_profile
                .oauth
                .as_ref()
                .and_then(|oauth| oauth.refresh_token.as_deref()),
            Some("refresh-token")
        );
        Ok(())
    }

    #[test]
    fn maybe_bootstrap_openclaw_codex_auth_imports_when_default_is_missing() -> anyhow::Result<()> {
        let root = tempdir()?;
        let auth_store_path = root.path().join("auth-profiles.json");
        let store = splcw_llm::FileAuthProfileStore::new(&auth_store_path);
        let plan = super::OpenClawImportPlan {
            source_path: root.path().join("openclaw").join("auth-profiles.json"),
            profiles: vec![super::OpenClawImportProfile {
                source_profile_id: "openai-codex:default".into(),
                provider: ProviderKind::OpenAiCodex,
                label: "OpenClaw Codex [openai-codex:default]".into(),
                account_label: Some("operator@example.com".into()),
                access_token: Some("access-token".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::minutes(30)),
            }],
            preferred_index: 0,
        };

        let outcome = run_async(|| async {
            maybe_bootstrap_openclaw_codex_auth_into_store(&store, Some(plan)).await
        })?
        .context("expected import outcome")?;
        assert_eq!(outcome.imported_count, 1);
        assert_eq!(outcome.default_source_profile_id, "openai-codex:default");

        let default_profile = run_async(|| async { store.load_default_profile().await })?
            .context("expected imported default profile")?;
        assert_eq!(default_profile.provider, ProviderKind::OpenAiCodex);
        Ok(())
    }

    #[test]
    fn maybe_bootstrap_openclaw_codex_auth_skips_when_default_exists() -> anyhow::Result<()> {
        let root = tempdir()?;
        let auth_store_path = root.path().join("auth-profiles.json");
        let store = splcw_llm::FileAuthProfileStore::new(&auth_store_path);
        let existing_profile = AuthProfile {
            id: Uuid::new_v4(),
            provider: ProviderKind::OpenAiApi,
            mode: AuthMode::OAuth,
            label: "Existing API OAuth".into(),
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("existing@example.com".into()),
                access_token: Some("existing-access".into()),
                refresh_token: Some("existing-refresh".into()),
                expires_at: Some(Utc::now() + Duration::minutes(30)),
            }),
            api_key: None,
            updated_at: Utc::now(),
        };
        run_async(|| async {
            store.upsert_profile(&existing_profile).await?;
            store.set_default_profile(existing_profile.id).await
        })?;

        let plan = super::OpenClawImportPlan {
            source_path: root.path().join("openclaw").join("auth-profiles.json"),
            profiles: vec![super::OpenClawImportProfile {
                source_profile_id: "openai-codex:default".into(),
                provider: ProviderKind::OpenAiCodex,
                label: "OpenClaw Codex [openai-codex:default]".into(),
                account_label: Some("operator@example.com".into()),
                access_token: Some("access-token".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::minutes(30)),
            }],
            preferred_index: 0,
        };

        let outcome = run_async(|| async {
            maybe_bootstrap_openclaw_codex_auth_into_store(&store, Some(plan)).await
        })?;
        assert!(outcome.is_none());

        let default_profile = run_async(|| async { store.load_default_profile().await })?
            .context("expected preserved default profile")?;
        assert_eq!(default_profile.label, "Existing API OAuth");
        Ok(())
    }

    #[test]
    fn combine_optional_notices_joins_non_empty_segments() {
        assert_eq!(
            combine_optional_notices([
                Some("first notice".into()),
                None,
                Some("second notice".into()),
                Some(String::new()),
            ]),
            Some("first notice | second notice".into())
        );
    }

    #[test]
    fn objective_needs_project_artifact_context_detects_roadmap_work() {
        assert!(objective_needs_project_artifact_context(
            "Identify the next bounded roadmap-aligned implementation slice"
        ));
        assert!(objective_needs_project_artifact_context(
            "Use GitHub context so the harness can work autonomously"
        ));
        assert!(!objective_needs_project_artifact_context(
            "Safely click the focused button"
        ));
    }

    #[test]
    fn build_repo_git_context_reports_branch_status_and_commits() {
        let context = build_repo_git_context_from(
            Path::new("C:\\repo"),
            Some("main"),
            Some("f8899e77cd4797a127d3e04df7fa0e4b0959f720"),
            Some("origin/main"),
            Some("https://github.com/jessybrenenstahl/AIM"),
            Some(
                "## main...origin/main\n M ultimentality-pilot/harness/README.md\n?? offload/history/\n",
            ),
            Some("f8899e7 Add operator shell reattach ownership\n97bbe6e Keep operator handoff ownership target-scoped"),
        )
        .expect("expected repo context");

        assert!(context.contains("## Repo / GitHub Context"));
        assert!(context.contains("branch: main"));
        assert!(context.contains("tracked worktree: 1 tracked change(s)"));
        assert!(context.contains("untracked entries: 1"));
        assert!(context.contains("Recent Commits"));
        assert!(context.contains("Add operator shell reattach ownership"));
    }

    #[test]
    fn build_github_cli_context_reports_repo_prs_and_issues() {
        let context = build_github_cli_context_from(
            Some(
                "- repo: jessybrenenstahl/AIM\n- url: https://github.com/jessybrenenstahl/AIM\n- default branch: main\n",
            ),
            Some(
                "- #101 Add operator shell reattach ownership [OPEN] head=main base=main https://github.com/jessybrenenstahl/AIM/pull/101",
            ),
            Some(
                "- #102 Add bounded repo context [OPEN] head=codex/repo-context base=main https://github.com/jessybrenenstahl/AIM/pull/102",
            ),
            Some(
                "- #77 Give harness GitHub access [OPEN] https://github.com/jessybrenenstahl/AIM/issues/77",
            ),
        )
        .expect("expected GitHub context");

        assert!(context.contains("## GitHub Remote Context"));
        assert!(context.contains("repo: jessybrenenstahl/AIM"));
        assert!(context.contains("Pull Requests For Current Branch"));
        assert!(context.contains("Recent Open Pull Requests"));
        assert!(context.contains("Recent Open Issues"));
        assert!(context.contains("Give harness GitHub access"));
    }

    #[test]
    fn build_project_artifact_context_reads_project_docs() -> anyhow::Result<()> {
        let root = tempdir()?;
        let repo_root = root.path().join("repo");
        fs::create_dir_all(repo_root.join("ultimentality-pilot").join("harness"))?;
        fs::create_dir_all(repo_root.join("artifacts").join("ultimentality-pilot"))?;
        fs::create_dir_all(repo_root.join("artifacts").join("analysis"))?;
        fs::create_dir_all(repo_root.join("offload").join("current"))?;
        fs::create_dir_all(
            repo_root
                .join("artifacts")
                .join("ultimentality-pilot")
                .join("operator"),
        )?;
        fs::write(
            repo_root
                .join("artifacts")
                .join("ultimentality-pilot")
                .join("current-plan.md"),
            "# Current Plan\nplan body\n",
        )?;
        fs::write(
            repo_root
                .join("artifacts")
                .join("ultimentality-pilot")
                .join("roadmap.md"),
            "# Roadmap\nroadmap body\n",
        )?;
        fs::write(
            repo_root
                .join("artifacts")
                .join("analysis")
                .join("certainty-superslice.md"),
            "# Superslice\nsuperslice body\n",
        )?;
        fs::write(
            repo_root
                .join("offload")
                .join("current")
                .join("open-gaps.md"),
            "# Open Gaps\ngap body\n",
        )?;
        fs::write(
            repo_root.join("offload").join("current").join("handoff.md"),
            "# Handoff\nhandoff body\n",
        )?;

        let paths = OperatorPaths {
            repo_root: repo_root.clone(),
            harness_root: repo_root.join("ultimentality-pilot").join("harness"),
            operator_root: repo_root
                .join("artifacts")
                .join("ultimentality-pilot")
                .join("operator"),
            operator_env_path: repo_root
                .join("artifacts")
                .join("ultimentality-pilot")
                .join("operator")
                .join("operator.env"),
            status_path: repo_root
                .join("artifacts")
                .join("ultimentality-pilot")
                .join("operator")
                .join("status.json"),
            github_action_request_path: repo_root
                .join("artifacts")
                .join("ultimentality-pilot")
                .join("operator")
                .join("github-action-request.json"),
            github_action_history_path: repo_root
                .join("artifacts")
                .join("ultimentality-pilot")
                .join("operator")
                .join("github-action-history.jsonl"),
            background_runner_path: repo_root
                .join("artifacts")
                .join("ultimentality-pilot")
                .join("operator")
                .join("background-runner.json"),
            background_stop_path: repo_root
                .join("artifacts")
                .join("ultimentality-pilot")
                .join("operator")
                .join("background-stop.request"),
            background_handoff_path: repo_root
                .join("artifacts")
                .join("ultimentality-pilot")
                .join("operator")
                .join("background-handoff.json"),
            codex_cli_session_path: repo_root
                .join("artifacts")
                .join("ultimentality-pilot")
                .join("operator")
                .join("codex-cli-session.json"),
            codex_cli_live_stream_path: repo_root
                .join("artifacts")
                .join("ultimentality-pilot")
                .join("operator")
                .join("codex-cli-live-stream.json"),
            session_root: repo_root
                .join("artifacts")
                .join("ultimentality-pilot")
                .join("operator")
                .join("sessions"),
            session_id: DEFAULT_SESSION_ID.into(),
            state_db_path: repo_root
                .join("artifacts")
                .join("ultimentality-pilot")
                .join("operator")
                .join("state.sqlite"),
            auth_store_path: repo_root
                .join("artifacts")
                .join("ultimentality-pilot")
                .join("operator")
                .join("auth-profiles.json"),
        };

        let context = build_project_artifact_context(
            &paths,
            "Identify the next bounded roadmap-aligned implementation slice",
        )?
        .context("expected roadmap bundle")?;

        assert!(context.contains("## Project Current Plan"));
        assert!(context.contains("plan body"));
        assert!(context.contains("## Project Roadmap"));
        assert!(context.contains("roadmap body"));
        assert!(context.contains("## Certainty Superslice"));
        assert!(context.contains("superslice body"));
        assert!(context.contains("## Current Open Gaps"));
        assert!(context.contains("gap body"));
        assert!(context.contains("## Current Handoff"));
        assert!(context.contains("handoff body"));
        Ok(())
    }

    #[test]
    fn build_runtime_grounding_bundle_reads_memory_and_continuity_surfaces()
    -> anyhow::Result<()> {
        let root = tempdir()?;
        let repo_root = root.path().join("repo");
        fs::create_dir_all(
            repo_root
                .join("artifacts")
                .join("ultimentality-pilot")
                .join("memory"),
        )?;
        fs::create_dir_all(
            repo_root
                .join("artifacts")
                .join("ultimentality-pilot")
                .join("baseline"),
        )?;
        fs::create_dir_all(repo_root.join("offload").join("current"))?;
        fs::create_dir_all(repo_root.join("ultimentality-pilot").join("harness"))?;
        fs::create_dir_all(
            repo_root
                .join("artifacts")
                .join("ultimentality-pilot")
                .join("operator"),
        )?;

        fs::write(
            repo_root
                .join("artifacts")
                .join("ultimentality-pilot")
                .join("memory")
                .join("os.md"),
            "# OS\nwindows body\n",
        )?;
        fs::write(
            repo_root
                .join("artifacts")
                .join("ultimentality-pilot")
                .join("memory")
                .join("memory.md"),
            "# Memory\nworking set\n",
        )?;
        fs::write(
            repo_root
                .join("artifacts")
                .join("ultimentality-pilot")
                .join("baseline")
                .join("clean-splcw-harness-2026-04-03.md"),
            "# Baseline\nclean baseline\n",
        )?;
        fs::write(
            repo_root
                .join("artifacts")
                .join("ultimentality-pilot")
                .join("current-plan.md"),
            "# Current Plan\nactive work\n",
        )?;
        fs::write(
            repo_root.join("offload").join("current").join("plan.md"),
            "# Plan\nmirror plan\n",
        )?;
        fs::write(
            repo_root.join("offload").join("current").join("open-gaps.md"),
            "# Open Gaps\nlead gap\n",
        )?;
        fs::write(
            repo_root.join("offload").join("current").join("handoff.md"),
            "# Handoff\nnext step\n",
        )?;

        let paths = test_operator_paths(&repo_root);
        let bundle = build_runtime_grounding_bundle(&paths)?
            .context("expected runtime grounding bundle")?;

        assert!(bundle.contains("## Operating System Context"));
        assert!(bundle.contains("windows body"));
        assert!(bundle.contains("## Working Memory"));
        assert!(bundle.contains("working set"));
        assert!(bundle.contains("## Current Open Gaps"));
        assert!(bundle.contains("lead gap"));
        Ok(())
    }

    #[test]
    fn build_codex_cli_context_prompt_embeds_grounding_bundle() {
        let paths = test_operator_paths(Path::new(r"C:\repo"));
        let prompt = build_codex_cli_context_prompt(
            &paths,
            "Do the next grounded step",
            Some("## Working Memory\nsource: memory.md\n\nmemory body"),
        );

        assert!(prompt.contains("# Operating Memory Bundle"));
        assert!(prompt.contains("memory body"));
        assert!(prompt.contains("Current objective:\nDo the next grounded step"));
    }

    #[test]
    fn pending_oauth_launch_url_uses_browser_and_device_targets() {
        let browser = PendingOAuthAuthorization {
            id: Uuid::new_v4(),
            profile_id: Uuid::new_v4(),
            provider: ProviderKind::OpenAiCodex,
            label: "browser".into(),
            issuer: "https://auth.openai.com".into(),
            started_at: Utc::now(),
            expires_at: None,
            client_id: "client-id".into(),
            token_endpoint: "https://auth.openai.com/oauth/token".into(),
            scopes: vec!["openid".into()],
            state: Some("state".into()),
            pkce_verifier: Some("verifier".into()),
            kind: OAuthAuthorizationKind::BrowserCallback {
                authorization_url: "https://auth.openai.com/oauth/authorize?state=abc".into(),
                redirect_uri: "http://127.0.0.1/callback".into(),
                callback_host: "127.0.0.1".into(),
                callback_path: "/callback".into(),
                paste_prompt: "paste callback".into(),
            },
        };
        assert_eq!(
            pending_oauth_launch_url(&browser),
            Some((
                "https://auth.openai.com/oauth/authorize?state=abc",
                "authorization URL"
            ))
        );

        let device = PendingOAuthAuthorization {
            kind: OAuthAuthorizationKind::DeviceCode {
                verification_uri: "https://auth.openai.com/activate".into(),
                user_code: "ABCD".into(),
                device_code: "device".into(),
                poll_interval_seconds: 5,
            },
            ..browser
        };
        assert_eq!(
            pending_oauth_launch_url(&device),
            Some(("https://auth.openai.com/activate", "verification URL"))
        );
    }

    #[test]
    fn parse_operator_env_assignments_filters_comments_unknowns_and_empty_values() {
        let assignments = parse_operator_env_assignments(
            r#"
                # comment
                SPLCW_OPENAI_CODEX_OAUTH_CLIENT_ID=codex-client
                export SPLCW_OPENAI_API_OAUTH_CLIENT_ID="api-client"
                OPENCLAW_STATE_DIR="C:\state\openclaw"
                UNKNOWN_KEY=ignored
                OPENAI_OAUTH_CLIENT_ID=
            "#,
        );
        assert_eq!(
            assignments,
            vec![
                (
                    "SPLCW_OPENAI_CODEX_OAUTH_CLIENT_ID".into(),
                    "codex-client".into()
                ),
                (
                    "SPLCW_OPENAI_API_OAUTH_CLIENT_ID".into(),
                    "api-client".into()
                ),
                ("OPENCLAW_STATE_DIR".into(), r#"C:\state\openclaw"#.into())
            ]
        );
    }

    #[test]
    fn apply_operator_env_assignments_only_sets_missing_values() {
        let assignments = vec![
            (
                "SPLCW_OPENAI_CODEX_OAUTH_CLIENT_ID".to_string(),
                "codex-client".to_string(),
            ),
            (
                "SPLCW_OPENAI_API_OAUTH_CLIENT_ID".to_string(),
                "api-client".to_string(),
            ),
        ];
        let mut applied_values = Vec::new();
        let applied = apply_operator_env_assignments_with(
            &assignments,
            |name| {
                if name == "SPLCW_OPENAI_CODEX_OAUTH_CLIENT_ID" {
                    Some("already-set".into())
                } else {
                    None
                }
            },
            |key, value| applied_values.push((key.to_string(), value.to_string())),
        );

        assert_eq!(
            applied,
            vec!["SPLCW_OPENAI_API_OAUTH_CLIENT_ID".to_string()]
        );
        assert_eq!(
            applied_values,
            vec![(
                "SPLCW_OPENAI_API_OAUTH_CLIENT_ID".to_string(),
                "api-client".to_string()
            )]
        );
    }

    #[test]
    fn ensure_operator_env_template_creates_a_helpful_file() -> anyhow::Result<()> {
        let root = tempdir()?;
        let path = root.path().join("operator.env");
        ensure_operator_env_template(&path)?;
        let body = std::fs::read_to_string(&path)?;
        assert!(body.contains("CODEX_BIN"));
        assert!(body.contains("SPLCW_OPENAI_CODEX_OAUTH_CLIENT_ID"));
        assert!(body.contains("OPENCLAW_STATE_DIR"));
        assert!(body.contains("loaded automatically"));
        Ok(())
    }

    #[test]
    fn effective_operator_status_prefers_background_runner_summary() {
        let runner = OperatorBackgroundRunnerState {
            runner_id: "runner-live".into(),
            pid: 4242,
            owner_shell_id: None,
            owner_shell_pid: None,
            started_at: Utc::now(),
            updated_at: Utc::now(),
            phase: "sleeping".into(),
            loop_pause_seconds: 2.0,
            objective: "objective".into(),
            model: "gpt-5.4".into(),
            thread_id: "main".into(),
            thread_label: "Main".into(),
            engine_mode: OperatorEngineMode::NativeHarness,
            completed_turn_count: 3,
            last_summary: Some("background loop active".into()),
            last_error: None,
        };

        let (state, mode, summary) = effective_operator_status(
            "idle",
            OperatorRunMode::Idle,
            "foreground",
            Some(&runner),
            Some(BackgroundRunnerDisposition::Live),
        );
        assert_eq!(state, "background_looping");
        assert_eq!(mode, OperatorRunMode::Continuous);
        assert_eq!(summary, "background loop active");
    }

    #[test]
    fn effective_operator_status_uses_terminal_idle_runner_summary() {
        let runner = OperatorBackgroundRunnerState {
            runner_id: "runner-stop".into(),
            pid: 4242,
            owner_shell_id: None,
            owner_shell_pid: None,
            started_at: Utc::now(),
            updated_at: Utc::now(),
            phase: "stop_requested".into(),
            loop_pause_seconds: 2.0,
            objective: "objective".into(),
            model: "gpt-5.4".into(),
            thread_id: "main".into(),
            thread_label: "Main".into(),
            engine_mode: OperatorEngineMode::NativeHarness,
            completed_turn_count: 3,
            last_summary: Some("background loop stopped cleanly".into()),
            last_error: None,
        };

        let (state, mode, summary) = effective_operator_status(
            "background_looping",
            OperatorRunMode::Continuous,
            "foreground",
            Some(&runner),
            Some(BackgroundRunnerDisposition::TerminalIdle),
        );
        assert_eq!(state, "idle");
        assert_eq!(mode, OperatorRunMode::Idle);
        assert_eq!(summary, "background loop stopped cleanly");
    }

    #[test]
    fn effective_operator_status_uses_terminal_error_runner_summary() {
        let runner = OperatorBackgroundRunnerState {
            runner_id: "runner-failed".into(),
            pid: 4242,
            owner_shell_id: None,
            owner_shell_pid: None,
            started_at: Utc::now(),
            updated_at: Utc::now(),
            phase: "failed".into(),
            loop_pause_seconds: 2.0,
            objective: "objective".into(),
            model: "gpt-5.4".into(),
            thread_id: "main".into(),
            thread_label: "Main".into(),
            engine_mode: OperatorEngineMode::NativeHarness,
            completed_turn_count: 3,
            last_summary: Some("background loop failed after 3 turn(s)".into()),
            last_error: Some("synthetic detached runner failure".into()),
        };

        let (state, mode, summary) = effective_operator_status(
            "background_looping",
            OperatorRunMode::Continuous,
            "foreground",
            Some(&runner),
            Some(BackgroundRunnerDisposition::TerminalError),
        );
        assert_eq!(state, "error");
        assert_eq!(mode, OperatorRunMode::Idle);
        assert_eq!(summary, "background loop failed after 3 turn(s)");
    }

    #[test]
    fn effective_operator_status_reports_crashed_runner() {
        let runner = OperatorBackgroundRunnerState {
            runner_id: "runner-crashed".into(),
            pid: 4242,
            owner_shell_id: None,
            owner_shell_pid: None,
            started_at: Utc::now() - Duration::minutes(2),
            updated_at: Utc::now() - Duration::minutes(3),
            phase: "running_turn".into(),
            loop_pause_seconds: 2.0,
            objective: "objective".into(),
            model: "gpt-5.4".into(),
            thread_id: "main".into(),
            thread_label: "Main".into(),
            engine_mode: OperatorEngineMode::NativeHarness,
            completed_turn_count: 3,
            last_summary: Some("background loop active".into()),
            last_error: None,
        };

        let (state, mode, summary) = effective_operator_status(
            "background_looping",
            OperatorRunMode::Continuous,
            "foreground",
            Some(&runner),
            Some(BackgroundRunnerDisposition::Crashed),
        );
        assert_eq!(state, "idle");
        assert_eq!(mode, OperatorRunMode::Idle);
        assert!(summary.contains("runner-crashed"));
        assert!(summary.contains("became stale"));
    }

    #[test]
    fn effective_operator_status_prefers_crashed_runner_summary() {
        let runner = OperatorBackgroundRunnerState {
            runner_id: "runner-crashed".into(),
            pid: 4242,
            owner_shell_id: None,
            owner_shell_pid: None,
            started_at: Utc::now(),
            updated_at: Utc::now(),
            phase: "crashed".into(),
            loop_pause_seconds: 2.0,
            objective: "objective".into(),
            model: "gpt-5.4".into(),
            thread_id: "main".into(),
            thread_label: "Main".into(),
            engine_mode: OperatorEngineMode::NativeHarness,
            completed_turn_count: 1,
            last_summary: Some("background runner 4242 exited during running_turn".into()),
            last_error: Some("runner died".into()),
        };

        let (state, mode, summary) = effective_operator_status(
            "background_looping",
            OperatorRunMode::Continuous,
            "foreground",
            Some(&runner),
            Some(BackgroundRunnerDisposition::Crashed),
        );
        assert_eq!(state, "idle");
        assert_eq!(mode, OperatorRunMode::Idle);
        assert_eq!(summary, "background runner 4242 exited during running_turn");
    }

    #[test]
    fn effective_operator_status_clears_stale_background_state_when_runner_is_missing() {
        let (state, mode, summary) = effective_operator_status(
            "background_looping",
            OperatorRunMode::Continuous,
            "background loop stopped cleanly",
            None,
            None,
        );
        assert_eq!(state, "idle");
        assert_eq!(mode, OperatorRunMode::Idle);
        assert_eq!(summary, "background loop stopped cleanly");
    }

    #[test]
    fn recoverable_background_settings_accepts_crashed_runner_snapshot() {
        let snapshot = OperatorSnapshot {
            background_runner_status: Some("crashed".into()),
            background_runner_objective: Some("Recover the operator shell".into()),
            background_runner_model: Some("gpt-5.4".into()),
            background_runner_thread_id: Some("ops".into()),
            background_runner_thread_label: Some("Operations".into()),
            background_runner_engine_mode: Some("Codex CLI".into()),
            background_runner_loop_pause_seconds: Some(7.5),
            ..OperatorSnapshot::default()
        };

        let (settings, pause) = recoverable_background_settings(&snapshot)
            .expect("crashed runner should be recoverable");
        assert_eq!(settings.objective, "Recover the operator shell");
        assert_eq!(settings.model, "gpt-5.4");
        assert_eq!(settings.thread_id, "ops");
        assert_eq!(settings.thread_label, "Operations");
        assert_eq!(settings.engine_mode, OperatorEngineMode::CodexCli);
        assert!((pause - 7.5).abs() < 0.01);
    }

    #[test]
    fn recoverable_background_settings_rejects_terminal_idle_runner_snapshot() {
        let snapshot = OperatorSnapshot {
            background_runner_status: Some("terminal_idle".into()),
            background_runner_objective: Some("Finished cleanly".into()),
            background_runner_model: Some("gpt-5.4".into()),
            background_runner_thread_id: Some("ops".into()),
            background_runner_thread_label: Some("Operations".into()),
            background_runner_loop_pause_seconds: Some(2.0),
            ..OperatorSnapshot::default()
        };

        assert!(recoverable_background_settings(&snapshot).is_none());
        assert!(background_recovery_action_label(&snapshot).is_none());
    }

    #[test]
    fn background_recovery_action_label_matches_runner_status() {
        let crashed = OperatorSnapshot {
            background_runner_status: Some("crashed".into()),
            background_runner_objective: Some("Recover".into()),
            background_runner_model: Some("gpt-5.4".into()),
            background_runner_thread_id: Some("ops".into()),
            background_runner_thread_label: Some("Operations".into()),
            background_runner_loop_pause_seconds: Some(2.0),
            ..OperatorSnapshot::default()
        };
        let failed = OperatorSnapshot {
            background_runner_status: Some("terminal_error".into()),
            background_runner_objective: Some("Retry".into()),
            background_runner_model: Some("gpt-5.4".into()),
            background_runner_thread_id: Some("ops".into()),
            background_runner_thread_label: Some("Operations".into()),
            background_runner_loop_pause_seconds: Some(2.0),
            ..OperatorSnapshot::default()
        };

        assert_eq!(
            background_recovery_action_label(&crashed),
            Some("Recover Crashed Background Loop")
        );
        assert_eq!(
            background_recovery_action_label(&failed),
            Some("Retry Failed Background Loop")
        );
    }

    #[test]
    fn background_runner_owner_shell_alive_detects_current_process() {
        let runner = OperatorBackgroundRunnerState {
            runner_id: "runner-live".into(),
            pid: 4242,
            owner_shell_id: Some("shell-current".into()),
            owner_shell_pid: Some(std::process::id()),
            started_at: Utc::now(),
            updated_at: Utc::now(),
            phase: "sleeping".into(),
            loop_pause_seconds: 2.0,
            objective: "objective".into(),
            model: "gpt-5.4".into(),
            thread_id: "main".into(),
            thread_label: "Main".into(),
            engine_mode: OperatorEngineMode::NativeHarness,
            completed_turn_count: 1,
            last_summary: Some("background loop active".into()),
            last_error: None,
        };

        assert_eq!(background_runner_owner_shell_alive(&runner), Some(true));
    }

    #[test]
    fn background_reattach_recommendation_surfaces_when_launch_shell_is_missing() {
        let runner = OperatorBackgroundRunnerState {
            runner_id: "runner-live".into(),
            pid: 4242,
            owner_shell_id: Some("shell-missing".into()),
            owner_shell_pid: Some(999_999),
            started_at: Utc::now(),
            updated_at: Utc::now(),
            phase: "sleeping".into(),
            loop_pause_seconds: 2.0,
            objective: "objective".into(),
            model: "gpt-5.4".into(),
            thread_id: "main".into(),
            thread_label: "Main".into(),
            engine_mode: OperatorEngineMode::NativeHarness,
            completed_turn_count: 1,
            last_summary: Some("background loop active".into()),
            last_error: None,
        };

        let recommendation = background_reattach_recommendation(
            &runner,
            Some(BackgroundRunnerDisposition::Live),
            Some(false),
        )
        .expect("missing launch shell should require reattach");
        assert!(recommendation.contains("Reattach"));
        assert!(recommendation.contains("runner-live"));
    }

    #[test]
    fn operator_command_defaults_are_safe() {
        let command = OperatorCommand::default();
        assert!(!command.smoke_test);
        assert!(!command.background_loop);
        assert_eq!(command.settings.objective, DEFAULT_OBJECTIVE);
        assert_eq!(command.settings.model, DEFAULT_MODEL);
        assert_eq!(command.settings.engine_mode, OperatorEngineMode::CodexCli);
        assert_eq!(command.loop_pause_seconds, 0.0);
        assert!(command.background_runner_id.is_none());
    }

    #[test]
    fn background_settings_match_form_detects_handoff_alignment() -> anyhow::Result<()> {
        let root = tempdir()?;
        let controller = Arc::new(HarnessController::new(test_operator_paths(root.path()))?);
        let mut app = OperatorApp::new(controller);
        let snapshot = OperatorSnapshot {
            background_runner_id: Some("runner-live".into()),
            background_runner_objective: Some(DEFAULT_OBJECTIVE.into()),
            background_runner_model: Some(DEFAULT_MODEL.into()),
            background_runner_thread_id: Some(DEFAULT_THREAD_ID.into()),
            background_runner_thread_label: Some(DEFAULT_THREAD_LABEL.into()),
            background_runner_engine_mode: Some("Codex CLI".into()),
            background_runner_loop_pause_seconds: Some(DEFAULT_LOOP_PAUSE_SECONDS),
            ..OperatorSnapshot::default()
        };

        assert!(app.background_settings_match_form(&snapshot));
        app.settings.thread_label = "Other".into();
        assert!(!app.background_settings_match_form(&snapshot));
        Ok(())
    }

    #[test]
    fn adopt_background_settings_copies_runner_into_launch_form() -> anyhow::Result<()> {
        let root = tempdir()?;
        let controller = Arc::new(HarnessController::new(test_operator_paths(root.path()))?);
        let mut app = OperatorApp::new(controller);
        let snapshot = OperatorSnapshot {
            background_runner_id: Some("runner-42".into()),
            background_runner_objective: Some("Keep the dashboard current".into()),
            background_runner_model: Some("gpt-5.4-mini".into()),
            background_runner_thread_id: Some("ops".into()),
            background_runner_thread_label: Some("Operations".into()),
            background_runner_engine_mode: Some("Native Harness".into()),
            background_runner_loop_pause_seconds: Some(4.5),
            ..OperatorSnapshot::default()
        };

        app.adopt_background_settings(&snapshot);

        assert_eq!(app.settings.objective, "Keep the dashboard current");
        assert_eq!(app.settings.model, "gpt-5.4-mini");
        assert_eq!(app.settings.thread_id, "ops");
        assert_eq!(app.settings.thread_label, "Operations");
        assert_eq!(app.settings.engine_mode, OperatorEngineMode::NativeHarness);
        assert!((app.loop_pause_seconds - 4.5).abs() < 0.01);
        let updated_snapshot = app
            .snapshot
            .lock()
            .expect("operator snapshot poisoned")
            .clone();
        assert!(
            updated_snapshot
                .auth_notice
                .as_deref()
                .unwrap_or_default()
                .contains("runner-42")
        );
        Ok(())
    }

    #[test]
    fn handoff_settings_match_form_detects_alignment() -> anyhow::Result<()> {
        let root = tempdir()?;
        let controller = Arc::new(HarnessController::new(test_operator_paths(root.path()))?);
        let mut app = OperatorApp::new(controller);
        let snapshot = OperatorSnapshot {
            background_handoff_pending: true,
            background_handoff_objective: Some(DEFAULT_OBJECTIVE.into()),
            background_handoff_model: Some(DEFAULT_MODEL.into()),
            background_handoff_thread_id: Some(DEFAULT_THREAD_ID.into()),
            background_handoff_thread_label: Some(DEFAULT_THREAD_LABEL.into()),
            background_handoff_engine_mode: Some("Codex CLI".into()),
            background_handoff_loop_pause_seconds: Some(DEFAULT_LOOP_PAUSE_SECONDS),
            ..OperatorSnapshot::default()
        };

        assert!(app.handoff_settings_match_form(&snapshot));
        app.settings.thread_id = "ops".into();
        assert!(!app.handoff_settings_match_form(&snapshot));
        Ok(())
    }

    #[test]
    fn adopt_background_handoff_copies_request_into_launch_form() -> anyhow::Result<()> {
        let root = tempdir()?;
        let controller = Arc::new(HarnessController::new(test_operator_paths(root.path()))?);
        let mut app = OperatorApp::new(controller);
        let snapshot = OperatorSnapshot {
            background_handoff_pending: true,
            background_handoff_target_runner_id: Some("runner-old".into()),
            background_handoff_objective: Some("Replace the old worker".into()),
            background_handoff_model: Some("gpt-5.4-mini".into()),
            background_handoff_thread_id: Some("ops".into()),
            background_handoff_thread_label: Some("Operations".into()),
            background_handoff_engine_mode: Some("Native Harness".into()),
            background_handoff_loop_pause_seconds: Some(5.0),
            ..OperatorSnapshot::default()
        };

        app.adopt_background_handoff(&snapshot);

        assert_eq!(app.settings.objective, "Replace the old worker");
        assert_eq!(app.settings.model, "gpt-5.4-mini");
        assert_eq!(app.settings.thread_id, "ops");
        assert_eq!(app.settings.thread_label, "Operations");
        assert_eq!(app.settings.engine_mode, OperatorEngineMode::NativeHarness);
        assert!((app.loop_pause_seconds - 5.0).abs() < 0.01);
        let updated_snapshot = app
            .snapshot
            .lock()
            .expect("operator snapshot poisoned")
            .clone();
        assert!(
            updated_snapshot
                .auth_notice
                .as_deref()
                .unwrap_or_default()
                .contains("runner-old")
        );
        Ok(())
    }

    #[test]
    fn classify_background_handoff_distinguishes_waiting_ready_and_obsolete() {
        let request = OperatorBackgroundHandoffRequest {
            target_runner_id: Some("runner-a".into()),
            requested_at: Utc::now(),
            settings: RunSettings::default(),
            loop_pause_seconds: DEFAULT_LOOP_PAUSE_SECONDS,
        };
        let mut runner = OperatorBackgroundRunnerState {
            runner_id: "runner-a".into(),
            pid: 4242,
            owner_shell_id: None,
            owner_shell_pid: None,
            started_at: Utc::now(),
            updated_at: Utc::now(),
            phase: "sleeping".into(),
            loop_pause_seconds: 2.0,
            objective: "objective".into(),
            model: "gpt-5.4".into(),
            thread_id: "main".into(),
            thread_label: "Main".into(),
            engine_mode: OperatorEngineMode::NativeHarness,
            completed_turn_count: 3,
            last_summary: Some("background loop active".into()),
            last_error: None,
        };

        assert_eq!(
            classify_background_handoff(
                &request,
                Some(&runner),
                Some(BackgroundRunnerDisposition::Live)
            ),
            BackgroundHandoffDisposition::WaitingForTarget
        );

        assert_eq!(
            classify_background_handoff(
                &request,
                Some(&runner),
                Some(BackgroundRunnerDisposition::TerminalIdle)
            ),
            BackgroundHandoffDisposition::ReadyToLaunch
        );

        runner.runner_id = "runner-b".into();
        assert_eq!(
            classify_background_handoff(
                &request,
                Some(&runner),
                Some(BackgroundRunnerDisposition::Live)
            ),
            BackgroundHandoffDisposition::Obsolete
        );

        assert_eq!(
            classify_background_handoff(
                &request,
                Some(&runner),
                Some(BackgroundRunnerDisposition::Crashed)
            ),
            BackgroundHandoffDisposition::Obsolete
        );

        assert_eq!(
            classify_background_handoff(
                &request,
                Some(&runner),
                Some(BackgroundRunnerDisposition::TerminalError)
            ),
            BackgroundHandoffDisposition::Obsolete
        );
    }

    #[test]
    fn provisional_background_runner_state_marks_launching_phase() {
        let settings = RunSettings::default();
        let state =
            provisional_background_runner_state("runner-launch", 4242, None, None, &settings, 3.5);
        assert_eq!(state.runner_id, "runner-launch");
        assert_eq!(state.pid, 4242);
        assert_eq!(state.phase, "launching");
        assert_eq!(state.objective, settings.objective);
        assert!((state.loop_pause_seconds - 3.5).abs() < 0.01);
        assert_eq!(
            state.last_summary.as_deref(),
            Some("background loop launching")
        );
    }

    #[test]
    fn bootstrap_background_runner_state_preserves_provisional_start_time() {
        let settings = RunSettings::default();
        let started_at = Utc::now() - Duration::seconds(15);
        let provisional = OperatorBackgroundRunnerState {
            runner_id: "runner-live".into(),
            pid: 1111,
            owner_shell_id: Some("shell-a".into()),
            owner_shell_pid: Some(31337),
            started_at,
            updated_at: Utc::now() - Duration::seconds(10),
            phase: "launching".into(),
            loop_pause_seconds: 2.0,
            objective: settings.objective.clone(),
            model: settings.model.clone(),
            thread_id: settings.thread_id.clone(),
            thread_label: settings.thread_label.clone(),
            engine_mode: settings.engine_mode,
            completed_turn_count: 0,
            last_summary: Some("background loop launching".into()),
            last_error: None,
        };

        let bootstrapped = bootstrap_background_runner_state(
            Some(&provisional),
            "runner-live",
            2222,
            None,
            None,
            &settings,
            4.0,
        );
        assert_eq!(bootstrapped.started_at, started_at);
        assert_eq!(bootstrapped.pid, 2222);
        assert_eq!(bootstrapped.phase, "starting");
        assert_eq!(bootstrapped.owner_shell_id.as_deref(), Some("shell-a"));
        assert_eq!(bootstrapped.owner_shell_pid, Some(31337));
        assert_eq!(
            bootstrapped.last_summary.as_deref(),
            Some("background loop starting")
        );
        assert!((bootstrapped.loop_pause_seconds - 4.0).abs() < 0.01);
    }

    #[test]
    fn read_background_runner_state_clears_stale_runner_file() -> anyhow::Result<()> {
        let root = tempdir()?;
        let paths = test_operator_paths(root.path());
        let controller = HarnessController::new(paths.clone())?;
        let stale = OperatorBackgroundRunnerState {
            runner_id: "runner-stale".into(),
            pid: 4242,
            owner_shell_id: None,
            owner_shell_pid: None,
            started_at: Utc::now() - Duration::minutes(10),
            updated_at: Utc::now() - Duration::minutes(5),
            phase: "sleeping".into(),
            loop_pause_seconds: 2.0,
            objective: "objective".into(),
            model: "gpt-5.4".into(),
            thread_id: "main".into(),
            thread_label: "Main".into(),
            engine_mode: OperatorEngineMode::NativeHarness,
            completed_turn_count: 3,
            last_summary: Some("stale runner".into()),
            last_error: None,
        };
        controller.write_background_runner_state(&stale)?;

        let read = controller.read_background_runner_state()?;
        assert_eq!(read.disposition, Some(BackgroundRunnerDisposition::Crashed));
        assert_eq!(
            read.state.as_ref().map(|state| state.runner_id.as_str()),
            Some("runner-stale")
        );
        assert!(
            read.notice
                .as_deref()
                .unwrap_or_default()
                .contains("treating record as crashed")
        );
        assert!(paths.background_runner_path.exists());
        Ok(())
    }

    #[test]
    fn read_background_runner_state_keeps_fresh_runner_file() -> anyhow::Result<()> {
        let root = tempdir()?;
        let paths = test_operator_paths(root.path());
        let controller = HarnessController::new(paths.clone())?;
        let fresh = OperatorBackgroundRunnerState {
            runner_id: "runner-fresh".into(),
            pid: 8080,
            owner_shell_id: None,
            owner_shell_pid: None,
            started_at: Utc::now() - Duration::seconds(10),
            updated_at: Utc::now(),
            phase: "sleeping".into(),
            loop_pause_seconds: 2.0,
            objective: "objective".into(),
            model: "gpt-5.4".into(),
            thread_id: "main".into(),
            thread_label: "Main".into(),
            engine_mode: OperatorEngineMode::NativeHarness,
            completed_turn_count: 2,
            last_summary: Some("fresh runner".into()),
            last_error: None,
        };
        controller.write_background_runner_state(&fresh)?;

        let read = controller.read_background_runner_state()?;
        let runner = read.state.expect("fresh runner should remain active");
        assert!(read.notice.is_none());
        assert_eq!(read.disposition, Some(BackgroundRunnerDisposition::Live));
        assert_eq!(runner.runner_id, "runner-fresh");
        assert_eq!(runner.pid, 8080);
        assert_eq!(runner.phase, "sleeping");
        assert!(paths.background_runner_path.exists());
        Ok(())
    }

    #[test]
    fn read_snapshot_reconciles_crashed_runner_and_clears_stale_stop_request() -> anyhow::Result<()>
    {
        let root = tempdir()?;
        let paths = test_operator_paths(root.path());
        let controller = HarnessController::new(paths.clone())?;
        let stale = OperatorBackgroundRunnerState {
            runner_id: "runner-stale".into(),
            pid: 4242,
            owner_shell_id: None,
            owner_shell_pid: None,
            started_at: Utc::now() - Duration::minutes(10),
            updated_at: Utc::now() - Duration::minutes(5),
            phase: "running_turn".into(),
            loop_pause_seconds: 2.0,
            objective: "objective".into(),
            model: "gpt-5.4".into(),
            thread_id: "main".into(),
            thread_label: "Main".into(),
            engine_mode: OperatorEngineMode::NativeHarness,
            completed_turn_count: 3,
            last_summary: Some("background loop active".into()),
            last_error: None,
        };
        controller.write_background_runner_state(&stale)?;
        controller.request_background_stop(Some("runner-stale"))?;

        let snapshot = run_async(|| async {
            controller
                .read_snapshot("idle", OperatorRunMode::Idle, "foreground", None, 0, None)
                .await
        })?;

        assert_eq!(
            snapshot.background_runner_status.as_deref(),
            Some("crashed")
        );
        assert_eq!(snapshot.background_runner_phase.as_deref(), Some("crashed"));
        assert!(!paths.background_stop_path.exists());
        assert!(
            snapshot
                .background_runner_last_error
                .as_deref()
                .unwrap_or_default()
                .contains("stopped reporting")
                || snapshot
                    .background_runner_last_error
                    .as_deref()
                    .unwrap_or_default()
                    .contains("no longer running")
        );
        assert!(
            snapshot
                .background_recovery_recommendation
                .as_deref()
                .unwrap_or_default()
                .contains("relaunch or replace")
        );
        assert!(
            snapshot
                .auth_notice
                .as_deref()
                .unwrap_or_default()
                .contains("reconciled stale background runner")
                || snapshot
                    .auth_notice
                    .as_deref()
                    .unwrap_or_default()
                    .contains("process 4242 disappeared")
        );
        assert!(
            snapshot
                .auth_notice
                .as_deref()
                .unwrap_or_default()
                .contains("cleared stale background stop request")
        );
        let reconciled = controller
            .read_background_runner_state()?
            .state
            .expect("crashed runner record should remain visible");
        assert_eq!(reconciled.phase, "crashed");
        Ok(())
    }

    #[test]
    fn reconcile_crashed_background_runner_marks_missing_process_immediately() {
        let runner = OperatorBackgroundRunnerState {
            runner_id: "runner-gone".into(),
            pid: 5150,
            owner_shell_id: None,
            owner_shell_pid: None,
            started_at: Utc::now(),
            updated_at: Utc::now(),
            phase: "running_turn".into(),
            loop_pause_seconds: 2.0,
            objective: "objective".into(),
            model: "gpt-5.4".into(),
            thread_id: "main".into(),
            thread_label: "Main".into(),
            engine_mode: OperatorEngineMode::NativeHarness,
            completed_turn_count: 2,
            last_summary: Some("background loop active".into()),
            last_error: None,
        };

        let (summary, last_error, notice) = reconcile_crashed_background_runner(&runner, |_| false)
            .expect("missing process should reconcile");
        assert!(summary.contains("exited or disappeared"));
        assert!(last_error.contains("no longer running"));
        assert!(notice.contains("process 5150 disappeared"));
    }

    #[test]
    fn background_runner_process_liveness_detects_current_process() {
        assert!(background_runner_process_is_alive(std::process::id()));
    }

    #[test]
    fn reconcile_background_control_artifacts_marks_missing_process_without_stale_timeout()
    -> anyhow::Result<()> {
        let root = tempdir()?;
        let paths = test_operator_paths(root.path());
        let controller = HarnessController::new(paths.clone())?;
        let runner = OperatorBackgroundRunnerState {
            runner_id: "runner-missing".into(),
            pid: 5150,
            owner_shell_id: None,
            owner_shell_pid: None,
            started_at: Utc::now(),
            updated_at: Utc::now(),
            phase: "sleeping".into(),
            loop_pause_seconds: 2.0,
            objective: "objective".into(),
            model: "gpt-5.4".into(),
            thread_id: "main".into(),
            thread_label: "Main".into(),
            engine_mode: OperatorEngineMode::NativeHarness,
            completed_turn_count: 4,
            last_summary: Some("background loop active".into()),
            last_error: None,
        };
        controller.write_background_runner_state(&runner)?;

        let notice = controller.reconcile_background_control_artifacts_with(|_| false)?;
        assert!(
            notice
                .as_deref()
                .unwrap_or_default()
                .contains("process 5150 disappeared")
        );

        let reconciled = controller
            .read_background_runner_state()?
            .state
            .expect("crashed runner record should remain visible");
        assert_eq!(reconciled.phase, "crashed");
        assert_eq!(
            reconciled.last_summary.as_deref(),
            Some(
                "background runner runner-missing process 5150 exited or disappeared during sleeping"
            )
        );
        assert!(
            reconciled
                .last_error
                .as_deref()
                .unwrap_or_default()
                .contains("no longer running")
        );
        Ok(())
    }

    #[test]
    fn read_snapshot_clears_obsolete_background_handoff_request() -> anyhow::Result<()> {
        let root = tempdir()?;
        let paths = test_operator_paths(root.path());
        let controller = HarnessController::new(paths.clone())?;
        let live = OperatorBackgroundRunnerState {
            runner_id: "runner-live".into(),
            pid: std::process::id(),
            owner_shell_id: None,
            owner_shell_pid: None,
            started_at: Utc::now() - Duration::seconds(30),
            updated_at: Utc::now(),
            phase: "sleeping".into(),
            loop_pause_seconds: 2.0,
            objective: "objective".into(),
            model: "gpt-5.4".into(),
            thread_id: "main".into(),
            thread_label: "Main".into(),
            engine_mode: OperatorEngineMode::NativeHarness,
            completed_turn_count: 4,
            last_summary: Some("background loop active".into()),
            last_error: None,
        };
        controller.write_background_runner_state(&live)?;
        controller.request_background_handoff(
            Some("runner-old"),
            &RunSettings::default(),
            DEFAULT_LOOP_PAUSE_SECONDS,
        )?;

        let snapshot = run_async(|| async {
            controller
                .read_snapshot("idle", OperatorRunMode::Idle, "foreground", None, 0, None)
                .await
        })?;

        assert!(!snapshot.background_handoff_pending);
        assert!(!paths.background_handoff_path.exists());
        assert!(
            snapshot
                .auth_notice
                .as_deref()
                .unwrap_or_default()
                .contains("cleared obsolete background handoff request")
        );
        Ok(())
    }

    #[test]
    fn read_snapshot_surfaces_background_failure_error_and_recovery() -> anyhow::Result<()> {
        let root = tempdir()?;
        let paths = test_operator_paths(root.path());
        let controller = HarnessController::new(paths)?;
        let failed = OperatorBackgroundRunnerState {
            runner_id: "runner-failed".into(),
            pid: 9090,
            owner_shell_id: None,
            owner_shell_pid: None,
            started_at: Utc::now() - Duration::seconds(45),
            updated_at: Utc::now(),
            phase: "failed".into(),
            loop_pause_seconds: 2.0,
            objective: "objective".into(),
            model: "gpt-5.4".into(),
            thread_id: "main".into(),
            thread_label: "Main".into(),
            engine_mode: OperatorEngineMode::NativeHarness,
            completed_turn_count: 7,
            last_summary: Some("background loop failed after 7 turn(s)".into()),
            last_error: Some("synthetic worker failure".into()),
        };
        controller.write_background_runner_state(&failed)?;

        let snapshot = run_async(|| async {
            controller
                .read_snapshot("idle", OperatorRunMode::Idle, "foreground", None, 0, None)
                .await
        })?;

        assert_eq!(
            snapshot.background_runner_status.as_deref(),
            Some("terminal_error")
        );
        assert_eq!(
            snapshot.background_runner_last_error.as_deref(),
            Some("synthetic worker failure")
        );
        assert!(
            snapshot
                .background_recovery_recommendation
                .as_deref()
                .unwrap_or_default()
                .contains("Inspect the preserved detached-runner error")
        );
        Ok(())
    }

    #[test]
    fn read_snapshot_surfaces_reattach_guidance_for_live_runner_from_missing_shell()
    -> anyhow::Result<()> {
        let root = tempdir()?;
        let paths = test_operator_paths(root.path());
        let controller = HarnessController::new(paths)?;
        let live = OperatorBackgroundRunnerState {
            runner_id: "runner-live".into(),
            pid: std::process::id(),
            owner_shell_id: Some("shell-old".into()),
            owner_shell_pid: Some(999_999),
            started_at: Utc::now() - Duration::seconds(45),
            updated_at: Utc::now(),
            phase: "sleeping".into(),
            loop_pause_seconds: 2.0,
            objective: "objective".into(),
            model: "gpt-5.4".into(),
            thread_id: "main".into(),
            thread_label: "Main".into(),
            engine_mode: OperatorEngineMode::NativeHarness,
            completed_turn_count: 3,
            last_summary: Some("background loop active".into()),
            last_error: None,
        };
        controller.write_background_runner_state(&live)?;

        let snapshot = run_async(|| async {
            controller
                .read_snapshot("idle", OperatorRunMode::Idle, "foreground", None, 0, None)
                .await
        })?;

        assert_eq!(snapshot.background_runner_status.as_deref(), Some("live"));
        assert_eq!(snapshot.background_runner_owner_shell_alive, Some(false));
        assert!(snapshot.background_reattach_required);
        assert!(
            snapshot
                .background_reattach_recommendation
                .as_deref()
                .unwrap_or_default()
                .contains("Reattach")
        );
        Ok(())
    }

    #[test]
    fn attach_background_runner_claims_live_runner_for_new_shell() -> anyhow::Result<()> {
        let root = tempdir()?;
        let paths = test_operator_paths(root.path());
        let controller = HarnessController::new(paths.clone())?;
        let live = OperatorBackgroundRunnerState {
            runner_id: "runner-live".into(),
            pid: std::process::id(),
            owner_shell_id: Some("shell-old".into()),
            owner_shell_pid: Some(999_999),
            started_at: Utc::now() - Duration::seconds(30),
            updated_at: Utc::now(),
            phase: "sleeping".into(),
            loop_pause_seconds: 2.0,
            objective: "objective".into(),
            model: "gpt-5.4".into(),
            thread_id: "main".into(),
            thread_label: "Main".into(),
            engine_mode: OperatorEngineMode::NativeHarness,
            completed_turn_count: 4,
            last_summary: Some("background loop active".into()),
            last_error: None,
        };
        controller.write_background_runner_state(&live)?;

        let summary = controller.attach_background_runner("runner-live", "shell-new", 31337)?;
        assert!(summary.contains("reattached this shell"));

        let claimed = controller
            .read_background_runner_state()?
            .state
            .expect("live runner should remain visible");
        assert_eq!(claimed.owner_shell_id.as_deref(), Some("shell-new"));
        assert_eq!(claimed.owner_shell_pid, Some(31337));
        assert_eq!(claimed.updated_at, live.updated_at);
        Ok(())
    }

    #[test]
    fn sleep_while_background_running_refreshes_runner_timestamp() -> anyhow::Result<()> {
        let root = tempdir()?;
        let paths = test_operator_paths(root.path());
        let controller = HarnessController::new(paths.clone())?;
        let before = Utc::now() - Duration::seconds(2);
        let runner = OperatorBackgroundRunnerState {
            runner_id: "runner-sleep".into(),
            pid: 5150,
            owner_shell_id: None,
            owner_shell_pid: None,
            started_at: Utc::now() - Duration::seconds(30),
            updated_at: before,
            phase: "sleeping".into(),
            loop_pause_seconds: 1.0,
            objective: "objective".into(),
            model: "gpt-5.4".into(),
            thread_id: "main".into(),
            thread_label: "Main".into(),
            engine_mode: OperatorEngineMode::NativeHarness,
            completed_turn_count: 4,
            last_summary: Some("waiting".into()),
            last_error: None,
        };
        controller.write_background_runner_state(&runner)?;

        assert!(sleep_while_background_running(
            &controller,
            "runner-sleep",
            std::time::Duration::from_millis(300)
        )?);

        let updated = controller
            .read_background_runner_state()?
            .state
            .expect("runner should still exist");
        assert!(updated.updated_at > before);
        assert_eq!(updated.phase, "sleeping");
        Ok(())
    }

    #[test]
    fn targeted_background_stop_only_matches_the_same_runner() -> anyhow::Result<()> {
        let root = tempdir()?;
        let paths = test_operator_paths(root.path());
        let controller = HarnessController::new(paths)?;
        controller.request_background_stop(Some("runner-a"))?;

        assert!(controller.background_stop_requested_for(Some("runner-a"))?);
        assert!(!controller.background_stop_requested_for(Some("runner-b"))?);
        assert!(!controller.background_stop_requested_for(None)?);
        Ok(())
    }

    #[test]
    fn legacy_background_stop_request_still_applies_as_wildcard() -> anyhow::Result<()> {
        let root = tempdir()?;
        let paths = test_operator_paths(root.path());
        let controller = HarnessController::new(paths.clone())?;
        std::fs::write(&paths.background_stop_path, b"stop\n")
            .context("write legacy stop request")?;

        assert!(controller.background_stop_requested_for(Some("runner-a"))?);
        assert!(controller.background_stop_requested_for(None)?);
        Ok(())
    }

    #[test]
    fn classify_background_runner_distinguishes_terminal_and_crashed() {
        let mut runner = OperatorBackgroundRunnerState {
            runner_id: "runner-state".into(),
            pid: 4242,
            owner_shell_id: None,
            owner_shell_pid: None,
            started_at: Utc::now(),
            updated_at: Utc::now(),
            phase: "sleeping".into(),
            loop_pause_seconds: 2.0,
            objective: "objective".into(),
            model: "gpt-5.4".into(),
            thread_id: "main".into(),
            thread_label: "Main".into(),
            engine_mode: OperatorEngineMode::NativeHarness,
            completed_turn_count: 3,
            last_summary: Some("background loop active".into()),
            last_error: None,
        };
        assert_eq!(
            classify_background_runner(&runner),
            BackgroundRunnerDisposition::Live
        );

        runner.phase = "stop_requested".into();
        assert_eq!(
            classify_background_runner(&runner),
            BackgroundRunnerDisposition::TerminalIdle
        );

        runner.phase = "failed".into();
        assert_eq!(
            classify_background_runner(&runner),
            BackgroundRunnerDisposition::TerminalError
        );

        runner.phase = "running_turn".into();
        runner.updated_at = Utc::now() - Duration::minutes(5);
        assert_eq!(
            classify_background_runner(&runner),
            BackgroundRunnerDisposition::Crashed
        );

        runner.phase = "crashed".into();
        runner.updated_at = Utc::now();
        assert_eq!(
            classify_background_runner(&runner),
            BackgroundRunnerDisposition::Crashed
        );
    }

    #[test]
    fn background_runner_spawn_gate_only_blocks_live_records() {
        assert!(!background_runner_allows_spawn(Some(
            BackgroundRunnerDisposition::Live
        )));
        assert!(background_runner_allows_spawn(Some(
            BackgroundRunnerDisposition::TerminalIdle
        )));
        assert!(background_runner_allows_spawn(Some(
            BackgroundRunnerDisposition::TerminalError
        )));
        assert!(background_runner_allows_spawn(Some(
            BackgroundRunnerDisposition::Crashed
        )));
        assert!(background_runner_allows_spawn(None));
    }

    #[test]
    fn format_background_runner_age_reports_minutes_and_seconds() {
        let updated_at = Utc::now() - Duration::seconds(125);
        let age = format_background_runner_age(updated_at);
        assert!(age.starts_with("2m"));
    }
}

