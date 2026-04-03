use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration as StdDuration, Instant};
use std::{
    collections::{BTreeMap, HashSet},
    fmt::Write as _,
};

use anyhow::{Context, anyhow, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use splcw_computer_use::{ActionExecution, ObservationFrame, ProposedAction};
use splcw_core::Receipt;
use splcw_llm::{ChatMessage, ChatRequest, ChatResponse, ContentBlock};
use tokio::fs::{self, OpenOptions as TokioOpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use tokio::time::sleep;
use uuid::Uuid;

static ACTIVE_SESSION_LOCKS: OnceLock<Mutex<std::collections::HashMap<PathBuf, usize>>> =
    OnceLock::new();
static ACTIVE_SESSION_WRITERS: OnceLock<
    Mutex<std::collections::HashMap<PathBuf, std::sync::Arc<AsyncMutex<()>>>>,
> = OnceLock::new();
const DEFAULT_COMPACTION_TIMEOUT: StdDuration = StdDuration::from_secs(15);
const DEFAULT_COMPACTION_RETRY_ATTEMPTS: usize = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeSessionConfig {
    pub root_dir: PathBuf,
    pub session_id: String,
    pub max_lock_age_seconds: i64,
    pub lock_retry_interval_ms: u64,
    pub lock_retry_attempts: usize,
}

impl RuntimeSessionConfig {
    pub fn new(root_dir: impl Into<PathBuf>, session_id: impl Into<String>) -> Self {
        Self {
            root_dir: root_dir.into(),
            session_id: session_id.into(),
            max_lock_age_seconds: 300,
            lock_retry_interval_ms: 50,
            lock_retry_attempts: 40,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeSessionEventKind {
    SessionRepaired,
    TurnStarted,
    TurnRoundStarted,
    TurnActionSelected,
    TurnActionExecuted,
    TurnActionVerified,
    TurnRoundCompleted,
    TurnCompleted,
    TurnGap,
    MemoryFlushed,
    CompactionStarted,
    CompactionCompleted,
    PostCompactionRefreshQueued,
    PostCompactionRefreshConsumed,
    StartupSummaryInjected,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeSessionEvent {
    pub recorded_at: DateTime<Utc>,
    pub session_id: String,
    pub kind: RuntimeSessionEventKind,
    pub plan_id: Option<String>,
    pub provider_id: Option<String>,
    pub model: Option<String>,
    pub summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeSessionState {
    pub version: u32,
    pub session_id: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub current_transcript_path: String,
    pub compaction_count: u64,
    pub memory_flush_compaction_count: Option<u64>,
    pub memory_flush_at: Option<DateTime<Utc>>,
    pub last_context_hash: Option<String>,
    pub startup_summary_injected_at: Option<DateTime<Utc>>,
    pub last_compaction_at: Option<DateTime<Utc>>,
    pub pending_post_compaction_refresh: Option<String>,
    #[serde(default)]
    pub pending_compaction: Option<RuntimePendingCompaction>,
    #[serde(default = "default_foreground_thread_id")]
    pub foreground_thread_id: String,
    #[serde(default = "default_runtime_threads")]
    pub threads: BTreeMap<String, RuntimeThreadState>,
}

impl RuntimeSessionState {
    pub fn new(session_id: impl Into<String>, current_transcript_path: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            version: 1,
            session_id: session_id.into(),
            created_at: now,
            updated_at: now,
            current_transcript_path: current_transcript_path.into(),
            compaction_count: 0,
            memory_flush_compaction_count: None,
            memory_flush_at: None,
            last_context_hash: None,
            startup_summary_injected_at: None,
            last_compaction_at: None,
            pending_post_compaction_refresh: None,
            pending_compaction: None,
            foreground_thread_id: default_foreground_thread_id(),
            threads: default_runtime_threads(),
        }
    }

    pub fn has_flushed_current_compaction(&self) -> bool {
        self.memory_flush_compaction_count == Some(self.compaction_count)
    }

    fn normalize(&mut self) {
        if self.foreground_thread_id.trim().is_empty() {
            self.foreground_thread_id = default_foreground_thread_id();
        }
        if self.threads.is_empty() {
            self.threads = default_runtime_threads();
        }
        let foreground_id = normalize_thread_id(&self.foreground_thread_id);
        self.foreground_thread_id = foreground_id.clone();
        self.threads
            .entry(foreground_id.clone())
            .or_insert_with(|| {
                RuntimeThreadState::new(&foreground_id, derive_thread_label(&foreground_id))
            });
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeThreadState {
    pub thread_id: String,
    pub label: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub compact_summary: Option<String>,
    #[serde(default)]
    pub recent_turns: Vec<RuntimeThreadTurn>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeThreadTurn {
    pub recorded_at: DateTime<Utc>,
    pub summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimePendingCompaction {
    pub started_at: DateTime<Utc>,
    pub snapshot_path: String,
    pub next_compaction_count: u64,
    pub keep_recent_events: usize,
    pub summary: String,
    #[serde(default)]
    pub next_transcript_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RuntimeCompactionSnapshot {
    pub captured_at: DateTime<Utc>,
    pub state: RuntimeSessionState,
    pub transcript_body: String,
    pub turn_log_body: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_turn_body: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuntimePendingTurnPhase {
    AwaitingProvider,
    AwaitingHostExecution,
    HostEffectsUncertain,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RuntimePendingTurnCheckpoint {
    pub checkpoint_id: Uuid,
    pub plan_id: Uuid,
    pub thread_id: String,
    pub round_index: usize,
    pub max_tool_rounds: usize,
    pub phase: RuntimePendingTurnPhase,
    pub system_prompt: String,
    pub conversation_messages: Vec<ChatMessage>,
    pub observation: ObservationFrame,
    pub tool_outcome_history: Vec<RuntimeToolOutcomeRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_action: Option<ProposedAction>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_execution: Option<ActionExecution>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_verification: Option<ObservationFrame>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_receipt: Option<Receipt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_provider_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_request: Option<ChatRequest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_response: Option<ChatResponse>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_narrative: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_action: Option<ProposedAction>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_tool_call_id: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RuntimeToolOutcomeRecord {
    pub call_id: String,
    pub action: ProposedAction,
    pub execution: ActionExecution,
    pub verification: ObservationFrame,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification_ok: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification_proof_level: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification_summary: Option<String>,
    pub receipt_id: Option<Uuid>,
    pub receipt_changed: Option<String>,
    pub contradiction: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeGapRecord {
    pub gap_id: Uuid,
    pub title: String,
    pub permanent_fix_target: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RuntimeTurnRecord {
    pub recorded_at: DateTime<Utc>,
    pub turn_id: Uuid,
    pub thread_id: String,
    pub provider_id: String,
    pub model: String,
    pub request: ChatRequest,
    pub response: ChatResponse,
    pub narrative: String,
    pub tool_outcome: Option<RuntimeToolOutcomeRecord>,
    pub surfaced_gap: Option<RuntimeGapRecord>,
}

impl RuntimeThreadState {
    fn new(thread_id: impl Into<String>, label: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            thread_id: thread_id.into(),
            label: label.into(),
            created_at: now,
            updated_at: now,
            compact_summary: None,
            recent_turns: Vec::new(),
        }
    }
}

impl RuntimeTurnRecord {
    pub fn summarize(&self) -> String {
        if let Some(gap) = &self.surfaced_gap {
            return format!("gap={} :: {}", gap.title, gap.permanent_fix_target);
        }

        if let Some(outcome) = &self.tool_outcome {
            let mut summary = format!(
                "action={} | result={}",
                describe_action_compact(&outcome.action),
                outcome.execution.summary
            );
            if let Some(verification_summary) = outcome.verification_summary.as_deref() {
                summary.push_str(&format!(" | verify={verification_summary}"));
            }
            if let Some(changed) = outcome.receipt_changed.as_deref() {
                summary.push_str(&format!(" | changed={changed}"));
            }
            if let Some(contradiction) = outcome.contradiction.as_deref() {
                summary.push_str(&format!(" | contradiction={contradiction}"));
            }
            return summary;
        }

        if !self.narrative.trim().is_empty() {
            return self.narrative.clone();
        }

        "turn recorded without tool outcome".into()
    }
}

fn default_foreground_thread_id() -> String {
    "main".into()
}

fn default_runtime_threads() -> BTreeMap<String, RuntimeThreadState> {
    let mut threads = BTreeMap::new();
    threads.insert("main".into(), RuntimeThreadState::new("main", "Main"));
    threads
}

fn normalize_thread_id(thread_id: &str) -> String {
    let trimmed = thread_id.trim();
    if trimmed.is_empty() {
        return default_foreground_thread_id();
    }
    trimmed.to_ascii_lowercase().replace(' ', "-")
}

fn format_turn_round_summary(round: usize, summary: impl Into<String>) -> String {
    let summary = summary.into();
    if summary.trim().is_empty() {
        format!("round={round}")
    } else {
        format!("round={round} | {summary}")
    }
}

impl RuntimeSessionEvent {
    fn turn_progress(
        kind: RuntimeSessionEventKind,
        session_id: &str,
        plan_id: impl Into<String>,
        provider_id: Option<String>,
        model: Option<String>,
        round: usize,
        summary: impl Into<String>,
    ) -> Self {
        Self {
            recorded_at: Utc::now(),
            session_id: session_id.into(),
            kind,
            plan_id: Some(plan_id.into()),
            provider_id,
            model,
            summary: format_turn_round_summary(round, summary),
        }
    }

    pub fn session_repaired(session_id: &str, summary: impl Into<String>) -> Self {
        Self {
            recorded_at: Utc::now(),
            session_id: session_id.into(),
            kind: RuntimeSessionEventKind::SessionRepaired,
            plan_id: None,
            provider_id: None,
            model: None,
            summary: summary.into(),
        }
    }

    pub fn turn_started(
        session_id: &str,
        plan_id: impl Into<String>,
        summary: impl Into<String>,
    ) -> Self {
        Self {
            recorded_at: Utc::now(),
            session_id: session_id.into(),
            kind: RuntimeSessionEventKind::TurnStarted,
            plan_id: Some(plan_id.into()),
            provider_id: None,
            model: None,
            summary: summary.into(),
        }
    }

    pub fn turn_round_started(
        session_id: &str,
        plan_id: impl Into<String>,
        round: usize,
        summary: impl Into<String>,
    ) -> Self {
        Self::turn_progress(
            RuntimeSessionEventKind::TurnRoundStarted,
            session_id,
            plan_id,
            None,
            None,
            round,
            summary,
        )
    }

    pub fn turn_action_selected(
        session_id: &str,
        plan_id: impl Into<String>,
        provider_id: impl Into<String>,
        model: impl Into<String>,
        round: usize,
        summary: impl Into<String>,
    ) -> Self {
        Self::turn_progress(
            RuntimeSessionEventKind::TurnActionSelected,
            session_id,
            plan_id,
            Some(provider_id.into()),
            Some(model.into()),
            round,
            summary,
        )
    }

    pub fn turn_action_executed(
        session_id: &str,
        plan_id: impl Into<String>,
        provider_id: impl Into<String>,
        model: impl Into<String>,
        round: usize,
        summary: impl Into<String>,
    ) -> Self {
        Self::turn_progress(
            RuntimeSessionEventKind::TurnActionExecuted,
            session_id,
            plan_id,
            Some(provider_id.into()),
            Some(model.into()),
            round,
            summary,
        )
    }

    pub fn turn_action_verified(
        session_id: &str,
        plan_id: impl Into<String>,
        provider_id: impl Into<String>,
        model: impl Into<String>,
        round: usize,
        summary: impl Into<String>,
    ) -> Self {
        Self::turn_progress(
            RuntimeSessionEventKind::TurnActionVerified,
            session_id,
            plan_id,
            Some(provider_id.into()),
            Some(model.into()),
            round,
            summary,
        )
    }

    pub fn turn_round_completed(
        session_id: &str,
        plan_id: impl Into<String>,
        provider_id: impl Into<String>,
        model: impl Into<String>,
        round: usize,
        summary: impl Into<String>,
    ) -> Self {
        Self::turn_progress(
            RuntimeSessionEventKind::TurnRoundCompleted,
            session_id,
            plan_id,
            Some(provider_id.into()),
            Some(model.into()),
            round,
            summary,
        )
    }

    pub fn turn_completed(
        session_id: &str,
        plan_id: impl Into<String>,
        provider_id: impl Into<String>,
        model: impl Into<String>,
        summary: impl Into<String>,
    ) -> Self {
        Self {
            recorded_at: Utc::now(),
            session_id: session_id.into(),
            kind: RuntimeSessionEventKind::TurnCompleted,
            plan_id: Some(plan_id.into()),
            provider_id: Some(provider_id.into()),
            model: Some(model.into()),
            summary: summary.into(),
        }
    }

    pub fn turn_gap(
        session_id: &str,
        plan_id: impl Into<String>,
        provider_id: impl Into<String>,
        model: impl Into<String>,
        summary: impl Into<String>,
    ) -> Self {
        Self {
            recorded_at: Utc::now(),
            session_id: session_id.into(),
            kind: RuntimeSessionEventKind::TurnGap,
            plan_id: Some(plan_id.into()),
            provider_id: Some(provider_id.into()),
            model: Some(model.into()),
            summary: summary.into(),
        }
    }

    pub fn memory_flushed(
        session_id: &str,
        summary: impl Into<String>,
        context_hash: Option<&str>,
    ) -> Self {
        let mut summary = summary.into();
        if let Some(hash) = context_hash {
            summary.push_str(&format!(" | context_hash={hash}"));
        }
        Self {
            recorded_at: Utc::now(),
            session_id: session_id.into(),
            kind: RuntimeSessionEventKind::MemoryFlushed,
            plan_id: None,
            provider_id: None,
            model: None,
            summary,
        }
    }

    pub fn compaction_started(session_id: &str, summary: impl Into<String>) -> Self {
        Self {
            recorded_at: Utc::now(),
            session_id: session_id.into(),
            kind: RuntimeSessionEventKind::CompactionStarted,
            plan_id: None,
            provider_id: None,
            model: None,
            summary: summary.into(),
        }
    }

    pub fn compaction_completed(session_id: &str, summary: impl Into<String>) -> Self {
        Self {
            recorded_at: Utc::now(),
            session_id: session_id.into(),
            kind: RuntimeSessionEventKind::CompactionCompleted,
            plan_id: None,
            provider_id: None,
            model: None,
            summary: summary.into(),
        }
    }

    pub fn post_compaction_refresh_queued(session_id: &str, summary: impl Into<String>) -> Self {
        Self {
            recorded_at: Utc::now(),
            session_id: session_id.into(),
            kind: RuntimeSessionEventKind::PostCompactionRefreshQueued,
            plan_id: None,
            provider_id: None,
            model: None,
            summary: summary.into(),
        }
    }

    pub fn post_compaction_refresh_consumed(session_id: &str, summary: impl Into<String>) -> Self {
        Self {
            recorded_at: Utc::now(),
            session_id: session_id.into(),
            kind: RuntimeSessionEventKind::PostCompactionRefreshConsumed,
            plan_id: None,
            provider_id: None,
            model: None,
            summary: summary.into(),
        }
    }

    pub fn startup_summary_injected(session_id: &str, summary: impl Into<String>) -> Self {
        Self {
            recorded_at: Utc::now(),
            session_id: session_id.into(),
            kind: RuntimeSessionEventKind::StartupSummaryInjected,
            plan_id: None,
            provider_id: None,
            model: None,
            summary: summary.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeSessionJournal {
    config: RuntimeSessionConfig,
    transcript_path: PathBuf,
    turn_log_path: PathBuf,
    pending_turn_path: PathBuf,
    canonical_transcript_path: PathBuf,
    lock_path: PathBuf,
    state_path: PathBuf,
    state: RuntimeSessionState,
    repaired: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeCompactionResult {
    pub archive_path: PathBuf,
    pub preserved_event_count: usize,
    pub dropped_event_count: usize,
    pub pending_post_compaction_refresh: bool,
    pub compaction_count: u64,
    pub artifact_paths: Vec<String>,
}

impl RuntimeSessionJournal {
    pub async fn open(config: RuntimeSessionConfig) -> anyhow::Result<Self> {
        let session_root = config.root_dir.join(&config.session_id);
        fs::create_dir_all(&session_root)
            .await
            .with_context(|| format!("create session directory {}", session_root.display()))?;

        let state_path = session_root.join("state.json");
        let (mut state, repaired_state) =
            load_or_initialize_state(&state_path, &config.session_id).await?;
        let recovered_pending_compaction =
            recover_pending_compaction_if_needed(&session_root, &state_path, &mut state).await?;
        let transcript_path = session_root.join(&state.current_transcript_path);
        let had_transcript_file = transcript_path.exists();
        if !had_transcript_file {
            fs::write(&transcript_path, "")
                .await
                .with_context(|| format!("create transcript {}", transcript_path.display()))?;
        }

        let repaired = repair_transcript_if_needed(&transcript_path).await?;
        if had_transcript_file {
            prewarm_transcript(&transcript_path).await?;
        }

        let turn_log_path = session_root.join("turn-log.jsonl");
        let pending_turn_path = session_root.join("pending-turn.json");
        let had_turn_log_file = turn_log_path.exists();
        if !had_turn_log_file {
            fs::write(&turn_log_path, "")
                .await
                .with_context(|| format!("create turn log {}", turn_log_path.display()))?;
        }
        let repaired_turn_log = repair_turn_log_if_needed(&turn_log_path).await?;
        if had_turn_log_file {
            prewarm_transcript(&turn_log_path).await?;
        }
        let repaired_pending_turn =
            repair_pending_turn_if_needed(&pending_turn_path, &transcript_path).await?;

        let canonical_transcript_path = std::fs::canonicalize(&transcript_path)
            .with_context(|| format!("canonicalize transcript {}", transcript_path.display()))?;
        let lock_path = session_root.join("write.lock");
        state.updated_at = Utc::now();
        persist_state(&state_path, &state).await?;

        Ok(Self {
            config,
            transcript_path,
            turn_log_path,
            pending_turn_path,
            canonical_transcript_path,
            lock_path,
            state_path,
            state,
            repaired: repaired_state
                || recovered_pending_compaction
                || repaired
                || repaired_turn_log
                || repaired_pending_turn,
        })
    }

    pub async fn open_with_stale_lock_cleanup(
        config: RuntimeSessionConfig,
    ) -> anyhow::Result<Self> {
        clean_stale_lock_files(&config.root_dir, config.max_lock_age_seconds)?;
        Self::open(config).await
    }

    pub fn repaired(&self) -> bool {
        self.repaired
    }

    pub fn state(&self) -> &RuntimeSessionState {
        &self.state
    }

    pub fn transcript_path(&self) -> &Path {
        &self.transcript_path
    }

    pub fn turn_log_path(&self) -> &Path {
        &self.turn_log_path
    }

    pub fn has_flushed_current_compaction(&self) -> bool {
        self.state.has_flushed_current_compaction()
    }

    pub fn pending_post_compaction_refresh(&self) -> Option<&str> {
        self.state.pending_post_compaction_refresh.as_deref()
    }

    pub fn foreground_thread_id(&self) -> &str {
        &self.state.foreground_thread_id
    }

    pub async fn switch_foreground_thread(
        &mut self,
        thread_id: &str,
        label: Option<&str>,
    ) -> anyhow::Result<()> {
        let normalized = normalize_thread_id(thread_id);
        if self.state.foreground_thread_id != normalized {
            let prior_foreground = self.state.foreground_thread_id.clone();
            compact_thread_before_backgrounding(&mut self.state, &prior_foreground);
        }

        let thread = self
            .state
            .threads
            .entry(normalized.clone())
            .or_insert_with(|| RuntimeThreadState::new(&normalized, label.unwrap_or("Thread")));
        if let Some(label) = label {
            if !label.trim().is_empty() {
                thread.label = label.trim().to_string();
            }
        }
        thread.updated_at = Utc::now();
        self.state.foreground_thread_id = normalized;
        self.state.updated_at = Utc::now();
        persist_state(&self.state_path, &self.state).await?;
        Ok(())
    }

    pub async fn record_foreground_turn(
        &mut self,
        summary: impl Into<String>,
    ) -> anyhow::Result<()> {
        let summary = summary.into();
        if summary.trim().is_empty() {
            return Ok(());
        }
        let thread_id = self.state.foreground_thread_id.clone();
        let thread = self
            .state
            .threads
            .entry(thread_id.clone())
            .or_insert_with(|| RuntimeThreadState::new(&thread_id, "Main"));
        thread.recent_turns.push(RuntimeThreadTurn {
            recorded_at: Utc::now(),
            summary,
        });
        if thread.recent_turns.len() > 6 {
            compact_thread_recent_turns(thread, 3);
        }
        thread.updated_at = Utc::now();
        self.state.updated_at = Utc::now();
        persist_state(&self.state_path, &self.state).await?;
        Ok(())
    }

    pub fn build_thread_prompt_context(&self) -> Option<String> {
        let foreground_id = self.state.foreground_thread_id.as_str();
        let foreground = self.state.threads.get(foreground_id)?;

        let mut out = String::new();
        let _ = writeln!(
            out,
            "# Current Thread\nid: {}\nlabel: {}",
            foreground.thread_id, foreground.label
        );
        if let Some(summary) = foreground.compact_summary.as_deref() {
            let _ = writeln!(out, "summary: {summary}");
        }
        if foreground.recent_turns.is_empty() {
            let _ = writeln!(out, "recent turns:\n- none");
        } else {
            let _ = writeln!(out, "recent turns:");
            for turn in foreground.recent_turns.iter().rev().take(3).rev() {
                let _ = writeln!(out, "- {}", turn.summary);
            }
        }

        let background = self
            .state
            .threads
            .values()
            .filter(|thread| thread.thread_id != foreground_id)
            .filter_map(|thread| {
                let summary = thread
                    .compact_summary
                    .clone()
                    .or_else(|| thread.recent_turns.last().map(|turn| turn.summary.clone()))?;
                Some((thread.label.clone(), summary))
            })
            .take(3)
            .collect::<Vec<_>>();

        if !background.is_empty() {
            let _ = writeln!(out, "\n# Background Thread Context");
            for (label, summary) in background {
                let _ = writeln!(out, "- {} :: {}", label, summary);
            }
        }

        Some(out.trim().to_string())
    }

    pub async fn acquire_write_lock(&self) -> anyhow::Result<RuntimeSessionWriteGuard> {
        let session_key = self.session_lock_identity_key();
        let lock_registry =
            ACTIVE_SESSION_LOCKS.get_or_init(|| Mutex::new(std::collections::HashMap::new()));
        {
            let mut active = lock_registry
                .lock()
                .expect("session lock registry poisoned");
            if let Some(count) = active.get_mut(&session_key) {
                *count += 1;
                return Ok(RuntimeSessionWriteGuard {
                    path: self.lock_path.clone(),
                    session_key: session_key.clone(),
                    file: None,
                });
            }
        }

        let retry_delay = StdDuration::from_millis(self.config.lock_retry_interval_ms.max(1));
        for attempt in 0..=self.config.lock_retry_attempts {
            match try_create_lock(&self.lock_path) {
                Ok(mut file) => {
                    let payload = serde_json::json!({
                        "pid": std::process::id(),
                        "acquired_at": Utc::now().to_rfc3339(),
                        "session_id": self.config.session_id,
                        "canonical_transcript_path": self.canonical_transcript_path,
                    });
                    let bytes = serde_json::to_vec_pretty(&payload)?;
                    file.write_all(&bytes)
                        .with_context(|| format!("write lock file {}", self.lock_path.display()))?;
                    file.flush()
                        .with_context(|| format!("flush lock file {}", self.lock_path.display()))?;
                    let mut active = lock_registry
                        .lock()
                        .expect("session lock registry poisoned");
                    active.insert(session_key.clone(), 1);
                    return Ok(RuntimeSessionWriteGuard {
                        path: self.lock_path.clone(),
                        session_key,
                        file: Some(file),
                    });
                }
                Err(error) if is_lock_exists(&error) => {
                    if is_stale_lock(&self.lock_path, self.config.max_lock_age_seconds)? {
                        let _ = std::fs::remove_file(&self.lock_path);
                        continue;
                    }
                    if attempt == self.config.lock_retry_attempts {
                        return Err(anyhow!(
                            "session write lock busy for {}",
                            self.lock_path.display()
                        ));
                    }
                    sleep(retry_delay).await;
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("acquire session lock {}", self.lock_path.display())
                    });
                }
            }
        }

        Err(anyhow!(
            "failed to acquire session lock {}",
            self.lock_path.display()
        ))
    }

    pub async fn append_event(&self, event: &RuntimeSessionEvent) -> anyhow::Result<()> {
        let _write_gate = self.acquire_write_gate().await;
        let _guard = self.acquire_write_lock().await?;
        self.append_event_locked(event).await
    }

    async fn append_event_locked(&self, event: &RuntimeSessionEvent) -> anyhow::Result<()> {
        self.ensure_write_lock_held()?;
        let serialized = serde_json::to_string(event)?;
        append_jsonl_line(&self.transcript_path, &serialized).await?;
        Ok(())
    }

    pub async fn read_events(&self) -> anyhow::Result<Vec<RuntimeSessionEvent>> {
        let body = fs::read_to_string(&self.transcript_path)
            .await
            .with_context(|| format!("read transcript {}", self.transcript_path.display()))?;
        let mut events = Vec::new();
        for line in body.lines().filter(|line| !line.trim().is_empty()) {
            events.push(serde_json::from_str(line).with_context(|| {
                format!(
                    "deserialize session event from {}",
                    self.transcript_path.display()
                )
            })?);
        }
        Ok(events)
    }

    pub async fn append_turn_record(&self, record: &RuntimeTurnRecord) -> anyhow::Result<()> {
        let _write_gate = self.acquire_write_gate().await;
        let _guard = self.acquire_write_lock().await?;
        self.append_turn_record_locked(record).await
    }

    async fn append_turn_record_locked(&self, record: &RuntimeTurnRecord) -> anyhow::Result<()> {
        self.ensure_write_lock_held()?;
        let serialized = serde_json::to_string(record)?;
        append_jsonl_line(&self.turn_log_path, &serialized).await?;
        Ok(())
    }

    pub async fn read_turn_records(&self) -> anyhow::Result<Vec<RuntimeTurnRecord>> {
        let body = fs::read_to_string(&self.turn_log_path)
            .await
            .with_context(|| format!("read turn log {}", self.turn_log_path.display()))?;
        let mut records = Vec::new();
        for line in body.lines().filter(|line| !line.trim().is_empty()) {
            records.push(serde_json::from_str(line).with_context(|| {
                format!(
                    "deserialize turn record from {}",
                    self.turn_log_path.display()
                )
            })?);
        }
        Ok(records)
    }

    pub async fn read_pending_turn_checkpoint(
        &self,
    ) -> anyhow::Result<Option<RuntimePendingTurnCheckpoint>> {
        if !self.pending_turn_path.exists() {
            return Ok(None);
        }
        let body = fs::read_to_string(&self.pending_turn_path)
            .await
            .with_context(|| format!("read pending turn {}", self.pending_turn_path.display()))?;
        let checkpoint = serde_json::from_str(&body).with_context(|| {
            format!(
                "deserialize pending turn checkpoint from {}",
                self.pending_turn_path.display()
            )
        })?;
        Ok(Some(checkpoint))
    }

    pub async fn write_pending_turn_checkpoint(
        &self,
        checkpoint: &RuntimePendingTurnCheckpoint,
    ) -> anyhow::Result<()> {
        let _write_gate = self.acquire_write_gate().await;
        let _guard = self.acquire_write_lock().await?;
        self.write_pending_turn_checkpoint_locked(checkpoint).await
    }

    async fn write_pending_turn_checkpoint_locked(
        &self,
        checkpoint: &RuntimePendingTurnCheckpoint,
    ) -> anyhow::Result<()> {
        self.ensure_write_lock_held()?;
        let serialized = serde_json::to_string_pretty(checkpoint)?;
        write_atomic_text(&self.pending_turn_path, &serialized)
            .await
            .with_context(|| format!("write pending turn {}", self.pending_turn_path.display()))
    }

    pub async fn clear_pending_turn_checkpoint(&self) -> anyhow::Result<()> {
        let _write_gate = self.acquire_write_gate().await;
        let _guard = self.acquire_write_lock().await?;
        self.clear_pending_turn_checkpoint_locked().await
    }

    async fn clear_pending_turn_checkpoint_locked(&self) -> anyhow::Result<()> {
        self.ensure_write_lock_held()?;
        if self.pending_turn_path.exists() {
            fs::remove_file(&self.pending_turn_path)
                .await
                .with_context(|| {
                    format!("remove pending turn {}", self.pending_turn_path.display())
                })?;
        }
        Ok(())
    }

    pub async fn build_turn_history_prompt_context(
        &self,
        max_turns: usize,
    ) -> anyhow::Result<Option<String>> {
        if max_turns == 0 {
            return Ok(None);
        }

        let records = self.read_turn_records().await?;
        if records.is_empty() {
            return Ok(None);
        }

        let mut out = String::new();
        let _ = writeln!(out, "# Recent Runtime Turns");
        for record in records.iter().rev().take(max_turns).rev() {
            let _ = writeln!(
                out,
                "- thread={} | provider={} | model={} | {}",
                record.thread_id,
                record.provider_id,
                record.model,
                record.summarize()
            );
        }
        Ok(Some(out.trim_end().to_string()))
    }

    pub async fn record_memory_flush(
        &mut self,
        summary: impl Into<String>,
        context_hash: Option<String>,
    ) -> anyhow::Result<bool> {
        if self.state.has_flushed_current_compaction() {
            return Ok(false);
        }
        let summary = summary.into();
        let now = Utc::now();
        self.state.updated_at = now;
        self.state.memory_flush_at = Some(now);
        self.state.memory_flush_compaction_count = Some(self.state.compaction_count);
        self.state.last_context_hash = context_hash.clone();
        persist_state(&self.state_path, &self.state).await?;
        self.append_event(&RuntimeSessionEvent::memory_flushed(
            &self.config.session_id,
            summary,
            context_hash.as_deref(),
        ))
        .await?;
        Ok(true)
    }

    pub async fn mark_startup_summary_injected(
        &mut self,
        summary: impl Into<String>,
    ) -> anyhow::Result<bool> {
        if self.state.startup_summary_injected_at.is_some() {
            return Ok(false);
        }
        let summary = summary.into();
        let now = Utc::now();
        self.state.updated_at = now;
        self.state.startup_summary_injected_at = Some(now);
        persist_state(&self.state_path, &self.state).await?;
        self.append_event(&RuntimeSessionEvent::startup_summary_injected(
            &self.config.session_id,
            summary,
        ))
        .await?;
        Ok(true)
    }

    pub async fn begin_compaction(&mut self, summary: impl Into<String>) -> anyhow::Result<()> {
        self.append_event(&RuntimeSessionEvent::compaction_started(
            &self.config.session_id,
            summary,
        ))
        .await
    }

    pub async fn take_post_compaction_refresh(&mut self) -> anyhow::Result<Option<String>> {
        let Some(refresh) = self.state.pending_post_compaction_refresh.take() else {
            return Ok(None);
        };
        self.state.updated_at = Utc::now();
        persist_state(&self.state_path, &self.state).await?;
        self.append_event(&RuntimeSessionEvent::post_compaction_refresh_consumed(
            &self.config.session_id,
            "consumed queued post-compaction refresh",
        ))
        .await?;
        Ok(Some(refresh))
    }

    pub async fn compact_transcript(
        &mut self,
        keep_recent_events: usize,
        summary: impl Into<String>,
        post_compaction_refresh: Option<String>,
    ) -> anyhow::Result<RuntimeCompactionResult> {
        self.compact_transcript_with_policy(
            keep_recent_events,
            summary.into(),
            post_compaction_refresh,
            DEFAULT_COMPACTION_TIMEOUT,
            DEFAULT_COMPACTION_RETRY_ATTEMPTS,
        )
        .await
    }

    async fn compact_transcript_with_policy(
        &mut self,
        keep_recent_events: usize,
        summary: String,
        post_compaction_refresh: Option<String>,
        timeout_grace: StdDuration,
        retry_attempts: usize,
    ) -> anyhow::Result<RuntimeCompactionResult> {
        let total_attempts = retry_attempts.saturating_add(1).max(1);
        let mut last_error = None;

        for attempt in 1..=total_attempts {
            let deadline = Instant::now() + timeout_grace;
            match self
                .compact_transcript_once(
                    keep_recent_events,
                    summary.clone(),
                    post_compaction_refresh.clone(),
                    deadline,
                )
                .await
            {
                Ok(result) => return Ok(result),
                Err(error) => {
                    let recovery_result = self.restore_pending_compaction_from_snapshot().await;
                    last_error = Some(match recovery_result {
                        Ok(true) => error.context(format!(
                            "compaction attempt {attempt}/{total_attempts} failed after snapshot-backed recovery"
                        )),
                        Ok(false) => error.context(format!(
                            "compaction attempt {attempt}/{total_attempts} failed without snapshot-backed recovery"
                        )),
                        Err(recovery_error) => error.context(format!(
                            "compaction attempt {attempt}/{total_attempts} failed and snapshot-backed recovery also failed: {recovery_error:#}"
                        )),
                    });
                    if attempt < total_attempts {
                        sleep(StdDuration::from_millis(50 * attempt as u64)).await;
                    }
                }
            }
        }

        Err(last_error
            .unwrap_or_else(|| anyhow!("compaction attempts exhausted without error detail")))
    }

    async fn compact_transcript_once(
        &mut self,
        keep_recent_events: usize,
        summary: String,
        post_compaction_refresh: Option<String>,
        deadline: Instant,
    ) -> anyhow::Result<RuntimeCompactionResult> {
        let _write_gate = self.acquire_write_gate().await;
        let _guard = self.acquire_write_lock().await?;
        self.ensure_write_lock_held()?;
        let session_root = self
            .transcript_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        let prior_transcript_path = self.transcript_path.clone();
        let preexisting_events = self.read_events().await?;
        self.append_event_locked(&RuntimeSessionEvent::compaction_started(
            &self.config.session_id,
            format!("{summary} | keep_recent_events={keep_recent_events}"),
        ))
        .await?;

        let total_before = preexisting_events.len();
        let preserve_from = total_before.saturating_sub(keep_recent_events);
        let preserved_events = preexisting_events
            .iter()
            .skip(preserve_from)
            .cloned()
            .collect::<Vec<_>>();
        let preexisting_turns = self.read_turn_records().await?;
        let total_turns_before = preexisting_turns.len();
        let preserve_turns_from = total_turns_before.saturating_sub(keep_recent_events);
        let preserved_turns = preexisting_turns
            .iter()
            .skip(preserve_turns_from)
            .cloned()
            .collect::<Vec<_>>();

        let next_count = self.state.compaction_count + 1;
        let next_transcript_name = next_live_transcript_name(next_count);
        let next_transcript_path = session_root.join(&next_transcript_name);
        let snapshot_path = self.create_compaction_snapshot(next_count).await?;
        self.mark_pending_compaction(
            next_count,
            keep_recent_events,
            summary.clone(),
            &snapshot_path,
            &next_transcript_name,
        )
        .await?;
        ensure_compaction_within_deadline(deadline, "archive transcript")?;
        let archive_name = format!("transcript-compaction-{next_count}.jsonl");
        let archive_path = session_root.join(&archive_name);

        if prior_transcript_path.exists() {
            move_transcript_to_archive(&prior_transcript_path, &archive_path)
                .await
                .with_context(|| {
                    format!(
                        "archive transcript {} -> {}",
                        prior_transcript_path.display(),
                        archive_path.display()
                    )
                })?;
        }

        ensure_compaction_within_deadline(deadline, "archive turn log")?;
        let turn_archive_name = format!("turn-log-compaction-{next_count}.jsonl");
        let turn_archive_path = self
            .turn_log_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(&turn_archive_name);
        if self.turn_log_path.exists() {
            fs::copy(&self.turn_log_path, &turn_archive_path)
                .await
                .with_context(|| {
                    format!(
                        "archive turn log {} -> {}",
                        self.turn_log_path.display(),
                        turn_archive_path.display()
                    )
                })?;
        }

        ensure_compaction_within_deadline(deadline, "rewrite transcript")?;
        let rewritten = if preserved_events.is_empty() {
            String::new()
        } else {
            let lines = preserved_events
                .iter()
                .map(serde_json::to_string)
                .collect::<Result<Vec<_>, _>>()?;
            format!("{}\n", lines.join("\n"))
        };
        write_atomic_text(&next_transcript_path, &rewritten)
            .await
            .with_context(|| {
                format!(
                    "rewrite compacted transcript {}",
                    next_transcript_path.display()
                )
            })?;
        self.transcript_path = next_transcript_path;
        self.canonical_transcript_path = std::fs::canonicalize(&self.transcript_path)
            .with_context(|| {
                format!("canonicalize transcript {}", self.transcript_path.display())
            })?;

        ensure_compaction_within_deadline(deadline, "rewrite turn log")?;
        let rewritten_turns = if preserved_turns.is_empty() {
            String::new()
        } else {
            let lines = preserved_turns
                .iter()
                .map(serde_json::to_string)
                .collect::<Result<Vec<_>, _>>()?;
            format!("{}\n", lines.join("\n"))
        };
        write_atomic_text(&self.turn_log_path, &rewritten_turns)
            .await
            .with_context(|| {
                format!(
                    "rewrite compacted turn log {}",
                    self.turn_log_path.display()
                )
            })?;
        ensure_compaction_within_deadline(deadline, "repair compacted turn log policy")?;
        let _ = repair_turn_log_policy_if_needed(&self.turn_log_path).await?;

        let queued_refresh = post_compaction_refresh
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());

        ensure_compaction_within_deadline(deadline, "persist compacted state")?;
        let now = Utc::now();
        self.state.updated_at = now;
        self.state.compaction_count = next_count;
        self.state.last_compaction_at = Some(now);
        self.state.startup_summary_injected_at = None;
        self.state.pending_post_compaction_refresh = queued_refresh.clone();
        self.state.pending_compaction = None;
        self.state.current_transcript_path = next_transcript_name;
        persist_state(&self.state_path, &self.state).await?;

        self.append_event_locked(&RuntimeSessionEvent::compaction_completed(
            &self.config.session_id,
            format!(
                "{summary} | archived={archive_name} | preserved={} | dropped={}",
                preserved_events.len(),
                total_before.saturating_sub(preserved_events.len())
            ),
        ))
        .await?;
        if let Some(refresh) = queued_refresh.as_ref() {
            self.append_event_locked(&RuntimeSessionEvent::post_compaction_refresh_queued(
                &self.config.session_id,
                refresh.clone(),
            ))
            .await?;
        }
        let _ = fs::remove_file(&snapshot_path).await;

        let mut artifact_paths = vec![
            archive_name,
            turn_archive_name,
            self.state.current_transcript_path.clone(),
            "turn-log.jsonl".into(),
        ];
        if self.pending_turn_path.exists() {
            artifact_paths.push("pending-turn.json".into());
        }

        Ok(RuntimeCompactionResult {
            archive_path,
            preserved_event_count: preserved_events.len(),
            dropped_event_count: total_before.saturating_sub(preserved_events.len()),
            pending_post_compaction_refresh: queued_refresh.is_some(),
            compaction_count: next_count,
            artifact_paths,
        })
    }

    pub async fn complete_compaction(
        &mut self,
        summary: impl Into<String>,
    ) -> anyhow::Result<PathBuf> {
        Ok(self
            .compact_transcript(0, summary.into(), None)
            .await?
            .archive_path)
    }

    fn ensure_write_lock_held(&self) -> anyhow::Result<()> {
        let session_key = self.session_lock_identity_key();
        let lock_registry =
            ACTIVE_SESSION_LOCKS.get_or_init(|| Mutex::new(std::collections::HashMap::new()));
        let active = lock_registry
            .lock()
            .expect("session lock registry poisoned");
        match active.get(&session_key) {
            Some(count) if *count > 0 => Ok(()),
            _ => Err(anyhow!(
                "write lock is required for session {}",
                self.config.session_id
            )),
        }
    }

    async fn acquire_write_gate(&self) -> OwnedMutexGuard<()> {
        let session_key = self.session_lock_identity_key();
        let registry =
            ACTIVE_SESSION_WRITERS.get_or_init(|| Mutex::new(std::collections::HashMap::new()));
        let gate = {
            let mut guard = registry.lock().expect("session writer registry poisoned");
            guard
                .entry(session_key)
                .or_insert_with(|| std::sync::Arc::new(AsyncMutex::new(())))
                .clone()
        };
        gate.lock_owned().await
    }

    fn session_lock_identity_key(&self) -> PathBuf {
        self.lock_path.clone()
    }

    async fn create_compaction_snapshot(&self, next_count: u64) -> anyhow::Result<PathBuf> {
        let snapshot_name = format!("compaction-snapshot-{next_count}.json");
        let snapshot_path = self
            .transcript_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(&snapshot_name);
        let transcript_body = if self.transcript_path.exists() {
            fs::read_to_string(&self.transcript_path)
                .await
                .with_context(|| format!("read transcript {}", self.transcript_path.display()))?
        } else {
            String::new()
        };
        let turn_log_body = if self.turn_log_path.exists() {
            fs::read_to_string(&self.turn_log_path)
                .await
                .with_context(|| format!("read turn log {}", self.turn_log_path.display()))?
        } else {
            String::new()
        };
        let pending_turn_body = if self.pending_turn_path.exists() {
            Some(
                fs::read_to_string(&self.pending_turn_path)
                    .await
                    .with_context(|| {
                        format!("read pending turn {}", self.pending_turn_path.display())
                    })?,
            )
        } else {
            None
        };
        let mut snapshot_state = self.state.clone();
        snapshot_state.pending_compaction = None;
        let snapshot = RuntimeCompactionSnapshot {
            captured_at: Utc::now(),
            state: snapshot_state,
            transcript_body,
            turn_log_body,
            pending_turn_body,
        };
        let serialized = serde_json::to_string_pretty(&snapshot)?;
        write_atomic_text(&snapshot_path, &serialized)
            .await
            .with_context(|| format!("write compaction snapshot {}", snapshot_path.display()))?;
        Ok(snapshot_path)
    }

    async fn mark_pending_compaction(
        &mut self,
        next_compaction_count: u64,
        keep_recent_events: usize,
        summary: String,
        snapshot_path: &Path,
        next_transcript_path: &str,
    ) -> anyhow::Result<()> {
        let snapshot_name = snapshot_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                anyhow!(
                    "invalid compaction snapshot path {}",
                    snapshot_path.display()
                )
            })?
            .to_string();
        self.state.pending_compaction = Some(RuntimePendingCompaction {
            started_at: Utc::now(),
            snapshot_path: snapshot_name,
            next_compaction_count,
            keep_recent_events,
            summary,
            next_transcript_path: next_transcript_path.to_string(),
        });
        self.state.updated_at = Utc::now();
        persist_state(&self.state_path, &self.state).await
    }

    async fn restore_pending_compaction_from_snapshot(&mut self) -> anyhow::Result<bool> {
        let session_root = self
            .transcript_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        let recovered =
            recover_pending_compaction_if_needed(&session_root, &self.state_path, &mut self.state)
                .await?;
        if recovered {
            self.transcript_path = session_root.join(&self.state.current_transcript_path);
            self.canonical_transcript_path = std::fs::canonicalize(&self.transcript_path)
                .with_context(|| {
                    format!("canonicalize transcript {}", self.transcript_path.display())
                })?;
        }
        Ok(recovered)
    }
}

fn ensure_compaction_within_deadline(deadline: Instant, phase: &str) -> anyhow::Result<()> {
    if Instant::now() > deadline {
        bail!("compaction timed out before phase: {phase}");
    }
    Ok(())
}

fn next_live_transcript_name(next_count: u64) -> String {
    format!("transcript-{next_count}.jsonl")
}

async fn move_transcript_to_archive(from: &Path, to: &Path) -> anyhow::Result<()> {
    match fs::rename(from, to).await {
        Ok(()) => Ok(()),
        Err(error) => {
            fs::copy(from, to).await.with_context(|| {
                format!("copy transcript {} -> {}", from.display(), to.display())
            })?;
            fs::remove_file(from)
                .await
                .with_context(|| format!("remove rotated transcript {}", from.display()))?;
            let _ = error;
            Ok(())
        }
    }
}

fn compact_thread_before_backgrounding(state: &mut RuntimeSessionState, thread_id: &str) {
    let Some(thread) = state.threads.get_mut(thread_id) else {
        return;
    };
    compact_thread_recent_turns(thread, 3);
    thread.updated_at = Utc::now();
}

fn compact_thread_recent_turns(thread: &mut RuntimeThreadState, keep_recent: usize) {
    if thread.recent_turns.len() <= keep_recent {
        return;
    }
    let split_at = thread.recent_turns.len().saturating_sub(keep_recent);
    let older = thread.recent_turns.drain(..split_at).collect::<Vec<_>>();
    if older.is_empty() {
        return;
    }

    let merged = older
        .into_iter()
        .map(|turn| turn.summary)
        .collect::<Vec<_>>()
        .join(" | ");
    thread.compact_summary = match thread.compact_summary.take() {
        Some(existing) if !existing.trim().is_empty() => Some(format!("{existing} | {merged}")),
        _ => Some(merged),
    };
}

fn describe_action_compact(action: &ProposedAction) -> String {
    match action {
        ProposedAction::FocusWindow { title } => format!("focus_window:{title}"),
        ProposedAction::Click { x, y, .. } => format!("click:{x},{y}"),
        ProposedAction::DoubleClick { x, y, .. } => format!("double_click:{x},{y}"),
        ProposedAction::Drag {
            from_x,
            from_y,
            to_x,
            to_y,
        } => format!("drag:{from_x},{from_y}->{to_x},{to_y}"),
        ProposedAction::TypeText { text, submit } => {
            format!("type_text:{} chars submit={submit}", text.len())
        }
        ProposedAction::Hotkey { chord } => format!("hotkey:{chord}"),
        ProposedAction::Scroll { delta } => format!("scroll:{delta}"),
        ProposedAction::LaunchProcess { command, args } => {
            format!("launch_process:{command} argc={}", args.len())
        }
        ProposedAction::WaitFor { signal, timeout_ms } => {
            format!("wait_for:{signal} timeout_ms={timeout_ms}")
        }
        ProposedAction::CaptureObservation => "capture_observation".into(),
    }
}

pub struct RuntimeSessionWriteGuard {
    path: PathBuf,
    session_key: PathBuf,
    file: Option<std::fs::File>,
}

impl Drop for RuntimeSessionWriteGuard {
    fn drop(&mut self) {
        let _ = self.file.take();
        let lock_registry =
            ACTIVE_SESSION_LOCKS.get_or_init(|| Mutex::new(std::collections::HashMap::new()));
        let mut active = lock_registry
            .lock()
            .expect("session lock registry poisoned");
        if let Some(count) = active.get_mut(&self.session_key) {
            if *count > 1 {
                *count -= 1;
                return;
            }
            active.remove(&self.session_key);
        }
        let _ = std::fs::remove_file(&self.path);
    }
}

async fn repair_transcript_if_needed(path: &Path) -> anyhow::Result<bool> {
    repair_jsonl_if_needed::<RuntimeSessionEvent>(path, "session event").await
}

async fn repair_turn_log_if_needed(path: &Path) -> anyhow::Result<bool> {
    let repaired_physical =
        repair_jsonl_if_needed::<RuntimeTurnRecord>(path, "turn record").await?;
    let repaired_policy = repair_turn_log_policy_if_needed(path).await?;
    Ok(repaired_physical || repaired_policy)
}

async fn repair_jsonl_if_needed<T>(path: &Path, noun: &str) -> anyhow::Result<bool>
where
    T: for<'de> Deserialize<'de>,
{
    let body = fs::read_to_string(path)
        .await
        .with_context(|| format!("read transcript {}", path.display()))?;
    if body.trim().is_empty() {
        return Ok(false);
    }

    let mut valid = Vec::new();
    let mut repaired = false;
    for line in body.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<T>(trimmed) {
            Ok(_) => valid.push(trimmed.to_string()),
            Err(_) => {
                repaired = true;
                break;
            }
        }
    }

    if !repaired {
        return Ok(false);
    }

    let backup = path.with_extension(format!(
        "jsonl.repair-{}.bak",
        Utc::now().format("%Y%m%d%H%M%S")
    ));
    fs::copy(path, &backup)
        .await
        .with_context(|| format!("backup transcript {}", backup.display()))?;

    let rewritten = if valid.is_empty() {
        String::new()
    } else {
        format!("{}\n", valid.join("\n"))
    };
    fs::write(path, rewritten)
        .await
        .with_context(|| format!("repair {noun} log {}", path.display()))?;
    Ok(true)
}

async fn repair_turn_log_policy_if_needed(path: &Path) -> anyhow::Result<bool> {
    let records = read_jsonl_entries::<RuntimeTurnRecord>(path).await?;
    if records.is_empty() {
        return Ok(false);
    }

    let mut repaired = false;
    let mut seen_turn_ids = HashSet::new();
    let mut rewritten_records = Vec::new();
    for mut record in records {
        let normalized_thread_id = normalize_thread_id(&record.thread_id);
        if record.thread_id != normalized_thread_id {
            record.thread_id = normalized_thread_id;
            repaired = true;
        }
        if !seen_turn_ids.insert(record.turn_id) {
            repaired = true;
            continue;
        }
        if repair_runtime_turn_record_provider_policy(&mut record) {
            repaired = true;
        }
        rewritten_records.push(record);
    }

    if !repaired {
        return Ok(false);
    }

    let backup = path.with_extension(format!(
        "jsonl.policy-repair-{}.bak",
        Utc::now().format("%Y%m%d%H%M%S")
    ));
    fs::copy(path, &backup)
        .await
        .with_context(|| format!("backup turn log {}", backup.display()))?;

    let rewritten = if rewritten_records.is_empty() {
        String::new()
    } else {
        let lines = rewritten_records
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()?;
        format!("{}\n", lines.join("\n"))
    };
    write_atomic_text(path, &rewritten)
        .await
        .with_context(|| format!("rewrite policy-repaired turn log {}", path.display()))?;
    Ok(true)
}

fn repair_runtime_turn_record_provider_policy(record: &mut RuntimeTurnRecord) -> bool {
    repair_request_tool_pairing(&mut record.request)
}

fn repair_request_tool_pairing(request: &mut ChatRequest) -> bool {
    if request.messages.is_empty() {
        return false;
    }

    let mut repaired = false;
    let mut pending_tool_calls = HashSet::new();
    let mut resolved_tool_calls = HashSet::new();
    let original_messages = std::mem::take(&mut request.messages);
    let mut rewritten_messages = Vec::with_capacity(original_messages.len());

    for message in original_messages {
        let original_len = message.content.len();
        let mut rewritten_content = Vec::with_capacity(original_len);

        for block in message.content {
            match block {
                ContentBlock::ToolCall {
                    id,
                    name,
                    arguments,
                } => {
                    if pending_tool_calls.contains(&id) || resolved_tool_calls.contains(&id) {
                        repaired = true;
                        continue;
                    }
                    pending_tool_calls.insert(id.clone());
                    rewritten_content.push(ContentBlock::ToolCall {
                        id,
                        name,
                        arguments,
                    });
                }
                ContentBlock::ToolResult { id, content } => {
                    if pending_tool_calls.remove(&id) {
                        resolved_tool_calls.insert(id.clone());
                        rewritten_content.push(ContentBlock::ToolResult { id, content });
                    } else {
                        repaired = true;
                    }
                }
                other => rewritten_content.push(other),
            }
        }

        if rewritten_content.len() != original_len {
            repaired = true;
        }
        if rewritten_content.is_empty() {
            if original_len > 0 {
                repaired = true;
            }
            continue;
        }

        rewritten_messages.push(splcw_llm::ChatMessage {
            role: message.role,
            content: rewritten_content,
        });
    }

    if pending_tool_calls.is_empty() {
        request.messages = rewritten_messages;
        return repaired;
    }

    let unresolved_tool_calls = pending_tool_calls;
    let mut final_messages = Vec::with_capacity(rewritten_messages.len());
    for message in rewritten_messages {
        let original_len = message.content.len();
        let filtered_content = message
            .content
            .into_iter()
            .filter(|block| {
                !matches!(
                    block,
                    ContentBlock::ToolCall { id, .. } if unresolved_tool_calls.contains(id)
                )
            })
            .collect::<Vec<_>>();
        if filtered_content.len() != original_len {
            repaired = true;
        }
        if filtered_content.is_empty() {
            if original_len > 0 {
                repaired = true;
            }
            continue;
        }
        final_messages.push(splcw_llm::ChatMessage {
            role: message.role,
            content: filtered_content,
        });
    }

    request.messages = final_messages;
    repaired
}

async fn load_or_initialize_state(
    path: &Path,
    session_id: &str,
) -> anyhow::Result<(RuntimeSessionState, bool)> {
    let mut repaired = false;
    if path.exists() {
        let body = fs::read_to_string(path)
            .await
            .with_context(|| format!("read session state {}", path.display()))?;
        if !body.trim().is_empty() {
            match serde_json::from_str::<RuntimeSessionState>(&body) {
                Ok(mut state) => {
                    state.normalize();
                    return Ok((state, false));
                }
                Err(error) => {
                    let backup = path.with_extension(format!(
                        "json.repair-{}.bak",
                        Utc::now().format("%Y%m%d%H%M%S")
                    ));
                    fs::copy(path, &backup)
                        .await
                        .with_context(|| format!("backup session state {}", backup.display()))?;
                    repaired = true;
                    let _ = error;
                }
            }
        }
    }

    let session_root = path.parent().unwrap_or_else(|| Path::new("."));
    let current_transcript_name = infer_current_transcript_path(session_root)?;
    let default_transcript_path = session_root.join(&current_transcript_name);
    let turn_log_path = session_root.join("turn-log.jsonl");
    let repaired_transcript = if default_transcript_path.exists() {
        repair_transcript_if_needed(&default_transcript_path).await?
    } else {
        false
    };
    let repaired_turn_log = if turn_log_path.exists() {
        repair_turn_log_if_needed(&turn_log_path).await?
    } else {
        false
    };
    repaired = repaired || repaired_transcript || repaired_turn_log;

    if let Some(mut reconstructed) =
        reconstruct_state_from_existing_logs(session_root, session_id).await?
    {
        reconstructed.normalize();
        persist_state(path, &reconstructed).await?;
        return Ok((reconstructed, true));
    }

    let state = RuntimeSessionState::new(session_id, current_transcript_name);
    persist_state(path, &state).await?;
    Ok((state, repaired))
}

async fn recover_pending_compaction_if_needed(
    session_root: &Path,
    state_path: &Path,
    state: &mut RuntimeSessionState,
) -> anyhow::Result<bool> {
    let Some(pending) = state.pending_compaction.clone() else {
        return Ok(false);
    };
    let snapshot_path = session_root.join(&pending.snapshot_path);
    if !snapshot_path.exists() {
        return Err(anyhow!(
            "pending compaction snapshot is missing: {}",
            snapshot_path.display()
        ));
    }

    let body = fs::read_to_string(&snapshot_path)
        .await
        .with_context(|| format!("read compaction snapshot {}", snapshot_path.display()))?;
    let mut snapshot: RuntimeCompactionSnapshot =
        serde_json::from_str(&body).with_context(|| {
            format!(
                "deserialize compaction snapshot {}",
                snapshot_path.display()
            )
        })?;
    snapshot.state.pending_compaction = None;
    snapshot.state.updated_at = Utc::now();

    let transcript_path = session_root.join(&snapshot.state.current_transcript_path);
    if !pending.next_transcript_path.trim().is_empty() {
        let rotated_path = session_root.join(&pending.next_transcript_path);
        if rotated_path != transcript_path && rotated_path.exists() {
            let _ = fs::remove_file(&rotated_path).await;
        }
    }
    write_atomic_text(&transcript_path, &snapshot.transcript_body)
        .await
        .with_context(|| format!("restore transcript {}", transcript_path.display()))?;

    let turn_log_path = session_root.join("turn-log.jsonl");
    write_atomic_text(&turn_log_path, &snapshot.turn_log_body)
        .await
        .with_context(|| format!("restore turn log {}", turn_log_path.display()))?;

    let pending_turn_path = session_root.join("pending-turn.json");
    match snapshot.pending_turn_body.as_deref() {
        Some(body) => {
            write_atomic_text(&pending_turn_path, body)
                .await
                .with_context(|| format!("restore pending turn {}", pending_turn_path.display()))?;
        }
        None if pending_turn_path.exists() => {
            fs::remove_file(&pending_turn_path).await.with_context(|| {
                format!("remove stale pending turn {}", pending_turn_path.display())
            })?;
        }
        None => {}
    }

    persist_state(state_path, &snapshot.state).await?;
    *state = snapshot.state;
    let _ = fs::remove_file(&snapshot_path).await;
    Ok(true)
}

async fn repair_pending_turn_if_needed(
    pending_turn_path: &Path,
    transcript_path: &Path,
) -> anyhow::Result<bool> {
    if !pending_turn_path.exists() {
        return Ok(false);
    }

    let body = fs::read_to_string(pending_turn_path)
        .await
        .with_context(|| format!("read pending turn {}", pending_turn_path.display()))?;
    let checkpoint: RuntimePendingTurnCheckpoint = match serde_json::from_str(&body) {
        Ok(checkpoint) => checkpoint,
        Err(_) => {
            backup_and_remove_pending_turn(pending_turn_path).await?;
            return Ok(true);
        }
    };

    let latest_terminal_event = read_jsonl_entries::<RuntimeSessionEvent>(transcript_path)
        .await?
        .into_iter()
        .filter(|event| {
            matches!(
                event.kind,
                RuntimeSessionEventKind::TurnCompleted | RuntimeSessionEventKind::TurnGap
            )
        })
        .map(|event| event.recorded_at)
        .max();
    if latest_terminal_event
        .map(|recorded_at| recorded_at >= checkpoint.updated_at)
        .unwrap_or(false)
    {
        fs::remove_file(pending_turn_path).await.with_context(|| {
            format!("remove stale pending turn {}", pending_turn_path.display())
        })?;
        return Ok(true);
    }

    Ok(false)
}

async fn backup_and_remove_pending_turn(path: &Path) -> anyhow::Result<()> {
    let backup = path.with_extension(format!(
        "json.repair-{}.bak",
        Utc::now().format("%Y%m%d%H%M%S")
    ));
    fs::copy(path, &backup)
        .await
        .with_context(|| format!("backup corrupt pending turn {}", path.display()))?;
    fs::remove_file(path)
        .await
        .with_context(|| format!("remove corrupt pending turn {}", path.display()))?;
    Ok(())
}

async fn reconstruct_state_from_existing_logs(
    session_root: &Path,
    session_id: &str,
) -> anyhow::Result<Option<RuntimeSessionState>> {
    let current_transcript_name = infer_current_transcript_path(session_root)?;
    let transcript_path = session_root.join(&current_transcript_name);
    let turn_log_path = session_root.join("turn-log.jsonl");
    let archive_count = infer_compaction_archive_count(session_root)?;
    let transcript_exists = transcript_path.exists();
    let turn_log_exists = turn_log_path.exists();
    if !transcript_exists && !turn_log_exists && archive_count == 0 {
        return Ok(None);
    }

    let events = read_jsonl_entries::<RuntimeSessionEvent>(&transcript_path).await?;
    let turn_records = read_jsonl_entries::<RuntimeTurnRecord>(&turn_log_path).await?;
    if events.is_empty() && turn_records.is_empty() && archive_count == 0 {
        return Ok(None);
    }

    let mut state = RuntimeSessionState::new(session_id, current_transcript_name);
    state.created_at = infer_created_at(&events, &turn_records).unwrap_or_else(Utc::now);
    state.updated_at = infer_updated_at(&events, &turn_records).unwrap_or(state.created_at);
    state.compaction_count = infer_compaction_count(&events, archive_count);
    state.last_compaction_at = events
        .iter()
        .rev()
        .find(|event| event.kind == RuntimeSessionEventKind::CompactionCompleted)
        .map(|event| event.recorded_at);
    state.startup_summary_injected_at =
        infer_startup_summary_injected_at(&events, state.last_compaction_at);
    state.pending_post_compaction_refresh = infer_pending_post_compaction_refresh(&events);
    state.memory_flush_at = infer_memory_flush_at(&events);
    state.memory_flush_compaction_count = state.memory_flush_at.map(|flush_at| {
        infer_memory_flush_compaction_count(
            flush_at,
            state.last_compaction_at,
            state.compaction_count,
        )
    });

    rebuild_threads_from_turn_records(&mut state, &turn_records);
    state.normalize();

    Ok(Some(state))
}

async fn read_jsonl_entries<T>(path: &Path) -> anyhow::Result<Vec<T>>
where
    T: for<'de> Deserialize<'de>,
{
    if !path.exists() {
        return Ok(Vec::new());
    }
    let body = fs::read_to_string(path)
        .await
        .with_context(|| format!("read jsonl log {}", path.display()))?;
    let mut entries = Vec::new();
    for line in body.lines().filter(|line| !line.trim().is_empty()) {
        entries.push(
            serde_json::from_str(line)
                .with_context(|| format!("deserialize jsonl entry from {}", path.display()))?,
        );
    }
    Ok(entries)
}

fn infer_created_at(
    events: &[RuntimeSessionEvent],
    turn_records: &[RuntimeTurnRecord],
) -> Option<DateTime<Utc>> {
    events
        .iter()
        .map(|event| event.recorded_at)
        .chain(turn_records.iter().map(|record| record.recorded_at))
        .min()
}

fn infer_updated_at(
    events: &[RuntimeSessionEvent],
    turn_records: &[RuntimeTurnRecord],
) -> Option<DateTime<Utc>> {
    events
        .iter()
        .map(|event| event.recorded_at)
        .chain(turn_records.iter().map(|record| record.recorded_at))
        .max()
}

fn infer_compaction_archive_count(session_root: &Path) -> anyhow::Result<u64> {
    let mut max_count = 0;
    if !session_root.exists() {
        return Ok(0);
    }
    for entry in std::fs::read_dir(session_root)
        .with_context(|| format!("scan session root {}", session_root.display()))?
    {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        for prefix in ["transcript-compaction-", "turn-log-compaction-"] {
            if let Some(suffix) = name
                .strip_prefix(prefix)
                .and_then(|rest| rest.strip_suffix(".jsonl"))
                .and_then(|raw| raw.parse::<u64>().ok())
            {
                max_count = max_count.max(suffix);
            }
        }
    }
    Ok(max_count)
}

fn infer_current_transcript_path(session_root: &Path) -> anyhow::Result<String> {
    let mut best: Option<(u64, String)> = None;
    for entry in std::fs::read_dir(session_root)
        .with_context(|| format!("read session directory {}", session_root.display()))?
    {
        let entry = entry
            .with_context(|| format!("iterate session directory {}", session_root.display()))?;
        let file_type = entry
            .file_type()
            .with_context(|| format!("read file type {}", entry.path().display()))?;
        if !file_type.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(generation) = parse_live_transcript_generation(name) else {
            continue;
        };
        let replace = match best.as_ref() {
            Some((current_generation, _)) => generation >= *current_generation,
            None => true,
        };
        if replace {
            best = Some((generation, name.to_string()));
        }
    }
    Ok(best
        .map(|(_, name)| name)
        .unwrap_or_else(|| "transcript.jsonl".to_string()))
}

fn parse_live_transcript_generation(name: &str) -> Option<u64> {
    if name == "transcript.jsonl" {
        return Some(0);
    }
    let suffix = name.strip_prefix("transcript-")?.strip_suffix(".jsonl")?;
    if suffix.starts_with("compaction-") {
        return None;
    }
    suffix.parse::<u64>().ok()
}

fn infer_compaction_count(events: &[RuntimeSessionEvent], archive_count: u64) -> u64 {
    let event_count = events
        .iter()
        .filter(|event| event.kind == RuntimeSessionEventKind::CompactionCompleted)
        .count() as u64;
    archive_count.max(event_count)
}

fn infer_startup_summary_injected_at(
    events: &[RuntimeSessionEvent],
    last_compaction_at: Option<DateTime<Utc>>,
) -> Option<DateTime<Utc>> {
    let startup_at = events
        .iter()
        .rev()
        .find(|event| event.kind == RuntimeSessionEventKind::StartupSummaryInjected)
        .map(|event| event.recorded_at)?;
    if let Some(compacted_at) = last_compaction_at {
        if startup_at <= compacted_at {
            return None;
        }
    }
    Some(startup_at)
}

fn infer_pending_post_compaction_refresh(events: &[RuntimeSessionEvent]) -> Option<String> {
    let mut pending = None;
    for event in events {
        match event.kind {
            RuntimeSessionEventKind::PostCompactionRefreshQueued => {
                pending = Some(event.summary.clone());
            }
            RuntimeSessionEventKind::PostCompactionRefreshConsumed => {
                pending = None;
            }
            _ => {}
        }
    }
    pending
}

fn infer_memory_flush_at(events: &[RuntimeSessionEvent]) -> Option<DateTime<Utc>> {
    events
        .iter()
        .rev()
        .find(|event| event.kind == RuntimeSessionEventKind::MemoryFlushed)
        .map(|event| event.recorded_at)
}

fn infer_memory_flush_compaction_count(
    flush_at: DateTime<Utc>,
    last_compaction_at: Option<DateTime<Utc>>,
    compaction_count: u64,
) -> u64 {
    match last_compaction_at {
        Some(compacted_at) if flush_at < compacted_at => compaction_count.saturating_sub(1),
        _ => compaction_count,
    }
}

fn rebuild_threads_from_turn_records(
    state: &mut RuntimeSessionState,
    turn_records: &[RuntimeTurnRecord],
) {
    if turn_records.is_empty() {
        return;
    }
    state.threads.clear();
    let mut latest_thread: Option<(DateTime<Utc>, String)> = None;

    for record in turn_records {
        let thread_id = normalize_thread_id(&record.thread_id);
        let thread = state.threads.entry(thread_id.clone()).or_insert_with(|| {
            RuntimeThreadState::new(&thread_id, derive_thread_label(&thread_id))
        });
        thread.recent_turns.push(RuntimeThreadTurn {
            recorded_at: record.recorded_at,
            summary: record.summarize(),
        });
        thread.updated_at = record.recorded_at;
        latest_thread = Some(match latest_thread {
            Some((current_at, current_thread)) if current_at >= record.recorded_at => {
                (current_at, current_thread)
            }
            _ => (record.recorded_at, thread_id),
        });
    }

    for thread in state.threads.values_mut() {
        if thread.recent_turns.len() > 6 {
            compact_thread_recent_turns(thread, 3);
        }
    }

    if let Some((_, thread_id)) = latest_thread {
        state.foreground_thread_id = thread_id;
    }
}

fn derive_thread_label(thread_id: &str) -> String {
    if thread_id == "main" {
        return "Main".into();
    }
    thread_id
        .split(['-', '_'])
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => format!("{}{}", first.to_ascii_uppercase(), chars.as_str()),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

async fn persist_state(path: &Path, state: &RuntimeSessionState) -> anyhow::Result<()> {
    let serialized = serde_json::to_string_pretty(state)?;
    write_atomic_text(path, &serialized)
        .await
        .with_context(|| format!("write session state {}", path.display()))?;
    Ok(())
}

async fn append_jsonl_line(path: &Path, serialized: &str) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create parent directory {}", parent.display()))?;
    }
    let mut file = TokioOpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .with_context(|| format!("open jsonl log {}", path.display()))?;
    file.write_all(serialized.as_bytes())
        .await
        .with_context(|| format!("append jsonl line {}", path.display()))?;
    file.write_all(b"\n")
        .await
        .with_context(|| format!("append jsonl newline {}", path.display()))?;
    file.flush()
        .await
        .with_context(|| format!("flush jsonl log {}", path.display()))?;
    file.sync_all()
        .await
        .with_context(|| format!("sync jsonl log {}", path.display()))?;
    Ok(())
}

async fn write_atomic_text(path: &Path, body: &str) -> anyhow::Result<()> {
    let temp_path = path.with_extension(format!("{}.tmp", Uuid::new_v4()));
    if let Some(parent) = temp_path.parent() {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create atomic write directory {}", parent.display()))?;
    }
    let mut file = TokioOpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&temp_path)
        .await
        .with_context(|| format!("open temp file {}", temp_path.display()))?;
    file.write_all(body.as_bytes())
        .await
        .with_context(|| format!("write temp file {}", temp_path.display()))?;
    file.flush()
        .await
        .with_context(|| format!("flush temp file {}", temp_path.display()))?;
    file.sync_all()
        .await
        .with_context(|| format!("sync temp file {}", temp_path.display()))?;
    drop(file);
    replace_file_atomically(&temp_path, path)?;
    Ok(())
}

fn replace_file_atomically(temp_path: &Path, target_path: &Path) -> anyhow::Result<()> {
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;

        const MOVEFILE_REPLACE_EXISTING: u32 = 0x1;
        const MOVEFILE_WRITE_THROUGH: u32 = 0x8;

        unsafe extern "system" {
            fn MoveFileExW(existing: *const u16, new: *const u16, flags: u32) -> i32;
        }

        let existing = temp_path
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<u16>>();
        let new = target_path
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<u16>>();
        let result = unsafe {
            MoveFileExW(
                existing.as_ptr(),
                new.as_ptr(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
            )
        };
        if result == 0 {
            let error = std::io::Error::last_os_error();
            let _ = std::fs::remove_file(temp_path);
            return Err(anyhow!(error)).with_context(|| {
                format!(
                    "replace {} with {}",
                    target_path.display(),
                    temp_path.display()
                )
            });
        }
        Ok(())
    }
    #[cfg(not(windows))]
    {
        std::fs::rename(temp_path, target_path).with_context(|| {
            format!(
                "replace {} with {}",
                target_path.display(),
                temp_path.display()
            )
        })?;
        Ok(())
    }
}

async fn prewarm_transcript(path: &Path) -> anyhow::Result<()> {
    let body = fs::read(path)
        .await
        .with_context(|| format!("prewarm transcript {}", path.display()))?;
    let _ = body.into_iter().take(4096).count();
    Ok(())
}

fn try_create_lock(path: &Path) -> anyhow::Result<std::fs::File> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| format!("open lock file {}", path.display()))
}

fn is_lock_exists(error: &anyhow::Error) -> bool {
    error
        .root_cause()
        .downcast_ref::<std::io::Error>()
        .map(|io| io.kind() == std::io::ErrorKind::AlreadyExists)
        .unwrap_or(false)
}

fn is_stale_lock(path: &Path, max_age_seconds: i64) -> anyhow::Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    if max_age_seconds <= 0 {
        return Ok(true);
    }
    let metadata =
        std::fs::metadata(path).with_context(|| format!("stat lock file {}", path.display()))?;
    let modified = metadata
        .modified()
        .with_context(|| format!("read lock mtime {}", path.display()))?;
    let age = modified.elapsed().unwrap_or_default();
    Ok(age.as_secs() > max_age_seconds as u64)
}

fn clean_stale_lock_files(root: &Path, max_age_seconds: i64) -> anyhow::Result<()> {
    if !root.exists() {
        return Ok(());
    }
    for entry in std::fs::read_dir(root).with_context(|| format!("scan {}", root.display()))? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            clean_stale_lock_files(&path, max_age_seconds)?;
            continue;
        }
        if path.file_name().and_then(|name| name.to_str()) == Some("write.lock")
            && is_stale_lock(&path, max_age_seconds)?
        {
            let _ = std::fs::remove_file(&path);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use splcw_llm::{ChatMessage, ContentBlock};

    fn temp_session_root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "splcw-runtime-session-{}-{}",
            label,
            uuid::Uuid::new_v4()
        ))
    }

    fn sample_turn_record_for_thread(summary: &str, thread_id: &str) -> RuntimeTurnRecord {
        RuntimeTurnRecord {
            recorded_at: Utc::now(),
            turn_id: uuid::Uuid::new_v4(),
            thread_id: thread_id.into(),
            provider_id: "local-mock".into(),
            model: "mock-model".into(),
            request: ChatRequest {
                model: "mock-model".into(),
                system_prompt: Some("runtime system prompt".into()),
                messages: vec![ChatMessage {
                    role: "user".into(),
                    content: vec![ContentBlock::Text {
                        text: format!("request::{summary}"),
                    }],
                }],
                tools: vec![],
            },
            response: ChatResponse {
                provider_id: "local-mock".into(),
                model: "mock-model".into(),
                content: vec![ContentBlock::Text {
                    text: format!("response::{summary}"),
                }],
            },
            narrative: summary.into(),
            tool_outcome: None,
            surfaced_gap: None,
        }
    }

    fn sample_turn_record(summary: &str) -> RuntimeTurnRecord {
        sample_turn_record_for_thread(summary, "main")
    }

    fn sample_pending_turn_checkpoint(
        plan_id: Uuid,
        phase: RuntimePendingTurnPhase,
    ) -> RuntimePendingTurnCheckpoint {
        let conversation_messages = vec![ChatMessage {
            role: "user".into(),
            content: vec![ContentBlock::Text {
                text: "resume runtime".into(),
            }],
        }];
        RuntimePendingTurnCheckpoint {
            checkpoint_id: Uuid::new_v4(),
            plan_id,
            thread_id: "main".into(),
            round_index: 0,
            max_tool_rounds: 3,
            phase,
            system_prompt: "bounded runtime system prompt".into(),
            conversation_messages: conversation_messages.clone(),
            observation: ObservationFrame {
                captured_at: Utc::now(),
                summary: "pending observation".into(),
                screenshot_path: Some("C:/tmp/pending.png".into()),
                ocr_text: Some("pending ocr".into()),
                active_window: Some("Desktop Chat".into()),
                window_titles: vec!["Desktop Chat".into()],
                clipboard_text: Some("clipboard".into()),
                structured_signals: Vec::new(),
            },
            tool_outcome_history: Vec::new(),
            last_action: None,
            last_execution: None,
            last_verification: None,
            last_receipt: None,
            pending_provider_id: Some("local-mock".into()),
            pending_model: Some("mock-model".into()),
            pending_request: Some(ChatRequest {
                model: "mock-model".into(),
                system_prompt: Some("bounded runtime system prompt".into()),
                messages: conversation_messages,
                tools: Vec::new(),
            }),
            pending_response: Some(ChatResponse {
                provider_id: "local-mock".into(),
                model: "mock-model".into(),
                content: vec![ContentBlock::ToolCall {
                    id: "call-pending".into(),
                    name: "host_action".into(),
                    arguments: serde_json::json!({"kind": "capture_observation"}),
                }],
            }),
            pending_narrative: Some("resume from pending checkpoint".into()),
            pending_action: Some(ProposedAction::CaptureObservation),
            pending_tool_call_id: Some("call-pending".into()),
            updated_at: Utc::now(),
        }
    }

    fn sample_turn_record_with_request_messages(
        summary: &str,
        thread_id: &str,
        messages: Vec<ChatMessage>,
    ) -> RuntimeTurnRecord {
        let mut record = sample_turn_record_for_thread(summary, thread_id);
        record.request.messages = messages;
        record
    }

    fn collect_tool_call_ids(messages: &[ChatMessage]) -> Vec<String> {
        messages
            .iter()
            .flat_map(|message| message.content.iter())
            .filter_map(|block| match block {
                ContentBlock::ToolCall { id, .. } => Some(id.clone()),
                _ => None,
            })
            .collect()
    }

    fn collect_tool_result_ids(messages: &[ChatMessage]) -> Vec<String> {
        messages
            .iter()
            .flat_map(|message| message.content.iter())
            .filter_map(|block| match block {
                ContentBlock::ToolResult { id, .. } => Some(id.clone()),
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn journal_repairs_malformed_tail() {
        let root = temp_session_root("repair");
        let config = RuntimeSessionConfig::new(&root, "alpha");
        let session_dir = root.join("alpha");
        fs::create_dir_all(&session_dir).await.unwrap();
        let transcript = session_dir.join("transcript.jsonl");
        let good = serde_json::to_string(&RuntimeSessionEvent::turn_started(
            "alpha", "plan-1", "started",
        ))
        .unwrap();
        fs::write(&transcript, format!("{good}\n{{bad json"))
            .await
            .unwrap();

        let journal = RuntimeSessionJournal::open(config).await.unwrap();
        assert!(journal.repaired());
        let events = journal.read_events().await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, RuntimeSessionEventKind::TurnStarted);

        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn journal_reclaims_stale_lock() {
        let root = temp_session_root("stale");
        let mut config = RuntimeSessionConfig::new(&root, "beta");
        config.max_lock_age_seconds = 0;
        let journal = RuntimeSessionJournal::open(config).await.unwrap();

        std::fs::write(journal.lock_path.clone(), "stale").unwrap();
        let _guard = journal.acquire_write_lock().await.unwrap();

        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn journal_open_with_stale_lock_cleanup_removes_abandoned_lock() {
        let root = temp_session_root("startup-stale-cleanup");
        let session_dir = root.join("beta-startup");
        fs::create_dir_all(&session_dir).await.unwrap();
        fs::write(session_dir.join("write.lock"), "stale")
            .await
            .unwrap();

        let mut config = RuntimeSessionConfig::new(&root, "beta-startup");
        config.max_lock_age_seconds = 0;

        let journal = RuntimeSessionJournal::open_with_stale_lock_cleanup(config)
            .await
            .unwrap();

        assert!(!session_dir.join("write.lock").exists());
        assert_eq!(journal.state().session_id, "beta-startup");

        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn journal_allows_same_process_reentrant_locking() {
        let root = temp_session_root("reentrant");
        let config = RuntimeSessionConfig::new(&root, "gamma");
        let journal = RuntimeSessionJournal::open(config).await.unwrap();

        let guard_one = journal.acquire_write_lock().await.unwrap();
        assert!(journal.lock_path.exists());
        let guard_two = journal.acquire_write_lock().await.unwrap();
        assert!(journal.lock_path.exists());

        drop(guard_one);
        assert!(journal.lock_path.exists());
        drop(guard_two);
        assert!(!journal.lock_path.exists());

        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn journal_append_event_is_lock_safe_across_handles() {
        let root = temp_session_root("append-concurrent");
        let config = RuntimeSessionConfig::new(&root, "gamma-append");
        let journal_one = RuntimeSessionJournal::open(config.clone()).await.unwrap();
        let journal_two = RuntimeSessionJournal::open(config).await.unwrap();

        let event_one = RuntimeSessionEvent::turn_started("gamma-append", "plan-1", "started-one");
        let event_two = RuntimeSessionEvent::turn_started("gamma-append", "plan-2", "started-two");

        let (first, second) = tokio::join!(
            journal_one.append_event(&event_one),
            journal_two.append_event(&event_two)
        );
        first.unwrap();
        second.unwrap();

        let reopened =
            RuntimeSessionJournal::open(RuntimeSessionConfig::new(&root, "gamma-append"))
                .await
                .unwrap();
        let events = reopened.read_events().await.unwrap();
        assert_eq!(events.len(), 2);
        assert!(events.iter().any(|event| event.summary == "started-one"));
        assert!(events.iter().any(|event| event.summary == "started-two"));

        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn journal_marks_startup_summary_only_once() {
        let root = temp_session_root("startup");
        let config = RuntimeSessionConfig::new(&root, "delta");
        let mut journal = RuntimeSessionJournal::open(config).await.unwrap();

        assert!(
            journal
                .mark_startup_summary_injected("trusted startup summary")
                .await
                .unwrap()
        );
        assert!(
            !journal
                .mark_startup_summary_injected("trusted startup summary")
                .await
                .unwrap()
        );
        assert!(journal.state().startup_summary_injected_at.is_some());
        let events = journal.read_events().await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].kind,
            RuntimeSessionEventKind::StartupSummaryInjected
        );

        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn journal_bootstraps_state_from_preexisting_transcript_when_state_is_missing() {
        let root = temp_session_root("bootstrap-missing-state");
        let session_dir = root.join("kappa");
        fs::create_dir_all(&session_dir).await.unwrap();

        let transcript = session_dir.join("transcript.jsonl");
        let events = vec![
            RuntimeSessionEvent::turn_started("kappa", "plan-1", "started"),
            RuntimeSessionEvent::compaction_completed("kappa", "compaction completed"),
            RuntimeSessionEvent::post_compaction_refresh_queued("kappa", "refresh from bootstrap"),
        ];
        let transcript_body = events
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join("\n");
        fs::write(&transcript, format!("{transcript_body}\n"))
            .await
            .unwrap();

        let turn_log = session_dir.join("turn-log.jsonl");
        let tooling = sample_turn_record_for_thread("tooling turn", "tooling-verification");
        fs::write(
            &turn_log,
            format!("{}\n", serde_json::to_string(&tooling).unwrap()),
        )
        .await
        .unwrap();

        let journal = RuntimeSessionJournal::open(RuntimeSessionConfig::new(&root, "kappa"))
            .await
            .unwrap();
        assert!(journal.repaired());
        assert_eq!(journal.state().session_id, "kappa");
        assert_eq!(journal.state().current_transcript_path, "transcript.jsonl");
        assert_eq!(journal.state().compaction_count, 1);
        assert!(journal.state().last_compaction_at.is_some());
        assert_eq!(
            journal.pending_post_compaction_refresh(),
            Some("refresh from bootstrap")
        );
        assert_eq!(journal.foreground_thread_id(), "tooling-verification");
        assert!(journal.state().threads.contains_key("tooling-verification"));

        let persisted = fs::read_to_string(session_dir.join("state.json"))
            .await
            .unwrap();
        assert!(persisted.contains("\"session_id\": \"kappa\""));

        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn journal_bootstrap_repaints_state_from_tail_if_state_is_corrupt_or_truncated() {
        let root = temp_session_root("bootstrap-corrupt-state");
        let session_dir = root.join("lambda-bootstrap");
        fs::create_dir_all(&session_dir).await.unwrap();

        fs::write(session_dir.join("state.json"), "{bad json")
            .await
            .unwrap();
        let good = serde_json::to_string(&RuntimeSessionEvent::turn_started(
            "lambda-bootstrap",
            "plan-1",
            "started-from-tail",
        ))
        .unwrap();
        fs::write(
            session_dir.join("transcript.jsonl"),
            format!("{good}\n{{bad json"),
        )
        .await
        .unwrap();

        let journal =
            RuntimeSessionJournal::open(RuntimeSessionConfig::new(&root, "lambda-bootstrap"))
                .await
                .unwrap();
        assert!(journal.repaired());
        assert_eq!(journal.state().session_id, "lambda-bootstrap");
        let events = journal.read_events().await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].summary, "started-from-tail");

        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn journal_repairs_turn_log_policy_after_truncation() {
        let root = temp_session_root("turn-log-policy-repair");
        let session_dir = root.join("lambda-turn-policy");
        fs::create_dir_all(&session_dir).await.unwrap();

        let good = serde_json::to_string(&RuntimeSessionEvent::turn_started(
            "lambda-turn-policy",
            "plan-1",
            "started",
        ))
        .unwrap();
        fs::write(session_dir.join("transcript.jsonl"), format!("{good}\n"))
            .await
            .unwrap();

        let mut record = sample_turn_record_for_thread("tooling turn", " Tooling Verification ");
        record.turn_id = Uuid::new_v4();
        let duplicate = record.clone();
        let body = format!(
            "{}\n{}\n",
            serde_json::to_string(&record).unwrap(),
            serde_json::to_string(&duplicate).unwrap()
        );
        fs::write(session_dir.join("turn-log.jsonl"), body)
            .await
            .unwrap();

        let journal =
            RuntimeSessionJournal::open(RuntimeSessionConfig::new(&root, "lambda-turn-policy"))
                .await
                .unwrap();
        assert!(journal.repaired());

        let turns = journal.read_turn_records().await.unwrap();
        assert_eq!(turns.len(), 1);
        assert_eq!(turns[0].thread_id, "tooling-verification");
        assert_eq!(journal.foreground_thread_id(), "tooling-verification");
        assert!(journal.state().threads.contains_key("tooling-verification"));

        let rewritten = fs::read_to_string(session_dir.join("turn-log.jsonl"))
            .await
            .unwrap();
        assert_eq!(
            rewritten
                .lines()
                .filter(|line| !line.trim().is_empty())
                .count(),
            1
        );
        assert!(rewritten.contains("\"thread_id\":\"tooling-verification\""));

        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn journal_repairs_provider_tool_pairing_after_truncation() {
        let root = temp_session_root("turn-log-tool-pair-repair");
        let session_dir = root.join("sigma-turn-tool-pair");
        fs::create_dir_all(&session_dir).await.unwrap();

        let good = serde_json::to_string(&RuntimeSessionEvent::turn_started(
            "sigma-turn-tool-pair",
            "plan-1",
            "started",
        ))
        .unwrap();
        fs::write(session_dir.join("transcript.jsonl"), format!("{good}\n"))
            .await
            .unwrap();

        let record = sample_turn_record_with_request_messages(
            "paired tooling turn",
            "tooling-verification",
            vec![
                ChatMessage {
                    role: "user".into(),
                    content: vec![ContentBlock::Text {
                        text: "observe the workspace".into(),
                    }],
                },
                ChatMessage {
                    role: "assistant".into(),
                    content: vec![
                        ContentBlock::Text {
                            text: "calling the tool".into(),
                        },
                        ContentBlock::ToolCall {
                            id: "call-1".into(),
                            name: "host_action".into(),
                            arguments: serde_json::json!({"kind":"capture_observation"}),
                        },
                    ],
                },
                ChatMessage {
                    role: "user".into(),
                    content: vec![
                        ContentBlock::ToolResult {
                            id: "orphan".into(),
                            content: serde_json::json!({"status":"ok"}),
                        },
                        ContentBlock::ToolResult {
                            id: "call-1".into(),
                            content: serde_json::json!({"status":"ok"}),
                        },
                        ContentBlock::ToolResult {
                            id: "call-1".into(),
                            content: serde_json::json!({"status":"duplicate"}),
                        },
                    ],
                },
                ChatMessage {
                    role: "assistant".into(),
                    content: vec![ContentBlock::ToolCall {
                        id: "dangling".into(),
                        name: "host_action".into(),
                        arguments: serde_json::json!({"kind":"click","x":1,"y":2}),
                    }],
                },
            ],
        );
        fs::write(
            session_dir.join("turn-log.jsonl"),
            format!("{}\n", serde_json::to_string(&record).unwrap()),
        )
        .await
        .unwrap();

        let journal =
            RuntimeSessionJournal::open(RuntimeSessionConfig::new(&root, "sigma-turn-tool-pair"))
                .await
                .unwrap();
        assert!(journal.repaired());

        let turns = journal.read_turn_records().await.unwrap();
        assert_eq!(turns.len(), 1);
        let repaired_messages = &turns[0].request.messages;
        assert_eq!(collect_tool_call_ids(repaired_messages), vec!["call-1"]);
        assert_eq!(collect_tool_result_ids(repaired_messages), vec!["call-1"]);
        let rewritten = fs::read_to_string(session_dir.join("turn-log.jsonl"))
            .await
            .unwrap();
        assert!(!rewritten.contains("\"id\":\"orphan\""));
        assert!(!rewritten.contains("\"id\":\"dangling\""));

        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn journal_open_first_bootstrap_preserves_repaired_flag() {
        let root = temp_session_root("bootstrap-repaired-flag");
        let session_dir = root.join("mu");
        fs::create_dir_all(&session_dir).await.unwrap();
        fs::write(session_dir.join("transcript.jsonl"), "{bad json")
            .await
            .unwrap();

        let journal = RuntimeSessionJournal::open(RuntimeSessionConfig::new(&root, "mu"))
            .await
            .unwrap();
        assert!(journal.repaired());
        assert_eq!(journal.state().session_id, "mu");

        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn compact_transcript_clears_pending_snapshot_on_success() {
        let root = temp_session_root("compaction-snapshot-success");
        let config = RuntimeSessionConfig::new(&root, "nu");
        let mut journal = RuntimeSessionJournal::open(config).await.unwrap();

        journal
            .append_event(&RuntimeSessionEvent::turn_started(
                "nu", "plan-1", "started",
            ))
            .await
            .unwrap();

        let result = journal
            .compact_transcript(1, "compaction completed", Some("refresh".into()))
            .await
            .unwrap();

        assert!(result.archive_path.exists());
        assert!(journal.state().pending_compaction.is_none());
        assert_eq!(
            journal.state().current_transcript_path,
            "transcript-1.jsonl"
        );
        assert_eq!(
            journal
                .transcript_path()
                .file_name()
                .and_then(|name| name.to_str()),
            Some("transcript-1.jsonl")
        );
        assert!(!root.join("nu").join("compaction-snapshot-1.json").exists());

        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn compact_transcript_repairs_provider_tool_pairing_in_turn_log() {
        let root = temp_session_root("compaction-turn-log-tool-pair");
        let config = RuntimeSessionConfig::new(&root, "omicron");
        let mut journal = RuntimeSessionJournal::open(config).await.unwrap();

        journal
            .append_event(&RuntimeSessionEvent::turn_started(
                "omicron", "plan-1", "started",
            ))
            .await
            .unwrap();

        let record = sample_turn_record_with_request_messages(
            "compaction pairing turn",
            "tooling-verification",
            vec![
                ChatMessage {
                    role: "assistant".into(),
                    content: vec![ContentBlock::ToolCall {
                        id: "call-keep".into(),
                        name: "host_action".into(),
                        arguments: serde_json::json!({"kind":"capture_observation"}),
                    }],
                },
                ChatMessage {
                    role: "user".into(),
                    content: vec![
                        ContentBlock::ToolResult {
                            id: "call-keep".into(),
                            content: serde_json::json!({"status":"ok"}),
                        },
                        ContentBlock::ToolResult {
                            id: "call-keep".into(),
                            content: serde_json::json!({"status":"duplicate"}),
                        },
                    ],
                },
            ],
        );
        journal.append_turn_record(&record).await.unwrap();

        journal
            .compact_transcript(1, "compaction completed", None)
            .await
            .unwrap();

        let turns = journal.read_turn_records().await.unwrap();
        assert_eq!(turns.len(), 1);
        assert_eq!(
            collect_tool_call_ids(&turns[0].request.messages),
            vec!["call-keep"]
        );
        assert_eq!(
            collect_tool_result_ids(&turns[0].request.messages),
            vec!["call-keep"]
        );

        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn journal_open_recovers_pending_compaction_from_snapshot() {
        let root = temp_session_root("recover-pending-compaction");
        let config = RuntimeSessionConfig::new(&root, "xi");
        let mut journal = RuntimeSessionJournal::open(config.clone()).await.unwrap();

        journal
            .append_event(&RuntimeSessionEvent::turn_started(
                "xi", "plan-1", "started",
            ))
            .await
            .unwrap();
        journal
            .append_turn_record(&sample_turn_record_for_thread(
                "tooling progress",
                "tooling-verification",
            ))
            .await
            .unwrap();

        let snapshot_path = journal.create_compaction_snapshot(1).await.unwrap();
        journal
            .mark_pending_compaction(
                1,
                0,
                "compaction pending".into(),
                &snapshot_path,
                "transcript-1.jsonl",
            )
            .await
            .unwrap();

        fs::write(journal.transcript_path(), "{\"corrupt\":true}\n")
            .await
            .unwrap();
        fs::write(journal.turn_log_path(), "").await.unwrap();

        let reopened = RuntimeSessionJournal::open(config).await.unwrap();
        assert!(reopened.repaired());
        assert!(reopened.state().pending_compaction.is_none());
        assert_eq!(reopened.state().current_transcript_path, "transcript.jsonl");
        assert!(!root.join("xi").join("compaction-snapshot-1.json").exists());
        let events = reopened.read_events().await.unwrap();
        assert!(events.iter().any(|event| event.summary == "started"));
        let turns = reopened.read_turn_records().await.unwrap();
        assert_eq!(turns.len(), 1);
        assert_eq!(turns[0].thread_id, "tooling-verification");

        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn journal_recovers_pending_turn_checkpoint_from_compaction_snapshot() {
        let root = temp_session_root("recover-pending-turn");
        let config = RuntimeSessionConfig::new(&root, "xi-pending-turn");
        let mut journal = RuntimeSessionJournal::open(config.clone()).await.unwrap();

        let checkpoint = sample_pending_turn_checkpoint(
            Uuid::new_v4(),
            RuntimePendingTurnPhase::AwaitingProvider,
        );
        journal
            .write_pending_turn_checkpoint(&checkpoint)
            .await
            .unwrap();

        let snapshot_path = journal.create_compaction_snapshot(1).await.unwrap();
        journal
            .mark_pending_compaction(
                1,
                0,
                "compaction pending".into(),
                &snapshot_path,
                "transcript-1.jsonl",
            )
            .await
            .unwrap();

        fs::remove_file(root.join("xi-pending-turn").join("pending-turn.json"))
            .await
            .unwrap();
        fs::write(journal.transcript_path(), "{\"corrupt\":true}\n")
            .await
            .unwrap();
        fs::write(journal.turn_log_path(), "").await.unwrap();

        let reopened = RuntimeSessionJournal::open(config).await.unwrap();
        assert!(reopened.repaired());
        let recovered = reopened
            .read_pending_turn_checkpoint()
            .await
            .unwrap()
            .expect("pending turn should be restored from snapshot");
        assert_eq!(recovered.phase, RuntimePendingTurnPhase::AwaitingProvider);
        assert_eq!(
            recovered.pending_tool_call_id.as_deref(),
            Some("call-pending")
        );

        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn journal_open_clears_stale_pending_turn_after_terminal_event() {
        let root = temp_session_root("stale-pending-turn");
        let config = RuntimeSessionConfig::new(&root, "sigma-pending");
        let journal = RuntimeSessionJournal::open(config.clone()).await.unwrap();

        let checkpoint = sample_pending_turn_checkpoint(
            Uuid::new_v4(),
            RuntimePendingTurnPhase::AwaitingProvider,
        );
        journal
            .write_pending_turn_checkpoint(&checkpoint)
            .await
            .unwrap();
        journal
            .append_event(&RuntimeSessionEvent::turn_completed(
                "sigma-pending",
                checkpoint.plan_id.to_string(),
                "local-mock",
                "mock-model",
                "terminal completion after pending checkpoint",
            ))
            .await
            .unwrap();
        drop(journal);

        let reopened = RuntimeSessionJournal::open(config).await.unwrap();
        assert!(reopened.repaired());
        assert!(
            reopened
                .read_pending_turn_checkpoint()
                .await
                .unwrap()
                .is_none()
        );

        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn journal_bootstraps_from_rotated_transcript_when_state_is_missing() {
        let root = temp_session_root("bootstrap-rotated-transcript");
        let config = RuntimeSessionConfig::new(&root, "xi-rotated");
        let mut journal = RuntimeSessionJournal::open(config.clone()).await.unwrap();

        journal
            .append_event(&RuntimeSessionEvent::turn_started(
                "xi-rotated",
                "plan-1",
                "started",
            ))
            .await
            .unwrap();
        journal
            .compact_transcript(0, "compaction completed", Some("refresh".into()))
            .await
            .unwrap();

        fs::remove_file(root.join("xi-rotated").join("state.json"))
            .await
            .unwrap();

        let reopened = RuntimeSessionJournal::open(config).await.unwrap();
        assert!(reopened.repaired());
        assert_eq!(
            reopened.state().current_transcript_path,
            "transcript-1.jsonl"
        );
        assert_eq!(
            reopened
                .transcript_path()
                .file_name()
                .and_then(|name| name.to_str()),
            Some("transcript-1.jsonl")
        );
        assert_eq!(reopened.pending_post_compaction_refresh(), Some("refresh"));
        assert!(!root.join("xi-rotated").join("transcript.jsonl").exists());

        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn compact_transcript_retries_and_restores_after_timeout() {
        let root = temp_session_root("compaction-timeout-retry");
        let config = RuntimeSessionConfig::new(&root, "omicron");
        let mut journal = RuntimeSessionJournal::open(config).await.unwrap();

        journal
            .append_event(&RuntimeSessionEvent::turn_started(
                "omicron", "plan-1", "started",
            ))
            .await
            .unwrap();
        journal
            .append_turn_record(&sample_turn_record_for_thread(
                "tooling progress",
                "tooling-verification",
            ))
            .await
            .unwrap();

        let error = journal
            .compact_transcript_with_policy(
                1,
                "compaction completed".into(),
                Some("refresh".into()),
                StdDuration::ZERO,
                1,
            )
            .await
            .unwrap_err();
        let error_text = format!("{error:#}");

        assert!(error_text.contains("compaction attempt 2/2 failed"));
        assert_eq!(journal.state().compaction_count, 0);
        assert!(journal.state().pending_compaction.is_none());
        assert_eq!(journal.state().current_transcript_path, "transcript.jsonl");
        assert!(
            !root
                .join("omicron")
                .join("transcript-compaction-1.jsonl")
                .exists()
        );
        assert!(
            !root
                .join("omicron")
                .join("compaction-snapshot-1.json")
                .exists()
        );

        let events = journal.read_events().await.unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind == RuntimeSessionEventKind::CompactionStarted)
                .count(),
            2
        );
        assert!(
            !events
                .iter()
                .any(|event| event.kind == RuntimeSessionEventKind::CompactionCompleted)
        );

        let turns = journal.read_turn_records().await.unwrap();
        assert_eq!(turns.len(), 1);
        assert_eq!(turns[0].thread_id, "tooling-verification");

        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn journal_tracks_memory_flush_per_compaction_cycle() {
        let root = temp_session_root("flush");
        let config = RuntimeSessionConfig::new(&root, "epsilon");
        let mut journal = RuntimeSessionJournal::open(config).await.unwrap();

        assert!(
            journal
                .record_memory_flush("pre-compaction memory flush", Some("abc123".into()))
                .await
                .unwrap()
        );
        assert!(
            !journal
                .record_memory_flush("pre-compaction memory flush", Some("abc123".into()))
                .await
                .unwrap()
        );
        assert!(journal.has_flushed_current_compaction());

        journal
            .begin_compaction("starting compaction")
            .await
            .unwrap();
        let _archive = journal
            .complete_compaction("compaction completed")
            .await
            .unwrap();
        assert!(!journal.has_flushed_current_compaction());
        assert!(
            journal
                .record_memory_flush("pre-compaction memory flush", Some("def456".into()))
                .await
                .unwrap()
        );

        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn journal_rotates_transcript_on_compaction() {
        let root = temp_session_root("rotate");
        let config = RuntimeSessionConfig::new(&root, "zeta");
        let mut journal = RuntimeSessionJournal::open(config).await.unwrap();

        journal
            .append_event(&RuntimeSessionEvent::turn_started(
                "zeta", "plan-1", "started",
            ))
            .await
            .unwrap();
        journal
            .mark_startup_summary_injected("session startup summary")
            .await
            .unwrap();
        journal
            .begin_compaction("starting compaction")
            .await
            .unwrap();
        let archive_path = journal
            .complete_compaction("compaction completed")
            .await
            .unwrap();

        assert!(archive_path.exists());
        assert_eq!(journal.state().compaction_count, 1);
        assert!(journal.state().last_compaction_at.is_some());
        assert!(journal.state().startup_summary_injected_at.is_none());
        assert_eq!(
            journal.state().current_transcript_path,
            "transcript-1.jsonl"
        );
        assert!(!root.join("zeta").join("transcript.jsonl").exists());

        let archived = fs::read_to_string(&archive_path).await.unwrap();
        assert!(archived.contains("turn_started"));
        assert!(archived.contains("compaction_started"));

        let current = fs::read_to_string(journal.transcript_path()).await.unwrap();
        assert!(current.contains("compaction_completed"));

        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn compact_transcript_preserves_tail_and_queues_refresh() {
        let root = temp_session_root("preserve");
        let config = RuntimeSessionConfig::new(&root, "eta");
        let mut journal = RuntimeSessionJournal::open(config).await.unwrap();

        for idx in 0..4 {
            journal
                .append_event(&RuntimeSessionEvent::turn_started(
                    "eta",
                    format!("plan-{idx}"),
                    format!("started-{idx}"),
                ))
                .await
                .unwrap();
        }

        let result = journal
            .compact_transcript(
                2,
                "compaction completed",
                Some("Re-read Session Startup and Red Lines.".into()),
            )
            .await
            .unwrap();

        assert_eq!(result.preserved_event_count, 2);
        assert_eq!(result.dropped_event_count, 2);
        assert!(result.pending_post_compaction_refresh);
        assert_eq!(
            journal.pending_post_compaction_refresh(),
            Some("Re-read Session Startup and Red Lines.")
        );

        let current = fs::read_to_string(journal.transcript_path()).await.unwrap();
        assert!(current.contains("started-2"));
        assert!(current.contains("started-3"));
        assert!(current.contains("post_compaction_refresh_queued"));

        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn turn_log_round_trips_and_compacts_with_tail_preserved() {
        let root = temp_session_root("turn-log");
        let config = RuntimeSessionConfig::new(&root, "lambda");
        let mut journal = RuntimeSessionJournal::open(config).await.unwrap();

        for idx in 0..4 {
            journal
                .append_turn_record(&sample_turn_record(&format!("turn-{idx}")))
                .await
                .unwrap();
        }

        let records = journal.read_turn_records().await.unwrap();
        assert_eq!(records.len(), 4);
        assert_eq!(records[0].narrative, "turn-0");

        journal
            .compact_transcript(2, "compaction completed", None)
            .await
            .unwrap();

        let current = journal.read_turn_records().await.unwrap();
        assert_eq!(current.len(), 2);
        assert_eq!(current[0].narrative, "turn-2");
        assert_eq!(current[1].narrative, "turn-3");

        let archived = fs::read_to_string(root.join("lambda").join("turn-log-compaction-1.jsonl"))
            .await
            .unwrap();
        assert!(archived.contains("turn-0"));
        assert!(archived.contains("turn-3"));

        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn take_post_compaction_refresh_consumes_once() {
        let root = temp_session_root("consume-refresh");
        let config = RuntimeSessionConfig::new(&root, "theta");
        let mut journal = RuntimeSessionJournal::open(config).await.unwrap();

        journal
            .compact_transcript(0, "compaction completed", Some("refresh once".into()))
            .await
            .unwrap();

        let refresh = journal.take_post_compaction_refresh().await.unwrap();
        assert_eq!(refresh.as_deref(), Some("refresh once"));
        assert!(
            journal
                .take_post_compaction_refresh()
                .await
                .unwrap()
                .is_none()
        );
        assert!(journal.pending_post_compaction_refresh().is_none());

        let events = journal.read_events().await.unwrap();
        assert!(
            events
                .iter()
                .any(|event| event.kind == RuntimeSessionEventKind::PostCompactionRefreshConsumed)
        );

        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn switching_threads_compacts_backgrounded_thread() {
        let root = temp_session_root("threads");
        let config = RuntimeSessionConfig::new(&root, "iota");
        let mut journal = RuntimeSessionJournal::open(config).await.unwrap();

        journal.record_foreground_turn("main turn 1").await.unwrap();
        journal.record_foreground_turn("main turn 2").await.unwrap();
        journal.record_foreground_turn("main turn 3").await.unwrap();
        journal.record_foreground_turn("main turn 4").await.unwrap();

        journal
            .switch_foreground_thread("tooling", Some("Tooling"))
            .await
            .unwrap();

        let main = journal.state().threads.get("main").unwrap();
        assert!(
            main.compact_summary
                .as_deref()
                .unwrap_or("")
                .contains("main turn 1")
        );
        assert_eq!(main.recent_turns.len(), 3);
        assert_eq!(journal.foreground_thread_id(), "tooling");

        let context = journal.build_thread_prompt_context().unwrap();
        assert!(context.contains("# Current Thread"));
        assert!(context.contains("Tooling"));
        assert!(context.contains("# Background Thread Context"));
        assert!(context.contains("Main"));

        let _ = fs::remove_dir_all(root).await;
    }
}
