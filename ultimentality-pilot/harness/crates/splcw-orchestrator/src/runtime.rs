use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use anyhow::{Context, anyhow};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use splcw_computer_use::{ActionExecution, MouseButton, ObservationFrame, ProposedAction};
use splcw_core::{CapabilityGap, CapabilityGapStatus, Receipt, SplcwUnit};
use splcw_host::HostBody;
use splcw_llm::{
    AuthProfileStore, ChatMessage, ChatRequest, ChatResponse, ConfiguredLlmClient, ContentBlock,
    ToolDefinition,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::{Duration, timeout};
use uuid::Uuid;

use crate::{OrchestratorState, PersistentOrchestrator, RuntimeLanes};
use crate::{
    RuntimeGapRecord, RuntimePendingTurnCheckpoint, RuntimePendingTurnPhase, RuntimeSessionConfig,
    RuntimeSessionEvent, RuntimeSessionJournal, RuntimeToolOutcomeRecord, RuntimeTurnRecord,
};

use crate::gap_task_emitter::emit_gap_task;
use crate::host_verify_retry::{StabilizationOutcome, stabilize_host_effect};

const HOST_ACTION_TOOL: &str = "host_action";
const CAPABILITY_GAP_TOOL: &str = "capability_gap";
const SUPERVISED_GITHUB_ACTION_TOOL: &str = "supervised_github_action";
const DEFAULT_GLOBAL_LANE: &str = "runtime-global";

static LANE_REGISTRY: OnceLock<Mutex<HashMap<String, Arc<Semaphore>>>> = OnceLock::new();
const LANE_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_HOST_OBSERVE_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_HOST_ENACT_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_HOST_VERIFY_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeTurnOptions {
    pub model: Option<String>,
    pub max_tool_rounds: usize,
    pub receipt_context_limit: usize,
    pub receipt_unit: SplcwUnit,
    pub lanes: Option<RuntimeLanes>,
    pub session: Option<RuntimeSessionConfig>,
    pub thread_id: Option<String>,
    pub thread_label: Option<String>,
    pub host_observe_timeout: Duration,
    pub host_enact_timeout: Duration,
    pub host_verify_timeout: Duration,
    pub external_context: Option<String>,
}

impl Default for RuntimeTurnOptions {
    fn default() -> Self {
        Self {
            model: None,
            max_tool_rounds: 4,
            receipt_context_limit: 5,
            receipt_unit: SplcwUnit::Sculptor,
            lanes: None,
            session: None,
            thread_id: None,
            thread_label: None,
            host_observe_timeout: DEFAULT_HOST_OBSERVE_TIMEOUT,
            host_enact_timeout: DEFAULT_HOST_ENACT_TIMEOUT,
            host_verify_timeout: DEFAULT_HOST_VERIFY_TIMEOUT,
            external_context: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeTurnOutcome {
    pub provider_id: String,
    pub model: String,
    pub observation: ObservationFrame,
    pub response: ChatResponse,
    pub narrative: String,
    pub tool_outcome_history: Vec<RuntimeToolOutcomeRecord>,
    pub proposed_action: Option<ProposedAction>,
    pub execution: Option<ActionExecution>,
    pub verification: Option<ObservationFrame>,
    pub receipt: Option<Receipt>,
    pub github_action_request: Option<SupervisedGithubActionRequest>,
    pub surfaced_gap: Option<CapabilityGap>,
    pub acquired_lanes: Option<RuntimeLanes>,
}

#[derive(Debug, Deserialize)]
struct CapabilityGapDirective {
    title: String,
    permanent_fix_target: String,
    #[serde(default)]
    notes: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SupervisedGithubActionKind {
    CommentIssue,
    CommentPullRequest,
    AssignIssue,
    AssignPullRequest,
    CloseIssue,
    ClosePullRequest,
    ReopenIssue,
    ReopenPullRequest,
    LabelIssue,
    LabelPullRequest,
    RemoveLabelIssue,
    RemoveLabelPullRequest,
}

impl SupervisedGithubActionKind {
    pub fn as_label(self) -> &'static str {
        match self {
            Self::CommentIssue => "comment_issue",
            Self::CommentPullRequest => "comment_pull_request",
            Self::AssignIssue => "assign_issue",
            Self::AssignPullRequest => "assign_pull_request",
            Self::CloseIssue => "close_issue",
            Self::ClosePullRequest => "close_pull_request",
            Self::ReopenIssue => "reopen_issue",
            Self::ReopenPullRequest => "reopen_pull_request",
            Self::LabelIssue => "label_issue",
            Self::LabelPullRequest => "label_pull_request",
            Self::RemoveLabelIssue => "remove_label_issue",
            Self::RemoveLabelPullRequest => "remove_label_pull_request",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupervisedGithubActionRequest {
    pub kind: SupervisedGithubActionKind,
    pub repository: Option<String>,
    pub issue_number: Option<u64>,
    pub pull_request_number: Option<u64>,
    pub body: Option<String>,
    pub label: Option<String>,
    pub assignee: Option<String>,
    pub justification: Option<String>,
}

impl SupervisedGithubActionRequest {
    fn validate(self) -> anyhow::Result<Self> {
        let normalized = Self {
            kind: self.kind,
            repository: normalize_optional_field(self.repository),
            issue_number: self.issue_number,
            pull_request_number: self.pull_request_number,
            body: normalize_optional_field(self.body),
            label: normalize_optional_field(self.label),
            assignee: normalize_optional_field(self.assignee),
            justification: normalize_optional_field(self.justification),
        };

        match normalized.kind {
            SupervisedGithubActionKind::CommentIssue => {
                required_string_field("body", normalized.body.as_deref())?;
                reject_present_u64_field(
                    "pull_request_number",
                    normalized.pull_request_number,
                    normalized.kind,
                )?;
                reject_present_string_field("label", normalized.label.as_deref(), normalized.kind)?;
                reject_present_string_field(
                    "assignee",
                    normalized.assignee.as_deref(),
                    normalized.kind,
                )?;
            }
            SupervisedGithubActionKind::CommentPullRequest => {
                required_string_field("body", normalized.body.as_deref())?;
                reject_present_u64_field("issue_number", normalized.issue_number, normalized.kind)?;
                reject_present_string_field("label", normalized.label.as_deref(), normalized.kind)?;
                reject_present_string_field(
                    "assignee",
                    normalized.assignee.as_deref(),
                    normalized.kind,
                )?;
            }
            SupervisedGithubActionKind::AssignIssue => {
                required_string_field("assignee", normalized.assignee.as_deref())?;
                reject_present_u64_field(
                    "pull_request_number",
                    normalized.pull_request_number,
                    normalized.kind,
                )?;
                reject_present_string_field("body", normalized.body.as_deref(), normalized.kind)?;
                reject_present_string_field("label", normalized.label.as_deref(), normalized.kind)?;
            }
            SupervisedGithubActionKind::AssignPullRequest => {
                required_string_field("assignee", normalized.assignee.as_deref())?;
                reject_present_u64_field("issue_number", normalized.issue_number, normalized.kind)?;
                reject_present_string_field("body", normalized.body.as_deref(), normalized.kind)?;
                reject_present_string_field("label", normalized.label.as_deref(), normalized.kind)?;
            }
            SupervisedGithubActionKind::CloseIssue => {
                reject_present_u64_field(
                    "pull_request_number",
                    normalized.pull_request_number,
                    normalized.kind,
                )?;
                reject_present_string_field("label", normalized.label.as_deref(), normalized.kind)?;
                reject_present_string_field(
                    "assignee",
                    normalized.assignee.as_deref(),
                    normalized.kind,
                )?;
            }
            SupervisedGithubActionKind::ClosePullRequest => {
                reject_present_u64_field("issue_number", normalized.issue_number, normalized.kind)?;
                reject_present_string_field("label", normalized.label.as_deref(), normalized.kind)?;
                reject_present_string_field(
                    "assignee",
                    normalized.assignee.as_deref(),
                    normalized.kind,
                )?;
            }
            SupervisedGithubActionKind::ReopenIssue => {
                reject_present_u64_field(
                    "pull_request_number",
                    normalized.pull_request_number,
                    normalized.kind,
                )?;
                reject_present_string_field("label", normalized.label.as_deref(), normalized.kind)?;
                reject_present_string_field(
                    "assignee",
                    normalized.assignee.as_deref(),
                    normalized.kind,
                )?;
            }
            SupervisedGithubActionKind::ReopenPullRequest => {
                reject_present_u64_field("issue_number", normalized.issue_number, normalized.kind)?;
                reject_present_string_field("label", normalized.label.as_deref(), normalized.kind)?;
                reject_present_string_field(
                    "assignee",
                    normalized.assignee.as_deref(),
                    normalized.kind,
                )?;
            }
            SupervisedGithubActionKind::LabelIssue => {
                required_string_field("label", normalized.label.as_deref())?;
                reject_present_u64_field(
                    "pull_request_number",
                    normalized.pull_request_number,
                    normalized.kind,
                )?;
                reject_present_string_field("body", normalized.body.as_deref(), normalized.kind)?;
                reject_present_string_field(
                    "assignee",
                    normalized.assignee.as_deref(),
                    normalized.kind,
                )?;
            }
            SupervisedGithubActionKind::LabelPullRequest => {
                required_string_field("label", normalized.label.as_deref())?;
                reject_present_u64_field("issue_number", normalized.issue_number, normalized.kind)?;
                reject_present_string_field("body", normalized.body.as_deref(), normalized.kind)?;
                reject_present_string_field(
                    "assignee",
                    normalized.assignee.as_deref(),
                    normalized.kind,
                )?;
            }
            SupervisedGithubActionKind::RemoveLabelIssue => {
                required_string_field("label", normalized.label.as_deref())?;
                reject_present_u64_field(
                    "pull_request_number",
                    normalized.pull_request_number,
                    normalized.kind,
                )?;
                reject_present_string_field("body", normalized.body.as_deref(), normalized.kind)?;
                reject_present_string_field(
                    "assignee",
                    normalized.assignee.as_deref(),
                    normalized.kind,
                )?;
            }
            SupervisedGithubActionKind::RemoveLabelPullRequest => {
                required_string_field("label", normalized.label.as_deref())?;
                reject_present_u64_field("issue_number", normalized.issue_number, normalized.kind)?;
                reject_present_string_field("body", normalized.body.as_deref(), normalized.kind)?;
                reject_present_string_field(
                    "assignee",
                    normalized.assignee.as_deref(),
                    normalized.kind,
                )?;
            }
        }

        Ok(normalized)
    }

    pub fn summary(&self) -> String {
        let repo = self
            .repository
            .as_deref()
            .map(|repo| format!(" in {repo}"))
            .unwrap_or_default();
        let justification = self
            .justification
            .as_deref()
            .map(|value| format!(" | why: {value}"))
            .unwrap_or_default();
        match self.kind {
            SupervisedGithubActionKind::CommentIssue => self.issue_number.map_or_else(
                || {
                    format!(
                        "{} on an operator-selected issue{}{}",
                        self.kind.as_label(),
                        repo,
                        justification
                    )
                },
                |issue_number| {
                    format!(
                        "{} on issue #{}{}{}",
                        self.kind.as_label(),
                        issue_number,
                        repo,
                        justification
                    )
                },
            ),
            SupervisedGithubActionKind::CommentPullRequest => self.pull_request_number.map_or_else(
                || {
                    format!(
                        "{} on an operator-selected pull request{}{}",
                        self.kind.as_label(),
                        repo,
                        justification
                    )
                },
                |pull_request_number| {
                    format!(
                        "{} on pull request #{}{}{}",
                        self.kind.as_label(),
                        pull_request_number,
                        repo,
                        justification
                    )
                },
            ),
            SupervisedGithubActionKind::AssignIssue => self.issue_number.map_or_else(
                || {
                    format!(
                        "{} on an operator-selected issue assigning `{}`{}{}",
                        self.kind.as_label(),
                        self.assignee.as_deref().unwrap_or(""),
                        repo,
                        justification
                    )
                },
                |issue_number| {
                    format!(
                        "{} on issue #{} assigning `{}`{}{}",
                        self.kind.as_label(),
                        issue_number,
                        self.assignee.as_deref().unwrap_or(""),
                        repo,
                        justification
                    )
                },
            ),
            SupervisedGithubActionKind::AssignPullRequest => self.pull_request_number.map_or_else(
                || {
                    format!(
                        "{} on an operator-selected pull request assigning `{}`{}{}",
                        self.kind.as_label(),
                        self.assignee.as_deref().unwrap_or(""),
                        repo,
                        justification
                    )
                },
                |pull_request_number| {
                    format!(
                        "{} on pull request #{} assigning `{}`{}{}",
                        self.kind.as_label(),
                        pull_request_number,
                        self.assignee.as_deref().unwrap_or(""),
                        repo,
                        justification
                    )
                },
            ),
            SupervisedGithubActionKind::CloseIssue => self.issue_number.map_or_else(
                || {
                    format!(
                        "{} on an operator-selected issue{}{}",
                        self.kind.as_label(),
                        repo,
                        justification
                    )
                },
                |issue_number| {
                    format!(
                        "{} on issue #{}{}{}",
                        self.kind.as_label(),
                        issue_number,
                        repo,
                        justification
                    )
                },
            ),
            SupervisedGithubActionKind::ClosePullRequest => self.pull_request_number.map_or_else(
                || {
                    format!(
                        "{} on an operator-selected pull request{}{}",
                        self.kind.as_label(),
                        repo,
                        justification
                    )
                },
                |pull_request_number| {
                    format!(
                        "{} on pull request #{}{}{}",
                        self.kind.as_label(),
                        pull_request_number,
                        repo,
                        justification
                    )
                },
            ),
            SupervisedGithubActionKind::ReopenIssue => self.issue_number.map_or_else(
                || {
                    format!(
                        "{} on an operator-selected issue{}{}",
                        self.kind.as_label(),
                        repo,
                        justification
                    )
                },
                |issue_number| {
                    format!(
                        "{} on issue #{}{}{}",
                        self.kind.as_label(),
                        issue_number,
                        repo,
                        justification
                    )
                },
            ),
            SupervisedGithubActionKind::ReopenPullRequest => self.pull_request_number.map_or_else(
                || {
                    format!(
                        "{} on an operator-selected pull request{}{}",
                        self.kind.as_label(),
                        repo,
                        justification
                    )
                },
                |pull_request_number| {
                    format!(
                        "{} on pull request #{}{}{}",
                        self.kind.as_label(),
                        pull_request_number,
                        repo,
                        justification
                    )
                },
            ),
            SupervisedGithubActionKind::LabelIssue => self.issue_number.map_or_else(
                || {
                    format!(
                        "{} on an operator-selected issue with label `{}`{}{}",
                        self.kind.as_label(),
                        self.label.as_deref().unwrap_or(""),
                        repo,
                        justification
                    )
                },
                |issue_number| {
                    format!(
                        "{} on issue #{} with label `{}`{}{}",
                        self.kind.as_label(),
                        issue_number,
                        self.label.as_deref().unwrap_or(""),
                        repo,
                        justification
                    )
                },
            ),
            SupervisedGithubActionKind::LabelPullRequest => self.pull_request_number.map_or_else(
                || {
                    format!(
                        "{} on an operator-selected pull request with label `{}`{}{}",
                        self.kind.as_label(),
                        self.label.as_deref().unwrap_or(""),
                        repo,
                        justification
                    )
                },
                |pull_request_number| {
                    format!(
                        "{} on pull request #{} with label `{}`{}{}",
                        self.kind.as_label(),
                        pull_request_number,
                        self.label.as_deref().unwrap_or(""),
                        repo,
                        justification
                    )
                },
            ),
            SupervisedGithubActionKind::RemoveLabelIssue => self.issue_number.map_or_else(
                || {
                    format!(
                        "{} on an operator-selected issue removing label `{}`{}{}",
                        self.kind.as_label(),
                        self.label.as_deref().unwrap_or(""),
                        repo,
                        justification
                    )
                },
                |issue_number| {
                    format!(
                        "{} on issue #{} removing label `{}`{}{}",
                        self.kind.as_label(),
                        issue_number,
                        self.label.as_deref().unwrap_or(""),
                        repo,
                        justification
                    )
                },
            ),
            SupervisedGithubActionKind::RemoveLabelPullRequest => {
                self.pull_request_number.map_or_else(
                    || {
                        format!(
                            "{} on an operator-selected pull request removing label `{}`{}{}",
                            self.kind.as_label(),
                            self.label.as_deref().unwrap_or(""),
                            repo,
                            justification
                        )
                    },
                    |pull_request_number| {
                        format!(
                            "{} on pull request #{} removing label `{}`{}{}",
                            self.kind.as_label(),
                            pull_request_number,
                            self.label.as_deref().unwrap_or(""),
                            repo,
                            justification
                        )
                    },
                )
            }
        }
    }

    pub fn target_summary(&self) -> String {
        let repo = self
            .repository
            .as_deref()
            .map(|value| format!("{value} "))
            .unwrap_or_default();
        match self.kind {
            SupervisedGithubActionKind::CommentIssue
            | SupervisedGithubActionKind::AssignIssue
            | SupervisedGithubActionKind::CloseIssue
            | SupervisedGithubActionKind::ReopenIssue
            | SupervisedGithubActionKind::LabelIssue
            | SupervisedGithubActionKind::RemoveLabelIssue => self.issue_number.map_or_else(
                || format!("{repo}issue target pending operator selection"),
                |issue_number| format!("{repo}issue #{issue_number}"),
            ),
            SupervisedGithubActionKind::CommentPullRequest
            | SupervisedGithubActionKind::AssignPullRequest
            | SupervisedGithubActionKind::ClosePullRequest
            | SupervisedGithubActionKind::ReopenPullRequest
            | SupervisedGithubActionKind::LabelPullRequest
            | SupervisedGithubActionKind::RemoveLabelPullRequest => {
                self.pull_request_number.map_or_else(
                    || format!("{repo}pull request target pending operator selection"),
                    |pull_request_number| format!("{repo}pull request #{pull_request_number}"),
                )
            }
        }
    }

    pub fn requires_operator_target(&self) -> bool {
        match self.kind {
            SupervisedGithubActionKind::CommentIssue
            | SupervisedGithubActionKind::AssignIssue
            | SupervisedGithubActionKind::CloseIssue
            | SupervisedGithubActionKind::ReopenIssue
            | SupervisedGithubActionKind::LabelIssue
            | SupervisedGithubActionKind::RemoveLabelIssue => self.issue_number.is_none(),
            SupervisedGithubActionKind::CommentPullRequest
            | SupervisedGithubActionKind::AssignPullRequest
            | SupervisedGithubActionKind::ClosePullRequest
            | SupervisedGithubActionKind::ReopenPullRequest
            | SupervisedGithubActionKind::LabelPullRequest
            | SupervisedGithubActionKind::RemoveLabelPullRequest => {
                self.pull_request_number.is_none()
            }
        }
    }

    pub fn operator_target_kind(&self) -> &'static str {
        match self.kind {
            SupervisedGithubActionKind::CommentIssue
            | SupervisedGithubActionKind::AssignIssue
            | SupervisedGithubActionKind::CloseIssue
            | SupervisedGithubActionKind::ReopenIssue
            | SupervisedGithubActionKind::LabelIssue
            | SupervisedGithubActionKind::RemoveLabelIssue => "issue",
            SupervisedGithubActionKind::CommentPullRequest
            | SupervisedGithubActionKind::AssignPullRequest
            | SupervisedGithubActionKind::ClosePullRequest
            | SupervisedGithubActionKind::ReopenPullRequest
            | SupervisedGithubActionKind::LabelPullRequest
            | SupervisedGithubActionKind::RemoveLabelPullRequest => "pull request",
        }
    }

    pub fn with_operator_target_number(&self, target_number: u64) -> Self {
        let mut request = self.clone();
        match request.kind {
            SupervisedGithubActionKind::CommentIssue
            | SupervisedGithubActionKind::AssignIssue
            | SupervisedGithubActionKind::CloseIssue
            | SupervisedGithubActionKind::ReopenIssue
            | SupervisedGithubActionKind::LabelIssue
            | SupervisedGithubActionKind::RemoveLabelIssue => {
                request.issue_number = Some(target_number)
            }
            SupervisedGithubActionKind::CommentPullRequest
            | SupervisedGithubActionKind::AssignPullRequest
            | SupervisedGithubActionKind::ClosePullRequest
            | SupervisedGithubActionKind::ReopenPullRequest
            | SupervisedGithubActionKind::LabelPullRequest
            | SupervisedGithubActionKind::RemoveLabelPullRequest => {
                request.pull_request_number = Some(target_number)
            }
        }
        request
    }
}

#[derive(Debug, Deserialize)]
struct FocusVerificationSignal {
    expected: String,
    observed: Option<String>,
    matched: bool,
    stable: bool,
    attempts: usize,
    timed_out: bool,
}

#[derive(Debug, Deserialize)]
struct PostActionVerificationSignal {
    kind: String,
    ok: bool,
    evidence: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RuntimeVerificationDecision {
    kind: String,
    ok: bool,
    proof_level: Option<String>,
    summary: String,
    contradiction: Option<String>,
}

#[derive(Debug, Clone)]
struct PendingProviderStep {
    provider_id: String,
    model: String,
    request: ChatRequest,
    response: ChatResponse,
    narrative: String,
    action: ProposedAction,
    tool_call_id: String,
}

#[derive(Debug, Clone)]
struct PendingResumeState {
    round_index: usize,
    max_tool_rounds: usize,
    observation: ObservationFrame,
    system_prompt: String,
    conversation_messages: Vec<ChatMessage>,
    tool_outcome_history: Vec<RuntimeToolOutcomeRecord>,
    last_action: Option<ProposedAction>,
    last_execution: Option<ActionExecution>,
    last_verification: Option<ObservationFrame>,
    last_receipt: Option<Receipt>,
    pending_provider_step: Option<PendingProviderStep>,
}

impl RuntimeVerificationDecision {
    fn new(
        kind: impl Into<String>,
        ok: bool,
        proof_level: Option<String>,
        contradiction: Option<String>,
    ) -> Self {
        let kind = kind.into();
        let summary = format!(
            "verify kind={} ok={} proof={}",
            kind,
            ok,
            proof_level.as_deref().unwrap_or("legacy_host_signal")
        );
        Self {
            kind,
            ok,
            proof_level,
            summary,
            contradiction,
        }
    }
}

fn build_pending_turn_checkpoint(
    plan_id: Uuid,
    thread_id: String,
    round_index: usize,
    max_tool_rounds: usize,
    phase: RuntimePendingTurnPhase,
    system_prompt: &str,
    conversation_messages: &[ChatMessage],
    observation: &ObservationFrame,
    tool_outcome_history: &[RuntimeToolOutcomeRecord],
    last_action: Option<&ProposedAction>,
    last_execution: Option<&ActionExecution>,
    last_verification: Option<&ObservationFrame>,
    last_receipt: Option<&Receipt>,
    pending_provider_step: Option<&PendingProviderStep>,
) -> RuntimePendingTurnCheckpoint {
    RuntimePendingTurnCheckpoint {
        checkpoint_id: Uuid::new_v4(),
        plan_id,
        thread_id,
        round_index,
        max_tool_rounds,
        phase,
        system_prompt: system_prompt.to_string(),
        conversation_messages: conversation_messages.to_vec(),
        observation: observation.clone(),
        tool_outcome_history: tool_outcome_history.to_vec(),
        last_action: last_action.cloned(),
        last_execution: last_execution.cloned(),
        last_verification: last_verification.cloned(),
        last_receipt: last_receipt.cloned(),
        pending_provider_id: pending_provider_step.map(|step| step.provider_id.clone()),
        pending_model: pending_provider_step.map(|step| step.model.clone()),
        pending_request: pending_provider_step.map(|step| step.request.clone()),
        pending_response: pending_provider_step.map(|step| step.response.clone()),
        pending_narrative: pending_provider_step.map(|step| step.narrative.clone()),
        pending_action: pending_provider_step.map(|step| step.action.clone()),
        pending_tool_call_id: pending_provider_step.map(|step| step.tool_call_id.clone()),
        updated_at: Utc::now(),
    }
}

async fn write_pending_turn_checkpoint_if_present(
    journal: Option<&RuntimeSessionJournal>,
    checkpoint: &RuntimePendingTurnCheckpoint,
) -> anyhow::Result<()> {
    if let Some(journal) = journal {
        journal.write_pending_turn_checkpoint(checkpoint).await?;
    }
    Ok(())
}

async fn clear_pending_turn_checkpoint_if_present(
    journal: Option<&RuntimeSessionJournal>,
) -> anyhow::Result<()> {
    if let Some(journal) = journal {
        journal.clear_pending_turn_checkpoint().await?;
    }
    Ok(())
}

fn build_pending_resume_state(
    checkpoint: RuntimePendingTurnCheckpoint,
) -> anyhow::Result<PendingResumeState> {
    match checkpoint.phase {
        RuntimePendingTurnPhase::AwaitingProvider => Ok(PendingResumeState {
            round_index: checkpoint.round_index,
            max_tool_rounds: checkpoint.max_tool_rounds.max(1),
            observation: checkpoint.observation,
            system_prompt: checkpoint.system_prompt,
            conversation_messages: checkpoint.conversation_messages,
            tool_outcome_history: checkpoint.tool_outcome_history,
            last_action: checkpoint.last_action,
            last_execution: checkpoint.last_execution,
            last_verification: checkpoint.last_verification,
            last_receipt: checkpoint.last_receipt,
            pending_provider_step: None,
        }),
        RuntimePendingTurnPhase::AwaitingHostExecution => Ok(PendingResumeState {
            round_index: checkpoint.round_index,
            max_tool_rounds: checkpoint.max_tool_rounds.max(1),
            observation: checkpoint.observation,
            system_prompt: checkpoint.system_prompt,
            conversation_messages: checkpoint.conversation_messages,
            tool_outcome_history: checkpoint.tool_outcome_history,
            last_action: checkpoint.last_action,
            last_execution: checkpoint.last_execution,
            last_verification: checkpoint.last_verification,
            last_receipt: checkpoint.last_receipt,
            pending_provider_step: Some(PendingProviderStep {
                provider_id: checkpoint
                    .pending_provider_id
                    .context("missing pending_provider_id")?,
                model: checkpoint.pending_model.context("missing pending_model")?,
                request: checkpoint
                    .pending_request
                    .context("missing pending_request")?,
                response: checkpoint
                    .pending_response
                    .context("missing pending_response")?,
                narrative: checkpoint
                    .pending_narrative
                    .context("missing pending_narrative")?,
                action: checkpoint
                    .pending_action
                    .context("missing pending_action")?,
                tool_call_id: checkpoint
                    .pending_tool_call_id
                    .unwrap_or_else(|| "host_action".into()),
            }),
        }),
        RuntimePendingTurnPhase::HostEffectsUncertain => {
            Err(anyhow!("host effects became uncertain"))
        }
    }
}

#[derive(Debug, Deserialize)]
struct ActionDirective {
    kind: String,
    title: Option<String>,
    x: Option<i32>,
    y: Option<i32>,
    button: Option<MouseButton>,
    from_x: Option<i32>,
    from_y: Option<i32>,
    to_x: Option<i32>,
    to_y: Option<i32>,
    text: Option<String>,
    submit: Option<bool>,
    chord: Option<String>,
    delta: Option<i32>,
    command: Option<String>,
    #[serde(default)]
    args: Option<Vec<String>>,
    signal: Option<String>,
    timeout_ms: Option<u64>,
}

#[derive(Debug)]
struct RuntimeLaneGuard {
    _session: OwnedSemaphorePermit,
    _global: Option<OwnedSemaphorePermit>,
}

async fn acquire_runtime_lanes(lanes: &RuntimeLanes) -> anyhow::Result<RuntimeLaneGuard> {
    acquire_runtime_lanes_with_timeout(lanes, LANE_ACQUIRE_TIMEOUT).await
}

async fn acquire_runtime_lanes_with_timeout(
    lanes: &RuntimeLanes,
    acquire_timeout: Duration,
) -> anyhow::Result<RuntimeLaneGuard> {
    let session_key = normalize_lane_key(&lanes.session, "runtime-session")?;
    let global_key = normalize_lane_key(&lanes.global, DEFAULT_GLOBAL_LANE)?;

    let session_permit = timeout(acquire_timeout, lookup_lane(&session_key).acquire_owned())
        .await
        .with_context(|| format!("timed out acquiring session lane {}", session_key))?
        .with_context(|| format!("acquire session lane {}", session_key))?;

    let global_permit = if global_key == session_key {
        None
    } else {
        Some(
            timeout(acquire_timeout, lookup_lane(&global_key).acquire_owned())
                .await
                .with_context(|| format!("timed out acquiring global lane {}", global_key))?
                .with_context(|| format!("acquire global lane {}", global_key))?,
        )
    };

    Ok(RuntimeLaneGuard {
        _session: session_permit,
        _global: global_permit,
    })
}

fn lookup_lane(key: &str) -> Arc<Semaphore> {
    let registry = LANE_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()));
    let mut map = registry.lock().expect("lane registry poisoned");
    map.entry(key.to_string())
        .or_insert_with(|| Arc::new(Semaphore::new(1)))
        .clone()
}

fn normalize_lane_key(raw: &str, fallback: &str) -> anyhow::Result<String> {
    let trimmed = raw.trim();
    let effective = if trimmed.is_empty() {
        fallback
    } else {
        trimmed
    };
    if effective.is_empty() {
        return Err(anyhow!("runtime lane key cannot be empty"));
    }
    Ok(effective.to_ascii_lowercase())
}

fn normalize_optional_field(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    })
}

fn extract_optional_string_alias(
    arguments: &serde_json::Map<String, serde_json::Value>,
    names: &[&str],
) -> anyhow::Result<Option<String>> {
    for name in names {
        let Some(value) = arguments.get(*name) else {
            continue;
        };
        return match value {
            serde_json::Value::Null => Ok(None),
            serde_json::Value::String(text) => Ok(normalize_optional_field(Some(text.clone()))),
            other => Err(anyhow!(
                "expected string or null for supervised_github_action field {name}, got {other}"
            )),
        };
    }

    Ok(None)
}

fn extract_optional_u64_alias(
    arguments: &serde_json::Map<String, serde_json::Value>,
    names: &[&str],
) -> anyhow::Result<Option<u64>> {
    for name in names {
        let Some(value) = arguments.get(*name) else {
            continue;
        };
        return match value {
            serde_json::Value::Null => Ok(None),
            serde_json::Value::Number(number) => number.as_u64().map(Some).ok_or_else(|| {
                anyhow!("expected unsigned integer for supervised_github_action field {name}")
            }),
            serde_json::Value::String(text) => {
                let trimmed = text.trim();
                if trimmed.is_empty() {
                    Ok(None)
                } else {
                    trimmed.parse::<u64>().with_context(|| {
                        format!(
                            "expected integer-like string for supervised_github_action field {name}"
                        )
                    }).map(Some)
                }
            }
            other => Err(anyhow!(
                "expected integer, integer-like string, or null for supervised_github_action field {name}, got {other}"
            )),
        };
    }

    Ok(None)
}

fn parse_supervised_github_action_kind(raw: &str) -> anyhow::Result<SupervisedGithubActionKind> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "comment_issue" | "issue_comment" | "comment_on_issue" => {
            Ok(SupervisedGithubActionKind::CommentIssue)
        }
        "comment_pull_request" | "comment_pr" | "pull_request_comment" | "pr_comment" => {
            Ok(SupervisedGithubActionKind::CommentPullRequest)
        }
        "assign_issue" | "issue_assign" => Ok(SupervisedGithubActionKind::AssignIssue),
        "assign_pull_request" | "assign_pr" | "pull_request_assign" | "pr_assign" => {
            Ok(SupervisedGithubActionKind::AssignPullRequest)
        }
        "close_issue" | "issue_close" => Ok(SupervisedGithubActionKind::CloseIssue),
        "close_pull_request" | "close_pr" | "pull_request_close" | "pr_close" => {
            Ok(SupervisedGithubActionKind::ClosePullRequest)
        }
        "reopen_issue" | "issue_reopen" => Ok(SupervisedGithubActionKind::ReopenIssue),
        "reopen_pull_request" | "reopen_pr" | "pull_request_reopen" | "pr_reopen" => {
            Ok(SupervisedGithubActionKind::ReopenPullRequest)
        }
        "label_issue" | "issue_label" => Ok(SupervisedGithubActionKind::LabelIssue),
        "label_pull_request" | "label_pr" | "pull_request_label" | "pr_label" => {
            Ok(SupervisedGithubActionKind::LabelPullRequest)
        }
        "remove_label_issue" | "unlabel_issue" | "issue_remove_label" | "issue_unlabel" => {
            Ok(SupervisedGithubActionKind::RemoveLabelIssue)
        }
        "remove_label_pull_request"
        | "remove_label_pr"
        | "pull_request_remove_label"
        | "pr_remove_label"
        | "unlabel_pull_request"
        | "unlabel_pr" => Ok(SupervisedGithubActionKind::RemoveLabelPullRequest),
        other => Err(anyhow!("unknown supervised_github_action kind {other}")),
    }
}

fn required_string_field(name: &str, value: Option<&str>) -> anyhow::Result<()> {
    if value.is_some() {
        Ok(())
    } else {
        Err(anyhow!(
            "missing required {name} for supervised_github_action"
        ))
    }
}

fn reject_present_string_field(
    name: &str,
    value: Option<&str>,
    kind: SupervisedGithubActionKind,
) -> anyhow::Result<()> {
    if value.is_some() {
        Err(anyhow!(
            "unexpected {name} for supervised_github_action kind {}",
            kind.as_label()
        ))
    } else {
        Ok(())
    }
}

fn reject_present_u64_field(
    name: &str,
    value: Option<u64>,
    kind: SupervisedGithubActionKind,
) -> anyhow::Result<()> {
    if value.is_some() {
        Err(anyhow!(
            "unexpected {name} for supervised_github_action kind {}",
            kind.as_label()
        ))
    } else {
        Ok(())
    }
}

fn runtime_placeholder_observation(summary: impl Into<String>) -> ObservationFrame {
    ObservationFrame {
        captured_at: Utc::now(),
        summary: summary.into(),
        screenshot_path: None,
        ocr_text: None,
        active_window: None,
        window_titles: Vec::new(),
        clipboard_text: None,
        structured_signals: Vec::new(),
    }
}

impl ActionDirective {
    fn into_action(self) -> anyhow::Result<ProposedAction> {
        let kind = self.kind.to_ascii_lowercase();
        match kind.as_str() {
            "focus_window" => Ok(ProposedAction::FocusWindow {
                title: required_string("title", self.title)?,
            }),
            "click" => Ok(ProposedAction::Click {
                x: required_i32("x", self.x)?,
                y: required_i32("y", self.y)?,
                button: self.button.unwrap_or(MouseButton::Left),
            }),
            "double_click" => Ok(ProposedAction::DoubleClick {
                x: required_i32("x", self.x)?,
                y: required_i32("y", self.y)?,
                button: self.button.unwrap_or(MouseButton::Left),
            }),
            "drag" => Ok(ProposedAction::Drag {
                from_x: required_i32("from_x", self.from_x)?,
                from_y: required_i32("from_y", self.from_y)?,
                to_x: required_i32("to_x", self.to_x)?,
                to_y: required_i32("to_y", self.to_y)?,
            }),
            "type_text" => Ok(ProposedAction::TypeText {
                text: required_string("text", self.text)?,
                submit: self.submit.unwrap_or(false),
            }),
            "hotkey" => Ok(ProposedAction::Hotkey {
                chord: required_string("chord", self.chord)?,
            }),
            "scroll" => Ok(ProposedAction::Scroll {
                delta: required_i32("delta", self.delta)?,
            }),
            "launch_process" => Ok(ProposedAction::LaunchProcess {
                command: required_string("command", self.command)?,
                args: self.args.unwrap_or_default(),
            }),
            "wait_for" => Ok(ProposedAction::WaitFor {
                signal: required_string("signal", self.signal)?,
                timeout_ms: required_u64("timeout_ms", self.timeout_ms)?,
            }),
            "capture_observation" => Ok(ProposedAction::CaptureObservation),
            _ => Err(anyhow!("unsupported host action kind {}", self.kind)),
        }
    }
}

impl<S, O> PersistentOrchestrator<S, O>
where
    S: splcw_memory::StateStore,
    O: splcw_memory::OffloadSink,
{
    pub async fn run_runtime_turn<A, H>(
        &self,
        llm: &ConfiguredLlmClient<A>,
        host: &H,
        options: RuntimeTurnOptions,
    ) -> anyhow::Result<RuntimeTurnOutcome>
    where
        A: AuthProfileStore + 'static,
        H: HostBody,
    {
        let acquired_lanes = options.lanes.clone();
        let _lane_guard = match acquired_lanes.as_ref() {
            Some(lanes) => Some(acquire_runtime_lanes(lanes).await?),
            None => None,
        };
        let mut session_journal = match options.session.clone() {
            Some(config) => {
                Some(RuntimeSessionJournal::open_with_stale_lock_cleanup(config).await?)
            }
            None => None,
        };
        let _session_guard = match session_journal.as_ref() {
            Some(journal) => Some(journal.acquire_write_lock().await?),
            None => None,
        };
        let pending_checkpoint = match session_journal.as_ref() {
            Some(journal) => journal.read_pending_turn_checkpoint().await?,
            None => None,
        };
        if let Some(journal) = session_journal.as_mut() {
            let thread_id = pending_checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.thread_id.as_str())
                .unwrap_or_else(|| options.thread_id.as_deref().unwrap_or("main"));
            let thread_label = options.thread_label.as_deref();
            journal
                .switch_foreground_thread(thread_id, thread_label)
                .await?;
        }
        let post_compaction_refresh = match session_journal.as_mut() {
            Some(journal) => journal.take_post_compaction_refresh().await?,
            None => None,
        };
        let thread_context = session_journal
            .as_ref()
            .and_then(|journal| journal.build_thread_prompt_context());
        let turn_history_context = match session_journal.as_ref() {
            Some(journal) => journal.build_turn_history_prompt_context(5).await?,
            None => None,
        };

        let state = self
            .hydrate(options.receipt_context_limit)
            .await?
            .context("cannot run runtime turn without a current plan snapshot")?;
        let gap_task_root: Option<std::path::PathBuf> =
            session_journal.as_ref().map(|j| j.session_root());
        let session_event_id = options
            .session
            .as_ref()
            .map(|config| config.session_id.as_str())
            .unwrap_or("runtime")
            .to_string();
        let current_plan_id = state.current_snapshot.plan.id;
        let plan_id = current_plan_id.to_string();
        if let Some(journal) = session_journal.as_ref() {
            if journal.repaired() {
                journal
                    .append_event(&RuntimeSessionEvent::session_repaired(
                        &session_event_id,
                        "repaired session transcript or turn-log continuity before turn start",
                    ))
                    .await?;
            }
            journal
                .append_event(&RuntimeSessionEvent::turn_started(
                    &session_event_id,
                    plan_id.clone(),
                    format!(
                        "objective={} module={}",
                        state.plan.objective, state.plan.active_module
                    ),
                ))
                .await?;
        }
        let resume_state = match pending_checkpoint {
            Some(checkpoint) if checkpoint.plan_id != current_plan_id => {
                clear_pending_turn_checkpoint_if_present(session_journal.as_ref()).await?;
                let gap = self
                    .surface_runtime_gap(
                        CapabilityGapDirective {
                            title: "Pending runtime turn targeted a different plan snapshot".into(),
                            permanent_fix_target:
                                "Reset in-flight runtime checkpoints whenever the active plan changes"
                                    .into(),
                            notes: vec![format!(
                                "checkpoint plan_id={} current plan_id={}",
                                checkpoint.plan_id, current_plan_id
                            )],
                        },
                        "runtime-session",
                        &checkpoint.observation,
                        "",
                        gap_task_root.as_deref(),
                    )
                    .await?;
                if let Some(journal) = session_journal.as_ref() {
                    journal
                        .append_event(&RuntimeSessionEvent::turn_gap(
                            &session_event_id,
                            plan_id.clone(),
                            "runtime-session",
                            options.model.clone().unwrap_or_else(|| "runtime".into()),
                            gap.title.clone(),
                        ))
                        .await?;
                }
                if let Some(journal) = session_journal.as_mut() {
                    journal
                        .record_foreground_turn(format!(
                            "gap: {} | {}",
                            gap.title, gap.permanent_fix_target
                        ))
                        .await?;
                }
                return Ok(RuntimeTurnOutcome {
                    provider_id: "runtime-session".into(),
                    model: options.model.clone().unwrap_or_else(|| "runtime".into()),
                    observation: checkpoint.observation,
                    response: ChatResponse {
                        provider_id: "runtime-session".into(),
                        model: options.model.clone().unwrap_or_else(|| "runtime".into()),
                        content: vec![ContentBlock::Text {
                            text: "[pending turn mismatch] plan changed before safe resume".into(),
                        }],
                    },
                    narrative: "[pending turn mismatch] plan changed before safe resume".into(),
                    tool_outcome_history: checkpoint.tool_outcome_history,
                    proposed_action: checkpoint.last_action,
                    execution: checkpoint.last_execution,
                    verification: checkpoint.last_verification,
                    receipt: checkpoint.last_receipt,
                    github_action_request: None,
                    surfaced_gap: Some(gap),
                    acquired_lanes,
                });
            }
            Some(checkpoint)
                if checkpoint.phase == RuntimePendingTurnPhase::HostEffectsUncertain =>
            {
                clear_pending_turn_checkpoint_if_present(session_journal.as_ref()).await?;
                let action_summary = checkpoint
                    .pending_action
                    .as_ref()
                    .map(describe_action)
                    .unwrap_or_else(|| "the pending host action".into());
                let gap = self
                    .surface_runtime_gap(
                        CapabilityGapDirective {
                            title: "Runtime turn reopened after host effects became uncertain".into(),
                            permanent_fix_target:
                                "Resume only from pre-action checkpoints or add a deterministic host-effect recovery path"
                                    .into(),
                            notes: vec![format!(
                                "checkpoint {:?} became unsafe to replay after {}",
                                checkpoint.checkpoint_id, action_summary
                            )],
                        },
                        "runtime-session",
                        &checkpoint.observation,
                        "",
                        gap_task_root.as_deref(),
                    )
                    .await?;
                if let Some(journal) = session_journal.as_ref() {
                    journal
                        .append_event(&RuntimeSessionEvent::turn_gap(
                            &session_event_id,
                            plan_id.clone(),
                            "runtime-session",
                            options.model.clone().unwrap_or_else(|| "runtime".into()),
                            gap.title.clone(),
                        ))
                        .await?;
                }
                if let Some(journal) = session_journal.as_mut() {
                    journal
                        .record_foreground_turn(format!(
                            "gap: {} | {}",
                            gap.title, gap.permanent_fix_target
                        ))
                        .await?;
                }
                return Ok(RuntimeTurnOutcome {
                    provider_id: "runtime-session".into(),
                    model: options.model.clone().unwrap_or_else(|| "runtime".into()),
                    observation: checkpoint.observation,
                    response: ChatResponse {
                        provider_id: "runtime-session".into(),
                        model: options.model.clone().unwrap_or_else(|| "runtime".into()),
                        content: vec![ContentBlock::Text {
                            text: "[pending turn blocked] host effects were already in-flight"
                                .into(),
                        }],
                    },
                    narrative: "[pending turn blocked] host effects were already in-flight".into(),
                    tool_outcome_history: checkpoint.tool_outcome_history,
                    proposed_action: checkpoint.last_action,
                    execution: checkpoint.last_execution,
                    verification: checkpoint.last_verification,
                    receipt: checkpoint.last_receipt,
                    github_action_request: None,
                    surfaced_gap: Some(gap),
                    acquired_lanes,
                });
            }
            Some(checkpoint) => match build_pending_resume_state(checkpoint.clone()) {
                Ok(state) => Some(state),
                Err(error) => {
                    clear_pending_turn_checkpoint_if_present(session_journal.as_ref()).await?;
                    let gap = self
                        .surface_runtime_gap(
                            CapabilityGapDirective {
                                title: "Pending runtime turn could not be resumed safely".into(),
                                permanent_fix_target:
                                    "Persist complete pre-action runtime checkpoint data before resuming"
                                        .into(),
                                notes: vec![format!("{error:#}")],
                            },
                            "runtime-session",
                            &checkpoint.observation,
                            "",
                            gap_task_root.as_deref(),
                        )
                        .await?;
                    if let Some(journal) = session_journal.as_ref() {
                        journal
                            .append_event(&RuntimeSessionEvent::turn_gap(
                                &session_event_id,
                                plan_id.clone(),
                                "runtime-session",
                                options.model.clone().unwrap_or_else(|| "runtime".into()),
                                gap.title.clone(),
                            ))
                            .await?;
                    }
                    if let Some(journal) = session_journal.as_mut() {
                        journal
                            .record_foreground_turn(format!(
                                "gap: {} | {}",
                                gap.title, gap.permanent_fix_target
                            ))
                            .await?;
                    }
                    return Ok(RuntimeTurnOutcome {
                        provider_id: "runtime-session".into(),
                        model: options.model.clone().unwrap_or_else(|| "runtime".into()),
                        observation: checkpoint.observation,
                        response: ChatResponse {
                            provider_id: "runtime-session".into(),
                            model: options.model.clone().unwrap_or_else(|| "runtime".into()),
                            content: vec![ContentBlock::Text {
                                text: "[pending turn blocked] checkpoint data was incomplete"
                                    .into(),
                            }],
                        },
                        narrative: "[pending turn blocked] checkpoint data was incomplete".into(),
                        tool_outcome_history: checkpoint.tool_outcome_history,
                        proposed_action: checkpoint.last_action,
                        execution: checkpoint.last_execution,
                        verification: checkpoint.last_verification,
                        receipt: checkpoint.last_receipt,
                        github_action_request: None,
                        surfaced_gap: Some(gap),
                        acquired_lanes,
                    });
                }
            },
            None => None,
        };
        let (
            observation,
            system_prompt,
            mut conversation_messages,
            max_tool_rounds,
            mut tool_outcome_history,
            mut last_action,
            mut last_execution,
            mut last_verification,
            mut last_receipt,
            resume_round_index,
            mut resumed_provider_step,
        ) = if let Some(resume_state) = resume_state {
            (
                resume_state.observation,
                resume_state.system_prompt,
                resume_state.conversation_messages,
                resume_state.max_tool_rounds,
                resume_state.tool_outcome_history,
                resume_state.last_action,
                resume_state.last_execution,
                resume_state.last_verification,
                resume_state.last_receipt,
                resume_state.round_index,
                resume_state.pending_provider_step,
            )
        } else {
            let observation = match timeout(options.host_observe_timeout, host.observe()).await {
                Ok(Ok(observation)) => observation,
                Ok(Err(error)) => return Err(error).context("observe host before runtime turn"),
                Err(_) => {
                    let gap = self
                        .surface_runtime_gap(
                            CapabilityGapDirective {
                                title: "Host observation timed out before the next bounded step"
                                    .into(),
                                permanent_fix_target:
                                    "Add a reusable host observation timeout and diagnosis path"
                                        .into(),
                                notes: vec![format!(
                                    "observe timeout exceeded: {:?}",
                                    options.host_observe_timeout
                                )],
                            },
                            "runtime-host",
                            &runtime_placeholder_observation(
                                "host observation timed out before runtime turn",
                            ),
                            "",
                            gap_task_root.as_deref(),
                        )
                        .await?;
                    if let Some(journal) = session_journal.as_ref() {
                        journal
                            .append_event(&RuntimeSessionEvent::turn_gap(
                                &session_event_id,
                                plan_id.clone(),
                                "runtime-host",
                                options.model.clone().unwrap_or_else(|| "runtime".into()),
                                gap.title.clone(),
                            ))
                            .await?;
                    }
                    if let Some(journal) = session_journal.as_mut() {
                        journal
                            .record_foreground_turn(format!(
                                "gap: {} | {}",
                                gap.title, gap.permanent_fix_target
                            ))
                            .await?;
                    }
                    return Ok(RuntimeTurnOutcome {
                        provider_id: "runtime-host".into(),
                        model: options.model.clone().unwrap_or_else(|| "runtime".into()),
                        observation: runtime_placeholder_observation(
                            "host observation timed out before runtime turn",
                        ),
                        response: ChatResponse {
                            provider_id: "runtime-host".into(),
                            model: options.model.clone().unwrap_or_else(|| "runtime".into()),
                            content: vec![ContentBlock::Text {
                                text: format!(
                                    "[host observation timeout] {:?}",
                                    options.host_observe_timeout
                                ),
                            }],
                        },
                        narrative: format!(
                            "[host observation timeout] {:?}",
                            options.host_observe_timeout
                        ),
                        tool_outcome_history: Vec::new(),
                        proposed_action: None,
                        execution: None,
                        verification: None,
                        receipt: None,
                        github_action_request: None,
                        surfaced_gap: Some(gap),
                        acquired_lanes,
                    });
                }
            };
            // Load always-on operating memory from os.md + memory.md (non-fatal if missing).
            let operating_memory: Option<String> = {
                let memory_dir = session_journal
                    .as_ref()
                    .and_then(|j| {
                        crate::compaction_publisher::find_repo_root(&j.session_root()).ok()
                    })
                    .map(|repo| repo.join("artifacts/ultimentality-pilot/memory"));
                if let Some(dir) = memory_dir {
                    let os_content = tokio::fs::read_to_string(dir.join("os.md")).await.unwrap_or_default();
                    let mem_content = tokio::fs::read_to_string(dir.join("memory.md")).await.unwrap_or_default();
                    let combined = format!("{}\n\n---\n\n{}", os_content.trim(), mem_content.trim());
                    if combined.trim() == "---" { None } else { Some(combined) }
                } else {
                    None
                }
            };
            (
                observation.clone(),
                runtime_system_prompt(
                    post_compaction_refresh.as_deref(),
                    thread_context.as_deref(),
                    turn_history_context.as_deref(),
                    operating_memory.as_deref(),
                ),
                build_initial_runtime_messages(
                    &state,
                    &observation,
                    options.external_context.as_deref(),
                ),
                options.max_tool_rounds.max(1),
                Vec::new(),
                None,
                None,
                None,
                None,
                0,
                None,
            )
        };
        let mut final_provider_id = String::new();
        let mut final_model = options.model.clone().unwrap_or_default();
        let mut final_response: Option<ChatResponse> = None;
        let mut final_narrative = String::new();

        for round in resume_round_index..max_tool_rounds {
            let round_number = round + 1;
            append_session_event_if_present(
                session_journal.as_ref(),
                RuntimeSessionEvent::turn_round_started(
                    &session_event_id,
                    plan_id.clone(),
                    round_number,
                    format!(
                        "requesting next bounded provider step | prior_tool_outcomes={}",
                        tool_outcome_history.len()
                    ),
                ),
            )
            .await?;
            let resumed_step_for_round = if round == resume_round_index {
                resumed_provider_step.take()
            } else {
                None
            };
            if resumed_step_for_round.is_none() {
                let checkpoint = build_pending_turn_checkpoint(
                    current_plan_id,
                    session_journal
                        .as_ref()
                        .map(|journal| journal.foreground_thread_id().to_string())
                        .unwrap_or_else(|| {
                            options.thread_id.clone().unwrap_or_else(|| "main".into())
                        }),
                    round,
                    max_tool_rounds,
                    RuntimePendingTurnPhase::AwaitingProvider,
                    &system_prompt,
                    &conversation_messages,
                    last_verification.as_ref().unwrap_or(&observation),
                    &tool_outcome_history,
                    last_action.as_ref(),
                    last_execution.as_ref(),
                    last_verification.as_ref(),
                    last_receipt.as_ref(),
                    None,
                );
                write_pending_turn_checkpoint_if_present(session_journal.as_ref(), &checkpoint)
                    .await?;
            }
            let (request, provider_id, model, response, narrative, resumed_action, tool_call_id) =
                if let Some(step) = resumed_step_for_round {
                    (
                        step.request,
                        step.provider_id,
                        step.model,
                        step.response,
                        step.narrative,
                        Some(step.action),
                        step.tool_call_id,
                    )
                } else {
                    let provider_call = match llm
                        .chat_with_controller_using(|resolved| {
                            let model = options
                                .model
                                .clone()
                                .unwrap_or_else(|| resolved.descriptor.default_model.clone());
                            build_runtime_chat_request(
                                model,
                                system_prompt.clone(),
                                conversation_messages.clone(),
                            )
                        })
                        .await
                    {
                        Ok(call) => call,
                        Err(error) => {
                            let gap = self
                                .surface_runtime_gap(
                                    CapabilityGapDirective {
                                        title: "Runtime model call failed before the next bounded step".into(),
                                        permanent_fix_target:
                                            "Stabilize provider transport, auth rotation, or runtime model availability"
                                                .into(),
                                        notes: vec![format!("{error:#}")],
                                    },
                                    "runtime-provider",
                                    last_verification.as_ref().unwrap_or(&observation),
                                    &final_narrative,
                                    gap_task_root.as_deref(),
                                )
                                .await?;
                            if let Some(journal) = session_journal.as_ref() {
                                journal
                                    .append_event(&RuntimeSessionEvent::turn_gap(
                                        &session_event_id,
                                        plan_id.clone(),
                                        "runtime-provider",
                                        final_model.clone(),
                                        gap.title.clone(),
                                    ))
                                    .await?;
                            }
                            if let Some(journal) = session_journal.as_mut() {
                                journal
                                    .record_foreground_turn(format!(
                                        "gap: {} | {}",
                                        gap.title, gap.permanent_fix_target
                                    ))
                                    .await?;
                            }
                            let retry_checkpoint = build_pending_turn_checkpoint(
                                current_plan_id,
                                session_journal
                                    .as_ref()
                                    .map(|journal| journal.foreground_thread_id().to_string())
                                    .unwrap_or_else(|| {
                                        options.thread_id.clone().unwrap_or_else(|| "main".into())
                                    }),
                                round,
                                max_tool_rounds,
                                RuntimePendingTurnPhase::AwaitingProvider,
                                &system_prompt,
                                &conversation_messages,
                                last_verification.as_ref().unwrap_or(&observation),
                                &tool_outcome_history,
                                last_action.as_ref(),
                                last_execution.as_ref(),
                                last_verification.as_ref(),
                                last_receipt.as_ref(),
                                None,
                            );
                            write_pending_turn_checkpoint_if_present(
                                session_journal.as_ref(),
                                &retry_checkpoint,
                            )
                            .await?;
                            return Ok(RuntimeTurnOutcome {
                                provider_id: "runtime-provider".into(),
                                model: final_model.clone(),
                                observation,
                                response: ChatResponse {
                                    provider_id: "runtime-provider".into(),
                                    model: final_model.clone(),
                                    content: vec![ContentBlock::Text {
                                        text: format!("[provider failure] {error:#}"),
                                    }],
                                },
                                narrative: format!("[provider failure] {error:#}"),
                                tool_outcome_history,
                                proposed_action: last_action,
                                execution: last_execution,
                                verification: last_verification,
                                receipt: last_receipt,
                                github_action_request: None,
                                surfaced_gap: Some(gap),
                                acquired_lanes,
                            });
                        }
                    };
                    let model = provider_call.response.model.clone();
                    let response = provider_call.response;
                    let tool_call_id = first_tool_call_id(&response, HOST_ACTION_TOOL)
                        .unwrap_or_else(|| "host_action".into());
                    (
                        provider_call.request,
                        provider_call.descriptor.id.clone(),
                        model,
                        response.clone(),
                        extract_narrative(&response),
                        None,
                        tool_call_id,
                    )
                };
            final_provider_id = provider_id.clone();
            final_model = model.clone();
            final_response = Some(response.clone());
            final_narrative = narrative.clone();

            if let Some(gap_call) = first_tool_arguments(&response, CAPABILITY_GAP_TOOL) {
                let gap = self
                    .surface_runtime_gap(
                        parse_gap_directive(gap_call)?,
                        &provider_id,
                        &observation,
                        &narrative,
                        gap_task_root.as_deref(),
                    )
                    .await?;
                append_session_event_if_present(
                    session_journal.as_ref(),
                    RuntimeSessionEvent::turn_round_completed(
                        &session_event_id,
                        plan_id.clone(),
                        provider_id.clone(),
                        model.clone(),
                        round_number,
                        format!("provider surfaced capability gap: {}", gap.title),
                    ),
                )
                .await?;
                if let Some(journal) = session_journal.as_ref() {
                    journal
                        .append_event(&RuntimeSessionEvent::turn_gap(
                            &session_event_id,
                            plan_id.clone(),
                            provider_id.clone(),
                            model.clone(),
                            gap.title.clone(),
                        ))
                        .await?;
                    let turn_record = RuntimeTurnRecord {
                        recorded_at: Utc::now(),
                        turn_id: Uuid::new_v4(),
                        thread_id: journal.foreground_thread_id().to_string(),
                        provider_id: provider_id.clone(),
                        model: model.clone(),
                        request: request.clone(),
                        response: response.clone(),
                        narrative: narrative.clone(),
                        tool_outcome: None,
                        surfaced_gap: Some(RuntimeGapRecord {
                            gap_id: gap.id,
                            title: gap.title.clone(),
                            permanent_fix_target: gap.permanent_fix_target.clone(),
                        }),
                    };
                    journal.append_turn_record(&turn_record).await?;
                }
                clear_pending_turn_checkpoint_if_present(session_journal.as_ref()).await?;
                return Ok(RuntimeTurnOutcome {
                    provider_id,
                    model,
                    observation,
                    response,
                    narrative,
                    tool_outcome_history,
                    proposed_action: last_action,
                    execution: last_execution,
                    verification: last_verification,
                    receipt: last_receipt,
                    github_action_request: None,
                    surfaced_gap: Some(gap),
                    acquired_lanes,
                });
            }

            let github_action_calls = all_tool_arguments(&response, SUPERVISED_GITHUB_ACTION_TOOL);
            if github_action_calls.len() > 1 {
                let gap = self
                    .surface_runtime_gap(
                        CapabilityGapDirective {
                            title:
                                "Provider emitted multiple supervised GitHub requests in one bounded turn"
                                    .into(),
                            permanent_fix_target:
                                "Constrain runtime GitHub planning to one operator-approved mutation per turn"
                                    .into(),
                            notes: vec![format!(
                                "supervised_github_action count: {}",
                                github_action_calls.len()
                            )],
                        },
                        &provider_id,
                        &observation,
                        &narrative,
                        gap_task_root.as_deref(),
                    )
                    .await?;
                append_session_event_if_present(
                    session_journal.as_ref(),
                    RuntimeSessionEvent::turn_round_completed(
                        &session_event_id,
                        plan_id.clone(),
                        provider_id.clone(),
                        model.clone(),
                        round_number,
                        format!(
                            "provider emitted multiple supervised GitHub requests | count={}",
                            github_action_calls.len()
                        ),
                    ),
                )
                .await?;
                if let Some(journal) = session_journal.as_ref() {
                    journal
                        .append_event(&RuntimeSessionEvent::turn_gap(
                            &session_event_id,
                            plan_id.clone(),
                            provider_id.clone(),
                            model.clone(),
                            gap.title.clone(),
                        ))
                        .await?;
                    append_gap_turn_record(
                        journal,
                        &provider_id,
                        &model,
                        &request,
                        &response,
                        &narrative,
                        &gap,
                    )
                    .await?;
                }
                clear_pending_turn_checkpoint_if_present(session_journal.as_ref()).await?;
                return Ok(RuntimeTurnOutcome {
                    provider_id,
                    model,
                    observation,
                    response,
                    narrative,
                    tool_outcome_history,
                    proposed_action: last_action,
                    execution: last_execution,
                    verification: last_verification,
                    receipt: last_receipt,
                    github_action_request: None,
                    surfaced_gap: Some(gap),
                    acquired_lanes,
                });
            }

            let action_calls = all_tool_arguments(&response, HOST_ACTION_TOOL);
            if !github_action_calls.is_empty() && !action_calls.is_empty() {
                let gap = self
                    .surface_runtime_gap(
                        CapabilityGapDirective {
                            title:
                                "Provider mixed GitHub mutation planning with a host action in one bounded turn"
                                    .into(),
                            permanent_fix_target:
                                "Separate desktop actions from supervised GitHub mutations into distinct bounded turns"
                                    .into(),
                            notes: vec![format!(
                                "host_action count={} | supervised_github_action count={}",
                                action_calls.len(),
                                github_action_calls.len()
                            )],
                        },
                        &provider_id,
                        &observation,
                        &narrative,
                        gap_task_root.as_deref(),
                    )
                    .await?;
                append_session_event_if_present(
                    session_journal.as_ref(),
                    RuntimeSessionEvent::turn_round_completed(
                        &session_event_id,
                        plan_id.clone(),
                        provider_id.clone(),
                        model.clone(),
                        round_number,
                        "provider mixed host_action with supervised_github_action",
                    ),
                )
                .await?;
                if let Some(journal) = session_journal.as_ref() {
                    journal
                        .append_event(&RuntimeSessionEvent::turn_gap(
                            &session_event_id,
                            plan_id.clone(),
                            provider_id.clone(),
                            model.clone(),
                            gap.title.clone(),
                        ))
                        .await?;
                    append_gap_turn_record(
                        journal,
                        &provider_id,
                        &model,
                        &request,
                        &response,
                        &narrative,
                        &gap,
                    )
                    .await?;
                }
                clear_pending_turn_checkpoint_if_present(session_journal.as_ref()).await?;
                return Ok(RuntimeTurnOutcome {
                    provider_id,
                    model,
                    observation,
                    response,
                    narrative,
                    tool_outcome_history,
                    proposed_action: last_action,
                    execution: last_execution,
                    verification: last_verification,
                    receipt: last_receipt,
                    github_action_request: None,
                    surfaced_gap: Some(gap),
                    acquired_lanes,
                });
            }

            if let Some(github_action_value) = github_action_calls.first() {
                let github_action_request = match parse_supervised_github_action(
                    github_action_value,
                ) {
                    Ok(request) => request,
                    Err(error) => {
                        let gap = self
                            .surface_runtime_gap(
                                CapabilityGapDirective {
                                    title: "Provider emitted an invalid supervised GitHub action"
                                        .into(),
                                    permanent_fix_target:
                                        "Align provider GitHub action arguments with the supervised GitHub schema"
                                            .into(),
                                    notes: vec![error.to_string()],
                                },
                                &provider_id,
                                &observation,
                                &narrative,
                                gap_task_root.as_deref(),
                            )
                            .await?;
                        append_session_event_if_present(
                            session_journal.as_ref(),
                            RuntimeSessionEvent::turn_round_completed(
                                &session_event_id,
                                plan_id.clone(),
                                provider_id.clone(),
                                model.clone(),
                                round_number,
                                "provider emitted invalid supervised GitHub action payload",
                            ),
                        )
                        .await?;
                        if let Some(journal) = session_journal.as_ref() {
                            journal
                                .append_event(&RuntimeSessionEvent::turn_gap(
                                    &session_event_id,
                                    plan_id.clone(),
                                    provider_id.clone(),
                                    model.clone(),
                                    gap.title.clone(),
                                ))
                                .await?;
                            append_gap_turn_record(
                                journal,
                                &provider_id,
                                &model,
                                &request,
                                &response,
                                &narrative,
                                &gap,
                            )
                            .await?;
                        }
                        clear_pending_turn_checkpoint_if_present(session_journal.as_ref()).await?;
                        return Ok(RuntimeTurnOutcome {
                            provider_id,
                            model,
                            observation,
                            response,
                            narrative,
                            tool_outcome_history,
                            proposed_action: last_action,
                            execution: last_execution,
                            verification: last_verification,
                            receipt: last_receipt,
                            github_action_request: None,
                            surfaced_gap: Some(gap),
                            acquired_lanes,
                        });
                    }
                };

                let completion_summary = format!(
                    "supervised GitHub request queued: {}",
                    github_action_request.summary()
                );
                append_session_event_if_present(
                    session_journal.as_ref(),
                    RuntimeSessionEvent::turn_round_completed(
                        &session_event_id,
                        plan_id.clone(),
                        provider_id.clone(),
                        model.clone(),
                        round_number,
                        completion_summary.clone(),
                    ),
                )
                .await?;
                if let Some(journal) = session_journal.as_ref() {
                    append_terminal_turn_record(
                        journal,
                        &provider_id,
                        &model,
                        &request,
                        &response,
                        &narrative,
                    )
                    .await?;
                    journal
                        .append_event(&RuntimeSessionEvent::turn_completed(
                            &session_event_id,
                            plan_id.clone(),
                            provider_id.clone(),
                            model.clone(),
                            completion_summary.clone(),
                        ))
                        .await?;
                }
                if let Some(journal) = session_journal.as_mut() {
                    journal
                        .record_foreground_turn(completion_summary.clone())
                        .await?;
                }
                clear_pending_turn_checkpoint_if_present(session_journal.as_ref()).await?;
                return Ok(RuntimeTurnOutcome {
                    provider_id,
                    model,
                    observation,
                    response,
                    narrative,
                    tool_outcome_history,
                    proposed_action: last_action,
                    execution: last_execution,
                    verification: last_verification,
                    receipt: last_receipt,
                    github_action_request: Some(github_action_request),
                    surfaced_gap: None,
                    acquired_lanes,
                });
            }

            let Some(action_value) = action_calls.first() else {
                if !tool_outcome_history.is_empty() || !narrative.trim().is_empty() {
                    let completion_summary = last_receipt
                        .as_ref()
                        .map(|receipt| receipt.changed.clone())
                        .unwrap_or_else(|| {
                            if narrative.trim().is_empty() {
                                "runtime turn completed without a narrative".into()
                            } else {
                                narrative.clone()
                            }
                        });
                    append_session_event_if_present(
                        session_journal.as_ref(),
                        RuntimeSessionEvent::turn_round_completed(
                            &session_event_id,
                            plan_id.clone(),
                            provider_id.clone(),
                            model.clone(),
                            round_number,
                            format!(
                                "provider settled without another host action: {} | last_receipt={}",
                                narrative,
                                completion_summary
                            ),
                        ),
                    )
                    .await?;
                    if let Some(journal) = session_journal.as_ref() {
                        append_terminal_turn_record(
                            journal,
                            &provider_id,
                            &model,
                            &request,
                            &response,
                            &narrative,
                        )
                        .await?;
                        journal
                            .append_event(&RuntimeSessionEvent::turn_completed(
                                &session_event_id,
                                plan_id.clone(),
                                provider_id.clone(),
                                model.clone(),
                                completion_summary.clone(),
                            ))
                            .await?;
                    }
                    if let Some(journal) = session_journal.as_mut() {
                        let thread_summary = last_receipt
                            .as_ref()
                            .map(|receipt| receipt.changed.clone())
                            .unwrap_or_else(|| narrative.clone());
                        journal.record_foreground_turn(thread_summary).await?;
                    }
                    clear_pending_turn_checkpoint_if_present(session_journal.as_ref()).await?;
                    return Ok(RuntimeTurnOutcome {
                        provider_id,
                        model,
                        observation,
                        response,
                        narrative,
                        tool_outcome_history,
                        proposed_action: last_action,
                        execution: last_execution,
                        verification: last_verification,
                        receipt: last_receipt,
                        github_action_request: None,
                        surfaced_gap: None,
                        acquired_lanes,
                    });
                }

                let gap = self
                    .surface_runtime_gap(
                        CapabilityGapDirective {
                            title: "Provider response yielded no runnable host action".into(),
                            permanent_fix_target:
                                "Tighten the runtime action protocol or adapter translation".into(),
                            notes: vec![
                                "The provider returned neither host_action nor capability_gap."
                                    .into(),
                            ],
                        },
                        &provider_id,
                        &observation,
                        &narrative,
                        gap_task_root.as_deref(),
                    )
                    .await?;
                append_session_event_if_present(
                    session_journal.as_ref(),
                    RuntimeSessionEvent::turn_round_completed(
                        &session_event_id,
                        plan_id.clone(),
                        provider_id.clone(),
                        model.clone(),
                        round_number,
                        "provider yielded no runnable host action",
                    ),
                )
                .await?;
                if let Some(journal) = session_journal.as_ref() {
                    journal
                        .append_event(&RuntimeSessionEvent::turn_gap(
                            &session_event_id,
                            plan_id.clone(),
                            provider_id.clone(),
                            model.clone(),
                            gap.title.clone(),
                        ))
                        .await?;
                    let turn_record = RuntimeTurnRecord {
                        recorded_at: Utc::now(),
                        turn_id: Uuid::new_v4(),
                        thread_id: journal.foreground_thread_id().to_string(),
                        provider_id: provider_id.clone(),
                        model: model.clone(),
                        request: request.clone(),
                        response: response.clone(),
                        narrative: narrative.clone(),
                        tool_outcome: None,
                        surfaced_gap: Some(RuntimeGapRecord {
                            gap_id: gap.id,
                            title: gap.title.clone(),
                            permanent_fix_target: gap.permanent_fix_target.clone(),
                        }),
                    };
                    journal.append_turn_record(&turn_record).await?;
                }
                clear_pending_turn_checkpoint_if_present(session_journal.as_ref()).await?;
                return Ok(RuntimeTurnOutcome {
                    provider_id,
                    model,
                    observation,
                    response,
                    narrative,
                    tool_outcome_history,
                    proposed_action: last_action,
                    execution: last_execution,
                    verification: last_verification,
                    receipt: last_receipt,
                    github_action_request: None,
                    surfaced_gap: Some(gap),
                    acquired_lanes,
                });
            };

            if action_calls.len() > 1 {
                let gap = self
                    .surface_runtime_gap(
                        CapabilityGapDirective {
                            title: "Provider emitted multiple host actions in one bounded turn"
                                .into(),
                            permanent_fix_target:
                                "Constrain planner outputs to a single verified action per turn"
                                    .into(),
                            notes: vec![format!("host_action count: {}", action_calls.len())],
                        },
                        &provider_id,
                        &observation,
                        &narrative,
                        gap_task_root.as_deref(),
                    )
                    .await?;
                append_session_event_if_present(
                    session_journal.as_ref(),
                    RuntimeSessionEvent::turn_round_completed(
                        &session_event_id,
                        plan_id.clone(),
                        provider_id.clone(),
                        model.clone(),
                        round_number,
                        format!(
                            "provider emitted multiple host actions | count={}",
                            action_calls.len()
                        ),
                    ),
                )
                .await?;
                if let Some(journal) = session_journal.as_ref() {
                    journal
                        .append_event(&RuntimeSessionEvent::turn_gap(
                            &session_event_id,
                            plan_id.clone(),
                            provider_id.clone(),
                            model.clone(),
                            gap.title.clone(),
                        ))
                        .await?;
                }
                clear_pending_turn_checkpoint_if_present(session_journal.as_ref()).await?;
                return Ok(RuntimeTurnOutcome {
                    provider_id,
                    model,
                    observation,
                    response,
                    narrative,
                    tool_outcome_history,
                    proposed_action: last_action,
                    execution: last_execution,
                    verification: last_verification,
                    receipt: last_receipt,
                    github_action_request: None,
                    surfaced_gap: Some(gap),
                    acquired_lanes,
                });
            }

            let action = match resumed_action {
                Some(action) => action,
                None => match parse_host_action(action_value) {
                    Ok(action) => action,
                    Err(error) => {
                        let gap = self
                            .surface_runtime_gap(
                                CapabilityGapDirective {
                                    title: "Provider emitted an invalid host action".into(),
                                    permanent_fix_target:
                                        "Align provider action arguments with the host action schema"
                                            .into(),
                                    notes: vec![error.to_string()],
                                },
                                &provider_id,
                                &observation,
                                &narrative,
                                gap_task_root.as_deref(),
                            )
                            .await?;
                        append_session_event_if_present(
                            session_journal.as_ref(),
                            RuntimeSessionEvent::turn_round_completed(
                                &session_event_id,
                                plan_id.clone(),
                                provider_id.clone(),
                                model.clone(),
                                round_number,
                                "provider emitted invalid host action payload",
                            ),
                        )
                        .await?;
                        if let Some(journal) = session_journal.as_ref() {
                            journal
                                .append_event(&RuntimeSessionEvent::turn_gap(
                                    &session_event_id,
                                    plan_id.clone(),
                                    provider_id.clone(),
                                    model.clone(),
                                    gap.title.clone(),
                                ))
                                .await?;
                            let turn_record = RuntimeTurnRecord {
                                recorded_at: Utc::now(),
                                turn_id: Uuid::new_v4(),
                                thread_id: journal.foreground_thread_id().to_string(),
                                provider_id: provider_id.clone(),
                                model: model.clone(),
                                request: request.clone(),
                                response: response.clone(),
                                narrative: narrative.clone(),
                                tool_outcome: None,
                                surfaced_gap: Some(RuntimeGapRecord {
                                    gap_id: gap.id,
                                    title: gap.title.clone(),
                                    permanent_fix_target: gap.permanent_fix_target.clone(),
                                }),
                            };
                            journal.append_turn_record(&turn_record).await?;
                        }
                        clear_pending_turn_checkpoint_if_present(session_journal.as_ref()).await?;
                        return Ok(RuntimeTurnOutcome {
                            provider_id,
                            model,
                            observation,
                            response,
                            narrative,
                            tool_outcome_history,
                            proposed_action: last_action,
                            execution: last_execution,
                            verification: last_verification,
                            receipt: last_receipt,
                            github_action_request: None,
                            surfaced_gap: Some(gap),
                            acquired_lanes,
                        });
                    }
                },
            };
            append_session_event_if_present(
                session_journal.as_ref(),
                RuntimeSessionEvent::turn_action_selected(
                    &session_event_id,
                    plan_id.clone(),
                    provider_id.clone(),
                    model.clone(),
                    round_number,
                    format!("selected {}", describe_action(&action)),
                ),
            )
            .await?;
            let pending_provider_step = PendingProviderStep {
                provider_id: provider_id.clone(),
                model: model.clone(),
                request: request.clone(),
                response: response.clone(),
                narrative: narrative.clone(),
                action: action.clone(),
                tool_call_id: tool_call_id.clone(),
            };
            let awaiting_host_execution = build_pending_turn_checkpoint(
                current_plan_id,
                session_journal
                    .as_ref()
                    .map(|journal| journal.foreground_thread_id().to_string())
                    .unwrap_or_else(|| options.thread_id.clone().unwrap_or_else(|| "main".into())),
                round,
                max_tool_rounds,
                RuntimePendingTurnPhase::AwaitingHostExecution,
                &system_prompt,
                &conversation_messages,
                last_verification.as_ref().unwrap_or(&observation),
                &tool_outcome_history,
                last_action.as_ref(),
                last_execution.as_ref(),
                last_verification.as_ref(),
                last_receipt.as_ref(),
                Some(&pending_provider_step),
            );
            write_pending_turn_checkpoint_if_present(
                session_journal.as_ref(),
                &awaiting_host_execution,
            )
            .await?;
            let host_effects_uncertain = build_pending_turn_checkpoint(
                current_plan_id,
                session_journal
                    .as_ref()
                    .map(|journal| journal.foreground_thread_id().to_string())
                    .unwrap_or_else(|| options.thread_id.clone().unwrap_or_else(|| "main".into())),
                round,
                max_tool_rounds,
                RuntimePendingTurnPhase::HostEffectsUncertain,
                &system_prompt,
                &conversation_messages,
                last_verification.as_ref().unwrap_or(&observation),
                &tool_outcome_history,
                last_action.as_ref(),
                last_execution.as_ref(),
                last_verification.as_ref(),
                last_receipt.as_ref(),
                Some(&pending_provider_step),
            );
            write_pending_turn_checkpoint_if_present(
                session_journal.as_ref(),
                &host_effects_uncertain,
            )
            .await?;

            let execution = match timeout(options.host_enact_timeout, host.enact(&action)).await {
                Ok(Ok(execution)) => execution,
                Ok(Err(error)) => {
                    let gap = self
                        .surface_runtime_gap(
                            CapabilityGapDirective {
                                title: format!(
                                    "Host execution failed while attempting {}",
                                    describe_action(&action)
                                ),
                                permanent_fix_target:
                                    "Add a reusable host execution recovery and diagnosis path for this action class"
                                        .into(),
                                notes: vec![format!("{error:#}")],
                            },
                            &provider_id,
                            last_verification.as_ref().unwrap_or(&observation),
                            &narrative,
                            gap_task_root.as_deref(),
                        )
                        .await?;
                    if let Some(journal) = session_journal.as_ref() {
                        journal
                            .append_event(&RuntimeSessionEvent::turn_gap(
                                &session_event_id,
                                plan_id.clone(),
                                provider_id.clone(),
                                model.clone(),
                                gap.title.clone(),
                            ))
                            .await?;
                        append_gap_turn_record(
                            journal,
                            &provider_id,
                            &model,
                            &request,
                            &response,
                            &narrative,
                            &gap,
                        )
                        .await?;
                    }
                    if let Some(journal) = session_journal.as_mut() {
                        journal
                            .record_foreground_turn(format!(
                                "gap: {} | {}",
                                gap.title, gap.permanent_fix_target
                            ))
                            .await?;
                    }
                    clear_pending_turn_checkpoint_if_present(session_journal.as_ref()).await?;
                    return Ok(RuntimeTurnOutcome {
                        provider_id,
                        model,
                        observation,
                        response,
                        narrative,
                        tool_outcome_history,
                        proposed_action: Some(action),
                        execution: None,
                        verification: last_verification,
                        receipt: last_receipt,
                        github_action_request: None,
                        surfaced_gap: Some(gap),
                        acquired_lanes,
                    });
                }
                Err(_) => {
                    let gap = self
                        .surface_runtime_gap(
                            CapabilityGapDirective {
                                title: format!(
                                    "Host execution timed out while attempting {}",
                                    describe_action(&action)
                                ),
                                permanent_fix_target:
                                    "Add a reusable host execution timeout and diagnosis path for this action class"
                                        .into(),
                                notes: vec![format!(
                                    "execution timeout exceeded: {:?}",
                                    options.host_enact_timeout
                                )],
                            },
                            &provider_id,
                            last_verification.as_ref().unwrap_or(&observation),
                            &narrative,
                            gap_task_root.as_deref(),
                        )
                        .await?;
                    if let Some(journal) = session_journal.as_ref() {
                        journal
                            .append_event(&RuntimeSessionEvent::turn_gap(
                                &session_event_id,
                                plan_id.clone(),
                                provider_id.clone(),
                                model.clone(),
                                gap.title.clone(),
                            ))
                            .await?;
                        append_gap_turn_record(
                            journal,
                            &provider_id,
                            &model,
                            &request,
                            &response,
                            &narrative,
                            &gap,
                        )
                        .await?;
                    }
                    if let Some(journal) = session_journal.as_mut() {
                        journal
                            .record_foreground_turn(format!(
                                "gap: {} | {}",
                                gap.title, gap.permanent_fix_target
                            ))
                            .await?;
                    }
                    clear_pending_turn_checkpoint_if_present(session_journal.as_ref()).await?;
                    return Ok(RuntimeTurnOutcome {
                        provider_id,
                        model,
                        observation,
                        response,
                        narrative,
                        tool_outcome_history,
                        proposed_action: Some(action),
                        execution: None,
                        verification: last_verification,
                        receipt: last_receipt,
                        github_action_request: None,
                        surfaced_gap: Some(gap),
                        acquired_lanes,
                    });
                }
            };
            append_session_event_if_present(
                session_journal.as_ref(),
                RuntimeSessionEvent::turn_action_executed(
                    &session_event_id,
                    plan_id.clone(),
                    provider_id.clone(),
                    model.clone(),
                    round_number,
                    execution.summary.clone(),
                ),
            )
            .await?;
            let verification = match timeout(
                options.host_verify_timeout,
                host.verify_post_action(&execution),
            )
            .await
            {
                Ok(Ok(verification)) => verification,
                Ok(Err(ref verify_error)) => {
                    // Attempt bounded stabilization before declaring uncertain.
                    let stabilized = stabilize_host_effect(
                        host,
                        &action,
                        &execution,
                        |_obs| Ok(true), // any successful observe is treated as settled
                        |obs| format!("verify hard error: {verify_error:#}; last_obs: {}", obs.summary),
                        options.host_verify_timeout,
                        None,
                        None,
                    )
                    .await;
                    if let Ok(StabilizationOutcome::Settled {
                        observation: stable_obs,
                        attempt,
                    }) = stabilized
                    {
                        tracing::info!(
                            "host verification hard error resolved by stabilization after {attempt} attempt(s)"
                        );
                        last_verification = Some(stable_obs);
                        // Continue to the next round rather than surfacing a gap.
                        continue;
                    }
                    let verify_error = verify_error;
                    let gap = self
                        .surface_runtime_gap(
                            CapabilityGapDirective {
                                title: format!(
                                    "Host verification failed after {}",
                                    describe_action(&action)
                                ),
                                permanent_fix_target:
                                    "Add a reusable host verification recovery and fallback observation path for this action class"
                                        .into(),
                                notes: vec![format!("{verify_error:#}")],
                            },
                            &provider_id,
                            last_verification.as_ref().unwrap_or(&observation),
                            &narrative,
                            gap_task_root.as_deref(),
                        )
                        .await?;
                    if let Some(journal) = session_journal.as_ref() {
                        journal
                            .append_event(&RuntimeSessionEvent::turn_gap(
                                &session_event_id,
                                plan_id.clone(),
                                provider_id.clone(),
                                model.clone(),
                                gap.title.clone(),
                            ))
                            .await?;
                        append_gap_turn_record(
                            journal,
                            &provider_id,
                            &model,
                            &request,
                            &response,
                            &narrative,
                            &gap,
                        )
                        .await?;
                    }
                    if let Some(journal) = session_journal.as_mut() {
                        journal
                            .record_foreground_turn(format!(
                                "gap: {} | {}",
                                gap.title, gap.permanent_fix_target
                            ))
                            .await?;
                    }
                    clear_pending_turn_checkpoint_if_present(session_journal.as_ref()).await?;
                    return Ok(RuntimeTurnOutcome {
                        provider_id,
                        model,
                        observation,
                        response,
                        narrative,
                        tool_outcome_history,
                        proposed_action: Some(action),
                        execution: Some(execution),
                        verification: last_verification,
                        receipt: last_receipt,
                        github_action_request: None,
                        surfaced_gap: Some(gap),
                        acquired_lanes,
                    });
                }
                Err(_) => {
                    let gap = self
                        .surface_runtime_gap(
                            CapabilityGapDirective {
                                title: format!(
                                    "Host verification timed out after {}",
                                    describe_action(&action)
                                ),
                                permanent_fix_target:
                                    "Add a reusable host verification timeout and fallback observation path for this action class"
                                        .into(),
                                notes: vec![format!(
                                    "verification timeout exceeded: {:?}",
                                    options.host_verify_timeout
                                )],
                            },
                            &provider_id,
                            last_verification.as_ref().unwrap_or(&observation),
                            &narrative,
                            gap_task_root.as_deref(),
                        )
                        .await?;
                    if let Some(journal) = session_journal.as_ref() {
                        journal
                            .append_event(&RuntimeSessionEvent::turn_gap(
                                &session_event_id,
                                plan_id.clone(),
                                provider_id.clone(),
                                model.clone(),
                                gap.title.clone(),
                            ))
                            .await?;
                        append_gap_turn_record(
                            journal,
                            &provider_id,
                            &model,
                            &request,
                            &response,
                            &narrative,
                            &gap,
                        )
                        .await?;
                    }
                    if let Some(journal) = session_journal.as_mut() {
                        journal
                            .record_foreground_turn(format!(
                                "gap: {} | {}",
                                gap.title, gap.permanent_fix_target
                            ))
                            .await?;
                    }
                    clear_pending_turn_checkpoint_if_present(session_journal.as_ref()).await?;
                    return Ok(RuntimeTurnOutcome {
                        provider_id,
                        model,
                        observation,
                        response,
                        narrative,
                        tool_outcome_history,
                        proposed_action: Some(action),
                        execution: Some(execution),
                        verification: last_verification,
                        receipt: last_receipt,
                        github_action_request: None,
                        surfaced_gap: Some(gap),
                        acquired_lanes,
                    });
                }
            };
            let verification_decision = evaluate_verification(&action, &verification);
            append_session_event_if_present(
                session_journal.as_ref(),
                RuntimeSessionEvent::turn_action_verified(
                    &session_event_id,
                    plan_id.clone(),
                    provider_id.clone(),
                    model.clone(),
                    round_number,
                    verification_decision.summary.clone(),
                ),
            )
            .await?;
            let contradiction = verification_decision.contradiction.clone();
            let receipt = build_receipt(
                state.current_snapshot.plan.id,
                options.receipt_unit,
                last_verification.as_ref().unwrap_or(&observation),
                &action,
                &execution,
                &verification,
                &narrative,
                &verification_decision,
            );
            self.record_receipt(&receipt).await?;

            let surfaced_gap = if let Some(reason) = contradiction {
                Some(
                self.surface_runtime_gap(
                    CapabilityGapDirective {
                        title: format!(
                            "Post-action verification contradicted {}",
                            describe_action(&action)
                        ),
                        permanent_fix_target:
                            "Add a reusable verification and correction path for this action class"
                                .into(),
                        notes: vec![reason],
                    },
                    &provider_id,
                    &verification,
                    &narrative,
                    gap_task_root.as_deref(),
                )
                .await?,
            )
            } else {
                None
            };
            let tool_outcome = RuntimeToolOutcomeRecord {
                call_id: tool_call_id.clone(),
                action: action.clone(),
                execution: execution.clone(),
                verification: verification.clone(),
                verification_kind: Some(verification_decision.kind.clone()),
                verification_ok: Some(verification_decision.ok),
                verification_proof_level: verification_decision.proof_level.clone(),
                verification_summary: Some(verification_decision.summary.clone()),
                receipt_id: Some(receipt.id),
                receipt_changed: Some(receipt.changed.clone()),
                contradiction: receipt.contradicted.clone(),
            };
            tool_outcome_history.push(tool_outcome.clone());

            if let Some(journal) = session_journal.as_ref() {
                append_tool_turn_record(
                    journal,
                    &provider_id,
                    &model,
                    &request,
                    &response,
                    &narrative,
                    tool_outcome.clone(),
                    surfaced_gap.as_ref(),
                )
                .await?;
            }
            append_session_event_if_present(
                session_journal.as_ref(),
                RuntimeSessionEvent::turn_round_completed(
                    &session_event_id,
                    plan_id.clone(),
                    provider_id.clone(),
                    model.clone(),
                    round_number,
                    match surfaced_gap.as_ref() {
                        Some(gap) => format!(
                            "receipt={} | revealed_gap={}",
                            receipt.changed.clone(),
                            gap.title
                        ),
                        None => format!(
                            "receipt={} | {}",
                            receipt.changed.clone(),
                            verification_decision.summary
                        ),
                    },
                ),
            )
            .await?;

            last_action = Some(action.clone());
            last_execution = Some(execution.clone());
            last_verification = Some(verification.clone());
            last_receipt = Some(receipt.clone());

            if let Some(gap) = surfaced_gap {
                if let Some(journal) = session_journal.as_ref() {
                    journal
                        .append_event(&RuntimeSessionEvent::turn_completed(
                            &session_event_id,
                            plan_id.clone(),
                            provider_id.clone(),
                            model.clone(),
                            format!("completed with revealed gap: {}", gap.title),
                        ))
                        .await?;
                }
                if let Some(journal) = session_journal.as_mut() {
                    journal
                        .record_foreground_turn(format!(
                            "gap: {} | {}",
                            gap.title, gap.permanent_fix_target
                        ))
                        .await?;
                }
                clear_pending_turn_checkpoint_if_present(session_journal.as_ref()).await?;
                return Ok(RuntimeTurnOutcome {
                    provider_id,
                    model,
                    observation,
                    response,
                    narrative,
                    tool_outcome_history,
                    proposed_action: last_action,
                    execution: last_execution,
                    verification: last_verification,
                    receipt: last_receipt,
                    github_action_request: None,
                    surfaced_gap: Some(gap),
                    acquired_lanes,
                });
            }

            conversation_messages.push(ChatMessage {
                role: "assistant".into(),
                content: response.content.clone(),
            });
            conversation_messages.push(ChatMessage {
                role: "user".into(),
                content: vec![ContentBlock::ToolResult {
                    id: tool_call_id,
                    content: build_host_action_tool_result(
                        &execution,
                        &verification,
                        &receipt,
                        &verification_decision,
                    ),
                }],
            });
            conversation_messages.push(build_observation_message(&verification));
            if round + 1 < max_tool_rounds {
                let next_round_checkpoint = build_pending_turn_checkpoint(
                    current_plan_id,
                    session_journal
                        .as_ref()
                        .map(|journal| journal.foreground_thread_id().to_string())
                        .unwrap_or_else(|| {
                            options.thread_id.clone().unwrap_or_else(|| "main".into())
                        }),
                    round + 1,
                    max_tool_rounds,
                    RuntimePendingTurnPhase::AwaitingProvider,
                    &system_prompt,
                    &conversation_messages,
                    &verification,
                    &tool_outcome_history,
                    last_action.as_ref(),
                    last_execution.as_ref(),
                    last_verification.as_ref(),
                    last_receipt.as_ref(),
                    None,
                );
                write_pending_turn_checkpoint_if_present(
                    session_journal.as_ref(),
                    &next_round_checkpoint,
                )
                .await?;
            } else {
                clear_pending_turn_checkpoint_if_present(session_journal.as_ref()).await?;
            }
            continue;
        }

        let gap = self
            .surface_runtime_gap(
                CapabilityGapDirective {
                    title: "Runtime exhausted its bounded inner tool loop".into(),
                    permanent_fix_target:
                        "Tighten planning so the task completes within the configured tool-round budget"
                            .into(),
                    notes: vec![format!("max_tool_rounds={}", max_tool_rounds)],
                },
                &final_provider_id,
                last_verification.as_ref().unwrap_or(&observation),
                &final_narrative,
                gap_task_root.as_deref(),
            )
            .await?;
        if let Some(journal) = session_journal.as_ref() {
            journal
                .append_event(&RuntimeSessionEvent::turn_gap(
                    &session_event_id,
                    plan_id.clone(),
                    final_provider_id.clone(),
                    final_model.clone(),
                    gap.title.clone(),
                ))
                .await?;
            if let Some(response) = final_response.as_ref() {
                let request = build_runtime_chat_request(
                    final_model.clone(),
                    system_prompt.clone(),
                    conversation_messages.clone(),
                );
                append_gap_turn_record(
                    journal,
                    &final_provider_id,
                    &final_model,
                    &request,
                    response,
                    &final_narrative,
                    &gap,
                )
                .await?;
            }
        }
        clear_pending_turn_checkpoint_if_present(session_journal.as_ref()).await?;
        if let Some(journal) = session_journal.as_mut() {
            journal
                .record_foreground_turn(format!(
                    "gap: {} | {}",
                    gap.title, gap.permanent_fix_target
                ))
                .await?;
        }
        Ok(RuntimeTurnOutcome {
            provider_id: final_provider_id,
            model: final_model,
            observation,
            response: final_response.unwrap_or(ChatResponse {
                provider_id: "runtime".into(),
                model: "runtime".into(),
                content: vec![ContentBlock::Text {
                    text: final_narrative.clone(),
                }],
            }),
            narrative: final_narrative,
            tool_outcome_history,
            proposed_action: last_action,
            execution: last_execution,
            verification: last_verification,
            receipt: last_receipt,
            github_action_request: None,
            surfaced_gap: Some(gap),
            acquired_lanes,
        })
    }

    async fn surface_runtime_gap(
        &self,
        directive: CapabilityGapDirective,
        provider_id: &str,
        observation: &ObservationFrame,
        narrative: &str,
        session_root: Option<&std::path::Path>,
    ) -> anyhow::Result<CapabilityGap> {
        let now = Utc::now();
        let mut notes = directive.notes;
        notes.push(format!("provider: {}", provider_id));
        notes.push(format!("observation: {}", observation.summary));
        if !narrative.is_empty() {
            notes.push(format!("assistant: {}", narrative));
        }

        let gap = CapabilityGap {
            id: Uuid::new_v4(),
            title: directive.title,
            revealed_by: format!("runtime-turn:{}", provider_id),
            permanent_fix_target: directive.permanent_fix_target,
            status: CapabilityGapStatus::Open,
            discovered_at: now,
            last_touched_at: now,
            notes,
        };
        self.surface_gap(&gap).await?;
        if let Some(root) = session_root {
            if let Err(e) = emit_gap_task(root, &gap).await {
                tracing::warn!("gap task emission failed (non-fatal): {e:#}");
            }
        }
        Ok(gap)
    }
}

fn build_initial_runtime_messages(
    state: &OrchestratorState,
    observation: &ObservationFrame,
    external_context: Option<&str>,
) -> Vec<ChatMessage> {
    let mut text = format!(
        "{}\n\n{}",
        render_state_summary(state),
        render_observation_summary(observation)
    );
    if let Some(external_context) = external_context
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        text.push_str("\n\n# External Context\n");
        text.push_str(external_context);
    }

    let mut content = vec![ContentBlock::Text { text }];

    if let Some(path) = &observation.screenshot_path {
        content.push(ContentBlock::ImagePath { path: path.clone() });
    }

    vec![ChatMessage {
        role: "user".into(),
        content,
    }]
}

fn build_runtime_chat_request(
    model: String,
    system_prompt: String,
    messages: Vec<ChatMessage>,
) -> ChatRequest {
    ChatRequest {
        model,
        system_prompt: Some(system_prompt),
        messages,
        tools: vec![
            host_action_tool_definition(),
            capability_gap_tool_definition(),
            supervised_github_action_tool_definition(),
        ],
    }
}

fn build_observation_message(observation: &ObservationFrame) -> ChatMessage {
    let mut content = vec![ContentBlock::Text {
        text: render_observation_summary(observation),
    }];
    if let Some(path) = &observation.screenshot_path {
        content.push(ContentBlock::ImagePath { path: path.clone() });
    }
    ChatMessage {
        role: "user".into(),
        content,
    }
}

fn build_host_action_tool_result(
    execution: &ActionExecution,
    verification: &ObservationFrame,
    receipt: &Receipt,
    verification_decision: &RuntimeVerificationDecision,
) -> serde_json::Value {
    serde_json::json!({
        "status": if receipt.contradicted.is_some() { "contradicted" } else { "ok" },
        "execution": {
            "backend": execution.backend,
            "summary": execution.summary,
            "recorded_at": execution.recorded_at.to_rfc3339(),
        },
        "verification": {
            "summary": verification.summary,
            "active_window": verification.active_window,
            "ocr_text": verification.ocr_text,
            "window_titles": verification.window_titles,
            "structured_signal_keys": verification.structured_signals.iter().map(|entry| entry.key.clone()).collect::<Vec<_>>(),
        },
        "runtime_verification": {
            "kind": verification_decision.kind.clone(),
            "ok": verification_decision.ok,
            "proof_level": verification_decision.proof_level.clone(),
            "summary": verification_decision.summary.clone(),
        },
        "receipt": {
            "id": receipt.id.to_string(),
            "changed": receipt.changed,
            "enabled_next": receipt.enabled_next,
            "contradicted": receipt.contradicted,
        }
    })
}

async fn append_gap_turn_record(
    journal: &RuntimeSessionJournal,
    provider_id: &str,
    model: &str,
    request: &ChatRequest,
    response: &ChatResponse,
    narrative: &str,
    gap: &CapabilityGap,
) -> anyhow::Result<()> {
    journal
        .append_turn_record(&RuntimeTurnRecord {
            recorded_at: Utc::now(),
            turn_id: Uuid::new_v4(),
            thread_id: journal.foreground_thread_id().to_string(),
            provider_id: provider_id.into(),
            model: model.into(),
            request: request.clone(),
            response: response.clone(),
            narrative: narrative.into(),
            tool_outcome: None,
            surfaced_gap: Some(RuntimeGapRecord {
                gap_id: gap.id,
                title: gap.title.clone(),
                permanent_fix_target: gap.permanent_fix_target.clone(),
            }),
        })
        .await
}

async fn append_session_event_if_present(
    journal: Option<&RuntimeSessionJournal>,
    event: RuntimeSessionEvent,
) -> anyhow::Result<()> {
    if let Some(journal) = journal {
        journal.append_event(&event).await?;
    }
    Ok(())
}

async fn append_tool_turn_record(
    journal: &RuntimeSessionJournal,
    provider_id: &str,
    model: &str,
    request: &ChatRequest,
    response: &ChatResponse,
    narrative: &str,
    tool_outcome: RuntimeToolOutcomeRecord,
    surfaced_gap: Option<&CapabilityGap>,
) -> anyhow::Result<()> {
    journal
        .append_turn_record(&RuntimeTurnRecord {
            recorded_at: Utc::now(),
            turn_id: Uuid::new_v4(),
            thread_id: journal.foreground_thread_id().to_string(),
            provider_id: provider_id.into(),
            model: model.into(),
            request: request.clone(),
            response: response.clone(),
            narrative: narrative.into(),
            tool_outcome: Some(tool_outcome),
            surfaced_gap: surfaced_gap.map(|gap| RuntimeGapRecord {
                gap_id: gap.id,
                title: gap.title.clone(),
                permanent_fix_target: gap.permanent_fix_target.clone(),
            }),
        })
        .await
}

async fn append_terminal_turn_record(
    journal: &RuntimeSessionJournal,
    provider_id: &str,
    model: &str,
    request: &ChatRequest,
    response: &ChatResponse,
    narrative: &str,
) -> anyhow::Result<()> {
    journal
        .append_turn_record(&RuntimeTurnRecord {
            recorded_at: Utc::now(),
            turn_id: Uuid::new_v4(),
            thread_id: journal.foreground_thread_id().to_string(),
            provider_id: provider_id.into(),
            model: model.into(),
            request: request.clone(),
            response: response.clone(),
            narrative: narrative.into(),
            tool_outcome: None,
            surfaced_gap: None,
        })
        .await
}

fn host_action_tool_definition() -> ToolDefinition {
    ToolDefinition {
        name: HOST_ACTION_TOOL.into(),
        description: "Execute the next verified host/computer-use action.".into(),
        input_schema: serde_json::json!({
            "type": "object",
            "required": [
                "kind",
                "title",
                "x",
                "y",
                "button",
                "from_x",
                "from_y",
                "to_x",
                "to_y",
                "text",
                "submit",
                "chord",
                "delta",
                "command",
                "args",
                "signal",
                "timeout_ms"
            ],
            "properties": {
                "kind": {
                    "type": "string",
                    "enum": [
                        "focus_window",
                        "click",
                        "double_click",
                        "drag",
                        "type_text",
                        "hotkey",
                        "scroll",
                        "launch_process",
                        "wait_for",
                        "capture_observation"
                    ]
                },
                "title": { "type": ["string", "null"] },
                "x": { "type": ["integer", "null"] },
                "y": { "type": ["integer", "null"] },
                "button": {
                    "anyOf": [
                        { "type": "string", "enum": ["Left", "Right", "Middle"] },
                        { "type": "null" }
                    ]
                },
                "from_x": { "type": ["integer", "null"] },
                "from_y": { "type": ["integer", "null"] },
                "to_x": { "type": ["integer", "null"] },
                "to_y": { "type": ["integer", "null"] },
                "text": { "type": ["string", "null"] },
                "submit": { "type": ["boolean", "null"] },
                "chord": { "type": ["string", "null"] },
                "delta": { "type": ["integer", "null"] },
                "command": { "type": ["string", "null"] },
                "args": {
                    "anyOf": [
                        { "type": "array", "items": { "type": "string" } },
                        { "type": "null" }
                    ]
                },
                "signal": { "type": ["string", "null"] },
                "timeout_ms": { "type": ["integer", "null"] }
            }
        }),
    }
}

fn capability_gap_tool_definition() -> ToolDefinition {
    ToolDefinition {
        name: CAPABILITY_GAP_TOOL.into(),
        description:
            "Surface a durable capability gap instead of improvising through missing ability."
                .into(),
        input_schema: serde_json::json!({
            "type": "object",
            "required": ["title", "permanent_fix_target", "notes"],
            "properties": {
                "title": { "type": "string" },
                "permanent_fix_target": { "type": "string" },
                "notes": { "type": "array", "items": { "type": "string" } }
            }
        }),
    }
}

fn supervised_github_action_tool_definition() -> ToolDefinition {
    ToolDefinition {
        name: SUPERVISED_GITHUB_ACTION_TOOL.into(),
        description:
            "Request one operator-approved GitHub mutation when the next bounded step belongs on GitHub instead of the desktop."
                .into(),
        input_schema: serde_json::json!({
            "type": "object",
            "required": [
                "kind",
                "repository",
                "issue_number",
                "pull_request_number",
                "body",
                "label",
                "assignee",
                "justification"
            ],
            "properties": {
                    "kind": {
                    "type": "string",
                    "enum": [
                        "comment_issue",
                        "comment_pull_request",
                        "assign_issue",
                        "assign_pull_request",
                        "close_issue",
                        "close_pull_request",
                        "reopen_issue",
                        "reopen_pull_request",
                        "label_issue",
                        "label_pull_request",
                        "remove_label_issue",
                        "remove_label_pull_request"
                    ]
                },
                "repository": { "type": ["string", "null"] },
                "issue_number": { "type": ["integer", "null"] },
                "pull_request_number": { "type": ["integer", "null"] },
                "body": { "type": ["string", "null"] },
                "label": { "type": ["string", "null"] },
                "assignee": { "type": ["string", "null"] },
                "justification": { "type": ["string", "null"] }
            }
        }),
    }
}

fn runtime_system_prompt(
    post_compaction_refresh: Option<&str>,
    thread_context: Option<&str>,
    turn_history_context: Option<&str>,
    operating_memory: Option<&str>,
) -> String {
    let mut lines = vec![
        "You are the bounded SPLCW runtime controller.",
        "Use the current plan, open gaps, and fresh observation to decide the next safe move.",
        "Return at most one tool call named `host_action` per assistant step when the body can act now.",
        "Do not emit `host_action`, `capability_gap`, and `supervised_github_action` together in the same assistant step.",
        "If the work is complete for this bounded turn, return short text only and no tool call.",
        "After the runtime returns a tool result and refreshed observation, continue reasoning and emit the next `host_action` if more work is still needed.",
        "If the remaining work cannot be completed safely within the bounded tool budget, emit `capability_gap` instead of guessing.",
        "Return a tool call named `capability_gap` when the runtime lacks the capability or certainty to continue safely.",
        "Return a tool call named `supervised_github_action` when the next bounded step is a GitHub mutation that should be reviewed and applied by the operator shell instead of being executed inside the runtime loop.",
        "If you emit `host_action`, arguments may either match the ProposedAction enum JSON or this flat schema:",
        "{ \"kind\": \"type_text|focus_window|click|double_click|drag|hotkey|scroll|launch_process|wait_for|capture_observation\", ... }",
        "When emitting `host_action`, include every schema field and use null (or [] for args) for fields the chosen action does not need.",
        "If you emit `capability_gap`, arguments must be:",
        "{ \"title\": string, \"permanent_fix_target\": string, \"notes\": string[] }",
        "If you emit `supervised_github_action`, arguments must be:",
        "{ \"kind\": \"comment_issue|comment_pull_request|assign_issue|assign_pull_request|close_issue|close_pull_request|reopen_issue|reopen_pull_request|label_issue|label_pull_request|remove_label_issue|remove_label_pull_request\", \"repository\": string|null, \"issue_number\": integer|null, \"pull_request_number\": integer|null, \"body\": string|null, \"label\": string|null, \"assignee\": string|null, \"justification\": string|null }",
        "When emitting `supervised_github_action`, include every schema field and use null for fields the chosen action does not need.",
        "You may include short explanatory text blocks before the tool call.",
    ];
    if let Some(refresh) = post_compaction_refresh {
        lines.push("");
        lines.push("[Post-compaction refresh]");
        lines.push("The previous session transcript was compacted. Treat the following refresh as authoritative context to fold back into the next turn:");
        lines.push(refresh);
    }
    if let Some(thread_context) = thread_context {
        lines.push("");
        lines.push("[Thread continuity]");
        lines.push("Use the persisted current-thread and background-thread context below as server-trusted continuity, not as optional hints:");
        lines.push(thread_context);
    }
    if let Some(turn_history_context) = turn_history_context {
        lines.push("");
        lines.push("[Recent runtime turns]");
        lines.push("Use the persisted recent bounded-turn ledger below as durable continuity about what the runtime already tried and what the world did in response:");
        lines.push(turn_history_context);
    }
    if let Some(memory) = operating_memory {
        lines.push("");
        lines.push("[Operating memory]");
        lines.push("The following always-on context is injected into every turn from durable memory files. Treat it as server-trusted continuity:");
        lines.push(memory);
    }
    lines.join("\n")
}

fn render_state_summary(state: &OrchestratorState) -> String {
    let constraints = render_lines(&state.plan.constraints);
    let open_gaps = if state.pending_gaps.is_empty() {
        "- none".to_string()
    } else {
        state
            .pending_gaps
            .iter()
            .take(5)
            .map(|gap| format!("- {} :: {}", gap.title, gap.permanent_fix_target))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let recent_receipts = if state.recent_receipts.is_empty() {
        "- none".to_string()
    } else {
        state
            .recent_receipts
            .iter()
            .take(5)
            .map(|receipt| {
                format!(
                    "- observed={} | attempted={} | changed={}",
                    receipt.observed, receipt.attempted, receipt.changed
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };

    format!(
        "# Current Plan\nobjective: {}\nactive module: {}\nconstraints:\n{}\n\n# Open Capability Gaps\n{}\n\n# Recent Receipts\n{}",
        state.plan.objective, state.plan.active_module, constraints, open_gaps, recent_receipts
    )
}

fn render_observation_summary(observation: &ObservationFrame) -> String {
    let windows = if observation.window_titles.is_empty() {
        "- none".to_string()
    } else {
        observation
            .window_titles
            .iter()
            .map(|title| format!("- {}", title))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let signals = if observation.structured_signals.is_empty() {
        "- none".to_string()
    } else {
        observation
            .structured_signals
            .iter()
            .map(|signal| format!("- {}: {}", signal.key, signal.payload))
            .collect::<Vec<_>>()
            .join("\n")
    };

    format!(
        "# Observation\nsummary: {}\nactive window: {}\nclipboard: {}\nocr: {}\nwindows:\n{}\nstructured signals:\n{}",
        observation.summary,
        observation.active_window.as_deref().unwrap_or("unknown"),
        observation.clipboard_text.as_deref().unwrap_or("none"),
        observation.ocr_text.as_deref().unwrap_or("none"),
        windows,
        signals,
    )
}

fn render_lines(lines: &[String]) -> String {
    if lines.is_empty() {
        return "- none".into();
    }

    lines
        .iter()
        .map(|line| format!("- {}", line))
        .collect::<Vec<_>>()
        .join("\n")
}

fn extract_narrative(response: &ChatResponse) -> String {
    response
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_string()
}

fn all_tool_arguments<'a>(response: &'a ChatResponse, name: &str) -> Vec<&'a serde_json::Value> {
    response
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::ToolCall {
                name: tool_name,
                arguments,
                ..
            } if tool_name == name => Some(arguments),
            _ => None,
        })
        .collect()
}

fn first_tool_call_id(response: &ChatResponse, name: &str) -> Option<String> {
    response.content.iter().find_map(|block| match block {
        ContentBlock::ToolCall {
            id,
            name: block_name,
            ..
        } if block_name == name => Some(id.clone()),
        _ => None,
    })
}

fn first_tool_arguments<'a>(
    response: &'a ChatResponse,
    name: &str,
) -> Option<&'a serde_json::Value> {
    all_tool_arguments(response, name).into_iter().next()
}

fn parse_gap_directive(arguments: &serde_json::Value) -> anyhow::Result<CapabilityGapDirective> {
    serde_json::from_value(arguments.clone()).context("deserialize capability_gap tool arguments")
}

fn parse_supervised_github_action(
    arguments: &serde_json::Value,
) -> anyhow::Result<SupervisedGithubActionRequest> {
    let object = arguments
        .as_object()
        .ok_or_else(|| anyhow!("supervised_github_action arguments must be an object"))?;
    let kind_raw = extract_optional_string_alias(object, &["kind"])?
        .ok_or_else(|| anyhow!("missing required kind for supervised_github_action"))?;

    SupervisedGithubActionRequest {
        kind: parse_supervised_github_action_kind(&kind_raw)?,
        repository: extract_optional_string_alias(object, &["repository", "repo"])?,
        issue_number: extract_optional_u64_alias(object, &["issue_number", "issue", "issue_id"])?,
        pull_request_number: extract_optional_u64_alias(
            object,
            &["pull_request_number", "pull_request", "pr_number", "pr"],
        )?,
        body: extract_optional_string_alias(object, &["body", "comment", "message"])?,
        label: extract_optional_string_alias(object, &["label", "label_name"])?,
        assignee: extract_optional_string_alias(object, &["assignee", "assignee_name", "user"])?,
        justification: extract_optional_string_alias(object, &["justification", "reason", "why"])?,
    }
    .validate()
}

fn parse_host_action(arguments: &serde_json::Value) -> anyhow::Result<ProposedAction> {
    serde_json::from_value::<ProposedAction>(arguments.clone())
        .or_else(|_| serde_json::from_value::<ActionDirective>(arguments.clone())?.into_action())
        .context("deserialize host_action tool arguments")
}

fn build_receipt(
    plan_id: Uuid,
    unit: SplcwUnit,
    observation: &ObservationFrame,
    action: &ProposedAction,
    execution: &ActionExecution,
    verification: &ObservationFrame,
    narrative: &str,
    verification_decision: &RuntimeVerificationDecision,
) -> Receipt {
    let attempted = if narrative.is_empty() {
        describe_action(action)
    } else {
        format!("{} | assistant: {}", describe_action(action), narrative)
    };

    Receipt {
        id: Uuid::new_v4(),
        plan_id,
        unit,
        observed: observation.summary.clone(),
        attempted,
        changed: format!(
            "{} | verification: {} | {}",
            execution.summary, verification.summary, verification_decision.summary
        ),
        contradicted: verification_decision.contradiction.clone(),
        enabled_next: next_step_summary(verification),
        recorded_at: Utc::now(),
    }
}
#[allow(dead_code)]
fn detect_contradiction(
    action: &ProposedAction,
    verification: &ObservationFrame,
) -> Option<String> {
    evaluate_verification(action, verification).contradiction
}

fn evaluate_verification(
    action: &ProposedAction,
    verification: &ObservationFrame,
) -> RuntimeVerificationDecision {
    match action {
        ProposedAction::FocusWindow { title } => {
            if let Some(signal) = focus_verification_signal(verification) {
                let accepted = signal.matched && signal.stable;
                let proof_level = Some(
                    if accepted {
                        "stable_focus_match"
                    } else if signal.matched {
                        "unstable_focus"
                    } else {
                        "focus_mismatch"
                    }
                    .to_string(),
                );
                let contradiction = if accepted {
                    None
                } else if signal.matched {
                    Some(format!(
                        "focus verification for `{}` matched `{}` but did not stabilize after {} attempts (timed_out={})",
                        title,
                        signal.observed.as_deref().unwrap_or("none"),
                        signal.attempts,
                        signal.timed_out
                    ))
                } else {
                    Some(format!(
                        "expected active window containing `{}`, observed `{}` (focus verify expected=`{}` matched={} stable={} attempts={} timed_out={})",
                        title,
                        signal.observed.as_deref().unwrap_or("none"),
                        signal.expected,
                        signal.matched,
                        signal.stable,
                        signal.attempts,
                        signal.timed_out
                    ))
                };
                RuntimeVerificationDecision::new(
                    "focus_window",
                    accepted,
                    proof_level,
                    contradiction,
                )
            } else {
                RuntimeVerificationDecision::new(
                    "focus_window",
                    false,
                    Some("missing_signal".to_string()),
                    Some(format!(
                        "expected `focus_window` verification signal for `{}` but none was present in the verified observation",
                        title
                    )),
                )
            }
        }
        ProposedAction::LaunchProcess { command, .. } => {
            match post_action_verification_signal(verification, "launch_process") {
                Some(signal) => {
                    let accepted = signal.ok && launch_signal_has_process_delta(&signal);
                    let contradiction = if accepted {
                        None
                    } else if signal.ok {
                        Some(format!(
                            "expected launched process for `{}` to prove a fresh process delta, but post-action verification only showed weak launch evidence {}",
                            command, signal.evidence
                        ))
                    } else {
                        Some(format!(
                            "expected launched process for `{}` to be verifiable, but post-action verification reported ok=false with evidence {}",
                            command, signal.evidence
                        ))
                    };
                    RuntimeVerificationDecision::new(
                        "launch_process",
                        accepted,
                        post_action_signal_proof_level(&signal),
                        contradiction,
                    )
                }
                None => RuntimeVerificationDecision::new(
                    "launch_process",
                    false,
                    Some("missing_signal".into()),
                    Some(format!(
                        "expected `launch_process` verification signal for `{}` but none was present in the verified observation",
                        command
                    )),
                ),
            }
        }
        ProposedAction::Click { .. }
        | ProposedAction::DoubleClick { .. }
        | ProposedAction::Drag { .. } => {
            let kind = action_kind(action);
            match post_action_verification_signal(verification, kind) {
                Some(signal) => {
                    let accepted = signal.ok && pointer_signal_has_observable_effect(&signal);
                    let contradiction = if accepted {
                        None
                    } else if signal.ok {
                        Some(format!(
                            "expected `{}` verification to show a visible or focus effect, but post-action verification only proved pointer targeting with evidence {}",
                            kind, signal.evidence
                        ))
                    } else {
                        Some(format!(
                            "expected `{}` verification to succeed, but post-action verification reported ok=false with evidence {}",
                            kind, signal.evidence
                        ))
                    };
                    RuntimeVerificationDecision::new(
                        kind,
                        accepted,
                        post_action_signal_proof_level(&signal),
                        contradiction,
                    )
                }
                None => RuntimeVerificationDecision::new(
                    kind,
                    false,
                    Some("missing_signal".into()),
                    Some(format!(
                        "expected `{}` verification signal, but none was present in the verified observation",
                        kind
                    )),
                ),
            }
        }
        ProposedAction::TypeText { .. } => {
            match post_action_verification_signal(verification, "type_text") {
                Some(signal) => {
                    let accepted = signal.ok && type_text_signal_has_observable_effect(&signal);
                    let contradiction = if accepted {
                        None
                    } else if signal.ok {
                        Some(format!(
                            "expected `type_text` verification to show an observable input effect, but post-action verification only proved anchor preservation with evidence {}",
                            signal.evidence
                        ))
                    } else {
                        Some(format!(
                            "expected `type_text` verification to show an observable input effect, but post-action verification reported ok=false with evidence {}",
                            signal.evidence
                        ))
                    };
                    RuntimeVerificationDecision::new(
                        "type_text",
                        accepted,
                        post_action_signal_proof_level(&signal),
                        contradiction,
                    )
                }
                None => RuntimeVerificationDecision::new(
                    "type_text",
                    false,
                    Some("missing_signal".into()),
                    Some(
                        "expected `type_text` verification signal, but none was present in the verified observation"
                            .to_string(),
                    ),
                ),
            }
        }
        ProposedAction::Hotkey { .. } => {
            match post_action_verification_signal(verification, "hotkey") {
                Some(signal) => {
                    let accepted = signal.ok && hotkey_signal_has_observable_effect(&signal);
                    let contradiction = if accepted {
                        None
                    } else if signal.ok {
                        Some(format!(
                            "expected `hotkey` verification to show an observable effect, but post-action verification only proved anchor preservation with evidence {}",
                            signal.evidence
                        ))
                    } else {
                        Some(format!(
                            "expected `hotkey` verification to show an observable effect, but post-action verification reported ok=false with evidence {}",
                            signal.evidence
                        ))
                    };
                    RuntimeVerificationDecision::new(
                        "hotkey",
                        accepted,
                        post_action_signal_proof_level(&signal),
                        contradiction,
                    )
                }
                None => RuntimeVerificationDecision::new(
                    "hotkey",
                    false,
                    Some("missing_signal".into()),
                    Some(
                        "expected `hotkey` verification signal, but none was present in the verified observation"
                            .to_string(),
                    ),
                ),
            }
        }
        ProposedAction::Scroll { .. } => {
            match post_action_verification_signal(verification, "scroll") {
                Some(signal) => {
                    let accepted = signal.ok && scroll_signal_has_observable_effect(&signal);
                    let contradiction = if accepted {
                        None
                    } else if signal.ok {
                        Some(format!(
                            "expected `scroll` verification to show a viewport or focus effect, but post-action verification reported ok=true without observable scroll evidence {}",
                            signal.evidence
                        ))
                    } else {
                        Some(format!(
                            "expected `scroll` verification to show a viewport or focus effect, but post-action verification reported ok=false with evidence {}",
                            signal.evidence
                        ))
                    };
                    RuntimeVerificationDecision::new(
                        "scroll",
                        accepted,
                        post_action_signal_proof_level(&signal),
                        contradiction,
                    )
                }
                None => RuntimeVerificationDecision::new(
                    "scroll",
                    false,
                    Some("missing_signal".into()),
                    Some(
                        "expected `scroll` verification signal, but none was present in the verified observation"
                            .to_string(),
                    ),
                ),
            }
        }
        ProposedAction::WaitFor { signal, .. } => {
            match post_action_verification_signal(verification, "wait_for") {
                Some(signal_status) => {
                    let accepted = signal_status.ok && wait_signal_matched(&signal_status);
                    let contradiction = if accepted {
                        None
                    } else if signal_status.ok {
                        Some(format!(
                            "expected wait signal `{}` to be explicitly matched, but post-action verification reported ok=true without a matched wait result {}",
                            signal, signal_status.evidence
                        ))
                    } else {
                        Some(format!(
                            "expected wait signal `{}` before timeout, but post-action verification reported ok=false with evidence {}",
                            signal, signal_status.evidence
                        ))
                    };
                    RuntimeVerificationDecision::new(
                        "wait_for",
                        accepted,
                        post_action_signal_proof_level(&signal_status),
                        contradiction,
                    )
                }
                None => RuntimeVerificationDecision::new(
                    "wait_for",
                    false,
                    Some("missing_signal".into()),
                    Some(format!(
                        "expected `wait_for` verification signal for `{}` but none was present in the verified observation",
                        signal
                    )),
                ),
            }
        }
        ProposedAction::CaptureObservation => {
            match post_action_verification_signal(verification, "capture_observation") {
                Some(signal) => {
                    let accepted = signal.ok
                        && verification.screenshot_path.is_some()
                        && capture_observation_signal_has_readable_artifact(&signal);
                    let contradiction = if accepted {
                        None
                    } else if signal.ok {
                        Some(format!(
                            "expected `capture_observation` verification to prove a readable screenshot artifact, but post-action verification only reported weak capture evidence {}",
                            signal.evidence
                        ))
                    } else {
                        Some(format!(
                            "expected `capture_observation` verification to prove a readable screenshot artifact, but post-action verification reported ok=false with evidence {}",
                            signal.evidence
                        ))
                    };
                    RuntimeVerificationDecision::new(
                        "capture_observation",
                        accepted,
                        post_action_signal_proof_level(&signal),
                        contradiction,
                    )
                }
                None => RuntimeVerificationDecision::new(
                    "capture_observation",
                    false,
                    Some("missing_signal".into()),
                    Some(
                        "expected `capture_observation` verification signal, but none was present in the verified observation"
                            .into(),
                    ),
                ),
            }
        }
    }
}

fn focus_verification_signal(verification: &ObservationFrame) -> Option<FocusVerificationSignal> {
    verification
        .structured_signals
        .iter()
        .find(|entry| entry.key == "focus_verification")
        .and_then(|entry| serde_json::from_value(entry.payload.clone()).ok())
}

fn post_action_verification_signal(
    verification: &ObservationFrame,
    kind: &str,
) -> Option<PostActionVerificationSignal> {
    verification
        .structured_signals
        .iter()
        .filter(|entry| entry.key == "post_action_verification")
        .filter_map(|entry| {
            serde_json::from_value::<PostActionVerificationSignal>(entry.payload.clone()).ok()
        })
        .find(|signal| signal.kind == kind)
}

fn post_action_signal_proof_level(signal: &PostActionVerificationSignal) -> Option<String> {
    signal
        .evidence
        .as_object()
        .and_then(|evidence| evidence.get("proof_level"))
        .and_then(|value| value.as_str())
        .map(str::to_string)
}

fn capture_observation_signal_has_readable_artifact(signal: &PostActionVerificationSignal) -> bool {
    signal
        .evidence
        .as_object()
        .map(|evidence| {
            evidence
                .get("screenshot_readable")
                .and_then(|value| value.as_bool())
                .unwrap_or(false)
        })
        .unwrap_or(false)
}

fn hotkey_signal_has_observable_effect(signal: &PostActionVerificationSignal) -> bool {
    signal
        .evidence
        .as_object()
        .map(|evidence| {
            [
                "window_changed",
                "clipboard_changed",
                "focused_control_value_changed",
                "screenshot_changed",
            ]
            .into_iter()
            .any(|key| {
                evidence
                    .get(key)
                    .and_then(|value| value.as_bool())
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

fn wait_signal_matched(signal: &PostActionVerificationSignal) -> bool {
    signal
        .evidence
        .as_object()
        .and_then(|evidence| evidence.get("matched"))
        .and_then(|value| value.as_bool())
        .unwrap_or(signal.ok)
}

fn launch_signal_has_process_delta(signal: &PostActionVerificationSignal) -> bool {
    signal
        .evidence
        .as_object()
        .map(|evidence| {
            [
                "new_process_detected",
                "spawned_pid_present",
                "spawned_child_detected",
            ]
            .into_iter()
            .any(|key| {
                evidence
                    .get(key)
                    .and_then(|value| value.as_bool())
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

fn type_text_signal_has_observable_effect(signal: &PostActionVerificationSignal) -> bool {
    signal
        .evidence
        .as_object()
        .map(|evidence| {
            [
                "requested_text_observed",
                "focused_control_value_changed",
                "window_changed",
                "clipboard_changed",
                "screenshot_changed",
            ]
            .into_iter()
            .any(|key| {
                evidence
                    .get(key)
                    .and_then(|value| value.as_bool())
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

fn pointer_signal_has_observable_effect(signal: &PostActionVerificationSignal) -> bool {
    signal
        .evidence
        .as_object()
        .map(|evidence| {
            [
                "window_changed",
                "focused_control_changed",
                "screenshot_changed",
            ]
            .into_iter()
            .any(|key| {
                evidence
                    .get(key)
                    .and_then(|value| value.as_bool())
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

fn scroll_signal_has_observable_effect(signal: &PostActionVerificationSignal) -> bool {
    signal
        .evidence
        .as_object()
        .map(|evidence| {
            [
                "window_changed",
                "focused_control_changed",
                "screenshot_changed",
            ]
            .into_iter()
            .any(|key| {
                evidence
                    .get(key)
                    .and_then(|value| value.as_bool())
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

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

fn describe_action(action: &ProposedAction) -> String {
    match action {
        ProposedAction::FocusWindow { title } => format!("focus window {}", title),
        ProposedAction::Click { x, y, .. } => format!("click at {},{}", x, y),
        ProposedAction::DoubleClick { x, y, .. } => format!("double click at {},{}", x, y),
        ProposedAction::Drag {
            from_x,
            from_y,
            to_x,
            to_y,
        } => format!("drag from {},{} to {},{}", from_x, from_y, to_x, to_y),
        ProposedAction::TypeText { text, submit } => {
            format!("type {} characters submit={}", text.chars().count(), submit)
        }
        ProposedAction::Hotkey { chord } => format!("send hotkey {}", chord),
        ProposedAction::Scroll { delta } => format!("scroll {}", delta),
        ProposedAction::LaunchProcess { command, args } => {
            format!("launch {} {}", command, args.join(" "))
        }
        ProposedAction::WaitFor { signal, timeout_ms } => {
            format!("wait for {} up to {}ms", signal, timeout_ms)
        }
        ProposedAction::CaptureObservation => "capture observation".into(),
    }
}

fn next_step_summary(frame: &ObservationFrame) -> String {
    if let Some(active_window) = &frame.active_window {
        format!("Continue runtime loop from {}", active_window)
    } else {
        format!("Continue runtime loop from {}", frame.summary)
    }
}

fn required_string(field: &'static str, value: Option<String>) -> anyhow::Result<String> {
    value.ok_or_else(|| anyhow!("missing required field {}", field))
}

fn required_i32(field: &'static str, value: Option<i32>) -> anyhow::Result<i32> {
    value.ok_or_else(|| anyhow!("missing required field {}", field))
}

fn required_u64(field: &'static str, value: Option<u64>) -> anyhow::Result<u64> {
    value.ok_or_else(|| anyhow!("missing required field {}", field))
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;

    use async_trait::async_trait;
    use chrono::Utc;
    use splcw_core::{Invariant, PlanModule, PlanSnapshot, SufficientPlan};
    use splcw_llm::{
        ApiKeyState, AuthMode, AuthProfile, AuthProfileStore, ChatResponse, LlmProvider,
        OAuthState, ProviderDescriptor, ProviderKind,
    };
    use splcw_memory::{OffloadSink, SqliteStateStore, StateStore};

    use super::*;

    #[derive(Default)]
    struct RecordingOffloadSink {
        plan_snapshots: Mutex<usize>,
        receipts: Mutex<usize>,
        gaps: Mutex<usize>,
        recodifications: Mutex<usize>,
        checkpoints: Mutex<usize>,
        current_surfaces: Mutex<usize>,
    }

    #[async_trait]
    impl OffloadSink for RecordingOffloadSink {
        async fn push_plan_snapshot(
            &self,
            _snapshot: &splcw_core::PlanSnapshot,
        ) -> anyhow::Result<()> {
            *self.plan_snapshots.lock().unwrap() += 1;
            Ok(())
        }

        async fn push_receipt_summary(&self, _receipt: &splcw_core::Receipt) -> anyhow::Result<()> {
            *self.receipts.lock().unwrap() += 1;
            Ok(())
        }

        async fn push_capability_gap(
            &self,
            _gap: &splcw_core::CapabilityGap,
        ) -> anyhow::Result<()> {
            *self.gaps.lock().unwrap() += 1;
            Ok(())
        }

        async fn push_recodification(
            &self,
            _recodification: &splcw_core::Recodification,
        ) -> anyhow::Result<()> {
            *self.recodifications.lock().unwrap() += 1;
            Ok(())
        }

        async fn push_checkpoint_manifest(
            &self,
            _manifest: &splcw_memory::CheckpointManifest,
        ) -> anyhow::Result<()> {
            *self.checkpoints.lock().unwrap() += 1;
            Ok(())
        }

        async fn push_current_surface(
            &self,
            _surface: &splcw_memory::CurrentSurface,
        ) -> anyhow::Result<()> {
            *self.current_surfaces.lock().unwrap() += 1;
            Ok(())
        }
    }

    struct StaticAuthStore {
        profile: AuthProfile,
    }

    #[async_trait]
    impl AuthProfileStore for StaticAuthStore {
        async fn list_profiles(&self) -> anyhow::Result<Vec<AuthProfile>> {
            Ok(vec![self.profile.clone()])
        }

        async fn upsert_profile(&self, _profile: &AuthProfile) -> anyhow::Result<()> {
            Ok(())
        }

        async fn load_default_profile(&self) -> anyhow::Result<Option<AuthProfile>> {
            Ok(Some(self.profile.clone()))
        }

        async fn set_default_profile(&self, _profile_id: Uuid) -> anyhow::Result<()> {
            Ok(())
        }
    }

    struct StaticProvider {
        descriptor: ProviderDescriptor,
        response: ChatResponse,
        last_request: Mutex<Option<ChatRequest>>,
    }

    #[async_trait]
    impl LlmProvider for StaticProvider {
        fn descriptor(&self) -> ProviderDescriptor {
            self.descriptor.clone()
        }

        async fn chat(
            &self,
            _auth: &AuthProfile,
            request: &ChatRequest,
        ) -> anyhow::Result<ChatResponse> {
            *self.last_request.lock().unwrap() = Some(request.clone());
            Ok(self.response.clone())
        }
    }

    #[test]
    fn host_action_tool_schema_requires_all_keys_and_nullable_optionals() {
        let schema = host_action_tool_definition().input_schema;

        let required = schema["required"].as_array().unwrap();
        assert!(required.iter().any(|value| value == "kind"));
        assert!(required.iter().any(|value| value == "title"));
        assert!(required.iter().any(|value| value == "args"));
        assert_eq!(
            schema["properties"]["title"]["type"],
            serde_json::json!(["string", "null"])
        );
        assert_eq!(
            schema["properties"]["args"]["anyOf"][1]["type"],
            serde_json::json!("null")
        );
    }

    #[test]
    fn parse_host_action_accepts_null_optional_fields() {
        let action = parse_host_action(&serde_json::json!({
            "kind": "capture_observation",
            "title": null,
            "x": null,
            "y": null,
            "button": null,
            "from_x": null,
            "from_y": null,
            "to_x": null,
            "to_y": null,
            "text": null,
            "submit": null,
            "chord": null,
            "delta": null,
            "command": null,
            "args": null,
            "signal": null,
            "timeout_ms": null
        }))
        .unwrap();

        assert_eq!(action, ProposedAction::CaptureObservation);
    }

    #[test]
    fn supervised_github_action_tool_schema_requires_all_keys_and_nullable_optionals() {
        let schema = supervised_github_action_tool_definition().input_schema;

        let required = schema["required"].as_array().unwrap();
        assert!(required.iter().any(|value| value == "kind"));
        assert!(required.iter().any(|value| value == "repository"));
        assert!(required.iter().any(|value| value == "body"));
        assert!(required.iter().any(|value| value == "assignee"));
        assert_eq!(
            schema["properties"]["repository"]["type"],
            serde_json::json!(["string", "null"])
        );
        assert_eq!(
            schema["properties"]["pull_request_number"]["type"],
            serde_json::json!(["integer", "null"])
        );
        assert!(
            schema["properties"]["kind"]["enum"]
                .as_array()
                .unwrap()
                .iter()
                .any(|value| value == "close_issue")
        );
        assert!(
            schema["properties"]["kind"]["enum"]
                .as_array()
                .unwrap()
                .iter()
                .any(|value| value == "close_pull_request")
        );
        assert!(
            schema["properties"]["kind"]["enum"]
                .as_array()
                .unwrap()
                .iter()
                .any(|value| value == "assign_issue")
        );
        assert!(
            schema["properties"]["kind"]["enum"]
                .as_array()
                .unwrap()
                .iter()
                .any(|value| value == "assign_pull_request")
        );
        assert!(
            schema["properties"]["kind"]["enum"]
                .as_array()
                .unwrap()
                .iter()
                .any(|value| value == "reopen_issue")
        );
        assert!(
            schema["properties"]["kind"]["enum"]
                .as_array()
                .unwrap()
                .iter()
                .any(|value| value == "reopen_pull_request")
        );
        assert!(
            schema["properties"]["kind"]["enum"]
                .as_array()
                .unwrap()
                .iter()
                .any(|value| value == "remove_label_issue")
        );
        assert!(
            schema["properties"]["kind"]["enum"]
                .as_array()
                .unwrap()
                .iter()
                .any(|value| value == "remove_label_pull_request")
        );
    }

    #[test]
    fn parse_supervised_github_action_rejects_mismatched_fields() {
        let error = parse_supervised_github_action(&serde_json::json!({
            "kind": "label_pull_request",
            "repository": "jessybrenenstahl/FFR",
            "issue_number": 77,
            "pull_request_number": 101,
            "body": "should not be present",
            "label": "needs-review",
            "justification": "Keep the runtime bounded."
        }))
        .expect_err("mismatched fields should fail closed");

        assert!(error.to_string().contains("unexpected issue_number"));
    }

    #[test]
    fn parse_supervised_github_action_accepts_aliases_and_missing_nullable_keys() {
        let request = parse_supervised_github_action(&serde_json::json!({
            "kind": "comment_pr",
            "repo": "jessybrenenstahl/FFR",
            "comment": "This landed in the harness GUI and needs review.",
            "why": "Surface the bounded supervised GitHub lane for review."
        }))
        .expect("alias-heavy payload should parse");

        assert_eq!(request.kind, SupervisedGithubActionKind::CommentPullRequest);
        assert_eq!(request.repository.as_deref(), Some("jessybrenenstahl/FFR"));
        assert_eq!(request.pull_request_number, None);
        assert_eq!(
            request.body.as_deref(),
            Some("This landed in the harness GUI and needs review.")
        );
        assert_eq!(
            request.justification.as_deref(),
            Some("Surface the bounded supervised GitHub lane for review.")
        );
        assert_eq!(request.issue_number, None);
        assert_eq!(request.label, None);
        assert!(request.requires_operator_target());
        assert_eq!(request.operator_target_kind(), "pull request");
        assert_eq!(
            request.target_summary(),
            "jessybrenenstahl/FFR pull request target pending operator selection"
        );
    }

    #[test]
    fn parse_supervised_github_action_accepts_issue_label_aliases() {
        let request = parse_supervised_github_action(&serde_json::json!({
            "kind": "issue_label",
            "repo": "jessybrenenstahl/FFR",
            "issue": 91,
            "label_name": "needs-repro",
            "why": "Keep the lane supervised."
        }))
        .expect("issue label payload should parse");

        assert_eq!(request.kind, SupervisedGithubActionKind::LabelIssue);
        assert_eq!(request.repository.as_deref(), Some("jessybrenenstahl/FFR"));
        assert_eq!(request.issue_number, Some(91));
        assert_eq!(request.label.as_deref(), Some("needs-repro"));
        assert_eq!(
            request.justification.as_deref(),
            Some("Keep the lane supervised.")
        );
        assert_eq!(request.pull_request_number, None);
        assert_eq!(request.body, None);
        assert_eq!(request.operator_target_kind(), "issue");
        assert_eq!(request.target_summary(), "jessybrenenstahl/FFR issue #91");
    }

    #[test]
    fn parse_supervised_github_action_accepts_close_issue_aliases() {
        let request = parse_supervised_github_action(&serde_json::json!({
            "kind": "issue_close",
            "repo": "jessybrenenstahl/FFR",
            "issue": 17,
            "comment": "Closing this now that the supervised lane landed.",
            "why": "Keep the repo state aligned with the shipped capability."
        }))
        .expect("close issue payload should parse");

        assert_eq!(request.kind, SupervisedGithubActionKind::CloseIssue);
        assert_eq!(request.repository.as_deref(), Some("jessybrenenstahl/FFR"));
        assert_eq!(request.issue_number, Some(17));
        assert_eq!(
            request.body.as_deref(),
            Some("Closing this now that the supervised lane landed.")
        );
        assert_eq!(
            request.justification.as_deref(),
            Some("Keep the repo state aligned with the shipped capability.")
        );
        assert_eq!(request.pull_request_number, None);
        assert_eq!(request.label, None);
        assert_eq!(request.operator_target_kind(), "issue");
        assert_eq!(request.target_summary(), "jessybrenenstahl/FFR issue #17");
    }

    #[test]
    fn parse_supervised_github_action_accepts_close_pull_request_aliases() {
        let request = parse_supervised_github_action(&serde_json::json!({
            "kind": "pr_close",
            "repo": "jessybrenenstahl/FFR",
            "pull_request": 42,
            "comment": "Closing this PR now that the supervised lane landed.",
            "why": "Keep the repo state aligned with the shipped capability."
        }))
        .expect("close pull request payload should parse");

        assert_eq!(request.kind, SupervisedGithubActionKind::ClosePullRequest);
        assert_eq!(request.repository.as_deref(), Some("jessybrenenstahl/FFR"));
        assert_eq!(request.pull_request_number, Some(42));
        assert_eq!(
            request.body.as_deref(),
            Some("Closing this PR now that the supervised lane landed.")
        );
        assert_eq!(
            request.justification.as_deref(),
            Some("Keep the repo state aligned with the shipped capability.")
        );
        assert_eq!(request.issue_number, None);
        assert_eq!(request.label, None);
        assert_eq!(request.operator_target_kind(), "pull request");
        assert_eq!(
            request.target_summary(),
            "jessybrenenstahl/FFR pull request #42"
        );
    }

    #[test]
    fn parse_supervised_github_action_accepts_assign_issue_aliases() {
        let request = parse_supervised_github_action(&serde_json::json!({
            "kind": "issue_assign",
            "repo": "jessybrenenstahl/FFR",
            "issue": 17,
            "assignee_name": "@me",
            "why": "Keep issue ownership explicit."
        }))
        .expect("assign issue payload should parse");

        assert_eq!(request.kind, SupervisedGithubActionKind::AssignIssue);
        assert_eq!(request.repository.as_deref(), Some("jessybrenenstahl/FFR"));
        assert_eq!(request.issue_number, Some(17));
        assert_eq!(request.assignee.as_deref(), Some("@me"));
        assert_eq!(
            request.justification.as_deref(),
            Some("Keep issue ownership explicit.")
        );
        assert_eq!(request.pull_request_number, None);
        assert_eq!(request.body, None);
        assert_eq!(request.label, None);
    }

    #[test]
    fn parse_supervised_github_action_accepts_assign_pull_request_aliases() {
        let request = parse_supervised_github_action(&serde_json::json!({
            "kind": "pr_assign",
            "repo": "jessybrenenstahl/FFR",
            "pull_request": 42,
            "user": "@copilot",
            "why": "Keep review ownership explicit."
        }))
        .expect("assign pull request payload should parse");

        assert_eq!(request.kind, SupervisedGithubActionKind::AssignPullRequest);
        assert_eq!(request.repository.as_deref(), Some("jessybrenenstahl/FFR"));
        assert_eq!(request.pull_request_number, Some(42));
        assert_eq!(request.assignee.as_deref(), Some("@copilot"));
        assert_eq!(
            request.justification.as_deref(),
            Some("Keep review ownership explicit.")
        );
        assert_eq!(request.issue_number, None);
        assert_eq!(request.body, None);
        assert_eq!(request.label, None);
    }

    #[test]
    fn parse_supervised_github_action_accepts_reopen_issue_aliases() {
        let request = parse_supervised_github_action(&serde_json::json!({
            "kind": "issue_reopen",
            "repo": "jessybrenenstahl/FFR",
            "issue": 19,
            "comment": "Reopening this issue so the follow-up stays visible.",
            "why": "Keep the tracked issue aligned with the repo state."
        }))
        .expect("reopen issue payload should parse");

        assert_eq!(request.kind, SupervisedGithubActionKind::ReopenIssue);
        assert_eq!(request.repository.as_deref(), Some("jessybrenenstahl/FFR"));
        assert_eq!(request.issue_number, Some(19));
        assert_eq!(
            request.body.as_deref(),
            Some("Reopening this issue so the follow-up stays visible.")
        );
        assert_eq!(
            request.justification.as_deref(),
            Some("Keep the tracked issue aligned with the repo state.")
        );
        assert_eq!(request.pull_request_number, None);
        assert_eq!(request.label, None);
        assert_eq!(request.operator_target_kind(), "issue");
        assert_eq!(request.target_summary(), "jessybrenenstahl/FFR issue #19");
    }

    #[test]
    fn parse_supervised_github_action_accepts_reopen_pull_request_aliases() {
        let request = parse_supervised_github_action(&serde_json::json!({
            "kind": "pr_reopen",
            "repo": "jessybrenenstahl/FFR",
            "pull_request": 29,
            "comment": "Reopening this PR so the operator can finish review.",
            "why": "Keep the tracked pull request aligned with the repo state."
        }))
        .expect("reopen pull request payload should parse");

        assert_eq!(request.kind, SupervisedGithubActionKind::ReopenPullRequest);
        assert_eq!(request.repository.as_deref(), Some("jessybrenenstahl/FFR"));
        assert_eq!(request.pull_request_number, Some(29));
        assert_eq!(
            request.body.as_deref(),
            Some("Reopening this PR so the operator can finish review.")
        );
        assert_eq!(
            request.justification.as_deref(),
            Some("Keep the tracked pull request aligned with the repo state.")
        );
        assert_eq!(request.issue_number, None);
        assert_eq!(request.label, None);
        assert_eq!(request.operator_target_kind(), "pull request");
        assert_eq!(
            request.target_summary(),
            "jessybrenenstahl/FFR pull request #29"
        );
    }

    #[test]
    fn parse_supervised_github_action_accepts_remove_label_issue_aliases() {
        let request = parse_supervised_github_action(&serde_json::json!({
            "kind": "issue_unlabel",
            "repo": "jessybrenenstahl/FFR",
            "issue": 52,
            "label_name": "needs-repro",
            "why": "Remove the stale label now that the repro landed."
        }))
        .expect("remove label issue payload should parse");

        assert_eq!(request.kind, SupervisedGithubActionKind::RemoveLabelIssue);
        assert_eq!(request.repository.as_deref(), Some("jessybrenenstahl/FFR"));
        assert_eq!(request.issue_number, Some(52));
        assert_eq!(request.label.as_deref(), Some("needs-repro"));
        assert_eq!(
            request.justification.as_deref(),
            Some("Remove the stale label now that the repro landed.")
        );
        assert_eq!(request.pull_request_number, None);
        assert_eq!(request.body, None);
        assert_eq!(request.operator_target_kind(), "issue");
        assert_eq!(request.target_summary(), "jessybrenenstahl/FFR issue #52");
    }

    #[test]
    fn parse_supervised_github_action_accepts_remove_label_pull_request_aliases() {
        let request = parse_supervised_github_action(&serde_json::json!({
            "kind": "pr_remove_label",
            "repo": "jessybrenenstahl/FFR",
            "pull_request": 64,
            "label_name": "needs-review",
            "why": "Remove the stale review label after operator approval."
        }))
        .expect("remove label pull request payload should parse");

        assert_eq!(
            request.kind,
            SupervisedGithubActionKind::RemoveLabelPullRequest
        );
        assert_eq!(request.repository.as_deref(), Some("jessybrenenstahl/FFR"));
        assert_eq!(request.pull_request_number, Some(64));
        assert_eq!(request.label.as_deref(), Some("needs-review"));
        assert_eq!(
            request.justification.as_deref(),
            Some("Remove the stale review label after operator approval.")
        );
        assert_eq!(request.issue_number, None);
        assert_eq!(request.body, None);
        assert_eq!(request.operator_target_kind(), "pull request");
        assert_eq!(
            request.target_summary(),
            "jessybrenenstahl/FFR pull request #64"
        );
    }

    #[test]
    fn initial_runtime_messages_include_external_context() {
        let now = Utc::now();
        let snapshot = PlanSnapshot {
            snapshot_id: Uuid::new_v4(),
            plan: SufficientPlan {
                id: Uuid::new_v4(),
                version: 1,
                objective: "Inspect roadmap".into(),
                constraints: vec!["Stay bounded.".into()],
                invariants: vec![],
                modules: vec![],
                active_module: "operate".into(),
                recodification_rule: "Promote gaps.".into(),
                updated_at: now,
            },
            rationale: "test".into(),
            source_gap_id: None,
            recorded_at: now,
        };
        let state = OrchestratorState::new(snapshot, Vec::new(), Vec::new(), Vec::new());
        let observation = sample_observation("windows observe: active=Codex windows=1");
        let messages = build_initial_runtime_messages(
            &state,
            &observation,
            Some("## Project Roadmap\nroadmap body"),
        );

        let body = messages[0]
            .content
            .iter()
            .find_map(|block| match block {
                ContentBlock::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .unwrap();

        assert!(body.contains("# External Context"));
        assert!(body.contains("roadmap body"));
    }

    struct StepwiseProvider {
        descriptor: ProviderDescriptor,
        responses: Vec<ChatResponse>,
        last_request: Mutex<Option<ChatRequest>>,
        requests: Mutex<Vec<ChatRequest>>,
    }

    #[async_trait]
    impl LlmProvider for StepwiseProvider {
        fn descriptor(&self) -> ProviderDescriptor {
            self.descriptor.clone()
        }

        async fn chat(
            &self,
            _auth: &AuthProfile,
            request: &ChatRequest,
        ) -> anyhow::Result<ChatResponse> {
            let tool_result_count = request
                .messages
                .iter()
                .flat_map(|message| message.content.iter())
                .filter(|block| matches!(block, ContentBlock::ToolResult { .. }))
                .count();
            let index = tool_result_count.min(self.responses.len().saturating_sub(1));
            *self.last_request.lock().unwrap() = Some(request.clone());
            self.requests.lock().unwrap().push(request.clone());
            Ok(self.responses[index].clone())
        }
    }

    struct FailingProvider {
        descriptor: ProviderDescriptor,
        error_message: String,
    }

    #[async_trait]
    impl LlmProvider for FailingProvider {
        fn descriptor(&self) -> ProviderDescriptor {
            self.descriptor.clone()
        }

        async fn chat(
            &self,
            _auth: &AuthProfile,
            _request: &ChatRequest,
        ) -> anyhow::Result<ChatResponse> {
            Err(anyhow!(self.error_message.clone()))
        }
    }

    struct MockHost {
        observation: ObservationFrame,
        verification: ObservationFrame,
        actions: Mutex<Vec<ProposedAction>>,
    }

    #[async_trait]
    impl HostBody for MockHost {
        async fn observe(&self) -> anyhow::Result<ObservationFrame> {
            Ok(self.observation.clone())
        }

        async fn enact(&self, action: &ProposedAction) -> anyhow::Result<ActionExecution> {
            self.actions.lock().unwrap().push(action.clone());
            Ok(ActionExecution {
                id: Uuid::new_v4(),
                action: action.clone(),
                backend: "mock-host".into(),
                summary: format!("executed {}", describe_action(action)),
                evidence: None,
                recorded_at: Utc::now(),
            })
        }

        async fn verify_post_action(
            &self,
            _execution: &ActionExecution,
        ) -> anyhow::Result<ObservationFrame> {
            Ok(self.verification.clone())
        }
    }

    struct FailingHost {
        observation: ObservationFrame,
        verification: ObservationFrame,
        actions: Mutex<Vec<ProposedAction>>,
        enact_error: Option<String>,
        verify_error: Option<String>,
    }

    #[async_trait]
    impl HostBody for FailingHost {
        async fn observe(&self) -> anyhow::Result<ObservationFrame> {
            Ok(self.observation.clone())
        }

        async fn enact(&self, action: &ProposedAction) -> anyhow::Result<ActionExecution> {
            self.actions.lock().unwrap().push(action.clone());
            if let Some(error) = &self.enact_error {
                return Err(anyhow!(error.clone()));
            }
            Ok(ActionExecution {
                id: Uuid::new_v4(),
                action: action.clone(),
                backend: "failing-host".into(),
                summary: format!("executed {}", describe_action(action)),
                evidence: None,
                recorded_at: Utc::now(),
            })
        }

        async fn verify_post_action(
            &self,
            _execution: &ActionExecution,
        ) -> anyhow::Result<ObservationFrame> {
            if let Some(error) = &self.verify_error {
                return Err(anyhow!(error.clone()));
            }
            Ok(self.verification.clone())
        }
    }

    struct SlowHost {
        observation: ObservationFrame,
        verification: ObservationFrame,
        actions: Mutex<Vec<ProposedAction>>,
        observe_delay: Duration,
        enact_delay: Duration,
        verify_delay: Duration,
    }

    #[async_trait]
    impl HostBody for SlowHost {
        async fn observe(&self) -> anyhow::Result<ObservationFrame> {
            tokio::time::sleep(self.observe_delay).await;
            Ok(self.observation.clone())
        }

        async fn enact(&self, action: &ProposedAction) -> anyhow::Result<ActionExecution> {
            self.actions.lock().unwrap().push(action.clone());
            tokio::time::sleep(self.enact_delay).await;
            Ok(ActionExecution {
                id: Uuid::new_v4(),
                action: action.clone(),
                backend: "slow-host".into(),
                summary: format!("executed {}", describe_action(action)),
                evidence: None,
                recorded_at: Utc::now(),
            })
        }

        async fn verify_post_action(
            &self,
            _execution: &ActionExecution,
        ) -> anyhow::Result<ObservationFrame> {
            tokio::time::sleep(self.verify_delay).await;
            Ok(self.verification.clone())
        }
    }

    struct SerializingHost {
        observation: ObservationFrame,
        verification: ObservationFrame,
        in_flight: AtomicUsize,
        max_in_flight: AtomicUsize,
        actions: Mutex<Vec<ProposedAction>>,
    }

    #[async_trait]
    impl HostBody for SerializingHost {
        async fn observe(&self) -> anyhow::Result<ObservationFrame> {
            let current = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(current, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(50)).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            Ok(self.observation.clone())
        }

        async fn enact(&self, action: &ProposedAction) -> anyhow::Result<ActionExecution> {
            self.actions.lock().unwrap().push(action.clone());
            Ok(ActionExecution {
                id: Uuid::new_v4(),
                action: action.clone(),
                backend: "serializing-host".into(),
                summary: format!("executed {}", describe_action(action)),
                evidence: None,
                recorded_at: Utc::now(),
            })
        }

        async fn verify_post_action(
            &self,
            _execution: &ActionExecution,
        ) -> anyhow::Result<ObservationFrame> {
            Ok(self.verification.clone())
        }
    }

    fn sample_plan_snapshot() -> PlanSnapshot {
        let plan = SufficientPlan {
            id: Uuid::new_v4(),
            version: 1,
            objective: "Keep the desktop moving safely".into(),
            constraints: vec!["Preserve local truth".into()],
            invariants: vec![Invariant {
                key: "receipts".into(),
                description: "Every action produces a receipt".into(),
            }],
            modules: vec![PlanModule {
                key: "runtime".into(),
                description: "Drive the next verified desktop step".into(),
                success_checks: vec!["Observed fresh frame".into()],
                reveal_response: "Surface a capability gap before retry".into(),
            }],
            active_module: "runtime".into(),
            recodification_rule: "Encode missing capability into the next sufficient plan".into(),
            updated_at: Utc::now(),
        };

        PlanSnapshot {
            snapshot_id: Uuid::new_v4(),
            plan,
            rationale: "Bootstrap the first runtime loop".into(),
            source_gap_id: None,
            recorded_at: Utc::now(),
        }
    }

    fn sample_profile() -> AuthProfile {
        AuthProfile {
            id: Uuid::new_v4(),
            provider: ProviderKind::Local,
            mode: AuthMode::Local,
            label: "local".into(),
            oauth: Some(OAuthState {
                issuer: "local".into(),
                account_label: Some("test".into()),
                access_token: None,
                refresh_token: None,
                expires_at: None,
            }),
            api_key: Some(ApiKeyState {
                env_var: None,
                key_material: None,
            }),
            updated_at: Utc::now(),
        }
    }

    fn sample_observation(summary: &str) -> ObservationFrame {
        ObservationFrame {
            captured_at: Utc::now(),
            summary: summary.into(),
            screenshot_path: Some("C:/tmp/runtime.png".into()),
            ocr_text: Some("chat input ready".into()),
            active_window: Some("Desktop Chat".into()),
            window_titles: vec!["Desktop Chat".into(), "Browser".into()],
            clipboard_text: Some("clipboard".into()),
            structured_signals: vec![],
        }
    }

    fn sample_focus_verification_signal(
        expected: &str,
        observed: Option<&str>,
        matched: bool,
        stable: bool,
    ) -> splcw_computer_use::StructuredSignal {
        splcw_computer_use::StructuredSignal {
            key: "focus_verification".into(),
            payload: serde_json::json!({
                "expected": expected,
                "observed": observed,
                "matched": matched,
                "stable": stable,
                "attempts": 3,
                "timed_out": !stable,
            }),
        }
    }

    fn sample_post_action_verification_signal(
        kind: &str,
        ok: bool,
        evidence: serde_json::Value,
    ) -> splcw_computer_use::StructuredSignal {
        splcw_computer_use::StructuredSignal {
            key: "post_action_verification".into(),
            payload: serde_json::json!({
                "kind": kind,
                "ok": ok,
                "evidence": evidence,
            }),
        }
    }

    fn with_capture_observation_signal(mut verification: ObservationFrame) -> ObservationFrame {
        verification
            .structured_signals
            .push(sample_post_action_verification_signal(
                "capture_observation",
                true,
                serde_json::json!({
                    "screenshot_present": true,
                    "screenshot_readable": true,
                    "proof_level": "screenshot_readable",
                }),
            ));
        verification
    }

    fn sample_capture_observation_verification(summary: &str) -> ObservationFrame {
        with_capture_observation_signal(sample_observation(summary))
    }

    fn local_mock_descriptor() -> ProviderDescriptor {
        ProviderDescriptor {
            id: "local-mock".into(),
            provider: ProviderKind::Local,
            display_name: "Local Mock".into(),
            auth_modes: vec![AuthMode::Local],
            default_model: "mock-model".into(),
        }
    }

    fn terminal_text_response(text: &str) -> ChatResponse {
        ChatResponse {
            provider_id: "local-mock".into(),
            model: "mock-model".into(),
            content: vec![ContentBlock::Text { text: text.into() }],
        }
    }

    fn capture_observation_response(call_id: &str) -> ChatResponse {
        ChatResponse {
            provider_id: "local-mock".into(),
            model: "mock-model".into(),
            content: vec![ContentBlock::ToolCall {
                id: call_id.into(),
                name: HOST_ACTION_TOOL.into(),
                arguments: serde_json::json!({
                    "kind": "capture_observation"
                }),
            }],
        }
    }

    fn sample_pending_turn_checkpoint(
        plan_id: Uuid,
        phase: RuntimePendingTurnPhase,
        round_index: usize,
        max_tool_rounds: usize,
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
            round_index,
            max_tool_rounds,
            phase,
            system_prompt: "bounded runtime system prompt".into(),
            conversation_messages: conversation_messages.clone(),
            observation: sample_observation("pending runtime observation"),
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
                tools: vec![],
            }),
            pending_response: Some(capture_observation_response("call-pending")),
            pending_narrative: Some("resume the stored host action".into()),
            pending_action: Some(ProposedAction::CaptureObservation),
            pending_tool_call_id: Some("call-pending".into()),
            updated_at: Utc::now(),
        }
    }

    fn temp_session_root(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("splcw-runtime-turn-{}-{}", label, Uuid::new_v4()))
    }

    fn event_kinds(events: &[crate::RuntimeSessionEvent]) -> Vec<crate::RuntimeSessionEventKind> {
        events.iter().map(|event| event.kind.clone()).collect()
    }

    #[tokio::test]
    async fn runtime_turn_executes_host_action_and_records_receipt() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload.clone());
        let snapshot = sample_plan_snapshot();
        orchestrator.adopt_plan(&snapshot).await.unwrap();

        let provider = Arc::new(StepwiseProvider {
            descriptor: local_mock_descriptor(),
            responses: vec![
                ChatResponse {
                    provider_id: "local-mock".into(),
                    model: "mock-model".into(),
                    content: vec![
                        ContentBlock::Text {
                            text: "Type the greeting into the active window.".into(),
                        },
                        ContentBlock::ToolCall {
                            id: "call-1".into(),
                            name: HOST_ACTION_TOOL.into(),
                            arguments: serde_json::json!({
                                "kind": "type_text",
                                "text": "hello from the runtime spine",
                                "submit": true
                            }),
                        },
                    ],
                },
                terminal_text_response("Greeting entered and verified."),
            ],
            last_request: Mutex::new(None),
            requests: Mutex::new(Vec::new()),
        });

        let mut registry = splcw_llm::ProviderRegistry::new();
        registry.register(provider.clone()).unwrap();
        let llm = ConfiguredLlmClient::new(
            Arc::new(StaticAuthStore {
                profile: sample_profile(),
            }),
            registry,
        );
        let host = MockHost {
            observation: sample_observation("chat box is focused"),
            verification: ObservationFrame {
                structured_signals: vec![sample_post_action_verification_signal(
                    "type_text",
                    true,
                    serde_json::json!({
                        "focus_preserved": true,
                        "window_changed": false,
                        "clipboard_changed": false,
                        "focused_control_changed": false,
                        "focused_control_value_changed": true,
                        "requested_text_observed": true,
                        "proof_level": "focused_control_text_match",
                    }),
                )],
                ..sample_observation("greeting is now visible")
            },
            actions: Mutex::new(Vec::new()),
        };

        let outcome = orchestrator
            .run_runtime_turn(&llm, &host, RuntimeTurnOptions::default())
            .await
            .unwrap();

        assert_eq!(
            outcome.proposed_action,
            Some(ProposedAction::TypeText {
                text: "hello from the runtime spine".into(),
                submit: true,
            })
        );
        assert!(outcome.surfaced_gap.is_none());
        assert!(outcome.receipt.is_some());
        assert_eq!(store.list_recent_receipts(10).await.unwrap().len(), 1);
        assert_eq!(host.actions.lock().unwrap().len(), 1);
        assert_eq!(*offload.current_surfaces.lock().unwrap(), 2);
        assert_eq!(*offload.checkpoints.lock().unwrap(), 2);
        assert_eq!(
            provider
                .last_request
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .model,
            "mock-model"
        );
        assert_eq!(
            outcome.tool_outcome_history[0]
                .verification_proof_level
                .as_deref(),
            Some("focused_control_text_match")
        );
        assert_eq!(outcome.tool_outcome_history[0].verification_ok, Some(true));
        assert!(
            outcome
                .receipt
                .as_ref()
                .unwrap()
                .changed
                .contains("proof=focused_control_text_match")
        );
        let tool_result = provider.requests.lock().unwrap()[1]
            .messages
            .iter()
            .flat_map(|message| message.content.iter())
            .find_map(|block| match block {
                ContentBlock::ToolResult { content, .. } => Some(content.clone()),
                _ => None,
            })
            .expect("tool result should be present on the second request");
        assert_eq!(
            tool_result["runtime_verification"]["proof_level"],
            serde_json::json!("focused_control_text_match")
        );
        assert_eq!(
            tool_result["runtime_verification"]["ok"],
            serde_json::json!(true)
        );
    }

    #[tokio::test]
    async fn runtime_turn_accepts_plain_text_completion_without_host_action() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store, offload);
        let snapshot = sample_plan_snapshot();
        orchestrator.adopt_plan(&snapshot).await.unwrap();

        let provider = Arc::new(StaticProvider {
            descriptor: local_mock_descriptor(),
            response: terminal_text_response(
                "The next bounded roadmap-aligned implementation slice is detached-runner crash recovery.",
            ),
            last_request: Mutex::new(None),
        });

        let mut registry = splcw_llm::ProviderRegistry::new();
        registry.register(provider).unwrap();
        let llm = ConfiguredLlmClient::new(
            Arc::new(StaticAuthStore {
                profile: sample_profile(),
            }),
            registry,
        );
        let host = MockHost {
            observation: sample_observation("roadmap docs are readable"),
            verification: sample_observation("verification should not run"),
            actions: Mutex::new(Vec::new()),
        };

        let outcome = orchestrator
            .run_runtime_turn(&llm, &host, RuntimeTurnOptions::default())
            .await
            .unwrap();

        assert!(outcome.surfaced_gap.is_none());
        assert!(outcome.proposed_action.is_none());
        assert_eq!(
            outcome.narrative,
            "The next bounded roadmap-aligned implementation slice is detached-runner crash recovery."
        );
        assert!(host.actions.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn runtime_turn_returns_supervised_github_action_request_without_host_action() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store, offload);
        let snapshot = sample_plan_snapshot();
        orchestrator.adopt_plan(&snapshot).await.unwrap();

        let provider = Arc::new(StaticProvider {
            descriptor: local_mock_descriptor(),
            response: ChatResponse {
                provider_id: "local-mock".into(),
                model: "mock-model".into(),
                content: vec![
                    ContentBlock::Text {
                        text: "Ask the operator shell to comment on the tracking issue.".into(),
                    },
                    ContentBlock::ToolCall {
                        id: "gh-1".into(),
                        name: SUPERVISED_GITHUB_ACTION_TOOL.into(),
                        arguments: serde_json::json!({
                            "kind": "comment_issue",
                            "repository": "jessybrenenstahl/FFR",
                            "issue_number": 77,
                            "pull_request_number": null,
                            "body": "The harness now has live GitHub read context and needs the next supervised mutation lane.",
                            "label": null,
                            "justification": "Keep GitHub mutations operator-approved while broadening autonomy."
                        }),
                    },
                ],
            },
            last_request: Mutex::new(None),
        });

        let mut registry = splcw_llm::ProviderRegistry::new();
        registry.register(provider).unwrap();
        let llm = ConfiguredLlmClient::new(
            Arc::new(StaticAuthStore {
                profile: sample_profile(),
            }),
            registry,
        );
        let host = MockHost {
            observation: sample_observation("repo and issue context are available"),
            verification: sample_observation("verification should not run"),
            actions: Mutex::new(Vec::new()),
        };

        let outcome = orchestrator
            .run_runtime_turn(&llm, &host, RuntimeTurnOptions::default())
            .await
            .unwrap();

        assert!(outcome.surfaced_gap.is_none());
        assert!(outcome.proposed_action.is_none());
        assert!(outcome.receipt.is_none());
        assert!(host.actions.lock().unwrap().is_empty());
        let request = outcome
            .github_action_request
            .expect("expected supervised GitHub request");
        assert_eq!(request.kind, SupervisedGithubActionKind::CommentIssue);
        assert_eq!(request.issue_number, Some(77));
        assert_eq!(request.repository.as_deref(), Some("jessybrenenstahl/FFR"));
    }

    #[tokio::test]
    async fn runtime_turn_records_host_verification_proof_level_in_receipt() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload);
        let snapshot = sample_plan_snapshot();
        orchestrator.adopt_plan(&snapshot).await.unwrap();

        let provider = Arc::new(StepwiseProvider {
            descriptor: local_mock_descriptor(),
            responses: vec![
                ChatResponse {
                    provider_id: "local-mock".into(),
                    model: "mock-model".into(),
                    content: vec![
                        ContentBlock::Text {
                            text: "Click into the editor.".into(),
                        },
                        ContentBlock::ToolCall {
                            id: "call-proof".into(),
                            name: HOST_ACTION_TOOL.into(),
                            arguments: serde_json::json!({
                                "kind": "click",
                                "x": 320,
                                "y": 220,
                                "button": "Left"
                            }),
                        },
                    ],
                },
                terminal_text_response("Editor focus changed."),
            ],
            last_request: Mutex::new(None),
            requests: Mutex::new(Vec::new()),
        });

        let mut registry = splcw_llm::ProviderRegistry::new();
        registry.register(provider.clone()).unwrap();
        let llm = ConfiguredLlmClient::new(
            Arc::new(StaticAuthStore {
                profile: sample_profile(),
            }),
            registry,
        );
        let host = MockHost {
            observation: sample_observation("editor is visible"),
            verification: ObservationFrame {
                structured_signals: vec![sample_post_action_verification_signal(
                    "click",
                    true,
                    serde_json::json!({
                        "window_changed": false,
                        "focused_control_changed": true,
                        "screenshot_changed": false,
                        "proof_level": "focused_control_focus_shift",
                    }),
                )],
                ..sample_observation("editor field is focused")
            },
            actions: Mutex::new(Vec::new()),
        };

        let outcome = orchestrator
            .run_runtime_turn(&llm, &host, RuntimeTurnOptions::default())
            .await
            .unwrap();

        let tool_outcome = &outcome.tool_outcome_history[0];
        assert_eq!(tool_outcome.verification_kind.as_deref(), Some("click"));
        assert_eq!(
            tool_outcome.verification_proof_level.as_deref(),
            Some("focused_control_focus_shift")
        );
        assert_eq!(tool_outcome.verification_ok, Some(true));
        assert_eq!(
            tool_outcome.verification_summary.as_deref(),
            Some("verify kind=click ok=true proof=focused_control_focus_shift")
        );
        assert!(
            outcome
                .receipt
                .as_ref()
                .unwrap()
                .changed
                .contains("proof=focused_control_focus_shift")
        );
    }

    #[tokio::test]
    async fn runtime_turn_handles_unstable_focus_verification_as_non_progress() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload);
        let snapshot = sample_plan_snapshot();
        orchestrator.adopt_plan(&snapshot).await.unwrap();

        let provider = Arc::new(StepwiseProvider {
            descriptor: local_mock_descriptor(),
            responses: vec![ChatResponse {
                provider_id: "local-mock".into(),
                model: "mock-model".into(),
                content: vec![
                    ContentBlock::Text {
                        text: "Focus the Codex window.".into(),
                    },
                    ContentBlock::ToolCall {
                        id: "call-focus".into(),
                        name: HOST_ACTION_TOOL.into(),
                        arguments: serde_json::json!({
                            "kind": "focus_window",
                            "title": "Codex"
                        }),
                    },
                ],
            }],
            last_request: Mutex::new(None),
            requests: Mutex::new(Vec::new()),
        });

        let mut registry = splcw_llm::ProviderRegistry::new();
        registry.register(provider).unwrap();
        let llm = ConfiguredLlmClient::new(
            Arc::new(StaticAuthStore {
                profile: sample_profile(),
            }),
            registry,
        );
        let host = MockHost {
            observation: sample_observation("focus is about to move"),
            verification: ObservationFrame {
                active_window: Some("Codex".into()),
                structured_signals: vec![sample_focus_verification_signal(
                    "Codex",
                    Some("Codex"),
                    true,
                    false,
                )],
                ..sample_observation("focus flickered onto Codex")
            },
            actions: Mutex::new(Vec::new()),
        };

        let outcome = orchestrator
            .run_runtime_turn(&llm, &host, RuntimeTurnOptions::default())
            .await
            .unwrap();

        let tool_outcome = &outcome.tool_outcome_history[0];
        assert_eq!(
            tool_outcome.verification_kind.as_deref(),
            Some("focus_window")
        );
        assert_eq!(
            tool_outcome.verification_proof_level.as_deref(),
            Some("unstable_focus")
        );
        assert_eq!(tool_outcome.verification_ok, Some(false));
        assert!(
            tool_outcome
                .contradiction
                .as_deref()
                .unwrap()
                .contains("did not stabilize")
        );
        assert!(outcome.surfaced_gap.is_some());
        assert!(
            outcome
                .receipt
                .as_ref()
                .unwrap()
                .changed
                .contains("proof=unstable_focus")
        );
    }

    #[tokio::test]
    async fn runtime_turn_accepts_host_verification_without_proof_level_for_backward_compat() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload);
        let snapshot = sample_plan_snapshot();
        orchestrator.adopt_plan(&snapshot).await.unwrap();

        let provider = Arc::new(StepwiseProvider {
            descriptor: local_mock_descriptor(),
            responses: vec![
                ChatResponse {
                    provider_id: "local-mock".into(),
                    model: "mock-model".into(),
                    content: vec![
                        ContentBlock::Text {
                            text: "Type the fallback greeting.".into(),
                        },
                        ContentBlock::ToolCall {
                            id: "call-legacy".into(),
                            name: HOST_ACTION_TOOL.into(),
                            arguments: serde_json::json!({
                                "kind": "type_text",
                                "text": "legacy proof path",
                                "submit": false
                            }),
                        },
                    ],
                },
                terminal_text_response("Legacy typing evidence accepted."),
            ],
            last_request: Mutex::new(None),
            requests: Mutex::new(Vec::new()),
        });

        let mut registry = splcw_llm::ProviderRegistry::new();
        registry.register(provider.clone()).unwrap();
        let llm = ConfiguredLlmClient::new(
            Arc::new(StaticAuthStore {
                profile: sample_profile(),
            }),
            registry,
        );
        let host = MockHost {
            observation: sample_observation("chat input is ready"),
            verification: ObservationFrame {
                structured_signals: vec![sample_post_action_verification_signal(
                    "type_text",
                    true,
                    serde_json::json!({
                        "window_changed": false,
                        "clipboard_changed": false,
                        "focused_control_value_changed": true,
                        "requested_text_observed": true
                    }),
                )],
                ..sample_observation("legacy greeting is visible")
            },
            actions: Mutex::new(Vec::new()),
        };

        let outcome = orchestrator
            .run_runtime_turn(&llm, &host, RuntimeTurnOptions::default())
            .await
            .unwrap();

        let tool_outcome = &outcome.tool_outcome_history[0];
        assert_eq!(tool_outcome.verification_kind.as_deref(), Some("type_text"));
        assert_eq!(tool_outcome.verification_proof_level, None);
        assert_eq!(tool_outcome.verification_ok, Some(true));
        assert_eq!(
            tool_outcome.verification_summary.as_deref(),
            Some("verify kind=type_text ok=true proof=legacy_host_signal")
        );
        assert!(outcome.surfaced_gap.is_none());
        assert!(
            outcome
                .receipt
                .as_ref()
                .unwrap()
                .changed
                .contains("proof=legacy_host_signal")
        );
    }

    #[test]
    fn detect_contradiction_focus_uses_focus_verification_signal_when_present() {
        let mut verification = sample_observation("focus verification");
        verification.active_window = Some("Some Other App".into());
        verification.structured_signals = vec![sample_focus_verification_signal(
            "Codex",
            Some("Codex"),
            true,
            true,
        )];

        let contradiction = detect_contradiction(
            &ProposedAction::FocusWindow {
                title: "Codex".into(),
            },
            &verification,
        );

        assert!(contradiction.is_none());
    }

    #[test]
    fn detect_contradiction_focus_requires_structured_signal() {
        let mut verification = sample_observation("focus verification");
        verification.active_window = Some("Codex".into());

        let contradiction = detect_contradiction(
            &ProposedAction::FocusWindow {
                title: "Codex".into(),
            },
            &verification,
        )
        .expect("missing focus verification signal should contradict");

        assert!(contradiction.contains("expected `focus_window` verification signal"));
    }

    #[test]
    fn detect_contradiction_focus_treats_signal_mismatch_as_contradiction_even_if_active_matches() {
        let mut verification = sample_observation("focus verification");
        verification.active_window = Some("Codex".into());
        verification.structured_signals = vec![sample_focus_verification_signal(
            "Codex",
            Some("Browser"),
            false,
            false,
        )];

        let contradiction = detect_contradiction(
            &ProposedAction::FocusWindow {
                title: "Codex".into(),
            },
            &verification,
        )
        .expect("focus verification signal should override active window heuristic");

        assert!(contradiction.contains("matched=false"));
        assert!(contradiction.contains("stable=false"));
    }

    #[test]
    fn runtime_detects_launch_process_contradiction_from_verification_signal_false() {
        let mut verification = sample_observation("launch verification");
        verification.structured_signals = vec![sample_post_action_verification_signal(
            "launch_process",
            false,
            serde_json::json!({
                "command": "notepad.exe",
                "expected_process": "notepad",
            }),
        )];

        let contradiction = detect_contradiction(
            &ProposedAction::LaunchProcess {
                command: "notepad.exe".into(),
                args: vec![],
            },
            &verification,
        )
        .expect("failed launch verification should contradict");

        assert!(contradiction.contains("ok=false"));
    }

    #[test]
    fn runtime_rejects_launch_process_signal_without_fresh_process_delta() {
        let mut verification = sample_observation("launch verification");
        verification.structured_signals = vec![sample_post_action_verification_signal(
            "launch_process",
            true,
            serde_json::json!({
                "command": "notepad.exe",
                "expected_process": "notepad",
                "observed_process": "notepad",
                "observed_process_id": 700,
                "spawned_pid": null,
                "spawned_pid_present": false,
                "new_process_detected": false,
                "proof_level": "preexisting_process_only",
            }),
        )];

        let contradiction = detect_contradiction(
            &ProposedAction::LaunchProcess {
                command: "notepad.exe".into(),
                args: vec![],
            },
            &verification,
        )
        .expect("launch verification without a fresh process delta should contradict");

        assert!(contradiction.contains("fresh process delta"));
        assert!(contradiction.contains("preexisting_process_only"));
    }

    #[test]
    fn runtime_accepts_launch_process_signal_with_spawned_child_lineage() {
        let mut verification = sample_observation("launch verification");
        verification.structured_signals = vec![sample_post_action_verification_signal(
            "launch_process",
            true,
            serde_json::json!({
                "command": "launcher.exe",
                "expected_process": "launcher",
                "observed_process": "FinalApp.exe",
                "observed_process_id": 333,
                "spawned_pid": 222,
                "spawned_pid_present": false,
                "spawned_child_detected": true,
                "new_process_detected": false,
                "proof_level": "spawned_child_detected",
            }),
        )];

        let contradiction = detect_contradiction(
            &ProposedAction::LaunchProcess {
                command: "launcher.exe".into(),
                args: vec![],
            },
            &verification,
        );

        assert!(contradiction.is_none());
    }

    #[test]
    fn runtime_accepts_launch_process_signal_with_spawned_pid_presence() {
        let mut verification = sample_observation("launch verification");
        verification.structured_signals = vec![sample_post_action_verification_signal(
            "launch_process",
            true,
            serde_json::json!({
                "command": "Codex.app",
                "expected_process": "Codex",
                "observed_process": "Codex",
                "observed_process_id": 444,
                "spawned_pid": 444,
                "spawned_pid_present": true,
                "spawned_child_detected": false,
                "new_process_detected": false,
                "proof_level": "spawned_pid_still_present",
            }),
        )];

        let contradiction = detect_contradiction(
            &ProposedAction::LaunchProcess {
                command: "Codex.app".into(),
                args: vec![],
            },
            &verification,
        );

        assert!(contradiction.is_none());
    }

    #[test]
    fn runtime_accepts_wait_for_signal_when_matched() {
        let mut verification = sample_observation("wait verification");
        verification.structured_signals = vec![sample_post_action_verification_signal(
            "wait_for",
            true,
            serde_json::json!({
                "signal": "window:Browser",
                "matched": true,
                "attempts": 2,
                "elapsed_ms": 300,
                "timed_out": false,
                "proof_level": "wait_signal_match",
            }),
        )];

        let contradiction = detect_contradiction(
            &ProposedAction::WaitFor {
                signal: "window:Browser".into(),
                timeout_ms: 1_000,
            },
            &verification,
        );

        assert!(contradiction.is_none());
    }

    #[test]
    fn runtime_detects_wait_for_contradiction_from_timeout_signal() {
        let mut verification = sample_observation("wait verification");
        verification.structured_signals = vec![sample_post_action_verification_signal(
            "wait_for",
            false,
            serde_json::json!({
                "signal": "clipboard:done",
                "matched": false,
                "attempts": 4,
                "elapsed_ms": 1000,
                "timed_out": true,
                "proof_level": "wait_signal_timeout",
            }),
        )];

        let contradiction = detect_contradiction(
            &ProposedAction::WaitFor {
                signal: "clipboard:done".into(),
                timeout_ms: 1_000,
            },
            &verification,
        )
        .expect("timed-out wait signal should contradict");

        assert!(contradiction.contains("before timeout"));
        assert!(contradiction.contains("wait_signal_timeout"));
    }

    #[test]
    fn runtime_wait_for_requires_structured_signal() {
        let mut verification = sample_observation("wait verification");
        verification.clipboard_text = Some("all done".into());

        let contradiction = detect_contradiction(
            &ProposedAction::WaitFor {
                signal: "clipboard:done".into(),
                timeout_ms: 1_000,
            },
            &verification,
        )
        .expect("missing wait verification signal should contradict");

        assert!(contradiction.contains("expected `wait_for` verification signal"));
    }

    #[test]
    fn runtime_capture_observation_requires_structured_signal() {
        let verification = sample_observation("capture verification");

        let contradiction =
            detect_contradiction(&ProposedAction::CaptureObservation, &verification)
                .expect("missing capture verification signal should contradict");

        assert!(contradiction.contains("expected `capture_observation` verification signal"));
    }

    #[test]
    fn runtime_capture_observation_accepts_readable_signal() {
        let mut verification = sample_observation("capture verification");
        verification.structured_signals = vec![sample_post_action_verification_signal(
            "capture_observation",
            true,
            serde_json::json!({
                "screenshot_present": true,
                "screenshot_readable": true,
                "proof_level": "screenshot_readable",
            }),
        )];

        let contradiction =
            detect_contradiction(&ProposedAction::CaptureObservation, &verification);

        assert!(contradiction.is_none());
    }

    #[test]
    fn runtime_rejects_capture_observation_signal_without_readable_artifact() {
        let mut verification = sample_observation("capture verification");
        verification.structured_signals = vec![sample_post_action_verification_signal(
            "capture_observation",
            true,
            serde_json::json!({
                "screenshot_present": true,
                "screenshot_readable": false,
                "proof_level": "screenshot_unreadable",
            }),
        )];

        let contradiction =
            detect_contradiction(&ProposedAction::CaptureObservation, &verification)
                .expect("weak capture verification should contradict");

        assert!(contradiction.contains("readable screenshot artifact"));
        assert!(contradiction.contains("screenshot_unreadable"));
    }

    #[test]
    fn runtime_marks_click_verification_contradiction_when_signal_missing() {
        let verification = sample_observation("click verification");

        let contradiction = detect_contradiction(
            &ProposedAction::Click {
                x: 100,
                y: 200,
                button: MouseButton::Left,
            },
            &verification,
        )
        .expect("missing click verification signal should contradict");

        assert!(contradiction.contains("expected `click` verification signal"));
    }

    #[test]
    fn runtime_rejects_click_signal_without_observable_effect_even_if_ok() {
        let mut verification = sample_observation("click verification");
        verification.structured_signals = vec![sample_post_action_verification_signal(
            "click",
            true,
            serde_json::json!({
                "expected_x": 100,
                "expected_y": 200,
                "observed_x": 100,
                "observed_y": 200,
                "tolerance_px": 4,
                "window_changed": false,
                "focused_control_changed": false,
                "screenshot_changed": false,
                "proof_level": "pointer_target_only",
            }),
        )];

        let contradiction = detect_contradiction(
            &ProposedAction::Click {
                x: 100,
                y: 200,
                button: MouseButton::Left,
            },
            &verification,
        )
        .expect("pointer-target-only click verification should contradict");

        assert!(contradiction.contains("visible or focus effect"));
    }

    #[test]
    fn runtime_accepts_click_signal_with_screenshot_delta() {
        let mut verification = sample_observation("click verification");
        verification.structured_signals = vec![sample_post_action_verification_signal(
            "click",
            true,
            serde_json::json!({
                "expected_x": 100,
                "expected_y": 200,
                "observed_x": 100,
                "observed_y": 200,
                "tolerance_px": 4,
                "window_changed": false,
                "focused_control_changed": false,
                "screenshot_changed": true,
                "proof_level": "viewport_delta",
            }),
        )];

        let contradiction = detect_contradiction(
            &ProposedAction::Click {
                x: 100,
                y: 200,
                button: MouseButton::Left,
            },
            &verification,
        );

        assert!(contradiction.is_none());
    }

    #[test]
    fn runtime_accepts_click_signal_with_focused_control_shift() {
        let mut verification = sample_observation("click verification");
        verification.structured_signals = vec![sample_post_action_verification_signal(
            "click",
            true,
            serde_json::json!({
                "expected_x": 100,
                "expected_y": 200,
                "observed_x": 100,
                "observed_y": 200,
                "tolerance_px": 4,
                "window_changed": false,
                "focused_control_changed": true,
                "screenshot_changed": false,
                "proof_level": "focused_control_focus_shift",
            }),
        )];

        let contradiction = detect_contradiction(
            &ProposedAction::Click {
                x: 100,
                y: 200,
                button: MouseButton::Left,
            },
            &verification,
        );

        assert!(contradiction.is_none());
    }

    #[test]
    fn runtime_marks_scroll_verification_contradiction_when_signal_missing() {
        let verification = sample_observation("scroll verification");

        let contradiction =
            detect_contradiction(&ProposedAction::Scroll { delta: 400 }, &verification)
                .expect("missing scroll verification signal should contradict");

        assert!(contradiction.contains("expected `scroll` verification signal"));
    }

    #[test]
    fn runtime_rejects_type_text_focus_anchor_without_observable_effect() {
        let mut verification = sample_observation("type_text verification");
        verification.structured_signals = vec![sample_post_action_verification_signal(
            "type_text",
            true,
            serde_json::json!({
                "focus_preserved": true,
                "window_changed": false,
                "clipboard_changed": false,
                "focused_control_changed": false,
                "focused_control_value_changed": false,
                "requested_text_observed": false,
                "proof_level": "focus_anchor",
            }),
        )];

        let contradiction = detect_contradiction(
            &ProposedAction::TypeText {
                text: "hello".into(),
                submit: false,
            },
            &verification,
        )
        .expect("focus-anchor-only type_text verification should contradict");

        assert!(contradiction.contains("observable input effect"));
        assert!(contradiction.contains("anchor preservation"));
    }

    #[test]
    fn runtime_accepts_type_text_verification_with_focused_control_delta() {
        let mut verification = sample_observation("type_text verification");
        verification.structured_signals = vec![sample_post_action_verification_signal(
            "type_text",
            true,
            serde_json::json!({
                "focus_preserved": true,
                "window_changed": false,
                "clipboard_changed": false,
                "focused_control_changed": false,
                "focused_control_value_changed": true,
                "requested_text_observed": true,
                "proof_level": "focused_control_text_match",
            }),
        )];

        let contradiction = detect_contradiction(
            &ProposedAction::TypeText {
                text: "hello".into(),
                submit: false,
            },
            &verification,
        );

        assert!(contradiction.is_none());
    }

    #[test]
    fn runtime_accepts_type_text_verification_with_screenshot_delta() {
        let mut verification = sample_observation("type_text verification");
        verification.structured_signals = vec![sample_post_action_verification_signal(
            "type_text",
            true,
            serde_json::json!({
                "focus_preserved": true,
                "window_changed": false,
                "clipboard_changed": false,
                "focused_control_changed": false,
                "focused_control_value_changed": false,
                "requested_text_observed": false,
                "screenshot_changed": true,
                "proof_level": "viewport_delta",
            }),
        )];

        let contradiction = detect_contradiction(
            &ProposedAction::TypeText {
                text: "hello".into(),
                submit: false,
            },
            &verification,
        );

        assert!(contradiction.is_none());
    }

    #[test]
    fn runtime_accepts_scroll_verification_with_viewport_delta() {
        let mut verification = sample_observation("scroll verification");
        verification.structured_signals = vec![sample_post_action_verification_signal(
            "scroll",
            true,
            serde_json::json!({
                "window_changed": false,
                "focused_control_changed": false,
                "screenshot_changed": true,
                "proof_level": "viewport_delta",
            }),
        )];

        let contradiction =
            detect_contradiction(&ProposedAction::Scroll { delta: 400 }, &verification);

        assert!(contradiction.is_none());
    }

    #[test]
    fn runtime_rejects_scroll_signal_without_observable_effect_even_if_ok() {
        let mut verification = sample_observation("scroll verification");
        verification.structured_signals = vec![sample_post_action_verification_signal(
            "scroll",
            true,
            serde_json::json!({
                "window_changed": false,
                "focused_control_changed": false,
                "screenshot_changed": false,
                "proof_level": "none",
            }),
        )];

        let contradiction =
            detect_contradiction(&ProposedAction::Scroll { delta: 400 }, &verification)
                .expect("scroll verification without effect should contradict");

        assert!(contradiction.contains("viewport or focus effect"));
    }

    #[test]
    fn runtime_rejects_hotkey_focus_anchor_without_effect_even_if_signal_is_ok() {
        let mut verification = sample_observation("hotkey verification");
        verification.structured_signals = vec![sample_post_action_verification_signal(
            "hotkey",
            true,
            serde_json::json!({
                "focus_preserved": true,
                "window_changed": false,
                "clipboard_changed": false,
                "focused_control_changed": false,
                "focused_control_value_changed": false,
                "proof_level": "focus_anchor",
            }),
        )];

        let contradiction = detect_contradiction(
            &ProposedAction::Hotkey {
                chord: "ctrl+l".into(),
            },
            &verification,
        )
        .expect("focus-anchor-only hotkey verification should still contradict");

        assert!(contradiction.contains("observable effect"));
        assert!(contradiction.contains("anchor preservation"));
    }

    #[test]
    fn runtime_rejects_hotkey_verification_with_control_shift_only() {
        let mut verification = sample_observation("hotkey verification");
        verification.structured_signals = vec![sample_post_action_verification_signal(
            "hotkey",
            true,
            serde_json::json!({
                "focus_preserved": true,
                "window_changed": false,
                "clipboard_changed": false,
                "focused_control_changed": true,
                "focused_control_value_changed": false,
                "proof_level": "focused_control_focus_shift",
            }),
        )];

        let contradiction = detect_contradiction(
            &ProposedAction::Hotkey {
                chord: "tab".into(),
            },
            &verification,
        )
        .expect("control-shift-only hotkey verification should contradict");

        assert!(contradiction.contains("observable effect"));
        assert!(contradiction.contains("focused_control_focus_shift"));
    }

    #[test]
    fn runtime_accepts_hotkey_verification_with_screenshot_delta() {
        let mut verification = sample_observation("hotkey verification");
        verification.structured_signals = vec![sample_post_action_verification_signal(
            "hotkey",
            true,
            serde_json::json!({
                "focus_preserved": true,
                "window_changed": false,
                "clipboard_changed": false,
                "focused_control_changed": false,
                "focused_control_value_changed": false,
                "screenshot_changed": true,
                "proof_level": "viewport_delta",
            }),
        )];

        let contradiction = detect_contradiction(
            &ProposedAction::Hotkey {
                chord: "cmd+l".into(),
            },
            &verification,
        );

        assert!(contradiction.is_none());
    }

    #[test]
    fn runtime_detects_hotkey_contradiction_from_failed_keyboard_verification() {
        let mut verification = sample_observation("hotkey verification");
        verification.structured_signals = vec![sample_post_action_verification_signal(
            "hotkey",
            false,
            serde_json::json!({
                "focus_preserved": false,
                "window_changed": false,
                "clipboard_changed": false,
                "focused_control_changed": false,
                "focused_control_value_changed": false,
                "proof_level": "none",
            }),
        )];

        let contradiction = detect_contradiction(
            &ProposedAction::Hotkey {
                chord: "ctrl+l".into(),
            },
            &verification,
        )
        .expect("failed hotkey verification should contradict");

        assert!(contradiction.contains("observable effect"));
    }

    #[tokio::test]
    async fn runtime_turn_surfaces_gap_when_launch_process_verification_fails() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload.clone());
        let snapshot = sample_plan_snapshot();
        orchestrator.adopt_plan(&snapshot).await.unwrap();

        let provider = Arc::new(StepwiseProvider {
            descriptor: local_mock_descriptor(),
            responses: vec![ChatResponse {
                provider_id: "local-mock".into(),
                model: "mock-model".into(),
                content: vec![
                    ContentBlock::Text {
                        text: "Launch notepad.".into(),
                    },
                    ContentBlock::ToolCall {
                        id: "call-1".into(),
                        name: HOST_ACTION_TOOL.into(),
                        arguments: serde_json::json!({
                            "kind": "launch_process",
                            "command": "notepad.exe",
                            "args": []
                        }),
                    },
                ],
            }],
            last_request: Mutex::new(None),
            requests: Mutex::new(Vec::new()),
        });

        let mut registry = splcw_llm::ProviderRegistry::new();
        registry.register(provider.clone()).unwrap();
        let llm = ConfiguredLlmClient::new(
            Arc::new(StaticAuthStore {
                profile: sample_profile(),
            }),
            registry,
        );
        let host = MockHost {
            observation: sample_observation("desktop ready"),
            verification: ObservationFrame {
                structured_signals: vec![sample_post_action_verification_signal(
                    "launch_process",
                    false,
                    serde_json::json!({
                        "command": "notepad.exe",
                        "expected_process": "notepad",
                    }),
                )],
                ..sample_observation("launch verification failed")
            },
            actions: Mutex::new(Vec::new()),
        };

        let outcome = orchestrator
            .run_runtime_turn(&llm, &host, RuntimeTurnOptions::default())
            .await
            .unwrap();

        let gap = outcome
            .surfaced_gap
            .expect("launch contradiction should surface a capability gap");
        assert!(
            gap.title
                .contains("Post-action verification contradicted launch")
        );
        assert_eq!(host.actions.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn runtime_turn_surfaces_gap_when_launch_process_signal_is_missing() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload.clone());
        let snapshot = sample_plan_snapshot();
        orchestrator.adopt_plan(&snapshot).await.unwrap();

        let provider = Arc::new(StepwiseProvider {
            descriptor: local_mock_descriptor(),
            responses: vec![ChatResponse {
                provider_id: "local-mock".into(),
                model: "mock-model".into(),
                content: vec![
                    ContentBlock::Text {
                        text: "Launch Codex.".into(),
                    },
                    ContentBlock::ToolCall {
                        id: "call-1".into(),
                        name: HOST_ACTION_TOOL.into(),
                        arguments: serde_json::json!({
                            "kind": "launch_process",
                            "command": "Codex.app",
                            "args": []
                        }),
                    },
                ],
            }],
            last_request: Mutex::new(None),
            requests: Mutex::new(Vec::new()),
        });

        let mut registry = splcw_llm::ProviderRegistry::new();
        registry.register(provider.clone()).unwrap();
        let llm = ConfiguredLlmClient::new(
            Arc::new(StaticAuthStore {
                profile: sample_profile(),
            }),
            registry,
        );
        let host = MockHost {
            observation: sample_observation("desktop ready"),
            verification: sample_observation("launch verification lacked a signal"),
            actions: Mutex::new(Vec::new()),
        };

        let outcome = orchestrator
            .run_runtime_turn(&llm, &host, RuntimeTurnOptions::default())
            .await
            .unwrap();

        let gap = outcome
            .surfaced_gap
            .expect("missing launch verification signal should surface a capability gap");
        assert!(
            gap.title
                .contains("Post-action verification contradicted launch")
        );
        assert_eq!(host.actions.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn runtime_turn_surfaces_gap_when_capture_signal_is_missing() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload.clone());
        let snapshot = sample_plan_snapshot();
        orchestrator.adopt_plan(&snapshot).await.unwrap();

        let provider = Arc::new(StepwiseProvider {
            descriptor: local_mock_descriptor(),
            responses: vec![capture_observation_response("call-capture-missing-signal")],
            last_request: Mutex::new(None),
            requests: Mutex::new(Vec::new()),
        });
        let mut registry = splcw_llm::ProviderRegistry::new();
        registry.register(provider).unwrap();
        let llm = ConfiguredLlmClient::new(
            Arc::new(StaticAuthStore {
                profile: sample_profile(),
            }),
            registry,
        );
        let host = MockHost {
            observation: sample_observation("desktop ready"),
            verification: sample_observation("capture verification missing signal"),
            actions: Mutex::new(Vec::new()),
        };

        let outcome = orchestrator
            .run_runtime_turn(&llm, &host, RuntimeTurnOptions::default())
            .await
            .unwrap();

        let gap = outcome
            .surfaced_gap
            .expect("missing capture verification signal should surface a gap");
        assert!(
            gap.title
                .contains("Post-action verification contradicted capture")
        );
        assert_eq!(host.actions.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn runtime_turn_surfaces_gap_when_wait_for_times_out() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload.clone());
        let snapshot = sample_plan_snapshot();
        orchestrator.adopt_plan(&snapshot).await.unwrap();

        let provider = Arc::new(StepwiseProvider {
            descriptor: local_mock_descriptor(),
            responses: vec![ChatResponse {
                provider_id: "local-mock".into(),
                model: "mock-model".into(),
                content: vec![
                    ContentBlock::Text {
                        text: "Wait for the clipboard marker.".into(),
                    },
                    ContentBlock::ToolCall {
                        id: "call-wait".into(),
                        name: HOST_ACTION_TOOL.into(),
                        arguments: serde_json::json!({
                            "kind": "wait_for",
                            "signal": "clipboard:done",
                            "timeout_ms": 1000
                        }),
                    },
                ],
            }],
            last_request: Mutex::new(None),
            requests: Mutex::new(Vec::new()),
        });

        let mut registry = splcw_llm::ProviderRegistry::new();
        registry.register(provider).unwrap();
        let llm = ConfiguredLlmClient::new(
            Arc::new(StaticAuthStore {
                profile: sample_profile(),
            }),
            registry,
        );
        let host = MockHost {
            observation: sample_observation("desktop ready"),
            verification: ObservationFrame {
                structured_signals: vec![sample_post_action_verification_signal(
                    "wait_for",
                    false,
                    serde_json::json!({
                        "signal": "clipboard:done",
                        "matched": false,
                        "attempts": 4,
                        "elapsed_ms": 1000,
                        "timed_out": true,
                        "proof_level": "wait_signal_timeout",
                    }),
                )],
                ..sample_observation("wait verification failed")
            },
            actions: Mutex::new(Vec::new()),
        };

        let outcome = orchestrator
            .run_runtime_turn(&llm, &host, RuntimeTurnOptions::default())
            .await
            .unwrap();

        let gap = outcome
            .surfaced_gap
            .expect("wait timeout contradiction should surface a capability gap");
        assert!(
            gap.title
                .contains("Post-action verification contradicted wait")
        );
        assert_eq!(host.actions.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn runtime_turn_surfaces_gap_when_host_enact_fails() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload);
        let snapshot = sample_plan_snapshot();
        orchestrator.adopt_plan(&snapshot).await.unwrap();

        let provider = Arc::new(StepwiseProvider {
            descriptor: local_mock_descriptor(),
            responses: vec![ChatResponse {
                provider_id: "local-mock".into(),
                model: "mock-model".into(),
                content: vec![ContentBlock::ToolCall {
                    id: "call-enact-fail".into(),
                    name: HOST_ACTION_TOOL.into(),
                    arguments: serde_json::json!({
                        "kind": "capture_observation"
                    }),
                }],
            }],
            last_request: Mutex::new(None),
            requests: Mutex::new(Vec::new()),
        });
        let mut registry = splcw_llm::ProviderRegistry::new();
        registry.register(provider).unwrap();
        let llm = ConfiguredLlmClient::new(
            Arc::new(StaticAuthStore {
                profile: sample_profile(),
            }),
            registry,
        );
        let host = FailingHost {
            observation: sample_observation("enact fail observation"),
            verification: sample_observation("enact fail verification"),
            actions: Mutex::new(Vec::new()),
            enact_error: Some("host enact exploded".into()),
            verify_error: None,
        };

        let outcome = orchestrator
            .run_runtime_turn(&llm, &host, RuntimeTurnOptions::default())
            .await
            .unwrap();

        let gap = outcome
            .surfaced_gap
            .expect("host enact failure should surface a gap");
        assert!(gap.title.contains("Host execution failed"));
        assert!(outcome.receipt.is_none());
        assert!(outcome.execution.is_none());
        assert_eq!(
            outcome.proposed_action,
            Some(ProposedAction::CaptureObservation)
        );
        assert_eq!(host.actions.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn runtime_turn_surfaces_gap_when_host_verify_fails() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload);
        let snapshot = sample_plan_snapshot();
        orchestrator.adopt_plan(&snapshot).await.unwrap();

        let provider = Arc::new(StepwiseProvider {
            descriptor: local_mock_descriptor(),
            responses: vec![ChatResponse {
                provider_id: "local-mock".into(),
                model: "mock-model".into(),
                content: vec![ContentBlock::ToolCall {
                    id: "call-verify-fail".into(),
                    name: HOST_ACTION_TOOL.into(),
                    arguments: serde_json::json!({
                        "kind": "capture_observation"
                    }),
                }],
            }],
            last_request: Mutex::new(None),
            requests: Mutex::new(Vec::new()),
        });
        let mut registry = splcw_llm::ProviderRegistry::new();
        registry.register(provider).unwrap();
        let llm = ConfiguredLlmClient::new(
            Arc::new(StaticAuthStore {
                profile: sample_profile(),
            }),
            registry,
        );
        let host = FailingHost {
            observation: sample_observation("verify fail observation"),
            verification: sample_observation("verify fail verification"),
            actions: Mutex::new(Vec::new()),
            enact_error: None,
            verify_error: Some("host verify exploded".into()),
        };

        let outcome = orchestrator
            .run_runtime_turn(&llm, &host, RuntimeTurnOptions::default())
            .await
            .unwrap();

        let gap = outcome
            .surfaced_gap
            .expect("host verify failure should surface a gap");
        assert!(gap.title.contains("Host verification failed"));
        assert!(outcome.receipt.is_none());
        assert!(outcome.execution.is_some());
        assert_eq!(
            outcome.proposed_action,
            Some(ProposedAction::CaptureObservation)
        );
        assert_eq!(host.actions.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn runtime_turn_surfaces_gap_when_host_observe_times_out() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload);
        orchestrator
            .adopt_plan(&sample_plan_snapshot())
            .await
            .unwrap();

        let provider = Arc::new(StepwiseProvider {
            descriptor: local_mock_descriptor(),
            responses: vec![terminal_text_response("unused")],
            last_request: Mutex::new(None),
            requests: Mutex::new(Vec::new()),
        });
        let mut registry = splcw_llm::ProviderRegistry::new();
        registry.register(provider.clone()).unwrap();
        let llm = ConfiguredLlmClient::new(
            Arc::new(StaticAuthStore {
                profile: sample_profile(),
            }),
            registry,
        );
        let host = SlowHost {
            observation: sample_observation("slow observation"),
            verification: sample_observation("unused"),
            actions: Mutex::new(Vec::new()),
            observe_delay: Duration::from_millis(100),
            enact_delay: Duration::from_millis(0),
            verify_delay: Duration::from_millis(0),
        };

        let outcome = orchestrator
            .run_runtime_turn(
                &llm,
                &host,
                RuntimeTurnOptions {
                    host_observe_timeout: Duration::from_millis(10),
                    ..RuntimeTurnOptions::default()
                },
            )
            .await
            .unwrap();

        let gap = outcome
            .surfaced_gap
            .expect("observe timeout should surface a capability gap");
        assert!(gap.title.contains("Host observation timed out"));
        assert!(outcome.receipt.is_none());
        assert!(host.actions.lock().unwrap().is_empty());
        assert!(provider.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn runtime_turn_surfaces_gap_when_host_enact_times_out() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload);
        orchestrator
            .adopt_plan(&sample_plan_snapshot())
            .await
            .unwrap();

        let provider = Arc::new(StepwiseProvider {
            descriptor: local_mock_descriptor(),
            responses: vec![capture_observation_response("call-enact-timeout")],
            last_request: Mutex::new(None),
            requests: Mutex::new(Vec::new()),
        });
        let mut registry = splcw_llm::ProviderRegistry::new();
        registry.register(provider).unwrap();
        let llm = ConfiguredLlmClient::new(
            Arc::new(StaticAuthStore {
                profile: sample_profile(),
            }),
            registry,
        );
        let host = SlowHost {
            observation: sample_observation("enact timeout observation"),
            verification: sample_observation("unused"),
            actions: Mutex::new(Vec::new()),
            observe_delay: Duration::from_millis(0),
            enact_delay: Duration::from_millis(100),
            verify_delay: Duration::from_millis(0),
        };

        let outcome = orchestrator
            .run_runtime_turn(
                &llm,
                &host,
                RuntimeTurnOptions {
                    host_enact_timeout: Duration::from_millis(10),
                    ..RuntimeTurnOptions::default()
                },
            )
            .await
            .unwrap();

        let gap = outcome
            .surfaced_gap
            .expect("enact timeout should surface a capability gap");
        assert!(gap.title.contains("Host execution timed out"));
        assert!(outcome.execution.is_none());
        assert_eq!(
            outcome.proposed_action,
            Some(ProposedAction::CaptureObservation)
        );
        assert_eq!(host.actions.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn runtime_turn_surfaces_gap_when_host_verify_times_out() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload);
        orchestrator
            .adopt_plan(&sample_plan_snapshot())
            .await
            .unwrap();

        let provider = Arc::new(StepwiseProvider {
            descriptor: local_mock_descriptor(),
            responses: vec![capture_observation_response("call-verify-timeout")],
            last_request: Mutex::new(None),
            requests: Mutex::new(Vec::new()),
        });
        let mut registry = splcw_llm::ProviderRegistry::new();
        registry.register(provider).unwrap();
        let llm = ConfiguredLlmClient::new(
            Arc::new(StaticAuthStore {
                profile: sample_profile(),
            }),
            registry,
        );
        let host = SlowHost {
            observation: sample_observation("verify timeout observation"),
            verification: sample_observation("verify timeout verification"),
            actions: Mutex::new(Vec::new()),
            observe_delay: Duration::from_millis(0),
            enact_delay: Duration::from_millis(0),
            verify_delay: Duration::from_millis(100),
        };

        let outcome = orchestrator
            .run_runtime_turn(
                &llm,
                &host,
                RuntimeTurnOptions {
                    host_verify_timeout: Duration::from_millis(10),
                    ..RuntimeTurnOptions::default()
                },
            )
            .await
            .unwrap();

        let gap = outcome
            .surfaced_gap
            .expect("verify timeout should surface a capability gap");
        assert!(gap.title.contains("Host verification timed out"));
        assert!(outcome.execution.is_some());
        assert_eq!(
            outcome.proposed_action,
            Some(ProposedAction::CaptureObservation)
        );
        assert_eq!(host.actions.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn runtime_turn_surfaces_capability_gap_without_enacting() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload.clone());
        orchestrator
            .adopt_plan(&sample_plan_snapshot())
            .await
            .unwrap();

        let provider = Arc::new(StaticProvider {
            descriptor: ProviderDescriptor {
                id: "local-mock".into(),
                provider: ProviderKind::Local,
                display_name: "Local Mock".into(),
                auth_modes: vec![AuthMode::Local],
                default_model: "mock-model".into(),
            },
            response: ChatResponse {
                provider_id: "local-mock".into(),
                model: "mock-model".into(),
                content: vec![
                    ContentBlock::Text {
                        text: "The runtime cannot localize the target control safely.".into(),
                    },
                    ContentBlock::ToolCall {
                        id: "gap-1".into(),
                        name: CAPABILITY_GAP_TOOL.into(),
                        arguments: serde_json::json!({
                            "title": "Need stable selector acquisition for the target control",
                            "permanent_fix_target": "Add a selector-backed perception hook before retrying",
                            "notes": ["OCR alone is not sufficient"]
                        }),
                    },
                ],
            },
            last_request: Mutex::new(None),
        });

        let mut registry = splcw_llm::ProviderRegistry::new();
        registry.register(provider).unwrap();
        let llm = ConfiguredLlmClient::new(
            Arc::new(StaticAuthStore {
                profile: sample_profile(),
            }),
            registry,
        );
        let host = MockHost {
            observation: sample_observation("target control is ambiguous"),
            verification: sample_observation("unused"),
            actions: Mutex::new(Vec::new()),
        };

        let outcome = orchestrator
            .run_runtime_turn(&llm, &host, RuntimeTurnOptions::default())
            .await
            .unwrap();

        assert!(outcome.proposed_action.is_none());
        assert!(outcome.receipt.is_none());
        assert!(outcome.surfaced_gap.is_some());
        assert_eq!(host.actions.lock().unwrap().len(), 0);
        let gaps = store
            .list_capability_gaps(Some(CapabilityGapStatus::Open))
            .await
            .unwrap();
        assert_eq!(gaps.len(), 1);
        assert_eq!(*offload.current_surfaces.lock().unwrap(), 2);
        assert_eq!(*offload.checkpoints.lock().unwrap(), 2);
        assert_eq!(
            gaps[0].title,
            "Need stable selector acquisition for the target control"
        );
    }

    #[tokio::test]
    async fn runtime_turn_serializes_shared_lanes() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload.clone());
        orchestrator
            .adopt_plan(&sample_plan_snapshot())
            .await
            .unwrap();

        let provider = Arc::new(StepwiseProvider {
            descriptor: local_mock_descriptor(),
            responses: vec![
                capture_observation_response("call-serial"),
                terminal_text_response("Serialized observation captured."),
            ],
            last_request: Mutex::new(None),
            requests: Mutex::new(Vec::new()),
        });

        let mut registry = splcw_llm::ProviderRegistry::new();
        registry.register(provider).unwrap();
        let llm = ConfiguredLlmClient::new(
            Arc::new(StaticAuthStore {
                profile: sample_profile(),
            }),
            registry,
        );
        let host = SerializingHost {
            observation: sample_observation("serialized observation"),
            verification: sample_capture_observation_verification("serialized verification"),
            in_flight: AtomicUsize::new(0),
            max_in_flight: AtomicUsize::new(0),
            actions: Mutex::new(Vec::new()),
        };
        let lanes = RuntimeLanes::new("session-runtime", "global-runtime");

        let first = orchestrator.run_runtime_turn(
            &llm,
            &host,
            RuntimeTurnOptions {
                lanes: Some(lanes.clone()),
                ..RuntimeTurnOptions::default()
            },
        );
        let second = orchestrator.run_runtime_turn(
            &llm,
            &host,
            RuntimeTurnOptions {
                lanes: Some(lanes.clone()),
                ..RuntimeTurnOptions::default()
            },
        );

        let (first, second) = tokio::join!(first, second);
        let first = first.unwrap();
        let second = second.unwrap();

        assert_eq!(first.acquired_lanes, Some(lanes.clone()));
        assert_eq!(second.acquired_lanes, Some(lanes));
        assert_eq!(host.max_in_flight.load(Ordering::SeqCst), 1);
        assert_eq!(store.list_recent_receipts(10).await.unwrap().len(), 2);
        assert_eq!(host.actions.lock().unwrap().len(), 2);
        assert_eq!(*offload.current_surfaces.lock().unwrap(), 3);
        assert_eq!(*offload.checkpoints.lock().unwrap(), 3);
    }

    #[tokio::test]
    async fn acquire_runtime_lanes_times_out_when_lane_is_held() {
        let lanes = RuntimeLanes::new("session-timeout", "global-timeout");
        let held = acquire_runtime_lanes_with_timeout(&lanes, Duration::from_secs(1))
            .await
            .unwrap();
        let error = acquire_runtime_lanes_with_timeout(&lanes, Duration::from_millis(50))
            .await
            .unwrap_err();

        assert!(format!("{error:#}").contains("timed out acquiring session lane"));
        drop(held);
    }

    #[tokio::test]
    async fn runtime_turn_surfaces_provider_failures_as_capability_gap() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload);
        orchestrator
            .adopt_plan(&sample_plan_snapshot())
            .await
            .unwrap();

        let provider = Arc::new(FailingProvider {
            descriptor: local_mock_descriptor(),
            error_message: "status_code=503 upstream unavailable".into(),
        });

        let mut registry = splcw_llm::ProviderRegistry::new();
        registry.register(provider).unwrap();
        let llm = ConfiguredLlmClient::new(
            Arc::new(StaticAuthStore {
                profile: sample_profile(),
            }),
            registry,
        );
        let host = MockHost {
            observation: sample_observation("provider failure observation"),
            verification: sample_observation("unused"),
            actions: Mutex::new(Vec::new()),
        };

        let outcome = orchestrator
            .run_runtime_turn(&llm, &host, RuntimeTurnOptions::default())
            .await
            .unwrap();

        assert!(outcome.surfaced_gap.is_some());
        assert!(outcome.receipt.is_none());
        assert_eq!(host.actions.lock().unwrap().len(), 0);
        assert!(matches!(
            outcome.response.content.first(),
            Some(ContentBlock::Text { text }) if text.contains("[provider failure]")
        ));
    }

    #[tokio::test]
    async fn runtime_turn_supports_bounded_multi_step_tool_loops() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload.clone());
        orchestrator
            .adopt_plan(&sample_plan_snapshot())
            .await
            .unwrap();

        let provider = Arc::new(StepwiseProvider {
            descriptor: local_mock_descriptor(),
            responses: vec![
                ChatResponse {
                    provider_id: "local-mock".into(),
                    model: "mock-model".into(),
                    content: vec![ContentBlock::ToolCall {
                        id: "call-step-1".into(),
                        name: HOST_ACTION_TOOL.into(),
                        arguments: serde_json::json!({
                            "kind": "type_text",
                            "text": "first step",
                            "submit": false
                        }),
                    }],
                },
                capture_observation_response("call-step-2"),
                terminal_text_response("Two-step sequence complete."),
            ],
            last_request: Mutex::new(None),
            requests: Mutex::new(Vec::new()),
        });

        let mut registry = splcw_llm::ProviderRegistry::new();
        registry.register(provider.clone()).unwrap();
        let llm = ConfiguredLlmClient::new(
            Arc::new(StaticAuthStore {
                profile: sample_profile(),
            }),
            registry,
        );
        let host = MockHost {
            observation: sample_observation("multi-step observation"),
            verification: with_capture_observation_signal(ObservationFrame {
                structured_signals: vec![sample_post_action_verification_signal(
                    "type_text",
                    true,
                    serde_json::json!({
                        "focus_preserved": true,
                        "window_changed": false,
                        "clipboard_changed": false,
                        "focused_control_changed": false,
                        "focused_control_value_changed": true,
                        "requested_text_observed": true,
                        "proof_level": "focused_control_text_match",
                    }),
                )],
                ..sample_observation("multi-step verification")
            }),
            actions: Mutex::new(Vec::new()),
        };

        let outcome = orchestrator
            .run_runtime_turn(
                &llm,
                &host,
                RuntimeTurnOptions {
                    max_tool_rounds: 3,
                    ..RuntimeTurnOptions::default()
                },
            )
            .await
            .unwrap();

        assert!(outcome.surfaced_gap.is_none());
        assert_eq!(outcome.tool_outcome_history.len(), 2);
        assert_eq!(host.actions.lock().unwrap().len(), 2);
        assert_eq!(store.list_recent_receipts(10).await.unwrap().len(), 2);
        assert_eq!(
            outcome.proposed_action,
            Some(ProposedAction::CaptureObservation)
        );
        let requests = provider.requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        let tool_result_counts = requests
            .iter()
            .map(|request| {
                request
                    .messages
                    .iter()
                    .flat_map(|message| message.content.iter())
                    .filter(|block| matches!(block, ContentBlock::ToolResult { .. }))
                    .count()
            })
            .collect::<Vec<_>>();
        assert_eq!(tool_result_counts, vec![0, 1, 2]);
        assert_eq!(*offload.current_surfaces.lock().unwrap(), 3);
        assert_eq!(*offload.checkpoints.lock().unwrap(), 3);
    }

    #[tokio::test]
    async fn runtime_turn_preserves_pending_checkpoint_across_provider_failure_before_host_action()
    {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload.clone());
        orchestrator
            .adopt_plan(&sample_plan_snapshot())
            .await
            .unwrap();

        let root = temp_session_root("resume-provider-failure");
        let session = RuntimeSessionConfig::new(&root, "alpha-resume-provider");
        let host = MockHost {
            observation: sample_observation("resume provider failure observation"),
            verification: sample_capture_observation_verification(
                "resume provider failure verification",
            ),
            actions: Mutex::new(Vec::new()),
        };

        let mut registry = splcw_llm::ProviderRegistry::new();
        registry
            .register(Arc::new(FailingProvider {
                descriptor: local_mock_descriptor(),
                error_message: "status_code=503 upstream unavailable".into(),
            }))
            .unwrap();
        let failing_llm = ConfiguredLlmClient::new(
            Arc::new(StaticAuthStore {
                profile: sample_profile(),
            }),
            registry,
        );

        let failed_outcome = orchestrator
            .run_runtime_turn(
                &failing_llm,
                &host,
                RuntimeTurnOptions {
                    session: Some(session.clone()),
                    max_tool_rounds: 2,
                    ..RuntimeTurnOptions::default()
                },
            )
            .await
            .unwrap();
        assert!(failed_outcome.surfaced_gap.is_some());
        assert!(host.actions.lock().unwrap().is_empty());

        let journal = RuntimeSessionJournal::open(session.clone()).await.unwrap();
        let checkpoint = journal
            .read_pending_turn_checkpoint()
            .await
            .unwrap()
            .expect("provider failure should preserve the pending turn checkpoint");
        assert_eq!(checkpoint.phase, RuntimePendingTurnPhase::AwaitingProvider);
        drop(journal);

        let provider = Arc::new(StepwiseProvider {
            descriptor: local_mock_descriptor(),
            responses: vec![
                capture_observation_response("call-resume-provider"),
                terminal_text_response("Recovered runtime turn."),
            ],
            last_request: Mutex::new(None),
            requests: Mutex::new(Vec::new()),
        });
        let mut registry = splcw_llm::ProviderRegistry::new();
        registry.register(provider.clone()).unwrap();
        let llm = ConfiguredLlmClient::new(
            Arc::new(StaticAuthStore {
                profile: sample_profile(),
            }),
            registry,
        );

        let resumed_outcome = orchestrator
            .run_runtime_turn(
                &llm,
                &host,
                RuntimeTurnOptions {
                    session: Some(session.clone()),
                    max_tool_rounds: 2,
                    ..RuntimeTurnOptions::default()
                },
            )
            .await
            .unwrap();

        assert!(resumed_outcome.surfaced_gap.is_none());
        assert_eq!(host.actions.lock().unwrap().len(), 1);
        let requests = provider.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        let tool_result_counts = requests
            .iter()
            .map(|request| {
                request
                    .messages
                    .iter()
                    .flat_map(|message| message.content.iter())
                    .filter(|block| matches!(block, ContentBlock::ToolResult { .. }))
                    .count()
            })
            .collect::<Vec<_>>();
        assert_eq!(tool_result_counts, vec![0, 1]);
        drop(requests);

        let reopened = RuntimeSessionJournal::open(session).await.unwrap();
        assert!(
            reopened
                .read_pending_turn_checkpoint()
                .await
                .unwrap()
                .is_none()
        );

        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn runtime_turn_resumes_from_awaiting_host_execution_checkpoint_without_duplicate_action()
    {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload);
        let snapshot = sample_plan_snapshot();
        orchestrator.adopt_plan(&snapshot).await.unwrap();

        let root = temp_session_root("resume-awaiting-host");
        let session = RuntimeSessionConfig::new(&root, "alpha-awaiting-host");
        let journal = RuntimeSessionJournal::open(session.clone()).await.unwrap();
        journal
            .write_pending_turn_checkpoint(&sample_pending_turn_checkpoint(
                snapshot.plan.id,
                RuntimePendingTurnPhase::AwaitingHostExecution,
                0,
                2,
            ))
            .await
            .unwrap();
        drop(journal);

        let provider = Arc::new(StepwiseProvider {
            descriptor: local_mock_descriptor(),
            responses: vec![terminal_text_response("Resumed host action complete.")],
            last_request: Mutex::new(None),
            requests: Mutex::new(Vec::new()),
        });
        let mut registry = splcw_llm::ProviderRegistry::new();
        registry.register(provider.clone()).unwrap();
        let llm = ConfiguredLlmClient::new(
            Arc::new(StaticAuthStore {
                profile: sample_profile(),
            }),
            registry,
        );
        let host = MockHost {
            observation: sample_observation("unused fresh observation"),
            verification: sample_capture_observation_verification("awaiting-host verification"),
            actions: Mutex::new(Vec::new()),
        };

        let outcome = orchestrator
            .run_runtime_turn(
                &llm,
                &host,
                RuntimeTurnOptions {
                    session: Some(session.clone()),
                    max_tool_rounds: 2,
                    ..RuntimeTurnOptions::default()
                },
            )
            .await
            .unwrap();

        assert!(outcome.surfaced_gap.is_none());
        assert_eq!(host.actions.lock().unwrap().len(), 1);
        let requests = provider.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let tool_result_count = requests[0]
            .messages
            .iter()
            .flat_map(|message| message.content.iter())
            .filter(|block| matches!(block, ContentBlock::ToolResult { .. }))
            .count();
        assert_eq!(tool_result_count, 1);
        drop(requests);

        let reopened = RuntimeSessionJournal::open(session).await.unwrap();
        assert!(
            reopened
                .read_pending_turn_checkpoint()
                .await
                .unwrap()
                .is_none()
        );

        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn runtime_turn_does_not_replay_host_action_after_host_effects_uncertain_checkpoint() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload);
        let snapshot = sample_plan_snapshot();
        orchestrator.adopt_plan(&snapshot).await.unwrap();

        let root = temp_session_root("resume-host-effects-uncertain");
        let session = RuntimeSessionConfig::new(&root, "alpha-host-effects-uncertain");
        let journal = RuntimeSessionJournal::open(session.clone()).await.unwrap();
        journal
            .write_pending_turn_checkpoint(&sample_pending_turn_checkpoint(
                snapshot.plan.id,
                RuntimePendingTurnPhase::HostEffectsUncertain,
                0,
                2,
            ))
            .await
            .unwrap();
        drop(journal);

        let provider = Arc::new(StepwiseProvider {
            descriptor: local_mock_descriptor(),
            responses: vec![terminal_text_response("unused")],
            last_request: Mutex::new(None),
            requests: Mutex::new(Vec::new()),
        });
        let mut registry = splcw_llm::ProviderRegistry::new();
        registry.register(provider.clone()).unwrap();
        let llm = ConfiguredLlmClient::new(
            Arc::new(StaticAuthStore {
                profile: sample_profile(),
            }),
            registry,
        );
        let host = MockHost {
            observation: sample_observation("unused fresh observation"),
            verification: sample_observation("unused verification"),
            actions: Mutex::new(Vec::new()),
        };

        let outcome = orchestrator
            .run_runtime_turn(
                &llm,
                &host,
                RuntimeTurnOptions {
                    session: Some(session.clone()),
                    max_tool_rounds: 2,
                    ..RuntimeTurnOptions::default()
                },
            )
            .await
            .unwrap();

        assert!(outcome.surfaced_gap.is_some());
        assert!(host.actions.lock().unwrap().is_empty());
        assert!(provider.requests.lock().unwrap().is_empty());
        let reopened = RuntimeSessionJournal::open(session).await.unwrap();
        assert!(
            reopened
                .read_pending_turn_checkpoint()
                .await
                .unwrap()
                .is_none()
        );

        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn runtime_turn_clears_pending_checkpoint_on_terminal_completion() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload);
        orchestrator
            .adopt_plan(&sample_plan_snapshot())
            .await
            .unwrap();

        let provider = Arc::new(StepwiseProvider {
            descriptor: local_mock_descriptor(),
            responses: vec![
                capture_observation_response("call-clear"),
                terminal_text_response("Pending checkpoint cleared."),
            ],
            last_request: Mutex::new(None),
            requests: Mutex::new(Vec::new()),
        });
        let mut registry = splcw_llm::ProviderRegistry::new();
        registry.register(provider).unwrap();
        let llm = ConfiguredLlmClient::new(
            Arc::new(StaticAuthStore {
                profile: sample_profile(),
            }),
            registry,
        );
        let host = MockHost {
            observation: sample_observation("clear checkpoint observation"),
            verification: sample_capture_observation_verification("clear checkpoint verification"),
            actions: Mutex::new(Vec::new()),
        };
        let root = temp_session_root("clear-pending-terminal");
        let session = RuntimeSessionConfig::new(&root, "alpha-clear-pending");

        let outcome = orchestrator
            .run_runtime_turn(
                &llm,
                &host,
                RuntimeTurnOptions {
                    session: Some(session.clone()),
                    max_tool_rounds: 2,
                    ..RuntimeTurnOptions::default()
                },
            )
            .await
            .unwrap();

        assert!(outcome.surfaced_gap.is_none());
        let reopened = RuntimeSessionJournal::open(session).await.unwrap();
        assert!(
            reopened
                .read_pending_turn_checkpoint()
                .await
                .unwrap()
                .is_none()
        );

        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn runtime_turn_writes_session_journal_events() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload);
        let snapshot = sample_plan_snapshot();
        orchestrator.adopt_plan(&snapshot).await.unwrap();

        let provider = Arc::new(StepwiseProvider {
            descriptor: local_mock_descriptor(),
            responses: vec![
                capture_observation_response("call-journal"),
                terminal_text_response("Journal observation captured."),
            ],
            last_request: Mutex::new(None),
            requests: Mutex::new(Vec::new()),
        });

        let mut registry = splcw_llm::ProviderRegistry::new();
        registry.register(provider).unwrap();
        let llm = ConfiguredLlmClient::new(
            Arc::new(StaticAuthStore {
                profile: sample_profile(),
            }),
            registry,
        );
        let host = MockHost {
            observation: sample_observation("journal observation"),
            verification: sample_capture_observation_verification("journal verification"),
            actions: Mutex::new(Vec::new()),
        };
        let root = temp_session_root("journal");
        let session = RuntimeSessionConfig::new(&root, "alpha");

        let outcome = orchestrator
            .run_runtime_turn(
                &llm,
                &host,
                RuntimeTurnOptions {
                    session: Some(session.clone()),
                    ..RuntimeTurnOptions::default()
                },
            )
            .await
            .unwrap();

        assert!(outcome.receipt.is_some());
        let journal = RuntimeSessionJournal::open(session).await.unwrap();
        let events = journal.read_events().await.unwrap();
        assert_eq!(
            event_kinds(&events),
            vec![
                crate::RuntimeSessionEventKind::TurnStarted,
                crate::RuntimeSessionEventKind::TurnRoundStarted,
                crate::RuntimeSessionEventKind::TurnActionSelected,
                crate::RuntimeSessionEventKind::TurnActionExecuted,
                crate::RuntimeSessionEventKind::TurnActionVerified,
                crate::RuntimeSessionEventKind::TurnRoundCompleted,
                crate::RuntimeSessionEventKind::TurnRoundStarted,
                crate::RuntimeSessionEventKind::TurnRoundCompleted,
                crate::RuntimeSessionEventKind::TurnCompleted,
            ]
        );
        let turn_records = journal.read_turn_records().await.unwrap();
        assert_eq!(turn_records.len(), 2);
        assert_eq!(turn_records[0].provider_id, "local-mock");
        assert!(
            turn_records[0]
                .tool_outcome
                .as_ref()
                .map(|outcome| outcome.call_id == "call-journal")
                .unwrap_or(false)
        );
        assert!(turn_records[1].tool_outcome.is_none());

        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn runtime_turn_writes_step_progress_events_for_multistep_flow() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload);
        orchestrator
            .adopt_plan(&sample_plan_snapshot())
            .await
            .unwrap();

        let provider = Arc::new(StepwiseProvider {
            descriptor: local_mock_descriptor(),
            responses: vec![
                ChatResponse {
                    provider_id: "local-mock".into(),
                    model: "mock-model".into(),
                    content: vec![ContentBlock::ToolCall {
                        id: "call-step-1".into(),
                        name: HOST_ACTION_TOOL.into(),
                        arguments: serde_json::json!({
                            "kind": "type_text",
                            "text": "first step",
                            "submit": false
                        }),
                    }],
                },
                capture_observation_response("call-step-2"),
                terminal_text_response("Two-step sequence complete."),
            ],
            last_request: Mutex::new(None),
            requests: Mutex::new(Vec::new()),
        });

        let mut registry = splcw_llm::ProviderRegistry::new();
        registry.register(provider).unwrap();
        let llm = ConfiguredLlmClient::new(
            Arc::new(StaticAuthStore {
                profile: sample_profile(),
            }),
            registry,
        );
        let host = MockHost {
            observation: sample_observation("multi-step observation"),
            verification: with_capture_observation_signal(ObservationFrame {
                structured_signals: vec![sample_post_action_verification_signal(
                    "type_text",
                    true,
                    serde_json::json!({
                        "focus_preserved": true,
                        "window_changed": false,
                        "clipboard_changed": false,
                        "focused_control_changed": false,
                        "focused_control_value_changed": true,
                        "requested_text_observed": true,
                        "proof_level": "focused_control_text_match",
                    }),
                )],
                ..sample_observation("multi-step verification")
            }),
            actions: Mutex::new(Vec::new()),
        };
        let root = temp_session_root("progress-multistep");
        let session = RuntimeSessionConfig::new(&root, "alpha-progress");

        let outcome = orchestrator
            .run_runtime_turn(
                &llm,
                &host,
                RuntimeTurnOptions {
                    session: Some(session.clone()),
                    max_tool_rounds: 3,
                    ..RuntimeTurnOptions::default()
                },
            )
            .await
            .unwrap();

        assert!(outcome.surfaced_gap.is_none());
        let journal = RuntimeSessionJournal::open(session).await.unwrap();
        let events = journal.read_events().await.unwrap();
        assert_eq!(
            event_kinds(&events),
            vec![
                crate::RuntimeSessionEventKind::TurnStarted,
                crate::RuntimeSessionEventKind::TurnRoundStarted,
                crate::RuntimeSessionEventKind::TurnActionSelected,
                crate::RuntimeSessionEventKind::TurnActionExecuted,
                crate::RuntimeSessionEventKind::TurnActionVerified,
                crate::RuntimeSessionEventKind::TurnRoundCompleted,
                crate::RuntimeSessionEventKind::TurnRoundStarted,
                crate::RuntimeSessionEventKind::TurnActionSelected,
                crate::RuntimeSessionEventKind::TurnActionExecuted,
                crate::RuntimeSessionEventKind::TurnActionVerified,
                crate::RuntimeSessionEventKind::TurnRoundCompleted,
                crate::RuntimeSessionEventKind::TurnRoundStarted,
                crate::RuntimeSessionEventKind::TurnRoundCompleted,
                crate::RuntimeSessionEventKind::TurnCompleted,
            ]
        );
        assert!(
            events
                .iter()
                .filter(|event| event.kind == crate::RuntimeSessionEventKind::TurnRoundCompleted)
                .any(|event| event.summary.contains("Two-step sequence complete"))
        );

        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn runtime_turn_writes_step_progress_events_on_provider_gap() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload);
        orchestrator
            .adopt_plan(&sample_plan_snapshot())
            .await
            .unwrap();

        let provider = Arc::new(StaticProvider {
            descriptor: local_mock_descriptor(),
            response: ChatResponse {
                provider_id: "local-mock".into(),
                model: "mock-model".into(),
                content: vec![ContentBlock::ToolCall {
                    id: "gap-1".into(),
                    name: CAPABILITY_GAP_TOOL.into(),
                    arguments: serde_json::json!({
                        "title": "Need stable selector acquisition for the target control",
                        "permanent_fix_target": "Add a selector-backed perception hook before retrying",
                        "notes": ["OCR alone is not sufficient"]
                    }),
                }],
            },
            last_request: Mutex::new(None),
        });

        let mut registry = splcw_llm::ProviderRegistry::new();
        registry.register(provider).unwrap();
        let llm = ConfiguredLlmClient::new(
            Arc::new(StaticAuthStore {
                profile: sample_profile(),
            }),
            registry,
        );
        let host = MockHost {
            observation: sample_observation("target control is ambiguous"),
            verification: sample_observation("unused"),
            actions: Mutex::new(Vec::new()),
        };
        let root = temp_session_root("progress-gap");
        let session = RuntimeSessionConfig::new(&root, "alpha-gap");

        let outcome = orchestrator
            .run_runtime_turn(
                &llm,
                &host,
                RuntimeTurnOptions {
                    session: Some(session.clone()),
                    ..RuntimeTurnOptions::default()
                },
            )
            .await
            .unwrap();

        assert!(outcome.surfaced_gap.is_some());
        let journal = RuntimeSessionJournal::open(session).await.unwrap();
        let events = journal.read_events().await.unwrap();
        assert_eq!(
            event_kinds(&events),
            vec![
                crate::RuntimeSessionEventKind::TurnStarted,
                crate::RuntimeSessionEventKind::TurnRoundStarted,
                crate::RuntimeSessionEventKind::TurnRoundCompleted,
                crate::RuntimeSessionEventKind::TurnGap,
            ]
        );
        assert!(
            events[2]
                .summary
                .contains("provider surfaced capability gap")
        );

        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn runtime_turn_writes_progress_events_with_host_verification_gap() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload);
        orchestrator
            .adopt_plan(&sample_plan_snapshot())
            .await
            .unwrap();

        let provider = Arc::new(StepwiseProvider {
            descriptor: local_mock_descriptor(),
            responses: vec![ChatResponse {
                provider_id: "local-mock".into(),
                model: "mock-model".into(),
                content: vec![ContentBlock::ToolCall {
                    id: "call-verify-fail".into(),
                    name: HOST_ACTION_TOOL.into(),
                    arguments: serde_json::json!({
                        "kind": "capture_observation"
                    }),
                }],
            }],
            last_request: Mutex::new(None),
            requests: Mutex::new(Vec::new()),
        });
        let mut registry = splcw_llm::ProviderRegistry::new();
        registry.register(provider).unwrap();
        let llm = ConfiguredLlmClient::new(
            Arc::new(StaticAuthStore {
                profile: sample_profile(),
            }),
            registry,
        );
        let host = FailingHost {
            observation: sample_observation("verify fail observation"),
            verification: sample_observation("verify fail verification"),
            actions: Mutex::new(Vec::new()),
            enact_error: None,
            verify_error: Some("host verify exploded".into()),
        };
        let root = temp_session_root("progress-verify-gap");
        let session = RuntimeSessionConfig::new(&root, "alpha-verify-gap");

        let outcome = orchestrator
            .run_runtime_turn(
                &llm,
                &host,
                RuntimeTurnOptions {
                    session: Some(session.clone()),
                    ..RuntimeTurnOptions::default()
                },
            )
            .await
            .unwrap();

        assert!(outcome.surfaced_gap.is_some());
        let journal = RuntimeSessionJournal::open(session).await.unwrap();
        let events = journal.read_events().await.unwrap();
        assert_eq!(
            event_kinds(&events),
            vec![
                crate::RuntimeSessionEventKind::TurnStarted,
                crate::RuntimeSessionEventKind::TurnRoundStarted,
                crate::RuntimeSessionEventKind::TurnActionSelected,
                crate::RuntimeSessionEventKind::TurnActionExecuted,
                crate::RuntimeSessionEventKind::TurnGap,
            ]
        );

        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn runtime_turn_cleans_stale_lock_before_session_open() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload);
        let snapshot = sample_plan_snapshot();
        orchestrator.adopt_plan(&snapshot).await.unwrap();

        let provider = Arc::new(StepwiseProvider {
            descriptor: local_mock_descriptor(),
            responses: vec![
                capture_observation_response("call-journal-stale"),
                terminal_text_response("Journal observation captured."),
            ],
            last_request: Mutex::new(None),
            requests: Mutex::new(Vec::new()),
        });

        let mut registry = splcw_llm::ProviderRegistry::new();
        registry.register(provider).unwrap();
        let llm = ConfiguredLlmClient::new(
            Arc::new(StaticAuthStore {
                profile: sample_profile(),
            }),
            registry,
        );
        let host = MockHost {
            observation: sample_observation("journal observation"),
            verification: sample_capture_observation_verification("journal verification"),
            actions: Mutex::new(Vec::new()),
        };
        let root = temp_session_root("journal-stale-open");
        let mut session = RuntimeSessionConfig::new(&root, "alpha-stale");
        session.max_lock_age_seconds = 0;
        let session_dir = root.join("alpha-stale");
        tokio::fs::create_dir_all(&session_dir).await.unwrap();
        tokio::fs::write(session_dir.join("write.lock"), "stale")
            .await
            .unwrap();

        let outcome = orchestrator
            .run_runtime_turn(
                &llm,
                &host,
                RuntimeTurnOptions {
                    session: Some(session.clone()),
                    ..RuntimeTurnOptions::default()
                },
            )
            .await
            .unwrap();

        assert!(outcome.receipt.is_some());
        assert!(!session_dir.join("write.lock").exists());

        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn runtime_turn_records_session_repaired_for_turn_log_policy_repair() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload);
        let snapshot = sample_plan_snapshot();
        orchestrator.adopt_plan(&snapshot).await.unwrap();

        let provider = Arc::new(StepwiseProvider {
            descriptor: local_mock_descriptor(),
            responses: vec![
                capture_observation_response("call-journal-policy"),
                terminal_text_response("Journal observation captured."),
            ],
            last_request: Mutex::new(None),
            requests: Mutex::new(Vec::new()),
        });

        let mut registry = splcw_llm::ProviderRegistry::new();
        registry.register(provider).unwrap();
        let llm = ConfiguredLlmClient::new(
            Arc::new(StaticAuthStore {
                profile: sample_profile(),
            }),
            registry,
        );
        let host = MockHost {
            observation: sample_observation("journal observation"),
            verification: sample_capture_observation_verification("journal verification"),
            actions: Mutex::new(Vec::new()),
        };
        let root = temp_session_root("journal-policy-repair");
        let session = RuntimeSessionConfig::new(&root, "alpha-policy");
        let session_dir = root.join("alpha-policy");
        tokio::fs::create_dir_all(&session_dir).await.unwrap();

        let good = serde_json::to_string(&RuntimeSessionEvent::turn_started(
            "alpha-policy",
            "plan-1",
            "started",
        ))
        .unwrap();
        tokio::fs::write(session_dir.join("transcript.jsonl"), format!("{good}\n"))
            .await
            .unwrap();

        let record = RuntimeTurnRecord {
            recorded_at: Utc::now(),
            turn_id: Uuid::new_v4(),
            thread_id: " Tooling Verification ".into(),
            provider_id: "local-mock".into(),
            model: "mock-model".into(),
            request: ChatRequest {
                model: "mock-model".into(),
                system_prompt: Some("runtime system prompt".into()),
                messages: vec![
                    ChatMessage {
                        role: "user".into(),
                        content: vec![ContentBlock::Text {
                            text: "prior request".into(),
                        }],
                    },
                    ChatMessage {
                        role: "assistant".into(),
                        content: vec![ContentBlock::ToolCall {
                            id: "call-repair".into(),
                            name: HOST_ACTION_TOOL.into(),
                            arguments: serde_json::json!({
                                "kind": "capture_observation"
                            }),
                        }],
                    },
                    ChatMessage {
                        role: "user".into(),
                        content: vec![ContentBlock::ToolResult {
                            id: "orphan-result".into(),
                            content: serde_json::json!({"status":"ok"}),
                        }],
                    },
                ],
                tools: vec![],
            },
            response: ChatResponse {
                provider_id: "local-mock".into(),
                model: "mock-model".into(),
                content: vec![ContentBlock::Text {
                    text: "prior response".into(),
                }],
            },
            narrative: "prior narrative".into(),
            tool_outcome: None,
            surfaced_gap: None,
        };
        let duplicate = record.clone();
        let turn_log_body = format!(
            "{}\n{}\n",
            serde_json::to_string(&record).unwrap(),
            serde_json::to_string(&duplicate).unwrap()
        );
        tokio::fs::write(session_dir.join("turn-log.jsonl"), turn_log_body)
            .await
            .unwrap();

        let outcome = orchestrator
            .run_runtime_turn(
                &llm,
                &host,
                RuntimeTurnOptions {
                    session: Some(session.clone()),
                    ..RuntimeTurnOptions::default()
                },
            )
            .await
            .unwrap();

        assert!(outcome.receipt.is_some());
        let journal = RuntimeSessionJournal::open(session).await.unwrap();
        let events = journal.read_events().await.unwrap();
        assert!(events.iter().any(|event| {
            event.kind == crate::RuntimeSessionEventKind::SessionRepaired
                && event
                    .summary
                    .contains("repaired session transcript or turn-log continuity")
        }));
        assert!(
            events
                .iter()
                .filter(|event| event.kind == crate::RuntimeSessionEventKind::TurnStarted)
                .count()
                >= 2
        );
        assert!(
            events
                .iter()
                .any(|event| event.kind == crate::RuntimeSessionEventKind::TurnCompleted)
        );

        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn runtime_turn_injects_post_compaction_refresh_into_system_prompt() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload);
        let snapshot = sample_plan_snapshot();
        orchestrator.adopt_plan(&snapshot).await.unwrap();

        let provider = Arc::new(StepwiseProvider {
            descriptor: local_mock_descriptor(),
            responses: vec![
                capture_observation_response("call-refresh"),
                terminal_text_response("Refresh observation captured."),
            ],
            last_request: Mutex::new(None),
            requests: Mutex::new(Vec::new()),
        });

        let mut registry = splcw_llm::ProviderRegistry::new();
        registry.register(provider.clone()).unwrap();
        let llm = ConfiguredLlmClient::new(
            Arc::new(StaticAuthStore {
                profile: sample_profile(),
            }),
            registry,
        );
        let host = MockHost {
            observation: sample_observation("refresh observation"),
            verification: sample_capture_observation_verification("refresh verification"),
            actions: Mutex::new(Vec::new()),
        };
        let root = temp_session_root("post-compaction-refresh");
        let session = RuntimeSessionConfig::new(&root, "omega");

        let mut journal = RuntimeSessionJournal::open(session.clone()).await.unwrap();
        journal
            .compact_transcript(
                0,
                "compaction completed",
                Some("Re-read Session Startup before responding.".into()),
            )
            .await
            .unwrap();

        let outcome = orchestrator
            .run_runtime_turn(
                &llm,
                &host,
                RuntimeTurnOptions {
                    session: Some(session.clone()),
                    ..RuntimeTurnOptions::default()
                },
            )
            .await
            .unwrap();

        assert!(outcome.receipt.is_some());
        let request = provider.last_request.lock().unwrap().clone().unwrap();
        let system_prompt = request.system_prompt.unwrap();
        assert!(system_prompt.contains("[Post-compaction refresh]"));
        assert!(system_prompt.contains("Re-read Session Startup before responding."));

        let reopened = RuntimeSessionJournal::open(session).await.unwrap();
        assert!(reopened.pending_post_compaction_refresh().is_none());
        let events = reopened.read_events().await.unwrap();
        assert!(events.iter().any(|event| {
            event.kind == crate::RuntimeSessionEventKind::PostCompactionRefreshConsumed
        }));

        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn runtime_turn_injects_thread_context_into_system_prompt() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload);
        let snapshot = sample_plan_snapshot();
        orchestrator.adopt_plan(&snapshot).await.unwrap();

        let provider = Arc::new(StepwiseProvider {
            descriptor: local_mock_descriptor(),
            responses: vec![
                capture_observation_response("call-thread"),
                terminal_text_response("Thread observation captured."),
            ],
            last_request: Mutex::new(None),
            requests: Mutex::new(Vec::new()),
        });

        let mut registry = splcw_llm::ProviderRegistry::new();
        registry.register(provider.clone()).unwrap();
        let llm = ConfiguredLlmClient::new(
            Arc::new(StaticAuthStore {
                profile: sample_profile(),
            }),
            registry,
        );
        let host = MockHost {
            observation: sample_observation("thread observation"),
            verification: sample_capture_observation_verification("thread verification"),
            actions: Mutex::new(Vec::new()),
        };
        let root = temp_session_root("thread-context");
        let session = RuntimeSessionConfig::new(&root, "sigma");

        let mut journal = RuntimeSessionJournal::open(session.clone()).await.unwrap();
        journal
            .record_foreground_turn("main continuity")
            .await
            .unwrap();
        journal
            .switch_foreground_thread("tooling", Some("Tooling Verification"))
            .await
            .unwrap();
        journal
            .record_foreground_turn("tooling continuity")
            .await
            .unwrap();

        let outcome = orchestrator
            .run_runtime_turn(
                &llm,
                &host,
                RuntimeTurnOptions {
                    session: Some(session.clone()),
                    thread_id: Some("tooling".into()),
                    thread_label: Some("Tooling Verification".into()),
                    ..RuntimeTurnOptions::default()
                },
            )
            .await
            .unwrap();

        assert!(outcome.receipt.is_some());
        let request = provider.last_request.lock().unwrap().clone().unwrap();
        let system_prompt = request.system_prompt.unwrap();
        assert!(system_prompt.contains("[Thread continuity]"));
        assert!(system_prompt.contains("Tooling Verification"));
        assert!(system_prompt.contains("tooling continuity"));
        assert!(system_prompt.contains("Main"));

        let reopened = RuntimeSessionJournal::open(session).await.unwrap();
        let tooling = reopened.state().threads.get("tooling").unwrap();
        assert!(
            tooling
                .recent_turns
                .last()
                .map(|turn| turn.summary.contains("executed capture observation"))
                .unwrap_or(false)
        );

        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn runtime_turn_injects_recent_turn_history_into_system_prompt() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload);
        let snapshot = sample_plan_snapshot();
        orchestrator.adopt_plan(&snapshot).await.unwrap();

        let provider = Arc::new(StepwiseProvider {
            descriptor: local_mock_descriptor(),
            responses: vec![
                capture_observation_response("call-history"),
                terminal_text_response("History observation captured."),
            ],
            last_request: Mutex::new(None),
            requests: Mutex::new(Vec::new()),
        });

        let mut registry = splcw_llm::ProviderRegistry::new();
        registry.register(provider.clone()).unwrap();
        let llm = ConfiguredLlmClient::new(
            Arc::new(StaticAuthStore {
                profile: sample_profile(),
            }),
            registry,
        );
        let host = MockHost {
            observation: sample_observation("history observation"),
            verification: sample_capture_observation_verification("history verification"),
            actions: Mutex::new(Vec::new()),
        };
        let root = temp_session_root("turn-history");
        let session = RuntimeSessionConfig::new(&root, "tau");

        let journal = RuntimeSessionJournal::open(session.clone()).await.unwrap();
        journal
            .append_turn_record(&RuntimeTurnRecord {
                recorded_at: Utc::now(),
                turn_id: Uuid::new_v4(),
                thread_id: "main".into(),
                provider_id: "local-mock".into(),
                model: "mock-model".into(),
                request: ChatRequest {
                    model: "mock-model".into(),
                    system_prompt: Some("runtime system prompt".into()),
                    messages: vec![ChatMessage {
                        role: "user".into(),
                        content: vec![ContentBlock::Text {
                            text: "prior request".into(),
                        }],
                    }],
                    tools: vec![],
                },
                response: ChatResponse {
                    provider_id: "local-mock".into(),
                    model: "mock-model".into(),
                    content: vec![ContentBlock::Text {
                        text: "prior response".into(),
                    }],
                },
                narrative: "prior narrative".into(),
                tool_outcome: Some(RuntimeToolOutcomeRecord {
                    call_id: "prior-call".into(),
                    action: ProposedAction::CaptureObservation,
                    execution: ActionExecution {
                        id: Uuid::new_v4(),
                        action: ProposedAction::CaptureObservation,
                        backend: "mock-host".into(),
                        summary: "captured prior observation".into(),
                        evidence: None,
                        recorded_at: Utc::now(),
                    },
                    verification: sample_observation("prior verification"),
                    verification_kind: Some("capture_observation".into()),
                    verification_ok: Some(true),
                    verification_proof_level: Some("screenshot_present".into()),
                    verification_summary: Some(
                        "verify kind=capture_observation ok=true proof=screenshot_present".into(),
                    ),
                    receipt_id: None,
                    receipt_changed: Some("captured prior observation".into()),
                    contradiction: None,
                }),
                surfaced_gap: None,
            })
            .await
            .unwrap();

        let outcome = orchestrator
            .run_runtime_turn(
                &llm,
                &host,
                RuntimeTurnOptions {
                    session: Some(session.clone()),
                    ..RuntimeTurnOptions::default()
                },
            )
            .await
            .unwrap();

        assert!(outcome.receipt.is_some());
        let request = provider.last_request.lock().unwrap().clone().unwrap();
        let system_prompt = request.system_prompt.unwrap();
        assert!(system_prompt.contains("[Recent runtime turns]"));
        assert!(system_prompt.contains("captured prior observation"));

        let _ = tokio::fs::remove_dir_all(root).await;
    }
}
