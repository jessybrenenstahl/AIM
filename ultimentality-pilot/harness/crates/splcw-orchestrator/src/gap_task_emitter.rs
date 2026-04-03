//! Gap-to-task emitter: the missing half of the contradiction-to-capability pipeline.
//!
//! # Problem
//!
//! `splcw-core` defines `CapabilityGap` with an `Open → InRecodification → Closed`
//! status lifecycle. `splcw-orchestrator` creates gap records correctly via
//! `surface_runtime_gap`. But nothing ever reads those records and acts on them
//! autonomously. The pipeline stops at the recording layer — a human has to
//! notice the gap and decide what to do.
//!
//! # Solution
//!
//! This module provides two things:
//!
//! 1. **`emit_gap_task`** — called immediately after `surface_runtime_gap`
//!    returns. Appends a `GapTask` to a durable JSONL queue at
//!    `{session_root}/gap-task-queue.jsonl`. The task records what gap was
//!    found and what the fix target is.
//!
//! 2. **`build_gap_task_context`** — called at the start of each bounded turn
//!    (alongside the existing receipt/thread context injection). If there is a
//!    pending gap task, it injects the gap details into the system prompt so
//!    the model knows it should attempt a fix this turn.
//!
//! # Integration points
//!
//! **After `surface_runtime_gap` in runtime.rs:**
//!
//! ```rust,ignore
//! let gap = self.surface_runtime_gap(directive, ...).await?;
//!
//! // NEW: emit a self-directed task so the next turn picks it up.
//! if let Err(e) = emit_gap_task(&session_root, &gap).await {
//!     tracing::warn!("gap task emission failed (non-fatal): {e:#}");
//! }
//! ```
//!
//! **Before building the system prompt in `run_runtime_turn`:**
//!
//! ```rust,ignore
//! let gap_task_context = build_gap_task_context(&session_root).await.ok().flatten();
//!
//! // Merge gap_task_context into `external_context` or inject it as a
//! // separate section in the system prompt alongside receipt/thread context.
//! ```
//!
//! **After a receipt is written for a successful fix:**
//!
//! ```rust,ignore
//! // Mark the oldest pending task as resolved so it is not re-injected.
//! mark_oldest_pending_task_resolved(&session_root).await?;
//! ```

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use splcw_core::{CapabilityGap, CapabilityGapStatus};
use std::path::{Path, PathBuf};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

// ── Types ────────────────────────────────────────────────────────────────────

/// A pending self-directed task derived from a surfaced [`CapabilityGap`].
///
/// Tasks are stored append-only in `gap-task-queue.jsonl`. Status transitions
/// are written as new tail records; the most-recent record for a given
/// `task_id` wins.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GapTask {
    /// Unique ID for this task emission (not the same as `gap_id`).
    pub task_id: Uuid,
    /// The gap that triggered this task.
    pub gap_id: Uuid,
    pub gap_title: String,
    pub permanent_fix_target: String,
    /// When this task was first emitted.
    pub emitted_at: DateTime<Utc>,
    /// When this record was written (may differ from `emitted_at` for updates).
    pub updated_at: DateTime<Utc>,
    /// Current lifecycle state.
    pub status: GapTaskStatus,
    /// Notes accumulated during processing attempts.
    pub notes: Vec<String>,
}

/// Lifecycle state of a [`GapTask`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GapTaskStatus {
    /// Emitted and waiting to be picked up by a bounded turn.
    Pending,
    /// A bounded turn is currently attempting a fix.
    InProgress,
    /// A fix was applied; the underlying gap should now be `Closed`.
    Resolved,
    /// Processing was attempted but no automated fix was possible this cycle.
    /// The task remains visible but is deprioritised on the next attempt.
    Deferred,
    /// Permanently abandoned (e.g., the gap was manually closed by the operator).
    Abandoned,
}

impl GapTask {
    fn from_gap(gap: &CapabilityGap) -> Self {
        let now = Utc::now();
        Self {
            task_id: Uuid::new_v4(),
            gap_id: gap.id,
            gap_title: gap.title.clone(),
            permanent_fix_target: gap.permanent_fix_target.clone(),
            emitted_at: now,
            updated_at: now,
            status: GapTaskStatus::Pending,
            notes: gap.notes.clone(),
        }
    }

    fn with_status(mut self, status: GapTaskStatus, note: Option<String>) -> Self {
        self.status = status;
        self.updated_at = Utc::now();
        if let Some(n) = note {
            self.notes.push(n);
        }
        self
    }
}

// ── Public API ───────────────────────────────────────────────────────────────

/// Append a new [`GapTask`] to the durable queue for `gap`.
///
/// Only emits for `Open` gaps — if the gap is already `InRecodification` or
/// `Closed` this is a no-op (returns `Ok` with the would-be task for logging).
pub async fn emit_gap_task(session_root: &Path, gap: &CapabilityGap) -> Result<GapTask> {
    if gap.status != CapabilityGapStatus::Open {
        // Gap already being handled; emit a no-op task so the caller has a
        // record but do not append to the queue.
        return Ok(GapTask::from_gap(gap));
    }

    let task = GapTask::from_gap(gap);
    append_task(session_root, &task).await?;
    Ok(task)
}

/// Read every task whose status is [`GapTaskStatus::Pending`] from the queue.
/// Deduplicates by `gap_id`, keeping the most recent emission per gap.
pub async fn read_pending_gap_tasks(session_root: &Path) -> Result<Vec<GapTask>> {
    let path = queue_path(session_root);
    if !path.exists() {
        return Ok(Vec::new());
    }

    let content = tokio::fs::read_to_string(&path).await?;
    // Build a map keyed by task_id; last record wins for status updates.
    let mut by_task: std::collections::HashMap<Uuid, GapTask> = std::collections::HashMap::new();
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<GapTask>(trimmed) {
            Ok(task) => {
                by_task.insert(task.task_id, task);
            }
            Err(e) => {
                eprintln!("warn: skipping malformed gap task record: {e}");
            }
        }
    }

    // Deduplicate by gap_id, keeping only Pending tasks, sorted oldest first.
    let mut seen_gaps: std::collections::HashSet<Uuid> = std::collections::HashSet::new();
    let mut pending: Vec<GapTask> = by_task
        .into_values()
        .filter(|t| t.status == GapTaskStatus::Pending)
        .filter(|t| seen_gaps.insert(t.gap_id))
        .collect();
    pending.sort_by_key(|t| t.emitted_at);
    Ok(pending)
}

/// Build a system-prompt context snippet for the oldest pending gap task.
///
/// Returns `None` when the queue is empty so the caller can skip the injection.
pub async fn build_gap_task_context(session_root: &Path) -> Result<Option<String>> {
    let pending = read_pending_gap_tasks(session_root).await?;
    let Some(task) = pending.into_iter().next() else {
        return Ok(None);
    };

    let notes_section = if task.notes.is_empty() {
        String::new()
    } else {
        format!(
            "notes:\n{}\n",
            task.notes
                .iter()
                .map(|n| format!("  - {n}"))
                .collect::<Vec<_>>()
                .join("\n")
        )
    };

    let context = format!(
        "## Active Gap Task\n\
         task_id: {}\n\
         gap_id: {}\n\
         title: {}\n\
         permanent_fix_target: {}\n\
         emitted_at: {}\n\
         {}\n\
         You have a pending self-directed task to address the gap above.\n\
         Attempt a bounded fix this turn. If you succeed, write a receipt and\n\
         call `mark_oldest_pending_task_resolved` so the task is not re-injected.\n\
         If you cannot fix it this turn, emit a contradiction record and the task\n\
         will be re-presented next turn.",
        task.task_id,
        task.gap_id,
        task.gap_title,
        task.permanent_fix_target,
        task.emitted_at.to_rfc3339(),
        notes_section,
    );

    Ok(Some(context))
}

/// Mark the oldest pending task for `gap_id` as [`GapTaskStatus::Resolved`].
///
/// Appends an updated record to the queue (the append-only log means old
/// records are preserved for history).
pub async fn mark_oldest_pending_task_resolved(
    session_root: &Path,
    gap_id: Uuid,
    note: Option<String>,
) -> Result<()> {
    let pending = read_pending_gap_tasks(session_root).await?;
    let Some(task) = pending.into_iter().find(|t| t.gap_id == gap_id) else {
        return Ok(()); // nothing to resolve
    };
    let updated = task.with_status(GapTaskStatus::Resolved, note);
    append_task(session_root, &updated).await
}

/// Mark the oldest pending task for `gap_id` as [`GapTaskStatus::Deferred`].
///
/// Use this when the turn could not fix the gap but intends to try again later.
pub async fn defer_oldest_pending_task(
    session_root: &Path,
    gap_id: Uuid,
    note: Option<String>,
) -> Result<()> {
    let pending = read_pending_gap_tasks(session_root).await?;
    let Some(task) = pending.into_iter().find(|t| t.gap_id == gap_id) else {
        return Ok(());
    };
    // Re-emit as Pending so it stays in the rotation (deferred = still pending
    // in this simple implementation; add a priority field later if needed).
    let updated = task.with_status(GapTaskStatus::Pending, note);
    append_task(session_root, &updated).await
}

// ── Internals ────────────────────────────────────────────────────────────────

async fn append_task(session_root: &Path, task: &GapTask) -> Result<()> {
    let path = queue_path(session_root);
    let mut line = serde_json::to_string(task)?;
    line.push('\n');

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await?;
    file.write_all(line.as_bytes()).await?;
    file.flush().await?;
    Ok(())
}

fn queue_path(session_root: &Path) -> PathBuf {
    session_root.join("gap-task-queue.jsonl")
}
