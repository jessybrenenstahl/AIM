//! Compaction mirror publisher — the missing write side of continuity publication.
//!
//! # Problem
//!
//! `splcw-memory` has `reconcile_mirror_continuity`, which reads the GitHub
//! mirror and compares it against local checkpoint state. If the checksums
//! differ it surfaces a capability gap. But nothing currently *writes* to the
//! mirror after a compaction completes. The reconcile function detects drift;
//! this module creates the artifacts that prevent drift in the first place.
//!
//! The current flow after a compaction:
//!   local snapshot written → compaction_count incremented → session re-opened
//!
//! The intended flow:
//!   local snapshot written → **mirror push** → compaction_count incremented
//!   → session re-opened → reconcile finds no drift
//!
//! # Solution
//!
//! `publish_compaction_to_mirror` stages the `offload/current/` directory,
//! commits with a timestamped message, and pushes to the configured remote
//! branch. If the push fails (no network, expired credentials, etc.) the
//! error is returned so the caller can surface it as a capability gap and
//! retry next compaction rather than silently losing the artifact.
//!
//! # Integration point (splcw-orchestrator session.rs or lib.rs)
//!
//! Find the site in `RuntimeSessionJournal` where compaction completes — the
//! point where `CompactionCompleted` is appended — and add:
//!
//! ```rust,ignore
//! use crate::compaction_publisher::{publish_compaction_to_mirror, MirrorPublishConfig};
//!
//! let publish_config = MirrorPublishConfig {
//!     repo_root: find_repo_root(&self.root_dir)?,
//!     ..MirrorPublishConfig::default()
//! };
//!
//! match publish_compaction_to_mirror(
//!     &publish_config,
//!     new_compaction_count,
//!     &self.config.session_id,
//! )
//! .await
//! {
//!     Ok(receipt) => {
//!         tracing::info!(
//!             "compaction mirror push ok: sha={:?} dir={}",
//!             receipt.committed_sha,
//!             receipt.checkpoint_dir,
//!         );
//!     }
//!     Err(e) => {
//!         // Non-fatal: log and surface as a gap so the next turn knows.
//!         tracing::warn!("compaction mirror push failed (will surface gap): {e:#}");
//!         let _ = self.surface_mirror_push_gap(e.to_string()).await;
//!     }
//! }
//! ```

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tokio::process::Command;

// ── Config ───────────────────────────────────────────────────────────────────

/// Configuration for the GitHub mirror push performed after each compaction.
#[derive(Debug, Clone)]
pub struct MirrorPublishConfig {
    /// Absolute path to the root of the FFR git repository.
    ///
    /// Tip: walk parent directories from `session_root` until you find `.git/`.
    /// See [`find_repo_root`] below.
    pub repo_root: PathBuf,

    /// Path to the offload directory **relative to `repo_root`**.
    ///
    /// Default: `"offload/current"`.
    pub offload_rel_path: String,

    /// Git remote name.
    ///
    /// Default: `"origin"`.
    pub remote: String,

    /// Branch to push to (must already exist on the remote).
    ///
    /// Default: `"main"`.
    pub branch: String,

    /// Git author name written into the compaction commit.
    pub author_name: String,

    /// Git author email written into the compaction commit.
    pub author_email: String,
}

impl Default for MirrorPublishConfig {
    fn default() -> Self {
        Self {
            repo_root: PathBuf::from("."),
            offload_rel_path: "offload/current".into(),
            remote: "origin".into(),
            branch: "main".into(),
            author_name: "AGRO Harness".into(),
            author_email: "harness@agro.local".into(),
        }
    }
}

// ── Receipt ──────────────────────────────────────────────────────────────────

/// Record of a successful (or no-op) mirror publication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionPublishReceipt {
    pub published_at: DateTime<Utc>,
    pub compaction_count: u64,
    /// Absolute path to the offload directory that was staged.
    pub checkpoint_dir: String,
    /// Short SHA of the commit that was pushed, if any.
    ///
    /// `None` when there were no changes to commit (idempotent push).
    pub committed_sha: Option<String>,
}

// ── Public API ───────────────────────────────────────────────────────────────

/// Stage `offload/current/`, commit, and push to the configured remote branch.
///
/// The function is idempotent: if there are no staged changes after `git add`
/// it returns an `Ok` receipt with `committed_sha: None` rather than failing.
///
/// # Errors
///
/// Returns `Err` if:
/// - `git add` fails (unexpected filesystem issue)
/// - `git commit` fails for a reason other than "nothing to commit"
/// - `git push` fails (network error, expired credential, etc.)
///
/// The caller is expected to surface push failures as capability gaps so the
/// next session can retry rather than silently losing the compaction record.
pub async fn publish_compaction_to_mirror(
    config: &MirrorPublishConfig,
    compaction_count: u64,
    session_id: &str,
) -> Result<CompactionPublishReceipt> {
    let repo = &config.repo_root;
    let offload_path = repo.join(&config.offload_rel_path);
    let now = Utc::now();

    // Stage the offload directory.
    git_run(
        repo,
        &["add", &config.offload_rel_path],
        "stage offload directory",
    )
    .await?;

    // Check whether there is anything new to commit.
    let status_out = git_output(
        repo,
        &["status", "--porcelain", &config.offload_rel_path],
    )
    .await?;

    if status_out.trim().is_empty() {
        // Nothing changed — this compaction was a no-op on the offload side.
        return Ok(CompactionPublishReceipt {
            published_at: now,
            compaction_count,
            checkpoint_dir: offload_path.display().to_string(),
            committed_sha: None,
        });
    }

    let message = format!(
        "harness: compaction #{compaction_count} checkpoint\n\nsession={session_id} at={}",
        now.to_rfc3339(),
    );

    git_run_with_env(
        repo,
        &["commit", "-m", &message],
        &[
            ("GIT_AUTHOR_NAME", config.author_name.as_str()),
            ("GIT_AUTHOR_EMAIL", config.author_email.as_str()),
            ("GIT_COMMITTER_NAME", config.author_name.as_str()),
            ("GIT_COMMITTER_EMAIL", config.author_email.as_str()),
        ],
        "commit compaction checkpoint",
    )
    .await?;

    // Capture the short SHA of the commit we just made.
    let raw_sha = git_output(repo, &["rev-parse", "--short", "HEAD"])
        .await
        .unwrap_or_default();
    let sha = raw_sha.trim().to_string();
    let committed_sha = if sha.is_empty() { None } else { Some(sha) };

    // Push to the remote.  Failure here is intentionally propagated so the
    // caller can gap it rather than masking a lost compaction record.
    git_run(
        repo,
        &[
            "push",
            &config.remote,
            &format!("HEAD:{}", config.branch),
        ],
        "push compaction checkpoint to mirror",
    )
    .await
    .context(
        "mirror push failed — surface as capability gap and retry next compaction cycle",
    )?;

    Ok(CompactionPublishReceipt {
        published_at: now,
        compaction_count,
        checkpoint_dir: offload_path.display().to_string(),
        committed_sha,
    })
}

/// Walk parent directories starting from `start` to locate the `.git` root.
///
/// Use this to derive `MirrorPublishConfig::repo_root` from `session_root`
/// without hard-coding the path.
pub fn find_repo_root(start: &Path) -> Result<PathBuf> {
    let mut current = start.to_path_buf();
    loop {
        if current.join(".git").exists() {
            return Ok(current);
        }
        match current.parent() {
            Some(parent) => current = parent.to_path_buf(),
            None => {
                return Err(anyhow::anyhow!(
                    "could not find .git root from {}",
                    start.display()
                ))
            }
        }
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

async fn git_run(repo: &Path, args: &[&str], context: &str) -> Result<()> {
    let status = Command::new("git")
        .current_dir(repo)
        .args(args)
        .status()
        .await
        .with_context(|| format!("spawn git for: {context}"))?;

    if !status.success() {
        return Err(anyhow::anyhow!(
            "git {} exited {:?}: {context}",
            args.join(" "),
            status.code()
        ));
    }
    Ok(())
}

async fn git_run_with_env(
    repo: &Path,
    args: &[&str],
    env: &[(&str, &str)],
    context: &str,
) -> Result<()> {
    let mut cmd = Command::new("git");
    cmd.current_dir(repo).args(args);
    for (key, val) in env {
        cmd.env(key, val);
    }
    let status = cmd
        .status()
        .await
        .with_context(|| format!("spawn git for: {context}"))?;

    if !status.success() {
        return Err(anyhow::anyhow!(
            "git {} exited {:?}: {context}",
            args.join(" "),
            status.code()
        ));
    }
    Ok(())
}

async fn git_output(repo: &Path, args: &[&str]) -> Result<String> {
    let out = Command::new("git")
        .current_dir(repo)
        .args(args)
        .output()
        .await
        .context("spawn git for output")?;
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}
