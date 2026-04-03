mod runtime;
mod session;

use std::collections::BTreeSet;
use std::sync::Arc;

use chrono::Utc;
use splcw_core::{
    CapabilityGap, CapabilityGapStatus, PlanSnapshot, Receipt, Recodification, SufficientPlan,
};
use splcw_memory::{
    CheckpointManifest, CompactionVerifier, CurrentSurface, FilesystemOffloadSink, OffloadSink,
    ResumeVerification, StateStore, VerificationWatermark,
};
use uuid::Uuid;

pub use runtime::{
    RuntimeTurnOptions, RuntimeTurnOutcome, SupervisedGithubActionKind,
    SupervisedGithubActionRequest,
};
pub use session::{
    RuntimeCompactionResult, RuntimeGapRecord, RuntimePendingTurnCheckpoint,
    RuntimePendingTurnPhase, RuntimeSessionConfig, RuntimeSessionEvent, RuntimeSessionEventKind,
    RuntimeSessionJournal, RuntimeSessionState, RuntimeToolOutcomeRecord, RuntimeTurnRecord,
};

const MIRROR_VERIFICATION_GAP_TITLE: &str = "Mirror checkpoint drift detected";
const MIRROR_VERIFICATION_FIX_TARGET: &str =
    "Reconcile local continuity watermark with mirrored checkpoint state";
const MIRROR_VERIFICATION_REVEALED_BY_PREFIX: &str = "mirror-checkpoint:";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeLanes {
    pub session: String,
    pub global: String,
}

impl RuntimeLanes {
    pub fn new(session: impl Into<String>, global: impl Into<String>) -> Self {
        Self {
            session: session.into(),
            global: global.into(),
        }
    }
}

pub struct OrchestratorState {
    pub plan: SufficientPlan,
    pub current_snapshot: PlanSnapshot,
    pub pending_gaps: Vec<CapabilityGap>,
    pub recent_receipts: Vec<Receipt>,
    pub recent_recodifications: Vec<Recodification>,
}

impl OrchestratorState {
    pub fn new(
        current_snapshot: PlanSnapshot,
        pending_gaps: Vec<CapabilityGap>,
        recent_receipts: Vec<Receipt>,
        recent_recodifications: Vec<Recodification>,
    ) -> Self {
        Self {
            plan: current_snapshot.plan.clone(),
            current_snapshot,
            pending_gaps,
            recent_receipts,
            recent_recodifications,
        }
    }
}

pub struct PersistentOrchestrator<S, O> {
    store: Arc<S>,
    offload: Arc<O>,
}

impl<S, O> PersistentOrchestrator<S, O>
where
    S: StateStore,
    O: OffloadSink,
{
    pub fn new(store: Arc<S>, offload: Arc<O>) -> Self {
        Self { store, offload }
    }

    pub async fn hydrate(&self, receipt_limit: usize) -> anyhow::Result<Option<OrchestratorState>> {
        let Some(snapshot) = self.store.load_current_snapshot().await? else {
            return Ok(None);
        };

        let pending_gaps = self
            .store
            .list_capability_gaps(Some(CapabilityGapStatus::Open))
            .await?;
        let recent_receipts = self.store.list_recent_receipts(receipt_limit).await?;
        let recent_recodifications = self
            .store
            .list_recent_recodifications(receipt_limit)
            .await?;

        Ok(Some(OrchestratorState::new(
            snapshot,
            pending_gaps,
            recent_receipts,
            recent_recodifications,
        )))
    }

    pub async fn adopt_plan(&self, snapshot: &PlanSnapshot) -> anyhow::Result<()> {
        self.store.save_plan_snapshot(snapshot).await?;
        self.offload.push_plan_snapshot(snapshot).await?;
        self.publish_runtime_continuity_if_available().await?;
        Ok(())
    }

    pub async fn record_receipt(&self, receipt: &Receipt) -> anyhow::Result<()> {
        self.store.append_receipt(receipt).await?;
        self.offload.push_receipt_summary(receipt).await?;
        self.publish_runtime_continuity_if_available().await?;
        Ok(())
    }

    pub async fn surface_gap(&self, gap: &CapabilityGap) -> anyhow::Result<()> {
        self.store.record_gap(gap).await?;
        self.offload.push_capability_gap(gap).await?;
        self.publish_runtime_continuity_if_available().await?;
        Ok(())
    }

    pub async fn recodify(
        &self,
        recodification: &Recodification,
        next_snapshot: &PlanSnapshot,
    ) -> anyhow::Result<()> {
        self.store.append_recodification(recodification).await?;
        self.store.save_plan_snapshot(next_snapshot).await?;
        self.offload.push_recodification(recodification).await?;
        self.offload.push_plan_snapshot(next_snapshot).await?;
        self.publish_runtime_continuity_if_available().await?;
        Ok(())
    }

    pub async fn compact_runtime_session(
        &self,
        config: RuntimeSessionConfig,
        keep_recent_events: usize,
        summary: impl Into<String>,
        post_compaction_refresh: Option<String>,
    ) -> anyhow::Result<RuntimeCompactionResult> {
        let mut journal = RuntimeSessionJournal::open_with_stale_lock_cleanup(config).await?;
        let result = journal
            .compact_transcript(keep_recent_events, summary, post_compaction_refresh)
            .await?;
        self.publish_runtime_continuity_with_artifacts_if_available(&result.artifact_paths)
            .await?;
        Ok(result)
    }

    async fn publish_runtime_continuity_if_available(&self) -> anyhow::Result<bool> {
        self.publish_runtime_continuity_with_artifacts_if_available(&[])
            .await
    }

    async fn publish_runtime_continuity_with_artifacts_if_available(
        &self,
        extra_artifact_paths: &[String],
    ) -> anyhow::Result<bool> {
        if self.store.load_current_snapshot().await?.is_none() {
            return Ok(false);
        }
        self.publish_runtime_continuity_with_artifacts(extra_artifact_paths)
            .await?;
        Ok(true)
    }

    pub async fn publish_runtime_continuity(&self) -> anyhow::Result<()> {
        self.publish_runtime_continuity_with_artifacts(&[]).await
    }

    async fn publish_runtime_continuity_with_artifacts(
        &self,
        extra_artifact_paths: &[String],
    ) -> anyhow::Result<()> {
        let Some(state) = self.hydrate(20).await? else {
            anyhow::bail!("cannot publish runtime continuity without a current plan snapshot");
        };
        let watermark = self.store.current_watermark().await?;
        let brief_markdown = format!(
            "# Runtime Brief\n\n- objective: {}\n- active module: `{}`\n- constraints tracked: {}\n- recent receipts tracked: {}\n- open gaps tracked: {}\n",
            state.plan.objective,
            state.plan.active_module,
            state.plan.constraints.len(),
            state.recent_receipts.len(),
            state.pending_gaps.len()
        );
        let handoff_markdown = render_runtime_handoff(&state);
        let surface = build_current_surface(
            &state,
            watermark.clone(),
            brief_markdown.clone(),
            handoff_markdown.clone(),
        )?;

        self.offload.push_current_surface(&surface).await?;

        let artifact_paths =
            merge_artifact_paths(current_surface_artifact_paths(), extra_artifact_paths);
        let manifest =
            CompactionVerifier::build_checkpoint(chrono::Utc::now(), watermark, artifact_paths);
        self.offload.push_checkpoint_manifest(&manifest).await?;
        if self.reconcile_mirror_continuity(&manifest).await? {
            let Some(updated_state) = self.hydrate(20).await? else {
                anyhow::bail!("cannot refresh runtime continuity without a current plan snapshot");
            };
            let updated_watermark = self.store.current_watermark().await?;
            let updated_surface = build_current_surface(
                &updated_state,
                updated_watermark,
                brief_markdown,
                handoff_markdown,
            )?;
            self.offload.push_current_surface(&updated_surface).await?;
        }
        Ok(())
    }

    async fn reconcile_mirror_continuity(
        &self,
        local_manifest: &CheckpointManifest,
    ) -> anyhow::Result<bool> {
        let open_gaps = self
            .store
            .list_capability_gaps(Some(CapabilityGapStatus::Open))
            .await?;
        let existing_gap = open_gaps
            .iter()
            .find(|gap| is_mirror_verification_gap(gap))
            .cloned();

        let Some(mirrored) = self.offload.latest_checkpoint_manifest().await? else {
            return Ok(false);
        };

        let normalized_local = normalize_watermark_for_existing_mirror_gap(
            &local_manifest.watermark,
            existing_gap.is_some(),
        );
        let verification = CompactionVerifier::verify_resume(&normalized_local, &mirrored);
        if verification.matches {
            return Ok(false);
        }

        let notes = build_mirror_verification_notes(&mirrored, &verification);
        if let Some(existing) = existing_gap {
            let revealed_by = mirror_gap_revealed_by(&mirrored);
            if existing.revealed_by == revealed_by && existing.notes == notes {
                return Ok(false);
            }

            let updated = CapabilityGap {
                revealed_by,
                last_touched_at: Utc::now(),
                notes,
                ..existing
            };
            self.store.record_gap(&updated).await?;
            self.offload.push_capability_gap(&updated).await?;
            return Ok(true);
        }

        let now = Utc::now();
        let gap = CapabilityGap {
            id: Uuid::new_v4(),
            title: MIRROR_VERIFICATION_GAP_TITLE.into(),
            revealed_by: mirror_gap_revealed_by(&mirrored),
            permanent_fix_target: MIRROR_VERIFICATION_FIX_TARGET.into(),
            status: CapabilityGapStatus::Open,
            discovered_at: now,
            last_touched_at: now,
            notes,
        };
        self.store.record_gap(&gap).await?;
        self.offload.push_capability_gap(&gap).await?;
        Ok(true)
    }
}

impl<S> PersistentOrchestrator<S, FilesystemOffloadSink>
where
    S: StateStore,
{
    pub async fn publish_current_surface(
        &self,
        brief_markdown: String,
        handoff_markdown: String,
    ) -> anyhow::Result<CurrentSurface> {
        let Some(state) = self.hydrate(20).await? else {
            anyhow::bail!("cannot publish current surface without a current plan snapshot");
        };

        let watermark = self.store.current_watermark().await?;
        let plan_markdown = format!(
            "# Current Plan\n\n- objective: {}\n- plan id: `{}`\n- version: `{}`\n- active module: `{}`\n",
            state.plan.objective, state.plan.id, state.plan.version, state.plan.active_module
        );

        let open_gaps_markdown = if state.pending_gaps.is_empty() {
            "# Open Gaps\n\n- none".to_string()
        } else {
            let items = state
                .pending_gaps
                .iter()
                .map(|gap| format!("- `{}`: {}", gap.id, gap.title))
                .collect::<Vec<_>>()
                .join("\n");
            format!("# Open Gaps\n\n{}", items)
        };

        let manifest = serde_json::json!({
            "surface": "current",
            "plan_id": watermark.plan_id,
            "plan_snapshot_id": watermark.plan_snapshot_id,
            "plan_version": watermark.plan_version,
            "receipt_count": watermark.receipt_count,
            "gap_count": watermark.gap_count,
            "open_gap_count": watermark.open_gap_count,
            "recodification_count": watermark.recodification_count
        });

        let surface = CurrentSurface {
            brief_markdown,
            plan_markdown,
            open_gaps_markdown,
            handoff_markdown,
            manifest_json: serde_json::to_string_pretty(&manifest)?,
        };

        self.offload.push_current_surface(&surface).await?;
        Ok(surface)
    }

    pub async fn publish_checkpoint(
        &self,
        artifact_paths: Vec<String>,
    ) -> anyhow::Result<CheckpointManifest> {
        let watermark = self.store.current_watermark().await?;
        let manifest =
            CompactionVerifier::build_checkpoint(chrono::Utc::now(), watermark, artifact_paths);
        self.offload.push_checkpoint_manifest(&manifest).await?;
        Ok(manifest)
    }
}

fn build_current_surface(
    state: &OrchestratorState,
    watermark: splcw_memory::VerificationWatermark,
    brief_markdown: String,
    handoff_markdown: String,
) -> anyhow::Result<CurrentSurface> {
    let plan_markdown = format!(
        "# Current Plan\n\n- objective: {}\n- plan id: `{}`\n- version: `{}`\n- active module: `{}`\n",
        state.plan.objective, state.plan.id, state.plan.version, state.plan.active_module
    );

    let open_gaps_markdown = if state.pending_gaps.is_empty() {
        "# Open Gaps\n\n- none".to_string()
    } else {
        let items = state
            .pending_gaps
            .iter()
            .map(|gap| format!("- `{}`: {}", gap.id, gap.title))
            .collect::<Vec<_>>()
            .join("\n");
        format!("# Open Gaps\n\n{}", items)
    };

    let manifest = serde_json::json!({
        "surface": "current",
        "plan_id": watermark.plan_id,
        "plan_snapshot_id": watermark.plan_snapshot_id,
        "plan_version": watermark.plan_version,
        "receipt_count": watermark.receipt_count,
        "gap_count": watermark.gap_count,
        "open_gap_count": watermark.open_gap_count,
        "recodification_count": watermark.recodification_count
    });

    Ok(CurrentSurface {
        brief_markdown,
        plan_markdown,
        open_gaps_markdown,
        handoff_markdown,
        manifest_json: serde_json::to_string_pretty(&manifest)?,
    })
}

fn is_mirror_verification_gap(gap: &CapabilityGap) -> bool {
    gap.title == MIRROR_VERIFICATION_GAP_TITLE
        && gap.permanent_fix_target == MIRROR_VERIFICATION_FIX_TARGET
}

fn mirror_gap_revealed_by(mirrored: &CheckpointManifest) -> String {
    format!(
        "{}{}",
        MIRROR_VERIFICATION_REVEALED_BY_PREFIX, mirrored.checkpoint_id
    )
}

fn normalize_watermark_for_existing_mirror_gap(
    watermark: &VerificationWatermark,
    has_existing_mirror_gap: bool,
) -> VerificationWatermark {
    if !has_existing_mirror_gap {
        return watermark.clone();
    }

    VerificationWatermark {
        open_gap_count: watermark.open_gap_count.saturating_sub(1),
        gap_count: watermark.gap_count.saturating_sub(1),
        ..watermark.clone()
    }
}

fn build_mirror_verification_notes(
    mirrored: &CheckpointManifest,
    verification: &ResumeVerification,
) -> Vec<String> {
    let mut notes = vec![
        format!("mirrored checkpoint: {}", mirrored.checkpoint_id),
        format!(
            "mirrored generated at: {}",
            mirrored.generated_at.to_rfc3339()
        ),
    ];
    notes.extend(
        verification
            .contradictions
            .iter()
            .map(|entry| format!("contradiction: {entry}")),
    );
    notes
}

fn render_runtime_handoff(state: &OrchestratorState) -> String {
    let next_receipt = state
        .recent_receipts
        .first()
        .map(|receipt| format!("- latest receipt: {}", receipt.changed))
        .unwrap_or_else(|| "- latest receipt: none".into());
    let next_gap = state
        .pending_gaps
        .first()
        .map(|gap| format!("- open gap: {} -> {}", gap.title, gap.permanent_fix_target))
        .unwrap_or_else(|| "- open gap: none".into());

    format!(
        "# Runtime Handoff\n\n- continue from active module: `{}`\n{}\n{}\n- next action: continue the runtime loop or recodify the strongest open gap before retrying\n",
        state.plan.active_module, next_receipt, next_gap
    )
}

fn current_surface_artifact_paths() -> Vec<String> {
    vec![
        "offload/current/brief.md".into(),
        "offload/current/plan.md".into(),
        "offload/current/open-gaps.md".into(),
        "offload/current/handoff.md".into(),
        "offload/current/manifest.json".into(),
    ]
}

fn merge_artifact_paths(base: Vec<String>, extras: &[String]) -> Vec<String> {
    let mut merged = BTreeSet::new();
    for path in base.into_iter().chain(extras.iter().cloned()) {
        let trimmed = path.trim();
        if !trimmed.is_empty() {
            merged.insert(trimmed.to_string());
        }
    }
    merged.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use chrono::Utc;
    use splcw_core::{
        CapabilityGapStatus, Invariant, PlanModule, PlanSnapshot, Receipt, Recodification,
        SplcwUnit, SufficientPlan,
    };
    use splcw_memory::{OffloadSink, SqliteStateStore};
    use uuid::Uuid;

    use super::*;

    #[derive(Default)]
    struct RecordingOffloadSink {
        plan_snapshots: Mutex<usize>,
        receipts: Mutex<usize>,
        gaps: Mutex<usize>,
        recodifications: Mutex<usize>,
        checkpoints: Mutex<usize>,
        current_surfaces: Mutex<usize>,
        latest_checkpoint: Mutex<Option<CheckpointManifest>>,
        forced_mirror_checkpoint: Mutex<Option<CheckpointManifest>>,
    }

    #[async_trait]
    impl OffloadSink for RecordingOffloadSink {
        async fn push_plan_snapshot(&self, _snapshot: &PlanSnapshot) -> anyhow::Result<()> {
            *self.plan_snapshots.lock().unwrap() += 1;
            Ok(())
        }

        async fn push_receipt_summary(&self, _receipt: &Receipt) -> anyhow::Result<()> {
            *self.receipts.lock().unwrap() += 1;
            Ok(())
        }

        async fn push_capability_gap(&self, _gap: &CapabilityGap) -> anyhow::Result<()> {
            *self.gaps.lock().unwrap() += 1;
            Ok(())
        }

        async fn push_recodification(
            &self,
            _recodification: &Recodification,
        ) -> anyhow::Result<()> {
            *self.recodifications.lock().unwrap() += 1;
            Ok(())
        }

        async fn push_checkpoint_manifest(
            &self,
            manifest: &CheckpointManifest,
        ) -> anyhow::Result<()> {
            *self.checkpoints.lock().unwrap() += 1;
            *self.latest_checkpoint.lock().unwrap() = Some(manifest.clone());
            Ok(())
        }

        async fn push_current_surface(&self, _surface: &CurrentSurface) -> anyhow::Result<()> {
            *self.current_surfaces.lock().unwrap() += 1;
            Ok(())
        }

        async fn latest_checkpoint_manifest(&self) -> anyhow::Result<Option<CheckpointManifest>> {
            Ok(self
                .forced_mirror_checkpoint
                .lock()
                .unwrap()
                .clone()
                .or_else(|| self.latest_checkpoint.lock().unwrap().clone()))
        }
    }

    fn sample_plan_snapshot(version: i64) -> PlanSnapshot {
        let plan = SufficientPlan {
            id: Uuid::new_v4(),
            version,
            objective: "Keep the pilot durable".into(),
            constraints: vec!["Publish continuity after state changes".into()],
            invariants: vec![Invariant {
                key: "continuity".into(),
                description: "Offloaded truth stays current".into(),
            }],
            modules: vec![PlanModule {
                key: "runtime".into(),
                description: "Advance one verified step".into(),
                success_checks: vec!["Current surface reflects state".into()],
                reveal_response: "Record the gap and recodify".into(),
            }],
            active_module: "runtime".into(),
            recodification_rule: "Encode revealed insufficiency into the next plan".into(),
            updated_at: Utc::now(),
        };

        PlanSnapshot {
            snapshot_id: Uuid::new_v4(),
            plan,
            rationale: "test snapshot".into(),
            source_gap_id: None,
            recorded_at: Utc::now(),
        }
    }

    fn temp_session_root(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "splcw-orchestrator-publication-{}-{}",
            label,
            Uuid::new_v4()
        ))
    }

    #[tokio::test]
    async fn orchestrator_mutations_publish_runtime_continuity() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload.clone());
        let snapshot = sample_plan_snapshot(1);

        orchestrator.adopt_plan(&snapshot).await.unwrap();
        assert_eq!(*offload.plan_snapshots.lock().unwrap(), 1);
        assert_eq!(*offload.current_surfaces.lock().unwrap(), 1);
        assert_eq!(*offload.checkpoints.lock().unwrap(), 1);

        let receipt = Receipt {
            id: Uuid::new_v4(),
            plan_id: snapshot.plan.id,
            unit: SplcwUnit::Sculptor,
            observed: "The desktop was steady".into(),
            attempted: "Published current state".into(),
            changed: "Continuity output refreshed".into(),
            contradicted: None,
            enabled_next: "Gap surfacing".into(),
            recorded_at: Utc::now(),
        };
        orchestrator.record_receipt(&receipt).await.unwrap();
        assert_eq!(*offload.receipts.lock().unwrap(), 1);
        assert_eq!(*offload.current_surfaces.lock().unwrap(), 2);
        assert_eq!(*offload.checkpoints.lock().unwrap(), 2);

        let gap = CapabilityGap {
            id: Uuid::new_v4(),
            title: "Continuity publication gap".into(),
            revealed_by: "test".into(),
            permanent_fix_target: "Refresh offload/current after mutations".into(),
            status: CapabilityGapStatus::Open,
            discovered_at: Utc::now(),
            last_touched_at: Utc::now(),
            notes: vec!["surfaced during test".into()],
        };
        orchestrator.surface_gap(&gap).await.unwrap();
        assert_eq!(*offload.gaps.lock().unwrap(), 1);
        assert_eq!(*offload.current_surfaces.lock().unwrap(), 3);
        assert_eq!(*offload.checkpoints.lock().unwrap(), 3);

        let next_snapshot = sample_plan_snapshot(2);
        let recodification = Recodification {
            id: Uuid::new_v4(),
            plan_id: snapshot.plan.id,
            triggered_by_gap: Some(gap.id),
            prior_plan_version: snapshot.plan.version,
            new_plan_version: next_snapshot.plan.version,
            rationale: "Broaden publication ownership".into(),
            verified_by: vec!["mutation continuity test".into()],
            recorded_at: Utc::now(),
        };
        orchestrator
            .recodify(&recodification, &next_snapshot)
            .await
            .unwrap();
        assert_eq!(*offload.recodifications.lock().unwrap(), 1);
        assert_eq!(*offload.plan_snapshots.lock().unwrap(), 2);
        assert_eq!(*offload.current_surfaces.lock().unwrap(), 4);
        assert_eq!(*offload.checkpoints.lock().unwrap(), 4);
    }

    #[tokio::test]
    async fn orchestrator_compaction_publishes_runtime_continuity() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload.clone());
        let snapshot = sample_plan_snapshot(1);
        orchestrator.adopt_plan(&snapshot).await.unwrap();

        let root = temp_session_root("compaction");
        let session = RuntimeSessionConfig::new(&root, "compaction-alpha");
        let journal = RuntimeSessionJournal::open(session.clone()).await.unwrap();
        journal
            .append_event(&RuntimeSessionEvent::turn_started(
                "compaction-alpha",
                "plan-1",
                "started",
            ))
            .await
            .unwrap();
        drop(journal);

        let result = orchestrator
            .compact_runtime_session(
                session.clone(),
                0,
                "compaction completed",
                Some("reload runtime state".into()),
            )
            .await
            .unwrap();

        assert!(result.archive_path.exists());
        assert_eq!(result.compaction_count, 1);
        assert_eq!(*offload.current_surfaces.lock().unwrap(), 2);
        assert_eq!(*offload.checkpoints.lock().unwrap(), 2);

        let reopened = RuntimeSessionJournal::open(session).await.unwrap();
        assert_eq!(reopened.state().compaction_count, 1);
        assert_eq!(
            reopened.pending_post_compaction_refresh(),
            Some("reload runtime state")
        );
        let manifest = offload.latest_checkpoint.lock().unwrap().clone().unwrap();
        assert!(
            manifest
                .artifact_paths
                .contains(&"offload/current/brief.md".into())
        );
        assert!(
            manifest
                .artifact_paths
                .contains(&"transcript-compaction-1.jsonl".into())
        );
        assert!(
            manifest
                .artifact_paths
                .contains(&"turn-log-compaction-1.jsonl".into())
        );
        assert!(
            manifest
                .artifact_paths
                .contains(&"transcript-1.jsonl".into())
        );
        assert!(manifest.artifact_paths.contains(&"turn-log.jsonl".into()));

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn orchestrator_compaction_without_snapshot_skips_publication() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload.clone());

        let root = temp_session_root("compaction-no-snapshot");
        let session = RuntimeSessionConfig::new(&root, "compaction-beta");
        let journal = RuntimeSessionJournal::open(session.clone()).await.unwrap();
        journal
            .append_event(&RuntimeSessionEvent::turn_started(
                "compaction-beta",
                "plan-1",
                "started",
            ))
            .await
            .unwrap();
        drop(journal);

        let result = orchestrator
            .compact_runtime_session(session.clone(), 0, "compaction completed", None)
            .await
            .unwrap();

        assert_eq!(result.compaction_count, 1);
        assert_eq!(*offload.current_surfaces.lock().unwrap(), 0);
        assert_eq!(*offload.checkpoints.lock().unwrap(), 0);
        assert!(offload.latest_checkpoint.lock().unwrap().is_none());

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn orchestrator_publication_surfaces_mirror_drift_as_capability_gap() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload.clone());
        let snapshot = sample_plan_snapshot(1);

        *offload.forced_mirror_checkpoint.lock().unwrap() = Some(CheckpointManifest {
            checkpoint_id: "2026-04-01T000000Z".into(),
            generated_at: Utc::now(),
            watermark: VerificationWatermark {
                receipt_count: 9,
                ..store.current_watermark().await.unwrap()
            },
            artifact_paths: vec!["offload/current/manifest.json".into()],
        });

        orchestrator.adopt_plan(&snapshot).await.unwrap();

        let gaps = store
            .list_capability_gaps(Some(CapabilityGapStatus::Open))
            .await
            .unwrap();
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].title, MIRROR_VERIFICATION_GAP_TITLE);
        assert!(
            gaps[0]
                .notes
                .iter()
                .any(|entry| entry.contains("receipt_count mismatch"))
        );
        assert_eq!(*offload.gaps.lock().unwrap(), 1);
        assert_eq!(*offload.checkpoints.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn orchestrator_publication_dedupes_repeated_mirror_drift_gap() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload.clone());
        let snapshot = sample_plan_snapshot(1);

        *offload.forced_mirror_checkpoint.lock().unwrap() = Some(CheckpointManifest {
            checkpoint_id: "2026-04-01T000000Z".into(),
            generated_at: Utc::now(),
            watermark: VerificationWatermark {
                receipt_count: 11,
                ..store.current_watermark().await.unwrap()
            },
            artifact_paths: vec!["offload/current/manifest.json".into()],
        });

        orchestrator.adopt_plan(&snapshot).await.unwrap();
        orchestrator.publish_runtime_continuity().await.unwrap();

        let gaps = store
            .list_capability_gaps(Some(CapabilityGapStatus::Open))
            .await
            .unwrap();
        assert_eq!(gaps.len(), 1);
        assert_eq!(*offload.gaps.lock().unwrap(), 1);
        assert_eq!(*offload.checkpoints.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn orchestrator_publication_skips_mirror_gap_when_manifest_matches() {
        let store = Arc::new(SqliteStateStore::connect_in_memory().await.unwrap());
        let offload = Arc::new(RecordingOffloadSink::default());
        let orchestrator = PersistentOrchestrator::new(store.clone(), offload.clone());
        let snapshot = sample_plan_snapshot(1);

        orchestrator.adopt_plan(&snapshot).await.unwrap();
        let watermark = store.current_watermark().await.unwrap();
        *offload.forced_mirror_checkpoint.lock().unwrap() = Some(CheckpointManifest {
            checkpoint_id: "2026-04-01T000001Z".into(),
            generated_at: Utc::now(),
            watermark,
            artifact_paths: vec!["offload/current/manifest.json".into()],
        });

        orchestrator.publish_runtime_continuity().await.unwrap();

        let gaps = store
            .list_capability_gaps(Some(CapabilityGapStatus::Open))
            .await
            .unwrap();
        assert!(gaps.is_empty());
        assert_eq!(*offload.gaps.lock().unwrap(), 0);
        assert_eq!(*offload.checkpoints.lock().unwrap(), 2);
    }
}
