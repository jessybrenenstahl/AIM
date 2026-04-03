use std::path::{Path, PathBuf};

use anyhow::Context;
use async_trait::async_trait;
use chrono::{DateTime, Datelike, NaiveDateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use splcw_core::{CapabilityGap, PlanSnapshot, Receipt, Recodification};
use tokio::fs;

use crate::OffloadSink;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffloadArtifact {
    pub relative_path: PathBuf,
    pub body: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CurrentSurface {
    pub brief_markdown: String,
    pub plan_markdown: String,
    pub open_gaps_markdown: String,
    pub handoff_markdown: String,
    pub manifest_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VerificationWatermark {
    pub plan_id: Option<String>,
    pub plan_snapshot_id: Option<String>,
    pub plan_version: i64,
    pub receipt_count: i64,
    pub latest_receipt_id: Option<String>,
    pub open_gap_count: i64,
    pub gap_count: i64,
    pub recodification_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointManifest {
    pub checkpoint_id: String,
    pub generated_at: DateTime<Utc>,
    pub watermark: VerificationWatermark,
    pub artifact_paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumeVerification {
    pub matches: bool,
    pub contradictions: Vec<String>,
}

pub struct CompactionVerifier;

impl CompactionVerifier {
    pub fn build_checkpoint(
        generated_at: DateTime<Utc>,
        watermark: VerificationWatermark,
        artifact_paths: Vec<String>,
    ) -> CheckpointManifest {
        CheckpointManifest {
            checkpoint_id: generated_at.format("%Y-%m-%dT%H%M%SZ").to_string(),
            generated_at,
            watermark,
            artifact_paths,
        }
    }

    pub fn verify_resume(
        local: &VerificationWatermark,
        mirrored: &CheckpointManifest,
    ) -> ResumeVerification {
        let mut contradictions = Vec::new();
        let remote = &mirrored.watermark;

        if local.plan_id != remote.plan_id {
            contradictions.push(format!(
                "plan_id mismatch: local={:?} mirrored={:?}",
                local.plan_id, remote.plan_id
            ));
        }
        if local.plan_snapshot_id != remote.plan_snapshot_id {
            contradictions.push(format!(
                "plan_snapshot_id mismatch: local={:?} mirrored={:?}",
                local.plan_snapshot_id, remote.plan_snapshot_id
            ));
        }
        if local.plan_version != remote.plan_version {
            contradictions.push(format!(
                "plan_version mismatch: local={} mirrored={}",
                local.plan_version, remote.plan_version
            ));
        }
        if local.receipt_count != remote.receipt_count {
            contradictions.push(format!(
                "receipt_count mismatch: local={} mirrored={}",
                local.receipt_count, remote.receipt_count
            ));
        }
        if local.latest_receipt_id != remote.latest_receipt_id {
            contradictions.push(format!(
                "latest_receipt_id mismatch: local={:?} mirrored={:?}",
                local.latest_receipt_id, remote.latest_receipt_id
            ));
        }
        if local.open_gap_count != remote.open_gap_count {
            contradictions.push(format!(
                "open_gap_count mismatch: local={} mirrored={}",
                local.open_gap_count, remote.open_gap_count
            ));
        }
        if local.gap_count != remote.gap_count {
            contradictions.push(format!(
                "gap_count mismatch: local={} mirrored={}",
                local.gap_count, remote.gap_count
            ));
        }
        if local.recodification_count != remote.recodification_count {
            contradictions.push(format!(
                "recodification_count mismatch: local={} mirrored={}",
                local.recodification_count, remote.recodification_count
            ));
        }

        ResumeVerification {
            matches: contradictions.is_empty(),
            contradictions,
        }
    }
}

pub struct FilesystemOffloadSink {
    root: PathBuf,
}

impl FilesystemOffloadSink {
    pub fn new<P: AsRef<Path>>(root: P) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    pub fn render_plan_snapshot(snapshot: &PlanSnapshot) -> OffloadArtifact {
        let filename = format!(
            "offload/history/checkpoints/{:04}-{:02}-{:02}T{}/plan-v{:04}-{}.md",
            snapshot.recorded_at.year(),
            snapshot.recorded_at.month(),
            snapshot.recorded_at.day(),
            snapshot.recorded_at.format("%H%M%SZ"),
            snapshot.plan.version,
            snapshot.snapshot_id
        );

        let constraints = render_bullets(&snapshot.plan.constraints);
        let invariants = render_invariants(snapshot);
        let modules = render_modules(snapshot);

        OffloadArtifact {
            relative_path: PathBuf::from(filename),
            body: format!(
                "# Plan Snapshot\n\n- snapshot: `{}`\n- plan id: `{}`\n- version: `{}`\n- recorded at: `{}`\n- source gap: `{}`\n\n## Objective\n{}\n\n## Active Module\n`{}`\n\n## Rationale\n{}\n\n## Constraints\n{}\n\n## Invariants\n{}\n\n## Modules\n{}\n\n## Recodification Rule\n{}\n",
                snapshot.snapshot_id,
                snapshot.plan.id,
                snapshot.plan.version,
                snapshot.recorded_at.to_rfc3339(),
                snapshot
                    .source_gap_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "none".into()),
                snapshot.plan.objective,
                snapshot.plan.active_module,
                snapshot.rationale,
                constraints,
                invariants,
                modules,
                snapshot.plan.recodification_rule
            ),
        }
    }

    pub fn render_receipt_summary(receipt: &Receipt) -> OffloadArtifact {
        let filename = format!(
            "offload/history/checkpoints/{:04}-{:02}-{:02}T{}/receipt-{}.md",
            receipt.recorded_at.year(),
            receipt.recorded_at.month(),
            receipt.recorded_at.day(),
            receipt.recorded_at.format("%H%M%SZ"),
            receipt.id
        );

        OffloadArtifact {
            relative_path: PathBuf::from(filename),
            body: format!(
                "# Receipt Summary\n\n- receipt: `{}`\n- plan id: `{}`\n- unit: `{:?}`\n- recorded at: `{}`\n\n## Observed\n{}\n\n## Attempted\n{}\n\n## Changed\n{}\n\n## Contradicted\n{}\n\n## Enabled Next\n{}\n",
                receipt.id,
                receipt.plan_id,
                receipt.unit,
                receipt.recorded_at.to_rfc3339(),
                receipt.observed,
                receipt.attempted,
                receipt.changed,
                receipt.contradicted.as_deref().unwrap_or("none"),
                receipt.enabled_next
            ),
        }
    }

    pub fn render_capability_gap(gap: &CapabilityGap) -> OffloadArtifact {
        OffloadArtifact {
            relative_path: PathBuf::from(format!("offload/history/capability-gaps/{}.md", gap.id)),
            body: format!(
                "# Capability Gap\n\n- gap: `{}`\n- status: `{:?}`\n- discovered at: `{}`\n- last touched at: `{}`\n- revealed by: `{}`\n\n## Title\n{}\n\n## Permanent Fix Target\n{}\n\n## Notes\n{}\n",
                gap.id,
                gap.status,
                gap.discovered_at.to_rfc3339(),
                gap.last_touched_at.to_rfc3339(),
                gap.revealed_by,
                gap.title,
                gap.permanent_fix_target,
                render_bullets(&gap.notes)
            ),
        }
    }

    pub fn render_recodification(recodification: &Recodification) -> OffloadArtifact {
        OffloadArtifact {
            relative_path: PathBuf::from(format!(
                "offload/history/recodifications/{}.md",
                recodification.id
            )),
            body: format!(
                "# Recodification\n\n- recodification: `{}`\n- plan id: `{}`\n- prior version: `{}`\n- new version: `{}`\n- recorded at: `{}`\n- triggered by gap: `{}`\n\n## Rationale\n{}\n\n## Verified By\n{}\n",
                recodification.id,
                recodification.plan_id,
                recodification.prior_plan_version,
                recodification.new_plan_version,
                recodification.recorded_at.to_rfc3339(),
                recodification
                    .triggered_by_gap
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "none".into()),
                recodification.rationale,
                render_bullets(&recodification.verified_by)
            ),
        }
    }

    pub fn render_checkpoint_manifest(manifest: &CheckpointManifest) -> OffloadArtifact {
        OffloadArtifact {
            relative_path: PathBuf::from(format!(
                "offload/history/checkpoints/{}/manifest.json",
                manifest.checkpoint_id
            )),
            body: serde_json::to_string_pretty(manifest)
                .expect("checkpoint manifest should serialize"),
        }
    }

    pub fn render_current_surface(surface: &CurrentSurface) -> Vec<OffloadArtifact> {
        vec![
            OffloadArtifact {
                relative_path: PathBuf::from("offload/current/brief.md"),
                body: surface.brief_markdown.clone(),
            },
            OffloadArtifact {
                relative_path: PathBuf::from("offload/current/plan.md"),
                body: surface.plan_markdown.clone(),
            },
            OffloadArtifact {
                relative_path: PathBuf::from("offload/current/open-gaps.md"),
                body: surface.open_gaps_markdown.clone(),
            },
            OffloadArtifact {
                relative_path: PathBuf::from("offload/current/handoff.md"),
                body: surface.handoff_markdown.clone(),
            },
            OffloadArtifact {
                relative_path: PathBuf::from("offload/current/manifest.json"),
                body: surface.manifest_json.clone(),
            },
        ]
    }

    pub async fn latest_checkpoint_manifest(&self) -> anyhow::Result<Option<CheckpointManifest>> {
        let checkpoints_root = self.root.join("offload/history/checkpoints");
        if !checkpoints_root.exists() {
            return Ok(None);
        }

        let mut entries = fs::read_dir(&checkpoints_root)
            .await
            .with_context(|| format!("read checkpoint directory {}", checkpoints_root.display()))?;

        let mut candidates = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_dir() {
                let name = entry.file_name().to_string_lossy().to_string();
                let Some(timestamp) = parse_checkpoint_id(&name) else {
                    continue;
                };
                candidates.push((timestamp, name));
            }
        }

        candidates.sort_by(|left, right| right.0.cmp(&left.0));

        for (_, name) in candidates {
            let path = checkpoints_root.join(name).join("manifest.json");
            let body = match fs::read_to_string(&path).await {
                Ok(body) => body,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("read checkpoint manifest {}", path.display()));
                }
            };
            return Ok(Some(
                serde_json::from_str(&body).context("deserialize checkpoint manifest")?,
            ));
        }

        Ok(None)
    }

    async fn write_artifact(&self, artifact: OffloadArtifact) -> anyhow::Result<()> {
        let full_path = self.root.join(&artifact.relative_path);
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create offload directory {}", parent.display()))?;
        }

        fs::write(&full_path, artifact.body)
            .await
            .with_context(|| format!("write offload artifact {}", full_path.display()))?;

        Ok(())
    }
}

#[async_trait]
impl OffloadSink for FilesystemOffloadSink {
    async fn push_plan_snapshot(&self, snapshot: &PlanSnapshot) -> anyhow::Result<()> {
        self.write_artifact(Self::render_plan_snapshot(snapshot))
            .await
    }

    async fn push_receipt_summary(&self, receipt: &Receipt) -> anyhow::Result<()> {
        self.write_artifact(Self::render_receipt_summary(receipt))
            .await
    }

    async fn push_capability_gap(&self, gap: &CapabilityGap) -> anyhow::Result<()> {
        self.write_artifact(Self::render_capability_gap(gap)).await
    }

    async fn push_recodification(&self, recodification: &Recodification) -> anyhow::Result<()> {
        self.write_artifact(Self::render_recodification(recodification))
            .await
    }

    async fn push_checkpoint_manifest(&self, manifest: &CheckpointManifest) -> anyhow::Result<()> {
        self.write_artifact(Self::render_checkpoint_manifest(manifest))
            .await
    }

    async fn push_current_surface(&self, surface: &CurrentSurface) -> anyhow::Result<()> {
        FilesystemOffloadSink::push_current_surface(self, surface).await
    }

    async fn latest_checkpoint_manifest(&self) -> anyhow::Result<Option<CheckpointManifest>> {
        FilesystemOffloadSink::latest_checkpoint_manifest(self).await
    }
}

impl FilesystemOffloadSink {
    pub async fn push_current_surface(&self, surface: &CurrentSurface) -> anyhow::Result<()> {
        for artifact in Self::render_current_surface(surface) {
            self.write_artifact(artifact).await?;
        }
        Ok(())
    }
}

fn render_bullets(lines: &[String]) -> String {
    if lines.is_empty() {
        return "- none".into();
    }

    lines
        .iter()
        .map(|line| format!("- {}", line))
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_invariants(snapshot: &PlanSnapshot) -> String {
    let lines = snapshot
        .plan
        .invariants
        .iter()
        .map(|invariant| format!("`{}`: {}", invariant.key, invariant.description))
        .collect::<Vec<_>>();
    render_bullets(&lines)
}

fn render_modules(snapshot: &PlanSnapshot) -> String {
    if snapshot.plan.modules.is_empty() {
        return "- none".into();
    }

    snapshot
        .plan
        .modules
        .iter()
        .map(|module| {
            format!(
                "- `{}`: {}\n  - success checks: {}\n  - reveal response: {}",
                module.key,
                module.description,
                render_inline_values(&module.success_checks),
                module.reveal_response
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_inline_values(values: &[String]) -> String {
    if values.is_empty() {
        return "none".into();
    }

    values.join("; ")
}

fn parse_checkpoint_id(checkpoint_id: &str) -> Option<DateTime<Utc>> {
    let timestamp = NaiveDateTime::parse_from_str(checkpoint_id, "%Y-%m-%dT%H%M%SZ").ok()?;
    Some(Utc.from_utc_datetime(&timestamp))
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use splcw_core::{
        CapabilityGap, CapabilityGapStatus, Invariant, PlanModule, PlanSnapshot, Receipt,
        Recodification, SplcwUnit, SufficientPlan,
    };
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::*;

    fn sample_plan(version: i64) -> SufficientPlan {
        SufficientPlan {
            id: Uuid::new_v4(),
            version,
            objective: "Pilot the desktop and preserve sufficiency".into(),
            constraints: vec!["Do not lose state".into()],
            invariants: vec![Invariant {
                key: "receipts".into(),
                description: "Every action produces a receipt".into(),
            }],
            modules: vec![PlanModule {
                key: "observe".into(),
                description: "Capture state before acting".into(),
                success_checks: vec!["Fresh screenshot".into()],
                reveal_response: "Recodify observation seam".into(),
            }],
            active_module: "observe".into(),
            recodification_rule: "Encode insufficiency into the next plan.".into(),
            updated_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn filesystem_sink_writes_mirror_tree() {
        let dir = tempdir().unwrap();
        let sink = FilesystemOffloadSink::new(dir.path());
        let plan = sample_plan(2);
        let gap = CapabilityGap {
            id: Uuid::new_v4(),
            title: "Need stronger thread selection".into(),
            revealed_by: "receipt-1".into(),
            permanent_fix_target: "Selector acquisition".into(),
            status: CapabilityGapStatus::Open,
            discovered_at: Utc::now(),
            last_touched_at: Utc::now(),
            notes: vec!["First reveal".into()],
        };
        let snapshot = PlanSnapshot {
            snapshot_id: Uuid::new_v4(),
            plan: plan.clone(),
            rationale: "Bootstrapped plan".into(),
            source_gap_id: Some(gap.id),
            recorded_at: Utc::now(),
        };
        let receipt = Receipt {
            id: Uuid::new_v4(),
            plan_id: plan.id,
            unit: SplcwUnit::Warden,
            observed: "Window list stable".into(),
            attempted: "Bound control loop".into(),
            changed: "Lease granted".into(),
            contradicted: None,
            enabled_next: "Action emission".into(),
            recorded_at: Utc::now(),
        };
        let recodification = Recodification {
            id: Uuid::new_v4(),
            plan_id: plan.id,
            triggered_by_gap: Some(gap.id),
            prior_plan_version: 2,
            new_plan_version: 3,
            rationale: "Promote selectors into the sufficient plan".into(),
            verified_by: vec!["receipt".into()],
            recorded_at: Utc::now(),
        };

        sink.push_plan_snapshot(&snapshot).await.unwrap();
        sink.push_receipt_summary(&receipt).await.unwrap();
        sink.push_capability_gap(&gap).await.unwrap();
        sink.push_recodification(&recodification).await.unwrap();

        let plan_path = dir
            .path()
            .join(FilesystemOffloadSink::render_plan_snapshot(&snapshot).relative_path);
        let gap_path = dir
            .path()
            .join(FilesystemOffloadSink::render_capability_gap(&gap).relative_path);
        let receipt_path = dir
            .path()
            .join(FilesystemOffloadSink::render_receipt_summary(&receipt).relative_path);
        let recodification_path = dir
            .path()
            .join(FilesystemOffloadSink::render_recodification(&recodification).relative_path);

        assert!(plan_path.exists());
        assert!(gap_path.exists());
        assert!(receipt_path.exists());
        assert!(recodification_path.exists());
    }

    #[tokio::test]
    async fn checkpoint_manifest_round_trips_and_verifies() {
        let dir = tempdir().unwrap();
        let sink = FilesystemOffloadSink::new(dir.path());
        let watermark = VerificationWatermark {
            plan_id: Some("plan-1".into()),
            plan_snapshot_id: Some("snap-1".into()),
            plan_version: 7,
            receipt_count: 12,
            latest_receipt_id: Some("receipt-12".into()),
            open_gap_count: 2,
            gap_count: 5,
            recodification_count: 3,
        };
        let manifest = CompactionVerifier::build_checkpoint(
            Utc::now(),
            watermark.clone(),
            vec!["offload/history/checkpoints/test/plan.md".into()],
        );

        sink.push_checkpoint_manifest(&manifest).await.unwrap();
        let as_sink: &dyn OffloadSink = &sink;
        let mirrored = as_sink.latest_checkpoint_manifest().await.unwrap().unwrap();
        assert_eq!(mirrored.watermark, watermark);

        let verification = CompactionVerifier::verify_resume(&watermark, &mirrored);
        assert!(verification.matches);

        let mismatched = VerificationWatermark {
            receipt_count: watermark.receipt_count + 1,
            ..watermark
        };
        let mismatch = CompactionVerifier::verify_resume(&mismatched, &mirrored);
        assert!(!mismatch.matches);
        assert!(
            mismatch
                .contradictions
                .iter()
                .any(|entry| entry.contains("receipt_count mismatch"))
        );
    }

    #[test]
    fn render_plan_snapshot_renders_empty_sections_as_none() {
        let mut plan = sample_plan(2);
        plan.constraints.clear();
        plan.invariants.clear();
        plan.modules.clear();
        let snapshot = PlanSnapshot {
            snapshot_id: Uuid::new_v4(),
            plan,
            rationale: "Bootstrapped plan".into(),
            source_gap_id: None,
            recorded_at: Utc::now(),
        };

        let artifact = FilesystemOffloadSink::render_plan_snapshot(&snapshot);

        assert!(artifact.body.contains("## Constraints\n- none"));
        assert!(artifact.body.contains("## Invariants\n- none"));
        assert!(artifact.body.contains("## Modules\n- none"));
    }

    #[tokio::test]
    async fn latest_checkpoint_manifest_uses_newest_valid_timestamp_directory() {
        let dir = tempdir().unwrap();
        let sink = FilesystemOffloadSink::new(dir.path());
        let older_generated_at = Utc.with_ymd_and_hms(2026, 4, 2, 9, 0, 0).unwrap();
        let newer_generated_at = Utc.with_ymd_and_hms(2026, 4, 2, 10, 0, 0).unwrap();
        let older = CompactionVerifier::build_checkpoint(
            older_generated_at,
            VerificationWatermark {
                plan_id: Some("plan-older".into()),
                plan_snapshot_id: Some("snap-older".into()),
                plan_version: 1,
                receipt_count: 1,
                latest_receipt_id: Some("receipt-1".into()),
                open_gap_count: 0,
                gap_count: 0,
                recodification_count: 0,
            },
            vec!["offload/history/checkpoints/older/plan.md".into()],
        );
        let newer = CompactionVerifier::build_checkpoint(
            newer_generated_at,
            VerificationWatermark {
                plan_id: Some("plan-newer".into()),
                plan_snapshot_id: Some("snap-newer".into()),
                plan_version: 2,
                receipt_count: 2,
                latest_receipt_id: Some("receipt-2".into()),
                open_gap_count: 1,
                gap_count: 1,
                recodification_count: 0,
            },
            vec!["offload/history/checkpoints/newer/plan.md".into()],
        );

        sink.push_checkpoint_manifest(&newer).await.unwrap();
        sink.push_checkpoint_manifest(&older).await.unwrap();

        let invalid_root = dir
            .path()
            .join("offload/history/checkpoints/not-a-checkpoint");
        fs::create_dir_all(&invalid_root).await.unwrap();
        fs::write(
            invalid_root.join("manifest.json"),
            serde_json::to_string_pretty(&older).unwrap(),
        )
        .await
        .unwrap();

        let mirrored = sink.latest_checkpoint_manifest().await.unwrap().unwrap();
        assert_eq!(mirrored.checkpoint_id, newer.checkpoint_id);
        assert_eq!(mirrored.watermark.plan_id.as_deref(), Some("plan-newer"));
    }

    #[tokio::test]
    async fn latest_checkpoint_manifest_skips_newer_timestamp_dir_without_manifest() {
        let dir = tempdir().unwrap();
        let sink = FilesystemOffloadSink::new(dir.path());
        let older_generated_at = Utc.with_ymd_and_hms(2026, 4, 2, 9, 0, 0).unwrap();
        let older = CompactionVerifier::build_checkpoint(
            older_generated_at,
            VerificationWatermark {
                plan_id: Some("plan-older".into()),
                plan_snapshot_id: Some("snap-older".into()),
                plan_version: 1,
                receipt_count: 1,
                latest_receipt_id: Some("receipt-1".into()),
                open_gap_count: 0,
                gap_count: 0,
                recodification_count: 0,
            },
            vec!["offload/history/checkpoints/older/plan.md".into()],
        );
        sink.push_checkpoint_manifest(&older).await.unwrap();

        let newer_without_manifest = dir
            .path()
            .join("offload/history/checkpoints/2026-04-02T100000Z");
        fs::create_dir_all(&newer_without_manifest).await.unwrap();
        fs::write(
            newer_without_manifest.join("plan-v0002-snapshot.md"),
            "# Plan",
        )
        .await
        .unwrap();

        let mirrored = sink.latest_checkpoint_manifest().await.unwrap().unwrap();
        assert_eq!(mirrored.checkpoint_id, older.checkpoint_id);
    }

    #[tokio::test]
    async fn latest_checkpoint_manifest_errors_on_corrupt_manifest() {
        let dir = tempdir().unwrap();
        let sink = FilesystemOffloadSink::new(dir.path());
        let corrupt_root = dir
            .path()
            .join("offload/history/checkpoints/2026-04-02T100000Z");
        fs::create_dir_all(&corrupt_root).await.unwrap();
        fs::write(corrupt_root.join("manifest.json"), "{not-json")
            .await
            .unwrap();

        let error = sink.latest_checkpoint_manifest().await.unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("deserialize checkpoint manifest"));
    }

    #[tokio::test]
    async fn current_surface_writes_all_resume_files() {
        let dir = tempdir().unwrap();
        let sink = FilesystemOffloadSink::new(dir.path());
        let surface = CurrentSurface {
            brief_markdown: "# Brief".into(),
            plan_markdown: "# Plan".into(),
            open_gaps_markdown: "# Gaps".into(),
            handoff_markdown: "# Handoff".into(),
            manifest_json: "{\"ok\":true}".into(),
        };

        sink.push_current_surface(&surface).await.unwrap();

        assert!(dir.path().join("offload/current/brief.md").exists());
        assert!(dir.path().join("offload/current/plan.md").exists());
        assert!(dir.path().join("offload/current/open-gaps.md").exists());
        assert!(dir.path().join("offload/current/handoff.md").exists());
        assert!(dir.path().join("offload/current/manifest.json").exists());
    }
}
