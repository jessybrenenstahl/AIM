mod offload;
mod sqlite_store;

use async_trait::async_trait;
use splcw_core::{
    CapabilityGap, CapabilityGapStatus, PlanSnapshot, Receipt, Recodification, SufficientPlan,
};

pub use offload::{
    CheckpointManifest, CompactionVerifier, CurrentSurface, FilesystemOffloadSink, OffloadArtifact,
    ResumeVerification, VerificationWatermark,
};
pub use sqlite_store::SqliteStateStore;

#[async_trait]
pub trait StateStore: Send + Sync {
    async fn load_plan(&self) -> anyhow::Result<Option<SufficientPlan>>;
    async fn load_current_snapshot(&self) -> anyhow::Result<Option<PlanSnapshot>>;
    async fn current_watermark(&self) -> anyhow::Result<VerificationWatermark>;
    async fn save_plan_snapshot(&self, snapshot: &PlanSnapshot) -> anyhow::Result<()>;
    async fn append_receipt(&self, receipt: &Receipt) -> anyhow::Result<()>;
    async fn list_recent_receipts(&self, limit: usize) -> anyhow::Result<Vec<Receipt>>;
    async fn record_gap(&self, gap: &CapabilityGap) -> anyhow::Result<()>;
    async fn list_capability_gaps(
        &self,
        status: Option<CapabilityGapStatus>,
    ) -> anyhow::Result<Vec<CapabilityGap>>;
    async fn append_recodification(&self, recodification: &Recodification) -> anyhow::Result<()>;
    async fn list_recent_recodifications(
        &self,
        limit: usize,
    ) -> anyhow::Result<Vec<Recodification>>;
}

#[async_trait]
pub trait OffloadSink: Send + Sync {
    async fn push_plan_snapshot(&self, snapshot: &PlanSnapshot) -> anyhow::Result<()>;
    async fn push_receipt_summary(&self, receipt: &Receipt) -> anyhow::Result<()>;
    async fn push_capability_gap(&self, gap: &CapabilityGap) -> anyhow::Result<()>;
    async fn push_recodification(&self, recodification: &Recodification) -> anyhow::Result<()>;
    async fn push_checkpoint_manifest(&self, manifest: &CheckpointManifest) -> anyhow::Result<()>;
    async fn push_current_surface(&self, _surface: &CurrentSurface) -> anyhow::Result<()> {
        Ok(())
    }
    async fn latest_checkpoint_manifest(&self) -> anyhow::Result<Option<CheckpointManifest>> {
        Ok(None)
    }
}
