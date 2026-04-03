use anyhow::Context;
use async_trait::async_trait;
use splcw_computer_use::{ActionExecution, ObservationFrame, ProposedAction};

use crate::HostBody;
use crate::backend;

#[derive(Debug, Default, Clone, Copy)]
pub struct EmbeddedHostBody;

#[async_trait]
impl HostBody for EmbeddedHostBody {
    async fn observe(&self) -> anyhow::Result<ObservationFrame> {
        tokio::task::spawn_blocking(backend::observe_frame)
            .await
            .context("join embedded observe task")?
    }

    async fn enact(&self, action: &ProposedAction) -> anyhow::Result<ActionExecution> {
        let action = action.clone();
        tokio::task::spawn_blocking(move || backend::enact_action_direct(&action))
            .await
            .context("join embedded enact task")?
    }

    async fn verify_post_action(
        &self,
        execution: &ActionExecution,
    ) -> anyhow::Result<ObservationFrame> {
        let execution = execution.clone();
        tokio::task::spawn_blocking(move || backend::verify_post_action_direct(&execution))
            .await
            .context("join embedded verify task")?
    }
}
