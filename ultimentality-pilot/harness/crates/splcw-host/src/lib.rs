pub mod backend;
mod command_host;
mod embedded_host;

use async_trait::async_trait;
use splcw_computer_use::{ActionExecution, ObservationFrame, ProposedAction};

pub use command_host::BackendPlatform;
pub use command_host::{CommandHostBody, HostCommandSpec};
pub use embedded_host::EmbeddedHostBody;

#[async_trait]
pub trait HostBody: Send + Sync {
    async fn observe(&self) -> anyhow::Result<ObservationFrame>;
    async fn enact(&self, action: &ProposedAction) -> anyhow::Result<ActionExecution>;
    async fn verify_post_action(
        &self,
        execution: &ActionExecution,
    ) -> anyhow::Result<ObservationFrame>;
}
