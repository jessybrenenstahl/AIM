use async_trait::async_trait;
use std::time::Duration as StdDuration;

use splcw_core::{CapabilityGap, Receipt};
use tokio::sync::mpsc;

#[async_trait]
pub trait ResidentSessionTransport: Send + Sync {
    async fn run_turn(
        &self,
        session_id: &str,
        objective: &str,
        grounding: &str,
        events: &mpsc::UnboundedSender<ResidentSessionEvent>,
    ) -> anyhow::Result<String>;
}

#[derive(Debug, Default)]
pub struct SimulatedResidentTransport;

#[async_trait]
impl ResidentSessionTransport for SimulatedResidentTransport {
    async fn run_turn(
        &self,
        _session_id: &str,
        objective: &str,
        grounding: &str,
        events: &mpsc::UnboundedSender<ResidentSessionEvent>,
    ) -> anyhow::Result<String> {
        let response = format!(
            "[RESIDENT SESSION]\nObjective: {objective}\nMemory injected: {} chars\n",
            grounding.len()
        );

        for chunk in response.split_whitespace() {
            let _ = events.send(ResidentSessionEvent::Chunk {
                delta: format!("{chunk} "),
            });
            tokio::time::sleep(StdDuration::from_millis(8)).await;
        }

        Ok(response)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResidentSessionEvent {
    StreamStart { session_id: String },
    Chunk { delta: String },
    ToolUse { tool: String, input: String },
    ToolResult { output: String },
    ResponseDone { full_response: String },
    GapDetected { gap: CapabilityGap },
    ContradictionDetected { contradiction: String },
    TurnComplete { receipt: Receipt },
}

pub type SessionEvent = ResidentSessionEvent;

pub fn create_resident_session_channel(
) -> (
    mpsc::UnboundedSender<ResidentSessionEvent>,
    mpsc::UnboundedReceiver<ResidentSessionEvent>,
) {
    mpsc::unbounded_channel()
}
