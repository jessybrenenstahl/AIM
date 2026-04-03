use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ObservationFrame {
    pub captured_at: DateTime<Utc>,
    pub summary: String,
    pub screenshot_path: Option<String>,
    pub ocr_text: Option<String>,
    pub active_window: Option<String>,
    pub window_titles: Vec<String>,
    pub clipboard_text: Option<String>,
    pub structured_signals: Vec<StructuredSignal>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StructuredSignal {
    pub key: String,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MouseButton {
    Left,
    Right,
    Middle,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ProposedAction {
    FocusWindow {
        title: String,
    },
    Click {
        x: i32,
        y: i32,
        button: MouseButton,
    },
    DoubleClick {
        x: i32,
        y: i32,
        button: MouseButton,
    },
    Drag {
        from_x: i32,
        from_y: i32,
        to_x: i32,
        to_y: i32,
    },
    TypeText {
        text: String,
        submit: bool,
    },
    Hotkey {
        chord: String,
    },
    Scroll {
        delta: i32,
    },
    LaunchProcess {
        command: String,
        args: Vec<String>,
    },
    WaitFor {
        signal: String,
        timeout_ms: u64,
    },
    CaptureObservation,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActionExecution {
    pub id: Uuid,
    pub action: ProposedAction,
    pub backend: String,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence: Option<Value>,
    pub recorded_at: DateTime<Utc>,
}

#[async_trait]
pub trait ComputerUsePlanner: Send + Sync {
    async fn plan_next_action(&self, frame: &ObservationFrame) -> anyhow::Result<ProposedAction>;
}
