mod auth_store;
mod client;
mod providers;

use std::fmt;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub use auth_store::FileAuthProfileStore;
pub use client::{
    AuthLifecycleBlockedProfile, AuthLifecycleResumeReport, ConfiguredLlmClient,
    ConfiguredProviderCall, ProviderRegistry, ResolvedProfile,
};
pub use providers::{
    OpenAiResponsesProvider, builtin_interactive_oauth_client_id, inspect_runtime_auth,
    register_openai_responses_providers, resolve_auth_bearer,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ProviderKind {
    OpenAiCodex,
    OpenAiApi,
    Anthropic,
    Xai,
    Local,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AuthMode {
    OAuth,
    ApiKey,
    Local,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OAuthState {
    pub issuer: String,
    pub account_label: Option<String>,
    pub access_token: Option<String>,
    pub refresh_token: Option<String>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum OAuthAuthorizationKind {
    BrowserCallback {
        authorization_url: String,
        redirect_uri: String,
        callback_host: String,
        callback_path: String,
        paste_prompt: String,
    },
    DeviceCode {
        verification_uri: String,
        user_code: String,
        device_code: String,
        poll_interval_seconds: u64,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum OAuthInitiationMode {
    BrowserCallback,
    DeviceCode,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PendingOAuthAuthorization {
    pub id: Uuid,
    pub profile_id: Uuid,
    pub provider: ProviderKind,
    pub label: String,
    pub issuer: String,
    pub started_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub client_id: String,
    pub token_endpoint: String,
    pub scopes: Vec<String>,
    pub state: Option<String>,
    pub pkce_verifier: Option<String>,
    pub kind: OAuthAuthorizationKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApiKeyState {
    pub env_var: Option<String>,
    pub key_material: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthProfile {
    pub id: Uuid,
    pub provider: ProviderKind,
    pub mode: AuthMode,
    pub label: String,
    pub oauth: Option<OAuthState>,
    pub api_key: Option<ApiKeyState>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AuthFailureKind {
    RateLimit,
    Overloaded,
    Billing,
    Authentication,
    Timeout,
    Network,
    ServerError,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ProviderErrorCategory {
    RateLimit,
    Overloaded,
    Billing,
    Authentication,
    Timeout,
    Network,
    ServerError,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderErrorContext {
    pub provider_id: String,
    pub operation: String,
    pub category: ProviderErrorCategory,
    pub status_code: Option<u16>,
    pub retryable: Option<bool>,
}

impl fmt::Display for ProviderErrorContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "provider {} operation {} failed with {:?}",
            self.provider_id, self.operation, self.category
        )?;
        if let Some(status_code) = self.status_code {
            write!(f, " status_code={status_code}")?;
        }
        if let Some(retryable) = self.retryable {
            write!(f, " retryable={retryable}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ProviderErrorContext {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthRuntimeHealthEntry {
    pub profile_id: Uuid,
    pub kind: AuthFailureKind,
    pub failures: u32,
    pub until: DateTime<Utc>,
    pub last_error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct AuthRuntimeHealth {
    pub last_successful_profile: Option<Uuid>,
    pub cooldowns: Vec<AuthRuntimeHealthEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RuntimeAuthReadiness {
    Ready,
    NeedsRefresh,
    Blocked(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderDescriptor {
    pub id: String,
    pub provider: ProviderKind,
    pub display_name: String,
    pub auth_modes: Vec<AuthMode>,
    pub default_model: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ContentBlock {
    Text {
        text: String,
    },
    ImagePath {
        path: String,
    },
    ToolCall {
        id: String,
        name: String,
        arguments: Value,
    },
    ToolResult {
        id: String,
        content: Value,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChatMessage {
    pub role: String,
    pub content: Vec<ContentBlock>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChatRequest {
    pub model: String,
    pub system_prompt: Option<String>,
    pub messages: Vec<ChatMessage>,
    pub tools: Vec<ToolDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChatResponse {
    pub provider_id: String,
    pub model: String,
    pub content: Vec<ContentBlock>,
}

#[async_trait]
pub trait AuthProfileStore: Send + Sync {
    async fn list_profiles(&self) -> anyhow::Result<Vec<AuthProfile>>;
    async fn upsert_profile(&self, profile: &AuthProfile) -> anyhow::Result<()>;
    async fn load_default_profile(&self) -> anyhow::Result<Option<AuthProfile>>;
    async fn set_default_profile(&self, profile_id: Uuid) -> anyhow::Result<()>;
    async fn list_pending_oauth(&self) -> anyhow::Result<Vec<PendingOAuthAuthorization>> {
        Ok(Vec::new())
    }
    async fn upsert_pending_oauth(
        &self,
        _pending: &PendingOAuthAuthorization,
    ) -> anyhow::Result<()> {
        Ok(())
    }
    async fn delete_pending_oauth(&self, _pending_id: Uuid) -> anyhow::Result<()> {
        Ok(())
    }
    async fn load_runtime_health(&self) -> anyhow::Result<Option<AuthRuntimeHealth>> {
        Ok(None)
    }
    async fn save_runtime_health(&self, _health: &AuthRuntimeHealth) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
pub trait LlmProvider: Send + Sync {
    fn descriptor(&self) -> ProviderDescriptor;
    async fn begin_oauth_authorization(
        &self,
        label: Option<&str>,
    ) -> anyhow::Result<Option<PendingOAuthAuthorization>> {
        self.begin_oauth_authorization_with_mode(label, OAuthInitiationMode::BrowserCallback)
            .await
    }
    async fn begin_oauth_authorization_with_mode(
        &self,
        _label: Option<&str>,
        _mode: OAuthInitiationMode,
    ) -> anyhow::Result<Option<PendingOAuthAuthorization>> {
        Ok(None)
    }
    async fn complete_oauth_authorization(
        &self,
        _pending: &PendingOAuthAuthorization,
        _callback_input: &str,
    ) -> anyhow::Result<Option<AuthProfile>> {
        Ok(None)
    }
    async fn materialize_runtime_auth(
        &self,
        _profile: &AuthProfile,
    ) -> anyhow::Result<Option<AuthProfile>> {
        Ok(None)
    }
    async fn chat(&self, auth: &AuthProfile, request: &ChatRequest)
    -> anyhow::Result<ChatResponse>;
}

pub fn openai_codex_descriptor(default_model: &str) -> ProviderDescriptor {
    ProviderDescriptor {
        id: "openai-codex".into(),
        provider: ProviderKind::OpenAiCodex,
        display_name: "OpenAI Codex".into(),
        auth_modes: vec![AuthMode::OAuth],
        default_model: default_model.into(),
    }
}

pub fn openai_api_descriptor(default_model: &str) -> ProviderDescriptor {
    ProviderDescriptor {
        id: "openai-api".into(),
        provider: ProviderKind::OpenAiApi,
        display_name: "OpenAI API".into(),
        auth_modes: vec![AuthMode::ApiKey, AuthMode::OAuth],
        default_model: default_model.into(),
    }
}
