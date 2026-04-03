use std::env;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow, bail};
use async_trait::async_trait;
use base64::Engine;
use chrono::Utc;
use reqwest::Client;
use reqwest::StatusCode;
use reqwest::Url;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::time::{sleep, timeout};
use uuid::Uuid;

use crate::{
    AuthMode, AuthProfile, ChatMessage, ChatRequest, ChatResponse, ContentBlock, LlmProvider,
    OAuthAuthorizationKind, OAuthInitiationMode, OAuthState, PendingOAuthAuthorization,
    ProviderDescriptor, ProviderErrorCategory, ProviderErrorContext, ProviderKind,
    ProviderRegistry, RuntimeAuthReadiness, ToolDefinition, openai_api_descriptor,
    openai_codex_descriptor,
};

const DEFAULT_OPENAI_RESPONSES_URL: &str = "https://api.openai.com/v1/responses";
const DEFAULT_OPENAI_OAUTH_AUTHORIZE_URL: &str = "https://auth.openai.com/oauth/authorize";
const DEFAULT_OPENAI_OAUTH_TOKEN_URL: &str = "https://auth.openai.com/oauth/token";
const DEFAULT_OPENAI_OAUTH_DEVICE_AUTHORIZATION_URL: &str =
    "https://auth0.openai.com/oauth/device/code";
const DEFAULT_OPENAI_OAUTH_REDIRECT_URI: &str = "http://localhost:1455/auth/callback";
const DEFAULT_OPENAI_OAUTH_CALLBACK_HOST: &str = "127.0.0.1";
const DEFAULT_OPENAI_OAUTH_CALLBACK_PATH: &str = "/auth/callback";
const DEFAULT_OPENAI_OAUTH_PASTE_PROMPT: &str = "Paste the redirect URL (or authorization code)";
const DEFAULT_OPENAI_OAUTH_ISSUER: &str = "openai";
const DEFAULT_OPENAI_OAUTH_SCOPES: &[&str] = &["openid", "profile", "email", "offline_access"];
const DEFAULT_OPENAI_PENDING_OAUTH_TTL_SECONDS: i64 = 900;
const DEFAULT_OPENAI_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_OPENAI_MAX_RETRIES: usize = 1;
const DEFAULT_OPENAI_RETRY_BACKOFF: Duration = Duration::from_millis(250);
const BUILTIN_OPENAI_CODEX_OAUTH_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";

#[derive(Debug, Clone)]
pub struct OpenAiOAuthConfig {
    pub client_id: String,
    pub authorize_url: String,
    pub token_url: String,
    pub device_authorization_url: String,
    pub redirect_uri: String,
    pub callback_host: String,
    pub callback_path: String,
    pub scopes: Vec<String>,
    pub paste_prompt: String,
}

#[derive(Debug)]
struct OAuthAuthorizationCallback {
    code: String,
    state: Option<String>,
}

pub fn inspect_runtime_auth(auth: &AuthProfile) -> RuntimeAuthReadiness {
    if let Some(oauth) = &auth.oauth {
        return inspect_oauth_runtime_auth(auth, oauth);
    }

    if let Some(api_key) = &auth.api_key {
        if let Some(key) = api_key.key_material.as_ref().map(|value| value.trim()) {
            if !key.is_empty() {
                return RuntimeAuthReadiness::Ready;
            }
        }
        if let Some(env_var) = api_key.env_var.as_ref().map(|value| value.trim()) {
            if !env_var.is_empty() {
                match env::var(env_var) {
                    Ok(resolved) if !resolved.trim().is_empty() => {
                        return RuntimeAuthReadiness::Ready;
                    }
                    Ok(_) | Err(_) => {
                        return RuntimeAuthReadiness::Blocked(format!(
                            "auth profile {} has no usable bearer credential (oauth access token or api key)",
                            auth.label
                        ));
                    }
                }
            }
        }
    }

    RuntimeAuthReadiness::Blocked(format!(
        "auth profile {} has no usable bearer credential (oauth access token or api key)",
        auth.label
    ))
}

pub fn resolve_auth_bearer(auth: &AuthProfile) -> anyhow::Result<String> {
    if let Some(oauth) = &auth.oauth {
        match inspect_oauth_runtime_auth(auth, oauth) {
            RuntimeAuthReadiness::Ready => {
                if let Some(token) = oauth.access_token.as_ref().map(|value| value.trim()) {
                    if !token.is_empty() {
                        return Ok(token.to_string());
                    }
                }
            }
            RuntimeAuthReadiness::NeedsRefresh => {
                if oauth
                    .access_token
                    .as_ref()
                    .map(|value| !value.trim().is_empty())
                    .unwrap_or(false)
                {
                    bail!(
                        "auth profile {} has an expired oauth access token and requires refresh materialization",
                        auth.label
                    );
                }
                bail!(
                    "auth profile {} requires oauth access-token materialization from the stored refresh token",
                    auth.label
                );
            }
            RuntimeAuthReadiness::Blocked(reason) => bail!(reason),
        }
    }

    if let Some(api_key) = &auth.api_key {
        if let Some(key) = api_key.key_material.as_ref().map(|value| value.trim()) {
            if !key.is_empty() {
                return Ok(key.to_string());
            }
        }
        if let Some(env_var) = api_key.env_var.as_ref().map(|value| value.trim()) {
            if !env_var.is_empty() {
                let resolved = env::var(env_var)
                    .with_context(|| format!("read api key from env var {}", env_var))?;
                let trimmed = resolved.trim();
                if !trimmed.is_empty() {
                    return Ok(trimmed.to_string());
                }
            }
        }
    }

    bail!(
        "auth profile {} has no usable bearer credential (oauth access token or api key)",
        auth.label
    )
}

pub fn register_openai_responses_providers(registry: &mut ProviderRegistry) -> anyhow::Result<()> {
    registry.register(Arc::new(OpenAiResponsesProvider::new(
        openai_codex_descriptor("gpt-5.4"),
        None::<String>,
    )?))?;
    registry.register(Arc::new(OpenAiResponsesProvider::new(
        openai_api_descriptor("gpt-5.4-mini"),
        None::<String>,
    )?))?;
    Ok(())
}

pub fn builtin_interactive_oauth_client_id(provider: &ProviderKind) -> Option<&'static str> {
    match provider {
        ProviderKind::OpenAiCodex => Some(BUILTIN_OPENAI_CODEX_OAUTH_CLIENT_ID),
        _ => None,
    }
}

fn default_openai_oauth_config(descriptor: &ProviderDescriptor) -> Option<OpenAiOAuthConfig> {
    let client_id = match descriptor.provider {
        ProviderKind::OpenAiCodex => env_trimmed("SPLCW_OPENAI_CODEX_OAUTH_CLIENT_ID")
            .or_else(|| env_trimmed("OPENAI_CODEX_OAUTH_CLIENT_ID"))
            .or_else(|| env_trimmed("SPLCW_OPENAI_OAUTH_CLIENT_ID"))
            .or_else(|| env_trimmed("OPENAI_OAUTH_CLIENT_ID"))
            .or_else(|| {
                builtin_interactive_oauth_client_id(&descriptor.provider).map(str::to_string)
            }),
        ProviderKind::OpenAiApi => env_trimmed("SPLCW_OPENAI_API_OAUTH_CLIENT_ID")
            .or_else(|| env_trimmed("OPENAI_API_OAUTH_CLIENT_ID"))
            .or_else(|| env_trimmed("SPLCW_OPENAI_OAUTH_CLIENT_ID"))
            .or_else(|| env_trimmed("OPENAI_OAUTH_CLIENT_ID")),
        _ => None,
    }?;

    Some(OpenAiOAuthConfig {
        client_id,
        authorize_url: DEFAULT_OPENAI_OAUTH_AUTHORIZE_URL.to_string(),
        token_url: DEFAULT_OPENAI_OAUTH_TOKEN_URL.to_string(),
        device_authorization_url: DEFAULT_OPENAI_OAUTH_DEVICE_AUTHORIZATION_URL.to_string(),
        redirect_uri: DEFAULT_OPENAI_OAUTH_REDIRECT_URI.to_string(),
        callback_host: DEFAULT_OPENAI_OAUTH_CALLBACK_HOST.to_string(),
        callback_path: DEFAULT_OPENAI_OAUTH_CALLBACK_PATH.to_string(),
        scopes: DEFAULT_OPENAI_OAUTH_SCOPES
            .iter()
            .map(|scope| scope.to_string())
            .collect(),
        paste_prompt: DEFAULT_OPENAI_OAUTH_PASTE_PROMPT.to_string(),
    })
}

fn env_trimmed(name: &str) -> Option<String> {
    env::var(name).ok().and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    })
}

fn default_openai_responses_endpoint(_provider: &ProviderKind) -> &'static str {
    DEFAULT_OPENAI_RESPONSES_URL
}

pub struct OpenAiResponsesProvider {
    descriptor: ProviderDescriptor,
    endpoint: String,
    client: Client,
    oauth: Option<OpenAiOAuthConfig>,
    request_timeout: Duration,
    max_retries: usize,
    retry_backoff: Duration,
}

impl OpenAiResponsesProvider {
    pub fn new(
        descriptor: ProviderDescriptor,
        endpoint: Option<impl Into<String>>,
    ) -> anyhow::Result<Self> {
        Self::with_transport_policy(
            descriptor,
            endpoint,
            DEFAULT_OPENAI_REQUEST_TIMEOUT,
            DEFAULT_OPENAI_MAX_RETRIES,
            DEFAULT_OPENAI_RETRY_BACKOFF,
        )
    }

    pub fn with_transport_policy(
        descriptor: ProviderDescriptor,
        endpoint: Option<impl Into<String>>,
        request_timeout: Duration,
        max_retries: usize,
        retry_backoff: Duration,
    ) -> anyhow::Result<Self> {
        let oauth = default_openai_oauth_config(&descriptor);
        let endpoint = endpoint
            .map(Into::into)
            .unwrap_or_else(|| default_openai_responses_endpoint(&descriptor.provider).to_string());
        Ok(Self {
            descriptor,
            endpoint,
            client: Client::builder()
                .user_agent("splcw-pilot/0.1")
                .timeout(request_timeout)
                .build()
                .context("build OpenAI responses client")?,
            oauth,
            request_timeout,
            max_retries,
            retry_backoff,
        })
    }

    pub fn with_oauth_config(mut self, oauth: OpenAiOAuthConfig) -> Self {
        self.oauth = Some(oauth);
        self
    }

    fn interactive_oauth_config(&self) -> anyhow::Result<&OpenAiOAuthConfig> {
        self.oauth.as_ref().with_context(|| {
            let hint = match self.descriptor.provider {
                ProviderKind::OpenAiCodex => "OpenAI Codex should use the built-in OAuth client id automatically unless you are intentionally overriding it via SPLCW_OPENAI_CODEX_OAUTH_CLIENT_ID (or OPENAI_CODEX_OAUTH_CLIENT_ID)",
                ProviderKind::OpenAiApi => {
                    "set SPLCW_OPENAI_API_OAUTH_CLIENT_ID (or OPENAI_API_OAUTH_CLIENT_ID)"
                }
                _ => "configure an OAuth client id for this provider",
            };
            format!(
                "provider {} is not configured for interactive OAuth initiation; {}",
                self.descriptor.id, hint
            )
        })
    }

    fn provider_error(
        &self,
        operation: &str,
        category: ProviderErrorCategory,
        status_code: Option<StatusCode>,
        retryable: Option<bool>,
    ) -> anyhow::Error {
        anyhow::Error::new(ProviderErrorContext {
            provider_id: self.descriptor.id.clone(),
            operation: operation.to_string(),
            category,
            status_code: status_code.map(|status| status.as_u16()),
            retryable,
        })
    }

    fn begin_browser_callback_oauth(
        &self,
        label: Option<&str>,
    ) -> anyhow::Result<PendingOAuthAuthorization> {
        let oauth = self.interactive_oauth_config()?;
        let profile_label = label
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| default_oauth_label(&self.descriptor));
        let state = compact_nonce();
        let pkce_verifier = format!("{}{}", compact_nonce(), compact_nonce());
        let authorization_url = build_browser_authorization_url(oauth, &state, &pkce_verifier)?;

        let started_at = Utc::now();
        Ok(PendingOAuthAuthorization {
            id: Uuid::new_v4(),
            profile_id: Uuid::new_v4(),
            provider: self.descriptor.provider.clone(),
            label: profile_label.to_string(),
            issuer: DEFAULT_OPENAI_OAUTH_ISSUER.to_string(),
            started_at,
            expires_at: Some(
                started_at + chrono::Duration::seconds(DEFAULT_OPENAI_PENDING_OAUTH_TTL_SECONDS),
            ),
            client_id: oauth.client_id.clone(),
            token_endpoint: oauth.token_url.clone(),
            scopes: oauth.scopes.clone(),
            state: Some(state),
            pkce_verifier: Some(pkce_verifier),
            kind: OAuthAuthorizationKind::BrowserCallback {
                authorization_url,
                redirect_uri: oauth.redirect_uri.clone(),
                callback_host: oauth.callback_host.clone(),
                callback_path: oauth.callback_path.clone(),
                paste_prompt: oauth.paste_prompt.clone(),
            },
        })
    }

    async fn begin_device_code_oauth(
        &self,
        label: Option<&str>,
    ) -> anyhow::Result<PendingOAuthAuthorization> {
        let oauth = self.interactive_oauth_config()?;
        let profile_label = label
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| default_oauth_label(&self.descriptor));
        let device_code = self
            .request_device_code(oauth)
            .await
            .with_context(|| format!("begin device-code oauth for {}", self.descriptor.id))?;
        let poll_interval_seconds = device_code.poll_interval_seconds();
        let started_at = Utc::now();
        let expires_at = match device_code.expires_in {
            Some(seconds) if seconds > 0 => Some(Utc::now() + chrono::Duration::seconds(seconds)),
            Some(seconds) => {
                bail!("device-code oauth returned a non-positive expires_in value: {seconds}");
            }
            None => None,
        };
        Ok(PendingOAuthAuthorization {
            id: Uuid::new_v4(),
            profile_id: Uuid::new_v4(),
            provider: self.descriptor.provider.clone(),
            label: profile_label.to_string(),
            issuer: DEFAULT_OPENAI_OAUTH_ISSUER.to_string(),
            started_at,
            expires_at,
            client_id: oauth.client_id.clone(),
            token_endpoint: oauth.token_url.clone(),
            scopes: oauth.scopes.clone(),
            state: None,
            pkce_verifier: None,
            kind: OAuthAuthorizationKind::DeviceCode {
                verification_uri: device_code
                    .verification_uri_complete
                    .unwrap_or(device_code.verification_uri),
                user_code: device_code.user_code,
                device_code: device_code.device_code,
                poll_interval_seconds,
            },
        })
    }

    fn oauth_token_endpoint(
        &self,
        profile: &AuthProfile,
        oauth: &OAuthState,
    ) -> anyhow::Result<String> {
        let issuer = oauth.issuer.trim();
        if issuer.is_empty() {
            bail!(
                "auth profile {} is missing an oauth issuer for runtime materialization",
                profile.label
            );
        }
        if issuer.eq_ignore_ascii_case("openai") {
            return Ok(DEFAULT_OPENAI_OAUTH_TOKEN_URL.to_string());
        }

        let mut base = Url::parse(issuer).with_context(|| {
            format!(
                "auth profile {} has a non-URL oauth issuer {}; runtime refresh needs a token endpoint",
                profile.label, issuer
            )
        })?;
        if base.path().ends_with("/oauth/token") {
            return Ok(base.to_string());
        }
        if !base.path().ends_with('/') {
            let path = base.path().trim_end_matches('/');
            let next = if path.is_empty() {
                "/".to_string()
            } else {
                format!("{path}/")
            };
            base.set_path(&next);
        }
        Ok(base
            .join("oauth/token")
            .context("derive oauth token endpoint from issuer")?
            .to_string())
    }

    async fn refresh_oauth_profile(&self, profile: &AuthProfile) -> anyhow::Result<AuthProfile> {
        let oauth = profile
            .oauth
            .as_ref()
            .context("runtime oauth materialization requires oauth state")?;
        let refresh_token = oauth
            .refresh_token
            .as_ref()
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .context("runtime oauth materialization requires a refresh token")?;
        let token_endpoint = self.oauth_token_endpoint(profile, oauth)?;
        let mut last_error: Option<anyhow::Error> = None;
        let mut token_response = None;
        for attempt in 0..=self.max_retries {
            match self
                .refresh_token_once(&token_endpoint, refresh_token)
                .await
            {
                Ok(response) => {
                    token_response = Some(response);
                    break;
                }
                Err(error) => {
                    let retryable = is_retryable_openai_error(&error);
                    last_error = Some(error);
                    if retryable && attempt < self.max_retries {
                        sleep(self.retry_backoff.saturating_mul((attempt + 1) as u32)).await;
                        continue;
                    }
                    break;
                }
            }
        }
        let token_response = token_response
            .ok_or_else(|| last_error.unwrap_or_else(|| anyhow!("oauth refresh request failed")))
            .with_context(|| format!("refresh runtime auth for {}", profile.label))?;
        let access_token = token_response
            .access_token
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .context("oauth refresh response did not return a usable access token")?
            .to_string();
        let refresh_token = token_response
            .refresh_token
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string)
            .or_else(|| oauth.refresh_token.clone());
        let expires_at = match token_response.expires_in {
            Some(seconds) if seconds > 0 => Some(Utc::now() + chrono::Duration::seconds(seconds)),
            Some(seconds) => {
                bail!("oauth refresh response returned a non-positive expires_in value: {seconds}");
            }
            None => None,
        };

        Ok(AuthProfile {
            oauth: Some(OAuthState {
                issuer: oauth.issuer.clone(),
                account_label: oauth.account_label.clone(),
                access_token: Some(access_token),
                refresh_token,
                expires_at,
            }),
            updated_at: Utc::now(),
            ..profile.clone()
        })
    }

    async fn refresh_token_once(
        &self,
        token_endpoint: &str,
        refresh_token: &str,
    ) -> anyhow::Result<OAuthRefreshResponse> {
        let response = timeout(self.request_timeout, async {
            self.client
                .post(token_endpoint)
                .form(&[
                    ("grant_type", "refresh_token"),
                    ("refresh_token", refresh_token),
                ])
                .send()
                .await
        })
        .await
        .map_err(|_| {
            self.provider_error(
                "oauth_refresh",
                ProviderErrorCategory::Timeout,
                None,
                Some(false),
            )
        })
        .with_context(|| format!("OAuth refresh timed out after {:?}", self.request_timeout))?
        .map_err(|error| {
            let category = if error.is_timeout() {
                ProviderErrorCategory::Timeout
            } else {
                ProviderErrorCategory::Network
            };
            self.provider_error("oauth_refresh", category, error.status(), Some(false))
                .context(format!("POST {}", token_endpoint))
        })?;
        let status = response.status();
        let text = timeout(self.request_timeout, response.text())
            .await
            .map_err(|_| {
                self.provider_error(
                    "oauth_refresh_response_body",
                    ProviderErrorCategory::Timeout,
                    Some(status),
                    Some(false),
                )
            })
            .with_context(|| {
                format!(
                    "OAuth refresh response body timed out after {:?}",
                    self.request_timeout
                )
            })?
            .context("read oauth refresh response body")?;
        if !status.is_success() {
            return Err(self
                .provider_error(
                    "oauth_refresh",
                    openai_category_from_status(status, &text),
                    Some(status),
                    Some(is_retryable_status(status)),
                )
                .context(format!(
                    "OAuth refresh status_code={} retryable={} body={}",
                    status.as_u16(),
                    is_retryable_status(status),
                    summarize_error_body(&text)
                )));
        }
        serde_json::from_str(&text).context("deserialize oauth refresh response")
    }

    async fn exchange_authorization_code(
        &self,
        pending: &PendingOAuthAuthorization,
        callback: &OAuthAuthorizationCallback,
    ) -> anyhow::Result<AuthProfile> {
        self.interactive_oauth_config()?;
        let redirect_uri = match &pending.kind {
            OAuthAuthorizationKind::BrowserCallback { redirect_uri, .. } => redirect_uri.as_str(),
            OAuthAuthorizationKind::DeviceCode { .. } => {
                return self.exchange_device_code_authorization(pending).await;
            }
        };
        let mut last_error: Option<anyhow::Error> = None;
        let mut token_response = None;
        for attempt in 0..=self.max_retries {
            match self
                .exchange_authorization_code_once(
                    pending.token_endpoint.as_str(),
                    pending.client_id.as_str(),
                    redirect_uri,
                    callback.code.as_str(),
                    pending.pkce_verifier.as_deref(),
                )
                .await
            {
                Ok(response) => {
                    token_response = Some(response);
                    break;
                }
                Err(error) => {
                    let retryable = is_retryable_openai_error(&error);
                    last_error = Some(error);
                    if retryable && attempt < self.max_retries {
                        sleep(self.retry_backoff.saturating_mul((attempt + 1) as u32)).await;
                        continue;
                    }
                    break;
                }
            }
        }
        let token_response = token_response
            .ok_or_else(|| {
                last_error.unwrap_or_else(|| anyhow!("oauth authorization-code exchange failed"))
            })
            .with_context(|| format!("complete oauth authorization for {}", pending.label))?;
        build_oauth_profile_from_token_response(pending, &token_response)
    }

    async fn exchange_authorization_code_once(
        &self,
        token_endpoint: &str,
        client_id: &str,
        redirect_uri: &str,
        code: &str,
        code_verifier: Option<&str>,
    ) -> anyhow::Result<OAuthRefreshResponse> {
        let mut form = vec![
            ("grant_type", "authorization_code".to_string()),
            ("client_id", client_id.to_string()),
            ("code", code.to_string()),
            ("redirect_uri", redirect_uri.to_string()),
        ];
        if let Some(verifier) = code_verifier
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            form.push(("code_verifier", verifier.to_string()));
        }

        let response = timeout(self.request_timeout, async {
            self.client.post(token_endpoint).form(&form).send().await
        })
        .await
        .map_err(|_| {
            self.provider_error(
                "oauth_authorization_code",
                ProviderErrorCategory::Timeout,
                None,
                Some(false),
            )
        })
        .with_context(|| {
            format!(
                "OAuth authorization-code exchange timed out after {:?}",
                self.request_timeout
            )
        })?
        .map_err(|error| {
            let category = if error.is_timeout() {
                ProviderErrorCategory::Timeout
            } else {
                ProviderErrorCategory::Network
            };
            self.provider_error(
                "oauth_authorization_code",
                category,
                error.status(),
                Some(false),
            )
            .context(format!("POST {}", token_endpoint))
        })?;
        let status = response.status();
        let text = timeout(self.request_timeout, response.text())
            .await
            .map_err(|_| {
                self.provider_error(
                    "oauth_authorization_code_response_body",
                    ProviderErrorCategory::Timeout,
                    Some(status),
                    Some(false),
                )
            })
            .with_context(|| {
                format!(
                    "OAuth authorization-code response body timed out after {:?}",
                    self.request_timeout
                )
            })?
            .context("read oauth authorization-code response body")?;
        if !status.is_success() {
            return Err(self
                .provider_error(
                    "oauth_authorization_code",
                    openai_category_from_status(status, &text),
                    Some(status),
                    Some(is_retryable_status(status)),
                )
                .context(format!(
                    "OAuth authorization-code status_code={} retryable={} body={}",
                    status.as_u16(),
                    is_retryable_status(status),
                    summarize_error_body(&text)
                )));
        }
        serde_json::from_str(&text).context("deserialize oauth authorization-code response")
    }

    async fn request_device_code(
        &self,
        oauth: &OpenAiOAuthConfig,
    ) -> anyhow::Result<OpenAiDeviceAuthorizationResponse> {
        let response = timeout(self.request_timeout, async {
            self.client
                .post(&oauth.device_authorization_url)
                .form(&[
                    ("client_id", oauth.client_id.as_str()),
                    ("scope", oauth.scopes.join(" ").as_str()),
                ])
                .send()
                .await
        })
        .await
        .map_err(|_| {
            self.provider_error(
                "oauth_device_authorization",
                ProviderErrorCategory::Timeout,
                None,
                Some(false),
            )
        })
        .with_context(|| {
            format!(
                "OAuth device authorization timed out after {:?}",
                self.request_timeout
            )
        })?
        .map_err(|error| {
            let category = if error.is_timeout() {
                ProviderErrorCategory::Timeout
            } else {
                ProviderErrorCategory::Network
            };
            self.provider_error(
                "oauth_device_authorization",
                category,
                error.status(),
                Some(false),
            )
            .context(format!("POST {}", oauth.device_authorization_url))
        })?;
        let status = response.status();
        let text = timeout(self.request_timeout, response.text())
            .await
            .map_err(|_| {
                self.provider_error(
                    "oauth_device_authorization_response_body",
                    ProviderErrorCategory::Timeout,
                    Some(status),
                    Some(false),
                )
            })
            .with_context(|| {
                format!(
                    "OAuth device authorization response body timed out after {:?}",
                    self.request_timeout
                )
            })?
            .context("read oauth device authorization response body")?;
        if !status.is_success() {
            return Err(self
                .provider_error(
                    "oauth_device_authorization",
                    openai_category_from_status(status, &text),
                    Some(status),
                    Some(is_retryable_status(status)),
                )
                .context(format!(
                    "OAuth device authorization status_code={} retryable={} body={}",
                    status.as_u16(),
                    is_retryable_status(status),
                    summarize_error_body(&text)
                )));
        }
        serde_json::from_str(&text).context("deserialize oauth device authorization response")
    }

    async fn exchange_device_code_authorization(
        &self,
        pending: &PendingOAuthAuthorization,
    ) -> anyhow::Result<AuthProfile> {
        let (device_code, poll_interval_seconds) = match &pending.kind {
            OAuthAuthorizationKind::DeviceCode {
                device_code,
                poll_interval_seconds,
                ..
            } => (device_code.as_str(), *poll_interval_seconds),
            OAuthAuthorizationKind::BrowserCallback { .. } => {
                bail!(
                    "provider {} expected a device-code oauth pending record",
                    self.descriptor.id
                );
            }
        };
        let mut poll_interval = Duration::from_secs(poll_interval_seconds.max(1));
        let deadline = pending
            .expires_at
            .map(|expires_at| expires_at + chrono::Duration::seconds(5));

        loop {
            if let Some(deadline) = deadline {
                if Utc::now() >= deadline {
                    bail!(
                        "device-code oauth authorization for {} expired before completion",
                        pending.label
                    );
                }
            }

            match self
                .exchange_device_code_once(
                    pending.token_endpoint.as_str(),
                    pending.client_id.as_str(),
                    device_code,
                )
                .await?
            {
                DeviceCodePollOutcome::Authorized(token_response) => {
                    return build_oauth_profile_from_token_response(pending, &token_response);
                }
                DeviceCodePollOutcome::Pending {
                    poll_interval_seconds,
                } => {
                    if let Some(seconds) = poll_interval_seconds {
                        poll_interval = Duration::from_secs(seconds.max(1));
                    }
                    sleep(poll_interval).await;
                }
                DeviceCodePollOutcome::SlowDown {
                    poll_interval_seconds,
                } => {
                    let seconds = poll_interval_seconds
                        .unwrap_or_else(|| poll_interval.as_secs().max(1).saturating_add(5));
                    poll_interval = Duration::from_secs(seconds.max(1));
                    sleep(poll_interval).await;
                }
            }
        }
    }

    async fn exchange_device_code_once(
        &self,
        token_endpoint: &str,
        client_id: &str,
        device_code: &str,
    ) -> anyhow::Result<DeviceCodePollOutcome> {
        let response = timeout(self.request_timeout, async {
            self.client
                .post(token_endpoint)
                .form(&[
                    ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                    ("client_id", client_id),
                    ("device_code", device_code),
                ])
                .send()
                .await
        })
        .await
        .map_err(|_| {
            self.provider_error(
                "oauth_device_code",
                ProviderErrorCategory::Timeout,
                None,
                Some(false),
            )
        })
        .with_context(|| {
            format!(
                "OAuth device-code token exchange timed out after {:?}",
                self.request_timeout
            )
        })?
        .map_err(|error| {
            let category = if error.is_timeout() {
                ProviderErrorCategory::Timeout
            } else {
                ProviderErrorCategory::Network
            };
            self.provider_error("oauth_device_code", category, error.status(), Some(false))
                .context(format!("POST {}", token_endpoint))
        })?;
        let status = response.status();
        let text = timeout(self.request_timeout, response.text())
            .await
            .map_err(|_| {
                self.provider_error(
                    "oauth_device_code_response_body",
                    ProviderErrorCategory::Timeout,
                    Some(status),
                    Some(false),
                )
            })
            .with_context(|| {
                format!(
                    "OAuth device-code response body timed out after {:?}",
                    self.request_timeout
                )
            })?
            .context("read oauth device-code response body")?;
        if status.is_success() {
            let token_response: OAuthRefreshResponse =
                serde_json::from_str(&text).context("deserialize oauth device-code response")?;
            return Ok(DeviceCodePollOutcome::Authorized(token_response));
        }

        let error_response = serde_json::from_str::<OpenAiOAuthErrorResponse>(&text).ok();
        if let Some(error_response) = error_response.as_ref() {
            match error_response.error.as_str() {
                "authorization_pending" => {
                    return Ok(DeviceCodePollOutcome::Pending {
                        poll_interval_seconds: error_response.interval,
                    });
                }
                "slow_down" => {
                    return Ok(DeviceCodePollOutcome::SlowDown {
                        poll_interval_seconds: error_response
                            .interval
                            .map(|seconds| seconds.saturating_add(5)),
                    });
                }
                _ => {}
            }
        }
        let error_description = error_response
            .as_ref()
            .and_then(|response| response.error_description.as_deref())
            .filter(|value| !value.trim().is_empty());

        Err(self
            .provider_error(
                "oauth_device_code",
                openai_category_from_status(status, &text),
                Some(status),
                Some(is_retryable_status(status)),
            )
            .context(format!(
                "OAuth device-code status_code={} retryable={} body={}{}",
                status.as_u16(),
                is_retryable_status(status),
                summarize_error_body(&text),
                error_description
                    .map(|value| format!(" description={value}"))
                    .unwrap_or_default()
            )))
    }
}

#[async_trait]
impl LlmProvider for OpenAiResponsesProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        self.descriptor.clone()
    }

    async fn begin_oauth_authorization_with_mode(
        &self,
        label: Option<&str>,
        mode: OAuthInitiationMode,
    ) -> anyhow::Result<Option<PendingOAuthAuthorization>> {
        if !matches!(
            self.descriptor.provider,
            ProviderKind::OpenAiCodex | ProviderKind::OpenAiApi
        ) {
            return Ok(None);
        }
        match mode {
            OAuthInitiationMode::BrowserCallback => {
                Ok(Some(self.begin_browser_callback_oauth(label)?))
            }
            OAuthInitiationMode::DeviceCode => Ok(Some(self.begin_device_code_oauth(label).await?)),
        }
    }

    async fn complete_oauth_authorization(
        &self,
        pending: &PendingOAuthAuthorization,
        callback_input: &str,
    ) -> anyhow::Result<Option<AuthProfile>> {
        if pending.provider != self.descriptor.provider {
            return Ok(None);
        }
        let callback = match &pending.kind {
            OAuthAuthorizationKind::BrowserCallback { .. } => {
                let callback = parse_oauth_callback_input(callback_input)?;
                if let (Some(expected), Some(actual)) =
                    (pending.state.as_deref(), callback.state.as_deref())
                {
                    if expected != actual {
                        bail!(
                            "oauth state mismatch for {}: expected {}, got {}",
                            pending.label,
                            expected,
                            actual
                        );
                    }
                }
                callback
            }
            OAuthAuthorizationKind::DeviceCode { .. } => OAuthAuthorizationCallback {
                code: String::new(),
                state: None,
            },
        };
        Ok(Some(
            self.exchange_authorization_code(pending, &callback).await?,
        ))
    }

    async fn materialize_runtime_auth(
        &self,
        profile: &AuthProfile,
    ) -> anyhow::Result<Option<AuthProfile>> {
        if profile.provider != self.descriptor.provider {
            return Ok(None);
        }
        if profile.mode != AuthMode::OAuth {
            return Ok(None);
        }
        if profile
            .oauth
            .as_ref()
            .and_then(|oauth| oauth.refresh_token.as_ref())
            .map(|value| value.trim().is_empty())
            .unwrap_or(true)
        {
            return Ok(None);
        }
        Ok(Some(self.refresh_oauth_profile(profile).await?))
    }

    async fn chat(
        &self,
        auth: &AuthProfile,
        request: &ChatRequest,
    ) -> anyhow::Result<ChatResponse> {
        if auth.provider != self.descriptor.provider {
            bail!(
                "provider/profile mismatch: adapter {:?} cannot use auth profile {:?}",
                self.descriptor.provider,
                auth.provider
            );
        }

        let bearer = resolve_auth_bearer(auth)?;
        let body = build_responses_request(request, &self.descriptor.provider);
        let mut last_error: Option<anyhow::Error> = None;

        for attempt in 0..=self.max_retries {
            match self.send_once(&bearer, &body).await {
                Ok(value) => return translate_response(&self.descriptor.id, &request.model, value),
                Err(error) => {
                    let retryable = is_retryable_openai_error(&error);
                    last_error = Some(error);
                    if retryable && attempt < self.max_retries {
                        sleep(self.retry_backoff.saturating_mul((attempt + 1) as u32)).await;
                        continue;
                    }
                    break;
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow!("OpenAI responses request failed")))
    }
}

#[derive(Debug, Deserialize)]
struct OAuthRefreshResponse {
    access_token: Option<String>,
    refresh_token: Option<String>,
    expires_in: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct OpenAiDeviceAuthorizationResponse {
    device_code: String,
    user_code: String,
    verification_uri: String,
    #[serde(default)]
    verification_uri_complete: Option<String>,
    #[serde(default)]
    expires_in: Option<i64>,
    #[serde(default, alias = "interval")]
    poll_interval_seconds: Option<u64>,
}

impl OpenAiDeviceAuthorizationResponse {
    fn poll_interval_seconds(&self) -> u64 {
        self.poll_interval_seconds.unwrap_or(5).max(1)
    }
}

#[derive(Debug, Deserialize)]
struct OpenAiOAuthErrorResponse {
    error: String,
    #[serde(default)]
    error_description: Option<String>,
    #[serde(default)]
    interval: Option<u64>,
}

enum DeviceCodePollOutcome {
    Authorized(OAuthRefreshResponse),
    Pending { poll_interval_seconds: Option<u64> },
    SlowDown { poll_interval_seconds: Option<u64> },
}

fn build_browser_authorization_url(
    oauth: &OpenAiOAuthConfig,
    state: &str,
    pkce_verifier: &str,
) -> anyhow::Result<String> {
    let mut url = Url::parse(&oauth.authorize_url)
        .with_context(|| format!("parse oauth authorize url {}", oauth.authorize_url))?;
    let code_challenge = pkce_code_challenge(pkce_verifier);
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("response_type", "code");
        query.append_pair("client_id", oauth.client_id.as_str());
        query.append_pair("redirect_uri", oauth.redirect_uri.as_str());
        query.append_pair("state", state);
        query.append_pair("code_challenge", code_challenge.as_str());
        query.append_pair("code_challenge_method", "S256");
        query.append_pair("id_token_add_organizations", "true");
        query.append_pair("codex_cli_simplified_flow", "true");
        if matches!(
            oauth.client_id.as_str(),
            BUILTIN_OPENAI_CODEX_OAUTH_CLIENT_ID
        ) {
            query.append_pair("originator", "pi");
        }
        if !oauth.scopes.is_empty() {
            query.append_pair("scope", oauth.scopes.join(" ").as_str());
        }
    }
    Ok(url.to_string())
}

fn pkce_code_challenge(verifier: &str) -> String {
    let digest = Sha256::digest(verifier.as_bytes());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest)
}

fn compact_nonce() -> String {
    format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
}

fn default_oauth_label(descriptor: &ProviderDescriptor) -> &'static str {
    match descriptor.provider {
        ProviderKind::OpenAiCodex => "openai-codex",
        ProviderKind::OpenAiApi => "openai-api",
        _ => "oauth-profile",
    }
}

fn parse_oauth_callback_input(input: &str) -> anyhow::Result<OAuthAuthorizationCallback> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        bail!("oauth callback input is empty");
    }

    if let Ok(url) = Url::parse(trimmed) {
        return parse_oauth_callback_url(&url);
    }

    if trimmed.contains("code=") || trimmed.contains("error=") || trimmed.starts_with('?') {
        let synthetic = format!(
            "http://localhost{}",
            if trimmed.starts_with('?') {
                format!("/auth/callback{trimmed}")
            } else if trimmed.starts_with("code=") || trimmed.starts_with("error=") {
                format!("/auth/callback?{trimmed}")
            } else {
                trimmed.to_string()
            }
        );
        let url = Url::parse(&synthetic)
            .with_context(|| format!("parse oauth callback query {}", trimmed))?;
        return parse_oauth_callback_url(&url);
    }

    Ok(OAuthAuthorizationCallback {
        code: trimmed.to_string(),
        state: None,
    })
}

fn parse_oauth_callback_url(url: &Url) -> anyhow::Result<OAuthAuthorizationCallback> {
    let mut code = None;
    let mut state = None;
    let mut error = None;
    let mut error_description = None;

    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "code" => code = Some(value.to_string()),
            "state" => state = Some(value.to_string()),
            "error" => error = Some(value.to_string()),
            "error_description" => error_description = Some(value.to_string()),
            _ => {}
        }
    }

    if let Some(kind) = error {
        let suffix = error_description
            .filter(|value| !value.trim().is_empty())
            .map(|value| format!(": {value}"))
            .unwrap_or_default();
        bail!("oauth callback returned {kind}{suffix}");
    }

    let code = code
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .context("oauth callback did not include an authorization code")?;

    Ok(OAuthAuthorizationCallback { code, state })
}

fn build_oauth_profile_from_token_response(
    pending: &PendingOAuthAuthorization,
    token_response: &OAuthRefreshResponse,
) -> anyhow::Result<AuthProfile> {
    let access_token = token_response
        .access_token
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .context("oauth token exchange did not return a usable access token")?
        .to_string();
    let refresh_token = token_response
        .refresh_token
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);
    let expires_at = match token_response.expires_in {
        Some(seconds) if seconds > 0 => Some(Utc::now() + chrono::Duration::seconds(seconds)),
        Some(seconds) => {
            bail!("oauth token response returned a non-positive expires_in value: {seconds}");
        }
        None => None,
    };

    Ok(AuthProfile {
        id: pending.profile_id,
        provider: pending.provider.clone(),
        mode: AuthMode::OAuth,
        label: pending.label.clone(),
        oauth: Some(OAuthState {
            issuer: pending.issuer.clone(),
            account_label: Some(pending.label.clone()),
            access_token: Some(access_token),
            refresh_token,
            expires_at,
        }),
        api_key: None,
        updated_at: Utc::now(),
    })
}

fn inspect_oauth_runtime_auth(auth: &AuthProfile, oauth: &OAuthState) -> RuntimeAuthReadiness {
    if let Some(expires_at) = oauth.expires_at {
        if expires_at <= Utc::now() {
            if oauth
                .refresh_token
                .as_ref()
                .map(|value| !value.trim().is_empty())
                .unwrap_or(false)
            {
                return RuntimeAuthReadiness::NeedsRefresh;
            }
            return RuntimeAuthReadiness::Blocked(format!(
                "auth profile {} has an expired oauth access token",
                auth.label
            ));
        }
    }
    if let Some(token) = oauth.access_token.as_ref().map(|value| value.trim()) {
        if !token.is_empty() {
            return RuntimeAuthReadiness::Ready;
        }
    }

    if oauth
        .refresh_token
        .as_ref()
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
    {
        return RuntimeAuthReadiness::NeedsRefresh;
    }

    RuntimeAuthReadiness::Blocked(format!(
        "auth profile {} has no usable bearer credential (oauth access token or api key)",
        auth.label
    ))
}

impl OpenAiResponsesProvider {
    async fn send_once(&self, bearer: &str, body: &Value) -> anyhow::Result<Value> {
        self.send_standard_once(bearer, body).await
    }

    async fn send_standard_once(&self, bearer: &str, body: &Value) -> anyhow::Result<Value> {
        let response = timeout(self.request_timeout, async {
            self.client
                .post(&self.endpoint)
                .bearer_auth(bearer)
                .json(body)
                .send()
                .await
        })
        .await
        .map_err(|_| {
            self.provider_error(
                "responses_api",
                ProviderErrorCategory::Timeout,
                None,
                Some(false),
            )
        })
        .with_context(|| format!("OpenAI request timed out after {:?}", self.request_timeout))?
        .map_err(|error| {
            let category = if error.is_timeout() {
                ProviderErrorCategory::Timeout
            } else {
                ProviderErrorCategory::Network
            };
            self.provider_error("responses_api", category, error.status(), Some(false))
                .context(format!("POST {}", self.endpoint))
        })?;
        let status = response.status();
        let text = timeout(self.request_timeout, response.text())
            .await
            .map_err(|_| {
                self.provider_error(
                    "responses_api_response_body",
                    ProviderErrorCategory::Timeout,
                    Some(status),
                    Some(false),
                )
            })
            .with_context(|| {
                format!(
                    "OpenAI response body timed out after {:?}",
                    self.request_timeout
                )
            })?
            .context("read OpenAI response body")?;
        if !status.is_success() {
            return Err(self
                .provider_error(
                    "responses_api",
                    openai_category_from_status(status, &text),
                    Some(status),
                    Some(is_retryable_status(status)),
                )
                .context(format!(
                    "OpenAI responses API status_code={} retryable={} body={}",
                    status.as_u16(),
                    is_retryable_status(status),
                    summarize_error_body(&text)
                )));
        }
        serde_json::from_str(&text).context("deserialize OpenAI response")
    }

}

fn build_responses_request(request: &ChatRequest, provider: &ProviderKind) -> Value {
    let mut instructions = Vec::new();
    if let Some(system_prompt) = request.system_prompt.as_ref().map(|value| value.trim()) {
        if !system_prompt.is_empty() {
            instructions.push(system_prompt.to_string());
        }
    }

    let input = build_input_items(&request.messages, provider, &mut instructions);

    let tools = request
        .tools
        .iter()
        .map(tool_to_openai_function)
        .collect::<Vec<_>>();

    let mut body = json!({
        "model": request.model,
        "input": input,
    });
    if !instructions.is_empty() {
        body["instructions"] = json!(instructions.join("\n\n"));
    }
    if !tools.is_empty() {
        body["tools"] = Value::Array(tools);
    }
    body
}

fn build_input_items(
    messages: &[ChatMessage],
    provider: &ProviderKind,
    instructions: &mut Vec<String>,
) -> Vec<Value> {
    let mut input = Vec::new();

    for message in messages {
        let role = message.role.trim().to_ascii_lowercase();
        if role == "system" {
            let system_text = collapse_blocks_to_text(&message.content);
            if !system_text.is_empty() {
                instructions.push(system_text);
            }
            continue;
        }

        let content = message
            .content
            .iter()
            .map(block_to_input_item)
            .collect::<Vec<_>>();
        if content.is_empty() {
            continue;
        }
        input.push(json!({
            "role": role,
            "content": content,
        }));
    }

    input
}


fn parse_openai_event_stream_response(body: &str) -> anyhow::Result<Value> {
    let normalized = body.replace("\r\n", "\n");
    let mut last_response: Option<Value> = None;

    for chunk in normalized.split("\n\n") {
        let trimmed = chunk.trim();
        if trimmed.is_empty() {
            continue;
        }
        let mut event_name: Option<&str> = None;
        let mut data_lines = Vec::new();
        for line in trimmed.lines() {
            if let Some(rest) = line.strip_prefix("event:") {
                event_name = Some(rest.trim());
            } else if let Some(rest) = line.strip_prefix("data:") {
                data_lines.push(rest.trim_start());
            }
        }
        if data_lines.is_empty() {
            continue;
        }
        let data = data_lines.join("\n");
        if data == "[DONE]" {
            continue;
        }
        let payload: Value = serde_json::from_str(&data).with_context(|| {
            format!(
                "deserialize OpenAI Codex event stream payload for {}",
                event_name.unwrap_or("unknown event")
            )
        })?;
        if let Some(response) = payload.get("response").cloned() {
            if matches!(event_name, Some("response.completed")) {
                return Ok(response);
            }
            last_response = Some(response);
        }
    }

    if let Some(response) = last_response {
        return Ok(response);
    }

    Err(anyhow!(
        "OpenAI Codex event stream contained no response payload"
    ))
}

fn block_to_input_item(block: &ContentBlock) -> Value {
    match block {
        ContentBlock::Text { text } => json!({
            "type": "input_text",
            "text": text,
        }),
        ContentBlock::ImagePath { path } => json!({
            "type": "input_text",
            "text": format!("[local image path] {}", path),
        }),
        ContentBlock::ToolCall {
            id,
            name,
            arguments,
        } => json!({
            "type": "input_text",
            "text": format!(
                "[prior tool call] id={} name={} arguments={}",
                id,
                name,
                arguments
            ),
        }),
        ContentBlock::ToolResult { id, content } => json!({
            "type": "function_call_output",
            "call_id": id,
            "output": function_call_output_value(content),
        }),
    }
}

fn function_call_output_value(content: &Value) -> Value {
    if content.is_string() || content.is_array() {
        content.clone()
    } else {
        Value::String(content.to_string())
    }
}

fn collapse_blocks_to_text(blocks: &[ContentBlock]) -> String {
    blocks
        .iter()
        .map(|block| match block {
            ContentBlock::Text { text } => text.clone(),
            ContentBlock::ImagePath { path } => format!("[local image path] {}", path),
            ContentBlock::ToolCall {
                id,
                name,
                arguments,
            } => format!(
                "[prior tool call] id={} name={} arguments={}",
                id, name, arguments
            ),
            ContentBlock::ToolResult { id, content } => {
                format!("[prior tool result] call_id={} output={}", id, content)
            }
        })
        .filter(|text| !text.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn close_object_schemas(schema: &mut Value) {
    let Value::Object(object) = schema else {
        return;
    };

    if object
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|kind| kind == "object")
        || object.contains_key("properties")
    {
        object
            .entry("additionalProperties")
            .or_insert_with(|| Value::Bool(false));
    }

    if let Some(Value::Object(properties)) = object.get_mut("properties") {
        for property in properties.values_mut() {
            close_object_schemas(property);
        }
    }

    if let Some(items) = object.get_mut("items") {
        close_object_schemas(items);
    }

    for keyword in ["oneOf", "anyOf", "allOf"] {
        if let Some(Value::Array(branches)) = object.get_mut(keyword) {
            for branch in branches {
                close_object_schemas(branch);
            }
        }
    }

    if let Some(Value::Object(definitions)) = object.get_mut("$defs") {
        for definition in definitions.values_mut() {
            close_object_schemas(definition);
        }
    }
}

fn tool_to_openai_function(tool: &ToolDefinition) -> Value {
    let mut input_schema = tool.input_schema.clone();
    close_object_schemas(&mut input_schema);
    json!({
        "type": "function",
        "name": tool.name,
        "description": tool.description,
        "parameters": input_schema,
        "strict": true,
    })
}

fn translate_response(
    provider_id: &str,
    fallback_model: &str,
    value: Value,
) -> anyhow::Result<ChatResponse> {
    let model = value
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or(fallback_model)
        .to_string();

    let mut content = Vec::new();
    if let Some(output) = value.get("output").and_then(Value::as_array) {
        for item in output {
            let Some(kind) = item.get("type").and_then(Value::as_str) else {
                continue;
            };
            match kind {
                "message" => {
                    if let Some(parts) = item.get("content").and_then(Value::as_array) {
                        for part in parts {
                            let Some(part_kind) = part.get("type").and_then(Value::as_str) else {
                                continue;
                            };
                            match part_kind {
                                "output_text" => {
                                    if let Some(text) = part.get("text").and_then(Value::as_str) {
                                        if !text.trim().is_empty() {
                                            content.push(ContentBlock::Text {
                                                text: text.to_string(),
                                            });
                                        }
                                    }
                                }
                                "refusal" => {
                                    if let Some(text) = part.get("refusal").and_then(Value::as_str)
                                    {
                                        if !text.trim().is_empty() {
                                            content.push(ContentBlock::Text {
                                                text: format!("[refusal] {}", text),
                                            });
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
                "function_call" => {
                    let id = item
                        .get("call_id")
                        .or_else(|| item.get("id"))
                        .and_then(Value::as_str)
                        .unwrap_or("call")
                        .to_string();
                    let name = item
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or("unnamed")
                        .to_string();
                    let arguments = item
                        .get("arguments")
                        .and_then(Value::as_str)
                        .map(parse_json_or_wrap)
                        .unwrap_or_else(|| json!({}));
                    content.push(ContentBlock::ToolCall {
                        id,
                        name,
                        arguments,
                    });
                }
                _ => {}
            }
        }
    }

    if content.is_empty() {
        if let Some(text) = value.get("output_text").and_then(Value::as_str) {
            if !text.trim().is_empty() {
                content.push(ContentBlock::Text {
                    text: text.to_string(),
                });
            }
        }
    }

    if content.is_empty() {
        return Err(anyhow!(
            "OpenAI response contained no output_text or function_call items"
        ));
    }

    Ok(ChatResponse {
        provider_id: provider_id.to_string(),
        model,
        content,
    })
}

fn parse_json_or_wrap(raw: &str) -> Value {
    serde_json::from_str(raw).unwrap_or_else(|_| json!({ "raw": raw }))
}

fn summarize_error_body(body: &str) -> String {
    let trimmed = body.trim();
    if trimmed.len() <= 400 {
        return trimmed.to_string();
    }
    format!("{}...", &trimmed[..400])
}

fn openai_category_from_status(status: StatusCode, body: &str) -> ProviderErrorCategory {
    let text = body.to_ascii_lowercase();
    match status {
        StatusCode::TOO_MANY_REQUESTS => ProviderErrorCategory::RateLimit,
        StatusCode::PAYMENT_REQUIRED => ProviderErrorCategory::Billing,
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => ProviderErrorCategory::Authentication,
        status if status.is_server_error() => {
            if text.contains("overloaded") || text.contains("overload") {
                ProviderErrorCategory::Overloaded
            } else {
                ProviderErrorCategory::ServerError
            }
        }
        _ => ProviderErrorCategory::Unknown,
    }
}

fn is_retryable_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

fn is_retryable_openai_error(error: &anyhow::Error) -> bool {
    let text = format!("{error:#}").to_ascii_lowercase();
    text.contains("timed out")
        || text.contains("timeout")
        || text.contains("connection")
        || text.contains("connect")
        || text.contains("network")
        || text.contains("status_code=429")
        || text.contains("status_code=500")
        || text.contains("status_code=502")
        || text.contains("status_code=503")
        || text.contains("status_code=504")
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;

    use chrono::Utc;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use uuid::Uuid;

    use crate::{
        ApiKeyState, AuthMode, OAuthAuthorizationKind, OAuthState, ProviderErrorCategory,
        ProviderErrorContext, ProviderKind,
    };

    use super::*;

    fn sample_request() -> ChatRequest {
        ChatRequest {
            model: "gpt-5.4-mini".into(),
            system_prompt: Some("Stay bounded.".into()),
            messages: vec![crate::ChatMessage {
                role: "user".into(),
                content: vec![ContentBlock::Text {
                    text: "Focus Codex and report.".into(),
                }],
            }],
            tools: vec![ToolDefinition {
                name: "host_action".into(),
                description: "Run one host action.".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "kind": { "type": "string" }
                    },
                    "required": ["kind"]
                }),
            }],
        }
    }

    #[test]
    fn bearer_prefers_oauth_token() {
        let profile = AuthProfile {
            id: Uuid::new_v4(),
            provider: ProviderKind::OpenAiCodex,
            mode: AuthMode::OAuth,
            label: "codex".into(),
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("jessy".into()),
                access_token: Some("oauth-token".into()),
                refresh_token: None,
                expires_at: None,
            }),
            api_key: Some(ApiKeyState {
                env_var: None,
                key_material: Some("api-key".into()),
            }),
            updated_at: Utc::now(),
        };

        assert_eq!(resolve_auth_bearer(&profile).unwrap(), "oauth-token");
    }

    #[test]
    fn builtin_codex_oauth_client_id_is_available() {
        assert_eq!(
            builtin_interactive_oauth_client_id(&ProviderKind::OpenAiCodex),
            Some("app_EMoamEEZ73f0CkXaXp7hrann")
        );
        assert_eq!(
            builtin_interactive_oauth_client_id(&ProviderKind::OpenAiApi),
            None
        );
    }

    #[test]
    fn built_in_codex_authorization_url_matches_openclaw_contract() {
        let mut oauth = sample_oauth_config(DEFAULT_OPENAI_OAUTH_TOKEN_URL);
        oauth.client_id = BUILTIN_OPENAI_CODEX_OAUTH_CLIENT_ID.into();
        let url = build_browser_authorization_url(&oauth, "state-123", "verifier-456").unwrap();
        assert!(url.contains("redirect_uri=http%3A%2F%2Flocalhost%3A1455%2Fauth%2Fcallback"));
        assert!(url.contains("id_token_add_organizations=true"));
        assert!(url.contains("codex_cli_simplified_flow=true"));
        assert!(url.contains("originator=pi"));
    }

    #[test]
    fn build_request_preserves_tools_and_instructions() {
        let body = build_responses_request(&sample_request(), &ProviderKind::OpenAiApi);
        assert_eq!(body["model"], "gpt-5.4-mini");
        assert_eq!(body["instructions"], "Stay bounded.");
        assert_eq!(body["input"][0]["role"], "user");
        assert_eq!(body["tools"][0]["type"], "function");
        assert_eq!(body["tools"][0]["name"], "host_action");
        assert_eq!(
            body["tools"][0]["parameters"]["additionalProperties"],
            false
        );
    }

    #[test]
    fn codex_provider_defaults_to_standard_openai_endpoint() {
        let provider =
            OpenAiResponsesProvider::new(openai_codex_descriptor("gpt-5.4"), None::<String>)
                .unwrap();

        assert_eq!(provider.endpoint, DEFAULT_OPENAI_RESPONSES_URL);
    }


    #[test]
    fn tool_schema_closes_nested_object_properties() {
        let tool = ToolDefinition {
            name: "nested".into(),
            description: "Nested schema".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "outer": {
                        "type": "object",
                        "properties": {
                            "inner": { "type": "string" }
                        },
                        "required": ["inner"]
                    }
                },
                "required": ["outer"]
            }),
        };

        let function = tool_to_openai_function(&tool);

        assert_eq!(function["parameters"]["additionalProperties"], false);
        assert_eq!(
            function["parameters"]["properties"]["outer"]["additionalProperties"],
            false
        );
    }

    #[test]
    fn translate_response_maps_function_calls_and_text() {
        let response = translate_response(
            "openai-api",
            "gpt-5.4-mini",
            json!({
                "model": "gpt-5.4-mini",
                "output": [
                    {
                        "type": "message",
                        "content": [
                            { "type": "output_text", "text": "I can act now." }
                        ]
                    },
                    {
                        "type": "function_call",
                        "call_id": "call_123",
                        "name": "host_action",
                        "arguments": "{\"kind\":\"focus_window\",\"title\":\"Codex\"}"
                    }
                ]
            }),
        )
        .unwrap();

        assert_eq!(response.provider_id, "openai-api");
        assert_eq!(response.content.len(), 2);
        match &response.content[0] {
            ContentBlock::Text { text } => assert_eq!(text, "I can act now."),
            other => panic!("unexpected first block: {other:?}"),
        }
        match &response.content[1] {
            ContentBlock::ToolCall {
                id,
                name,
                arguments,
            } => {
                assert_eq!(id, "call_123");
                assert_eq!(name, "host_action");
                assert_eq!(arguments["kind"], "focus_window");
            }
            other => panic!("unexpected second block: {other:?}"),
        }
    }

    async fn spawn_response_server(
        responses: Vec<(String, Duration)>,
        hits: Arc<AtomicUsize>,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            for (response, delay) in responses {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buffer = [0u8; 4096];
                let _ = socket.read(&mut buffer).await;
                hits.fetch_add(1, Ordering::SeqCst);
                if !delay.is_zero() {
                    sleep(delay).await;
                }
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });
        format!("http://{}", addr)
    }

    fn http_response(status: &str, body: Value) -> String {
        let body = body.to_string();
        format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        )
    }

    fn sample_auth_profile() -> AuthProfile {
        AuthProfile {
            id: Uuid::new_v4(),
            provider: ProviderKind::OpenAiApi,
            mode: AuthMode::ApiKey,
            label: "api".into(),
            oauth: None,
            api_key: Some(ApiKeyState {
                env_var: None,
                key_material: Some("test-key".into()),
            }),
            updated_at: Utc::now(),
        }
    }

    fn sample_oauth_profile(issuer: &str) -> AuthProfile {
        AuthProfile {
            id: Uuid::new_v4(),
            provider: ProviderKind::OpenAiCodex,
            mode: AuthMode::OAuth,
            label: "codex-oauth".into(),
            oauth: Some(OAuthState {
                issuer: issuer.into(),
                account_label: Some("jessy".into()),
                access_token: None,
                refresh_token: Some("refresh-token".into()),
                expires_at: None,
            }),
            api_key: None,
            updated_at: Utc::now(),
        }
    }

    fn sample_ready_oauth_profile() -> AuthProfile {
        AuthProfile {
            id: Uuid::new_v4(),
            provider: ProviderKind::OpenAiCodex,
            mode: AuthMode::OAuth,
            label: "codex-ready".into(),
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("jessy".into()),
                access_token: Some("oauth-token".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(Utc::now() + chrono::Duration::hours(1)),
            }),
            api_key: None,
            updated_at: Utc::now(),
        }
    }

    fn sample_oauth_config(token_url: &str) -> OpenAiOAuthConfig {
        OpenAiOAuthConfig {
            client_id: "client-id".into(),
            authorize_url: DEFAULT_OPENAI_OAUTH_AUTHORIZE_URL.into(),
            token_url: token_url.into(),
            device_authorization_url: token_url.into(),
            redirect_uri: DEFAULT_OPENAI_OAUTH_REDIRECT_URI.into(),
            callback_host: DEFAULT_OPENAI_OAUTH_CALLBACK_HOST.into(),
            callback_path: DEFAULT_OPENAI_OAUTH_CALLBACK_PATH.into(),
            scopes: DEFAULT_OPENAI_OAUTH_SCOPES
                .iter()
                .map(|scope| scope.to_string())
                .collect(),
            paste_prompt: DEFAULT_OPENAI_OAUTH_PASTE_PROMPT.into(),
        }
    }

    #[test]
    fn resolve_auth_bearer_rejects_expired_oauth_token_even_with_refresh_token() {
        let profile = AuthProfile {
            id: Uuid::new_v4(),
            provider: ProviderKind::OpenAiCodex,
            mode: AuthMode::OAuth,
            label: "expired".into(),
            oauth: Some(crate::OAuthState {
                issuer: "openai".into(),
                account_label: Some("expired".into()),
                access_token: Some("expired-token".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(Utc::now() - chrono::Duration::seconds(60)),
            }),
            api_key: None,
            updated_at: Utc::now(),
        };

        let error = resolve_auth_bearer(&profile).unwrap_err();
        assert!(format!("{error:#}").contains("requires refresh materialization"));
    }

    #[test]
    fn resolve_auth_bearer_rejects_refresh_only_profile_until_materialized() {
        let profile = AuthProfile {
            id: Uuid::new_v4(),
            provider: ProviderKind::OpenAiCodex,
            mode: AuthMode::OAuth,
            label: "refresh-only".into(),
            oauth: Some(crate::OAuthState {
                issuer: "openai".into(),
                account_label: Some("refresh-only".into()),
                access_token: None,
                refresh_token: Some("refresh-token".into()),
                expires_at: None,
            }),
            api_key: None,
            updated_at: Utc::now(),
        };

        let error = resolve_auth_bearer(&profile).unwrap_err();
        assert!(format!("{error:#}").contains("requires oauth access-token materialization"));
    }

    #[tokio::test]
    async fn openai_provider_begins_browser_oauth_authorization() {
        let provider = OpenAiResponsesProvider::with_transport_policy(
            openai_codex_descriptor("gpt-5.4"),
            Some("https://api.openai.com/v1/responses"),
            Duration::from_millis(200),
            0,
            Duration::from_millis(10),
        )
        .unwrap()
        .with_oauth_config(sample_oauth_config(DEFAULT_OPENAI_OAUTH_TOKEN_URL));

        let pending = provider
            .begin_oauth_authorization(Some("codex-browser"))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(pending.provider, ProviderKind::OpenAiCodex);
        assert_eq!(pending.label, "codex-browser");
        assert_eq!(pending.client_id, "client-id");
        assert_eq!(pending.token_endpoint, DEFAULT_OPENAI_OAUTH_TOKEN_URL);
        assert_eq!(
            pending.scopes,
            DEFAULT_OPENAI_OAUTH_SCOPES
                .iter()
                .map(|scope| scope.to_string())
                .collect::<Vec<_>>()
        );
        assert!(pending.expires_at.is_some());
        match &pending.kind {
            OAuthAuthorizationKind::BrowserCallback {
                authorization_url,
                redirect_uri,
                callback_host,
                callback_path,
                paste_prompt,
            } => {
                assert!(authorization_url.contains("response_type=code"));
                assert!(authorization_url.contains("client_id=client-id"));
                assert!(authorization_url.contains("code_challenge="));
                assert!(authorization_url.contains("id_token_add_organizations=true"));
                assert!(authorization_url.contains("codex_cli_simplified_flow=true"));
                assert!(authorization_url.contains("scope=openid+profile+email+offline_access"));
                assert_eq!(redirect_uri, DEFAULT_OPENAI_OAUTH_REDIRECT_URI);
                assert_eq!(callback_host, DEFAULT_OPENAI_OAUTH_CALLBACK_HOST);
                assert_eq!(callback_path, DEFAULT_OPENAI_OAUTH_CALLBACK_PATH);
                assert_eq!(paste_prompt, DEFAULT_OPENAI_OAUTH_PASTE_PROMPT);
            }
            other => panic!("unexpected oauth kind: {other:?}"),
        }
        assert!(
            pending
                .state
                .as_ref()
                .is_some_and(|value| !value.is_empty())
        );
        assert!(
            pending
                .pkce_verifier
                .as_ref()
                .is_some_and(|value| value.len() >= 64)
        );
    }

    #[tokio::test]
    async fn openai_provider_begins_device_code_oauth_authorization() {
        let hits = Arc::new(AtomicUsize::new(0));
        let device_host = spawn_response_server(
            vec![(
                http_response(
                    "200 OK",
                    json!({
                        "device_code": "device-code-123",
                        "user_code": "USER-CODE",
                        "verification_uri": "https://auth.openai.com/activate",
                        "verification_uri_complete": "https://auth.openai.com/activate?user_code=USER-CODE",
                        "expires_in": 900,
                        "interval": 3
                    }),
                ),
                Duration::ZERO,
            )],
            hits.clone(),
        )
        .await;
        let provider = OpenAiResponsesProvider::with_transport_policy(
            openai_codex_descriptor("gpt-5.4"),
            Some("https://api.openai.com/v1/responses"),
            Duration::from_millis(200),
            0,
            Duration::from_millis(10),
        )
        .unwrap()
        .with_oauth_config(sample_oauth_config(&device_host));

        let pending = provider
            .begin_oauth_authorization_with_mode(
                Some("codex-device"),
                OAuthInitiationMode::DeviceCode,
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(hits.load(Ordering::SeqCst), 1);
        assert_eq!(pending.provider, ProviderKind::OpenAiCodex);
        assert_eq!(pending.label, "codex-device");
        assert_eq!(pending.client_id, "client-id");
        assert_eq!(pending.token_endpoint, device_host);
        assert!(pending.state.is_none());
        assert!(pending.pkce_verifier.is_none());
        match &pending.kind {
            OAuthAuthorizationKind::DeviceCode {
                verification_uri,
                user_code,
                device_code,
                poll_interval_seconds,
            } => {
                assert_eq!(
                    verification_uri,
                    "https://auth.openai.com/activate?user_code=USER-CODE"
                );
                assert_eq!(user_code, "USER-CODE");
                assert_eq!(device_code, "device-code-123");
                assert_eq!(*poll_interval_seconds, 3_u64);
            }
            other => panic!("unexpected oauth kind: {other:?}"),
        }
        assert!(pending.expires_at.is_some());
    }

    #[tokio::test]
    async fn openai_provider_completes_browser_oauth_authorization_from_redirect_url() {
        let hits = Arc::new(AtomicUsize::new(0));
        let token_host = spawn_response_server(
            vec![(
                http_response(
                    "200 OK",
                    json!({
                        "access_token": "initial-access",
                        "refresh_token": "initial-refresh",
                        "expires_in": 1200
                    }),
                ),
                Duration::ZERO,
            )],
            hits.clone(),
        )
        .await;
        let provider = OpenAiResponsesProvider::with_transport_policy(
            openai_codex_descriptor("gpt-5.4"),
            Some("https://api.openai.com/v1/responses"),
            Duration::from_millis(200),
            0,
            Duration::from_millis(10),
        )
        .unwrap()
        .with_oauth_config(sample_oauth_config(&token_host));
        let pending = provider
            .begin_oauth_authorization(Some("codex-browser"))
            .await
            .unwrap()
            .unwrap();
        let callback_input = format!(
            "{}?code=test-code&state={}",
            DEFAULT_OPENAI_OAUTH_REDIRECT_URI,
            pending.state.clone().unwrap()
        );

        let profile = provider
            .complete_oauth_authorization(&pending, &callback_input)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(hits.load(Ordering::SeqCst), 1);
        assert_eq!(profile.id, pending.profile_id);
        assert_eq!(profile.label, "codex-browser");
        assert_eq!(
            profile
                .oauth
                .as_ref()
                .and_then(|oauth| oauth.access_token.as_deref()),
            Some("initial-access")
        );
        assert_eq!(
            profile
                .oauth
                .as_ref()
                .and_then(|oauth| oauth.refresh_token.as_deref()),
            Some("initial-refresh")
        );
        assert!(
            profile
                .oauth
                .as_ref()
                .and_then(|oauth| oauth.expires_at)
                .is_some()
        );
    }

    #[tokio::test]
    async fn openai_provider_completes_device_code_oauth_authorization_after_pending_poll() {
        let hits = Arc::new(AtomicUsize::new(0));
        let token_host = spawn_response_server(
            vec![
                (
                    http_response(
                        "200 OK",
                        json!({
                            "device_code": "device-code-123",
                            "user_code": "USER-CODE",
                            "verification_uri": "https://auth.openai.com/activate",
                            "expires_in": 900,
                            "interval": 1
                        }),
                    ),
                    Duration::ZERO,
                ),
                (
                    http_response(
                        "400 Bad Request",
                        json!({
                            "error": "authorization_pending"
                        }),
                    ),
                    Duration::ZERO,
                ),
                (
                    http_response(
                        "200 OK",
                        json!({
                            "access_token": "device-access",
                            "refresh_token": "device-refresh",
                            "expires_in": 1200
                        }),
                    ),
                    Duration::ZERO,
                ),
            ],
            hits.clone(),
        )
        .await;
        let provider = OpenAiResponsesProvider::with_transport_policy(
            openai_codex_descriptor("gpt-5.4"),
            Some("https://api.openai.com/v1/responses"),
            Duration::from_millis(200),
            0,
            Duration::from_millis(10),
        )
        .unwrap()
        .with_oauth_config(sample_oauth_config(&token_host));
        let pending = provider
            .begin_oauth_authorization_with_mode(
                Some("codex-device"),
                OAuthInitiationMode::DeviceCode,
            )
            .await
            .unwrap()
            .unwrap();

        let profile = provider
            .complete_oauth_authorization(&pending, "")
            .await
            .unwrap()
            .unwrap();

        assert_eq!(hits.load(Ordering::SeqCst), 3);
        assert_eq!(profile.id, pending.profile_id);
        assert_eq!(profile.label, "codex-device");
        assert_eq!(
            profile
                .oauth
                .as_ref()
                .and_then(|oauth| oauth.access_token.as_deref()),
            Some("device-access")
        );
        assert_eq!(
            profile
                .oauth
                .as_ref()
                .and_then(|oauth| oauth.refresh_token.as_deref()),
            Some("device-refresh")
        );
    }

    #[tokio::test]
    async fn openai_provider_fails_device_code_oauth_on_invalid_device_code() {
        let hits = Arc::new(AtomicUsize::new(0));
        let token_host = spawn_response_server(
            vec![
                (
                    http_response(
                        "200 OK",
                        json!({
                            "device_code": "device-code-123",
                            "user_code": "USER-CODE",
                            "verification_uri": "https://auth.openai.com/activate",
                            "expires_in": 900,
                            "interval": 1
                        }),
                    ),
                    Duration::ZERO,
                ),
                (
                    http_response(
                        "400 Bad Request",
                        json!({
                            "error": "invalid_grant",
                            "error_description": "device code expired"
                        }),
                    ),
                    Duration::ZERO,
                ),
            ],
            hits.clone(),
        )
        .await;
        let provider = OpenAiResponsesProvider::with_transport_policy(
            openai_codex_descriptor("gpt-5.4"),
            Some("https://api.openai.com/v1/responses"),
            Duration::from_millis(200),
            0,
            Duration::from_millis(10),
        )
        .unwrap()
        .with_oauth_config(sample_oauth_config(&token_host));
        let pending = provider
            .begin_oauth_authorization_with_mode(
                Some("codex-device"),
                OAuthInitiationMode::DeviceCode,
            )
            .await
            .unwrap()
            .unwrap();

        let error = provider
            .complete_oauth_authorization(&pending, "")
            .await
            .unwrap_err();

        assert_eq!(hits.load(Ordering::SeqCst), 2);
        assert!(format!("{error:#}").contains("invalid_grant"));
    }

    #[tokio::test]
    async fn openai_provider_retries_retryable_status_once() {
        let hits = Arc::new(AtomicUsize::new(0));
        let endpoint = spawn_response_server(
            vec![
                (
                    http_response(
                        "500 Internal Server Error",
                        json!({ "error": { "message": "temporary outage" } }),
                    ),
                    Duration::ZERO,
                ),
                (
                    http_response(
                        "200 OK",
                        json!({
                            "model": "gpt-5.4-mini",
                            "output": [
                                {
                                    "type": "message",
                                    "content": [
                                        { "type": "output_text", "text": "Recovered after retry." }
                                    ]
                                }
                            ]
                        }),
                    ),
                    Duration::ZERO,
                ),
            ],
            hits.clone(),
        )
        .await;

        let provider = OpenAiResponsesProvider::with_transport_policy(
            openai_api_descriptor("gpt-5.4-mini"),
            Some(endpoint),
            Duration::from_millis(200),
            1,
            Duration::from_millis(10),
        )
        .unwrap();

        let response = provider
            .chat(&sample_auth_profile(), &sample_request())
            .await
            .unwrap();

        assert_eq!(hits.load(Ordering::SeqCst), 2);
        assert!(matches!(
            response.content.first(),
            Some(ContentBlock::Text { text }) if text == "Recovered after retry."
        ));
    }


    #[tokio::test]
    async fn openai_provider_materializes_refresh_token() {
        let hits = Arc::new(AtomicUsize::new(0));
        let token_host = spawn_response_server(
            vec![(
                http_response(
                    "200 OK",
                    json!({
                        "access_token": "next-access",
                        "refresh_token": "next-refresh",
                        "expires_in": 3600
                    }),
                ),
                Duration::ZERO,
            )],
            hits.clone(),
        )
        .await;
        let provider = OpenAiResponsesProvider::with_transport_policy(
            openai_codex_descriptor("gpt-5.4"),
            Some("https://api.openai.com/v1/responses"),
            Duration::from_millis(200),
            0,
            Duration::from_millis(10),
        )
        .unwrap();
        let profile = sample_oauth_profile(&token_host);

        let updated = provider
            .materialize_runtime_auth(&profile)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(hits.load(Ordering::SeqCst), 1);
        assert_eq!(
            updated
                .oauth
                .as_ref()
                .and_then(|oauth| oauth.access_token.as_deref()),
            Some("next-access")
        );
        assert_eq!(
            updated
                .oauth
                .as_ref()
                .and_then(|oauth| oauth.refresh_token.as_deref()),
            Some("next-refresh")
        );
        assert!(
            updated
                .oauth
                .as_ref()
                .and_then(|oauth| oauth.expires_at)
                .is_some()
        );
    }

    #[tokio::test]
    async fn openai_provider_materializes_refresh_token_even_when_access_token_exists() {
        let hits = Arc::new(AtomicUsize::new(0));
        let token_host = spawn_response_server(
            vec![(
                http_response(
                    "200 OK",
                    json!({
                        "access_token": "next-access",
                        "refresh_token": "next-refresh",
                        "expires_in": 1800
                    }),
                ),
                Duration::ZERO,
            )],
            hits.clone(),
        )
        .await;
        let provider = OpenAiResponsesProvider::with_transport_policy(
            openai_codex_descriptor("gpt-5.4"),
            Some("https://api.openai.com/v1/responses"),
            Duration::from_millis(200),
            0,
            Duration::from_millis(10),
        )
        .unwrap();
        let mut profile = sample_oauth_profile(&token_host);
        profile.oauth.as_mut().unwrap().access_token = Some("still-valid".into());
        profile.oauth.as_mut().unwrap().expires_at =
            Some(Utc::now() + chrono::Duration::seconds(60));

        let updated = provider
            .materialize_runtime_auth(&profile)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(hits.load(Ordering::SeqCst), 1);
        assert_eq!(
            updated
                .oauth
                .as_ref()
                .and_then(|oauth| oauth.access_token.as_deref()),
            Some("next-access")
        );
    }

    #[tokio::test]
    async fn openai_provider_materialize_bubbles_refresh_http_error() {
        let hits = Arc::new(AtomicUsize::new(0));
        let token_host = spawn_response_server(
            vec![(
                http_response(
                    "401 Unauthorized",
                    json!({
                        "error": {
                            "message": "invalid_grant"
                        }
                    }),
                ),
                Duration::ZERO,
            )],
            hits.clone(),
        )
        .await;
        let provider = OpenAiResponsesProvider::with_transport_policy(
            openai_codex_descriptor("gpt-5.4"),
            Some("https://api.openai.com/v1/responses"),
            Duration::from_millis(200),
            0,
            Duration::from_millis(10),
        )
        .unwrap();
        let profile = sample_oauth_profile(&token_host);

        let error = provider
            .materialize_runtime_auth(&profile)
            .await
            .unwrap_err();

        assert_eq!(hits.load(Ordering::SeqCst), 1);
        assert!(format!("{error:#}").contains("status_code=401"));
        let context = error
            .chain()
            .find_map(|cause| cause.downcast_ref::<ProviderErrorContext>())
            .expect("provider error context should be preserved");
        assert_eq!(context.provider_id, "openai-codex");
        assert_eq!(context.operation, "oauth_refresh");
        assert_eq!(context.category, ProviderErrorCategory::Authentication);
        assert_eq!(context.status_code, Some(401));
    }

    #[tokio::test]
    async fn openai_provider_times_out_slow_response() {
        let hits = Arc::new(AtomicUsize::new(0));
        let endpoint = spawn_response_server(
            vec![(
                http_response(
                    "200 OK",
                    json!({
                        "model": "gpt-5.4-mini",
                        "output": [
                            {
                                "type": "message",
                                "content": [
                                    { "type": "output_text", "text": "too slow" }
                                ]
                            }
                        ]
                    }),
                ),
                Duration::from_millis(200),
            )],
            hits,
        )
        .await;

        let provider = OpenAiResponsesProvider::with_transport_policy(
            openai_api_descriptor("gpt-5.4-mini"),
            Some(endpoint),
            Duration::from_millis(50),
            0,
            Duration::from_millis(10),
        )
        .unwrap();

        let error = provider
            .chat(&sample_auth_profile(), &sample_request())
            .await
            .unwrap_err();

        let context = error
            .chain()
            .find_map(|cause| cause.downcast_ref::<ProviderErrorContext>())
            .expect("provider error context should be preserved");
        assert_eq!(context.provider_id, "openai-api");
        assert_eq!(context.operation, "responses_api");
        assert_eq!(context.category, ProviderErrorCategory::Timeout);
    }
}
