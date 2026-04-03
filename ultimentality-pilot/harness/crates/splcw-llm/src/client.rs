use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, bail};
use chrono::{DateTime, Duration, Utc};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use uuid::Uuid;

use crate::{
    AuthFailureKind, AuthProfile, AuthProfileStore, AuthRuntimeHealth, AuthRuntimeHealthEntry,
    ChatRequest, ChatResponse, LlmProvider, OAuthInitiationMode, PendingOAuthAuthorization,
    ProviderDescriptor, ProviderErrorCategory, ProviderErrorContext, ProviderKind,
    RuntimeAuthReadiness, inspect_runtime_auth,
};

#[derive(Default, Clone)]
pub struct ProviderRegistry {
    providers: HashMap<ProviderKind, Arc<dyn LlmProvider>>,
}

impl ProviderRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, provider: Arc<dyn LlmProvider>) -> anyhow::Result<()> {
        let descriptor = provider.descriptor();
        if self.providers.contains_key(&descriptor.provider) {
            bail!(
                "provider {:?} is already registered in the registry",
                descriptor.provider
            );
        }
        self.providers.insert(descriptor.provider.clone(), provider);
        Ok(())
    }

    pub fn get(&self, kind: &ProviderKind) -> Option<Arc<dyn LlmProvider>> {
        self.providers.get(kind).cloned()
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedProfile {
    pub profile: AuthProfile,
    pub descriptor: ProviderDescriptor,
}

#[derive(Debug, Clone)]
pub struct ConfiguredProviderCall {
    pub profile: AuthProfile,
    pub descriptor: ProviderDescriptor,
    pub request: ChatRequest,
    pub response: ChatResponse,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthLifecycleBlockedProfile {
    pub profile_id: Uuid,
    pub label: String,
    pub provider_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AuthLifecycleResumeReport {
    pub cleaned_pending_oauth: Vec<Uuid>,
    pub materialized_profiles: Vec<Uuid>,
    pub armed_profiles: Vec<Uuid>,
    pub blocked_profiles: Vec<AuthLifecycleBlockedProfile>,
}

#[derive(Debug, Clone)]
struct AuthCooldownEntry {
    kind: AuthFailureKind,
    failures: u32,
    until: DateTime<Utc>,
    _last_error: String,
}

#[derive(Debug, Default)]
struct AuthRuntimeState {
    cooldowns: HashMap<Uuid, AuthCooldownEntry>,
    last_successful_profile: Option<Uuid>,
}

impl AuthRuntimeState {
    fn from_health(health: AuthRuntimeHealth, now: DateTime<Utc>) -> Self {
        let cooldowns = health
            .cooldowns
            .into_iter()
            .filter(|entry| entry.until > now)
            .map(|entry| {
                (
                    entry.profile_id,
                    AuthCooldownEntry {
                        kind: entry.kind,
                        failures: entry.failures,
                        until: entry.until,
                        _last_error: entry.last_error,
                    },
                )
            })
            .collect();

        Self {
            cooldowns,
            last_successful_profile: health.last_successful_profile,
        }
    }

    fn to_health(&self, now: DateTime<Utc>) -> AuthRuntimeHealth {
        AuthRuntimeHealth {
            last_successful_profile: self.last_successful_profile,
            cooldowns: self
                .cooldowns
                .iter()
                .filter(|(_, entry)| entry.until > now)
                .map(|(profile_id, entry)| AuthRuntimeHealthEntry {
                    profile_id: *profile_id,
                    kind: entry.kind.clone(),
                    failures: entry.failures,
                    until: entry.until,
                    last_error: entry._last_error.clone(),
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AuthControllerConfig {
    pub rate_limit_backoff_seconds: Vec<i64>,
    pub overload_backoff_seconds: i64,
    pub billing_backoff_seconds: i64,
    pub auth_backoff_seconds: i64,
    pub timeout_backoff_seconds: i64,
    pub network_backoff_seconds: i64,
    pub server_error_backoff_seconds: i64,
    pub unknown_backoff_seconds: i64,
    pub proactive_refresh_seconds: i64,
    pub proactive_refresh_retry_seconds: i64,
    pub proactive_refresh_min_delay_millis: u64,
    pub resident_lifecycle_interval_millis: u64,
}

impl Default for AuthControllerConfig {
    fn default() -> Self {
        Self {
            rate_limit_backoff_seconds: vec![30, 60, 300],
            overload_backoff_seconds: 15,
            billing_backoff_seconds: 3600,
            auth_backoff_seconds: 300,
            timeout_backoff_seconds: 10,
            network_backoff_seconds: 10,
            server_error_backoff_seconds: 15,
            unknown_backoff_seconds: 5,
            proactive_refresh_seconds: 300,
            proactive_refresh_retry_seconds: 60,
            proactive_refresh_min_delay_millis: 5_000,
            resident_lifecycle_interval_millis: 60_000,
        }
    }
}

struct ScheduledRefreshTask {
    generation: u64,
    resident_generation: u64,
    target_expires_at: Option<DateTime<Utc>>,
    handle: JoinHandle<()>,
}

#[derive(Default)]
struct ResidentLifecycleLoopState {
    generation: u64,
    handle: Option<JoinHandle<()>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RefreshScheduleDecision {
    Arm(std::time::Duration),
    Cancel,
    Noop,
    ClearGateAndNoop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StaleScheduleOwnershipBehavior {
    Noop,
    ClearGateAndNoop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScheduledRefreshReplacementDecision {
    Replace { next_generation: u64 },
    Noop,
    ClearGateAndNoop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeRefreshPlan {
    NoRefresh,
    RefreshNow,
    RefreshAfter(std::time::Duration),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RuntimeRefreshAction {
    UseCurrentProfile,
    MaterializeNow { required: bool },
    Reschedule(std::time::Duration),
    Blocked(String),
}

pub struct ConfiguredLlmClient<S> {
    store: Arc<S>,
    registry: ProviderRegistry,
    runtime: Arc<Mutex<AuthRuntimeState>>,
    controller: AuthControllerConfig,
    scheduled_refreshes: Arc<Mutex<HashMap<Uuid, ScheduledRefreshTask>>>,
    resident_lifecycle: Arc<Mutex<ResidentLifecycleLoopState>>,
    resident_resume_gate: Arc<tokio::sync::Mutex<()>>,
    refresh_gates: Arc<Mutex<HashMap<Uuid, Arc<tokio::sync::Mutex<()>>>>>,
    background_auth_enabled: Arc<AtomicBool>,
    alive: Arc<AtomicBool>,
}

impl<S> ConfiguredLlmClient<S>
where
    S: AuthProfileStore + 'static,
{
    pub fn new(store: Arc<S>, registry: ProviderRegistry) -> Self {
        Self::with_controller(store, registry, AuthControllerConfig::default())
    }

    pub fn with_controller(
        store: Arc<S>,
        registry: ProviderRegistry,
        controller: AuthControllerConfig,
    ) -> Self {
        Self {
            store,
            registry,
            runtime: Arc::new(Mutex::new(AuthRuntimeState::default())),
            controller,
            scheduled_refreshes: Arc::new(Mutex::new(HashMap::new())),
            resident_lifecycle: Arc::new(Mutex::new(ResidentLifecycleLoopState::default())),
            resident_resume_gate: Arc::new(tokio::sync::Mutex::new(())),
            refresh_gates: Arc::new(Mutex::new(HashMap::new())),
            background_auth_enabled: Arc::new(AtomicBool::new(true)),
            alive: Arc::new(AtomicBool::new(true)),
        }
    }

    pub async fn resolve_default_profile(&self) -> anyhow::Result<ResolvedProfile> {
        let profile = self
            .store
            .load_default_profile()
            .await?
            .context("no default auth profile is configured")?;
        let provider = self.provider_for(&profile.provider)?;
        let descriptor = provider.descriptor();

        Ok(ResolvedProfile {
            profile,
            descriptor,
        })
    }

    pub async fn resolve_active_profile(&self) -> anyhow::Result<ResolvedProfile> {
        self.resolve_candidate_profiles()
            .await?
            .into_iter()
            .next()
            .context("no active auth profile is available")
    }

    pub async fn begin_oauth_authorization(
        &self,
        provider_kind: ProviderKind,
        label: Option<String>,
    ) -> anyhow::Result<PendingOAuthAuthorization> {
        self.begin_oauth_authorization_with_mode(
            provider_kind,
            label,
            OAuthInitiationMode::BrowserCallback,
        )
        .await
    }

    pub async fn begin_oauth_authorization_with_mode(
        &self,
        provider_kind: ProviderKind,
        label: Option<String>,
        mode: OAuthInitiationMode,
    ) -> anyhow::Result<PendingOAuthAuthorization> {
        let provider = self.provider_for(&provider_kind)?;
        let descriptor = provider.descriptor();
        let pending = provider
            .begin_oauth_authorization_with_mode(label.as_deref(), mode)
            .await
            .with_context(|| {
                format!(
                    "provider {} begin oauth authorization failed",
                    descriptor.id
                )
            })?
            .with_context(|| {
                format!(
                    "provider {} does not support browser/device oauth initiation",
                    descriptor.id
                )
            })?;
        self.store.upsert_pending_oauth(&pending).await?;
        Ok(pending)
    }

    pub async fn complete_device_oauth_authorization(
        &self,
        pending_id: Uuid,
        set_default: bool,
    ) -> anyhow::Result<AuthProfile> {
        self.complete_oauth_authorization(pending_id, "", set_default)
            .await
    }

    pub async fn complete_oauth_authorization(
        &self,
        pending_id: Uuid,
        callback_input: &str,
        set_default: bool,
    ) -> anyhow::Result<AuthProfile> {
        let pending = self
            .store
            .list_pending_oauth()
            .await?
            .into_iter()
            .find(|entry| entry.id == pending_id)
            .with_context(|| format!("pending oauth authorization {} not found", pending_id))?;
        let provider = self.provider_for(&pending.provider)?;
        let descriptor = provider.descriptor();
        let profile = provider
            .complete_oauth_authorization(&pending, callback_input)
            .await
            .with_context(|| {
                format!(
                    "provider {} complete oauth authorization failed",
                    descriptor.id
                )
            })?
            .with_context(|| {
                format!(
                    "provider {} does not support oauth authorization completion",
                    descriptor.id
                )
            })?;
        self.store.upsert_profile(&profile).await?;
        if set_default || self.store.load_default_profile().await?.is_none() {
            self.store.set_default_profile(profile.id).await?;
        }
        self.store.delete_pending_oauth(pending.id).await?;
        self.arm_runtime_auth_refresh(&profile).await?;
        Ok(profile)
    }

    pub async fn resume_auth_lifecycle(&self) -> anyhow::Result<AuthLifecycleResumeReport> {
        let resident_generation = current_resident_lifecycle_generation(&self.resident_lifecycle);
        resume_auth_lifecycle_with_gate(
            self.store.clone(),
            self.registry.clone(),
            self.runtime.clone(),
            self.controller.clone(),
            self.scheduled_refreshes.clone(),
            self.resident_lifecycle.clone(),
            resident_generation,
            self.resident_resume_gate.clone(),
            self.refresh_gates.clone(),
            self.background_auth_enabled.clone(),
            self.alive.clone(),
        )
        .await
    }

    pub async fn start_resident_auth_lifecycle(&self) -> anyhow::Result<AuthLifecycleResumeReport> {
        {
            let resident = self
                .resident_lifecycle
                .lock()
                .expect("resident auth lifecycle poisoned");
            if resident
                .handle
                .as_ref()
                .is_some_and(|handle| !handle.is_finished())
            {
                self.background_auth_enabled.store(true, Ordering::SeqCst);
                return Ok(AuthLifecycleResumeReport::default());
            }
        }

        self.background_auth_enabled.store(true, Ordering::SeqCst);
        let interval = self.controller.resident_lifecycle_interval_millis;
        if interval == 0 {
            return self.resume_auth_lifecycle().await;
        }

        let store = self.store.clone();
        let registry = self.registry.clone();
        let runtime = self.runtime.clone();
        let controller = self.controller.clone();
        let scheduled_refreshes = self.scheduled_refreshes.clone();
        let resident_resume_gate = self.resident_resume_gate.clone();
        let refresh_gates = self.refresh_gates.clone();
        let background_auth_enabled = self.background_auth_enabled.clone();
        let alive = self.alive.clone();
        let resident_lifecycle = self.resident_lifecycle.clone();
        let mut resident = self
            .resident_lifecycle
            .lock()
            .expect("resident auth lifecycle poisoned");
        if resident
            .handle
            .as_ref()
            .is_some_and(|handle| !handle.is_finished())
        {
            return Ok(AuthLifecycleResumeReport::default());
        }
        resident.handle = None;
        resident.generation = resident.generation.saturating_add(1);
        let generation = resident.generation;
        let handle = tokio::spawn(async move {
            let interval = std::time::Duration::from_millis(interval);
            loop {
                if !should_continue_resident_lifecycle_loop(
                    &alive,
                    &background_auth_enabled,
                    &resident_lifecycle,
                    generation,
                ) {
                    break;
                }
                sleep(interval).await;
                if !should_continue_resident_lifecycle_loop(
                    &alive,
                    &background_auth_enabled,
                    &resident_lifecycle,
                    generation,
                ) {
                    break;
                }
                let _ = resume_auth_lifecycle_with_gate(
                    store.clone(),
                    registry.clone(),
                    runtime.clone(),
                    controller.clone(),
                    scheduled_refreshes.clone(),
                    resident_lifecycle.clone(),
                    generation,
                    resident_resume_gate.clone(),
                    refresh_gates.clone(),
                    background_auth_enabled.clone(),
                    alive.clone(),
                )
                .await;
            }
        });
        resident.handle = Some(handle);
        drop(resident);

        resume_auth_lifecycle_with_gate(
            self.store.clone(),
            self.registry.clone(),
            self.runtime.clone(),
            self.controller.clone(),
            self.scheduled_refreshes.clone(),
            self.resident_lifecycle.clone(),
            generation,
            self.resident_resume_gate.clone(),
            self.refresh_gates.clone(),
            self.background_auth_enabled.clone(),
            self.alive.clone(),
        )
        .await
    }

    pub fn stop_resident_auth_lifecycle(&self) {
        self.background_auth_enabled.store(false, Ordering::SeqCst);
        let handle = {
            let mut resident = self
                .resident_lifecycle
                .lock()
                .expect("resident auth lifecycle poisoned");
            resident.generation = resident.generation.saturating_add(1);
            resident.handle.take()
        };
        if let Some(handle) = handle {
            handle.abort();
        }
        abort_all_scheduled_refreshes(&self.scheduled_refreshes, &self.refresh_gates);
    }

    pub async fn chat_with_default(
        &self,
        request: &ChatRequest,
    ) -> anyhow::Result<ConfiguredProviderCall> {
        let resolved = self.resolve_default_profile().await?;
        self.chat_with_profile(&resolved.profile, request).await
    }

    pub async fn chat_with_profile(
        &self,
        profile: &AuthProfile,
        request: &ChatRequest,
    ) -> anyhow::Result<ConfiguredProviderCall> {
        let provider = self.provider_for(&profile.provider)?;
        let descriptor = provider.descriptor();
        let response = provider
            .chat(profile, request)
            .await
            .with_context(|| format!("provider {} chat request failed", descriptor.id))?;
        self.arm_runtime_auth_refresh(profile).await?;

        Ok(ConfiguredProviderCall {
            profile: profile.clone(),
            descriptor,
            request: request.clone(),
            response,
        })
    }

    pub async fn chat_with_controller(
        &self,
        request: &ChatRequest,
    ) -> anyhow::Result<ConfiguredProviderCall> {
        self.chat_with_controller_using(|_| request.clone()).await
    }

    pub async fn chat_with_controller_using<F>(
        &self,
        mut build_request: F,
    ) -> anyhow::Result<ConfiguredProviderCall>
    where
        F: FnMut(&ResolvedProfile) -> ChatRequest,
    {
        let candidates = self.resolve_candidate_profiles().await?;
        let mut failures = Vec::new();

        for resolved in candidates {
            let request = build_request(&resolved);
            match self.chat_with_profile(&resolved.profile, &request).await {
                Ok(call) => {
                    self.record_profile_success(resolved.profile.id).await?;
                    return Ok(call);
                }
                Err(error) => {
                    let kind = classify_provider_error(&error);
                    self.record_profile_failure(resolved.profile.id, kind, format!("{error:#}"))
                        .await?;
                    failures.push(format!(
                        "{} [{}]: {}",
                        resolved.profile.label, resolved.descriptor.id, error
                    ));
                }
            }
        }

        bail!(
            "all candidate auth profiles failed:\n{}",
            failures.join("\n")
        )
    }

    fn provider_for(&self, kind: &ProviderKind) -> anyhow::Result<Arc<dyn LlmProvider>> {
        provider_for_registry(&self.registry, kind)
    }

    async fn resolve_candidate_profiles(&self) -> anyhow::Result<Vec<ResolvedProfile>> {
        self.restore_runtime_health().await?;
        let profiles = self.store.list_profiles().await?;
        if profiles.is_empty() {
            bail!("no auth profiles are configured");
        }

        let default_profile_id = self
            .store
            .load_default_profile()
            .await?
            .map(|profile| profile.id);

        let now = Utc::now();
        let runtime = self.runtime.lock().expect("auth runtime poisoned");
        let last_successful = runtime.last_successful_profile;
        let mut ready = Vec::new();
        let mut cooling = Vec::new();
        let mut blocked = Vec::new();

        for profile in profiles {
            let Ok(provider) = self.provider_for(&profile.provider) else {
                continue;
            };
            let descriptor = provider.descriptor();
            let label = profile.label.clone();
            let profile = match self
                .prepare_runtime_profile(provider, profile)
                .await
                .with_context(|| format!("prepare runtime auth for {}", label))?
            {
                Ok(profile) => profile,
                Err(reason) => {
                    blocked.push((label, descriptor.id, reason));
                    continue;
                }
            };
            let resolved = ResolvedProfile {
                profile,
                descriptor,
            };
            match runtime.cooldowns.get(&resolved.profile.id) {
                Some(entry) if entry.until > now => {
                    cooling.push((resolved, entry.until));
                }
                _ => ready.push(resolved),
            }
        }
        drop(runtime);

        if ready.is_empty() && cooling.is_empty() {
            if !blocked.is_empty() {
                let details = blocked
                    .into_iter()
                    .map(|(label, provider_id, reason)| {
                        format!("{} [{}]: {}", label, provider_id, reason)
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                bail!(
                    "no auth profile is ready for runtime materialization:\n{}",
                    details
                );
            }
            bail!("no auth profile matches a registered provider");
        }

        sort_profiles(&mut ready, default_profile_id, last_successful);
        if !ready.is_empty() {
            return Ok(ready);
        }

        cooling.sort_by_key(|(resolved, until)| {
            (
                *until,
                profile_rank(&resolved.profile, default_profile_id, last_successful),
            )
        });
        Ok(cooling.into_iter().map(|(resolved, _)| resolved).collect())
    }

    async fn restore_runtime_health(&self) -> anyhow::Result<()> {
        let Some(health) = self.store.load_runtime_health().await? else {
            return Ok(());
        };
        let mut runtime = self.runtime.lock().expect("auth runtime poisoned");
        *runtime = AuthRuntimeState::from_health(health, Utc::now());
        Ok(())
    }

    async fn persist_runtime_health(&self) -> anyhow::Result<()> {
        let health = {
            let runtime = self.runtime.lock().expect("auth runtime poisoned");
            runtime.to_health(Utc::now())
        };
        self.store.save_runtime_health(&health).await
    }

    async fn record_profile_success(&self, profile_id: Uuid) -> anyhow::Result<()> {
        let mut runtime = self.runtime.lock().expect("auth runtime poisoned");
        runtime.last_successful_profile = Some(profile_id);
        runtime.cooldowns.remove(&profile_id);
        drop(runtime);
        self.persist_runtime_health().await
    }

    async fn record_profile_failure(
        &self,
        profile_id: Uuid,
        kind: AuthFailureKind,
        last_error: String,
    ) -> anyhow::Result<()> {
        let mut runtime = self.runtime.lock().expect("auth runtime poisoned");
        let now = Utc::now();
        let next_failures = runtime
            .cooldowns
            .get(&profile_id)
            .filter(|entry| entry.until > now && entry.kind == kind)
            .map(|entry| entry.failures + 1)
            .unwrap_or(1);
        let until = now + self.cooldown_window(&kind, next_failures);
        runtime.cooldowns.insert(
            profile_id,
            AuthCooldownEntry {
                kind,
                failures: next_failures,
                until,
                _last_error: last_error,
            },
        );
        drop(runtime);
        self.persist_runtime_health().await
    }

    fn cooldown_window(&self, kind: &AuthFailureKind, failures: u32) -> Duration {
        let seconds = match kind {
            AuthFailureKind::RateLimit => {
                stepped_seconds(&self.controller.rate_limit_backoff_seconds, failures)
            }
            AuthFailureKind::Overloaded => self.controller.overload_backoff_seconds,
            AuthFailureKind::Billing => self.controller.billing_backoff_seconds,
            AuthFailureKind::Authentication => self.controller.auth_backoff_seconds,
            AuthFailureKind::Timeout => self.controller.timeout_backoff_seconds,
            AuthFailureKind::Network => self.controller.network_backoff_seconds,
            AuthFailureKind::ServerError => self.controller.server_error_backoff_seconds,
            AuthFailureKind::Unknown => self.controller.unknown_backoff_seconds,
        };
        Duration::seconds(seconds.max(0))
    }

    async fn prepare_runtime_profile(
        &self,
        provider: Arc<dyn LlmProvider>,
        profile: AuthProfile,
    ) -> anyhow::Result<Result<AuthProfile, String>> {
        let original = profile.clone();
        let prepared = prepare_runtime_profile_with(&self.controller, provider, profile).await?;
        if let Ok(updated) = &prepared {
            if updated != &original {
                self.store.upsert_profile(updated).await?;
            }
        }
        Ok(prepared)
    }

    async fn arm_runtime_auth_refresh(&self, profile: &AuthProfile) -> anyhow::Result<()> {
        let resident_generation = current_resident_lifecycle_generation(&self.resident_lifecycle);
        let _ = arm_runtime_auth_refresh_with(
            self.store.clone(),
            self.registry.clone(),
            self.controller.clone(),
            self.scheduled_refreshes.clone(),
            self.resident_lifecycle.clone(),
            resident_generation,
            self.refresh_gates.clone(),
            self.background_auth_enabled.clone(),
            self.alive.clone(),
            profile.clone(),
        )
        .await?;
        Ok(())
    }
}

impl<S> Drop for ConfiguredLlmClient<S> {
    fn drop(&mut self) {
        self.background_auth_enabled.store(false, Ordering::SeqCst);
        self.alive.store(false, Ordering::SeqCst);
        if let Ok(mut resident) = self.resident_lifecycle.lock() {
            resident.generation = resident.generation.saturating_add(1);
            if let Some(handle) = resident.handle.take() {
                handle.abort();
            }
        }
        abort_all_scheduled_refreshes(&self.scheduled_refreshes, &self.refresh_gates);
    }
}

fn provider_for_registry(
    registry: &ProviderRegistry,
    kind: &ProviderKind,
) -> anyhow::Result<Arc<dyn LlmProvider>> {
    registry
        .get(kind)
        .with_context(|| format!("no provider adapter is registered for {:?}", kind))
}

async fn restore_runtime_health_from_store<S>(
    store: &Arc<S>,
    runtime: &Arc<Mutex<AuthRuntimeState>>,
) -> anyhow::Result<()>
where
    S: AuthProfileStore + 'static,
{
    let Some(health) = store.load_runtime_health().await? else {
        return Ok(());
    };
    let mut guard = runtime.lock().expect("auth runtime poisoned");
    *guard = AuthRuntimeState::from_health(health, Utc::now());
    Ok(())
}

async fn materialize_runtime_profile_with(
    provider: Arc<dyn LlmProvider>,
    profile: AuthProfile,
    required: bool,
) -> anyhow::Result<Result<AuthProfile, String>> {
    let descriptor = provider.descriptor();
    match provider.materialize_runtime_auth(&profile).await {
        Ok(Some(updated)) => match inspect_runtime_auth(&updated) {
            RuntimeAuthReadiness::Ready => Ok(Ok(updated)),
            RuntimeAuthReadiness::NeedsRefresh => {
                if required {
                    Ok(Err(format!(
                        "provider {} did not finish runtime oauth materialization for profile {}",
                        descriptor.id, profile.label
                    )))
                } else {
                    Ok(Ok(profile))
                }
            }
            RuntimeAuthReadiness::Blocked(reason) => {
                if required {
                    Ok(Err(reason))
                } else {
                    Ok(Ok(profile))
                }
            }
        },
        Ok(None) => {
            if required {
                Ok(Err(format!(
                    "provider {} does not support runtime oauth materialization for profile {}",
                    descriptor.id, profile.label
                )))
            } else {
                Ok(Ok(profile))
            }
        }
        Err(error) => {
            if required {
                Ok(Err(format!(
                    "runtime oauth materialization failed for profile {} via provider {}: {error:#}",
                    profile.label, descriptor.id
                )))
            } else {
                Ok(Ok(profile))
            }
        }
    }
}

async fn prepare_runtime_profile_with(
    controller: &AuthControllerConfig,
    provider: Arc<dyn LlmProvider>,
    profile: AuthProfile,
) -> anyhow::Result<Result<AuthProfile, String>> {
    if profile.mode == crate::AuthMode::Local || profile.provider == ProviderKind::Local {
        return Ok(Ok(profile));
    }

    match classify_runtime_refresh_action(&profile, Utc::now(), controller) {
        RuntimeRefreshAction::UseCurrentProfile | RuntimeRefreshAction::Reschedule(_) => {
            Ok(Ok(profile))
        }
        RuntimeRefreshAction::Blocked(reason) => Ok(Err(reason)),
        RuntimeRefreshAction::MaterializeNow { required } => {
            materialize_runtime_profile_with(provider, profile, required).await
        }
    }
}

async fn arm_runtime_auth_refresh_with<S>(
    store: Arc<S>,
    registry: ProviderRegistry,
    controller: AuthControllerConfig,
    scheduled_refreshes: Arc<Mutex<HashMap<Uuid, ScheduledRefreshTask>>>,
    resident_lifecycle: Arc<Mutex<ResidentLifecycleLoopState>>,
    resident_generation: u64,
    refresh_gates: Arc<Mutex<HashMap<Uuid, Arc<tokio::sync::Mutex<()>>>>>,
    background_auth_enabled: Arc<AtomicBool>,
    alive: Arc<AtomicBool>,
    profile: AuthProfile,
) -> anyhow::Result<bool>
where
    S: AuthProfileStore + 'static,
{
    let delay = match decide_refresh_schedule(
        &resident_lifecycle,
        &background_auth_enabled,
        &alive,
        resident_generation,
        &profile,
        &controller,
        StaleScheduleOwnershipBehavior::Noop,
    ) {
        RefreshScheduleDecision::Arm(delay) => delay,
        RefreshScheduleDecision::Cancel => {
            cancel_scheduled_refresh(&scheduled_refreshes, &refresh_gates, profile.id);
            return Ok(false);
        }
        RefreshScheduleDecision::Noop => return Ok(false),
        RefreshScheduleDecision::ClearGateAndNoop => {
            clear_refresh_gate(&refresh_gates, profile.id);
            return Ok(false);
        }
    };
    let Some(provider) = registry.get(&profile.provider) else {
        cancel_scheduled_refresh(&scheduled_refreshes, &refresh_gates, profile.id);
        return Ok(false);
    };
    Ok(spawn_or_replace_scheduled_refresh_task(
        store,
        provider,
        scheduled_refreshes,
        resident_lifecycle,
        refresh_gates,
        controller,
        background_auth_enabled,
        alive,
        profile,
        delay,
        None,
        resident_generation,
        1,
    ))
}

async fn reconcile_runtime_auth_refresh_from_lifecycle<S>(
    store: Arc<S>,
    registry: ProviderRegistry,
    controller: AuthControllerConfig,
    scheduled_refreshes: Arc<Mutex<HashMap<Uuid, ScheduledRefreshTask>>>,
    resident_lifecycle: Arc<Mutex<ResidentLifecycleLoopState>>,
    resident_generation: u64,
    refresh_gates: Arc<Mutex<HashMap<Uuid, Arc<tokio::sync::Mutex<()>>>>>,
    background_auth_enabled: Arc<AtomicBool>,
    alive: Arc<AtomicBool>,
    profile: AuthProfile,
) -> anyhow::Result<bool>
where
    S: AuthProfileStore + 'static,
{
    arm_runtime_auth_refresh_with(
        store,
        registry,
        controller,
        scheduled_refreshes,
        resident_lifecycle,
        resident_generation,
        refresh_gates,
        background_auth_enabled,
        alive,
        profile,
    )
    .await
}

async fn resume_auth_lifecycle_inner<S>(
    store: Arc<S>,
    registry: ProviderRegistry,
    runtime: Arc<Mutex<AuthRuntimeState>>,
    controller: AuthControllerConfig,
    scheduled_refreshes: Arc<Mutex<HashMap<Uuid, ScheduledRefreshTask>>>,
    resident_lifecycle: Arc<Mutex<ResidentLifecycleLoopState>>,
    resident_generation: u64,
    refresh_gates: Arc<Mutex<HashMap<Uuid, Arc<tokio::sync::Mutex<()>>>>>,
    background_auth_enabled: Arc<AtomicBool>,
    alive: Arc<AtomicBool>,
) -> anyhow::Result<AuthLifecycleResumeReport>
where
    S: AuthProfileStore + 'static,
{
    restore_runtime_health_from_store(&store, &runtime).await?;
    let mut report = AuthLifecycleResumeReport::default();
    let now = Utc::now();

    for pending in store.list_pending_oauth().await? {
        if !resident_lifecycle_sweep_is_current(&resident_lifecycle, resident_generation, &alive) {
            return Ok(report);
        }
        if pending
            .expires_at
            .is_some_and(|expires_at| expires_at <= now)
        {
            store.delete_pending_oauth(pending.id).await?;
            report.cleaned_pending_oauth.push(pending.id);
        }
    }

    for profile in store.list_profiles().await? {
        if !resident_lifecycle_sweep_is_current(&resident_lifecycle, resident_generation, &alive) {
            return Ok(report);
        }
        let Ok(provider) = provider_for_registry(&registry, &profile.provider) else {
            continue;
        };
        let descriptor = provider.descriptor();
        let original = profile.clone();
        match prepare_runtime_profile_with(&controller, provider, profile)
            .await
            .with_context(|| format!("resume runtime auth for {}", original.label))?
        {
            Ok(prepared) => {
                if !resident_lifecycle_sweep_is_current(
                    &resident_lifecycle,
                    resident_generation,
                    &alive,
                ) {
                    return Ok(report);
                }
                if prepared != original {
                    store.upsert_profile(&prepared).await?;
                    report.materialized_profiles.push(prepared.id);
                }
                if reconcile_runtime_auth_refresh_from_lifecycle(
                    store.clone(),
                    registry.clone(),
                    controller.clone(),
                    scheduled_refreshes.clone(),
                    resident_lifecycle.clone(),
                    resident_generation,
                    refresh_gates.clone(),
                    background_auth_enabled.clone(),
                    alive.clone(),
                    prepared.clone(),
                )
                .await?
                {
                    report.armed_profiles.push(prepared.id);
                }
            }
            Err(reason) => {
                cancel_scheduled_refresh(&scheduled_refreshes, &refresh_gates, original.id);
                report.blocked_profiles.push(AuthLifecycleBlockedProfile {
                    profile_id: original.id,
                    label: original.label,
                    provider_id: descriptor.id,
                    reason,
                });
            }
        }
    }

    Ok(report)
}

async fn resume_auth_lifecycle_with_gate<S>(
    store: Arc<S>,
    registry: ProviderRegistry,
    runtime: Arc<Mutex<AuthRuntimeState>>,
    controller: AuthControllerConfig,
    scheduled_refreshes: Arc<Mutex<HashMap<Uuid, ScheduledRefreshTask>>>,
    resident_lifecycle: Arc<Mutex<ResidentLifecycleLoopState>>,
    resident_generation: u64,
    resident_resume_gate: Arc<tokio::sync::Mutex<()>>,
    refresh_gates: Arc<Mutex<HashMap<Uuid, Arc<tokio::sync::Mutex<()>>>>>,
    background_auth_enabled: Arc<AtomicBool>,
    alive: Arc<AtomicBool>,
) -> anyhow::Result<AuthLifecycleResumeReport>
where
    S: AuthProfileStore + 'static,
{
    let _resume_guard = resident_resume_gate.lock().await;
    if !resident_lifecycle_sweep_is_current(&resident_lifecycle, resident_generation, &alive) {
        return Ok(AuthLifecycleResumeReport::default());
    }
    resume_auth_lifecycle_inner(
        store,
        registry,
        runtime,
        controller,
        scheduled_refreshes,
        resident_lifecycle,
        resident_generation,
        refresh_gates,
        background_auth_enabled,
        alive,
    )
    .await
}

fn compute_refresh_delay(
    profile: &AuthProfile,
    now: DateTime<Utc>,
    controller: &AuthControllerConfig,
) -> Option<std::time::Duration> {
    let oauth = profile.oauth.as_ref()?;
    let expires_at = oauth.expires_at?;
    if oauth
        .refresh_token
        .as_ref()
        .map(|value| value.trim().is_empty())
        .unwrap_or(true)
    {
        return None;
    }
    if oauth
        .access_token
        .as_ref()
        .map(|value| value.trim().is_empty())
        .unwrap_or(true)
    {
        return None;
    }
    if expires_at <= now {
        return Some(std::time::Duration::from_millis(
            controller.proactive_refresh_min_delay_millis,
        ));
    }
    let target = expires_at - Duration::seconds(controller.proactive_refresh_seconds.max(0));
    let millis = if target <= now {
        controller.proactive_refresh_min_delay_millis
    } else {
        let candidate = (target - now).num_milliseconds().max(0) as u64;
        candidate.max(controller.proactive_refresh_min_delay_millis)
    };
    Some(std::time::Duration::from_millis(millis))
}

fn plan_runtime_auth_refresh(
    profile: &AuthProfile,
    now: DateTime<Utc>,
    controller: &AuthControllerConfig,
) -> RuntimeRefreshPlan {
    match compute_refresh_delay(profile, now, controller) {
        Some(delay)
            if delay
                <= std::time::Duration::from_millis(
                    controller.proactive_refresh_min_delay_millis,
                ) =>
        {
            RuntimeRefreshPlan::RefreshNow
        }
        Some(delay) => RuntimeRefreshPlan::RefreshAfter(delay),
        None => RuntimeRefreshPlan::NoRefresh,
    }
}

fn classify_runtime_refresh_action(
    profile: &AuthProfile,
    now: DateTime<Utc>,
    controller: &AuthControllerConfig,
) -> RuntimeRefreshAction {
    match inspect_runtime_auth(profile) {
        RuntimeAuthReadiness::Ready => match plan_runtime_auth_refresh(profile, now, controller) {
            RuntimeRefreshPlan::RefreshNow => {
                RuntimeRefreshAction::MaterializeNow { required: false }
            }
            RuntimeRefreshPlan::RefreshAfter(delay) => RuntimeRefreshAction::Reschedule(delay),
            RuntimeRefreshPlan::NoRefresh => RuntimeRefreshAction::UseCurrentProfile,
        },
        RuntimeAuthReadiness::NeedsRefresh => {
            RuntimeRefreshAction::MaterializeNow { required: true }
        }
        RuntimeAuthReadiness::Blocked(reason) => RuntimeRefreshAction::Blocked(reason),
    }
}

fn decide_refresh_schedule(
    resident_lifecycle: &Arc<Mutex<ResidentLifecycleLoopState>>,
    background_auth_enabled: &Arc<AtomicBool>,
    alive: &Arc<AtomicBool>,
    resident_generation: u64,
    profile: &AuthProfile,
    controller: &AuthControllerConfig,
    stale_ownership_behavior: StaleScheduleOwnershipBehavior,
) -> RefreshScheduleDecision {
    if !background_auth_enabled.load(Ordering::SeqCst) {
        return RefreshScheduleDecision::Cancel;
    }
    if !scheduled_refresh_ownership_is_current(
        resident_lifecycle,
        background_auth_enabled,
        alive,
        resident_generation,
    ) {
        return match stale_ownership_behavior {
            StaleScheduleOwnershipBehavior::Noop => RefreshScheduleDecision::Noop,
            StaleScheduleOwnershipBehavior::ClearGateAndNoop => {
                RefreshScheduleDecision::ClearGateAndNoop
            }
        };
    }
    match compute_refresh_delay(profile, Utc::now(), controller) {
        Some(delay) => RefreshScheduleDecision::Arm(delay),
        None => RefreshScheduleDecision::Cancel,
    }
}

fn refresh_target_expires_at(profile: &AuthProfile) -> Option<DateTime<Utc>> {
    profile.oauth.as_ref().and_then(|oauth| oauth.expires_at)
}

async fn load_profile_by_id<S>(
    store: &Arc<S>,
    profile_id: Uuid,
) -> anyhow::Result<Option<AuthProfile>>
where
    S: AuthProfileStore + 'static,
{
    Ok(store
        .list_profiles()
        .await?
        .into_iter()
        .find(|profile| profile.id == profile_id))
}

fn refresh_gate_for(
    refresh_gates: &Arc<Mutex<HashMap<Uuid, Arc<tokio::sync::Mutex<()>>>>>,
    profile_id: Uuid,
) -> Arc<tokio::sync::Mutex<()>> {
    let mut guard = refresh_gates
        .lock()
        .expect("refresh gate registry poisoned");
    guard
        .entry(profile_id)
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone()
}

fn clear_refresh_gate(
    refresh_gates: &Arc<Mutex<HashMap<Uuid, Arc<tokio::sync::Mutex<()>>>>>,
    profile_id: Uuid,
) {
    refresh_gates
        .lock()
        .expect("refresh gate registry poisoned")
        .remove(&profile_id);
}

fn cancel_scheduled_refresh(
    tasks: &Arc<Mutex<HashMap<Uuid, ScheduledRefreshTask>>>,
    refresh_gates: &Arc<Mutex<HashMap<Uuid, Arc<tokio::sync::Mutex<()>>>>>,
    profile_id: Uuid,
) {
    if let Some(existing) = tasks
        .lock()
        .expect("scheduled refresh registry poisoned")
        .remove(&profile_id)
    {
        existing.handle.abort();
    }
    clear_refresh_gate(refresh_gates, profile_id);
}

fn abort_all_scheduled_refreshes(
    tasks: &Arc<Mutex<HashMap<Uuid, ScheduledRefreshTask>>>,
    refresh_gates: &Arc<Mutex<HashMap<Uuid, Arc<tokio::sync::Mutex<()>>>>>,
) {
    let mut guard = tasks.lock().expect("scheduled refresh registry poisoned");
    for (_, entry) in guard.drain() {
        entry.handle.abort();
    }
    drop(guard);
    refresh_gates
        .lock()
        .expect("refresh gate registry poisoned")
        .clear();
}

fn resident_lifecycle_generation_matches(
    resident_lifecycle: &Arc<Mutex<ResidentLifecycleLoopState>>,
    generation: u64,
) -> bool {
    resident_lifecycle
        .lock()
        .expect("resident auth lifecycle poisoned")
        .generation
        == generation
}

fn resident_lifecycle_sweep_is_current(
    resident_lifecycle: &Arc<Mutex<ResidentLifecycleLoopState>>,
    generation: u64,
    alive: &Arc<AtomicBool>,
) -> bool {
    alive.load(Ordering::SeqCst)
        && resident_lifecycle_generation_matches(resident_lifecycle, generation)
}

fn should_continue_resident_lifecycle_loop(
    alive: &Arc<AtomicBool>,
    background_auth_enabled: &Arc<AtomicBool>,
    resident_lifecycle: &Arc<Mutex<ResidentLifecycleLoopState>>,
    generation: u64,
) -> bool {
    alive.load(Ordering::SeqCst)
        && background_auth_enabled.load(Ordering::SeqCst)
        && resident_lifecycle_generation_matches(resident_lifecycle, generation)
}

fn current_resident_lifecycle_generation(
    resident_lifecycle: &Arc<Mutex<ResidentLifecycleLoopState>>,
) -> u64 {
    resident_lifecycle
        .lock()
        .expect("resident auth lifecycle poisoned")
        .generation
}

fn scheduled_refresh_matches_target(
    tasks: &Arc<Mutex<HashMap<Uuid, ScheduledRefreshTask>>>,
    profile_id: Uuid,
    resident_generation: u64,
    target_expires_at: Option<DateTime<Utc>>,
) -> bool {
    tasks
        .lock()
        .expect("scheduled refresh registry poisoned")
        .get(&profile_id)
        .map(|entry| {
            !entry.handle.is_finished()
                && entry.resident_generation == resident_generation
                && entry.target_expires_at == target_expires_at
        })
        .unwrap_or(false)
}

fn decide_scheduled_refresh_replacement(
    tasks: &Arc<Mutex<HashMap<Uuid, ScheduledRefreshTask>>>,
    profile_id: Uuid,
    resident_generation: u64,
    target_expires_at: Option<DateTime<Utc>>,
    expected_generation: Option<u64>,
) -> ScheduledRefreshReplacementDecision {
    let guard = tasks.lock().expect("scheduled refresh registry poisoned");
    match expected_generation {
        Some(expected) => {
            let Some(current) = guard.get(&profile_id) else {
                return ScheduledRefreshReplacementDecision::ClearGateAndNoop;
            };
            if current.generation == expected {
                return ScheduledRefreshReplacementDecision::Replace {
                    next_generation: expected + 1,
                };
            }
            if !current.handle.is_finished()
                && current.resident_generation == resident_generation
                && current.target_expires_at == target_expires_at
            {
                return ScheduledRefreshReplacementDecision::Noop;
            }
            ScheduledRefreshReplacementDecision::Noop
        }
        None => {
            drop(guard);
            if scheduled_refresh_matches_target(
                tasks,
                profile_id,
                resident_generation,
                target_expires_at,
            ) {
                return ScheduledRefreshReplacementDecision::Noop;
            }
            let guard = tasks.lock().expect("scheduled refresh registry poisoned");
            ScheduledRefreshReplacementDecision::Replace {
                next_generation: guard
                    .get(&profile_id)
                    .map(|entry| entry.generation + 1)
                    .unwrap_or(1),
            }
        }
    }
}

fn is_current_scheduled_refresh(
    tasks: &Arc<Mutex<HashMap<Uuid, ScheduledRefreshTask>>>,
    resident_lifecycle: &Arc<Mutex<ResidentLifecycleLoopState>>,
    profile_id: Uuid,
    generation: u64,
    resident_generation: u64,
) -> bool {
    tasks
        .lock()
        .expect("scheduled refresh registry poisoned")
        .get(&profile_id)
        .map(|entry| {
            entry.generation == generation
                && entry.resident_generation == resident_generation
                && resident_lifecycle_generation_matches(resident_lifecycle, resident_generation)
        })
        .unwrap_or(false)
}

fn clear_scheduled_refresh_if_current(
    tasks: &Arc<Mutex<HashMap<Uuid, ScheduledRefreshTask>>>,
    refresh_gates: &Arc<Mutex<HashMap<Uuid, Arc<tokio::sync::Mutex<()>>>>>,
    profile_id: Uuid,
    generation: u64,
) {
    let mut guard = tasks.lock().expect("scheduled refresh registry poisoned");
    if guard
        .get(&profile_id)
        .map(|entry| entry.generation == generation)
        .unwrap_or(false)
    {
        guard.remove(&profile_id);
        clear_refresh_gate(refresh_gates, profile_id);
    }
}

fn schedule_refresh_task<S>(
    store: Arc<S>,
    provider: Arc<dyn LlmProvider>,
    tasks: Arc<Mutex<HashMap<Uuid, ScheduledRefreshTask>>>,
    resident_lifecycle: Arc<Mutex<ResidentLifecycleLoopState>>,
    refresh_gates: Arc<Mutex<HashMap<Uuid, Arc<tokio::sync::Mutex<()>>>>>,
    controller: AuthControllerConfig,
    background_auth_enabled: Arc<AtomicBool>,
    alive: Arc<AtomicBool>,
    profile: AuthProfile,
    expected_generation: Option<u64>,
    resident_generation: u64,
    retry_budget: usize,
) where
    S: AuthProfileStore + 'static,
{
    let delay = match decide_refresh_schedule(
        &resident_lifecycle,
        &background_auth_enabled,
        &alive,
        resident_generation,
        &profile,
        &controller,
        StaleScheduleOwnershipBehavior::ClearGateAndNoop,
    ) {
        RefreshScheduleDecision::Arm(delay) => delay,
        RefreshScheduleDecision::Cancel => {
            cancel_scheduled_refresh(&tasks, &refresh_gates, profile.id);
            return;
        }
        RefreshScheduleDecision::Noop => return,
        RefreshScheduleDecision::ClearGateAndNoop => {
            clear_refresh_gate(&refresh_gates, profile.id);
            return;
        }
    };
    let _ = spawn_or_replace_scheduled_refresh_task(
        store,
        provider,
        tasks,
        resident_lifecycle,
        refresh_gates,
        controller,
        background_auth_enabled,
        alive,
        profile,
        delay,
        expected_generation,
        resident_generation,
        retry_budget,
    );
}

fn spawn_or_replace_scheduled_refresh_task<S>(
    store: Arc<S>,
    provider: Arc<dyn LlmProvider>,
    tasks: Arc<Mutex<HashMap<Uuid, ScheduledRefreshTask>>>,
    resident_lifecycle: Arc<Mutex<ResidentLifecycleLoopState>>,
    refresh_gates: Arc<Mutex<HashMap<Uuid, Arc<tokio::sync::Mutex<()>>>>>,
    controller: AuthControllerConfig,
    background_auth_enabled: Arc<AtomicBool>,
    alive: Arc<AtomicBool>,
    profile: AuthProfile,
    delay: std::time::Duration,
    expected_generation: Option<u64>,
    resident_generation: u64,
    retry_budget: usize,
) -> bool
where
    S: AuthProfileStore + 'static,
{
    let target_expires_at = refresh_target_expires_at(&profile);
    let generation = match decide_scheduled_refresh_replacement(
        &tasks,
        profile.id,
        resident_generation,
        target_expires_at,
        expected_generation,
    ) {
        ScheduledRefreshReplacementDecision::Replace { next_generation } => {
            let mut guard = tasks.lock().expect("scheduled refresh registry poisoned");
            if let Some(existing) = guard.remove(&profile.id) {
                existing.handle.abort();
            }
            next_generation
        }
        ScheduledRefreshReplacementDecision::Noop => return false,
        ScheduledRefreshReplacementDecision::ClearGateAndNoop => {
            clear_refresh_gate(&refresh_gates, profile.id);
            return false;
        }
    };
    let profile_id = profile.id;
    let tasks_for_spawn = tasks.clone();
    let resident_lifecycle_for_spawn = resident_lifecycle.clone();
    let refresh_gates_for_spawn = refresh_gates.clone();
    let alive_for_spawn = alive.clone();
    let handle = tokio::spawn(async move {
        sleep(delay).await;
        run_scheduled_refresh(
            store,
            provider,
            tasks_for_spawn,
            resident_lifecycle_for_spawn,
            refresh_gates_for_spawn,
            controller,
            background_auth_enabled,
            alive_for_spawn,
            profile_id,
            generation,
            resident_generation,
            retry_budget,
        )
        .await;
    });
    let mut guard = tasks.lock().expect("scheduled refresh registry poisoned");
    guard.insert(
        profile.id,
        ScheduledRefreshTask {
            generation,
            resident_generation,
            target_expires_at,
            handle,
        },
    );
    true
}

async fn run_scheduled_refresh<S>(
    store: Arc<S>,
    provider: Arc<dyn LlmProvider>,
    tasks: Arc<Mutex<HashMap<Uuid, ScheduledRefreshTask>>>,
    resident_lifecycle: Arc<Mutex<ResidentLifecycleLoopState>>,
    refresh_gates: Arc<Mutex<HashMap<Uuid, Arc<tokio::sync::Mutex<()>>>>>,
    controller: AuthControllerConfig,
    background_auth_enabled: Arc<AtomicBool>,
    alive: Arc<AtomicBool>,
    profile_id: Uuid,
    generation: u64,
    resident_generation: u64,
    retry_budget: usize,
) where
    S: AuthProfileStore + 'static,
{
    if !scheduled_refresh_should_continue(
        &tasks,
        &resident_lifecycle,
        &background_auth_enabled,
        &alive,
        profile_id,
        generation,
        resident_generation,
    ) {
        clear_scheduled_refresh_if_current(&tasks, &refresh_gates, profile_id, generation);
        return;
    }
    let gate = refresh_gate_for(&refresh_gates, profile_id);
    let _guard = gate.lock_owned().await;
    let retry_delay =
        std::time::Duration::from_secs(controller.proactive_refresh_retry_seconds.max(0) as u64);
    let mut retries_remaining = retry_budget;

    loop {
        if !scheduled_refresh_should_continue(
            &tasks,
            &resident_lifecycle,
            &background_auth_enabled,
            &alive,
            profile_id,
            generation,
            resident_generation,
        ) {
            clear_scheduled_refresh_if_current(&tasks, &refresh_gates, profile_id, generation);
            return;
        }
        let profile = match load_profile_by_id(&store, profile_id).await {
            Ok(Some(profile)) => profile,
            _ => {
                clear_scheduled_refresh_if_current(&tasks, &refresh_gates, profile_id, generation);
                return;
            }
        };

        match classify_runtime_refresh_action(&profile, Utc::now(), &controller) {
            RuntimeRefreshAction::Reschedule(_) => {
                schedule_refresh_task(
                    store,
                    provider,
                    tasks,
                    resident_lifecycle.clone(),
                    refresh_gates,
                    controller,
                    background_auth_enabled.clone(),
                    alive,
                    profile,
                    Some(generation),
                    resident_generation,
                    retry_budget,
                );
                return;
            }
            RuntimeRefreshAction::UseCurrentProfile | RuntimeRefreshAction::Blocked(_) => {
                clear_scheduled_refresh_if_current(&tasks, &refresh_gates, profile_id, generation);
                return;
            }
            RuntimeRefreshAction::MaterializeNow { .. } => {}
        }

        match provider.materialize_runtime_auth(&profile).await {
            Ok(Some(updated))
                if matches!(inspect_runtime_auth(&updated), RuntimeAuthReadiness::Ready) =>
            {
                if !scheduled_refresh_should_continue(
                    &tasks,
                    &resident_lifecycle,
                    &background_auth_enabled,
                    &alive,
                    profile_id,
                    generation,
                    resident_generation,
                ) {
                    clear_scheduled_refresh_if_current(
                        &tasks,
                        &refresh_gates,
                        profile_id,
                        generation,
                    );
                    return;
                }
                if store.upsert_profile(&updated).await.is_ok() {
                    schedule_refresh_task(
                        store,
                        provider,
                        tasks,
                        resident_lifecycle.clone(),
                        refresh_gates,
                        controller,
                        background_auth_enabled.clone(),
                        alive,
                        updated,
                        Some(generation),
                        resident_generation,
                        1,
                    );
                    return;
                }
                clear_scheduled_refresh_if_current(&tasks, &refresh_gates, profile_id, generation);
                return;
            }
            _ if retries_remaining > 0 => {
                if !scheduled_refresh_should_continue(
                    &tasks,
                    &resident_lifecycle,
                    &background_auth_enabled,
                    &alive,
                    profile_id,
                    generation,
                    resident_generation,
                ) {
                    clear_scheduled_refresh_if_current(
                        &tasks,
                        &refresh_gates,
                        profile_id,
                        generation,
                    );
                    return;
                }
                retries_remaining -= 1;
                sleep(retry_delay).await;
            }
            _ => {
                clear_scheduled_refresh_if_current(&tasks, &refresh_gates, profile_id, generation);
                return;
            }
        }
    }
}

fn scheduled_refresh_should_continue(
    tasks: &Arc<Mutex<HashMap<Uuid, ScheduledRefreshTask>>>,
    resident_lifecycle: &Arc<Mutex<ResidentLifecycleLoopState>>,
    background_auth_enabled: &Arc<AtomicBool>,
    alive: &Arc<AtomicBool>,
    profile_id: Uuid,
    generation: u64,
    resident_generation: u64,
) -> bool {
    scheduled_refresh_ownership_is_current(
        resident_lifecycle,
        background_auth_enabled,
        alive,
        resident_generation,
    ) && is_current_scheduled_refresh(
        tasks,
        resident_lifecycle,
        profile_id,
        generation,
        resident_generation,
    )
}

fn scheduled_refresh_ownership_is_current(
    resident_lifecycle: &Arc<Mutex<ResidentLifecycleLoopState>>,
    background_auth_enabled: &Arc<AtomicBool>,
    alive: &Arc<AtomicBool>,
    resident_generation: u64,
) -> bool {
    background_auth_enabled.load(Ordering::SeqCst)
        && alive.load(Ordering::SeqCst)
        && resident_lifecycle_generation_matches(resident_lifecycle, resident_generation)
}

fn classify_provider_error(error: &anyhow::Error) -> AuthFailureKind {
    if let Some(context) = provider_error_context(error) {
        return match context.category {
            ProviderErrorCategory::RateLimit => AuthFailureKind::RateLimit,
            ProviderErrorCategory::Overloaded => AuthFailureKind::Overloaded,
            ProviderErrorCategory::Billing => AuthFailureKind::Billing,
            ProviderErrorCategory::Authentication => AuthFailureKind::Authentication,
            ProviderErrorCategory::Timeout => AuthFailureKind::Timeout,
            ProviderErrorCategory::Network => AuthFailureKind::Network,
            ProviderErrorCategory::ServerError => AuthFailureKind::ServerError,
            ProviderErrorCategory::Unknown => AuthFailureKind::Unknown,
        };
    }
    let text = format!("{error:#}").to_ascii_lowercase();
    if text.contains("status_code=429")
        || text.contains("429")
        || text.contains("rate limit")
        || text.contains("rate_limit")
        || text.contains("throttl")
    {
        return AuthFailureKind::RateLimit;
    }
    if text.contains("overloaded") || text.contains("overload") || text.contains("529") {
        return AuthFailureKind::Overloaded;
    }
    if text.contains("status_code=402")
        || text.contains("402")
        || text.contains("billing")
        || text.contains("insufficient credit")
        || text.contains("insufficient_credit")
    {
        return AuthFailureKind::Billing;
    }
    if text.contains("status_code=401")
        || text.contains("status_code=403")
        || text.contains("401")
        || text.contains("403")
        || text.contains("unauthorized")
        || text.contains("invalid api key")
        || text.contains("expired token")
        || text.contains("token expired")
        || text.contains("missing scopes")
    {
        return AuthFailureKind::Authentication;
    }
    if text.contains("timeout") || text.contains("timed out") {
        return AuthFailureKind::Timeout;
    }
    if text.contains("network")
        || text.contains("connect")
        || text.contains("connection")
        || text.contains("reset")
        || text.contains("closed")
        || text.contains("unavailable")
    {
        return AuthFailureKind::Network;
    }
    if text.contains("status_code=500")
        || text.contains("status_code=502")
        || text.contains("status_code=503")
        || text.contains("status_code=504")
        || text.contains("500")
        || text.contains("502")
        || text.contains("503")
        || text.contains("504")
        || text.contains("server error")
    {
        return AuthFailureKind::ServerError;
    }
    AuthFailureKind::Unknown
}

fn provider_error_context(error: &anyhow::Error) -> Option<&ProviderErrorContext> {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<ProviderErrorContext>())
}

fn sort_profiles(
    profiles: &mut [ResolvedProfile],
    default_profile_id: Option<Uuid>,
    last_successful: Option<Uuid>,
) {
    profiles.sort_by_key(|resolved| {
        profile_rank(&resolved.profile, default_profile_id, last_successful)
    });
}

fn profile_rank(
    profile: &AuthProfile,
    default_profile_id: Option<Uuid>,
    last_successful: Option<Uuid>,
) -> (u8, u8, std::cmp::Reverse<DateTime<Utc>>, String) {
    (
        if Some(profile.id) == default_profile_id {
            0
        } else {
            1
        },
        if Some(profile.id) == last_successful {
            0
        } else {
            1
        },
        std::cmp::Reverse(profile.updated_at),
        profile.label.clone(),
    )
}

fn stepped_seconds(steps: &[i64], failures: u32) -> i64 {
    if steps.is_empty() {
        return 0;
    }
    let index = failures.saturating_sub(1) as usize;
    steps[index.min(steps.len() - 1)]
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use async_trait::async_trait;
    use chrono::Utc;

    use crate::{
        ApiKeyState, AuthFailureKind, AuthMode, AuthRuntimeHealth, AuthRuntimeHealthEntry,
        OAuthAuthorizationKind, OAuthInitiationMode, OAuthState, PendingOAuthAuthorization,
        ProviderErrorCategory, ProviderErrorContext, ProviderKind,
    };

    use super::*;

    struct MemoryAuthStore {
        profiles: Mutex<Vec<AuthProfile>>,
        default_profile_id: Mutex<Option<Uuid>>,
        runtime_health: Mutex<Option<AuthRuntimeHealth>>,
    }

    struct PendingAuthStore {
        profiles: Mutex<Vec<AuthProfile>>,
        default_profile_id: Mutex<Option<Uuid>>,
        runtime_health: Mutex<Option<AuthRuntimeHealth>>,
        pending_oauth: Mutex<Vec<PendingOAuthAuthorization>>,
    }

    #[async_trait]
    impl AuthProfileStore for MemoryAuthStore {
        async fn list_profiles(&self) -> anyhow::Result<Vec<AuthProfile>> {
            Ok(self.profiles.lock().unwrap().clone())
        }

        async fn upsert_profile(&self, profile: &AuthProfile) -> anyhow::Result<()> {
            let mut profiles = self.profiles.lock().unwrap();
            if let Some(existing) = profiles.iter_mut().find(|entry| entry.id == profile.id) {
                *existing = profile.clone();
            } else {
                profiles.push(profile.clone());
            }
            Ok(())
        }

        async fn load_default_profile(&self) -> anyhow::Result<Option<AuthProfile>> {
            let default_id = *self.default_profile_id.lock().unwrap();
            Ok(default_id.and_then(|id| {
                self.profiles
                    .lock()
                    .unwrap()
                    .iter()
                    .find(|profile| profile.id == id)
                    .cloned()
            }))
        }

        async fn set_default_profile(&self, profile_id: Uuid) -> anyhow::Result<()> {
            *self.default_profile_id.lock().unwrap() = Some(profile_id);
            Ok(())
        }

        async fn load_runtime_health(&self) -> anyhow::Result<Option<AuthRuntimeHealth>> {
            Ok(self.runtime_health.lock().unwrap().clone())
        }

        async fn save_runtime_health(&self, health: &AuthRuntimeHealth) -> anyhow::Result<()> {
            *self.runtime_health.lock().unwrap() = Some(health.clone());
            Ok(())
        }
    }

    #[async_trait]
    impl AuthProfileStore for PendingAuthStore {
        async fn list_profiles(&self) -> anyhow::Result<Vec<AuthProfile>> {
            Ok(self.profiles.lock().unwrap().clone())
        }

        async fn upsert_profile(&self, profile: &AuthProfile) -> anyhow::Result<()> {
            let mut profiles = self.profiles.lock().unwrap();
            if let Some(existing) = profiles.iter_mut().find(|entry| entry.id == profile.id) {
                *existing = profile.clone();
            } else {
                profiles.push(profile.clone());
            }
            Ok(())
        }

        async fn load_default_profile(&self) -> anyhow::Result<Option<AuthProfile>> {
            let default_id = *self.default_profile_id.lock().unwrap();
            Ok(default_id.and_then(|id| {
                self.profiles
                    .lock()
                    .unwrap()
                    .iter()
                    .find(|profile| profile.id == id)
                    .cloned()
            }))
        }

        async fn set_default_profile(&self, profile_id: Uuid) -> anyhow::Result<()> {
            *self.default_profile_id.lock().unwrap() = Some(profile_id);
            Ok(())
        }

        async fn list_pending_oauth(&self) -> anyhow::Result<Vec<PendingOAuthAuthorization>> {
            Ok(self.pending_oauth.lock().unwrap().clone())
        }

        async fn upsert_pending_oauth(
            &self,
            pending: &PendingOAuthAuthorization,
        ) -> anyhow::Result<()> {
            let mut pending_oauth = self.pending_oauth.lock().unwrap();
            if let Some(existing) = pending_oauth
                .iter_mut()
                .find(|entry| entry.id == pending.id)
            {
                *existing = pending.clone();
            } else {
                pending_oauth.push(pending.clone());
            }
            Ok(())
        }

        async fn delete_pending_oauth(&self, pending_id: Uuid) -> anyhow::Result<()> {
            self.pending_oauth
                .lock()
                .unwrap()
                .retain(|entry| entry.id != pending_id);
            Ok(())
        }

        async fn load_runtime_health(&self) -> anyhow::Result<Option<AuthRuntimeHealth>> {
            Ok(self.runtime_health.lock().unwrap().clone())
        }

        async fn save_runtime_health(&self, health: &AuthRuntimeHealth) -> anyhow::Result<()> {
            *self.runtime_health.lock().unwrap() = Some(health.clone());
            Ok(())
        }
    }

    struct RoutedProvider {
        descriptor: ProviderDescriptor,
        outcomes: Mutex<HashMap<String, VecDeque<anyhow::Result<ChatResponse>>>>,
        call_log: Mutex<Vec<String>>,
        materialized: Mutex<HashMap<String, VecDeque<anyhow::Result<AuthProfile>>>>,
        materialize_log: Mutex<Vec<String>>,
    }

    struct SlowMaterializeProvider {
        descriptor: ProviderDescriptor,
        profile: AuthProfile,
        delay_millis: u64,
        materialize_log: Mutex<Vec<String>>,
    }

    struct InteractiveOAuthProvider {
        descriptor: ProviderDescriptor,
        pending: PendingOAuthAuthorization,
        completed: AuthProfile,
    }

    #[async_trait]
    impl LlmProvider for RoutedProvider {
        fn descriptor(&self) -> ProviderDescriptor {
            self.descriptor.clone()
        }

        async fn materialize_runtime_auth(
            &self,
            profile: &AuthProfile,
        ) -> anyhow::Result<Option<AuthProfile>> {
            self.materialize_log
                .lock()
                .unwrap()
                .push(profile.label.clone());
            let mut materialized = self.materialized.lock().unwrap();
            if let Some(queue) = materialized.get_mut(&profile.label) {
                if let Some(result) = queue.pop_front() {
                    return result.map(Some);
                }
            }
            Ok(None)
        }

        async fn chat(
            &self,
            auth: &AuthProfile,
            request: &ChatRequest,
        ) -> anyhow::Result<ChatResponse> {
            self.call_log
                .lock()
                .unwrap()
                .push(format!("{}::{}", auth.label, request.model));
            let mut outcomes = self.outcomes.lock().unwrap();
            if let Some(queue) = outcomes.get_mut(&auth.label) {
                if let Some(result) = queue.pop_front() {
                    return result;
                }
            }

            Ok(ChatResponse {
                provider_id: self.descriptor.id.clone(),
                model: request.model.clone(),
                content: vec![],
            })
        }
    }

    #[async_trait]
    impl LlmProvider for SlowMaterializeProvider {
        fn descriptor(&self) -> ProviderDescriptor {
            self.descriptor.clone()
        }

        async fn materialize_runtime_auth(
            &self,
            profile: &AuthProfile,
        ) -> anyhow::Result<Option<AuthProfile>> {
            self.materialize_log
                .lock()
                .unwrap()
                .push(profile.label.clone());
            sleep(std::time::Duration::from_millis(self.delay_millis)).await;
            Ok(Some(self.profile.clone()))
        }

        async fn chat(
            &self,
            _auth: &AuthProfile,
            request: &ChatRequest,
        ) -> anyhow::Result<ChatResponse> {
            Ok(ChatResponse {
                provider_id: self.descriptor.id.clone(),
                model: request.model.clone(),
                content: vec![],
            })
        }
    }

    #[async_trait]
    impl LlmProvider for InteractiveOAuthProvider {
        fn descriptor(&self) -> ProviderDescriptor {
            self.descriptor.clone()
        }

        async fn begin_oauth_authorization_with_mode(
            &self,
            label: Option<&str>,
            mode: OAuthInitiationMode,
        ) -> anyhow::Result<Option<PendingOAuthAuthorization>> {
            let supports_mode = matches!(
                (&self.pending.kind, mode),
                (
                    OAuthAuthorizationKind::BrowserCallback { .. },
                    OAuthInitiationMode::BrowserCallback
                ) | (
                    OAuthAuthorizationKind::DeviceCode { .. },
                    OAuthInitiationMode::DeviceCode
                )
            );
            if !supports_mode {
                return Ok(None);
            }
            let mut pending = self.pending.clone();
            if let Some(label) = label.map(str::trim).filter(|value| !value.is_empty()) {
                pending.label = label.to_string();
            }
            Ok(Some(pending))
        }

        async fn complete_oauth_authorization(
            &self,
            pending: &PendingOAuthAuthorization,
            callback_input: &str,
        ) -> anyhow::Result<Option<AuthProfile>> {
            match &pending.kind {
                OAuthAuthorizationKind::BrowserCallback { .. } => {
                    if callback_input.trim() != "callback-code" {
                        bail!("unexpected callback input");
                    }
                }
                OAuthAuthorizationKind::DeviceCode { .. } => {
                    if !callback_input.trim().is_empty() {
                        bail!("device-code oauth completion should not require callback input");
                    }
                }
            }
            let mut profile = self.completed.clone();
            profile.id = pending.profile_id;
            profile.label = pending.label.clone();
            Ok(Some(profile))
        }

        async fn chat(
            &self,
            _auth: &AuthProfile,
            request: &ChatRequest,
        ) -> anyhow::Result<ChatResponse> {
            Ok(ChatResponse {
                provider_id: self.descriptor.id.clone(),
                model: request.model.clone(),
                content: vec![],
            })
        }
    }

    fn sample_profile(label: &str, updated_at: DateTime<Utc>) -> AuthProfile {
        AuthProfile {
            id: Uuid::new_v4(),
            provider: ProviderKind::OpenAiCodex,
            mode: AuthMode::OAuth,
            label: label.into(),
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some(label.into()),
                access_token: Some("token".into()),
                refresh_token: Some("refresh".into()),
                expires_at: None,
            }),
            api_key: Some(ApiKeyState {
                env_var: None,
                key_material: None,
            }),
            updated_at,
        }
    }

    #[test]
    fn classify_provider_error_prefers_structured_provider_context() {
        let error = anyhow::Error::new(ProviderErrorContext {
            provider_id: "openai-api".into(),
            operation: "responses_api".into(),
            category: ProviderErrorCategory::Billing,
            status_code: Some(402),
            retryable: Some(false),
        })
        .context("provider openai-api chat request failed");

        assert_eq!(classify_provider_error(&error), AuthFailureKind::Billing);
    }

    #[test]
    fn classify_provider_error_falls_back_to_text_when_no_provider_context_exists() {
        let error = anyhow::anyhow!("provider local chat request failed: status_code=429");

        assert_eq!(classify_provider_error(&error), AuthFailureKind::RateLimit);
    }

    fn sample_request(model: &str) -> ChatRequest {
        ChatRequest {
            model: model.into(),
            system_prompt: Some("system".into()),
            messages: vec![],
            tools: vec![],
        }
    }

    #[tokio::test]
    async fn controller_prefers_default_profile_when_available() {
        let primary = sample_profile("primary", Utc::now());
        let backup = sample_profile("backup", Utc::now() - Duration::seconds(30));
        let store = Arc::new(MemoryAuthStore {
            profiles: Mutex::new(vec![backup.clone(), primary.clone()]),
            default_profile_id: Mutex::new(Some(primary.id)),
            runtime_health: Mutex::new(None),
        });
        let provider = Arc::new(RoutedProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            outcomes: Mutex::new(HashMap::new()),
            call_log: Mutex::new(Vec::new()),
            materialized: Mutex::new(HashMap::new()),
            materialize_log: Mutex::new(Vec::new()),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider.clone()).unwrap();
        let client = ConfiguredLlmClient::new(store, registry);

        let outcome = client
            .chat_with_controller(&sample_request("gpt-5.4"))
            .await
            .unwrap();

        assert_eq!(outcome.profile.id, primary.id);
        assert_eq!(
            provider.call_log.lock().unwrap().as_slice(),
            ["primary::gpt-5.4"]
        );
    }

    #[tokio::test]
    async fn controller_rotates_after_rate_limit_and_skips_cooled_profile() {
        let primary = sample_profile("primary", Utc::now());
        let backup = sample_profile("backup", Utc::now() - Duration::seconds(30));
        let store = Arc::new(MemoryAuthStore {
            profiles: Mutex::new(vec![primary.clone(), backup.clone()]),
            default_profile_id: Mutex::new(Some(primary.id)),
            runtime_health: Mutex::new(None),
        });
        let provider = Arc::new(RoutedProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            outcomes: Mutex::new(HashMap::from([
                (
                    "primary".into(),
                    VecDeque::from([Err(anyhow::anyhow!("HTTP 429 rate limit exceeded"))]),
                ),
                ("backup".into(), VecDeque::new()),
            ])),
            call_log: Mutex::new(Vec::new()),
            materialized: Mutex::new(HashMap::new()),
            materialize_log: Mutex::new(Vec::new()),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider.clone()).unwrap();
        let client = ConfiguredLlmClient::with_controller(
            store,
            registry,
            AuthControllerConfig {
                rate_limit_backoff_seconds: vec![60],
                ..AuthControllerConfig::default()
            },
        );

        let first = client
            .chat_with_controller(&sample_request("gpt-5.4"))
            .await
            .unwrap();
        let second = client
            .chat_with_controller(&sample_request("gpt-5.4"))
            .await
            .unwrap();

        assert_eq!(first.profile.id, backup.id);
        assert_eq!(second.profile.id, backup.id);
        assert_eq!(
            provider.call_log.lock().unwrap().as_slice(),
            ["primary::gpt-5.4", "backup::gpt-5.4", "backup::gpt-5.4"]
        );
    }

    #[tokio::test]
    async fn controller_retries_only_profile_after_cooldown_sort_fallback() {
        let primary = sample_profile("primary", Utc::now());
        let store = Arc::new(MemoryAuthStore {
            profiles: Mutex::new(vec![primary.clone()]),
            default_profile_id: Mutex::new(Some(primary.id)),
            runtime_health: Mutex::new(None),
        });
        let provider = Arc::new(RoutedProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            outcomes: Mutex::new(HashMap::from([(
                "primary".into(),
                VecDeque::from([
                    Err(anyhow::anyhow!("HTTP 429 rate limit exceeded")),
                    Ok(ChatResponse {
                        provider_id: "openai-codex".into(),
                        model: "gpt-5.4".into(),
                        content: vec![],
                    }),
                ]),
            )])),
            call_log: Mutex::new(Vec::new()),
            materialized: Mutex::new(HashMap::new()),
            materialize_log: Mutex::new(Vec::new()),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider.clone()).unwrap();
        let client = ConfiguredLlmClient::with_controller(
            store,
            registry,
            AuthControllerConfig {
                rate_limit_backoff_seconds: vec![120],
                ..AuthControllerConfig::default()
            },
        );

        assert!(
            client
                .chat_with_controller(&sample_request("gpt-5.4"))
                .await
                .is_err()
        );
        let second = client
            .chat_with_controller(&sample_request("gpt-5.4"))
            .await
            .unwrap();

        assert_eq!(second.profile.id, primary.id);
        assert_eq!(
            provider.call_log.lock().unwrap().as_slice(),
            ["primary::gpt-5.4", "primary::gpt-5.4"]
        );
    }

    #[tokio::test]
    async fn controller_uses_persisted_cooldown_after_restart() {
        let primary = sample_profile("primary", Utc::now());
        let backup = sample_profile("backup", Utc::now() - Duration::seconds(30));
        let store = Arc::new(MemoryAuthStore {
            profiles: Mutex::new(vec![primary.clone(), backup.clone()]),
            default_profile_id: Mutex::new(Some(primary.id)),
            runtime_health: Mutex::new(None),
        });
        let provider = Arc::new(RoutedProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            outcomes: Mutex::new(HashMap::from([
                (
                    "primary".into(),
                    VecDeque::from([Err(anyhow::anyhow!("HTTP 429 rate limit exceeded"))]),
                ),
                (
                    "backup".into(),
                    VecDeque::from([Ok(ChatResponse {
                        provider_id: "openai-codex".into(),
                        model: "gpt-5.4".into(),
                        content: vec![],
                    })]),
                ),
            ])),
            call_log: Mutex::new(Vec::new()),
            materialized: Mutex::new(HashMap::new()),
            materialize_log: Mutex::new(Vec::new()),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider).unwrap();
        let first_client = ConfiguredLlmClient::with_controller(
            store.clone(),
            registry,
            AuthControllerConfig {
                rate_limit_backoff_seconds: vec![120],
                ..AuthControllerConfig::default()
            },
        );

        let first = first_client
            .chat_with_controller(&sample_request("gpt-5.4"))
            .await
            .unwrap();
        assert_eq!(first.profile.id, backup.id);
        assert_eq!(
            store
                .load_runtime_health()
                .await
                .unwrap()
                .unwrap()
                .cooldowns
                .len(),
            1
        );

        let provider = Arc::new(RoutedProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            outcomes: Mutex::new(HashMap::from([
                ("primary".into(), VecDeque::new()),
                ("backup".into(), VecDeque::new()),
            ])),
            call_log: Mutex::new(Vec::new()),
            materialized: Mutex::new(HashMap::new()),
            materialize_log: Mutex::new(Vec::new()),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider.clone()).unwrap();
        let second_client = ConfiguredLlmClient::with_controller(
            store,
            registry,
            AuthControllerConfig {
                rate_limit_backoff_seconds: vec![120],
                ..AuthControllerConfig::default()
            },
        );

        let second = second_client
            .chat_with_controller(&sample_request("gpt-5.4"))
            .await
            .unwrap();
        assert_eq!(second.profile.id, backup.id);
        assert_eq!(
            provider.call_log.lock().unwrap().as_slice(),
            ["backup::gpt-5.4"]
        );
    }

    #[tokio::test]
    async fn controller_clears_persisted_cooldown_after_success() {
        let primary = sample_profile("primary", Utc::now());
        let store = Arc::new(MemoryAuthStore {
            profiles: Mutex::new(vec![primary.clone()]),
            default_profile_id: Mutex::new(Some(primary.id)),
            runtime_health: Mutex::new(Some(AuthRuntimeHealth {
                last_successful_profile: None,
                cooldowns: vec![AuthRuntimeHealthEntry {
                    profile_id: primary.id,
                    kind: AuthFailureKind::RateLimit,
                    failures: 1,
                    until: Utc::now() + Duration::seconds(120),
                    last_error: "HTTP 429 rate limit exceeded".into(),
                }],
            })),
        });
        let provider = Arc::new(RoutedProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            outcomes: Mutex::new(HashMap::from([(
                "primary".into(),
                VecDeque::from([Ok(ChatResponse {
                    provider_id: "openai-codex".into(),
                    model: "gpt-5.4".into(),
                    content: vec![],
                })]),
            )])),
            call_log: Mutex::new(Vec::new()),
            materialized: Mutex::new(HashMap::new()),
            materialize_log: Mutex::new(Vec::new()),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider).unwrap();
        let client = ConfiguredLlmClient::new(store.clone(), registry);

        let outcome = client
            .chat_with_controller(&sample_request("gpt-5.4"))
            .await
            .unwrap();
        assert_eq!(outcome.profile.id, primary.id);

        let health = store.load_runtime_health().await.unwrap().unwrap();
        assert!(health.cooldowns.is_empty());
        assert_eq!(health.last_successful_profile, Some(primary.id));
    }

    #[tokio::test]
    async fn controller_materializes_expired_oauth_profile_before_chat() {
        let expired = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("expired".into()),
                access_token: Some("expired-token".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(Utc::now() - Duration::seconds(60)),
            }),
            label: "expired".into(),
            ..sample_profile("expired", Utc::now())
        };
        let refreshed = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("expired".into()),
                access_token: Some("renewed-token".into()),
                refresh_token: Some("next-refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::minutes(15)),
            }),
            updated_at: Utc::now() + Duration::seconds(1),
            ..expired.clone()
        };
        let backup = sample_profile("backup", Utc::now() - Duration::seconds(30));
        let store = Arc::new(MemoryAuthStore {
            profiles: Mutex::new(vec![expired.clone(), backup.clone()]),
            default_profile_id: Mutex::new(Some(expired.id)),
            runtime_health: Mutex::new(None),
        });
        let provider = Arc::new(RoutedProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            outcomes: Mutex::new(HashMap::new()),
            call_log: Mutex::new(Vec::new()),
            materialized: Mutex::new(HashMap::from([(
                "expired".into(),
                VecDeque::from([Ok(refreshed.clone())]),
            )])),
            materialize_log: Mutex::new(Vec::new()),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider.clone()).unwrap();
        let client = ConfiguredLlmClient::new(store.clone(), registry);

        let outcome = client
            .chat_with_controller(&sample_request("gpt-5.4"))
            .await
            .unwrap();

        assert_eq!(outcome.profile.id, expired.id);
        assert_eq!(
            outcome
                .profile
                .oauth
                .as_ref()
                .and_then(|oauth| oauth.access_token.as_deref()),
            Some("renewed-token")
        );
        assert_eq!(
            provider.call_log.lock().unwrap().as_slice(),
            ["expired::gpt-5.4"]
        );
        assert_eq!(
            provider.materialize_log.lock().unwrap().as_slice(),
            ["expired"]
        );
        let stored = store
            .list_profiles()
            .await
            .unwrap()
            .into_iter()
            .find(|profile| profile.id == expired.id)
            .unwrap();
        assert_eq!(
            stored
                .oauth
                .as_ref()
                .and_then(|oauth| oauth.access_token.as_deref()),
            Some("renewed-token")
        );
    }

    #[tokio::test]
    async fn controller_proactively_refreshes_near_expiry_profile_before_chat() {
        let near_expiry = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("soon".into()),
                access_token: Some("still-valid".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::seconds(60)),
            }),
            label: "soon".into(),
            ..sample_profile("soon", Utc::now())
        };
        let refreshed = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("soon".into()),
                access_token: Some("renewed-early".into()),
                refresh_token: Some("next-refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::minutes(15)),
            }),
            updated_at: Utc::now() + Duration::seconds(1),
            ..near_expiry.clone()
        };
        let store = Arc::new(MemoryAuthStore {
            profiles: Mutex::new(vec![near_expiry.clone()]),
            default_profile_id: Mutex::new(Some(near_expiry.id)),
            runtime_health: Mutex::new(None),
        });
        let provider = Arc::new(RoutedProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            outcomes: Mutex::new(HashMap::new()),
            call_log: Mutex::new(Vec::new()),
            materialized: Mutex::new(HashMap::from([(
                "soon".into(),
                VecDeque::from([Ok(refreshed.clone())]),
            )])),
            materialize_log: Mutex::new(Vec::new()),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider.clone()).unwrap();
        let client = ConfiguredLlmClient::new(store.clone(), registry);

        let outcome = client
            .chat_with_controller(&sample_request("gpt-5.4"))
            .await
            .unwrap();

        assert_eq!(outcome.profile.id, near_expiry.id);
        assert_eq!(
            outcome
                .profile
                .oauth
                .as_ref()
                .and_then(|oauth| oauth.access_token.as_deref()),
            Some("renewed-early")
        );
        assert_eq!(
            provider.materialize_log.lock().unwrap().as_slice(),
            ["soon"]
        );
        let stored = store
            .list_profiles()
            .await
            .unwrap()
            .into_iter()
            .find(|profile| profile.id == near_expiry.id)
            .unwrap();
        assert_eq!(
            stored
                .oauth
                .as_ref()
                .and_then(|oauth| oauth.access_token.as_deref()),
            Some("renewed-early")
        );
    }

    #[tokio::test]
    async fn controller_keeps_valid_profile_when_proactive_refresh_fails() {
        let near_expiry = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("soon".into()),
                access_token: Some("still-valid".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::seconds(60)),
            }),
            label: "soon".into(),
            ..sample_profile("soon", Utc::now())
        };
        let store = Arc::new(MemoryAuthStore {
            profiles: Mutex::new(vec![near_expiry.clone()]),
            default_profile_id: Mutex::new(Some(near_expiry.id)),
            runtime_health: Mutex::new(None),
        });
        let provider = Arc::new(RoutedProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            outcomes: Mutex::new(HashMap::new()),
            call_log: Mutex::new(Vec::new()),
            materialized: Mutex::new(HashMap::from([(
                "soon".into(),
                VecDeque::from([Err(anyhow::anyhow!("temporary refresh outage"))]),
            )])),
            materialize_log: Mutex::new(Vec::new()),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider.clone()).unwrap();
        let client = ConfiguredLlmClient::new(store.clone(), registry);

        let outcome = client
            .chat_with_controller(&sample_request("gpt-5.4"))
            .await
            .unwrap();

        assert_eq!(outcome.profile.id, near_expiry.id);
        assert_eq!(
            outcome
                .profile
                .oauth
                .as_ref()
                .and_then(|oauth| oauth.access_token.as_deref()),
            Some("still-valid")
        );
        assert_eq!(
            provider.call_log.lock().unwrap().as_slice(),
            ["soon::gpt-5.4"]
        );
        assert_eq!(
            provider.materialize_log.lock().unwrap().as_slice(),
            ["soon"]
        );
        let stored = store
            .list_profiles()
            .await
            .unwrap()
            .into_iter()
            .find(|profile| profile.id == near_expiry.id)
            .unwrap();
        assert_eq!(
            stored
                .oauth
                .as_ref()
                .and_then(|oauth| oauth.access_token.as_deref()),
            Some("still-valid")
        );
    }

    #[tokio::test]
    async fn controller_arms_background_refresh_after_successful_call() {
        let near_expiry = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("timer".into()),
                access_token: Some("still-valid".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::milliseconds(30)),
            }),
            label: "timer".into(),
            ..sample_profile("timer", Utc::now())
        };
        let refreshed = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("timer".into()),
                access_token: Some("background-renewed".into()),
                refresh_token: Some("next-refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::minutes(15)),
            }),
            updated_at: Utc::now() + Duration::seconds(1),
            ..near_expiry.clone()
        };
        let store = Arc::new(MemoryAuthStore {
            profiles: Mutex::new(vec![near_expiry.clone()]),
            default_profile_id: Mutex::new(Some(near_expiry.id)),
            runtime_health: Mutex::new(None),
        });
        let provider = Arc::new(RoutedProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            outcomes: Mutex::new(HashMap::new()),
            call_log: Mutex::new(Vec::new()),
            materialized: Mutex::new(HashMap::from([(
                "timer".into(),
                VecDeque::from([Ok(refreshed.clone())]),
            )])),
            materialize_log: Mutex::new(Vec::new()),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider.clone()).unwrap();
        let client = ConfiguredLlmClient::with_controller(
            store.clone(),
            registry,
            AuthControllerConfig {
                proactive_refresh_seconds: 0,
                proactive_refresh_min_delay_millis: 10,
                proactive_refresh_retry_seconds: 0,
                ..AuthControllerConfig::default()
            },
        );

        client
            .chat_with_controller(&sample_request("gpt-5.4"))
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(80)).await;

        let stored = store
            .list_profiles()
            .await
            .unwrap()
            .into_iter()
            .find(|profile| profile.id == near_expiry.id)
            .unwrap();
        assert_eq!(
            stored
                .oauth
                .as_ref()
                .and_then(|oauth| oauth.access_token.as_deref()),
            Some("background-renewed")
        );
        assert_eq!(
            provider.materialize_log.lock().unwrap().as_slice(),
            ["timer"]
        );
    }

    #[tokio::test]
    async fn controller_background_refresh_retries_once_after_failure() {
        let near_expiry = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("timer".into()),
                access_token: Some("still-valid".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::milliseconds(30)),
            }),
            label: "timer".into(),
            ..sample_profile("timer", Utc::now())
        };
        let refreshed = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("timer".into()),
                access_token: Some("background-renewed".into()),
                refresh_token: Some("next-refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::minutes(15)),
            }),
            updated_at: Utc::now() + Duration::seconds(1),
            ..near_expiry.clone()
        };
        let store = Arc::new(MemoryAuthStore {
            profiles: Mutex::new(vec![near_expiry.clone()]),
            default_profile_id: Mutex::new(Some(near_expiry.id)),
            runtime_health: Mutex::new(None),
        });
        let provider = Arc::new(RoutedProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            outcomes: Mutex::new(HashMap::new()),
            call_log: Mutex::new(Vec::new()),
            materialized: Mutex::new(HashMap::from([(
                "timer".into(),
                VecDeque::from([
                    Err(anyhow::anyhow!("temporary refresh outage")),
                    Ok(refreshed.clone()),
                ]),
            )])),
            materialize_log: Mutex::new(Vec::new()),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider.clone()).unwrap();
        let client = ConfiguredLlmClient::with_controller(
            store.clone(),
            registry,
            AuthControllerConfig {
                proactive_refresh_seconds: 0,
                proactive_refresh_min_delay_millis: 10,
                proactive_refresh_retry_seconds: 0,
                ..AuthControllerConfig::default()
            },
        );

        client
            .chat_with_controller(&sample_request("gpt-5.4"))
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(120)).await;

        let stored = store
            .list_profiles()
            .await
            .unwrap()
            .into_iter()
            .find(|profile| profile.id == near_expiry.id)
            .unwrap();
        assert_eq!(
            stored
                .oauth
                .as_ref()
                .and_then(|oauth| oauth.access_token.as_deref()),
            Some("background-renewed")
        );
        assert_eq!(
            provider.materialize_log.lock().unwrap().as_slice(),
            ["timer", "timer"]
        );
    }

    #[tokio::test]
    async fn background_refresh_ignores_stale_generation() {
        let profile = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("timer".into()),
                access_token: Some("still-valid".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::minutes(5)),
            }),
            label: "timer".into(),
            ..sample_profile("timer", Utc::now())
        };
        let store = Arc::new(MemoryAuthStore {
            profiles: Mutex::new(vec![profile.clone()]),
            default_profile_id: Mutex::new(Some(profile.id)),
            runtime_health: Mutex::new(None),
        });
        let provider = Arc::new(RoutedProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            outcomes: Mutex::new(HashMap::new()),
            call_log: Mutex::new(Vec::new()),
            materialized: Mutex::new(HashMap::new()),
            materialize_log: Mutex::new(Vec::new()),
        });
        let refresh_gates = Arc::new(Mutex::new(HashMap::new()));
        let resident_lifecycle = Arc::new(Mutex::new(ResidentLifecycleLoopState {
            generation: 2,
            handle: None,
        }));
        let stale_handle = tokio::spawn(async {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        });
        let tasks = Arc::new(Mutex::new(HashMap::from([(
            profile.id,
            ScheduledRefreshTask {
                generation: 2,
                resident_generation: 2,
                target_expires_at: refresh_target_expires_at(&profile),
                handle: stale_handle,
            },
        )])));

        run_scheduled_refresh(
            store,
            provider,
            tasks.clone(),
            resident_lifecycle,
            refresh_gates,
            AuthControllerConfig::default(),
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicBool::new(true)),
            profile.id,
            1,
            2,
            1,
        )
        .await;

        let guard = tasks.lock().unwrap();
        assert_eq!(
            guard.get(&profile.id).map(|entry| entry.generation),
            Some(2)
        );
    }

    #[tokio::test]
    async fn background_refresh_ignores_stale_resident_generation() {
        let profile = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("timer".into()),
                access_token: Some("still-valid".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::minutes(5)),
            }),
            label: "timer".into(),
            ..sample_profile("timer", Utc::now())
        };
        let store = Arc::new(MemoryAuthStore {
            profiles: Mutex::new(vec![profile.clone()]),
            default_profile_id: Mutex::new(Some(profile.id)),
            runtime_health: Mutex::new(None),
        });
        let provider = Arc::new(RoutedProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            outcomes: Mutex::new(HashMap::new()),
            call_log: Mutex::new(Vec::new()),
            materialized: Mutex::new(HashMap::new()),
            materialize_log: Mutex::new(Vec::new()),
        });
        let refresh_gates = Arc::new(Mutex::new(HashMap::new()));
        let resident_lifecycle = Arc::new(Mutex::new(ResidentLifecycleLoopState {
            generation: 2,
            handle: None,
        }));
        let stale_handle = tokio::spawn(async {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        });
        let tasks = Arc::new(Mutex::new(HashMap::from([(
            profile.id,
            ScheduledRefreshTask {
                generation: 2,
                resident_generation: 1,
                target_expires_at: refresh_target_expires_at(&profile),
                handle: stale_handle,
            },
        )])));

        run_scheduled_refresh(
            store,
            provider,
            tasks.clone(),
            resident_lifecycle,
            refresh_gates,
            AuthControllerConfig::default(),
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicBool::new(true)),
            profile.id,
            2,
            1,
            1,
        )
        .await;

        assert!(tasks.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn background_refresh_does_not_persist_after_background_auth_is_disabled() {
        let profile = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("timer".into()),
                access_token: Some("expired-token".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(Utc::now() - Duration::minutes(1)),
            }),
            label: "timer".into(),
            ..sample_profile("timer", Utc::now())
        };
        let refreshed = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("timer".into()),
                access_token: Some("background-renewed".into()),
                refresh_token: Some("refresh-token-2".into()),
                expires_at: Some(Utc::now() + Duration::minutes(20)),
            }),
            updated_at: Utc::now() + Duration::seconds(1),
            ..profile.clone()
        };
        let store = Arc::new(MemoryAuthStore {
            profiles: Mutex::new(vec![profile.clone()]),
            default_profile_id: Mutex::new(Some(profile.id)),
            runtime_health: Mutex::new(None),
        });
        let provider = Arc::new(SlowMaterializeProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            profile: refreshed,
            delay_millis: 40,
            materialize_log: Mutex::new(Vec::new()),
        });
        let refresh_gates = Arc::new(Mutex::new(HashMap::new()));
        let resident_lifecycle = Arc::new(Mutex::new(ResidentLifecycleLoopState {
            generation: 1,
            handle: None,
        }));
        let scheduled_handle = tokio::spawn(async {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        });
        let tasks = Arc::new(Mutex::new(HashMap::from([(
            profile.id,
            ScheduledRefreshTask {
                generation: 1,
                resident_generation: 1,
                target_expires_at: refresh_target_expires_at(&profile),
                handle: scheduled_handle,
            },
        )])));
        let background_auth_enabled = Arc::new(AtomicBool::new(true));
        let alive = Arc::new(AtomicBool::new(true));

        let refresh = tokio::spawn(run_scheduled_refresh(
            store.clone(),
            provider.clone(),
            tasks.clone(),
            resident_lifecycle,
            refresh_gates,
            AuthControllerConfig::default(),
            background_auth_enabled.clone(),
            alive,
            profile.id,
            1,
            1,
            1,
        ));
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        background_auth_enabled.store(false, Ordering::SeqCst);
        refresh.await.unwrap();

        let stored = store
            .list_profiles()
            .await
            .unwrap()
            .into_iter()
            .find(|candidate| candidate.id == profile.id)
            .unwrap();
        assert_eq!(
            stored
                .oauth
                .as_ref()
                .and_then(|oauth| oauth.access_token.as_deref()),
            Some("expired-token")
        );
        assert!(tasks.lock().unwrap().is_empty());
        assert_eq!(
            provider.materialize_log.lock().unwrap().as_slice(),
            ["timer"]
        );
    }

    #[tokio::test]
    async fn controller_materializes_refresh_only_profile_before_chat() {
        let blocked = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("blocked".into()),
                access_token: None,
                refresh_token: Some("refresh-token".into()),
                expires_at: None,
            }),
            label: "blocked".into(),
            ..sample_profile("blocked", Utc::now())
        };
        let materialized = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("blocked".into()),
                access_token: Some("fresh-token".into()),
                refresh_token: Some("fresh-refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::minutes(20)),
            }),
            updated_at: Utc::now() + Duration::seconds(1),
            ..blocked.clone()
        };
        let store = Arc::new(MemoryAuthStore {
            profiles: Mutex::new(vec![blocked.clone()]),
            default_profile_id: Mutex::new(None),
            runtime_health: Mutex::new(None),
        });
        let provider = Arc::new(RoutedProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            outcomes: Mutex::new(HashMap::new()),
            call_log: Mutex::new(Vec::new()),
            materialized: Mutex::new(HashMap::from([(
                "blocked".into(),
                VecDeque::from([Ok(materialized)]),
            )])),
            materialize_log: Mutex::new(Vec::new()),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider).unwrap();
        let client = ConfiguredLlmClient::new(store.clone(), registry);

        let outcome = client
            .chat_with_controller(&sample_request("gpt-5.4"))
            .await
            .unwrap();

        assert!(
            outcome
                .profile
                .oauth
                .as_ref()
                .and_then(|oauth| oauth.access_token.as_deref())
                == Some("fresh-token")
        );
        let stored = store
            .list_profiles()
            .await
            .unwrap()
            .into_iter()
            .find(|profile| profile.id == blocked.id)
            .unwrap();
        assert_eq!(
            stored
                .oauth
                .as_ref()
                .and_then(|oauth| oauth.access_token.as_deref()),
            Some("fresh-token")
        );
    }

    #[tokio::test]
    async fn controller_errors_when_provider_cannot_materialize_profile() {
        let blocked = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("blocked".into()),
                access_token: None,
                refresh_token: Some("refresh-token".into()),
                expires_at: None,
            }),
            label: "blocked".into(),
            ..sample_profile("blocked", Utc::now())
        };
        let store = Arc::new(MemoryAuthStore {
            profiles: Mutex::new(vec![blocked]),
            default_profile_id: Mutex::new(None),
            runtime_health: Mutex::new(None),
        });
        let provider = Arc::new(RoutedProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            outcomes: Mutex::new(HashMap::new()),
            call_log: Mutex::new(Vec::new()),
            materialized: Mutex::new(HashMap::new()),
            materialize_log: Mutex::new(Vec::new()),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider).unwrap();
        let client = ConfiguredLlmClient::new(store, registry);

        let error = client
            .chat_with_controller(&sample_request("gpt-5.4"))
            .await
            .unwrap_err();

        assert!(
            format!("{error:#}").contains("no auth profile is ready for runtime materialization")
        );
        assert!(
            format!("{error:#}")
                .contains("does not support runtime oauth materialization for profile blocked")
        );
    }

    #[tokio::test]
    async fn client_begin_oauth_authorization_persists_pending_record() {
        let pending = PendingOAuthAuthorization {
            id: Uuid::new_v4(),
            profile_id: Uuid::new_v4(),
            provider: ProviderKind::OpenAiCodex,
            label: "codex-browser".into(),
            issuer: "openai".into(),
            started_at: Utc::now(),
            expires_at: None,
            client_id: "client-id".into(),
            token_endpoint: "https://auth.openai.com/oauth/token".into(),
            scopes: vec!["openid".into(), "offline_access".into()],
            state: Some("state-123".into()),
            pkce_verifier: Some("verifier-123".into()),
            kind: OAuthAuthorizationKind::BrowserCallback {
                authorization_url: "https://auth.openai.com/oauth/authorize?state=state-123".into(),
                redirect_uri: "http://127.0.0.1:1455/auth/callback".into(),
                callback_host: "127.0.0.1".into(),
                callback_path: "/auth/callback".into(),
                paste_prompt: "Paste the redirect URL (or authorization code)".into(),
            },
        };
        let store = Arc::new(PendingAuthStore {
            profiles: Mutex::new(Vec::new()),
            default_profile_id: Mutex::new(None),
            runtime_health: Mutex::new(None),
            pending_oauth: Mutex::new(Vec::new()),
        });
        let provider = Arc::new(InteractiveOAuthProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            pending: pending.clone(),
            completed: sample_profile("completed", Utc::now()),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider).unwrap();
        let client = ConfiguredLlmClient::new(store.clone(), registry);

        let started = client
            .begin_oauth_authorization(ProviderKind::OpenAiCodex, Some("codex-browser".to_string()))
            .await
            .unwrap();

        assert_eq!(started.id, pending.id);
        assert_eq!(store.list_pending_oauth().await.unwrap(), vec![pending]);
    }

    #[tokio::test]
    async fn client_complete_oauth_authorization_upserts_profile_and_clears_pending() {
        let pending = PendingOAuthAuthorization {
            id: Uuid::new_v4(),
            profile_id: Uuid::new_v4(),
            provider: ProviderKind::OpenAiCodex,
            label: "codex-browser".into(),
            issuer: "openai".into(),
            started_at: Utc::now(),
            expires_at: None,
            client_id: "client-id".into(),
            token_endpoint: "https://auth.openai.com/oauth/token".into(),
            scopes: vec!["openid".into(), "offline_access".into()],
            state: Some("state-123".into()),
            pkce_verifier: Some("verifier-123".into()),
            kind: OAuthAuthorizationKind::BrowserCallback {
                authorization_url: "https://auth.openai.com/oauth/authorize?state=state-123".into(),
                redirect_uri: "http://127.0.0.1:1455/auth/callback".into(),
                callback_host: "127.0.0.1".into(),
                callback_path: "/auth/callback".into(),
                paste_prompt: "Paste the redirect URL (or authorization code)".into(),
            },
        };
        let completed = AuthProfile {
            id: pending.profile_id,
            provider: ProviderKind::OpenAiCodex,
            mode: AuthMode::OAuth,
            label: pending.label.clone(),
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("codex-browser".into()),
                access_token: Some("access-token".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::minutes(30)),
            }),
            api_key: None,
            updated_at: Utc::now(),
        };
        let store = Arc::new(PendingAuthStore {
            profiles: Mutex::new(Vec::new()),
            default_profile_id: Mutex::new(None),
            runtime_health: Mutex::new(None),
            pending_oauth: Mutex::new(vec![pending.clone()]),
        });
        let provider = Arc::new(InteractiveOAuthProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            pending,
            completed: completed.clone(),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider).unwrap();
        let client = ConfiguredLlmClient::new(store.clone(), registry);

        let profile = client
            .complete_oauth_authorization(
                store.list_pending_oauth().await.unwrap()[0].id,
                "callback-code",
                true,
            )
            .await
            .unwrap();

        assert_eq!(profile.id, completed.id);
        assert_eq!(
            store.list_pending_oauth().await.unwrap(),
            Vec::<PendingOAuthAuthorization>::new()
        );
        assert_eq!(
            store
                .load_default_profile()
                .await
                .unwrap()
                .map(|profile| profile.id),
            Some(completed.id)
        );
        assert!(
            client
                .scheduled_refreshes
                .lock()
                .unwrap()
                .contains_key(&completed.id)
        );
        assert_eq!(
            store
                .list_profiles()
                .await
                .unwrap()
                .into_iter()
                .find(|candidate| candidate.id == completed.id)
                .map(|candidate| candidate.id),
            Some(completed.id)
        );
    }

    #[tokio::test]
    async fn client_begin_device_oauth_authorization_persists_pending_record() {
        let pending = PendingOAuthAuthorization {
            id: Uuid::new_v4(),
            profile_id: Uuid::new_v4(),
            provider: ProviderKind::OpenAiCodex,
            label: "codex-device".into(),
            issuer: "openai".into(),
            started_at: Utc::now(),
            expires_at: Some(Utc::now() + Duration::minutes(15)),
            client_id: "client-id".into(),
            token_endpoint: "https://auth.openai.com/oauth/token".into(),
            scopes: vec!["openid".into(), "offline_access".into()],
            state: None,
            pkce_verifier: None,
            kind: OAuthAuthorizationKind::DeviceCode {
                verification_uri: "https://auth.openai.com/activate?user_code=DEVICE".into(),
                user_code: "DEVICE".into(),
                device_code: "device-code-123".into(),
                poll_interval_seconds: 5,
            },
        };
        let store = Arc::new(PendingAuthStore {
            profiles: Mutex::new(Vec::new()),
            default_profile_id: Mutex::new(None),
            runtime_health: Mutex::new(None),
            pending_oauth: Mutex::new(Vec::new()),
        });
        let provider = Arc::new(InteractiveOAuthProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            pending: pending.clone(),
            completed: sample_profile("completed", Utc::now()),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider).unwrap();
        let client = ConfiguredLlmClient::new(store.clone(), registry);

        let started = client
            .begin_oauth_authorization_with_mode(
                ProviderKind::OpenAiCodex,
                Some("codex-device".to_string()),
                OAuthInitiationMode::DeviceCode,
            )
            .await
            .unwrap();

        assert_eq!(started.id, pending.id);
        assert_eq!(store.list_pending_oauth().await.unwrap(), vec![pending]);
    }

    #[tokio::test]
    async fn client_complete_device_oauth_authorization_upserts_profile_and_clears_pending() {
        let pending = PendingOAuthAuthorization {
            id: Uuid::new_v4(),
            profile_id: Uuid::new_v4(),
            provider: ProviderKind::OpenAiCodex,
            label: "codex-device".into(),
            issuer: "openai".into(),
            started_at: Utc::now(),
            expires_at: Some(Utc::now() + Duration::minutes(15)),
            client_id: "client-id".into(),
            token_endpoint: "https://auth.openai.com/oauth/token".into(),
            scopes: vec!["openid".into(), "offline_access".into()],
            state: None,
            pkce_verifier: None,
            kind: OAuthAuthorizationKind::DeviceCode {
                verification_uri: "https://auth.openai.com/activate?user_code=DEVICE".into(),
                user_code: "DEVICE".into(),
                device_code: "device-code-123".into(),
                poll_interval_seconds: 5,
            },
        };
        let completed = AuthProfile {
            id: pending.profile_id,
            provider: ProviderKind::OpenAiCodex,
            mode: AuthMode::OAuth,
            label: pending.label.clone(),
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("codex-device".into()),
                access_token: Some("access-token".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::minutes(30)),
            }),
            api_key: None,
            updated_at: Utc::now(),
        };
        let store = Arc::new(PendingAuthStore {
            profiles: Mutex::new(Vec::new()),
            default_profile_id: Mutex::new(None),
            runtime_health: Mutex::new(None),
            pending_oauth: Mutex::new(vec![pending.clone()]),
        });
        let provider = Arc::new(InteractiveOAuthProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            pending,
            completed: completed.clone(),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider).unwrap();
        let client = ConfiguredLlmClient::new(store.clone(), registry);

        let profile = client
            .complete_device_oauth_authorization(
                store.list_pending_oauth().await.unwrap()[0].id,
                true,
            )
            .await
            .unwrap();

        assert_eq!(profile.id, completed.id);
        assert_eq!(
            store.list_pending_oauth().await.unwrap(),
            Vec::<PendingOAuthAuthorization>::new()
        );
        assert_eq!(
            store
                .load_default_profile()
                .await
                .unwrap()
                .map(|profile| profile.id),
            Some(completed.id)
        );
        assert!(
            client
                .scheduled_refreshes
                .lock()
                .unwrap()
                .contains_key(&completed.id)
        );
    }

    #[tokio::test]
    async fn resume_auth_lifecycle_cleans_expired_pending_and_arms_refresh() {
        let profile = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("resident".into()),
                access_token: Some("still-valid".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::milliseconds(30)),
            }),
            label: "resident".into(),
            ..sample_profile("resident", Utc::now())
        };
        let refreshed = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("resident".into()),
                access_token: Some("refreshed-by-resume".into()),
                refresh_token: Some("next-refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::minutes(20)),
            }),
            updated_at: Utc::now() + Duration::seconds(1),
            ..profile.clone()
        };
        let pending = PendingOAuthAuthorization {
            id: Uuid::new_v4(),
            profile_id: Uuid::new_v4(),
            provider: ProviderKind::OpenAiCodex,
            label: "expired-pending".into(),
            issuer: "openai".into(),
            started_at: Utc::now() - Duration::minutes(20),
            expires_at: Some(Utc::now() - Duration::minutes(5)),
            client_id: "client-id".into(),
            token_endpoint: "https://auth.openai.com/oauth/token".into(),
            scopes: vec!["openid".into()],
            state: Some("expired".into()),
            pkce_verifier: Some("verifier".into()),
            kind: OAuthAuthorizationKind::BrowserCallback {
                authorization_url: "https://auth.openai.com/oauth/authorize?state=expired".into(),
                redirect_uri: "http://127.0.0.1:1455/auth/callback".into(),
                callback_host: "127.0.0.1".into(),
                callback_path: "/auth/callback".into(),
                paste_prompt: "Paste the redirect URL (or authorization code)".into(),
            },
        };
        let store = Arc::new(PendingAuthStore {
            profiles: Mutex::new(vec![profile.clone()]),
            default_profile_id: Mutex::new(Some(profile.id)),
            runtime_health: Mutex::new(None),
            pending_oauth: Mutex::new(vec![pending.clone()]),
        });
        let provider = Arc::new(RoutedProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            outcomes: Mutex::new(HashMap::new()),
            call_log: Mutex::new(Vec::new()),
            materialized: Mutex::new(HashMap::from([(
                "resident".into(),
                VecDeque::from([Ok(refreshed.clone())]),
            )])),
            materialize_log: Mutex::new(Vec::new()),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider.clone()).unwrap();
        let client = ConfiguredLlmClient::with_controller(
            store.clone(),
            registry,
            AuthControllerConfig {
                proactive_refresh_seconds: 0,
                proactive_refresh_min_delay_millis: 10,
                proactive_refresh_retry_seconds: 0,
                ..AuthControllerConfig::default()
            },
        );

        let report = client.resume_auth_lifecycle().await.unwrap();

        assert_eq!(report.cleaned_pending_oauth, vec![pending.id]);
        assert!(report.materialized_profiles.is_empty());
        assert_eq!(report.armed_profiles, vec![profile.id]);
        assert!(store.list_pending_oauth().await.unwrap().is_empty());

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let stored = store
            .list_profiles()
            .await
            .unwrap()
            .into_iter()
            .find(|candidate| candidate.id == profile.id)
            .unwrap();
        assert_eq!(
            stored
                .oauth
                .as_ref()
                .and_then(|oauth| oauth.access_token.as_deref()),
            Some("refreshed-by-resume")
        );
        assert_eq!(
            provider.materialize_log.lock().unwrap().as_slice(),
            ["resident"]
        );
    }

    #[tokio::test]
    async fn resume_auth_lifecycle_materializes_refresh_only_profile() {
        let blocked = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("refresh-only".into()),
                access_token: None,
                refresh_token: Some("refresh-token".into()),
                expires_at: None,
            }),
            label: "refresh-only".into(),
            ..sample_profile("refresh-only", Utc::now())
        };
        let materialized = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("refresh-only".into()),
                access_token: Some("fresh-token".into()),
                refresh_token: Some("next-refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::minutes(20)),
            }),
            updated_at: Utc::now() + Duration::seconds(1),
            ..blocked.clone()
        };
        let store = Arc::new(PendingAuthStore {
            profiles: Mutex::new(vec![blocked.clone()]),
            default_profile_id: Mutex::new(Some(blocked.id)),
            runtime_health: Mutex::new(None),
            pending_oauth: Mutex::new(Vec::new()),
        });
        let provider = Arc::new(RoutedProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            outcomes: Mutex::new(HashMap::new()),
            call_log: Mutex::new(Vec::new()),
            materialized: Mutex::new(HashMap::from([(
                "refresh-only".into(),
                VecDeque::from([Ok(materialized.clone())]),
            )])),
            materialize_log: Mutex::new(Vec::new()),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider.clone()).unwrap();
        let client = ConfiguredLlmClient::new(store.clone(), registry);

        let report = client.resume_auth_lifecycle().await.unwrap();

        assert_eq!(report.materialized_profiles, vec![blocked.id]);
        assert_eq!(report.armed_profiles, vec![blocked.id]);
        assert!(report.blocked_profiles.is_empty());
        let stored = store
            .list_profiles()
            .await
            .unwrap()
            .into_iter()
            .find(|candidate| candidate.id == blocked.id)
            .unwrap();
        assert_eq!(
            stored
                .oauth
                .as_ref()
                .and_then(|oauth| oauth.access_token.as_deref()),
            Some("fresh-token")
        );
        assert_eq!(
            provider.materialize_log.lock().unwrap().as_slice(),
            ["refresh-only"]
        );
    }

    #[tokio::test]
    async fn concurrent_resume_auth_lifecycle_serializes_refresh_only_materialization() {
        let blocked = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("refresh-only".into()),
                access_token: None,
                refresh_token: Some("refresh-token".into()),
                expires_at: None,
            }),
            label: "refresh-only".into(),
            ..sample_profile("refresh-only", Utc::now())
        };
        let materialized = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("refresh-only".into()),
                access_token: Some("fresh-token".into()),
                refresh_token: Some("next-refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::minutes(20)),
            }),
            updated_at: Utc::now() + Duration::seconds(1),
            ..blocked.clone()
        };
        let store = Arc::new(PendingAuthStore {
            profiles: Mutex::new(vec![blocked.clone()]),
            default_profile_id: Mutex::new(Some(blocked.id)),
            runtime_health: Mutex::new(None),
            pending_oauth: Mutex::new(Vec::new()),
        });
        let provider = Arc::new(SlowMaterializeProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            profile: materialized.clone(),
            delay_millis: 40,
            materialize_log: Mutex::new(Vec::new()),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider.clone()).unwrap();
        let client = ConfiguredLlmClient::with_controller(
            store.clone(),
            registry,
            AuthControllerConfig {
                proactive_refresh_seconds: 0,
                proactive_refresh_min_delay_millis: 10,
                proactive_refresh_retry_seconds: 0,
                resident_lifecycle_interval_millis: 0,
                ..AuthControllerConfig::default()
            },
        );

        let (first_report, second_report) = tokio::join!(
            client.resume_auth_lifecycle(),
            client.resume_auth_lifecycle()
        );
        let first_report = first_report.unwrap();
        let second_report = second_report.unwrap();

        let total_materializations =
            first_report.materialized_profiles.len() + second_report.materialized_profiles.len();
        let total_armings = first_report.armed_profiles.len() + second_report.armed_profiles.len();

        assert_eq!(total_materializations, 1);
        assert_eq!(total_armings, 1);
        assert_eq!(
            provider.materialize_log.lock().unwrap().as_slice(),
            ["refresh-only"]
        );

        let stored = store
            .list_profiles()
            .await
            .unwrap()
            .into_iter()
            .find(|candidate| candidate.id == blocked.id)
            .unwrap();
        assert_eq!(
            stored
                .oauth
                .as_ref()
                .and_then(|oauth| oauth.access_token.as_deref()),
            Some("fresh-token")
        );
    }

    #[tokio::test]
    async fn stale_resume_does_not_persist_materialized_profile_after_stop() {
        let blocked = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("refresh-only".into()),
                access_token: None,
                refresh_token: Some("refresh-token".into()),
                expires_at: None,
            }),
            label: "refresh-only".into(),
            ..sample_profile("refresh-only", Utc::now())
        };
        let materialized = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("refresh-only".into()),
                access_token: Some("fresh-token".into()),
                refresh_token: Some("next-refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::minutes(20)),
            }),
            updated_at: Utc::now() + Duration::seconds(1),
            ..blocked.clone()
        };
        let store = Arc::new(PendingAuthStore {
            profiles: Mutex::new(vec![blocked.clone()]),
            default_profile_id: Mutex::new(Some(blocked.id)),
            runtime_health: Mutex::new(None),
            pending_oauth: Mutex::new(Vec::new()),
        });
        let provider = Arc::new(SlowMaterializeProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            profile: materialized,
            delay_millis: 40,
            materialize_log: Mutex::new(Vec::new()),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider.clone()).unwrap();
        let client = Arc::new(ConfiguredLlmClient::with_controller(
            store.clone(),
            registry,
            AuthControllerConfig {
                proactive_refresh_seconds: 0,
                proactive_refresh_min_delay_millis: 10,
                proactive_refresh_retry_seconds: 0,
                resident_lifecycle_interval_millis: 0,
                ..AuthControllerConfig::default()
            },
        ));

        let task_client = client.clone();
        let resume =
            tokio::spawn(async move { task_client.resume_auth_lifecycle().await.unwrap() });
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        client.stop_resident_auth_lifecycle();
        let report = resume.await.unwrap();

        assert!(report.materialized_profiles.is_empty());
        assert!(report.armed_profiles.is_empty());
        assert_eq!(
            provider.materialize_log.lock().unwrap().as_slice(),
            ["refresh-only"]
        );
        assert!(client.scheduled_refreshes.lock().unwrap().is_empty());

        let stored = store
            .list_profiles()
            .await
            .unwrap()
            .into_iter()
            .find(|candidate| candidate.id == blocked.id)
            .unwrap();
        assert_eq!(
            stored
                .oauth
                .as_ref()
                .and_then(|oauth| oauth.access_token.as_deref()),
            None
        );
    }

    #[tokio::test]
    async fn resident_auth_lifecycle_sweeps_expiring_pending_without_chat() {
        let profile = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("resident".into()),
                access_token: Some("still-valid".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::minutes(10)),
            }),
            label: "resident".into(),
            ..sample_profile("resident", Utc::now())
        };
        let pending = PendingOAuthAuthorization {
            id: Uuid::new_v4(),
            profile_id: Uuid::new_v4(),
            provider: ProviderKind::OpenAiCodex,
            label: "soon-expiring".into(),
            issuer: "openai".into(),
            started_at: Utc::now(),
            expires_at: Some(Utc::now() + Duration::milliseconds(25)),
            client_id: "client-id".into(),
            token_endpoint: "https://auth.openai.com/oauth/token".into(),
            scopes: vec!["openid".into()],
            state: Some("state".into()),
            pkce_verifier: Some("verifier".into()),
            kind: OAuthAuthorizationKind::BrowserCallback {
                authorization_url: "https://auth.openai.com/oauth/authorize?state=state".into(),
                redirect_uri: "http://127.0.0.1:1455/auth/callback".into(),
                callback_host: "127.0.0.1".into(),
                callback_path: "/auth/callback".into(),
                paste_prompt: "Paste the redirect URL (or authorization code)".into(),
            },
        };
        let store = Arc::new(PendingAuthStore {
            profiles: Mutex::new(vec![profile.clone()]),
            default_profile_id: Mutex::new(Some(profile.id)),
            runtime_health: Mutex::new(None),
            pending_oauth: Mutex::new(vec![pending.clone()]),
        });
        let provider = Arc::new(RoutedProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            outcomes: Mutex::new(HashMap::new()),
            call_log: Mutex::new(Vec::new()),
            materialized: Mutex::new(HashMap::new()),
            materialize_log: Mutex::new(Vec::new()),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider).unwrap();
        let client = ConfiguredLlmClient::with_controller(
            store.clone(),
            registry,
            AuthControllerConfig {
                resident_lifecycle_interval_millis: 10,
                ..AuthControllerConfig::default()
            },
        );

        let report = client.start_resident_auth_lifecycle().await.unwrap();

        assert!(report.cleaned_pending_oauth.is_empty());
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        assert!(store.list_pending_oauth().await.unwrap().is_empty());
        client.stop_resident_auth_lifecycle();
    }

    #[tokio::test]
    async fn stop_resident_auth_lifecycle_cancels_scheduled_refreshes_and_blocks_new_arming() {
        let profile = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("resident".into()),
                access_token: Some("still-valid".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::minutes(10)),
            }),
            label: "resident".into(),
            ..sample_profile("resident", Utc::now())
        };
        let store = Arc::new(PendingAuthStore {
            profiles: Mutex::new(vec![profile.clone()]),
            default_profile_id: Mutex::new(Some(profile.id)),
            runtime_health: Mutex::new(None),
            pending_oauth: Mutex::new(Vec::new()),
        });
        let provider = Arc::new(RoutedProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            outcomes: Mutex::new(HashMap::new()),
            call_log: Mutex::new(Vec::new()),
            materialized: Mutex::new(HashMap::new()),
            materialize_log: Mutex::new(Vec::new()),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider).unwrap();
        let client = ConfiguredLlmClient::with_controller(
            store,
            registry,
            AuthControllerConfig {
                resident_lifecycle_interval_millis: 0,
                ..AuthControllerConfig::default()
            },
        );

        client
            .chat_with_profile(&profile, &sample_request("gpt-5.4"))
            .await
            .unwrap();
        assert!(
            client
                .scheduled_refreshes
                .lock()
                .unwrap()
                .contains_key(&profile.id)
        );

        client.stop_resident_auth_lifecycle();

        assert!(!client.background_auth_enabled.load(Ordering::SeqCst));
        assert!(client.scheduled_refreshes.lock().unwrap().is_empty());

        client
            .chat_with_profile(&profile, &sample_request("gpt-5.4"))
            .await
            .unwrap();
        assert!(client.scheduled_refreshes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn start_resident_auth_lifecycle_reenables_background_refresh_arming() {
        let profile = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("resident".into()),
                access_token: Some("still-valid".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::minutes(10)),
            }),
            label: "resident".into(),
            ..sample_profile("resident", Utc::now())
        };
        let store = Arc::new(PendingAuthStore {
            profiles: Mutex::new(vec![profile.clone()]),
            default_profile_id: Mutex::new(Some(profile.id)),
            runtime_health: Mutex::new(None),
            pending_oauth: Mutex::new(Vec::new()),
        });
        let provider = Arc::new(RoutedProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            outcomes: Mutex::new(HashMap::new()),
            call_log: Mutex::new(Vec::new()),
            materialized: Mutex::new(HashMap::new()),
            materialize_log: Mutex::new(Vec::new()),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider).unwrap();
        let client = ConfiguredLlmClient::with_controller(
            store,
            registry,
            AuthControllerConfig {
                resident_lifecycle_interval_millis: 0,
                ..AuthControllerConfig::default()
            },
        );

        client.stop_resident_auth_lifecycle();
        assert!(client.scheduled_refreshes.lock().unwrap().is_empty());

        let report = client.start_resident_auth_lifecycle().await.unwrap();

        assert!(client.background_auth_enabled.load(Ordering::SeqCst));
        assert!(report.cleaned_pending_oauth.is_empty());
        assert_eq!(report.armed_profiles, vec![profile.id]);
        assert!(
            client
                .scheduled_refreshes
                .lock()
                .unwrap()
                .contains_key(&profile.id)
        );
    }

    #[tokio::test]
    async fn start_resident_auth_lifecycle_is_idempotent_while_running() {
        let profile = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("resident".into()),
                access_token: Some("still-valid".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::minutes(10)),
            }),
            label: "resident".into(),
            ..sample_profile("resident", Utc::now())
        };
        let store = Arc::new(PendingAuthStore {
            profiles: Mutex::new(vec![profile.clone()]),
            default_profile_id: Mutex::new(Some(profile.id)),
            runtime_health: Mutex::new(None),
            pending_oauth: Mutex::new(Vec::new()),
        });
        let provider = Arc::new(RoutedProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            outcomes: Mutex::new(HashMap::new()),
            call_log: Mutex::new(Vec::new()),
            materialized: Mutex::new(HashMap::new()),
            materialize_log: Mutex::new(Vec::new()),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider).unwrap();
        let client = ConfiguredLlmClient::with_controller(
            store,
            registry,
            AuthControllerConfig {
                resident_lifecycle_interval_millis: 50,
                ..AuthControllerConfig::default()
            },
        );

        let first_report = client.start_resident_auth_lifecycle().await.unwrap();
        let first_generation = client.resident_lifecycle.lock().unwrap().generation;
        let first_refresh_generation = client
            .scheduled_refreshes
            .lock()
            .unwrap()
            .get(&profile.id)
            .map(|entry| entry.generation)
            .unwrap();

        let second_report = client.start_resident_auth_lifecycle().await.unwrap();
        let resident = client.resident_lifecycle.lock().unwrap();
        let second_refresh_generation = client
            .scheduled_refreshes
            .lock()
            .unwrap()
            .get(&profile.id)
            .map(|entry| entry.generation)
            .unwrap();

        assert_eq!(first_report.armed_profiles, vec![profile.id]);
        assert!(second_report.armed_profiles.is_empty());
        assert_eq!(resident.generation, first_generation);
        assert_eq!(second_refresh_generation, first_refresh_generation);
        assert!(resident.handle.is_some());
        drop(resident);
        client.stop_resident_auth_lifecycle();
    }

    #[tokio::test]
    async fn resume_auth_lifecycle_does_not_rearm_unchanged_refresh_schedule() {
        let profile = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("resident".into()),
                access_token: Some("still-valid".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::minutes(10)),
            }),
            label: "resident".into(),
            ..sample_profile("resident", Utc::now())
        };
        let store = Arc::new(PendingAuthStore {
            profiles: Mutex::new(vec![profile.clone()]),
            default_profile_id: Mutex::new(Some(profile.id)),
            runtime_health: Mutex::new(None),
            pending_oauth: Mutex::new(Vec::new()),
        });
        let provider = Arc::new(RoutedProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            outcomes: Mutex::new(HashMap::new()),
            call_log: Mutex::new(Vec::new()),
            materialized: Mutex::new(HashMap::new()),
            materialize_log: Mutex::new(Vec::new()),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider).unwrap();
        let client = ConfiguredLlmClient::with_controller(
            store,
            registry,
            AuthControllerConfig {
                resident_lifecycle_interval_millis: 0,
                ..AuthControllerConfig::default()
            },
        );

        let first_report = client.resume_auth_lifecycle().await.unwrap();
        let first_generation = client
            .scheduled_refreshes
            .lock()
            .unwrap()
            .get(&profile.id)
            .map(|entry| entry.generation)
            .unwrap();

        let second_report = client.resume_auth_lifecycle().await.unwrap();
        let second_generation = client
            .scheduled_refreshes
            .lock()
            .unwrap()
            .get(&profile.id)
            .map(|entry| entry.generation)
            .unwrap();

        assert_eq!(first_report.armed_profiles, vec![profile.id]);
        assert!(second_report.armed_profiles.is_empty());
        assert_eq!(second_generation, first_generation);
        client.stop_resident_auth_lifecycle();
    }

    #[tokio::test]
    async fn resume_auth_lifecycle_with_stale_resident_generation_does_not_arm_refreshes() {
        let profile = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("resident".into()),
                access_token: Some("still-valid".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::minutes(10)),
            }),
            label: "resident".into(),
            ..sample_profile("resident", Utc::now())
        };
        let store = Arc::new(PendingAuthStore {
            profiles: Mutex::new(vec![profile.clone()]),
            default_profile_id: Mutex::new(Some(profile.id)),
            runtime_health: Mutex::new(None),
            pending_oauth: Mutex::new(Vec::new()),
        });
        let provider = Arc::new(RoutedProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            outcomes: Mutex::new(HashMap::new()),
            call_log: Mutex::new(Vec::new()),
            materialized: Mutex::new(HashMap::new()),
            materialize_log: Mutex::new(Vec::new()),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider).unwrap();
        let runtime = Arc::new(Mutex::new(AuthRuntimeState::default()));
        let scheduled_refreshes = Arc::new(Mutex::new(HashMap::new()));
        let resident_lifecycle = Arc::new(Mutex::new(ResidentLifecycleLoopState {
            generation: 2,
            handle: None,
        }));
        let refresh_gates = Arc::new(Mutex::new(HashMap::new()));

        let report = resume_auth_lifecycle_inner(
            store,
            registry,
            runtime,
            AuthControllerConfig::default(),
            scheduled_refreshes.clone(),
            resident_lifecycle,
            1,
            refresh_gates,
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicBool::new(true)),
        )
        .await
        .unwrap();

        assert!(report.armed_profiles.is_empty());
        assert!(scheduled_refreshes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn resume_auth_lifecycle_cancels_scheduled_refresh_for_blocked_profile() {
        let profile = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("blocked".into()),
                access_token: None,
                refresh_token: None,
                expires_at: None,
            }),
            label: "blocked".into(),
            ..sample_profile("blocked", Utc::now())
        };
        let store = Arc::new(PendingAuthStore {
            profiles: Mutex::new(vec![profile.clone()]),
            default_profile_id: Mutex::new(Some(profile.id)),
            runtime_health: Mutex::new(None),
            pending_oauth: Mutex::new(Vec::new()),
        });
        let provider = Arc::new(RoutedProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            outcomes: Mutex::new(HashMap::new()),
            call_log: Mutex::new(Vec::new()),
            materialized: Mutex::new(HashMap::new()),
            materialize_log: Mutex::new(Vec::new()),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider).unwrap();
        let client = ConfiguredLlmClient::with_controller(
            store,
            registry,
            AuthControllerConfig {
                resident_lifecycle_interval_millis: 0,
                ..AuthControllerConfig::default()
            },
        );
        let scheduled_handle = tokio::spawn(async {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        });
        client.scheduled_refreshes.lock().unwrap().insert(
            profile.id,
            ScheduledRefreshTask {
                generation: 1,
                resident_generation: 0,
                target_expires_at: refresh_target_expires_at(&profile),
                handle: scheduled_handle,
            },
        );

        let report = client.resume_auth_lifecycle().await.unwrap();

        assert!(report.armed_profiles.is_empty());
        assert_eq!(report.blocked_profiles.len(), 1);
        assert!(client.scheduled_refreshes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn resume_auth_lifecycle_cancels_scheduled_refresh_for_non_refreshable_profile() {
        let profile = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("no-refresh".into()),
                access_token: Some("still-valid".into()),
                refresh_token: None,
                expires_at: Some(Utc::now() + Duration::minutes(10)),
            }),
            label: "no-refresh".into(),
            ..sample_profile("no-refresh", Utc::now())
        };
        let store = Arc::new(PendingAuthStore {
            profiles: Mutex::new(vec![profile.clone()]),
            default_profile_id: Mutex::new(Some(profile.id)),
            runtime_health: Mutex::new(None),
            pending_oauth: Mutex::new(Vec::new()),
        });
        let provider = Arc::new(RoutedProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            outcomes: Mutex::new(HashMap::new()),
            call_log: Mutex::new(Vec::new()),
            materialized: Mutex::new(HashMap::new()),
            materialize_log: Mutex::new(Vec::new()),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider).unwrap();
        let client = ConfiguredLlmClient::with_controller(
            store,
            registry,
            AuthControllerConfig {
                resident_lifecycle_interval_millis: 0,
                ..AuthControllerConfig::default()
            },
        );
        let scheduled_handle = tokio::spawn(async {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        });
        client.scheduled_refreshes.lock().unwrap().insert(
            profile.id,
            ScheduledRefreshTask {
                generation: 1,
                resident_generation: 0,
                target_expires_at: refresh_target_expires_at(&profile),
                handle: scheduled_handle,
            },
        );

        let report = client.resume_auth_lifecycle().await.unwrap();

        assert!(report.armed_profiles.is_empty());
        assert!(report.blocked_profiles.is_empty());
        assert!(client.scheduled_refreshes.lock().unwrap().is_empty());
    }

    #[test]
    fn refresh_schedule_decision_cancels_when_background_auth_is_disabled() {
        let profile = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("disabled".into()),
                access_token: Some("still-valid".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::minutes(10)),
            }),
            label: "disabled".into(),
            ..sample_profile("disabled", Utc::now())
        };
        let resident_lifecycle = Arc::new(Mutex::new(ResidentLifecycleLoopState {
            generation: 1,
            handle: None,
        }));

        let decision = decide_refresh_schedule(
            &resident_lifecycle,
            &Arc::new(AtomicBool::new(false)),
            &Arc::new(AtomicBool::new(true)),
            1,
            &profile,
            &AuthControllerConfig::default(),
            StaleScheduleOwnershipBehavior::Noop,
        );

        assert_eq!(decision, RefreshScheduleDecision::Cancel);
    }

    #[test]
    fn refresh_schedule_decision_cancels_when_profile_is_not_refreshable() {
        let profile = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("no-refresh".into()),
                access_token: Some("still-valid".into()),
                refresh_token: None,
                expires_at: Some(Utc::now() + Duration::minutes(10)),
            }),
            label: "no-refresh".into(),
            ..sample_profile("no-refresh", Utc::now())
        };
        let resident_lifecycle = Arc::new(Mutex::new(ResidentLifecycleLoopState {
            generation: 1,
            handle: None,
        }));

        let decision = decide_refresh_schedule(
            &resident_lifecycle,
            &Arc::new(AtomicBool::new(true)),
            &Arc::new(AtomicBool::new(true)),
            1,
            &profile,
            &AuthControllerConfig::default(),
            StaleScheduleOwnershipBehavior::Noop,
        );

        assert_eq!(decision, RefreshScheduleDecision::Cancel);
    }

    #[test]
    fn refresh_schedule_decision_respects_stale_ownership_behavior() {
        let profile = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("stale".into()),
                access_token: Some("still-valid".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::minutes(10)),
            }),
            label: "stale".into(),
            ..sample_profile("stale", Utc::now())
        };
        let resident_lifecycle = Arc::new(Mutex::new(ResidentLifecycleLoopState {
            generation: 2,
            handle: None,
        }));
        let background_auth_enabled = Arc::new(AtomicBool::new(true));
        let alive = Arc::new(AtomicBool::new(true));

        let noop_decision = decide_refresh_schedule(
            &resident_lifecycle,
            &background_auth_enabled,
            &alive,
            1,
            &profile,
            &AuthControllerConfig::default(),
            StaleScheduleOwnershipBehavior::Noop,
        );
        let clear_gate_decision = decide_refresh_schedule(
            &resident_lifecycle,
            &background_auth_enabled,
            &alive,
            1,
            &profile,
            &AuthControllerConfig::default(),
            StaleScheduleOwnershipBehavior::ClearGateAndNoop,
        );

        assert_eq!(noop_decision, RefreshScheduleDecision::Noop);
        assert_eq!(
            clear_gate_decision,
            RefreshScheduleDecision::ClearGateAndNoop
        );
    }

    #[test]
    fn runtime_refresh_plan_refreshes_now_inside_window() {
        let controller = AuthControllerConfig {
            proactive_refresh_seconds: 300,
            proactive_refresh_min_delay_millis: 5_000,
            ..AuthControllerConfig::default()
        };
        let now = Utc::now();
        let profile = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("boundary".into()),
                access_token: Some("still-valid".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(now + Duration::seconds(300)),
            }),
            label: "boundary".into(),
            ..sample_profile("boundary", now)
        };

        let plan = plan_runtime_auth_refresh(&profile, now, &controller);

        assert_eq!(plan, RuntimeRefreshPlan::RefreshNow);
    }

    #[test]
    fn runtime_refresh_plan_defers_outside_window() {
        let controller = AuthControllerConfig {
            proactive_refresh_seconds: 300,
            proactive_refresh_min_delay_millis: 5_000,
            ..AuthControllerConfig::default()
        };
        let now = Utc::now();
        let profile = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("later".into()),
                access_token: Some("still-valid".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(now + Duration::minutes(20)),
            }),
            label: "later".into(),
            ..sample_profile("later", now)
        };

        let plan = plan_runtime_auth_refresh(&profile, now, &controller);

        match plan {
            RuntimeRefreshPlan::RefreshAfter(delay) => {
                assert!(
                    delay
                        > std::time::Duration::from_millis(
                            controller.proactive_refresh_min_delay_millis,
                        )
                );
            }
            other => panic!("expected deferred refresh plan, got {other:?}"),
        }
    }

    #[test]
    fn runtime_refresh_plan_is_none_when_profile_is_not_refreshable() {
        let controller = AuthControllerConfig::default();
        let now = Utc::now();
        let profile = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("none".into()),
                access_token: Some("still-valid".into()),
                refresh_token: None,
                expires_at: Some(now + Duration::minutes(20)),
            }),
            label: "none".into(),
            ..sample_profile("none", now)
        };

        let plan = plan_runtime_auth_refresh(&profile, now, &controller);

        assert_eq!(plan, RuntimeRefreshPlan::NoRefresh);
    }

    #[test]
    fn runtime_refresh_action_needs_refresh_is_materialize_now_required() {
        let controller = AuthControllerConfig::default();
        let now = Utc::now();
        let profile = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("needs-refresh".into()),
                access_token: None,
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(now + Duration::minutes(20)),
            }),
            label: "needs-refresh".into(),
            ..sample_profile("needs-refresh", now)
        };

        let action = classify_runtime_refresh_action(&profile, now, &controller);

        assert_eq!(
            action,
            RuntimeRefreshAction::MaterializeNow { required: true }
        );
    }

    #[test]
    fn runtime_refresh_action_ready_in_window_is_materialize_now() {
        let controller = AuthControllerConfig {
            proactive_refresh_seconds: 300,
            proactive_refresh_min_delay_millis: 5_000,
            ..AuthControllerConfig::default()
        };
        let now = Utc::now();
        let profile = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("boundary-action".into()),
                access_token: Some("still-valid".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(now + Duration::seconds(300)),
            }),
            label: "boundary-action".into(),
            ..sample_profile("boundary-action", now)
        };

        let action = classify_runtime_refresh_action(&profile, now, &controller);

        assert_eq!(
            action,
            RuntimeRefreshAction::MaterializeNow { required: false }
        );
    }

    #[test]
    fn runtime_refresh_action_ready_outside_window_is_reschedule() {
        let controller = AuthControllerConfig {
            proactive_refresh_seconds: 300,
            proactive_refresh_min_delay_millis: 5_000,
            ..AuthControllerConfig::default()
        };
        let now = Utc::now();
        let profile = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("later-action".into()),
                access_token: Some("still-valid".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(now + Duration::minutes(20)),
            }),
            label: "later-action".into(),
            ..sample_profile("later-action", now)
        };

        let action = classify_runtime_refresh_action(&profile, now, &controller);

        match action {
            RuntimeRefreshAction::Reschedule(delay) => {
                assert!(
                    delay
                        > std::time::Duration::from_millis(
                            controller.proactive_refresh_min_delay_millis,
                        )
                );
            }
            other => panic!("expected reschedule action, got {other:?}"),
        }
    }

    #[test]
    fn runtime_refresh_action_not_refreshable_uses_current_profile() {
        let controller = AuthControllerConfig::default();
        let now = Utc::now();
        let profile = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("ready-no-refresh".into()),
                access_token: Some("still-valid".into()),
                refresh_token: None,
                expires_at: Some(now + Duration::minutes(20)),
            }),
            label: "ready-no-refresh".into(),
            ..sample_profile("ready-no-refresh", now)
        };

        let action = classify_runtime_refresh_action(&profile, now, &controller);

        assert_eq!(action, RuntimeRefreshAction::UseCurrentProfile);
    }

    #[tokio::test]
    async fn scheduled_refresh_replacement_reuses_newer_matching_schedule() {
        let profile = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("matching".into()),
                access_token: Some("still-valid".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::minutes(10)),
            }),
            label: "matching".into(),
            ..sample_profile("matching", Utc::now())
        };
        let target_expires_at = refresh_target_expires_at(&profile);
        let tasks = Arc::new(Mutex::new(HashMap::new()));
        tasks.lock().unwrap().insert(
            profile.id,
            ScheduledRefreshTask {
                generation: 2,
                resident_generation: 7,
                target_expires_at,
                handle: tokio::spawn(async {
                    sleep(std::time::Duration::from_secs(60)).await;
                }),
            },
        );

        let decision =
            decide_scheduled_refresh_replacement(&tasks, profile.id, 7, target_expires_at, Some(1));

        assert_eq!(decision, ScheduledRefreshReplacementDecision::Noop);
        if let Some(existing) = tasks.lock().unwrap().remove(&profile.id) {
            existing.handle.abort();
        }
    }

    #[tokio::test]
    async fn scheduled_refresh_replacement_advances_current_timer_generation() {
        let profile = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("advance".into()),
                access_token: Some("still-valid".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::minutes(10)),
            }),
            label: "advance".into(),
            ..sample_profile("advance", Utc::now())
        };
        let target_expires_at = refresh_target_expires_at(&profile);
        let tasks = Arc::new(Mutex::new(HashMap::new()));
        tasks.lock().unwrap().insert(
            profile.id,
            ScheduledRefreshTask {
                generation: 1,
                resident_generation: 7,
                target_expires_at,
                handle: tokio::spawn(async {
                    sleep(std::time::Duration::from_secs(60)).await;
                }),
            },
        );

        let decision =
            decide_scheduled_refresh_replacement(&tasks, profile.id, 7, target_expires_at, Some(1));

        assert_eq!(
            decision,
            ScheduledRefreshReplacementDecision::Replace { next_generation: 2 }
        );
        if let Some(existing) = tasks.lock().unwrap().remove(&profile.id) {
            existing.handle.abort();
        }
    }

    #[test]
    fn schedule_refresh_task_does_not_spawn_when_resident_generation_is_stale() {
        let profile = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("resident".into()),
                access_token: Some("still-valid".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::minutes(10)),
            }),
            label: "resident".into(),
            ..sample_profile("resident", Utc::now())
        };
        let store = Arc::new(MemoryAuthStore {
            profiles: Mutex::new(vec![profile.clone()]),
            default_profile_id: Mutex::new(Some(profile.id)),
            runtime_health: Mutex::new(None),
        });
        let provider = Arc::new(RoutedProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            outcomes: Mutex::new(HashMap::new()),
            call_log: Mutex::new(Vec::new()),
            materialized: Mutex::new(HashMap::new()),
            materialize_log: Mutex::new(Vec::new()),
        });
        let tasks = Arc::new(Mutex::new(HashMap::new()));
        let refresh_gates = Arc::new(Mutex::new(HashMap::new()));
        let resident_lifecycle = Arc::new(Mutex::new(ResidentLifecycleLoopState {
            generation: 2,
            handle: None,
        }));

        schedule_refresh_task(
            store,
            provider,
            tasks.clone(),
            resident_lifecycle,
            refresh_gates.clone(),
            AuthControllerConfig::default(),
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicBool::new(true)),
            profile,
            None,
            1,
            1,
        );

        assert!(tasks.lock().unwrap().is_empty());
        assert!(refresh_gates.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn successful_calls_do_not_churn_unchanged_refresh_schedule() {
        let profile = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("resident".into()),
                access_token: Some("still-valid".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::minutes(10)),
            }),
            label: "resident".into(),
            ..sample_profile("resident", Utc::now())
        };
        let store = Arc::new(PendingAuthStore {
            profiles: Mutex::new(vec![profile.clone()]),
            default_profile_id: Mutex::new(Some(profile.id)),
            runtime_health: Mutex::new(None),
            pending_oauth: Mutex::new(Vec::new()),
        });
        let provider = Arc::new(RoutedProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            outcomes: Mutex::new(HashMap::new()),
            call_log: Mutex::new(Vec::new()),
            materialized: Mutex::new(HashMap::new()),
            materialize_log: Mutex::new(Vec::new()),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider).unwrap();
        let client = ConfiguredLlmClient::with_controller(
            store,
            registry,
            AuthControllerConfig {
                resident_lifecycle_interval_millis: 0,
                ..AuthControllerConfig::default()
            },
        );

        client
            .chat_with_profile(&profile, &sample_request("gpt-5.4"))
            .await
            .unwrap();
        let first_generation = client
            .scheduled_refreshes
            .lock()
            .unwrap()
            .get(&profile.id)
            .map(|entry| entry.generation)
            .unwrap();

        client
            .chat_with_profile(&profile, &sample_request("gpt-5.4"))
            .await
            .unwrap();
        let second_generation = client
            .scheduled_refreshes
            .lock()
            .unwrap()
            .get(&profile.id)
            .map(|entry| entry.generation)
            .unwrap();

        assert_eq!(second_generation, first_generation);
        client.stop_resident_auth_lifecycle();
    }

    #[tokio::test]
    async fn chat_with_profile_arms_refresh_with_current_resident_generation() {
        let profile = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("resident".into()),
                access_token: Some("still-valid".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::minutes(10)),
            }),
            label: "resident".into(),
            ..sample_profile("resident", Utc::now())
        };
        let store = Arc::new(PendingAuthStore {
            profiles: Mutex::new(vec![profile.clone()]),
            default_profile_id: Mutex::new(Some(profile.id)),
            runtime_health: Mutex::new(None),
            pending_oauth: Mutex::new(Vec::new()),
        });
        let provider = Arc::new(RoutedProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            outcomes: Mutex::new(HashMap::new()),
            call_log: Mutex::new(Vec::new()),
            materialized: Mutex::new(HashMap::new()),
            materialize_log: Mutex::new(Vec::new()),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider).unwrap();
        let client = ConfiguredLlmClient::with_controller(
            store,
            registry,
            AuthControllerConfig {
                resident_lifecycle_interval_millis: 0,
                ..AuthControllerConfig::default()
            },
        );

        {
            let mut resident = client.resident_lifecycle.lock().unwrap();
            resident.generation = 7;
        }

        client
            .chat_with_profile(&profile, &sample_request("gpt-5.4"))
            .await
            .unwrap();

        let scheduled = client
            .scheduled_refreshes
            .lock()
            .unwrap()
            .get(&profile.id)
            .map(|entry| entry.resident_generation)
            .unwrap();
        assert_eq!(scheduled, 7);
        client.stop_resident_auth_lifecycle();
    }

    #[tokio::test]
    async fn concurrent_start_resident_auth_lifecycle_spawns_single_generation() {
        let profile = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("resident".into()),
                access_token: Some("still-valid".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::minutes(10)),
            }),
            label: "resident".into(),
            ..sample_profile("resident", Utc::now())
        };
        let store = Arc::new(PendingAuthStore {
            profiles: Mutex::new(vec![profile.clone()]),
            default_profile_id: Mutex::new(Some(profile.id)),
            runtime_health: Mutex::new(None),
            pending_oauth: Mutex::new(Vec::new()),
        });
        let provider = Arc::new(RoutedProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            outcomes: Mutex::new(HashMap::new()),
            call_log: Mutex::new(Vec::new()),
            materialized: Mutex::new(HashMap::new()),
            materialize_log: Mutex::new(Vec::new()),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider).unwrap();
        let client = ConfiguredLlmClient::with_controller(
            store,
            registry,
            AuthControllerConfig {
                resident_lifecycle_interval_millis: 50,
                ..AuthControllerConfig::default()
            },
        );

        let (_first_report, _second_report) = tokio::join!(
            client.start_resident_auth_lifecycle(),
            client.start_resident_auth_lifecycle()
        );

        let resident = client.resident_lifecycle.lock().unwrap();
        assert_eq!(resident.generation, 1);
        assert!(
            resident
                .handle
                .as_ref()
                .is_some_and(|handle| !handle.is_finished())
        );
        drop(resident);
        client.stop_resident_auth_lifecycle();
    }

    #[tokio::test]
    async fn resident_lifecycle_stale_generation_stops_old_loop() {
        let profile = AuthProfile {
            oauth: Some(OAuthState {
                issuer: "openai".into(),
                account_label: Some("resident".into()),
                access_token: Some("still-valid".into()),
                refresh_token: Some("refresh-token".into()),
                expires_at: Some(Utc::now() + Duration::minutes(10)),
            }),
            label: "resident".into(),
            ..sample_profile("resident", Utc::now())
        };
        let store = Arc::new(PendingAuthStore {
            profiles: Mutex::new(vec![profile.clone()]),
            default_profile_id: Mutex::new(Some(profile.id)),
            runtime_health: Mutex::new(None),
            pending_oauth: Mutex::new(Vec::new()),
        });
        let provider = Arc::new(RoutedProvider {
            descriptor: ProviderDescriptor {
                id: "openai-codex".into(),
                provider: ProviderKind::OpenAiCodex,
                display_name: "OpenAI Codex".into(),
                auth_modes: vec![AuthMode::OAuth],
                default_model: "gpt-5.4".into(),
            },
            outcomes: Mutex::new(HashMap::new()),
            call_log: Mutex::new(Vec::new()),
            materialized: Mutex::new(HashMap::new()),
            materialize_log: Mutex::new(Vec::new()),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider).unwrap();
        let client = ConfiguredLlmClient::with_controller(
            store,
            registry,
            AuthControllerConfig {
                resident_lifecycle_interval_millis: 20,
                ..AuthControllerConfig::default()
            },
        );

        client.start_resident_auth_lifecycle().await.unwrap();
        let first_generation = client.resident_lifecycle.lock().unwrap().generation;
        {
            let mut resident = client.resident_lifecycle.lock().unwrap();
            resident.generation = resident.generation.saturating_add(1);
        }

        tokio::time::sleep(std::time::Duration::from_millis(60)).await;

        let resident = client.resident_lifecycle.lock().unwrap();
        assert_eq!(resident.generation, first_generation + 1);
        assert!(
            resident
                .handle
                .as_ref()
                .is_some_and(|handle| handle.is_finished())
        );
        drop(resident);
        client.stop_resident_auth_lifecycle();
    }
}
