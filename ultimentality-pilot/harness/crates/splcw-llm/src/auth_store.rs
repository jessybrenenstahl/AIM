use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use crate::{AuthProfile, AuthProfileStore, AuthRuntimeHealth, PendingOAuthAuthorization};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct AuthStoreFile {
    default_profile_id: Option<Uuid>,
    profiles: Vec<AuthProfile>,
    pending_oauth: Vec<PendingOAuthAuthorization>,
    runtime_health: Option<AuthRuntimeHealth>,
}

pub struct FileAuthProfileStore {
    path: PathBuf,
}

static AUTH_STORE_LOCKS: OnceLock<Mutex<HashMap<PathBuf, Arc<AsyncMutex<()>>>>> = OnceLock::new();

impl FileAuthProfileStore {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    async fn load_file(&self) -> anyhow::Result<AuthStoreFile> {
        if !self.path.exists() {
            return Ok(AuthStoreFile::default());
        }

        let body = fs::read_to_string(&self.path)
            .await
            .with_context(|| format!("read auth store {}", self.path.display()))?;
        Ok(serde_json::from_str(&body).context("deserialize auth store")?)
    }

    async fn save_file(&self, store: &AuthStoreFile) -> anyhow::Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create auth store directory {}", parent.display()))?;
        }

        let body = serde_json::to_string_pretty(store)?;
        write_atomic_text(&self.path, &body)
            .await
            .with_context(|| format!("write auth store {}", self.path.display()))?;
        Ok(())
    }

    async fn lock_for_path(&self) -> Arc<AsyncMutex<()>> {
        let registry = AUTH_STORE_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
        let mut guard = registry.lock().expect("auth store lock registry poisoned");
        guard
            .entry(self.path.clone())
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
    }
}

#[async_trait]
impl AuthProfileStore for FileAuthProfileStore {
    async fn list_profiles(&self) -> anyhow::Result<Vec<AuthProfile>> {
        Ok(self.load_file().await?.profiles)
    }

    async fn upsert_profile(&self, profile: &AuthProfile) -> anyhow::Result<()> {
        let path_lock = self.lock_for_path().await;
        let _guard = path_lock.lock().await;
        let mut file = self.load_file().await?;
        if let Some(existing) = file
            .profiles
            .iter_mut()
            .find(|entry| entry.id == profile.id)
        {
            *existing = profile.clone();
        } else {
            file.profiles.push(profile.clone());
        }
        self.save_file(&file).await
    }

    async fn load_default_profile(&self) -> anyhow::Result<Option<AuthProfile>> {
        let file = self.load_file().await?;
        Ok(file
            .default_profile_id
            .and_then(|id| file.profiles.into_iter().find(|profile| profile.id == id)))
    }

    async fn set_default_profile(&self, profile_id: Uuid) -> anyhow::Result<()> {
        let path_lock = self.lock_for_path().await;
        let _guard = path_lock.lock().await;
        let mut file = self.load_file().await?;
        file.default_profile_id = Some(profile_id);
        self.save_file(&file).await
    }

    async fn list_pending_oauth(&self) -> anyhow::Result<Vec<PendingOAuthAuthorization>> {
        Ok(self.load_file().await?.pending_oauth)
    }

    async fn upsert_pending_oauth(
        &self,
        pending: &PendingOAuthAuthorization,
    ) -> anyhow::Result<()> {
        let path_lock = self.lock_for_path().await;
        let _guard = path_lock.lock().await;
        let mut file = self.load_file().await?;
        if let Some(existing) = file
            .pending_oauth
            .iter_mut()
            .find(|entry| entry.id == pending.id)
        {
            *existing = pending.clone();
        } else {
            file.pending_oauth.push(pending.clone());
        }
        self.save_file(&file).await
    }

    async fn delete_pending_oauth(&self, pending_id: Uuid) -> anyhow::Result<()> {
        let path_lock = self.lock_for_path().await;
        let _guard = path_lock.lock().await;
        let mut file = self.load_file().await?;
        file.pending_oauth.retain(|entry| entry.id != pending_id);
        self.save_file(&file).await
    }

    async fn load_runtime_health(&self) -> anyhow::Result<Option<AuthRuntimeHealth>> {
        Ok(self.load_file().await?.runtime_health)
    }

    async fn save_runtime_health(&self, health: &AuthRuntimeHealth) -> anyhow::Result<()> {
        let path_lock = self.lock_for_path().await;
        let _guard = path_lock.lock().await;
        let mut file = self.load_file().await?;
        file.runtime_health = Some(health.clone());
        self.save_file(&file).await
    }
}

async fn write_atomic_text(path: &Path, body: &str) -> anyhow::Result<()> {
    let temp_path = path.with_extension(format!("{}.tmp", Uuid::new_v4()));
    if let Some(parent) = temp_path.parent() {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create auth temp directory {}", parent.display()))?;
    }

    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&temp_path)
        .await
        .with_context(|| format!("open auth temp file {}", temp_path.display()))?;
    file.write_all(body.as_bytes())
        .await
        .with_context(|| format!("write auth temp file {}", temp_path.display()))?;
    file.flush()
        .await
        .with_context(|| format!("flush auth temp file {}", temp_path.display()))?;
    file.sync_all()
        .await
        .with_context(|| format!("sync auth temp file {}", temp_path.display()))?;
    drop(file);
    replace_file_atomically(&temp_path, path)?;
    Ok(())
}

fn replace_file_atomically(temp_path: &Path, target_path: &Path) -> anyhow::Result<()> {
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;

        const MOVEFILE_REPLACE_EXISTING: u32 = 0x1;
        const MOVEFILE_WRITE_THROUGH: u32 = 0x8;

        unsafe extern "system" {
            fn MoveFileExW(existing: *const u16, new: *const u16, flags: u32) -> i32;
        }

        let existing = temp_path
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<u16>>();
        let new = target_path
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<u16>>();
        let result = unsafe {
            MoveFileExW(
                existing.as_ptr(),
                new.as_ptr(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
            )
        };
        if result == 0 {
            let error = std::io::Error::last_os_error();
            let _ = std::fs::remove_file(temp_path);
            return Err(anyhow!(error)).with_context(|| {
                format!(
                    "replace auth store {} with {}",
                    target_path.display(),
                    temp_path.display()
                )
            });
        }
        Ok(())
    }
    #[cfg(not(windows))]
    {
        std::fs::rename(temp_path, target_path).with_context(|| {
            format!(
                "replace auth store {} with {}",
                target_path.display(),
                temp_path.display()
            )
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::Utc;
    use tempfile::tempdir;
    use uuid::Uuid;

    use crate::{
        ApiKeyState, AuthFailureKind, AuthMode, AuthProfile, AuthRuntimeHealth,
        AuthRuntimeHealthEntry, OAuthAuthorizationKind, PendingOAuthAuthorization, ProviderKind,
    };

    use super::*;

    #[tokio::test]
    async fn file_auth_store_round_trips_profiles_and_default() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("auth").join("profiles.json");
        let store = FileAuthProfileStore::new(&path);

        let profile = AuthProfile {
            id: Uuid::new_v4(),
            provider: ProviderKind::OpenAiCodex,
            mode: AuthMode::OAuth,
            label: "primary".into(),
            oauth: Some(crate::OAuthState {
                issuer: "openai".into(),
                account_label: Some("jessy".into()),
                access_token: None,
                refresh_token: None,
                expires_at: None,
            }),
            api_key: Some(ApiKeyState {
                env_var: None,
                key_material: None,
            }),
            updated_at: Utc::now(),
        };

        store.upsert_profile(&profile).await.unwrap();
        store.set_default_profile(profile.id).await.unwrap();

        let listed = store.list_profiles().await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, profile.id);

        let default = store.load_default_profile().await.unwrap().unwrap();
        assert_eq!(default.id, profile.id);
    }

    #[tokio::test]
    async fn file_auth_store_serializes_concurrent_upserts() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("auth").join("profiles.json");
        let store = Arc::new(FileAuthProfileStore::new(&path));

        let first = AuthProfile {
            id: Uuid::new_v4(),
            provider: ProviderKind::OpenAiCodex,
            mode: AuthMode::OAuth,
            label: "first".into(),
            oauth: Some(crate::OAuthState {
                issuer: "openai".into(),
                account_label: Some("jessy".into()),
                access_token: Some("token-1".into()),
                refresh_token: None,
                expires_at: None,
            }),
            api_key: Some(ApiKeyState {
                env_var: None,
                key_material: None,
            }),
            updated_at: Utc::now(),
        };
        let second = AuthProfile {
            id: Uuid::new_v4(),
            provider: ProviderKind::OpenAiApi,
            mode: AuthMode::ApiKey,
            label: "second".into(),
            oauth: None,
            api_key: Some(ApiKeyState {
                env_var: None,
                key_material: Some("api-key".into()),
            }),
            updated_at: Utc::now(),
        };

        let first_store = store.clone();
        let second_store = store.clone();
        let first_profile = first.clone();
        let second_profile = second.clone();
        let (first_result, second_result) = tokio::join!(
            async move { first_store.upsert_profile(&first_profile).await },
            async move { second_store.upsert_profile(&second_profile).await }
        );
        first_result.unwrap();
        second_result.unwrap();

        let listed = store.list_profiles().await.unwrap();
        assert_eq!(listed.len(), 2);
        assert!(listed.iter().any(|profile| profile.id == first.id));
        assert!(listed.iter().any(|profile| profile.id == second.id));
    }

    #[tokio::test]
    async fn file_auth_store_round_trips_runtime_health() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("auth").join("profiles.json");
        let store = FileAuthProfileStore::new(&path);
        let profile_id = Uuid::new_v4();
        let health = AuthRuntimeHealth {
            last_successful_profile: Some(profile_id),
            cooldowns: vec![AuthRuntimeHealthEntry {
                profile_id,
                kind: AuthFailureKind::RateLimit,
                failures: 2,
                until: Utc::now(),
                last_error: "HTTP 429 rate limit exceeded".into(),
            }],
        };

        store.save_runtime_health(&health).await.unwrap();

        let loaded = store.load_runtime_health().await.unwrap().unwrap();
        assert_eq!(loaded, health);
    }

    #[tokio::test]
    async fn file_auth_store_round_trips_pending_oauth() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("auth").join("profiles.json");
        let store = FileAuthProfileStore::new(&path);
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

        store.upsert_pending_oauth(&pending).await.unwrap();
        assert_eq!(
            store.list_pending_oauth().await.unwrap(),
            vec![pending.clone()]
        );

        store.delete_pending_oauth(pending.id).await.unwrap();
        assert!(store.list_pending_oauth().await.unwrap().is_empty());
    }
}
