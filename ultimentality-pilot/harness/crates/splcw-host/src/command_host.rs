use anyhow::{Context, bail};
use serde::Serialize;
use splcw_computer_use::{ActionExecution, ObservationFrame, ProposedAction};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::time::{Duration, timeout};

use crate::HostBody;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendPlatform {
    Windows,
    MacOsAppleSilicon,
}

impl BackendPlatform {
    pub fn current() -> anyhow::Result<Self> {
        #[cfg(target_os = "windows")]
        {
            Ok(Self::Windows)
        }
        #[cfg(target_os = "macos")]
        {
            Ok(Self::MacOsAppleSilicon)
        }
        #[cfg(not(any(target_os = "windows", target_os = "macos")))]
        {
            bail!("unsupported platform for host backend binding")
        }
    }
}

#[derive(Debug, Clone)]
pub struct HostCommandSpec {
    pub program: String,
    pub args: Vec<String>,
}

pub struct CommandHostBody {
    observe: HostCommandSpec,
    enact: HostCommandSpec,
    verify: HostCommandSpec,
    command_timeout: Duration,
}

impl CommandHostBody {
    pub fn new(observe: HostCommandSpec, enact: HostCommandSpec, verify: HostCommandSpec) -> Self {
        Self {
            observe,
            enact,
            verify,
            command_timeout: Duration::from_secs(30),
        }
    }

    pub fn from_backend_binary<S>(program: S) -> Self
    where
        S: Into<String> + Clone,
    {
        let program = program.into();
        Self::new(
            HostCommandSpec {
                program: program.clone(),
                args: vec!["observe".into()],
            },
            HostCommandSpec {
                program: program.clone(),
                args: vec!["enact".into()],
            },
            HostCommandSpec {
                program,
                args: vec!["verify".into()],
            },
        )
    }

    pub fn with_command_timeout(mut self, command_timeout: Duration) -> Self {
        self.command_timeout = command_timeout;
        self
    }

    async fn run_json_command<T>(&self, spec: &HostCommandSpec, input: &T) -> anyhow::Result<String>
    where
        T: Serialize + ?Sized,
    {
        let mut child = Command::new(&spec.program)
            .args(&spec.args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .with_context(|| format!("spawn host command {}", spec.program))?;

        let payload = serde_json::to_vec(input)?;

        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(&payload)
                .await
                .context("write host command stdin")?;
        }

        let mut stdout = child.stdout.take().context("capture host command stdout")?;
        let mut stderr = child.stderr.take().context("capture host command stderr")?;

        let stdout_task = tokio::spawn(async move {
            let mut buffer = Vec::new();
            stdout.read_to_end(&mut buffer).await?;
            Ok::<Vec<u8>, std::io::Error>(buffer)
        });
        let stderr_task = tokio::spawn(async move {
            let mut buffer = Vec::new();
            stderr.read_to_end(&mut buffer).await?;
            Ok::<Vec<u8>, std::io::Error>(buffer)
        });

        let status = match timeout(self.command_timeout, child.wait()).await {
            Ok(result) => {
                result.with_context(|| format!("wait for host command {}", spec.program))?
            }
            Err(_) => {
                let _ = child.kill().await;
                let _ = child.wait().await;
                bail!(
                    "host command {} timed out after {:?}",
                    spec.program,
                    self.command_timeout
                );
            }
        };

        let stdout = stdout_task
            .await
            .context("join host command stdout task")?
            .context("read host command stdout")?;
        let stderr = stderr_task
            .await
            .context("join host command stderr task")?
            .context("read host command stderr")?;

        if !status.success() {
            bail!(
                "host command {} failed: {}",
                spec.program,
                String::from_utf8_lossy(&stderr)
            );
        }

        Ok(String::from_utf8(stdout)?)
    }
}

#[async_trait::async_trait]
impl HostBody for CommandHostBody {
    async fn observe(&self) -> anyhow::Result<ObservationFrame> {
        let output = self
            .run_json_command(&self.observe, &serde_json::Value::Null)
            .await?;
        Ok(serde_json::from_str(&output).context("deserialize observation frame")?)
    }

    async fn enact(&self, action: &ProposedAction) -> anyhow::Result<ActionExecution> {
        let output = self.run_json_command(&self.enact, action).await?;
        Ok(serde_json::from_str(&output).context("deserialize action execution")?)
    }

    async fn verify_post_action(
        &self,
        execution: &ActionExecution,
    ) -> anyhow::Result<ObservationFrame> {
        let output = self.run_json_command(&self.verify, execution).await?;
        Ok(serde_json::from_str(&output).context("deserialize post-action verification frame")?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "windows")]
    #[tokio::test]
    async fn run_json_command_times_out_hung_child() {
        let body = CommandHostBody::new(
            HostCommandSpec {
                program: "cmd".into(),
                args: vec!["/C".into(), "ping 127.0.0.1 -n 6 > nul".into()],
            },
            HostCommandSpec {
                program: "cmd".into(),
                args: vec!["/C".into(), "echo {}".into()],
            },
            HostCommandSpec {
                program: "cmd".into(),
                args: vec!["/C".into(), "echo {}".into()],
            },
        )
        .with_command_timeout(Duration::from_millis(100));

        let error = body
            .run_json_command(&body.observe, &serde_json::Value::Null)
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("timed out"));
    }
}
