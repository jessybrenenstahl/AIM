use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStderr, ChildStdin, ChildStdout, Stdio};
use std::sync::{Arc, Mutex, mpsc as std_mpsc};
use std::thread;

use anyhow::{Context, anyhow, bail};
use chrono::Utc;
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};

use splcw_orchestrator::{ResidentSessionTransport, SessionEvent};

use super::{
    CodexCliLiveStreamState, CodexCliSessionState, CodexCliTurnRecord, OperatorPaths,
    RECENT_LIVE_STREAM_LINE_LIMIT, RunSettings, DEFAULT_MODEL, DEFAULT_OBJECTIVE,
    append_jsonl_entry, append_limited, build_codex_cli_context_prompt, build_output_command,
    codex_cli_status, execute_codex_cli_turn, normalize_text, summarize_codex_cli_reply,
    truncate_for_summary, write_json_atomic,
};

#[derive(Clone)]
pub(crate) struct ResidentCodexTransport {
    paths: OperatorPaths,
    settings: RunSettings,
    worker: Arc<Mutex<Option<ResidentWorkerHandle>>>,
}

#[derive(Clone)]
struct ResidentWorkerHandle {
    session_id: String,
    command_tx: std_mpsc::Sender<ResidentWorkerCommand>,
}

enum ResidentWorkerCommand {
    RunTurn {
        objective: String,
        grounding: String,
        reply_tx: oneshot::Sender<anyhow::Result<String>>,
    },
    Shutdown,
}

struct ResidentWorker {
    paths: OperatorPaths,
    settings: RunSettings,
    connection: AppServerConnection,
    warning_log: Arc<Mutex<Vec<String>>>,
    events: mpsc::UnboundedSender<SessionEvent>,
}

struct AppServerConnection {
    _child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    next_request_id: u64,
    thread_id: String,
}

struct ResidentTurnState {
    live_stream: CodexCliLiveStreamState,
    event_lines: Vec<String>,
    warning_lines: Vec<String>,
    full_response: String,
    warning_cursor: usize,
    completed_turn_id: Option<String>,
}

impl ResidentCodexTransport {
    pub(crate) fn new(paths: OperatorPaths, settings: RunSettings) -> Self {
        Self {
            paths,
            settings,
            worker: Arc::new(Mutex::new(None)),
        }
    }

    fn ensure_worker(
        &self,
        session_id: &str,
        events: mpsc::UnboundedSender<SessionEvent>,
    ) -> anyhow::Result<ResidentWorkerHandle> {
        let mut guard = self
            .worker
            .lock()
            .map_err(|_| anyhow!("resident worker lock poisoned"))?;
        if let Some(handle) = guard.as_ref() {
            if handle.session_id == session_id {
                return Ok(handle.clone());
            }
            let _ = handle.command_tx.send(ResidentWorkerCommand::Shutdown);
            *guard = None;
        }

        let handle = spawn_resident_worker(
            self.paths.clone(),
            self.settings.clone(),
            session_id.to_string(),
            events,
        )?;
        *guard = Some(handle.clone());
        Ok(handle)
    }

    fn clear_worker(&self) {
        if let Ok(mut guard) = self.worker.lock() {
            if let Some(handle) = guard.take() {
                let _ = handle.command_tx.send(ResidentWorkerCommand::Shutdown);
            }
        }
    }

    fn fallback_exec_turn(
        &self,
        objective: &str,
        grounding: &str,
        events: &mpsc::UnboundedSender<SessionEvent>,
    ) -> anyhow::Result<String> {
        let execution = execute_codex_cli_turn(
            &self.paths,
            &self.settings,
            objective,
            grounding,
            Some(events.clone()),
        )?;
        Ok(execution.reply)
    }
}

#[async_trait::async_trait]
impl ResidentSessionTransport for ResidentCodexTransport {
    async fn run_turn(
        &self,
        session_id: &str,
        objective: &str,
        grounding: &str,
        events: &mpsc::UnboundedSender<SessionEvent>,
    ) -> anyhow::Result<String> {
        let worker = match self.ensure_worker(session_id, events.clone()) {
            Ok(worker) => worker,
            Err(error) => return self.fallback_exec_turn(objective, grounding, events).with_context(|| {
                format!("resident app-server startup failed ({error:#}); fallback exec bridge also failed")
            }),
        };

        let (reply_tx, reply_rx) = oneshot::channel();
        if worker
            .command_tx
            .send(ResidentWorkerCommand::RunTurn {
                objective: objective.to_string(),
                grounding: grounding.to_string(),
                reply_tx,
            })
            .is_err()
        {
            self.clear_worker();
            return self.fallback_exec_turn(objective, grounding, events).context(
                "resident app-server worker exited before it could accept the turn; fallback exec bridge failed",
            );
        }

        match reply_rx.await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(error)) => {
                self.clear_worker();
                self.fallback_exec_turn(objective, grounding, events).with_context(|| {
                    format!(
                        "resident app-server turn failed ({error:#}); fallback exec bridge also failed"
                    )
                })
            }
            Err(error) => {
                self.clear_worker();
                self.fallback_exec_turn(objective, grounding, events).with_context(|| {
                    format!(
                        "resident app-server worker dropped the turn reply ({error}); fallback exec bridge also failed"
                    )
                })
            }
        }
    }
}

fn spawn_resident_worker(
    paths: OperatorPaths,
    settings: RunSettings,
    session_id: String,
    events: mpsc::UnboundedSender<SessionEvent>,
) -> anyhow::Result<ResidentWorkerHandle> {
    let (command_tx, command_rx) = std_mpsc::channel();
    let (ready_tx, ready_rx) = std_mpsc::channel();
    let handle = ResidentWorkerHandle {
        session_id: session_id.clone(),
        command_tx: command_tx.clone(),
    };

    thread::spawn(move || {
        let startup = ResidentWorker::start(paths, settings, session_id, events);
        match startup {
            Ok(mut worker) => {
                let _ = ready_tx.send(Ok(()));
                while let Ok(command) = command_rx.recv() {
                    match command {
                        ResidentWorkerCommand::RunTurn {
                            objective,
                            grounding,
                            reply_tx,
                        } => {
                            let result = worker.run_turn(&objective, &grounding);
                            let _ = reply_tx.send(result);
                        }
                        ResidentWorkerCommand::Shutdown => break,
                    }
                }
            }
            Err(error) => {
                let _ = ready_tx.send(Err(error));
            }
        }
    });

    ready_rx
        .recv()
        .map_err(|_| anyhow!("resident worker startup channel closed"))??;
    Ok(handle)
}

impl ResidentWorker {
    fn start(
        paths: OperatorPaths,
        settings: RunSettings,
        session_id: String,
        events: mpsc::UnboundedSender<SessionEvent>,
    ) -> anyhow::Result<Self> {
        let warning_log = Arc::new(Mutex::new(Vec::new()));
        let connection = AppServerConnection::start(
            &paths,
            &settings,
            &session_id,
            warning_log.clone(),
            events.clone(),
        )?;
        Ok(Self {
            paths,
            settings,
            connection,
            warning_log,
            events,
        })
    }

    fn run_turn(&mut self, objective: &str, grounding: &str) -> anyhow::Result<String> {
        let prompt = build_codex_cli_context_prompt(&self.paths, objective, Some(grounding));
        let mut turn = ResidentTurnState::new(
            normalize_text(objective, DEFAULT_OBJECTIVE),
            self.connection.thread_id.clone(),
            warning_log_len(&self.warning_log),
        );
        turn.persist(&self.paths)?;

        let request_id = self.connection.send_turn_start(
            &self.settings,
            prompt.as_str(),
            self.paths.repo_root.as_path(),
        )?;

        let mut response_received = false;
        let mut turn_id: Option<String> = None;

        loop {
            turn.drain_warnings(&self.warning_log, &self.paths)?;
            let message = self
                .connection
                .read_message()?
                .context("resident Codex app-server closed stdout unexpectedly")?;

            if let Some(id) = message.get("id").and_then(|value| value.as_u64()) {
                if id != request_id {
                    continue;
                }
                if let Some(error) = message.get("error") {
                    bail!(
                        "Codex app-server turn request failed: {}",
                        serde_json::to_string(error).unwrap_or_else(|_| error.to_string())
                    );
                }
                turn_id = message
                    .get("result")
                    .and_then(|result| result.get("turn"))
                    .and_then(|turn| turn.get("id"))
                    .and_then(|value| value.as_str())
                    .map(|value| value.to_string());
                response_received = true;
                if turn.completed_for(turn_id.as_deref()) {
                    break;
                }
                continue;
            }

            if let Some(method) = message.get("method").and_then(|value| value.as_str()) {
                turn.apply_notification(
                    method,
                    message.get("params").unwrap_or(&Value::Null),
                    turn_id.as_deref(),
                    &self.paths,
                    &self.connection.thread_id,
                    &self.events,
                )?;
                if response_received && turn.completed_for(turn_id.as_deref()) {
                    break;
                }
            }
        }

        turn.drain_warnings(&self.warning_log, &self.paths)?;
        turn.finish(
            &self.paths,
            &self.settings,
            &self.connection.thread_id,
            objective,
        )
    }
}

impl AppServerConnection {
    fn start(
        paths: &OperatorPaths,
        settings: &RunSettings,
        session_id: &str,
        warning_log: Arc<Mutex<Vec<String>>>,
        events: mpsc::UnboundedSender<SessionEvent>,
    ) -> anyhow::Result<Self> {
        let cli_status = codex_cli_status(paths);
        if !cli_status.available {
            bail!(cli_status.summary);
        }
        if !cli_status.logged_in {
            bail!(
                "Codex CLI is installed but not logged in yet. Use the Auth page to launch `codex login` first."
            );
        }
        let command_path = cli_status
            .command_path
            .as_ref()
            .context("Codex CLI command path was not recorded")?;

        let args = vec![
            "app-server".to_string(),
            "--listen".to_string(),
            "stdio://".to_string(),
        ];
        let mut command = build_output_command(command_path, &paths.repo_root, &args);
        command.stdin(Stdio::piped());
        let mut child = command.spawn().with_context(|| {
            format!(
                "spawn {} {} in {}",
                command_path.display(),
                args.join(" "),
                paths.repo_root.display()
            )
        })?;
        let stdin = child
            .stdin
            .take()
            .context("capture resident Codex app-server stdin")?;
        let stdout = child
            .stdout
            .take()
            .context("capture resident Codex app-server stdout")?;
        let stderr = child
            .stderr
            .take()
            .context("capture resident Codex app-server stderr")?;

        spawn_stderr_relay(stderr, warning_log, events);

        let mut connection = Self {
            _child: child,
            stdin,
            stdout: BufReader::new(stdout),
            next_request_id: 1,
            thread_id: String::new(),
        };

        connection.initialize()?;
        let thread_id = connection.start_thread(paths, settings, session_id)?;
        connection.thread_id = thread_id.clone();

        write_json_atomic(
            &paths.codex_cli_session_path,
            &CodexCliSessionState {
                session_id: thread_id,
                updated_at: Utc::now(),
                model: normalize_text(&settings.model, DEFAULT_MODEL),
            },
        )?;

        Ok(connection)
    }

    fn initialize(&mut self) -> anyhow::Result<()> {
        let request_id = self.send_request(
            "initialize",
            json!({
                "clientInfo": {
                    "name": "AGRO Harness Operator",
                    "version": env!("CARGO_PKG_VERSION"),
                },
                "capabilities": {
                    "experimentalApi": true,
                },
            }),
        )?;
        let _ = self.wait_for_response(request_id)?;
        Ok(())
    }

    fn start_thread(
        &mut self,
        paths: &OperatorPaths,
        settings: &RunSettings,
        _session_id: &str,
    ) -> anyhow::Result<String> {
        let developer_instructions = format!(
            "You are Codex operating inside the AGRO / AIM repo at `{}`. Treat future turn input as already-grounded operator work. Prefer existing host control, verification, memory, and orchestrator surfaces over generic repo chatter.",
            paths.repo_root.display()
        );
        let request_id = self.send_request(
            "thread/start",
            json!({
                "model": normalize_text(&settings.model, DEFAULT_MODEL),
                "cwd": paths.repo_root.display().to_string(),
                "approvalPolicy": "never",
                "sandbox": "danger-full-access",
                "developerInstructions": developer_instructions,
                "ephemeral": true,
                "experimentalRawEvents": false,
                "persistExtendedHistory": true,
            }),
        )?;
        let response = self.wait_for_response(request_id)?;
        response
            .get("thread")
            .and_then(|thread| thread.get("id"))
            .and_then(|value| value.as_str())
            .map(|value| value.to_string())
            .context("app-server thread/start did not return a thread id")
    }

    fn send_turn_start(
        &mut self,
        settings: &RunSettings,
        prompt: &str,
        cwd: &std::path::Path,
    ) -> anyhow::Result<u64> {
        self.send_request(
            "turn/start",
            json!({
                "threadId": self.thread_id,
                "input": [
                    {
                        "type": "text",
                        "text": prompt,
                        "text_elements": [],
                    }
                ],
                "cwd": cwd.display().to_string(),
                "approvalPolicy": "never",
                "sandboxPolicy": {
                    "type": "dangerFullAccess",
                },
                "model": normalize_text(&settings.model, DEFAULT_MODEL),
            }),
        )
    }

    fn send_request(&mut self, method: &str, params: Value) -> anyhow::Result<u64> {
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        let line = json!({
            "id": request_id,
            "method": method,
            "params": params,
        })
        .to_string();
        self.stdin
            .write_all(line.as_bytes())
            .with_context(|| format!("write {method} request to Codex app-server"))?;
        self.stdin
            .write_all(b"\n")
            .with_context(|| format!("newline terminate {method} request"))?;
        self.stdin
            .flush()
            .with_context(|| format!("flush {method} request to Codex app-server"))?;
        Ok(request_id)
    }

    fn wait_for_response(&mut self, request_id: u64) -> anyhow::Result<Value> {
        loop {
            let message = self
                .read_message()?
                .context("Codex app-server closed before responding")?;
            let Some(id) = message.get("id").and_then(|value| value.as_u64()) else {
                continue;
            };
            if id != request_id {
                continue;
            }
            if let Some(error) = message.get("error") {
                bail!(
                    "Codex app-server request failed: {}",
                    serde_json::to_string(error).unwrap_or_else(|_| error.to_string())
                );
            }
            return Ok(message.get("result").cloned().unwrap_or(Value::Null));
        }
    }

    fn read_message(&mut self) -> anyhow::Result<Option<Value>> {
        let mut line = String::new();
        loop {
            line.clear();
            let read = self
                .stdout
                .read_line(&mut line)
                .context("read Codex app-server stdout line")?;
            if read == 0 {
                return Ok(None);
            }
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            return serde_json::from_str(trimmed)
                .with_context(|| format!("parse Codex app-server stdout line: {trimmed}"))
                .map(Some);
        }
    }
}

impl ResidentTurnState {
    fn new(objective: String, thread_id: String, warning_cursor: usize) -> Self {
        let now = Utc::now();
        Self {
            live_stream: CodexCliLiveStreamState {
                started_at: now,
                updated_at: now,
                active: true,
                objective,
                session_id: Some(thread_id),
                latest_text: String::new(),
                event_lines: Vec::new(),
                warning_lines: Vec::new(),
            },
            event_lines: Vec::new(),
            warning_lines: Vec::new(),
            full_response: String::new(),
            warning_cursor,
            completed_turn_id: None,
        }
    }

    fn persist(&self, paths: &OperatorPaths) -> anyhow::Result<()> {
        write_json_atomic(&paths.codex_cli_live_stream_path, &self.live_stream)
    }

    fn completed_for(&self, turn_id: Option<&str>) -> bool {
        match (self.completed_turn_id.as_deref(), turn_id) {
            (Some(completed), Some(expected)) => completed == expected,
            (Some(_), None) => true,
            _ => false,
        }
    }

    fn drain_warnings(
        &mut self,
        warning_log: &Arc<Mutex<Vec<String>>>,
        paths: &OperatorPaths,
    ) -> anyhow::Result<()> {
        let warnings = warning_log
            .lock()
            .map_err(|_| anyhow!("resident warning log lock poisoned"))?
            .clone();
        while self.warning_cursor < warnings.len() {
            let line = warnings[self.warning_cursor].clone();
            self.warning_cursor += 1;
            append_limited(
                &mut self.warning_lines,
                line,
                RECENT_LIVE_STREAM_LINE_LIMIT,
            );
        }
        self.live_stream.updated_at = Utc::now();
        self.live_stream.warning_lines = self.warning_lines.clone();
        self.persist(paths)
    }

    fn apply_notification(
        &mut self,
        method: &str,
        params: &Value,
        expected_turn_id: Option<&str>,
        paths: &OperatorPaths,
        thread_id: &str,
        events: &mpsc::UnboundedSender<SessionEvent>,
    ) -> anyhow::Result<()> {
        match method {
            "thread/started" => {
                let thread_id = params
                    .get("thread")
                    .and_then(|thread| thread.get("id"))
                    .and_then(|value| value.as_str())
                    .unwrap_or(thread_id);
                self.live_stream.session_id = Some(thread_id.to_string());
                self.record_event(format!("thread.started {thread_id}"));
            }
            "thread/status/changed" => {
                if let Some(status) = params
                    .get("status")
                    .and_then(|status| status.get("type"))
                    .and_then(|value| value.as_str())
                {
                    self.record_event(format!("thread.status {status}"));
                }
            }
            "turn/started" => {
                self.record_event("turn.started".into());
            }
            "item/started" => {
                let item = params.get("item").unwrap_or(&Value::Null);
                let item_type = item
                    .get("type")
                    .and_then(|value| value.as_str())
                    .unwrap_or("unknown");
                self.record_event(format!("item.started {item_type}"));
                match item_type {
                    "commandExecution" => {
                        let command = item
                            .get("command")
                            .and_then(|value| value.as_str())
                            .unwrap_or("commandExecution")
                            .to_string();
                        let _ = events.send(SessionEvent::ToolUse {
                            tool: command,
                            input: item.to_string(),
                        });
                    }
                    "mcpToolCall" => {
                        let tool = item
                            .get("tool")
                            .and_then(|value| value.as_str())
                            .unwrap_or("mcpToolCall")
                            .to_string();
                        let _ = events.send(SessionEvent::ToolUse {
                            tool,
                            input: item.to_string(),
                        });
                    }
                    "dynamicToolCall" => {
                        let tool = item
                            .get("tool")
                            .and_then(|value| value.as_str())
                            .unwrap_or("dynamicToolCall")
                            .to_string();
                        let _ = events.send(SessionEvent::ToolUse {
                            tool,
                            input: item.to_string(),
                        });
                    }
                    _ => {}
                }
            }
            "item/agentMessage/delta" => {
                if let Some(delta) = params.get("delta").and_then(|value| value.as_str()) {
                    self.full_response.push_str(delta);
                    self.live_stream.latest_text = self.full_response.clone();
                    if !delta.is_empty() {
                        let _ = events.send(SessionEvent::Chunk {
                            delta: delta.to_string(),
                        });
                    }
                }
            }
            "item/completed" => {
                let item = params.get("item").unwrap_or(&Value::Null);
                match item.get("type").and_then(|value| value.as_str()) {
                    Some("agentMessage") => {
                        if let Some(text) = item.get("text").and_then(|value| value.as_str()) {
                            self.full_response = text.to_string();
                            self.live_stream.latest_text = self.full_response.clone();
                            self.record_event(format!(
                                "agent_message {}",
                                truncate_for_summary(text, 120)
                            ));
                        }
                    }
                    Some("commandExecution") => {
                        let command = item
                            .get("command")
                            .and_then(|value| value.as_str())
                            .unwrap_or("commandExecution");
                        self.record_event(format!(
                            "command_execution {}",
                            truncate_for_summary(command, 120)
                        ));
                        let _ = events.send(SessionEvent::ToolResult {
                            output: item.to_string(),
                        });
                    }
                    Some("mcpToolCall") | Some("dynamicToolCall") => {
                        let other = item
                            .get("type")
                            .and_then(|value| value.as_str())
                            .unwrap_or("tool");
                        self.record_event(format!("item.completed {other}"));
                        let _ = events.send(SessionEvent::ToolResult {
                            output: item.to_string(),
                        });
                    }
                    Some(other) => {
                        self.record_event(format!("item.completed {other}"));
                    }
                    None => {}
                }
            }
            "turn/completed" => {
                let turn_id = params
                    .get("turn")
                    .and_then(|turn| turn.get("id"))
                    .and_then(|value| value.as_str())
                    .map(|value| value.to_string());
                if expected_turn_id.is_none() || turn_id.as_deref() == expected_turn_id {
                    self.completed_turn_id = turn_id;
                }
                self.record_event("turn.completed".into());
            }
            "configWarning" => {
                if let Some(message) = params.get("message").and_then(|value| value.as_str()) {
                    let warning = format!("config warning: {message}");
                    append_limited(
                        &mut self.warning_lines,
                        warning.clone(),
                        RECENT_LIVE_STREAM_LINE_LIMIT,
                    );
                    let _ = events.send(SessionEvent::ContradictionDetected {
                        contradiction: warning,
                    });
                }
            }
            "windows/worldWritableWarning" => {
                let warning = "windows warning: world-writable path detected".to_string();
                append_limited(
                    &mut self.warning_lines,
                    warning.clone(),
                    RECENT_LIVE_STREAM_LINE_LIMIT,
                );
                let _ = events.send(SessionEvent::ContradictionDetected {
                    contradiction: warning,
                });
            }
            "error" => {
                let warning = format!(
                    "server error: {}",
                    serde_json::to_string(params).unwrap_or_else(|_| params.to_string())
                );
                append_limited(
                    &mut self.warning_lines,
                    warning.clone(),
                    RECENT_LIVE_STREAM_LINE_LIMIT,
                );
                let _ = events.send(SessionEvent::ContradictionDetected {
                    contradiction: warning,
                });
            }
            _ => {}
        }

        self.live_stream.updated_at = Utc::now();
        self.live_stream.event_lines = self.event_lines.clone();
        self.live_stream.warning_lines = self.warning_lines.clone();
        self.persist(paths)
    }

    fn finish(
        &mut self,
        paths: &OperatorPaths,
        settings: &RunSettings,
        thread_id: &str,
        objective: &str,
    ) -> anyhow::Result<String> {
        self.live_stream.active = false;
        self.live_stream.updated_at = Utc::now();
        if self.full_response.trim().is_empty() {
            self.full_response = "Codex app-server completed without a visible reply.".into();
            self.live_stream.latest_text = self.full_response.clone();
        }
        self.persist(paths)?;

        write_json_atomic(
            &paths.codex_cli_session_path,
            &CodexCliSessionState {
                session_id: thread_id.to_string(),
                updated_at: Utc::now(),
                model: normalize_text(&settings.model, DEFAULT_MODEL),
            },
        )?;

        let summary = if let Some(first_warning) = self.warning_lines.first() {
            format!(
                "{} | warning: {}",
                summarize_codex_cli_reply(&self.full_response),
                truncate_for_summary(first_warning, 120)
            )
        } else {
            summarize_codex_cli_reply(&self.full_response)
        };

        let session_dir = paths.session_root.join(&paths.session_id);
        std::fs::create_dir_all(&session_dir)
            .with_context(|| format!("create session dir {}", session_dir.display()))?;
        append_jsonl_entry(
            &session_dir.join("codex-cli-turn-log.jsonl"),
            &CodexCliTurnRecord {
                recorded_at: Utc::now(),
                session_id: Some(thread_id.to_string()),
                model: normalize_text(&settings.model, DEFAULT_MODEL),
                objective: normalize_text(objective, DEFAULT_OBJECTIVE),
                reply: self.full_response.clone(),
                summary,
                event_lines: self.event_lines.clone(),
                warning_lines: self.warning_lines.clone(),
            },
        )?;

        Ok(self.full_response.clone())
    }

    fn record_event(&mut self, line: String) {
        append_limited(
            &mut self.event_lines,
            line,
            RECENT_LIVE_STREAM_LINE_LIMIT,
        );
    }
}

fn warning_log_len(warning_log: &Arc<Mutex<Vec<String>>>) -> usize {
    warning_log.lock().map(|guard| guard.len()).unwrap_or_default()
}

fn spawn_stderr_relay(
    stderr: ChildStderr,
    warning_log: Arc<Mutex<Vec<String>>>,
    events: mpsc::UnboundedSender<SessionEvent>,
) {
    thread::spawn(move || {
        let mut reader = BufReader::new(stderr);
        let mut line = String::new();
        loop {
            line.clear();
            let read = match reader.read_line(&mut line) {
                Ok(read) => read,
                Err(_) => break,
            };
            if read == 0 {
                break;
            }
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            if let Ok(mut warnings) = warning_log.lock() {
                warnings.push(trimmed.to_string());
            }
            let _ = events.send(SessionEvent::ContradictionDetected {
                contradiction: trimmed.to_string(),
            });
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires a logged-in local Codex CLI app-server"]
    async fn resident_codex_transport_runs_real_app_server_turn() -> anyhow::Result<()> {
        let paths = OperatorPaths::discover()?;
        let settings = RunSettings::default();
        let transport = ResidentCodexTransport::new(paths, settings);
        let (event_tx, _event_rx) = mpsc::unbounded_channel();

        let reply = transport
            .run_turn(
                "resident-transport-test",
                "Reply with exactly APP_SERVER_RESIDENT_OK.",
                "## Test Grounding\nReturn exactly APP_SERVER_RESIDENT_OK.",
                &event_tx,
            )
            .await?;

        assert!(reply.contains("APP_SERVER_RESIDENT_OK"));
        Ok(())
    }
}
