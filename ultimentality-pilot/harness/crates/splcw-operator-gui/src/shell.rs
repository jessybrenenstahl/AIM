use super::*;
use gpui::{
    AnyElement, App, Context, Entity, Hsla, InteractiveElement as _, IntoElement, KeyBinding,
    ParentElement as _, Render, SharedString, Styled as _, Timer, Window, actions, div, px, rems,
};
use gpui_component::{
    ActiveTheme, Disableable as _, StyledExt as _, h_flex,
    input::{Input, InputState},
    scroll::ScrollableElement as _,
    sidebar::{Sidebar, SidebarMenu, SidebarMenuItem},
    v_flex,
};
use gpui_component::{button::Button, button::ButtonVariants as _};

const UI_POLL_INTERVAL: Duration = Duration::from_millis(250);
const NATIVE_CODEX_ENGINE_LABEL: &str = "Native Codex runtime";
const NATIVE_CODEX_ENGINE_SUMMARY: &str =
    "This operator shell is a harness-native Codex client, not a wrapped Codex CLI session.";
const OPENAI_CODEX_ENDPOINT: &str = "https://chatgpt.com/backend-api/codex/responses";
const OPENAI_API_ENDPOINT: &str = "https://api.openai.com/v1/responses";
const SHELL_CONTEXT: &str = "OperatorShell";
const ZOOM_MIN: f32 = 0.70;
const ZOOM_MAX: f32 = 1.65;
const ZOOM_STEP: f32 = 0.1;
const DOC_SURFACE_MIN_REM: f32 = 12.0;

#[derive(Clone, Copy)]
enum DocumentSurfaceMode {
    Fit,
}

actions!(operator_shell, [ZoomIn, ZoomOut, ResetZoom]);

pub(crate) fn init(cx: &mut App) {
    cx.bind_keys([
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd-=", ZoomIn, None),
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd-+", ZoomIn, None),
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd-shift-=", ZoomIn, None),
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd--", ZoomOut, None),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("cmd-_", ZoomOut, None),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("cmd-0", ResetZoom, None),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-=", ZoomIn, None),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-+", ZoomIn, None),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-shift-=", ZoomIn, None),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl--", ZoomOut, None),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-_", ZoomOut, None),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-0", ResetZoom, None),
    ]);
}

fn shell_bg() -> Hsla {
    gpui::rgb(0x07111f).into()
}

fn shell_panel() -> Hsla {
    gpui::rgb(0x0f1726).into()
}

fn shell_panel_elevated() -> Hsla {
    gpui::rgb(0x111c2d).into()
}

fn shell_border() -> Hsla {
    gpui::rgb(0x223248).into()
}

fn shell_text() -> Hsla {
    gpui::rgb(0xe7edf6).into()
}

fn shell_muted_text() -> Hsla {
    gpui::rgb(0x91a0b6).into()
}

fn shell_chip_bg() -> Hsla {
    gpui::rgb(0x182436).into()
}

fn shell_chip_border() -> Hsla {
    gpui::rgb(0x28374b).into()
}

fn simplify_document_text(text: &str) -> String {
    let plain = text.replace("**", "").replace('`', "");
    plain
        .split_whitespace()
        .map(|token| wrap_long_token(token, 28))
        .collect::<Vec<_>>()
        .join(" ")
}

fn wrap_long_token(token: &str, max_chars: usize) -> String {
    if token.chars().count() <= max_chars {
        return token.to_string();
    }

    let mut wrapped = String::new();
    let mut current = String::new();
    let mut current_len = 0usize;
    for ch in token.chars() {
        current.push(ch);
        current_len += 1;
        let soft_break = matches!(ch, '/' | '\\' | '_' | '-' | '.' | ':' | '=' | '?' | '&');
        if current_len >= max_chars || soft_break {
            wrapped.push_str(&current);
            wrapped.push(' ');
            current.clear();
            current_len = 0;
        }
    }

    if !current.is_empty() {
        wrapped.push_str(&current);
    }

    wrapped.trim_end().to_string()
}

fn render_document_line(line: &str) -> AnyElement {
    let trimmed = line.trim_end();
    if trimmed.trim().is_empty() {
        return div().h(rems(0.75)).into_any_element();
    }

    if let Some(text) = trimmed.strip_prefix("# ") {
        return div()
            .w_full()
            .min_w_0()
            .overflow_hidden()
            .text_lg()
            .font_semibold()
            .text_color(shell_text())
            .whitespace_normal()
            .child(simplify_document_text(text))
            .into_any_element();
    }

    if let Some(text) = trimmed.strip_prefix("## ") {
        return div()
            .w_full()
            .min_w_0()
            .overflow_hidden()
            .text_base()
            .font_semibold()
            .text_color(shell_text())
            .whitespace_normal()
            .child(simplify_document_text(text))
            .into_any_element();
    }

    if let Some(text) = trimmed.strip_prefix("- ") {
        return h_flex()
            .w_full()
            .min_w_0()
            .items_start()
            .gap_2()
            .child(
                div()
                    .pt_0p5()
                    .text_color(gpui::rgb(0x6eb6ff))
                    .child("•"),
            )
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .overflow_hidden()
                    .text_sm()
                    .text_color(shell_text())
                    .whitespace_normal()
                    .child(simplify_document_text(text)),
            )
            .into_any_element();
    }

    div()
        .w_full()
        .min_w_0()
        .overflow_hidden()
        .text_sm()
        .text_color(shell_text())
        .whitespace_normal()
        .child(simplify_document_text(trimmed))
        .into_any_element()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum OperatorPanel {
    Operate,
    Auth,
    Background,
    Github,
    Artifacts,
    Activity,
}

impl OperatorPanel {
    fn title(self) -> &'static str {
        match self {
            Self::Operate => "Operate",
            Self::Auth => "Auth",
            Self::Background => "Background",
            Self::Github => "GitHub",
            Self::Artifacts => "Artifacts",
            Self::Activity => "Activity",
        }
    }

    fn description(self) -> &'static str {
        match self {
            Self::Operate => {
                "Launch bounded turns, inspect runtime identity, and keep the live session understandable."
            }
            Self::Auth => {
                "Manage native Codex auth, inspect pending OAuth, and verify the selected provider is truly ready."
            }
            Self::Background => {
                "Own detached runner state, handoffs, crash recovery, and reattach behavior from one place."
            }
            Self::Github => {
                "Review supervised GitHub requests with enough detail to approve, reject, or steer them safely."
            }
            Self::Artifacts => {
                "Read the current brief, plan, gaps, and handoff as full documents instead of truncated snippets."
            }
            Self::Activity => {
                "Inspect recent turns, events, and status details so the harness is legible, not opaque."
            }
        }
    }
}

pub(crate) struct OperatorShell {
    app: OperatorApp,
    selected_panel: OperatorPanel,
    initialized: bool,
    zoom_scale: f32,
    objective_input: Entity<InputState>,
    model_input: Entity<InputState>,
    thread_id_input: Entity<InputState>,
    thread_label_input: Entity<InputState>,
    loop_pause_input: Entity<InputState>,
    auth_label_input: Entity<InputState>,
    auth_callback_input: Entity<InputState>,
    github_target_input: Entity<InputState>,
}

impl OperatorShell {
    pub(crate) fn new(
        controller: Arc<HarnessController>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let app = OperatorApp::new(controller);
        let objective_input = cx.new(|cx| {
            InputState::new(window, cx)
                .multi_line(true)
                .rows(10)
                .default_value(app.settings.objective.clone())
        });
        let model_input =
            cx.new(|cx| InputState::new(window, cx).default_value(app.settings.model.clone()));
        let thread_id_input =
            cx.new(|cx| InputState::new(window, cx).default_value(app.settings.thread_id.clone()));
        let thread_label_input = cx
            .new(|cx| InputState::new(window, cx).default_value(app.settings.thread_label.clone()));
        let loop_pause_input = cx.new(|cx| {
            InputState::new(window, cx).default_value(format!("{:.1}", app.loop_pause_seconds))
        });
        let auth_label_input =
            cx.new(|cx| InputState::new(window, cx).default_value(app.auth_label.clone()));
        let auth_callback_input = cx.new(|cx| {
            InputState::new(window, cx)
                .multi_line(true)
                .rows(5)
                .default_value(app.auth_callback_input.clone())
        });
        let github_target_input =
            cx.new(|cx| InputState::new(window, cx).default_value(app.github_target_input.clone()));

        Self {
            app,
            selected_panel: OperatorPanel::Operate,
            initialized: false,
            zoom_scale: 1.0,
            objective_input,
            model_input,
            thread_id_input,
            thread_label_input,
            loop_pause_input,
            auth_label_input,
            auth_callback_input,
            github_target_input,
        }
    }

    fn ensure_started(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.initialized {
            return;
        }
        self.initialized = true;
        self.app.spawn_refresh(
            "idle".into(),
            OperatorRunMode::Idle,
            "operator launched".into(),
            None,
            0,
            None,
        );
        self.start_refresh_loop(window, cx);
    }

    fn start_refresh_loop(&mut self, window: &Window, cx: &mut Context<Self>) {
        cx.spawn_in(window, async move |view, cx| {
            loop {
                Timer::after(UI_POLL_INTERVAL).await;
                if view.update(cx, |this, cx| this.tick(cx)).is_err() {
                    break;
                }
            }
        })
        .detach();
    }

    fn tick(&mut self, cx: &mut Context<Self>) {
        if self.app.last_refresh.elapsed() >= REFRESH_INTERVAL {
            let snapshot = self.snapshot();
            self.app.spawn_refresh(
                snapshot.run_state,
                parse_run_mode(snapshot.run_mode.as_str()),
                snapshot.summary,
                snapshot.last_error,
                snapshot.completed_turn_count,
                snapshot.auth_notice,
            );
        }
        cx.notify();
    }

    fn zoom_in(&mut self, _: &ZoomIn, _: &mut Window, cx: &mut Context<Self>) {
        self.zoom_scale = (self.zoom_scale + ZOOM_STEP).min(ZOOM_MAX);
        cx.notify();
    }

    fn zoom_out(&mut self, _: &ZoomOut, _: &mut Window, cx: &mut Context<Self>) {
        self.zoom_scale = (self.zoom_scale - ZOOM_STEP).max(ZOOM_MIN);
        cx.notify();
    }

    fn reset_zoom(&mut self, _: &ResetZoom, _: &mut Window, cx: &mut Context<Self>) {
        self.zoom_scale = 1.0;
        cx.notify();
    }

    fn zoom_percent(&self) -> i32 {
        (self.zoom_scale * 100.0).round() as i32
    }

    fn snapshot(&self) -> OperatorSnapshot {
        self.app
            .snapshot
            .lock()
            .expect("operator snapshot poisoned")
            .clone()
    }

    fn sync_form_into_state(&mut self, cx: &mut Context<Self>) {
        self.app.settings.objective = self.objective_input.read(cx).value().to_string();
        self.app.settings.model = self.model_input.read(cx).value().to_string();
        self.app.settings.thread_id = self.thread_id_input.read(cx).value().to_string();
        self.app.settings.thread_label = self.thread_label_input.read(cx).value().to_string();
        self.app.auth_label = self.auth_label_input.read(cx).value().to_string();
        self.app.auth_callback_input = self.auth_callback_input.read(cx).value().to_string();
        self.app.github_target_input = self.github_target_input.read(cx).value().to_string();
        if let Ok(value) = self.loop_pause_input.read(cx).value().trim().parse::<f32>() {
            self.app.loop_pause_seconds = value.max(0.0);
        }
    }

    fn sync_state_into_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        set_input_value(
            &self.objective_input,
            self.app.settings.objective.clone(),
            window,
            cx,
        );
        set_input_value(
            &self.model_input,
            self.app.settings.model.clone(),
            window,
            cx,
        );
        set_input_value(
            &self.thread_id_input,
            self.app.settings.thread_id.clone(),
            window,
            cx,
        );
        set_input_value(
            &self.thread_label_input,
            self.app.settings.thread_label.clone(),
            window,
            cx,
        );
        set_input_value(
            &self.loop_pause_input,
            format!("{:.1}", self.app.loop_pause_seconds),
            window,
            cx,
        );
        set_input_value(
            &self.auth_label_input,
            self.app.auth_label.clone(),
            window,
            cx,
        );
        set_input_value(
            &self.auth_callback_input,
            self.app.auth_callback_input.clone(),
            window,
            cx,
        );
        set_input_value(
            &self.github_target_input,
            self.app.github_target_input.clone(),
            window,
            cx,
        );
    }

    fn begin_run(&mut self, run_mode: OperatorRunMode, cx: &mut Context<Self>) {
        self.sync_form_into_state(cx);
        self.app.begin_run(run_mode);
        cx.notify();
    }

    fn select_auth_provider(&mut self, provider: OperatorAuthProvider, cx: &mut Context<Self>) {
        self.app.auth_provider = provider;
        cx.notify();
    }

    fn adopt_background_settings(
        &mut self,
        snapshot: &OperatorSnapshot,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.app.adopt_background_settings(snapshot);
        self.sync_state_into_form(window, cx);
        cx.notify();
    }

    fn adopt_background_handoff(
        &mut self,
        snapshot: &OperatorSnapshot,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.app.adopt_background_handoff(snapshot);
        self.sync_state_into_form(window, cx);
        cx.notify();
    }

    fn pick_github_target(&mut self, number: u64, window: &mut Window, cx: &mut Context<Self>) {
        self.app.github_target_input = number.to_string();
        set_input_value(
            &self.github_target_input,
            self.app.github_target_input.clone(),
            window,
            cx,
        );
        cx.notify();
    }

    fn render_header(
        &self,
        snapshot: &OperatorSnapshot,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let provider = self.app.auth_provider;
        let background_active = snapshot.background_runner_active;
        let auth_ready = snapshot.auth_ready;

        let mut pills = vec![
            status_pill(
                NATIVE_CODEX_ENGINE_LABEL,
                cx.theme().secondary,
                cx.theme().secondary_foreground,
            ),
            status_pill(
                provider.as_label(),
                cx.theme().info,
                cx.theme().info_foreground,
            ),
            status_pill(
                if auth_ready {
                    "Auth Ready"
                } else {
                    "Auth Blocked"
                },
                if auth_ready {
                    cx.theme().success
                } else {
                    cx.theme().danger
                },
                if auth_ready {
                    cx.theme().success_foreground
                } else {
                    cx.theme().danger_foreground
                },
            ),
            status_pill(
                if background_active {
                    "Background Active"
                } else {
                    "Foreground Only"
                },
                if background_active {
                    cx.theme().warning
                } else {
                    cx.theme().muted
                },
                if background_active {
                    cx.theme().warning_foreground
                } else {
                    cx.theme().foreground
                },
            ),
        ];
        if snapshot.github_action_pending {
            pills.push(status_pill(
                "GitHub Review Pending",
                cx.theme().warning,
                cx.theme().warning_foreground,
            ));
        }

        v_flex()
            .gap_4()
            .w_full()
            .p_5()
            .rounded_lg()
            .border_1()
            .border_color(shell_border())
            .bg(shell_panel_elevated())
            .child(
                h_flex()
                    .items_start()
                    .justify_between()
                    .gap_6()
                    .child(
                        v_flex()
                            .gap_2()
                            .child(
                                div()
                                    .text_xs()
                                    .font_semibold()
                                    .text_color(gpui::rgb(0x6eb6ff))
                                    .child("NATIVE OPERATOR SHELL"),
                            )
                            .child(
                                div()
                                    .text_2xl()
                                    .font_semibold()
                                    .text_color(shell_text())
                                    .child("AGRO Harness Operator"),
                            )
                            .child(
                                div()
                                    .text_sm()
                                    .text_color(shell_muted_text())
                                    .child(self.selected_panel.description()),
                            ),
                    )
                    .child(h_flex().gap_2().flex_wrap().justify_end().children(pills)),
            )
            .child(
                h_flex().gap_3().flex_wrap().children([
                    metadata_chip("Run state", &snapshot.run_state, cx),
                    metadata_chip("Mode", &snapshot.run_mode, cx),
                    metadata_chip(
                        "Refreshed",
                        snapshot
                            .refreshed_at
                            .map(|value| value.to_rfc3339())
                            .as_deref()
                            .unwrap_or("not yet"),
                        cx,
                    ),
                    metadata_chip(
                        "Completed turns",
                        &snapshot.completed_turn_count.to_string(),
                        cx,
                    ),
                    metadata_chip("Zoom", &format!("{}%", self.zoom_percent()), cx),
                ]),
            )
            .child(
                h_flex()
                    .justify_between()
                    .items_center()
                    .gap_3()
                    .child(
                        div()
                            .text_xs()
                            .text_color(shell_muted_text())
                            .child("Use Ctrl+=, Ctrl+-, and Ctrl+0 to scale the shell."),
                    )
                    .child(
                        h_flex()
                            .gap_2()
                            .child(Button::new("zoom-out").label("A-").on_click(cx.listener(
                                |this, _, window, cx| {
                                    this.zoom_out(&ZoomOut, window, cx);
                                },
                            )))
                            .child(
                                Button::new("zoom-reset")
                                    .label(format!("{}%", self.zoom_percent()))
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.reset_zoom(&ResetZoom, window, cx);
                                    })),
                            )
                            .child(Button::new("zoom-in").label("A+").primary().on_click(
                                cx.listener(|this, _, window, cx| {
                                    this.zoom_in(&ZoomIn, window, cx);
                                }),
                            )),
                    ),
            )
    }

    fn render_sidebar(
        &self,
        snapshot: &OperatorSnapshot,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let mut items = vec![
            self.nav_item(OperatorPanel::Operate, None, cx),
            self.nav_item(OperatorPanel::Auth, Some(snapshot.pending_oauth.len()), cx),
            self.nav_item(
                OperatorPanel::Background,
                snapshot
                    .background_runner_active
                    .then_some(snapshot.background_runner_turn_count.unwrap_or_default() as usize),
                cx,
            ),
            self.nav_item(OperatorPanel::Artifacts, None, cx),
            self.nav_item(
                OperatorPanel::Activity,
                Some(snapshot.recent_events.len() + snapshot.recent_turns.len()),
                cx,
            ),
        ];
        if snapshot.github_action_pending {
            items.insert(
                3,
                self.nav_item(
                    OperatorPanel::Github,
                    snapshot.github_action_pending.then_some(1),
                    cx,
                ),
            );
        }

        Sidebar::left()
            .collapsible(false)
            .header(
                v_flex()
                    .gap_2()
                    .w_full()
                    .child(
                        div()
                            .text_sm()
                            .font_semibold()
                            .text_color(shell_text())
                            .child("Control Surface"),
                    )
                    .child(
                        div().text_xs().text_color(shell_muted_text()).child(
                            "Readable, scrollable, and explicit about what engine is running.",
                        ),
                    ),
            )
            .footer(
                v_flex()
                    .gap_1()
                    .w_full()
                    .child(
                        div()
                            .text_xs()
                            .text_color(shell_muted_text())
                            .child(format!(
                                "repo: {}",
                                self.app.controller.paths.repo_root.display()
                            )),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(shell_muted_text())
                            .child(format!(
                                "status: {}",
                                self.app.controller.paths.status_path.display()
                            )),
                    ),
            )
            .child(SidebarMenu::new().children(items))
    }

    fn nav_item(
        &self,
        panel: OperatorPanel,
        badge_count: Option<usize>,
        cx: &mut Context<Self>,
    ) -> SidebarMenuItem {
        let label = badge_count
            .filter(|count| *count > 0)
            .map(|count| format!("{} ({count})", panel.title()))
            .unwrap_or_else(|| panel.title().to_string());
        SidebarMenuItem::new(label)
            .active(self.selected_panel == panel)
            .on_click(cx.listener(move |this, _, _, cx| {
                this.selected_panel = panel;
                cx.notify();
            }))
    }

    fn render_panel(
        &mut self,
        snapshot: &OperatorSnapshot,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        if self.selected_panel == OperatorPanel::Github && !snapshot.github_action_pending {
            self.selected_panel = OperatorPanel::Operate;
        }
        match self.selected_panel {
            OperatorPanel::Operate => self.render_operate_panel(snapshot, window, cx),
            OperatorPanel::Auth => self.render_auth_panel(snapshot, window, cx),
            OperatorPanel::Background => self.render_background_panel(snapshot, window, cx),
            OperatorPanel::Github => self.render_github_panel(snapshot, window, cx),
            OperatorPanel::Artifacts => self.render_artifacts_panel(snapshot, window, cx),
            OperatorPanel::Activity => self.render_activity_panel(snapshot, window, cx),
        }
    }

    fn render_operate_panel(
        &mut self,
        snapshot: &OperatorSnapshot,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let auth_busy = self.app.auth_working.load(Ordering::SeqCst);
        let background_active = snapshot.background_runner_active;
        let can_run = snapshot.auth_ready
            && !self.app.running.load(Ordering::SeqCst)
            && !auth_busy
            && !background_active;
        let background_recorded = snapshot.background_runner_id.is_some();
        let background_owned_by_this_shell =
            self.app.background_runner_owned_by_this_shell(snapshot);
        let background_settings_match =
            background_recorded && self.app.background_settings_match_form(snapshot);
        let handoff_ready = snapshot.background_handoff_ready;
        let handoff_pending = snapshot.background_handoff_pending;
        let handoff_settings_match =
            handoff_pending && self.app.handoff_settings_match_form(snapshot);
        let provider_retry_needed = provider_retry_needed(snapshot);
        let provider_recovery_surface = if provider_retry_needed {
            empty_state(
                "Provider step needs recovery",
                "Auth is already complete. The last bounded attempt failed before the next provider/model step finished. Retry the turn, or run Auth Preflight if you want to re-check the provider session first.",
                cx,
            )
        } else {
            div().into_any_element()
        };

        page_scroll(
            v_flex()
                .gap_4()
                .child(
                    h_flex()
                        .gap_4()
                        .items_start()
                        .child(
                            div()
                                .flex_1()
                                .min_w_0()
                                .child(card(
                                    "Harnessed Model",
                                    Some(
                                        "This is the live proof that the model is authenticated, callable, and driving bounded runtime turns.",
                                    ),
                                    document_surface(
                                        "operate-harnessed-model",
                                        build_harnessed_model_markdown(
                                            self.app.auth_provider,
                                            snapshot,
                                            &self.app.settings,
                                        ),
                                        self.zoom_scale,
                                        10.0,
                                        Some(20.0),
                                        DocumentSurfaceMode::Fit,
                                        window,
                                        cx,
                                    ),
                                ))
                                .child(card(
                                    "Objective",
                                    Some(
                                        "This is the primary task contract for the next bounded turn or loop.",
                                    ),
                                    v_flex()
                                        .gap_3()
                                        .child(Input::new(&self.objective_input).h(px(220.0)))
                                        .child(
                                            h_flex()
                                                .gap_3()
                                                .items_start()
                                                .flex_wrap()
                                                .children([
                                                    labeled_input("Model", Input::new(&self.model_input)),
                                                    labeled_input(
                                                        "Thread Id",
                                                        Input::new(&self.thread_id_input),
                                                    ),
                                                    labeled_input(
                                                        "Thread Label",
                                                        Input::new(&self.thread_label_input),
                                                    ),
                                                    labeled_input(
                                                        "Loop Pause (s)",
                                                        Input::new(&self.loop_pause_input),
                                                    ),
                                                ]),
                                        )
                                        .child(
                                            h_flex()
                                                .gap_2()
                                                .flex_wrap()
                                                .children([
                                                    Button::new("refresh")
                                                        .label("Refresh")
                                                        .on_click(cx.listener(|this, _, _, cx| {
                                                            let snapshot = this.snapshot();
                                                            this.app.spawn_refresh(
                                                                snapshot.run_state,
                                                                parse_run_mode(snapshot.run_mode.as_str()),
                                                                snapshot.summary,
                                                                snapshot.last_error,
                                                                snapshot.completed_turn_count,
                                                                snapshot.auth_notice,
                                                            );
                                                            cx.notify();
                                                        }))
                                                        .into_any_element(),
                                                    Button::new("run-turn")
                                                        .primary()
                                                        .label("Run Turn")
                                                        .disabled(!can_run)
                                                        .on_click(cx.listener(|this, _, _, cx| {
                                                            this.begin_run(OperatorRunMode::SingleTurn, cx);
                                                        }))
                                                        .into_any_element(),
                                                    Button::new("start-loop")
                                                        .label("Start Loop")
                                                        .disabled(!can_run)
                                                        .on_click(cx.listener(|this, _, _, cx| {
                                                            this.begin_run(OperatorRunMode::Continuous, cx);
                                                        }))
                                                        .into_any_element(),
                                                    Button::new("stop-loop")
                                                        .warning()
                                                        .label("Stop Loop")
                                                        .disabled(!this_loop_requested(&self.app.loop_requested))
                                                        .on_click(cx.listener(|this, _, _, cx| {
                                                            this.app.stop_loop();
                                                            cx.notify();
                                                        }))
                                                        .into_any_element(),
                                                    Button::new("start-background-loop")
                                                        .label("Start Background Loop")
                                                        .disabled(!can_run)
                                                        .on_click(cx.listener(|this, _, _, cx| {
                                                            this.sync_form_into_state(cx);
                                                            this.app.spawn_background_loop();
                                                            cx.notify();
                                                        }))
                                                        .into_any_element(),
                                                    Button::new("stop-background-loop")
                                                        .warning()
                                                        .label("Stop Background")
                                                        .disabled(
                                                            !(background_active
                                                                && background_owned_by_this_shell
                                                                && !auth_busy),
                                                        )
                                                        .on_click(cx.listener(|this, _, _, cx| {
                                                            this.app.spawn_stop_background_loop();
                                                            cx.notify();
                                                        }))
                                                        .into_any_element(),
                                                ]),
                                        ),
                                )),
                        )
                        .child(
                            div()
                                .w(px(420.0))
                                .flex_none()
                                .child(card(
                                    "Engine Identity",
                                    Some(
                                        "This is the proof panel for what engine is actually driving the harness.",
                                    ),
                                    document_surface(
                                        "engine-identity",
                                        build_engine_identity_markdown(
                                            self.app.auth_provider,
                                            snapshot,
                                            &self.app.settings,
                                        ),
                                        self.zoom_scale,
                                        10.0,
                                        Some(18.0),
                                        DocumentSurfaceMode::Fit,
                                        window,
                                        cx,
                                    ),
                                )),
                        ),
                )
                .child(card(
                    "Live Status",
                    Some("This is the operator-readable snapshot of what the runtime is doing right now."),
                    document_surface(
                        "operate-live-status",
                        build_operate_status_markdown(snapshot),
                        self.zoom_scale,
                        10.0,
                        None,
                        DocumentSurfaceMode::Fit,
                        window,
                        cx,
                    ),
                ))
                .child(card(
                    "Control Follow-through",
                    Some("Recovery and handoff actions stay close to the launch controls instead of hiding in a debug view."),
                    v_flex()
                        .gap_3()
                        .child(provider_recovery_surface)
                        .child(
                            h_flex()
                                .gap_2()
                                .flex_wrap()
                                .children([
                                    Button::new("operate-auth-preflight")
                                        .label("Auth Preflight")
                                        .disabled(auth_busy || background_active)
                                        .on_click(cx.listener(|this, _, _, cx| {
                                            this.sync_form_into_state(cx);
                                            this.app.spawn_auth_preflight();
                                            cx.notify();
                                        }))
                                        .into_any_element(),
                                    optional_button(
                                        provider_retry_needed.then_some("Retry Turn".to_string()),
                                        can_run,
                                        "retry-provider-turn",
                                        cx.listener(|this, _, _, cx| {
                                            this.begin_run(OperatorRunMode::SingleTurn, cx);
                                        }),
                                    ),
                                    optional_button(
                                        background_recovery_action_label(snapshot).map(str::to_string),
                                        !background_active && !auth_busy,
                                        "background-recovery",
                                        cx.listener(|this, _, window, cx| {
                                            let snapshot = this.snapshot();
                                            this.adopt_background_settings(&snapshot, window, cx);
                                            this.sync_form_into_state(cx);
                                            this.app.spawn_background_loop();
                                            cx.notify();
                                        }),
                                    ),
                                    optional_button(
                                        background_recorded.then_some("Clear Background State".to_string()),
                                        background_recorded && !auth_busy,
                                        "clear-background-state",
                                        cx.listener(|this, _, _, cx| {
                                            this.app.spawn_clear_background_state();
                                            cx.notify();
                                        }),
                                    ),
                                    optional_button(
                                        (background_active
                                            && background_owned_by_this_shell
                                            && !background_settings_match
                                            && !auth_busy)
                                            .then_some("Request Handoff".to_string()),
                                        background_active
                                            && background_owned_by_this_shell
                                            && !background_settings_match
                                            && !auth_busy,
                                        "request-background-handoff",
                                        cx.listener(|this, _, _, cx| {
                                            this.sync_form_into_state(cx);
                                            this.app.spawn_request_background_handoff();
                                            cx.notify();
                                        }),
                                    ),
                                    optional_button(
                                        (handoff_pending && !handoff_settings_match)
                                            .then_some("Adopt Handoff Settings".to_string()),
                                        handoff_pending && !handoff_settings_match,
                                        "adopt-background-handoff",
                                        cx.listener(|this, _, window, cx| {
                                            let snapshot = this.snapshot();
                                            this.adopt_background_handoff(&snapshot, window, cx);
                                        }),
                                    ),
                                    optional_button(
                                        handoff_ready.then_some("Complete Handoff".to_string()),
                                        handoff_ready && !auth_busy,
                                        "complete-background-handoff",
                                        cx.listener(|this, _, _, cx| {
                                            this.app.spawn_complete_background_handoff();
                                            cx.notify();
                                        }),
                                    ),
                                    optional_button(
                                        handoff_pending.then_some("Clear Handoff".to_string()),
                                        handoff_pending && !auth_busy,
                                        "clear-background-handoff",
                                        cx.listener(|this, _, _, cx| {
                                            this.app.spawn_clear_background_handoff();
                                            cx.notify();
                                        }),
                                    ),
                                    optional_button(
                                        self.app
                                            .background_runner_can_reattach(snapshot)
                                            .then_some("Reattach Background Runner".to_string()),
                                        self.app.background_runner_can_reattach(snapshot) && !auth_busy,
                                        "reattach-background-runner",
                                        cx.listener(|this, _, _, cx| {
                                            this.app.spawn_reattach_background_runner();
                                            cx.notify();
                                        }),
                                    ),
                                ]),
                        ),
                ))
                .child(card(
                    "Runtime Paths",
                    Some("These are the live continuity files the shell is reading and writing."),
                    document_surface(
                        "operate-runtime-paths",
                        build_paths_markdown(&self.app.controller.paths),
                        self.zoom_scale,
                        8.0,
                        Some(18.0),
                        DocumentSurfaceMode::Fit,
                        window,
                        cx,
                    ),
                )),
        )
    }

    fn render_auth_panel(
        &mut self,
        snapshot: &OperatorSnapshot,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let auth_busy = self.app.auth_working.load(Ordering::SeqCst);
        let background_active = snapshot.background_runner_active;
        let oauth_launch_status = interactive_oauth_launch_status(self.app.auth_provider);
        let operator_env_status = operator_env_config_status(
            self.app.auth_provider,
            &snapshot.operator_env_configured_keys,
            &oauth_launch_status,
        );
        let openclaw_status = if self.app.auth_provider == OperatorAuthProvider::OpenAiCodex {
            Some(openclaw_import_status())
        } else {
            None
        };
        let openclaw_cli = if self.app.auth_provider == OperatorAuthProvider::OpenAiCodex {
            Some(openclaw_cli_status())
        } else {
            None
        };
        let auth_completion_surface = if !snapshot.pending_oauth.is_empty() {
            v_flex()
                .gap_3()
                .child(empty_state(
                    "Paste callback only when OAuth is pending",
                    "If the browser callback doesn't return automatically, paste the full callback URL or auth code here, then use the matching completion action below.",
                    cx,
                ))
                .child(labeled_input(
                    "Browser Callback URL Or Auth Code",
                    Input::new(&self.auth_callback_input).h(px(150.0)),
                ))
                .into_any_element()
        } else {
            empty_state(
                if snapshot.auth_ready {
                    "Auth is already complete"
                } else {
                    "No OAuth flow is waiting"
                },
                if snapshot.auth_ready {
                    "You do not need to paste a callback code right now. If the Operate page still mentions a provider wait, use Auth Preflight or retry the turn there; it is not waiting on OAuth."
                } else {
                    "Start a browser or device OAuth flow first. The callback/code field only appears when there is a live authorization to complete."
                },
                cx,
            )
        };

        page_scroll(
            v_flex()
                .gap_4()
                .child(
                    h_flex()
                        .gap_4()
                        .items_start()
                        .child(
                            div()
                                .flex_1()
                                .min_w_0()
                                .child(card(
                                    "Provider & Sign-in",
                                    Some("Native Codex auth is primary. Fallbacks stay visible but clearly secondary."),
                                    v_flex()
                                        .gap_3()
                                        .child(
                                            h_flex()
                                                .gap_2()
                                                .children([
                                                    auth_provider_button(
                                                        self.app.auth_provider
                                                            == OperatorAuthProvider::OpenAiCodex,
                                                        OperatorAuthProvider::OpenAiCodex.as_label(),
                                                        "auth-provider-codex",
                                                        cx.listener(|this, _, _, cx| {
                                                            this.select_auth_provider(
                                                                OperatorAuthProvider::OpenAiCodex,
                                                                cx,
                                                            );
                                                        }),
                                                    ),
                                                    auth_provider_button(
                                                        self.app.auth_provider
                                                            == OperatorAuthProvider::OpenAiApi,
                                                        OperatorAuthProvider::OpenAiApi.as_label(),
                                                        "auth-provider-api",
                                                        cx.listener(|this, _, _, cx| {
                                                            this.select_auth_provider(
                                                                OperatorAuthProvider::OpenAiApi,
                                                                cx,
                                                            );
                                                        }),
                                                    ),
                                                ]),
                                        )
                                        .child(labeled_input(
                                            "Auth Label",
                                            Input::new(&self.auth_label_input),
                                        ))
                                        .child(
                                            h_flex()
                                                .gap_2()
                                                .flex_wrap()
                                                .children([
                                                    Button::new("auth-preflight")
                                                        .label("Auth Preflight")
                                                        .disabled(auth_busy || background_active)
                                                        .on_click(cx.listener(|this, _, _, cx| {
                                                            this.sync_form_into_state(cx);
                                                            this.app.spawn_auth_preflight();
                                                            cx.notify();
                                                        }))
                                                        .into_any_element(),
                                                    Button::new("native-browser-oauth")
                                                        .primary()
                                                        .label(if self.app.auth_provider
                                                            == OperatorAuthProvider::OpenAiCodex
                                                        {
                                                            "Sign In With OpenAI Codex"
                                                        } else {
                                                            "Begin Browser OAuth"
                                                        })
                                                        .disabled(
                                                            auth_busy
                                                                || background_active
                                                                || !oauth_launch_status.ready,
                                                        )
                                                        .on_click(cx.listener(|this, _, _, cx| {
                                                            this.sync_form_into_state(cx);
                                                            this.app.spawn_begin_oauth(
                                                                OAuthInitiationMode::BrowserCallback,
                                                            );
                                                            cx.notify();
                                                        }))
                                                        .into_any_element(),
                                                    optional_button(
                                                        (self.app.auth_provider
                                                            != OperatorAuthProvider::OpenAiCodex)
                                                            .then_some("Begin Device OAuth".to_string()),
                                                        !auth_busy
                                                            && !background_active
                                                            && oauth_launch_status.ready
                                                            && self.app.auth_provider
                                                                != OperatorAuthProvider::OpenAiCodex,
                                                        "native-device-oauth",
                                                        cx.listener(|this, _, _, cx| {
                                                            this.sync_form_into_state(cx);
                                                            this.app.spawn_begin_oauth(
                                                                OAuthInitiationMode::DeviceCode,
                                                            );
                                                            cx.notify();
                                                        }),
                                                    ),
                                                    optional_button(
                                                        (self.app.auth_provider
                                                            == OperatorAuthProvider::OpenAiCodex)
                                                            .then_some("Launch OpenClaw Login (Fallback)".to_string()),
                                                        self.app.auth_provider
                                                            == OperatorAuthProvider::OpenAiCodex
                                                            && !auth_busy
                                                            && !background_active
                                                            && openclaw_cli
                                                                .as_ref()
                                                                .map(|status| status.available)
                                                                .unwrap_or(false),
                                                        "openclaw-login-fallback",
                                                        cx.listener(|this, _, _, cx| {
                                                            this.app.spawn_launch_openclaw_codex_login();
                                                            cx.notify();
                                                        }),
                                                    ),
                                                    optional_button(
                                                        (self.app.auth_provider
                                                            == OperatorAuthProvider::OpenAiCodex)
                                                            .then_some("Import OpenClaw Codex OAuth (Fallback)".to_string()),
                                                        self.app.auth_provider
                                                            == OperatorAuthProvider::OpenAiCodex
                                                            && !auth_busy
                                                            && !background_active
                                                            && openclaw_status
                                                                .as_ref()
                                                                .map(|status| status.available)
                                                                .unwrap_or(false),
                                                        "openclaw-import-fallback",
                                                        cx.listener(|this, _, _, cx| {
                                                            this.app.spawn_import_openclaw_codex_oauth();
                                                            cx.notify();
                                                        }),
                                                    ),
                                                ]),
                                        )
                                        .child(auth_completion_surface),
                                )),
                        )
                        .child(
                            div()
                                .w(px(420.0))
                                .flex_none()
                                .child(card(
                                    "Auth Status",
                                    Some("This keeps readiness, env truth, and fallback status visible without hiding detail."),
                                    document_surface(
                                        "auth-status",
                                        build_auth_status_markdown(
                                            snapshot,
                                            self.app.auth_provider,
                                            &oauth_launch_status,
                                            &operator_env_status,
                                            openclaw_status.as_ref(),
                                            openclaw_cli.as_ref(),
                                        ),
                                        self.zoom_scale,
                                        10.0,
                                        None,
                                        DocumentSurfaceMode::Fit,
                                        window,
                                        cx,
                                    ),
                                )),
                        ),
                )
                .child(
                    if snapshot.pending_oauth.is_empty() {
                        card(
                            "Pending OAuth",
                            Some("No pending OAuth authorizations are waiting right now."),
                            empty_state(
                                "No pending OAuth records",
                                "When a browser or device flow is in progress, the full record will stay here until it completes or expires.",
                                cx,
                            ),
                        )
                        .into_any_element()
                    } else {
                        card(
                            "Pending OAuth",
                            Some("Each authorization stays readable and actionable instead of collapsing into a tiny status field."),
                            v_flex().gap_3().children(snapshot.pending_oauth.iter().map(|pending| {
                                self.render_pending_oauth_card(pending, auth_busy, background_active, window, cx)
                            })),
                        )
                        .into_any_element()
                    },
                ),
        )
    }

    fn render_pending_oauth_card(
        &self,
        pending: &OperatorPendingOAuthView,
        auth_busy: bool,
        background_active: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let pending_id = Uuid::parse_str(&pending.id).ok();
        card(
            &format!("{} · {}", pending.label, pending.kind),
            Some(&pending.provider),
            v_flex()
                .gap_2()
                .child(document_surface(
                    SharedString::from(format!("pending-oauth-{}", pending.id)),
                    build_pending_oauth_markdown(pending),
                    self.zoom_scale,
                    10.0,
                    Some(20.0),
                    DocumentSurfaceMode::Fit,
                    window,
                    cx,
                ))
                .child(
                    h_flex().gap_2().flex_wrap().children([
                        optional_button(
                            pending
                                .authorization_url
                                .as_ref()
                                .and(pending_id.map(|_| "Complete Browser OAuth".to_string())),
                            !auth_busy && !background_active && pending.authorization_url.is_some(),
                            format!("complete-browser-oauth-{}", pending.id),
                            cx.listener({
                                let pending_id = pending_id;
                                move |this, _, _, cx| {
                                    if let Some(pending_id) = pending_id {
                                        this.sync_form_into_state(cx);
                                        this.app.spawn_complete_browser_oauth(pending_id);
                                        cx.notify();
                                    }
                                }
                            }),
                        ),
                        optional_button(
                            pending
                                .authorization_url
                                .is_none()
                                .then_some("Poll / Complete Device OAuth".to_string()),
                            !auth_busy && !background_active && pending.authorization_url.is_none(),
                            format!("complete-device-oauth-{}", pending.id),
                            cx.listener({
                                let pending_id = pending_id;
                                move |this, _, _, cx| {
                                    if let Some(pending_id) = pending_id {
                                        this.sync_form_into_state(cx);
                                        this.app.spawn_complete_device_oauth(pending_id);
                                        cx.notify();
                                    }
                                }
                            }),
                        ),
                    ]),
                ),
        )
        .into_any_element()
    }

    fn render_background_panel(
        &mut self,
        snapshot: &OperatorSnapshot,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let auth_busy = self.app.auth_working.load(Ordering::SeqCst);
        let background_recorded = snapshot.background_runner_id.is_some();
        let background_active = snapshot.background_runner_active;
        let background_owned_by_this_shell =
            self.app.background_runner_owned_by_this_shell(snapshot);
        let background_can_reattach = self.app.background_runner_can_reattach(snapshot);
        let background_attached_to_other_live_shell = self
            .app
            .background_runner_attached_to_other_live_shell(snapshot);
        let background_settings_match =
            background_recorded && self.app.background_settings_match_form(snapshot);
        let handoff_pending = snapshot.background_handoff_pending;
        let handoff_ready = snapshot.background_handoff_ready;
        let handoff_settings_match =
            handoff_pending && self.app.handoff_settings_match_form(snapshot);

        page_scroll(
            v_flex()
                .gap_4()
                .child(card(
                    "Detached Runner",
                    Some("This page owns background truth, shell ownership, and recovery instead of hiding it behind a single status line."),
                    document_surface(
                        "background-status",
                        build_background_markdown(snapshot, background_owned_by_this_shell),
                        self.zoom_scale,
                        10.0,
                        None,
                        DocumentSurfaceMode::Fit,
                        window,
                        cx,
                    ),
                ))
                .child(card(
                    "Recovery & Handoff",
                    Some("These controls keep crash recovery and shell takeover explicit."),
                    v_flex()
                        .gap_3()
                        .children([
                            optional_button(
                                background_can_reattach.then_some("Reattach Background Runner".to_string()),
                                background_can_reattach && !auth_busy,
                                "reattach-background-runner-page",
                                cx.listener(|this, _, _, cx| {
                                    this.app.spawn_reattach_background_runner();
                                    cx.notify();
                                }),
                            ),
                            optional_button(
                                (background_active
                                    && background_owned_by_this_shell
                                    && !background_settings_match
                                    && !auth_busy)
                                    .then_some("Request Background Handoff".to_string()),
                                background_active
                                    && background_owned_by_this_shell
                                    && !background_settings_match
                                    && !auth_busy,
                                "request-background-handoff-page",
                                cx.listener(|this, _, _, cx| {
                                    this.sync_form_into_state(cx);
                                    this.app.spawn_request_background_handoff();
                                    cx.notify();
                                }),
                            ),
                            optional_button(
                                (handoff_pending && !handoff_settings_match)
                                    .then_some("Adopt Handoff Settings".to_string()),
                                handoff_pending && !handoff_settings_match,
                                "adopt-background-handoff-page",
                                cx.listener(|this, _, window, cx| {
                                    let snapshot = this.snapshot();
                                    this.adopt_background_handoff(&snapshot, window, cx);
                                }),
                            ),
                            optional_button(
                                handoff_ready.then_some("Complete Handoff".to_string()),
                                handoff_ready && !auth_busy,
                                "complete-background-handoff-page",
                                cx.listener(|this, _, _, cx| {
                                    this.app.spawn_complete_background_handoff();
                                    cx.notify();
                                }),
                            ),
                            optional_button(
                                handoff_pending.then_some("Clear Handoff".to_string()),
                                handoff_pending && !auth_busy,
                                "clear-background-handoff-page",
                                cx.listener(|this, _, _, cx| {
                                    this.app.spawn_clear_background_handoff();
                                    cx.notify();
                                }),
                            ),
                            optional_button(
                                background_recorded.then_some("Clear Background State".to_string()),
                                background_recorded && !auth_busy,
                                "clear-background-state-page",
                                cx.listener(|this, _, _, cx| {
                                    this.app.spawn_clear_background_state();
                                    cx.notify();
                                }),
                            ),
                        ]),
                ))
                .child(
                    if background_attached_to_other_live_shell {
                        card(
                            "Live Ownership",
                            Some("Another operator window still owns the worker, so this shell stays read-only until that changes."),
                            empty_state(
                                "Worker attached elsewhere",
                                "This shell can see the detached runner, but it should not silently seize control while another live shell still owns it.",
                                cx,
                            ),
                        )
                        .into_any_element()
                    } else {
                        card(
                            "Launch Form Alignment",
                            Some("This keeps the foreground form and detached runner settings comparable."),
                            document_surface(
                                "background-launch-alignment",
                                build_background_alignment_markdown(snapshot, &self.app),
                                self.zoom_scale,
                                8.0,
                                Some(18.0),
                                DocumentSurfaceMode::Fit,
                                window,
                                cx,
                            ),
                        )
                        .into_any_element()
                    },
                ),
        )
    }

    fn render_github_panel(
        &mut self,
        snapshot: &OperatorSnapshot,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let auth_busy = self.app.auth_working.load(Ordering::SeqCst);
        let github_target_ready = if snapshot.github_action_requires_target {
            parse_optional_github_target_override(Some(
                self.github_target_input.read(cx).value().as_ref(),
            ))
            .ok()
            .flatten()
            .is_some()
        } else {
            true
        };

        page_scroll(
            v_flex()
                .gap_4()
                .child(card(
                    "Supervised GitHub Request",
                    Some("The shell keeps queued mutations explicit and reviewable instead of burying them in status text."),
                    document_surface(
                        "github-request",
                        build_github_request_markdown(snapshot),
                        self.zoom_scale,
                        10.0,
                        None,
                        DocumentSurfaceMode::Fit,
                        window,
                        cx,
                    ),
                ))
                .child(
                    if snapshot.github_action_pending {
                        card(
                            "Resolve Target & Decide",
                            Some("Suggestions and manual fallback live in one place so approval stays deliberate."),
                            v_flex()
                                .gap_3()
                                .child(
                                    if snapshot.github_action_target_suggestions.is_empty() {
                                        empty_state(
                                            "No trustworthy automatic target",
                                            snapshot
                                                .github_action_target_guidance
                                                .as_deref()
                                                .unwrap_or(
                                                    "Enter the intended issue or pull request number manually before applying the request.",
                                                ),
                                            cx,
                                        )
                                        .into_any_element()
                                    } else {
                                        v_flex()
                                            .gap_2()
                                            .children(snapshot.github_action_target_suggestions.iter().map(|suggestion| {
                                                let label = format!(
                                                    "Use #{} · {}",
                                                    suggestion.number, suggestion.source
                                                );
                                                Button::new(SharedString::from(format!(
                                                    "github-target-{}",
                                                    suggestion.number
                                                )))
                                                    .label(label)
                                                    .on_click(cx.listener({
                                                        let number = suggestion.number;
                                                        move |this, _, window, cx| {
                                                            this.pick_github_target(number, window, cx);
                                                        }
                                                    }))
                                                    .into_any_element()
                                            }))
                                            .into_any_element()
                                    },
                                )
                                .child(
                                    if snapshot.github_action_requires_target {
                                        labeled_input(
                                            "Target Override",
                                            Input::new(&self.github_target_input),
                                        )
                                        .into_any_element()
                                    } else {
                                        div().into_any_element()
                                    },
                                )
                                .child(
                                    h_flex()
                                        .gap_2()
                                        .flex_wrap()
                                        .children([
                                            Button::new("apply-github-request")
                                                .primary()
                                                .label("Apply GitHub Request")
                                                .disabled(
                                                    auth_busy
                                                        || (snapshot.github_action_requires_target
                                                            && !github_target_ready),
                                                )
                                                .on_click(cx.listener(|this, _, _, cx| {
                                                    this.sync_form_into_state(cx);
                                                    this.app.spawn_apply_github_action_request();
                                                    cx.notify();
                                                }))
                                                .into_any_element(),
                                            Button::new("reject-github-request")
                                                .warning()
                                                .label("Reject GitHub Request")
                                                .disabled(auth_busy)
                                                .on_click(cx.listener(|this, _, _, cx| {
                                                    this.app.spawn_reject_github_action_request();
                                                    cx.notify();
                                                }))
                                                .into_any_element(),
                                            Button::new("clear-github-request")
                                                .label("Clear GitHub Request")
                                                .disabled(auth_busy)
                                                .on_click(cx.listener(|this, _, _, cx| {
                                                    this.app.spawn_clear_github_action_request();
                                                    cx.notify();
                                                }))
                                                .into_any_element(),
                                        ]),
                                ),
                        )
                        .into_any_element()
                    } else {
                        card(
                            "No Pending Request",
                            Some("The lane is clear right now, but the latest settled outcome is still visible."),
                            empty_state(
                                "No supervised GitHub request is pending",
                                latest_settled_github_summary(snapshot).unwrap_or(
                                    "Once the harness queues a supervised GitHub action, it will appear here with target suggestions and explicit operator controls.",
                                ),
                                cx,
                            ),
                        )
                        .into_any_element()
                    },
                )
                .child(card(
                    "Recent GitHub Lifecycle",
                    Some("Settled request history stays close to the live request instead of disappearing after apply or reject."),
                    document_surface(
                        "github-history",
                        build_github_history_markdown(snapshot),
                        self.zoom_scale,
                        10.0,
                        Some(18.0),
                        DocumentSurfaceMode::Fit,
                        window,
                        cx,
                    ),
                )),
        )
    }

    fn render_artifacts_panel(
        &mut self,
        snapshot: &OperatorSnapshot,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        page_scroll(v_flex().gap_4().children([
            artifact_card(
                "Current Brief",
                "artifacts-brief",
                snapshot.current_brief.as_deref(),
                self.zoom_scale,
                window,
                cx,
            ),
            artifact_card(
                "Current Plan",
                "artifacts-plan",
                snapshot.current_plan.as_deref(),
                self.zoom_scale,
                window,
                cx,
            ),
            artifact_card(
                "Current Open Gaps",
                "artifacts-open-gaps",
                snapshot.current_open_gaps.as_deref(),
                self.zoom_scale,
                window,
                cx,
            ),
            artifact_card(
                "Current Handoff",
                "artifacts-handoff",
                snapshot.current_handoff.as_deref(),
                self.zoom_scale,
                window,
                cx,
            ),
        ]))
    }

    fn render_activity_panel(
        &mut self,
        snapshot: &OperatorSnapshot,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        page_scroll(
            v_flex()
                .gap_4()
                .child(card(
                    "Recent Turns",
                    Some("This is the readable turn ledger the existing shell was missing."),
                    document_surface(
                        "recent-turns",
                        bullet_markdown("Recent turns", &snapshot.recent_turns),
                        self.zoom_scale,
                        DOC_SURFACE_MIN_REM,
                        Some(18.0),
                        DocumentSurfaceMode::Fit,
                        window,
                        cx,
                    ),
                ))
                .child(card(
                    "Recent Session Events",
                    Some("Events are scrollable and preserved as a real reading surface now."),
                    document_surface(
                        "recent-events",
                        bullet_markdown("Recent events", &snapshot.recent_events),
                        self.zoom_scale,
                        DOC_SURFACE_MIN_REM,
                        Some(18.0),
                        DocumentSurfaceMode::Fit,
                        window,
                        cx,
                    ),
                ))
                .child(card(
                    "Runtime Status Details",
                    Some("This is the operator-readable status snapshot for deeper inspection."),
                    document_surface(
                        "runtime-status-details",
                        build_activity_markdown(snapshot),
                        self.zoom_scale,
                        10.0,
                        Some(18.0),
                        DocumentSurfaceMode::Fit,
                        window,
                        cx,
                    ),
                )),
        )
    }
}

impl Render for OperatorShell {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.ensure_started(window, cx);
        let snapshot = self.snapshot();
        window.set_rem_size(cx.theme().font_size * self.zoom_scale);

        div()
            .key_context(SHELL_CONTEXT)
            .on_action(cx.listener(Self::zoom_in))
            .on_action(cx.listener(Self::zoom_out))
            .on_action(cx.listener(Self::reset_zoom))
            .size_full()
            .bg(shell_bg())
            .text_color(shell_text())
            .child(
                h_flex()
                    .size_full()
                    .child(self.render_sidebar(&snapshot, cx))
                    .child(
                        div()
                            .flex_1()
                            .min_w_0()
                            .min_h_0()
                            .overflow_y_scrollbar()
                            .child(
                                h_flex().w_full().justify_center().child(
                                    div()
                                        .w_full()
                                        .min_w_0()
                                        .max_w(px(1540.0))
                                        .px_8()
                                        .py_6()
                                        .child(
                                            v_flex()
                                                .w_full()
                                                .gap_5()
                                                .child(self.render_header(&snapshot, cx))
                                                .child(self.render_panel(&snapshot, window, cx)),
                                        ),
                                ),
                            ),
                    ),
            )
    }
}

fn set_input_value(
    state: &Entity<InputState>,
    value: impl Into<SharedString>,
    window: &mut Window,
    cx: &mut Context<OperatorShell>,
) {
    let value = value.into();
    state.update(cx, |input, cx| input.set_value(value.clone(), window, cx));
}

fn this_loop_requested(flag: &Arc<AtomicBool>) -> bool {
    flag.load(Ordering::SeqCst)
}

fn page_scroll(content: impl IntoElement) -> AnyElement {
    div().w_full().min_w_0().child(content).into_any_element()
}

fn card(
    title: impl Into<SharedString>,
    subtitle: Option<&str>,
    body: impl IntoElement,
) -> impl IntoElement {
    let title = title.into();
    let header = {
        let header = v_flex()
            .gap_1()
            .child(div().text_lg().font_semibold().child(title));
        if let Some(subtitle) = subtitle {
            header.child(
                div()
                    .text_sm()
                    .text_color(shell_muted_text())
                    .child(subtitle.to_string()),
            )
        } else {
            header
        }
    };
    v_flex()
        .gap_3()
        .w_full()
        .p_4()
        .rounded_lg()
        .border_1()
        .border_color(shell_border())
        .bg(shell_panel())
        .shadow_sm()
        .child(header)
        .child(body)
}

fn labeled_input(label: &str, input: impl IntoElement) -> AnyElement {
    v_flex()
        .gap_1()
        .min_w(px(180.0))
        .child(
            div()
                .text_sm()
                .font_semibold()
                .text_color(shell_text())
                .child(label.to_string()),
        )
        .child(input)
        .into_any_element()
}

fn empty_state(title: &str, detail: &str, _cx: &Context<OperatorShell>) -> AnyElement {
    v_flex()
        .gap_1()
        .p_4()
        .rounded_lg()
        .border_1()
        .border_color(shell_chip_border())
        .bg(shell_chip_bg())
        .child(
            div()
                .font_semibold()
                .text_color(shell_text())
                .child(title.to_string()),
        )
        .child(
            div()
                .text_sm()
                .text_color(shell_muted_text())
                .child(detail.to_string()),
        )
        .into_any_element()
}

fn document_surface(
    id: impl Into<SharedString>,
    body: impl Into<SharedString>,
    zoom_scale: f32,
    min_height_rem: f32,
    max_height_rem: Option<f32>,
    mode: DocumentSurfaceMode,
    window: &mut Window,
    cx: &mut Context<OperatorShell>,
) -> AnyElement {
    let _ = (id.into(), zoom_scale, window, cx, mode);
    let body: SharedString = body.into();
    let mut container = div()
        .w_full()
        .min_w_0()
        .min_h(rems(min_height_rem))
        .rounded_md()
        .border_1()
        .border_color(shell_chip_border())
        .bg(shell_chip_bg())
        .p_3()
        .overflow_hidden();

    if matches!(mode, DocumentSurfaceMode::Fit) {
        let _ = max_height_rem;
    } else if let Some(max_height_rem) = max_height_rem {
        container = container.max_h(rems(max_height_rem));
    }

    let body = v_flex()
        .w_full()
        .min_w_0()
        .gap_1()
        .children(body.lines().map(render_document_line))
        .into_any_element();

    container.child(body).into_any_element()
}

fn optional_button(
    label: Option<String>,
    enabled: bool,
    id: impl Into<SharedString>,
    on_click: impl Fn(&gpui::ClickEvent, &mut Window, &mut App) + 'static,
) -> AnyElement {
    label.map_or_else(
        || div().into_any_element(),
        |label| {
            Button::new(id.into())
                .label(label)
                .disabled(!enabled)
                .on_click(on_click)
                .into_any_element()
        },
    )
}

fn auth_provider_button(
    active: bool,
    label: &str,
    id: impl Into<SharedString>,
    on_click: impl Fn(&gpui::ClickEvent, &mut Window, &mut App) + 'static,
) -> AnyElement {
    let mut button = Button::new(id.into()).label(label.to_string());
    if active {
        button = button.primary();
    }
    button.on_click(on_click).into_any_element()
}

fn status_pill(label: &str, background: Hsla, foreground: Hsla) -> AnyElement {
    div()
        .px_2p5()
        .py_1()
        .rounded_full()
        .bg(background)
        .text_color(foreground)
        .text_xs()
        .font_semibold()
        .child(label.to_string())
        .into_any_element()
}

fn metadata_chip(label: &str, value: &str, _cx: &Context<OperatorShell>) -> AnyElement {
    h_flex()
        .gap_1()
        .px_2()
        .py_1()
        .rounded_md()
        .border_1()
        .border_color(shell_chip_border())
        .bg(shell_chip_bg())
        .child(
            div()
                .text_xs()
                .font_semibold()
                .text_color(shell_muted_text())
                .child(format!("{label}:")),
        )
        .child(
            div()
                .text_xs()
                .text_color(shell_text())
                .child(value.to_string()),
        )
        .into_any_element()
}

fn artifact_card(
    title: &str,
    id: &str,
    body: Option<&str>,
    zoom_scale: f32,
    window: &mut Window,
    cx: &mut Context<OperatorShell>,
) -> AnyElement {
    let title = title.to_string();
    let body = body.unwrap_or("Not available yet.").to_string();
    card(
        title,
        Some("Full document view with selection and page scrolling."),
        document_surface(
            SharedString::from(id.to_string()),
            body,
            zoom_scale,
            DOC_SURFACE_MIN_REM,
            None,
            DocumentSurfaceMode::Fit,
            window,
            cx,
        ),
    )
    .into_any_element()
}

fn build_engine_identity_markdown(
    provider: OperatorAuthProvider,
    snapshot: &OperatorSnapshot,
    settings: &RunSettings,
) -> String {
    let endpoint = match provider {
        OperatorAuthProvider::OpenAiCodex => OPENAI_CODEX_ENDPOINT,
        OperatorAuthProvider::OpenAiApi => OPENAI_API_ENDPOINT,
    };
    let pending_turn = effective_pending_phase_label(snapshot);
    let last_completed_round = snapshot
        .recent_events
        .iter()
        .rev()
        .find(|event| event.contains("TurnRoundCompleted"))
        .cloned()
        .unwrap_or_else(|| "No completed provider round recorded yet.".to_string());
    format!(
        "# Codex Connection Proof\n\n- **Engine mode:** {NATIVE_CODEX_ENGINE_LABEL}\n- **Summary:** {NATIVE_CODEX_ENGINE_SUMMARY}\n- **Provider:** {}\n- **Endpoint:** `{endpoint}`\n- **Model:** `{}`\n- **Thread:** `{}` ({})\n- **Pending turn phase:** `{pending_turn}`\n- **Foreground runtime:** `{}`\n- **Background runner:** `{}`\n- **Auth evidence:** {}\n- **Latest completed provider round:** {}\n\n> This is proof of a live native Codex backend connection. It is not proof of a visible Codex CLI session because this shell is the client.",
        provider.as_label(),
        settings.model,
        settings.thread_id,
        settings.thread_label,
        snapshot.run_mode,
        snapshot
            .background_runner_status
            .as_deref()
            .unwrap_or("none"),
        snapshot.auth_summary,
        last_completed_round,
    )
}

fn build_harnessed_model_markdown(
    provider: OperatorAuthProvider,
    snapshot: &OperatorSnapshot,
    settings: &RunSettings,
) -> String {
    let mut lines = vec![
        format!("- **Provider:** {}", provider.as_label()),
        format!("- **Model:** `{}`", settings.model),
        format!(
            "- **Thread:** `{}` ({})",
            settings.thread_id, settings.thread_label
        ),
        format!(
            "- **Auth state:** {}",
            if snapshot.auth_ready {
                "ready"
            } else {
                "blocked"
            }
        ),
        format!("- **Run state:** {}", snapshot.run_state),
        format!("- **Run mode:** {}", snapshot.run_mode),
        format!(
            "- **Pending phase:** {}",
            effective_pending_phase_label(snapshot)
        ),
    ];
    if let Some(last_turn) = snapshot.last_turn_summary.as_deref() {
        lines.push(format!("- **Latest bounded outcome:** {last_turn}"));
    }
    if let Some(background_summary) = snapshot.background_runner_summary.as_deref() {
        lines.push(format!(
            "- **Background loop outcome:** {background_summary}"
        ));
    }
    if let Some(last_error) = snapshot.last_error.as_deref() {
        lines.push(format!("- **Latest runtime error:** {last_error}"));
    }
    if snapshot
        .summary
        .contains("No safe verifiable host step is identifiable")
    {
        lines.push(
            "- **What the model needs next:** a task-specific next action or a UI/page screenshot with the intended target so it can act safely."
                .to_string(),
        );
    }
    format!("# Harnessed Model\n\n{}", lines.join("\n"))
}

fn build_operate_status_markdown(snapshot: &OperatorSnapshot) -> String {
    let mut lines = vec![
        format!("- **Summary:** {}", snapshot.summary),
        format!("- **Auth readiness:** {}", snapshot.auth_readiness),
        format!("- **Completed turns:** {}", snapshot.completed_turn_count),
    ];
    if provider_retry_needed(snapshot) {
        lines.push(
            "- **Provider state:** The last bounded attempt failed after auth. This is not waiting on OAuth or a callback code.".to_string(),
        );
        lines.push(
            "- **Next step:** Use **Run Turn** to retry the bounded step, or **Auth Preflight** to re-check the provider session before retrying.".to_string(),
        );
    }
    if let Some(last_turn) = snapshot.last_turn_summary.as_deref() {
        lines.push(format!("- **Last turn:** {last_turn}"));
    }
    if let Some(last_error) = snapshot.last_error.as_deref() {
        lines.push(format!("- **Last error:** {last_error}"));
    }
    if let Some(phase) = snapshot.pending_turn_phase.as_deref() {
        lines.push(format!(
            "- **Pending phase:** {}",
            effective_pending_phase_label(snapshot)
        ));
        if phase == "AwaitingProvider" && snapshot.auth_ready && !provider_retry_needed(snapshot) {
            lines.push(
                "- **Meaning:** OAuth is already complete. The runtime is waiting on the next provider/model step.".to_string(),
            );
        }
    }
    if let Some(action) = snapshot.pending_turn_action.as_deref() {
        lines.push(format!("- **Pending action:** {action}"));
    }
    if let Some(auth_notice) = snapshot.auth_notice.as_deref() {
        lines.push(format!("- **Auth notice:** {auth_notice}"));
    }
    if let Some(background_summary) = snapshot.background_runner_summary.as_deref() {
        lines.push(format!("- **Background summary:** {background_summary}"));
    }
    if snapshot.github_action_pending {
        if let Some(github_summary) = snapshot.github_action_latest_summary.as_deref() {
            lines.push(format!("- **GitHub summary:** {github_summary}"));
        }
    }
    format!("# Live Status\n\n{}", lines.join("\n"))
}

fn provider_retry_needed(snapshot: &OperatorSnapshot) -> bool {
    snapshot.run_state == "idle"
        && snapshot.pending_turn_phase.as_deref() == Some("AwaitingProvider")
        && snapshot.auth_ready
        && snapshot
            .summary
            .contains("Runtime model call failed before the next bounded step")
}

fn effective_pending_phase_label(snapshot: &OperatorSnapshot) -> String {
    match snapshot.pending_turn_phase.as_deref() {
        Some("AwaitingProvider") if provider_retry_needed(snapshot) => {
            "AwaitingProvider (stale after the last failed provider step)".to_string()
        }
        Some(phase) => phase.to_string(),
        None => "none".to_string(),
    }
}

fn build_paths_markdown(paths: &OperatorPaths) -> String {
    format!(
        "# Operator Paths\n\n- **Repo root:** `{}`\n- **Harness root:** `{}`\n- **Operator root:** `{}`\n- **Status:** `{}`\n- **Session root:** `{}`\n- **State DB:** `{}`\n- **Auth store:** `{}`\n- **Operator env:** `{}`",
        paths.repo_root.display(),
        paths.harness_root.display(),
        paths.operator_root.display(),
        paths.status_path.display(),
        paths.session_root.display(),
        paths.state_db_path.display(),
        paths.auth_store_path.display(),
        paths.operator_env_path.display()
    )
}

fn build_auth_status_markdown(
    snapshot: &OperatorSnapshot,
    provider: OperatorAuthProvider,
    oauth_status: &InteractiveOAuthLaunchStatus,
    env_status: &OperatorEnvConfigStatus,
    openclaw_status: Option<&OpenClawImportStatus>,
    openclaw_cli: Option<&OpenClawCliStatus>,
) -> String {
    let mut lines = vec![
        format!("- **Provider:** {}", provider.as_label()),
        format!("- **Readiness:** {}", snapshot.auth_readiness),
        format!("- **Summary:** {}", snapshot.auth_summary),
        format!("- **Interactive OAuth:** {}", oauth_status.summary),
        format!("- **Operator env:** {}", env_status.summary),
    ];
    if snapshot.pending_oauth.is_empty() && snapshot.auth_ready {
        lines.push("- **Callback state:** No callback input is needed right now because auth is already complete.".to_string());
    } else if snapshot.pending_oauth.is_empty() {
        lines.push("- **Callback state:** No OAuth authorization is pending yet. Start a browser or device flow before trying to complete one.".to_string());
    }
    if let Some(auth_notice) = snapshot.auth_notice.as_deref() {
        lines.push(format!("- **Latest auth notice:** {auth_notice}"));
    }
    if let Some(openclaw_status) = openclaw_status {
        lines.push(format!(
            "- **OpenClaw import fallback:** {}",
            openclaw_status.summary
        ));
    }
    if let Some(openclaw_cli) = openclaw_cli {
        lines.push(format!(
            "- **OpenClaw CLI fallback:** {}",
            openclaw_cli.summary
        ));
    }
    format!("# Auth Status\n\n{}", lines.join("\n"))
}

fn build_pending_oauth_markdown(pending: &OperatorPendingOAuthView) -> String {
    let mut lines = vec![
        format!("- **Label:** {}", pending.label),
        format!("- **Provider:** {}", pending.provider),
        format!("- **Kind:** {}", pending.kind),
        format!("- **Started:** {}", pending.started_at),
        format!("- **Action hint:** {}", pending.action_hint),
    ];
    if let Some(expires_at) = pending.expires_at.as_deref() {
        lines.push(format!("- **Expires:** {expires_at}"));
    }
    if let Some(url) = pending.authorization_url.as_deref() {
        lines.push(format!("- **Authorization URL:** {url}"));
    }
    if let Some(redirect_uri) = pending.redirect_uri.as_deref() {
        lines.push(format!("- **Redirect URI:** {redirect_uri}"));
    }
    if let Some(prompt) = pending.callback_prompt.as_deref() {
        lines.push(format!("- **Callback prompt:** {prompt}"));
    }
    if let Some(verification_uri) = pending.verification_uri.as_deref() {
        lines.push(format!("- **Verification URI:** {verification_uri}"));
    }
    if let Some(user_code) = pending.user_code.as_deref() {
        lines.push(format!("- **User code:** `{user_code}`"));
    }
    lines.join("\n")
}

fn build_background_markdown(snapshot: &OperatorSnapshot, owned_by_this_shell: bool) -> String {
    let mut lines = vec![
        format!(
            "- **Status:** {}",
            snapshot
                .background_runner_status
                .as_deref()
                .unwrap_or("not recorded")
        ),
        format!(
            "- **Runner id:** {}",
            snapshot
                .background_runner_id
                .as_deref()
                .unwrap_or("not recorded")
        ),
        format!(
            "- **Phase:** {}",
            snapshot
                .background_runner_phase
                .as_deref()
                .unwrap_or("not recorded")
        ),
        format!(
            "- **Owned by this shell:** {}",
            if owned_by_this_shell { "yes" } else { "no" }
        ),
    ];
    if let Some(pid) = snapshot.background_runner_pid {
        lines.push(format!("- **PID:** {pid}"));
    }
    if let Some(owner_shell_id) = snapshot.background_runner_owner_shell_id.as_deref() {
        lines.push(format!("- **Launch shell id:** {owner_shell_id}"));
    }
    if let Some(owner_shell_pid) = snapshot.background_runner_owner_shell_pid {
        lines.push(format!("- **Launch shell pid:** {owner_shell_pid}"));
    }
    if let Some(owner_shell_alive) = snapshot.background_runner_owner_shell_alive {
        lines.push(format!(
            "- **Launch shell alive:** {}",
            if owner_shell_alive { "yes" } else { "no" }
        ));
    }
    if let Some(objective) = snapshot.background_runner_objective.as_deref() {
        lines.push(format!("- **Objective:** {objective}"));
    }
    if let Some(model) = snapshot.background_runner_model.as_deref() {
        lines.push(format!("- **Model:** {model}"));
    }
    if let Some(thread_id) = snapshot.background_runner_thread_id.as_deref() {
        lines.push(format!("- **Thread id:** {thread_id}"));
    }
    if let Some(thread_label) = snapshot.background_runner_thread_label.as_deref() {
        lines.push(format!("- **Thread label:** {thread_label}"));
    }
    if let Some(loop_pause_seconds) = snapshot.background_runner_loop_pause_seconds {
        lines.push(format!("- **Loop pause:** {:.1}s", loop_pause_seconds));
    }
    if let Some(turn_count) = snapshot.background_runner_turn_count {
        lines.push(format!("- **Completed turns:** {turn_count}"));
    }
    if let Some(summary) = snapshot.background_runner_summary.as_deref() {
        lines.push(format!("- **Summary:** {summary}"));
    }
    if let Some(last_error) = snapshot.background_runner_last_error.as_deref() {
        lines.push(format!("- **Last error:** {last_error}"));
    }
    if let Some(recovery) = snapshot.background_recovery_recommendation.as_deref() {
        lines.push(format!("- **Recovery recommendation:** {recovery}"));
    }
    if let Some(reattach) = snapshot.background_reattach_recommendation.as_deref() {
        lines.push(format!("- **Reattach recommendation:** {reattach}"));
    }
    format!("# Background Runner\n\n{}", lines.join("\n"))
}

fn build_background_alignment_markdown(snapshot: &OperatorSnapshot, app: &OperatorApp) -> String {
    format!(
        "# Launch Form Alignment\n\n- **Form objective:** {}\n- **Runner objective:** {}\n- **Form model:** `{}`\n- **Runner model:** `{}`\n- **Form thread:** `{}` ({})\n- **Runner thread:** `{}` ({})\n- **Form pause:** {:.1}s\n- **Runner pause:** {}\n- **Settings match:** {}",
        app.settings.objective,
        snapshot
            .background_runner_objective
            .as_deref()
            .unwrap_or("not recorded"),
        app.settings.model,
        snapshot
            .background_runner_model
            .as_deref()
            .unwrap_or("not recorded"),
        app.settings.thread_id,
        app.settings.thread_label,
        snapshot
            .background_runner_thread_id
            .as_deref()
            .unwrap_or("not recorded"),
        snapshot
            .background_runner_thread_label
            .as_deref()
            .unwrap_or("not recorded"),
        app.loop_pause_seconds,
        snapshot
            .background_runner_loop_pause_seconds
            .map(|value| format!("{value:.1}s"))
            .unwrap_or_else(|| "not recorded".into()),
        if app.background_settings_match_form(snapshot) {
            "yes"
        } else {
            "no"
        }
    )
}

fn build_github_request_markdown(snapshot: &OperatorSnapshot) -> String {
    let display_state = if !snapshot.github_action_pending
        && snapshot.github_action_state.as_deref() == Some("queued")
    {
        "none"
    } else {
        snapshot.github_action_state.as_deref().unwrap_or("none")
    };
    let latest_summary = latest_settled_github_summary(snapshot).unwrap_or("not available");
    let mut lines = vec![
        format!("- **State:** {display_state}"),
        format!("- **Latest summary:** {latest_summary}"),
    ];
    if let Some(summary) = snapshot.github_action_summary.as_deref() {
        lines.push(format!("- **Pending summary:** {summary}"));
    }
    if let Some(kind) = snapshot.github_action_kind.as_deref() {
        lines.push(format!("- **Kind:** {kind}"));
    }
    if let Some(repository) = snapshot.github_action_repository.as_deref() {
        lines.push(format!("- **Repository:** {repository}"));
    }
    if let Some(target) = snapshot.github_action_target.as_deref() {
        lines.push(format!("- **Target:** {target}"));
    }
    if snapshot.github_action_requires_target {
        lines.push("- **Target requirement:** operator must supply a concrete target".into());
    }
    if let Some(body) = snapshot.github_action_body.as_deref() {
        lines.push(format!("- **Body:** {body}"));
    }
    if let Some(justification) = snapshot.github_action_justification.as_deref() {
        lines.push(format!("- **Justification:** {justification}"));
    }
    if let Some(detail) = snapshot.github_action_detail.as_deref() {
        lines.push(format!("- **Detail:** {detail}"));
    }
    if let Some(guidance) = snapshot.github_action_target_guidance.as_deref() {
        lines.push(format!("- **Guidance:** {guidance}"));
    }
    if let Some(result_excerpt) = snapshot.github_action_result_excerpt.as_deref() {
        lines.push(format!("- **Latest result excerpt:** {result_excerpt}"));
    }
    if let Some(result_url) = snapshot.github_action_result_url.as_deref() {
        lines.push(format!("- **Latest result URL:** {result_url}"));
    }
    format!("# Supervised GitHub Request\n\n{}", lines.join("\n"))
}

fn build_github_history_markdown(snapshot: &OperatorSnapshot) -> String {
    if snapshot.github_action_recent_events.is_empty() {
        return "# Recent Lifecycle\n\n- No GitHub request lifecycle events have been recorded yet."
            .into();
    }
    bullet_markdown("Recent lifecycle", &snapshot.github_action_recent_events)
}

fn latest_settled_github_summary(snapshot: &OperatorSnapshot) -> Option<&str> {
    if snapshot.github_action_pending {
        return snapshot.github_action_latest_summary.as_deref();
    }

    match snapshot.github_action_state.as_deref() {
        Some("applied" | "rejected" | "cleared") => {
            snapshot.github_action_latest_summary.as_deref()
        }
        _ => None,
    }
}

fn build_activity_markdown(snapshot: &OperatorSnapshot) -> String {
    let mut lines = vec![
        format!("- **Summary:** {}", snapshot.summary),
        format!(
            "- **Compaction count:** {}",
            snapshot
                .compaction_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| "not available".into())
        ),
        format!(
            "- **Foreground thread:** {}",
            snapshot
                .foreground_thread_id
                .as_deref()
                .unwrap_or("not available")
        ),
    ];
    if let Some(phase) = snapshot.pending_turn_phase.as_deref() {
        lines.push(format!("- **Pending phase:** {phase}"));
    }
    if let Some(action) = snapshot.pending_turn_action.as_deref() {
        lines.push(format!("- **Pending action:** {action}"));
    }
    if let Some(last_turn) = snapshot.last_turn_summary.as_deref() {
        lines.push(format!("- **Last turn:** {last_turn}"));
    }
    if let Some(last_error) = snapshot.last_error.as_deref() {
        lines.push(format!("- **Last error:** {last_error}"));
    }
    format!("# Runtime Details\n\n{}", lines.join("\n"))
}

fn bullet_markdown(title: &str, lines: &[String]) -> String {
    if lines.is_empty() {
        format!("# {title}\n\n- Not available yet.")
    } else {
        format!(
            "# {title}\n\n{}",
            lines
                .iter()
                .map(|line| format!("- {line}"))
                .collect::<Vec<_>>()
                .join("\n")
        )
    }
}
