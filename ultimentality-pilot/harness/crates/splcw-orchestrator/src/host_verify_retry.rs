//! Bounded stabilization and retry logic for uncertain host effects.
//!
//! # Problem
//!
//! The existing runtime declares `HostEffectsUncertain` immediately when
//! post-action verification fails. There is no retry window — one bad
//! observation after a `FocusWindow` call that reports success but puts
//! the wrong window foreground immediately surfaces a gap and aborts the
//! turn. Transient UI lag (compositor stutter, window raise races) triggers
//! the same path as a genuine capability failure.
//!
//! # Solution
//!
//! Insert a `stabilize_host_effect` call between the first failed
//! verification and the `HostEffectsUncertain` checkpoint write. The
//! function re-observes up to `MAX_STABILIZATION_ATTEMPTS` times with
//! a short pause between each attempt, giving the host a chance to
//! settle before the turn declares defeat.
//!
//! # Integration point (runtime.rs)
//!
//! In `PersistentOrchestrator::run_runtime_turn`, after the initial
//! `host.verify(...)` call returns an uncertain result, replace the
//! immediate `HostEffectsUncertain` checkpoint write with:
//!
//! ```rust,ignore
//! match stabilize_host_effect(
//!     host,
//!     &action,
//!     &execution,
//!     |obs| check_verification_signals(obs),  // your existing signal checker
//!     |obs| format!("verification still failing after stabilization: {}", obs.summary),
//!     options.host_verify_timeout,
//!     None, // use defaults
//!     None,
//! )
//! .await?
//! {
//!     StabilizationOutcome::Settled { observation, attempt } => {
//!         // Proceed as a successful verification; record the attempt count.
//!         tracing::info!("host effect settled after {attempt} stabilization attempt(s)");
//!         // ... continue normal turn flow with `observation`
//!     }
//!     StabilizationOutcome::Uncertain { last_observation, contradiction } => {
//!         // Write the HostEffectsUncertain checkpoint as before.
//!         write_pending_turn_checkpoint_if_present(
//!             session_journal.as_ref(),
//!             &build_pending_turn_checkpoint(
//!                 ...,
//!                 RuntimePendingTurnPhase::HostEffectsUncertain,
//!                 ...
//!             ),
//!         ).await?;
//!         // Surface a gap with the contradiction string.
//!         return self.surface_runtime_gap(...).await;
//!     }
//! }
//! ```

use anyhow::Result;
use chrono::Utc;
use splcw_computer_use::{ActionExecution, ObservationFrame, ProposedAction};
use splcw_host::HostBody;
use std::time::Duration;
use tokio::time::sleep;

/// Maximum times to re-observe after an uncertain verification result before
/// declaring `HostEffectsUncertain`.
pub const MAX_STABILIZATION_ATTEMPTS: usize = 3;

/// Pause between each re-observation attempt. Long enough for most compositor
/// raise/focus races to resolve, short enough to keep turns snappy.
pub const STABILIZATION_DELAY: Duration = Duration::from_millis(800);

/// Outcome of a [`stabilize_host_effect`] call.
#[derive(Debug)]
pub enum StabilizationOutcome {
    /// Verification passed within the retry window.
    Settled {
        /// The observation frame that confirmed the effect.
        observation: ObservationFrame,
        /// Which attempt number (1-indexed) produced the settled result.
        attempt: usize,
    },
    /// All retries exhausted; host effects remain uncertain.
    Uncertain {
        /// The last observation taken during stabilization.
        last_observation: ObservationFrame,
        /// Human-readable contradiction string for gap / checkpoint recording.
        contradiction: String,
    },
}

/// Attempt to stabilize a failed host-action verification by re-observing the
/// host state up to `max_attempts` times with `delay` between each attempt.
///
/// # Arguments
///
/// * `host` — the `HostBody` used to take a fresh observation each attempt.
/// * `action` — the original `ProposedAction` whose effects are being checked
///   (used only for diagnostics; not re-executed).
/// * `execution` — the `ActionExecution` record from the original host enact
///   (used only for diagnostics).
/// * `is_settled` — a closure that examines a new `ObservationFrame` and
///   returns `Ok(true)` if the effects are now confirmed, `Ok(false)` if still
///   uncertain, or `Err(...)` for a hard verification error that should abort
///   immediately without further retries.
/// * `describe_contradiction` — closure that produces a human-readable
///   contradiction string from the last observation, used when all attempts fail.
/// * `observe_timeout` — per-observation deadline; maps directly to the
///   `host_verify_timeout` from `RuntimeTurnOptions`.
/// * `max_attempts` — maximum re-observations (defaults to
///   [`MAX_STABILIZATION_ATTEMPTS`] when `None`).
/// * `delay` — sleep between attempts (defaults to [`STABILIZATION_DELAY`]
///   when `None`).
pub async fn stabilize_host_effect<H, F, D>(
    host: &H,
    _action: &ProposedAction,
    _execution: &ActionExecution,
    is_settled: F,
    describe_contradiction: D,
    observe_timeout: Duration,
    max_attempts: Option<usize>,
    delay: Option<Duration>,
) -> Result<StabilizationOutcome>
where
    H: HostBody,
    F: Fn(&ObservationFrame) -> Result<bool>,
    D: Fn(&ObservationFrame) -> String,
{
    let attempts = max_attempts.unwrap_or(MAX_STABILIZATION_ATTEMPTS);
    let pause = delay.unwrap_or(STABILIZATION_DELAY);

    let mut last_observation: Option<ObservationFrame> = None;

    for attempt in 1..=attempts {
        sleep(pause).await;

        let observation = tokio::time::timeout(observe_timeout, host.observe())
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "stabilization observe timed out on attempt {attempt}/{attempts}"
                )
            })??;

        match is_settled(&observation) {
            Ok(true) => {
                return Ok(StabilizationOutcome::Settled {
                    observation,
                    attempt,
                });
            }
            Ok(false) => {
                last_observation = Some(observation);
            }
            Err(hard_error) => {
                // A hard verification error — do not retry further.
                return Err(hard_error.context(format!(
                    "hard verification error on stabilization attempt {attempt}/{attempts}"
                )));
            }
        }
    }

    let last = last_observation.unwrap_or_else(|| ObservationFrame {
        captured_at: Utc::now(),
        summary: "no observation captured during stabilization".into(),
        screenshot_path: None,
        ocr_text: None,
        active_window: None,
        window_titles: Vec::new(),
        clipboard_text: None,
        structured_signals: Vec::new(),
    });

    let contradiction = describe_contradiction(&last);

    Ok(StabilizationOutcome::Uncertain {
        last_observation: last,
        contradiction,
    })
}
