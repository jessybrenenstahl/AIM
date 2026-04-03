use anyhow::{Context, anyhow};
use splcw_host::backend::{BackendMode, run_cli};

fn main() -> anyhow::Result<()> {
    let mode = match std::env::args().nth(1).as_deref() {
        Some("observe") => BackendMode::Observe,
        Some("enact") => BackendMode::Enact,
        Some("verify") => BackendMode::Verify,
        other => {
            return Err(anyhow!(
                "expected one of observe|enact|verify, got {:?}",
                other
            ));
        }
    };

    run_cli(mode).context("run host backend")
}
