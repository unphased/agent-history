mod args;
mod cache;
mod config;
mod indexer;
mod search;
mod telemetry;
mod tui;

use anyhow::Context as _;
use clap::Parser;

fn main() -> anyhow::Result<()> {
    let cli = args::Cli::parse();
    match cli.command {
        Some(args::Command::Refresh(refresh)) => {
            indexer::refresh_local_cache(refresh).context("refresh failed")?;
        }
        Some(args::Command::Export(export)) => {
            indexer::export_records(export).context("export failed")?;
        }
        None => {
            tui::run(cli.run).context("TUI run failed")?;
        }
    }
    Ok(())
}
