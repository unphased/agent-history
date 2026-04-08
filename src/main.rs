mod args;
mod config;
mod cache;
mod indexer;
mod search;
mod telemetry;
mod tui;

use anyhow::Context as _;
use clap::Parser;
use std::io::{self, Write as _};

fn main() -> anyhow::Result<()> {
    let cli = args::Cli::parse();
    match cli.command {
        Some(args::Command::Refresh(refresh)) => {
            indexer::refresh_local_cache(refresh).context("refresh failed")?;
        }
        Some(args::Command::Export(export)) => {
            let stdout = io::stdout();
            let mut handle = stdout.lock();
            indexer::export_local_cache(export, &mut handle).context("export failed")?;
            handle.flush().ok();
        }
        None => {
            tui::run(cli.run).context("TUI run failed")?;
        }
    }
    Ok(())
}
