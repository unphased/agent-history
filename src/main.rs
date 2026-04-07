mod args;
mod cache;
mod indexer;
mod search;
mod telemetry;
mod tui;

use anyhow::Context as _;
use clap::Parser;

fn main() -> anyhow::Result<()> {
    let args = args::Args::parse();
    tui::run(args).context("TUIの実行に失敗しました")?;
    Ok(())
}
