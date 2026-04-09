use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

#[derive(Parser, Debug, Clone)]
#[command(name = "agent-history")]
#[command(
    about = "Search Codex/Claude/OpenCode history locally and across imported remote caches",
    long_about = None
)]
#[command(version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,

    #[command(flatten)]
    pub run: RunArgs,
}

#[derive(Subcommand, Debug, Clone)]
pub enum Command {
    Refresh(RefreshArgs),
    Export(ExportArgs),
}

#[derive(Parser, Debug, Clone)]
pub struct ScanArgs {
    /// Additional search roots (recursive). Can be repeated.
    #[arg(long = "root", value_name = "PATH")]
    pub roots: Vec<PathBuf>,

    /// Disable default search roots such as ~/.codex and ~/.claude
    #[arg(long)]
    pub no_default_roots: bool,

    /// Include ~/.codex/history.jsonl as well
    #[arg(long = "history")]
    pub include_history: bool,

    /// Ignore the persistent cache and do a full scan
    #[arg(long)]
    pub no_cache: bool,

    /// Rebuild the persistent cache from scratch
    #[arg(long)]
    pub rebuild_index: bool,

    /// JSONL log output path for events/metrics
    #[arg(long, value_name = "PATH")]
    pub telemetry_log: Option<PathBuf>,

    /// Disable the events/metrics log
    #[arg(long)]
    pub no_telemetry: bool,

    /// Optional config path
    #[arg(long, value_name = "PATH")]
    pub config: Option<PathBuf>,
}

#[derive(Parser, Debug, Clone)]
pub struct RunArgs {
    #[command(flatten)]
    pub scan: ScanArgs,

    /// Query to run on startup
    #[arg(long, short = 'q', value_name = "QUERY")]
    pub query: Option<String>,

    /// Max number of results shown (0 = unlimited)
    #[arg(long, default_value_t = 5000)]
    pub max_results: usize,
}

#[derive(Parser, Debug, Clone)]
pub struct RefreshArgs {
    #[command(flatten)]
    pub scan: ScanArgs,
}

#[derive(Parser, Debug, Clone)]
pub struct ExportArgs {
    #[command(flatten)]
    pub scan: ScanArgs,

    /// Optional query filter using the same matching rules as the TUI
    #[arg(long, short = 'q', value_name = "QUERY")]
    pub query: Option<String>,

    /// Output format
    #[arg(long, value_enum, default_value_t = ExportFormat::Tsv)]
    pub format: ExportFormat,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum ExportFormat {
    Tsv,
}
