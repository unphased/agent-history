# agent-history
Project-wide full-text search TUI for Codex / Claude / OpenCode conversation logs.

## Fork Changes
Compared with upstream `origin/main`, this fork has diverged substantially. Below is a list, initially exhaustive, but
now no longer updated, of improvments I've (slu) added to the project.

- Added OpenCode history indexing and session resume support.
- Parallelized OpenCode session indexing so large storage trees refresh faster.
- Changed search from a vague session-only summary view to hit-aware session results: matching sessions now show matched snippets, matched-message previews, and pager jumps around the actual hit.
- Added query highlighting in both the result list and preview pane.
- Added a much richer preview pane: keyboard scrolling, mouse-wheel scrolling, pane-aware wheel routing, wrap-aware scroll bounds, and multi-hit session previews instead of just the first matched message.
- Expanded preview diagnostics so large sessions can show many matching messages plus total query-occurrence counts, instead of hiding the scale of the match.
- Improved result-list formatting and provider labeling so sessions are easier to scan quickly.
- Added account-scoped Codex and Claude profile discovery for directories like `~/.codex-work` and `~/.claude-work`, keeps those histories in separate namespaces, and resumes them via `codex-account <name>` / `claude-account <name>`.
- Added a persistent SQLite index cache with incremental refresh, stale-source pruning, and fingerprint-based reuse instead of rescanning everything every launch.
- Added explicit cache controls: `--no-cache` for always-on full scans and `--rebuild-index` to discard and rebuild the persistent cache.
- Added JSONL telemetry for indexing/cache lifecycle metrics, with opt-out and custom log-path flags.
- Added a config file and federated remote-cache sync over SSH so one terminal can search imported history from multiple machines.
- Added Codex image attachment extraction so image references are preserved in indexed records and shown in previews.

In short: this is no longer just the original upstream search UI. It now behaves more like a serious local history browser for multiple agent ecosystems and multiple local accounts.

- Codex: `~/.codex/sessions/**.jsonl` and `~/.codex/archived_sessions/**.jsonl`
- Codex accounts: `~/.codex-<account>/{sessions,archived_sessions}/**.jsonl`
- Claude: `~/.claude/projects/**.jsonl`
- Claude accounts: `~/.claude-<account>/projects/**.jsonl`
- OpenCode: `~/.local/share/opencode/storage/{session,message,part}`
- Extra locations: use `--root` explicitly if you want to scan anything outside those home-scoped defaults

Japanese README: `README.ja.md`

## Install
```bash
cargo install --path .
```

## Requirements
- Rust toolchain (to build/install from source)
- Optional: `codex` CLI (for `Enter` resume on OpenAI/Codex sessions)
- Optional: `codex-account` wrapper (for `Enter` resume on account-scoped Codex sessions such as `~/.codex-work`)
- Optional: `claude` CLI (for `Enter` resume on Claude sessions)
- Optional: `claude-account` wrapper (for `Enter` resume on account-scoped Claude sessions such as `~/.claude-work`)
- Optional: `opencode` CLI (for `Enter` resume on OpenCode sessions)
- Optional: `$PAGER` (defaults to `less -R`) for `Ctrl+o`

No external fuzzy finder (e.g. `fzf`) is required.

## Architecture Notes
- The default persistent cache is SQLite at `~/.local/state/agent-history/index.sqlite`.
- The default telemetry log is JSONL at `~/.local/state/agent-history/events.jsonl`.
- The default config path is `~/.config/agent-history/config.toml`.
- `AGENT_HISTORY_CACHE_DB` overrides the cache database path.
- `AGENT_HISTORY_TELEMETRY_LOG` overrides the telemetry log path.
- Cached source units are fingerprinted so unchanged files/sessions are reused.
- Deleted sources are pruned from the cache automatically.
- OpenCode sessions are refreshed in parallel.
- Imported remote snapshots are stored in the same local cache and searched together with local records.
- Codex image attachments are surfaced in previews; embedded image data is materialized under the system temp directory on demand.

## Usage
```bash
agent-history
```

You can start typing the query immediately; indexing runs in the background and shows progress.

For development:
```bash
cargo run --release
```

For headless refresh/export:
```bash
agent-history refresh
agent-history export --format ndjson
```

## Multi-Machine Config
Create `~/.config/agent-history/config.toml`:

```toml
[machine]
id = "mbp"
name = "MacBook Pro"

[ui.tags]
show_provider = true
show_host = true
show_project = true

[[remotes]]
name = "mini"
host = "mini.tailnet.ts.net"
user = "slu"
command = "agent-history"
enabled = true
refresh_on_start = true

[[remotes]]
name = "workstation"
host = "workstation.tailnet.ts.net"
user = "slu"
command = "agent-history"
enabled = true
refresh_on_start = true
```

Field notes:
- `machine.id`: stable id stored on exported records
- `machine.name`: display label in the UI; falls back to hostname if omitted
- `remotes[].name`: local alias for the remote, also used as the imported record origin
- `remotes[].host`: SSH host
- `remotes[].user`: SSH user
- `remotes[].command`: remote executable name/path, usually just `agent-history`
- `remotes[].enabled`: whether the remote participates in background sync
- `remotes[].refresh_on_start`: whether startup should try to refresh/import that remote

When remotes are configured, startup does this:
1. Load the local cache.
2. Load any previously imported remote snapshots.
3. Bring up the UI immediately.
4. In the background, for each enabled remote, run:

```bash
ssh user@host agent-history refresh
ssh user@host agent-history export --format ndjson
```

The returned NDJSON is imported into the local SQLite cache, so search stays local and snappy even if a remote is temporarily offline. The log view shows remote sync freshness, counts, durations, and failures.

## List Format
Each result row is:

`timestamp(last activity)  C|O|OC[ account]  [dir]  first message (1st line)`

- `C`: Claude
- `O`: OpenAI/Codex
- `OC`: OpenCode
- Account-scoped home dirs such as `~/.claude-work` and `~/.codex-work` are shown as `C work` and `O work`
- The session directory name is shown in brackets, for example `[instrumenter]`

Notes:
- Codex logs may start with `AGENTS.md` / `<environment_context>` metadata; those are excluded from the “first message”.
- Sessions that contain only such metadata are not shown.
- `subagents/` logs are excluded by default because they are noisy and can be huge.
- When a matching Codex record includes image attachments, the preview shows them as file paths.

## Options
```bash
# Add extra search roots (repeatable)
agent-history --root /path/to/dir

# Disable default roots and search only explicit roots
agent-history --no-default-roots --root /path/to/dir

# Also include ~/.codex/history.jsonl (simple text format)
agent-history --history

# Disable the persistent cache and force a full rescan
agent-history --no-cache

# Drop and rebuild the persistent cache
agent-history --rebuild-index

# Refresh the local cache without launching the TUI
agent-history refresh

# Export normalized local records as NDJSON
agent-history export --format ndjson

# Write telemetry JSONL to a custom path
agent-history --telemetry-log /tmp/agent-history-events.jsonl

# Disable telemetry logging
agent-history --no-telemetry

# Start with a query
agent-history --query "DMARC"

# Space-separated tokens are AND
agent-history --query "cloud run"

# Smartcase: tokens with ASCII uppercase are case-sensitive
agent-history --query "GitHub Actions"
```

## Keys
- Typing: update query (search as you type)
- `Backspace`: delete one char
- `Ctrl+u`: clear query
- `↑/↓`: move selection
- `PageUp/PageDown`: move by page
- `Enter`: open a login-shell handoff for the selected session; local sessions resume locally, remote sessions SSH to the owning host
- `Ctrl+o`: open raw JSONL around the selected item in `$PAGER` (defaults to `less -R`)
- `Ctrl+t`: toggle the log/events view
- `Esc` / `Ctrl+c`: quit

## Privacy
This tool reads local log files. If you configure remotes, it will also use SSH to refresh/export remote caches and import those snapshots into the local cache.

## Security
See `SECURITY.md`.

## License
Licensed under either of:

- Apache License, Version 2.0 (`LICENSE-APACHE`)
- MIT license (`LICENSE-MIT`)
