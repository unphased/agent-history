# agent-history
Project-wide full-text search TUI for Codex / Claude / OpenCode conversation logs.

## Fork Changes
Compared with upstream `origin/main`, this fork has diverged substantially.

- Added OpenCode history indexing and session resume support.
- Changed search from a vague session-only summary view to hit-aware session results: matching sessions now show matched snippets, matched-message previews, and pager jumps around the actual hit.
- Added query highlighting in both the result list and preview pane.
- Added a much richer preview pane: keyboard scrolling, mouse-wheel scrolling, pane-aware wheel routing, wrap-aware scroll bounds, and multi-hit session previews instead of just the first matched message.
- Expanded preview diagnostics so large sessions can show many matching messages plus total query-occurrence counts, instead of hiding the scale of the match.
- Improved result-list formatting and provider labeling so sessions are easier to scan quickly.
- Added account-scoped Codex and Claude profile discovery for directories like `~/.codex-work` and `~/.claude-work`, keeps those histories in separate namespaces, and resumes them via `codex-account <name>` / `claude-account <name>`.

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

## Usage
```bash
agent-history
```

You can start typing the query immediately; indexing runs in the background and shows progress.

For development:
```bash
cargo run --release
```

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

## Options
```bash
# Add extra search roots (repeatable)
agent-history --root /path/to/dir

# Disable default roots and search only explicit roots
agent-history --no-default-roots --root /path/to/dir

# Also include ~/.codex/history.jsonl (simple text format)
agent-history --history

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
- `Enter`: resume in `codex resume` / `claude --resume` / `opencode --session`, or via `codex-account <name>` / `claude-account <name>` for account-scoped sessions
- `Ctrl+o`: open raw JSONL around the selected item in `$PAGER` (defaults to `less -R`)
- `Esc` / `Ctrl+c`: quit

## Privacy
This tool reads local log files and does not send your data over the network.

## Security
See `SECURITY.md`.

## License
Licensed under either of:

- Apache License, Version 2.0 (`LICENSE-APACHE`)
- MIT license (`LICENSE-MIT`)
