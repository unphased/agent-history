# agent-history
Project-wide full-text search TUI for Codex / Claude conversation logs.

- Codex: `~/.codex/sessions/**.jsonl` and `~/.codex/archived_sessions/**.jsonl`
- Claude: `~/.claude/projects/**.jsonl`
- Auto-discovery: project-local `**/.codex/{sessions,archived_sessions}` and `**/.codex/history.jsonl` under `$HOME` (skips `.git`, `node_modules`, etc.)

Japanese README: `README.ja.md`

## Install
```bash
cargo install --path .
```

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

`timestamp(last activity)  dir  C|O  first message (1st line)`

- `C`: Claude
- `O`: OpenAI/Codex

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
- `Enter`: resume in `codex resume` / `claude --resume` (requires those CLIs)
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
