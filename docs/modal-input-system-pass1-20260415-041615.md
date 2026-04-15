# Modal Input System for TUI — Pass 1 Snapshot

Captured: 2026-04-15 04:16:15 EDT
Source baseline: `/Users/slu/.claude/plans/cheeky-orbiting-wombat.md`

This snapshot preserves the build plan as of the capture time so it can become stale safely.

## Scope

This pass includes:
- `InputMode` enum and key dispatch restructuring
- `SessionSearch`, `PreviewNav`, `GitGraph`, and `GitCommit`
- Tab cycling, Esc retreat, click-to-focus transitions
- Preview `j/k` turn hopping with minimal-reveal scrolling
- Preview arrow scroll and page scroll
- Full-viewport git graph rendering
- In-memory structured git graph cache per repo for the lifetime of the UI session
- Anchor auto-reveal in the git graph when the anchor changes
- Keyboard pane resize via `-`, `=`, `_`, and `+` in normal modes
- Mode-specific `PgUp` and `PgDn`
- Focus fallback on `Ctrl+V` and `Ctrl+D`
- Visual focus styling
- Mode-specific footer help text
- Replace `ActivePane` with `InputMode`

Deferred to pass 2:
- Preview-local search
- Independent git-row cursor navigation
- Git-local search

Not touched in this pass:
- Telemetry remains an outer shell behind `show_telemetry`
- Dead session browser split scaffolding cleanup

## Clarifications Applied

- `Ctrl+D` while git is hidden enables both git graph and commit detail and moves focus to `GitCommit`
- `Enter` behavior is preserved
- Existing SessionSearch editing behavior is preserved, including `Ctrl+u`, `Ctrl+a`, `Ctrl+e`, and `Alt+Left` / `Alt+Right`
- `Ctrl+o` is not a compatibility target for this plan and may be removed later
- The footer help line should reflect the active mode and show the relevant key mappings

## Mode Model

```rust
enum InputMode {
    SessionSearch,
    PreviewNav,
    GitGraph,
    GitCommit,
}
```

Default mode: `SessionSearch`

When `show_telemetry` is true, telemetry keeps its existing dedicated input and layout flow. The main-mode system is suspended until telemetry is closed.

## Focus and Navigation

Tab cycle:
- `SessionSearch -> GitGraph` if visible, otherwise `PreviewNav`
- `GitGraph -> GitCommit` if visible, otherwise `PreviewNav`
- `GitCommit -> PreviewNav`
- `PreviewNav -> SessionSearch`

Shift+Tab reverses the cycle and skips hidden panes.

Esc:
- `PreviewNav -> SessionSearch`
- `GitGraph -> SessionSearch`
- `GitCommit -> SessionSearch`
- `SessionSearch -> no-op`

Clicks:
- Query bar or session list -> `SessionSearch`
- Preview pane -> `PreviewNav`
- Git graph -> `GitGraph`
- Git commit -> `GitCommit`

Visibility toggles:
- Hiding git while focused in `GitGraph` or `GitCommit` falls back to `PreviewNav`
- Hiding commit detail while focused in `GitCommit` falls back to `GitGraph`
- Showing git via `Ctrl+V` moves focus to `GitGraph`
- Showing commit detail via `Ctrl+D` moves focus to `GitCommit`
- If git is hidden, `Ctrl+D` enables both git graph and commit detail and focuses `GitCommit`

## Per-Mode Keys

SessionSearch:
- Printable keys edit the session query
- Backspace and Delete edit the query
- Left and Right move the query cursor
- Alt+Left and Alt+Right keep word movement
- Ctrl+A and Ctrl+E keep start and end movement
- Ctrl+U keeps clear-query-and-filters behavior
- Up and Down move the session selection
- PageUp and PageDown page the session selection by visible results height
- Home and End select first and last session
- Esc is a no-op
- Tab advances focus
- `-`, `=`, `_`, and `+` remain text

PreviewNav:
- `j` and `k` hop turns with minimal reveal
- Up and Down line-scroll the preview
- PageUp and PageDown page-scroll the preview
- `-` and `=` adjust `results_pct`
- `_` and `+` are no-ops
- `/` is reserved
- Esc returns to `SessionSearch`

GitGraph:
- Up and Down scroll the git graph viewport
- PageUp and PageDown page-scroll the git graph
- `-` and `=` adjust `results_pct`
- `_` and `+` adjust `git_pct`
- `/` is reserved
- Esc returns to `SessionSearch`

GitCommit:
- Up and Down scroll the commit viewport
- PageUp and PageDown page-scroll the commit view
- `-` and `=` adjust `results_pct`
- `_` and `+` adjust `graph_pct`
- Esc returns to `SessionSearch`

Global keys preserved across main modes:
- `Ctrl+C` quit
- `Ctrl+T` telemetry toggle
- `Ctrl+V` git toggle
- `Ctrl+D` commit detail toggle
- `Ctrl+L` layout preset cycle
- `Ctrl+N` and `Ctrl+P` preview match navigation
- `Enter` resume selected session

## Preview Minimal Reveal

`jump_preview_record` should stop pegging the selected turn to the top of the pane.

New behavior:
1. Compute the target turn start offset
2. If the target is already fully visible, keep the scroll offset unchanged
3. If the target is above the viewport, scroll just enough to place it at the top
4. If the target extends below the viewport, scroll just enough to place its start at the bottom edge that still reveals it

Implementation note:
- Add a dedicated helper for minimal-reveal preview selection
- Keep the existing top-align helper for callers that intentionally want that behavior

## Git Graph Cache and Rendering

The git graph should be cached in a structured form per repo for the lifetime of the UI session.

For each repo cache entry, store:
- Commit list with timestamps suitable for anchor lookup
- Full graph output lines in chronological git-log order
- Parsed commit hashes for graph lines
- Mapping from graph display lines to commit indices

Build strategy:
- Precompute the graph for a repo the first time it is needed
- Keep it in memory until the UI exits
- Use the cached commit timestamps for anchor lookup
- Use binary search where helpful for anchor-to-commit resolution

Graph parsing:
- The rendered git graph must retain the graph asterisks and line art
- Parse commit hashes from the git-log output with regex-compatible extraction
- Non-commit graph-only lines may remain in the display output but should be excluded from the commit-content indexing used for anchor and navigation calculations

Refresh relief valve:
- Add a key binding to rebuild the cached git graph data for the current repo on demand
- The footer help should advertise that binding while git modes are active

Viewport behavior:
- Render the full graph, not a clipped window
- Keep the textual header lines at the top of the document
- `git_graph_scroll` is a real viewport offset over the full graph
- When the anchor changes, auto-reveal the highlighted anchor commit with minimal scrolling
- Do not snap the graph viewport on every draw; only auto-reveal when the anchor target actually changes

## Visual Focus and Footer Help

- Active pane border gets stronger styling
- Inactive pane borders stay subdued
- Query bar uses active styling only in `SessionSearch`
- The terminal cursor is shown only in `SessionSearch`
- The footer help text changes by mode and advertises the relevant bindings for the focused pane

## Implementation Order

1. Add the plan snapshot and start checkpointing work in commits
2. Replace `ActivePane` with `InputMode` while preserving SessionSearch behavior
3. Add preview minimal-reveal helpers and update preview navigation
4. Rework git graph cache and full-viewport rendering with anchor auto-reveal
5. Add GitGraph and GitCommit mode routing
6. Add keyboard resize remap and page-routing
7. Add mode-specific footer help and focus styling
8. Run tests and fix regressions
