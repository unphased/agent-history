use crate::{
    args::{LogGroup, RunArgs},
    cache, config,
    indexer::{
        ImageAttachment, ImageAttachmentKind, IndexerEvent, MessageRecord, Role, SourceKind,
    },
    search, telemetry,
};
use anyhow::{Context as _, anyhow};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use crossterm::{
    cursor,
    event::{
        self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEvent, KeyModifiers,
        MouseButton, MouseEvent, MouseEventKind,
    },
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Gauge, Paragraph, Wrap},
};
use serde_json::{Value, json};
use std::{
    cmp,
    collections::{HashMap, HashSet, VecDeque},
    env, fs,
    io::{self, Stdout},
    mem,
    path::{Path, PathBuf},
    process::Command,
    sync::mpsc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum TagFilterKind {
    Provider,
    Host,
    Project,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TagFilter {
    kind: TagFilterKind,
    value: String,
}

#[derive(Debug, Clone)]
struct SessionTagSpec {
    filter: TagFilter,
    content: String,
    style: Style,
}

#[derive(Debug, Default, Clone)]
struct IndexingProgress {
    total_files: usize,
    processed_files: usize,
    records: usize,
    sessions: usize,
    current: Option<PathBuf>,
    last_warn: Option<String>,
}

#[derive(Debug, Clone)]
struct SessionSummary {
    source: SourceKind,
    session_id: String,
    account: Option<String>,
    machine_id: String,
    machine_name: String,
    origin: String,
    project_slug: Option<String>,
    first_user_idx: usize,
    last_ts: Option<String>,
    cwd: Option<String>,
    dir: String,
    first_line: String,
}

#[derive(Debug, Default)]
struct SessionAgg<'a> {
    record_indices: Vec<usize>,
    last_ts: Option<&'a str>,
    cwd: Option<&'a str>,
    project_slug: Option<&'a str>,
    machine_id: Option<&'a str>,
    machine_name: Option<&'a str>,
    origin: Option<&'a str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SessionHit {
    session_idx: usize,
    matched_record_idx: Option<usize>,
    hit_count: usize,
}

struct PreviewDoc {
    lines: Vec<Line<'static>>,
    first_match_line: usize,
    match_lines: Vec<usize>,
    line_record_indices: Vec<Option<usize>>,
}

#[derive(Debug, Default)]
struct EventBuffer {
    records: VecDeque<telemetry::EventRecord>,
    total_bytes: usize,
    max_bytes: usize,
}

impl EventBuffer {
    fn new(max_bytes: usize) -> Self {
        Self {
            records: VecDeque::new(),
            total_bytes: 0,
            max_bytes,
        }
    }

    fn seed(&mut self, records: Vec<telemetry::EventRecord>) {
        for record in records {
            self.push(record);
        }
    }

    fn push(&mut self, record: telemetry::EventRecord) {
        let record_len = record.encoded_len();
        self.records.push_back(record);
        self.total_bytes = self.total_bytes.saturating_add(record_len);
        while self.total_bytes > self.max_bytes {
            let Some(oldest) = self.records.pop_front() else {
                break;
            };
            self.total_bytes = self.total_bytes.saturating_sub(oldest.encoded_len());
        }
    }

    fn iter(&self) -> impl Iterator<Item = &telemetry::EventRecord> {
        self.records.iter()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionBrowserPane {
    Start,
    End,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LayoutPreset {
    Balanced,
    TurnsWide,
    GitWide,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActivePane {
    Results,
    TurnPreview,
    SessionBrowser(SessionBrowserPane),
    GitGraph,
    GitCommit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActiveSplit {
    ResultsGit,
    GitTurns,
    GitColumnVertical,
}

#[derive(Debug, Clone, Copy)]
struct LayoutRatios {
    results_pct: u16,
    git_pct: u16,
    graph_pct: u16,
}

#[derive(Debug, Clone, Copy)]
struct LayoutState {
    preset: LayoutPreset,
    results_pct: u16,
    git_pct: u16,
    graph_pct: u16,
}

#[derive(Debug, Clone)]
struct GitCommitEntry {
    hash: String,
    epoch: i64,
    summary: String,
}

#[derive(Debug, Clone)]
struct GitMatch {
    anchor_record_idx: usize,
    repo_root: PathBuf,
    anchor_ts: i64,
    selected_commit_idx: usize,
}

#[derive(Debug, Clone)]
struct GitRepoContext {
    repo_root: PathBuf,
    commits: Vec<GitCommitEntry>,
    graph_lines: Vec<String>,
    graph_line_commit_indices: Vec<Option<usize>>,
}

#[derive(Debug, Clone)]
struct GitPaneDoc {
    lines: Vec<Line<'static>>,
}

#[derive(Debug, Clone, Copy)]
struct PaneGeometry {
    root: Rect,
    query: Rect,
    results: Rect,
    git_graph: Option<Rect>,
    git_commit: Option<Rect>,
    turn_preview: Rect,
    turn_start: Option<Rect>,
    turn_end: Option<Rect>,
    browser_single: bool,
}

const PREVIEW_MAX_MATCHES: usize = 100;
const PREVIEW_MAX_LINES: usize = 5000;
const EVENT_BUFFER_MAX_BYTES: usize = 50 * 1024 * 1024;
const MIN_RESULTS_PCT: u16 = 12;
const MIN_GIT_PCT: u16 = 16;
const MIN_TURNS_PCT: u16 = 28;
const MIN_GRAPH_PCT: u16 = 20;
const MAX_GRAPH_PCT: u16 = 80;
impl LayoutPreset {
    fn next(self) -> Self {
        match self {
            Self::Balanced => Self::TurnsWide,
            Self::TurnsWide => Self::GitWide,
            Self::GitWide => Self::Balanced,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Balanced => "balanced",
            Self::TurnsWide => "turns-wide",
            Self::GitWide => "git-wide",
        }
    }
}

impl LayoutState {
    fn new() -> Self {
        let preset = LayoutPreset::Balanced;
        let ratios = preset.default_ratios();
        Self {
            preset,
            results_pct: ratios.results_pct,
            git_pct: ratios.git_pct,
            graph_pct: ratios.graph_pct,
        }
    }

    fn reset_to_preset(&mut self) {
        let ratios = self.preset.default_ratios();
        self.results_pct = ratios.results_pct;
        self.git_pct = ratios.git_pct;
        self.graph_pct = ratios.graph_pct;
    }
}

impl LayoutPreset {
    fn default_ratios(self) -> LayoutRatios {
        match self {
            Self::Balanced => LayoutRatios {
                results_pct: 20,
                git_pct: 28,
                graph_pct: 65,
            },
            Self::TurnsWide => LayoutRatios {
                results_pct: 18,
                git_pct: 22,
                graph_pct: 65,
            },
            Self::GitWide => LayoutRatios {
                results_pct: 16,
                git_pct: 34,
                graph_pct: 65,
            },
        }
    }
}

fn build_session_index(all: &[MessageRecord]) -> (Vec<SessionSummary>, Vec<Vec<usize>>) {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    struct SessionKeyRef<'a> {
        source: SourceKind,
        session_id: &'a str,
        account: Option<&'a str>,
        machine_id: &'a str,
        origin: &'a str,
    }

    let mut by_session: HashMap<SessionKeyRef<'_>, SessionAgg<'_>> = HashMap::new();

    fn pick_first_idx(
        all: &[MessageRecord],
        indices: &[usize],
        mut pred: impl FnMut(&MessageRecord) -> bool,
    ) -> Option<usize> {
        let mut best_idx: Option<usize> = None;
        let mut best_ts: Option<&str> = None;

        for &idx in indices {
            let rec = &all[idx];
            if !pred(rec) {
                continue;
            }

            let ts = rec.timestamp.as_deref();
            let is_earlier = match (ts, best_ts) {
                (Some(ts), Some(cur)) => ts_cmp_str(ts, cur) == cmp::Ordering::Less,
                (Some(_), None) => true,
                (None, None) => best_idx.is_none(),
                (None, Some(_)) => false,
            };

            if best_idx.is_none() || is_earlier {
                best_idx = Some(idx);
                best_ts = ts;
            }
        }

        best_idx
    }

    for (idx, rec) in all.iter().enumerate() {
        let Some(session_id) = rec.session_id.as_deref() else {
            continue;
        };

        let entry = by_session
            .entry(SessionKeyRef {
                source: rec.source,
                session_id,
                account: rec.account.as_deref(),
                machine_id: &rec.machine_id,
                origin: &rec.origin,
            })
            .or_default();

        entry.record_indices.push(idx);

        if let Some(ts) = rec.timestamp.as_deref()
            && entry
                .last_ts
                .is_none_or(|cur| ts_cmp_str(ts, cur) == cmp::Ordering::Greater)
        {
            entry.last_ts = Some(ts);
        }

        if entry.cwd.is_none_or(|s| s.trim().is_empty())
            && let Some(cwd) = rec.cwd.as_deref()
            && !cwd.trim().is_empty()
        {
            entry.cwd = Some(cwd);
        }
        if entry.project_slug.is_none()
            && let Some(project_slug) = rec.project_slug.as_deref()
            && !project_slug.trim().is_empty()
        {
            entry.project_slug = Some(project_slug);
        }
        if entry.machine_id.is_none() && !rec.machine_id.trim().is_empty() {
            entry.machine_id = Some(&rec.machine_id);
        }
        if entry.machine_name.is_none() && !rec.machine_name.trim().is_empty() {
            entry.machine_name = Some(&rec.machine_name);
        }
        if entry.origin.is_none() && !rec.origin.trim().is_empty() {
            entry.origin = Some(&rec.origin);
        }
    }

    let mut items: Vec<(SessionSummary, Vec<usize>)> = Vec::new();

    for (key, agg) in by_session {
        let first_user_idx = pick_first_idx(all, &agg.record_indices, |rec| match key.source {
            SourceKind::CodexHistoryJsonl => true,
            _ => rec.role == Role::User && !is_noise_user_message(&rec.text),
        });
        let Some(first_user_idx) = first_user_idx else {
            continue;
        };

        items.push((
            SessionSummary {
                source: key.source,
                session_id: key.session_id.to_string(),
                account: key.account.map(|s| s.to_string()),
                machine_id: key.machine_id.to_string(),
                machine_name: agg.machine_name.unwrap_or(key.machine_id).to_string(),
                origin: key.origin.to_string(),
                project_slug: agg.project_slug.map(|s| s.to_string()),
                first_user_idx,
                last_ts: agg.last_ts.map(|s| s.to_string()),
                cwd: agg.cwd.map(|s| s.to_string()),
                dir: dir_name_from_cwd(
                    agg.cwd
                        .or_else(|| all[first_user_idx].cwd.as_deref())
                        .unwrap_or(""),
                ),
                first_line: {
                    let rec = &all[first_user_idx];
                    let s = if rec.source == SourceKind::CodexSessionJsonl
                        && looks_like_codex_title_task_prompt(&rec.text)
                    {
                        extract_user_prompt_line_from_codex_title_task(&rec.text)
                            .unwrap_or_else(|| first_non_empty_line(&rec.text).to_string())
                    } else {
                        first_non_empty_line(&rec.text).to_string()
                    };
                    s.replace(['\t', '\n'], " ")
                },
            },
            agg.record_indices,
        ));
    }

    items.sort_by(|(a, _), (b, _)| {
        ts_cmp_opt(a.last_ts.as_deref(), b.last_ts.as_deref())
            .reverse()
            .then_with(|| a.machine_id.cmp(&b.machine_id))
            .then_with(|| a.origin.cmp(&b.origin))
            .then_with(|| a.account.cmp(&b.account))
            .then_with(|| a.session_id.cmp(&b.session_id))
            .then_with(|| source_sort_key(a.source).cmp(&source_sort_key(b.source)))
    });

    let (sessions, records): (Vec<_>, Vec<_>) = items.into_iter().unzip();
    (sessions, records)
}

fn source_sort_key(source: SourceKind) -> u8 {
    match source {
        SourceKind::CodexSessionJsonl => 0,
        SourceKind::CodexHistoryJsonl => 1,
        SourceKind::ClaudeProjectJsonl => 2,
        SourceKind::OpenCodeSession => 3,
    }
}

fn provider_label(source: SourceKind, account: Option<&str>) -> String {
    let base = match source {
        SourceKind::ClaudeProjectJsonl => "C",
        SourceKind::CodexSessionJsonl | SourceKind::CodexHistoryJsonl => "O",
        SourceKind::OpenCodeSession => "OC",
    };
    match account.map(str::trim).filter(|s| !s.is_empty()) {
        Some(account) => format!("{base} {account}"),
        None => base.to_string(),
    }
}

fn source_label(source: SourceKind) -> &'static str {
    match source {
        SourceKind::CodexSessionJsonl => "codex_session",
        SourceKind::CodexHistoryJsonl => "codex_history",
        SourceKind::ClaudeProjectJsonl => "claude_project",
        SourceKind::OpenCodeSession => "opencode_session",
    }
}

fn image_output_dir() -> PathBuf {
    std::env::temp_dir().join("agent-history-images")
}

fn file_extension_for_media_type(media_type: &str) -> &'static str {
    match media_type {
        "image/png" => "png",
        "image/jpeg" => "jpg",
        "image/webp" => "webp",
        "image/gif" => "gif",
        "image/bmp" => "bmp",
        _ => "bin",
    }
}

fn materialize_record_images(rec: &MessageRecord) -> Vec<String> {
    let mut out = Vec::new();
    let base = image_output_dir();
    let _ = fs::create_dir_all(&base);

    for (idx, image) in rec.images.iter().enumerate() {
        match image {
            ImageAttachment {
                kind: ImageAttachmentKind::LocalPath { path },
                label,
            } => {
                let exists = if path.exists() { "" } else { " (missing)" };
                let label_prefix = label
                    .as_deref()
                    .map(|label| format!("{label}: "))
                    .unwrap_or_default();
                out.push(format!(
                    "{label_prefix}{}{}",
                    file_url_for_path(path),
                    exists
                ));
            }
            ImageAttachment {
                kind:
                    ImageAttachmentKind::DataUrl {
                        media_type,
                        data_url,
                    },
                label,
            } => {
                let session = rec.session_id.as_deref().unwrap_or("session");
                let filename = format!(
                    "{}-{}-{}.{}",
                    sanitize_for_filename(session),
                    rec.line,
                    idx + 1,
                    file_extension_for_media_type(media_type)
                );
                let path = base.join(filename);
                if !path.exists() {
                    let Some((_, encoded)) = data_url.split_once(',') else {
                        continue;
                    };
                    let Ok(bytes) = STANDARD.decode(encoded) else {
                        continue;
                    };
                    let _ = fs::write(&path, bytes);
                }
                let label_prefix = label
                    .as_deref()
                    .map(|label| format!("{label}: "))
                    .unwrap_or_default();
                out.push(format!("{label_prefix}{}", file_url_for_path(&path)));
            }
        }
    }

    out
}

fn file_url_for_path(path: &Path) -> String {
    let mut encoded = String::from("file://");
    let raw = path.to_string_lossy();
    for b in raw.as_bytes() {
        match *b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' | b':' => {
                encoded.push(*b as char)
            }
            _ => encoded.push_str(&format!("%{:02X}", b)),
        }
    }
    encoded
}

fn sanitize_for_filename(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "image".to_string()
    } else {
        out
    }
}

fn wrapped_line_height(line: &Line<'_>, width: usize) -> usize {
    if width == 0 {
        return 1;
    }
    cmp::max(1, line.width().div_ceil(width))
}

fn preview_visual_line_count(lines: &[Line<'_>], width: usize) -> usize {
    lines
        .iter()
        .map(|line| wrapped_line_height(line, width))
        .sum()
}

fn preview_visual_line_offset(lines: &[Line<'_>], raw_line_index: usize, width: usize) -> usize {
    lines
        .iter()
        .take(raw_line_index.min(lines.len()))
        .map(|line| wrapped_line_height(line, width))
        .sum()
}

fn preview_raw_line_index_for_visual_offset(
    lines: &[Line<'_>],
    width: usize,
    visual_offset: usize,
) -> usize {
    if lines.is_empty() {
        return 0;
    }
    let mut acc = 0usize;
    for (idx, line) in lines.iter().enumerate() {
        let height = wrapped_line_height(line, width);
        if visual_offset < acc.saturating_add(height) {
            return idx;
        }
        acc = acc.saturating_add(height);
    }
    lines.len().saturating_sub(1)
}

fn preview_section_start_lines(line_record_indices: &[Option<usize>]) -> Vec<usize> {
    let mut starts = Vec::new();
    let mut last = None;
    for (idx, record_idx) in line_record_indices.iter().copied().enumerate() {
        if record_idx.is_some() && record_idx != last {
            starts.push(idx);
        }
        last = record_idx;
    }
    starts
}

fn preview_selected_section_start_line(
    doc: &PreviewDoc,
    width: usize,
    scroll: usize,
) -> Option<usize> {
    let starts = preview_section_start_lines(&doc.line_record_indices);
    if starts.is_empty() {
        return None;
    }
    starts
        .iter()
        .copied()
        .take_while(|&line| preview_visual_line_offset(&doc.lines, line, width) <= scroll)
        .last()
        .or_else(|| starts.first().copied())
}

fn preview_selected_record_idx(doc: &PreviewDoc, width: usize, scroll: usize) -> Option<usize> {
    let raw = preview_selected_section_start_line(doc, width, scroll)?;
    doc.line_record_indices.get(raw).and_then(|idx| *idx)
}

fn preview_section_record_indices(doc: &PreviewDoc) -> Vec<usize> {
    preview_section_start_lines(&doc.line_record_indices)
        .into_iter()
        .filter_map(|line| doc.line_record_indices.get(line).and_then(|idx| *idx))
        .collect()
}

fn preview_center_raw_line(
    lines: &[Line<'_>],
    width: usize,
    scroll: usize,
    inner_height: usize,
) -> usize {
    let center_offset = scroll.saturating_add(inner_height / 2);
    preview_raw_line_index_for_visual_offset(lines, width, center_offset)
}

fn short_ts(ts: Option<&str>) -> String {
    let ts = ts.unwrap_or("");
    if let Some(formatted) = display_timestamp(ts) {
        return formatted;
    }
    ts.get(0..19).unwrap_or(ts).to_string()
}

fn parse_timestamp_epoch(ts: &str) -> Option<i64> {
    if let Ok(n) = ts.parse::<i64>() {
        let abs = n.unsigned_abs();
        return Some(if abs >= 1_000_000_000_000 {
            n / 1000
        } else {
            n
        });
    }
    let trimmed = ts.trim();
    let date_end = trimmed.find('T')?;
    let time_start = date_end + 1;
    let year = trimmed.get(0..4)?.parse::<i32>().ok()?;
    let month = trimmed.get(5..7)?.parse::<u32>().ok()?;
    let day = trimmed.get(8..10)?.parse::<u32>().ok()?;
    let hour = trimmed
        .get(time_start..time_start + 2)?
        .parse::<i64>()
        .ok()?;
    let minute = trimmed
        .get(time_start + 3..time_start + 5)?
        .parse::<i64>()
        .ok()?;
    let second = trimmed
        .get(time_start + 6..time_start + 8)?
        .parse::<i64>()
        .ok()?;
    let days = days_from_civil(year, month, day)?;
    Some(days * 86_400 + hour * 3_600 + minute * 60 + second)
}

fn first_non_empty_line(s: &str) -> &str {
    for line in s.lines() {
        let line = line.trim();
        if !line.is_empty() {
            return line;
        }
    }
    ""
}

fn compact_single_line(s: &str) -> String {
    first_non_empty_line(s).replace(['\t', '\n'], " ")
}

fn telemetry_metric_u64(data: &Value, key: &str) -> Option<u64> {
    data.get(key).and_then(|value| value.as_u64())
}

fn telemetry_metric_bool(data: &Value, key: &str) -> Option<bool> {
    data.get(key).and_then(|value| value.as_bool())
}

fn telemetry_metric_str<'a>(data: &'a Value, key: &str) -> Option<&'a str> {
    data.get(key).and_then(|value| value.as_str())
}

fn log_group_label(group: LogGroup) -> &'static str {
    group.as_str()
}

fn log_group_toggle_key(group: LogGroup) -> &'static str {
    match group {
        LogGroup::Perf => "Ctrl+g",
    }
}

fn format_telemetry_event_line(record: &telemetry::EventRecord) -> String {
    let ts = short_ts(Some(&record.ts_ms.to_string()));
    let kind = record.kind.as_str();
    let data = &record.data;
    let prefix = record
        .group
        .as_deref()
        .map(|group| format!("[{group}] "))
        .unwrap_or_default();

    let summary = match kind {
        "indexer_started" => format!(
            "files={} cache={} rebuild={}",
            telemetry_metric_u64(data, "total_files").unwrap_or(0),
            data.get("cache_enabled")
                .and_then(|value| value.as_bool())
                .unwrap_or(false),
            data.get("rebuild_index")
                .and_then(|value| value.as_bool())
                .unwrap_or(false)
        ),
        "cache_open_finished" => format!(
            "ms={} bytes={}",
            telemetry_metric_u64(data, "duration_ms").unwrap_or(0),
            telemetry_metric_u64(data, "cache_bytes").unwrap_or(0)
        ),
        "cache_load_finished" | "cache_reload_finished" => format!(
            "ms={} records={} sessions={} bytes={}",
            telemetry_metric_u64(data, "duration_ms").unwrap_or(0),
            telemetry_metric_u64(data, "records").unwrap_or(0),
            telemetry_metric_u64(data, "sessions").unwrap_or(0),
            telemetry_metric_u64(data, "cache_bytes").unwrap_or(0)
        ),
        "fingerprint_scan_finished" => format!(
            "ms={} total={} cached={}",
            telemetry_metric_u64(data, "duration_ms").unwrap_or(0),
            telemetry_metric_u64(data, "total_units").unwrap_or(0),
            telemetry_metric_u64(data, "cached_units").unwrap_or(0)
        ),
        "refresh_finished" => format!(
            "ms={} skipped={} refreshed={} failed={}",
            telemetry_metric_u64(data, "duration_ms").unwrap_or(0),
            telemetry_metric_u64(data, "skipped_units").unwrap_or(0),
            telemetry_metric_u64(data, "refreshed_units").unwrap_or(0),
            telemetry_metric_u64(data, "failed_units").unwrap_or(0)
        ),
        "unit_reindexed" => {
            let mut suffix = format!(
                "ms={} recs={} msgs={} parts={} text_parts={} parse_failures={}",
                telemetry_metric_u64(data, "duration_ms").unwrap_or(0),
                telemetry_metric_u64(data, "records").unwrap_or(0),
                telemetry_metric_u64(data, "message_files").unwrap_or(0),
                telemetry_metric_u64(data, "part_files").unwrap_or(0),
                telemetry_metric_u64(data, "text_parts").unwrap_or(0),
                telemetry_metric_u64(data, "part_parse_failures").unwrap_or(0)
            );
            if telemetry_metric_bool(data, "used_title_fallback").unwrap_or(false) {
                suffix.push_str(" title_fallback=true");
            }
            if let Some(path) = telemetry_metric_str(data, "path") {
                suffix.push_str(&format!(" path={}", truncate_middle(path, 60)));
            }
            suffix
        }
        "unit_failed" => format!(
            "error={} path={}",
            telemetry_metric_str(data, "error").unwrap_or(""),
            truncate_middle(telemetry_metric_str(data, "path").unwrap_or(""), 60)
        ),
        "indexer_finished" | "full_scan_finished" => format!(
            "ms={} records={} sessions={}",
            telemetry_metric_u64(data, "duration_ms").unwrap_or(0),
            telemetry_metric_u64(data, "records").unwrap_or(0),
            telemetry_metric_u64(data, "sessions").unwrap_or(0)
        ),
        "log_groups_help" => telemetry_metric_str(data, "summary")
            .unwrap_or("While Events is open, Ctrl+g toggles perf logging.")
            .to_string(),
        "log_path_info" => format!(
            "events log path={}",
            telemetry_metric_str(data, "path").unwrap_or("")
        ),
        "ui_session_started" => format!(
            "================ agent-history ui started pid={} cwd={} ================",
            telemetry_metric_u64(data, "pid").unwrap_or(0),
            truncate_middle(telemetry_metric_str(data, "cwd").unwrap_or(""), 60)
        ),
        "log_group_toggle" => format!(
            "group={} enabled={} hotkey={} {}",
            telemetry_metric_str(data, "group").unwrap_or(""),
            telemetry_metric_bool(data, "enabled").unwrap_or(false),
            telemetry_metric_str(data, "hotkey").unwrap_or(""),
            telemetry_metric_str(data, "summary").unwrap_or("")
        ),
        "perf_results_summary" => format!(
            "query={} ms={} candidates={} matched_sessions={} matched_turns={} total_sessions={} total_turns={} reused_prefix_cache={}",
            truncate_middle(telemetry_metric_str(data, "query").unwrap_or(""), 40),
            telemetry_metric_u64(data, "duration_ms").unwrap_or(0),
            telemetry_metric_u64(data, "candidate_sessions").unwrap_or(0),
            telemetry_metric_u64(data, "matched_sessions").unwrap_or(0),
            telemetry_metric_u64(data, "matched_turns").unwrap_or(0),
            telemetry_metric_u64(data, "total_sessions").unwrap_or(0),
            telemetry_metric_u64(data, "total_turns").unwrap_or(0),
            telemetry_metric_bool(data, "reused_prefix_cache").unwrap_or(false)
        ),
        "perf_results_phase" => format!(
            "phase={} ms={} candidates={} matched_sessions={}",
            telemetry_metric_str(data, "phase").unwrap_or(""),
            telemetry_metric_u64(data, "duration_ms").unwrap_or(0),
            telemetry_metric_u64(data, "candidate_sessions").unwrap_or(0),
            telemetry_metric_u64(data, "matched_sessions").unwrap_or(0)
        ),
        "perf_preview_summary" => format!(
            "mode={} session={} ms={} session_records={} matched_records={} rendered_records={} raw_lines={} rendered_lines={} first_match_line={} truncated_matches={} truncated_lines={}",
            telemetry_metric_str(data, "mode").unwrap_or(""),
            truncate_middle(telemetry_metric_str(data, "session_id").unwrap_or(""), 24),
            telemetry_metric_u64(data, "duration_ms").unwrap_or(0),
            telemetry_metric_u64(data, "session_records").unwrap_or(0),
            telemetry_metric_u64(data, "matched_records").unwrap_or(0),
            telemetry_metric_u64(data, "rendered_records").unwrap_or(0),
            telemetry_metric_u64(data, "raw_lines").unwrap_or(0),
            telemetry_metric_u64(data, "rendered_lines").unwrap_or(0),
            telemetry_metric_u64(data, "first_match_line").unwrap_or(0),
            telemetry_metric_bool(data, "truncated_by_match_limit").unwrap_or(false),
            telemetry_metric_bool(data, "truncated_by_line_limit").unwrap_or(false)
        ),
        "perf_preview_phase" => format!(
            "mode={} phase={} ms={} records={} lines={} bytes={}",
            telemetry_metric_str(data, "mode").unwrap_or(""),
            telemetry_metric_str(data, "phase").unwrap_or(""),
            telemetry_metric_u64(data, "duration_ms").unwrap_or(0),
            telemetry_metric_u64(data, "records").unwrap_or(0),
            telemetry_metric_u64(data, "lines").unwrap_or(0),
            telemetry_metric_u64(data, "bytes").unwrap_or(0)
        ),
        "remote_sync_started" => format!(
            "remote={} host={} started refresh_cmd={} rsync_cmd={}",
            telemetry_metric_str(data, "remote_name").unwrap_or(""),
            telemetry_metric_str(data, "host").unwrap_or("")
            ,
            truncate_middle(telemetry_metric_str(data, "refresh_cmd").unwrap_or(""), 80),
            truncate_middle(telemetry_metric_str(data, "rsync_cmd").unwrap_or(""), 80)
        ),
        "remote_sync_finished" => format!(
            "remote={} host={} ms={} records={} sessions={}",
            telemetry_metric_str(data, "remote_name").unwrap_or(""),
            telemetry_metric_str(data, "host").unwrap_or(""),
            telemetry_metric_u64(data, "duration_ms").unwrap_or(0),
            telemetry_metric_u64(data, "records").unwrap_or(0),
            telemetry_metric_u64(data, "sessions").unwrap_or(0)
        ),
        "remote_sync_failed" => format!(
            "remote={} host={} ms={} error={}",
            telemetry_metric_str(data, "remote_name").unwrap_or(""),
            telemetry_metric_str(data, "host").unwrap_or(""),
            telemetry_metric_u64(data, "duration_ms").unwrap_or(0),
            telemetry_metric_str(data, "error").unwrap_or("")
        ),
        _ => serde_json::to_string(data).unwrap_or_default(),
    };

    if ts.is_empty() {
        format!("{prefix}{kind}: {summary}")
    } else {
        format!("{ts}  {prefix}{kind}: {summary}")
    }
}

fn truncate_middle(s: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }

    let char_count = s.chars().count();
    if char_count <= max_chars {
        return s.to_string();
    }

    if max_chars <= 3 {
        return ".".repeat(max_chars);
    }

    let mut out = String::new();
    for ch in s.chars().take(max_chars - 3) {
        out.push(ch);
    }
    out.push_str("...");
    out
}

fn tag_style(fg: Color, bg: Color) -> Style {
    Style::default().fg(fg).bg(bg)
}

fn should_show_host_tag(sess: &SessionSummary, tags: &config::UiTagConfig) -> bool {
    tags.show_host && sess.origin != "local" && !sess.machine_name.trim().is_empty()
}

const REMOTE_ACCENT: Color = Color::Rgb(138, 112, 144);
const REMOTE_TAG_BG: Color = Color::Rgb(82, 70, 88);
const REMOTE_TAG_FG: Color = Color::Rgb(224, 216, 228);
const REMOTE_PREVIEW_BORDER_FG: Color = Color::Rgb(36, 36, 36);
const QUERY_MATCH_FG: Color = Color::Black;

const QUERY_MATCH_PALETTE: [(u8, u8, u8); 12] = [
    (224, 132, 120),
    (223, 156, 112),
    (220, 180, 110),
    (191, 176, 104),
    (153, 179, 108),
    (112, 180, 130),
    (102, 178, 158),
    (101, 166, 188),
    (110, 146, 205),
    (136, 132, 206),
    (171, 125, 194),
    (198, 121, 171),
];

fn query_match_style_for(term_index: usize) -> Style {
    let (red, green, blue) = QUERY_MATCH_PALETTE[term_index % QUERY_MATCH_PALETTE.len()];
    Style::default()
        .fg(QUERY_MATCH_FG)
        .bg(Color::Rgb(red, green, blue))
        .add_modifier(Modifier::BOLD)
}

fn session_tag_specs(sess: &SessionSummary, tags: &config::UiTagConfig) -> Vec<SessionTagSpec> {
    let mut specs = Vec::new();
    if tags.show_provider {
        let value = provider_label(sess.source, sess.account.as_deref());
        specs.push(SessionTagSpec {
            filter: TagFilter {
                kind: TagFilterKind::Provider,
                value: value.clone(),
            },
            content: format!(" {value} "),
            style: tag_style(Color::Rgb(210, 217, 224), Color::Rgb(64, 78, 92)),
        });
    }
    if should_show_host_tag(sess, tags) {
        specs.push(SessionTagSpec {
            filter: TagFilter {
                kind: TagFilterKind::Host,
                value: sess.machine_name.clone(),
            },
            content: format!(" {} ", sess.machine_name),
            style: tag_style(REMOTE_TAG_FG, REMOTE_TAG_BG),
        });
    }
    if tags.show_project
        && let Some(project) = sess
            .project_slug
            .as_deref()
            .filter(|s| !s.trim().is_empty())
    {
        specs.push(SessionTagSpec {
            filter: TagFilter {
                kind: TagFilterKind::Project,
                value: project.to_string(),
            },
            content: format!(" {project} "),
            style: tag_style(Color::Rgb(224, 216, 194), Color::Rgb(92, 86, 64)),
        });
    }
    specs
}

fn session_tag_spans(sess: &SessionSummary, tags: &config::UiTagConfig) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    for spec in session_tag_specs(sess, tags) {
        spans.push(Span::styled(spec.content, spec.style));
        spans.push(Span::raw(" "));
    }
    spans
}

fn result_line_text(
    sess: &SessionSummary,
    matched: Option<&MessageRecord>,
    hit_count: usize,
) -> String {
    let ts = short_ts(sess.last_ts.as_deref());
    let dir_prefix = match sess.project_slug.as_deref() {
        Some(project) if project == sess.dir => String::new(),
        _ => format!("[{}] ", sess.dir),
    };
    let prefix = format!("{ts} {dir_prefix}{}", sess.first_line);

    let Some(rec) = matched else {
        return prefix;
    };

    let snippet = truncate_middle(&compact_single_line(&rec.text), 72);
    if snippet.is_empty() || snippet == sess.first_line {
        if hit_count > 1 {
            return format!("{prefix} [{hit_count} hits]");
        }
        return prefix;
    }

    if hit_count > 1 {
        format!("{prefix} :: {snippet} [{hit_count} hits]")
    } else {
        format!("{prefix} :: {snippet}")
    }
}

fn result_line(
    sess: &SessionSummary,
    matched: Option<&MessageRecord>,
    hit_count: usize,
    query: &str,
    ui_tags: &config::UiTagConfig,
    base_style: Style,
) -> Line<'static> {
    let full_text = result_line_text(sess, matched, hit_count);
    let ts = short_ts(sess.last_ts.as_deref());
    let rest = full_text
        .strip_prefix(&ts)
        .unwrap_or(&full_text)
        .trim_start();
    let tag_spans = session_tag_spans(sess, ui_tags);

    let mut spans: Vec<Span<'static>> = vec![Span::styled(ts, base_style)];
    if !tag_spans.is_empty() {
        spans.push(Span::raw(" "));
    } else if !rest.is_empty() {
        spans.push(Span::styled(" ".to_string(), base_style));
    }
    spans.extend(tag_spans);
    if !rest.is_empty() {
        let text_line = highlighted_line(rest, query, base_style);
        spans.extend(text_line.spans);
    }
    Line::from(spans)
}

fn highlighted_line(text: &str, query: &str, base_style: Style) -> Line<'static> {
    Line::from(highlight_spans(
        vec![Span::styled(text.to_string(), base_style)],
        query,
    ))
}

#[derive(Clone, Copy)]
struct MarkdownTheme {
    heading_markers: Style,
    headings: [Style; 6],
    quote_markers: Style,
    quote_text: Style,
    list_markers: Style,
    rule: Style,
    inline_code: Style,
    markdown_markers: Style,
    link_text: Style,
    link_url: Style,
    table_pipes: Style,
    table_rule: Style,
    code_fence: Style,
    code_text: Style,
    code_keyword: Style,
    code_string: Style,
    code_comment: Style,
    code_number: Style,
    code_symbol: Style,
    code_variable: Style,
    code_key: Style,
    emphasis: Style,
    strong: Style,
    strike: Style,
}

impl MarkdownTheme {
    fn new(base_style: Style) -> Self {
        Self {
            heading_markers: base_style.fg(Color::DarkGray).add_modifier(Modifier::BOLD),
            headings: [
                base_style.fg(Color::Yellow).add_modifier(Modifier::BOLD),
                base_style.fg(Color::Cyan).add_modifier(Modifier::BOLD),
                base_style.fg(Color::Green).add_modifier(Modifier::BOLD),
                base_style.fg(Color::Magenta).add_modifier(Modifier::BOLD),
                base_style.fg(Color::LightBlue).add_modifier(Modifier::BOLD),
                base_style.fg(Color::LightCyan).add_modifier(Modifier::BOLD),
            ],
            quote_markers: base_style.fg(Color::DarkGray).add_modifier(Modifier::BOLD),
            quote_text: base_style.fg(Color::Gray).add_modifier(Modifier::ITALIC),
            list_markers: base_style.fg(Color::Cyan).add_modifier(Modifier::BOLD),
            rule: base_style.fg(Color::DarkGray),
            inline_code: base_style
                .fg(Color::Yellow)
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
            markdown_markers: base_style.fg(Color::DarkGray),
            link_text: base_style
                .fg(Color::Blue)
                .add_modifier(Modifier::UNDERLINED),
            link_url: base_style.fg(Color::Cyan),
            table_pipes: base_style.fg(Color::DarkGray),
            table_rule: base_style.fg(Color::DarkGray),
            code_fence: base_style.fg(Color::DarkGray),
            code_text: base_style.fg(Color::LightYellow),
            code_keyword: base_style.fg(Color::Cyan).add_modifier(Modifier::BOLD),
            code_string: base_style.fg(Color::Green),
            code_comment: base_style
                .fg(Color::DarkGray)
                .add_modifier(Modifier::ITALIC),
            code_number: base_style.fg(Color::Magenta),
            code_symbol: base_style.fg(Color::Yellow),
            code_variable: base_style.fg(Color::LightBlue),
            code_key: base_style.fg(Color::Yellow).add_modifier(Modifier::BOLD),
            emphasis: base_style.add_modifier(Modifier::ITALIC),
            strong: base_style.add_modifier(Modifier::BOLD),
            strike: base_style.add_modifier(Modifier::CROSSED_OUT),
        }
    }

    fn heading_style(&self, level: usize) -> Style {
        self.headings[level.saturating_sub(1).min(self.headings.len() - 1)]
    }
}

#[derive(Clone, Debug)]
struct CodeFence {
    delimiter: char,
    marker_len: usize,
    language: Option<CodeLanguage>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CodeLanguage {
    Rust,
    Python,
    JavaScript,
    TypeScript,
    Json,
    Shell,
}

fn highlight_spans(spans: Vec<Span<'static>>, query: &str) -> Vec<Span<'static>> {
    if query.is_empty() {
        return spans;
    }

    let full_text = spans
        .iter()
        .map(|span| span.content.as_ref())
        .collect::<String>();
    let ranges = search::find_term_match_ranges(query, &full_text);
    if ranges.is_empty() {
        return spans;
    }

    let mut out: Vec<Span<'static>> = Vec::new();
    let mut absolute = 0usize;
    let mut range_idx = 0usize;

    for span in spans {
        let style = span.style;
        let content = span.content.into_owned();
        let span_start = absolute;
        let span_end = span_start + content.len();
        let mut local = 0usize;

        while range_idx < ranges.len() && ranges[range_idx].end <= span_start {
            range_idx += 1;
        }

        let mut cur = range_idx;
        while cur < ranges.len() && ranges[cur].start < span_end {
            let start = ranges[cur].start.max(span_start) - span_start;
            let end = ranges[cur].end.min(span_end) - span_start;

            if start > local {
                push_styled_text(&mut out, content[local..start].to_string(), style);
            }
            if end > start {
                push_styled_text(
                    &mut out,
                    content[start..end].to_string(),
                    style.patch(query_match_style_for(ranges[cur].term_index)),
                );
            }
            local = end;

            if ranges[cur].end <= span_end {
                cur += 1;
            } else {
                break;
            }
        }

        if local < content.len() {
            push_styled_text(&mut out, content[local..].to_string(), style);
        }

        absolute = span_end;
        range_idx = cur;
    }

    out
}

fn render_preview_message_lines(text: &str, query: &str, base_style: Style) -> Vec<Line<'static>> {
    let theme = MarkdownTheme::new(base_style);
    let mut lines = Vec::new();
    let mut code_fence: Option<CodeFence> = None;

    for raw_line in text.lines() {
        let spans = if let Some(fence) = code_fence.as_ref() {
            if let Some((delimiter, marker_len, _)) = parse_fence(raw_line)
                && delimiter == fence.delimiter
                && marker_len >= fence.marker_len
                && raw_line.trim_start()[marker_len..].trim().is_empty()
            {
                let spans = render_fence_line(raw_line, &theme);
                code_fence = None;
                spans
            } else {
                render_code_line(raw_line, fence.language, &theme)
            }
        } else if let Some((delimiter, marker_len, info)) = parse_fence(raw_line) {
            code_fence = Some(CodeFence {
                delimiter,
                marker_len,
                language: parse_code_language(info),
            });
            render_fence_line(raw_line, &theme)
        } else {
            render_markdown_line(raw_line, &theme)
        };

        lines.push(Line::from(highlight_spans(spans, query)));
    }

    if lines.is_empty() {
        lines.push(Line::raw(""));
    }

    lines
}

fn render_markdown_line(line: &str, theme: &MarkdownTheme) -> Vec<Span<'static>> {
    if line.is_empty() {
        return vec![Span::raw("")];
    }

    if is_thematic_break(line) {
        return vec![Span::styled(line.to_string(), theme.rule)];
    }

    if let Some((indent, level, content)) = parse_heading(line) {
        let mut spans = Vec::new();
        push_styled_text(&mut spans, indent.to_string(), Style::default());
        push_styled_text(&mut spans, "#".repeat(level), theme.heading_markers);
        if line.trim_start().len() > level {
            push_styled_text(&mut spans, " ".to_string(), theme.heading_markers);
        }
        spans.extend(render_inline_markdown(
            content,
            theme.heading_style(level),
            theme,
        ));
        return spans;
    }

    if let Some((prefix, content)) = parse_blockquote(line) {
        let mut spans = Vec::new();
        push_styled_text(&mut spans, prefix.to_string(), theme.quote_markers);
        spans.extend(render_inline_markdown(content, theme.quote_text, theme));
        return spans;
    }

    if let Some((indent, marker, content)) = parse_list_marker(line) {
        let mut spans = Vec::new();
        push_styled_text(&mut spans, indent.to_string(), Style::default());
        push_styled_text(&mut spans, marker.to_string(), theme.list_markers);
        spans.extend(render_inline_markdown(content, Style::default(), theme));
        return spans;
    }

    if is_table_rule(line) {
        return render_table_line(line, theme, theme.table_rule);
    }

    if line.contains('|') {
        return render_table_line(line, theme, Style::default());
    }

    render_inline_markdown(line, Style::default(), theme)
}

fn render_table_line(line: &str, theme: &MarkdownTheme, text_style: Style) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    let mut start = 0usize;
    for (idx, ch) in line.char_indices() {
        if ch != '|' {
            continue;
        }
        if start < idx {
            spans.extend(render_inline_markdown(&line[start..idx], text_style, theme));
        }
        push_styled_text(&mut spans, "|".to_string(), theme.table_pipes);
        start = idx + ch.len_utf8();
    }
    if start < line.len() {
        spans.extend(render_inline_markdown(&line[start..], text_style, theme));
    }
    spans
}

fn render_fence_line(line: &str, theme: &MarkdownTheme) -> Vec<Span<'static>> {
    let Some((_, marker_len, info)) = parse_fence(line) else {
        return vec![Span::styled(line.to_string(), theme.code_fence)];
    };
    let trimmed = line.trim_start();
    let indent = &line[..line.len() - trimmed.len()];
    let marker = &trimmed[..marker_len];
    let rest = &trimmed[marker_len..];

    let mut spans = Vec::new();
    push_styled_text(&mut spans, indent.to_string(), Style::default());
    push_styled_text(&mut spans, marker.to_string(), theme.code_fence);
    if !rest.is_empty() {
        let info_padding_len = rest.len().saturating_sub(rest.trim_start().len());
        if info_padding_len > 0 {
            push_styled_text(
                &mut spans,
                rest[..info_padding_len].to_string(),
                theme.code_fence,
            );
        }
        let info_text = rest[info_padding_len..].trim_end();
        if !info_text.is_empty() {
            push_styled_text(&mut spans, info_text.to_string(), theme.code_keyword);
        }
        let trailing_len = rest
            .len()
            .saturating_sub(info_padding_len + info_text.len());
        if trailing_len > 0 {
            push_styled_text(
                &mut spans,
                rest[rest.len() - trailing_len..].to_string(),
                theme.code_fence,
            );
        }
    }
    let _ = info;
    spans
}

fn render_code_line(
    line: &str,
    language: Option<CodeLanguage>,
    theme: &MarkdownTheme,
) -> Vec<Span<'static>> {
    match language {
        Some(CodeLanguage::Rust) => render_clike_code_line(line, theme, &RUST_KEYWORDS, true),
        Some(CodeLanguage::JavaScript) => {
            render_clike_code_line(line, theme, &JAVASCRIPT_KEYWORDS, true)
        }
        Some(CodeLanguage::TypeScript) => {
            render_clike_code_line(line, theme, &TYPESCRIPT_KEYWORDS, true)
        }
        Some(CodeLanguage::Python) => render_python_code_line(line, theme),
        Some(CodeLanguage::Json) => render_json_code_line(line, theme),
        Some(CodeLanguage::Shell) => render_shell_code_line(line, theme),
        None => vec![Span::styled(line.to_string(), theme.code_text)],
    }
}

fn render_clike_code_line(
    line: &str,
    theme: &MarkdownTheme,
    keywords: &[&str],
    backtick_strings: bool,
) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    let mut idx = 0usize;

    while idx < line.len() {
        let rest = &line[idx..];
        if rest.starts_with("//") {
            push_styled_text(&mut spans, rest.to_string(), theme.code_comment);
            break;
        }

        let ch = rest.chars().next().unwrap_or_default();
        if ch == '"' || ch == '\'' || (backtick_strings && ch == '`') {
            let end = consume_quoted(line, idx, ch);
            push_styled_text(&mut spans, line[idx..end].to_string(), theme.code_string);
            idx = end;
            continue;
        }

        if let Some(end) = consume_number(line, idx) {
            push_styled_text(&mut spans, line[idx..end].to_string(), theme.code_number);
            idx = end;
            continue;
        }

        if let Some(end) = consume_identifier(line, idx) {
            let ident = &line[idx..end];
            let style = if keywords.contains(&ident) {
                theme.code_keyword
            } else {
                theme.code_text
            };
            push_styled_text(&mut spans, ident.to_string(), style);
            idx = end;
            continue;
        }

        let end = idx + ch.len_utf8();
        let style = if is_code_symbol(ch) {
            theme.code_symbol
        } else {
            theme.code_text
        };
        push_styled_text(&mut spans, line[idx..end].to_string(), style);
        idx = end;
    }

    spans
}

fn render_python_code_line(line: &str, theme: &MarkdownTheme) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    let mut idx = 0usize;

    while idx < line.len() {
        let rest = &line[idx..];
        if rest.starts_with('#') {
            push_styled_text(&mut spans, rest.to_string(), theme.code_comment);
            break;
        }

        let ch = rest.chars().next().unwrap_or_default();
        if ch == '"' || ch == '\'' {
            let end = consume_quoted(line, idx, ch);
            push_styled_text(&mut spans, line[idx..end].to_string(), theme.code_string);
            idx = end;
            continue;
        }

        if let Some(end) = consume_number(line, idx) {
            push_styled_text(&mut spans, line[idx..end].to_string(), theme.code_number);
            idx = end;
            continue;
        }

        if let Some(end) = consume_identifier(line, idx) {
            let ident = &line[idx..end];
            let style = if PYTHON_KEYWORDS.contains(&ident) {
                theme.code_keyword
            } else {
                theme.code_text
            };
            push_styled_text(&mut spans, ident.to_string(), style);
            idx = end;
            continue;
        }

        let end = idx + ch.len_utf8();
        let style = if is_code_symbol(ch) {
            theme.code_symbol
        } else {
            theme.code_text
        };
        push_styled_text(&mut spans, line[idx..end].to_string(), style);
        idx = end;
    }

    spans
}

fn render_json_code_line(line: &str, theme: &MarkdownTheme) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    let mut idx = 0usize;

    while idx < line.len() {
        let rest = &line[idx..];
        let ch = rest.chars().next().unwrap_or_default();

        if ch == '"' {
            let end = consume_quoted(line, idx, ch);
            let style = if next_non_whitespace_char(line, end) == Some(':') {
                theme.code_key
            } else {
                theme.code_string
            };
            push_styled_text(&mut spans, line[idx..end].to_string(), style);
            idx = end;
            continue;
        }

        if let Some(end) = consume_number(line, idx) {
            push_styled_text(&mut spans, line[idx..end].to_string(), theme.code_number);
            idx = end;
            continue;
        }

        if let Some(end) = consume_identifier(line, idx) {
            let ident = &line[idx..end];
            let style = if ["true", "false", "null"].contains(&ident) {
                theme.code_keyword
            } else {
                theme.code_text
            };
            push_styled_text(&mut spans, ident.to_string(), style);
            idx = end;
            continue;
        }

        let end = idx + ch.len_utf8();
        let style = if is_code_symbol(ch) {
            theme.code_symbol
        } else {
            theme.code_text
        };
        push_styled_text(&mut spans, line[idx..end].to_string(), style);
        idx = end;
    }

    spans
}

fn render_shell_code_line(line: &str, theme: &MarkdownTheme) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    let mut idx = 0usize;

    while idx < line.len() {
        let rest = &line[idx..];
        if rest.starts_with('#') {
            push_styled_text(&mut spans, rest.to_string(), theme.code_comment);
            break;
        }

        if rest.starts_with("${") {
            let end = rest
                .find('}')
                .map(|end| idx + end + 1)
                .unwrap_or(line.len());
            push_styled_text(&mut spans, line[idx..end].to_string(), theme.code_variable);
            idx = end;
            continue;
        }

        let ch = rest.chars().next().unwrap_or_default();
        if ch == '$' {
            let end = consume_shell_variable(line, idx);
            push_styled_text(&mut spans, line[idx..end].to_string(), theme.code_variable);
            idx = end;
            continue;
        }

        if ch == '"' || ch == '\'' || ch == '`' {
            let end = consume_quoted(line, idx, ch);
            push_styled_text(&mut spans, line[idx..end].to_string(), theme.code_string);
            idx = end;
            continue;
        }

        if let Some(end) = consume_number(line, idx) {
            push_styled_text(&mut spans, line[idx..end].to_string(), theme.code_number);
            idx = end;
            continue;
        }

        if let Some(end) = consume_identifier(line, idx) {
            let ident = &line[idx..end];
            let style = if SHELL_KEYWORDS.contains(&ident) {
                theme.code_keyword
            } else {
                theme.code_text
            };
            push_styled_text(&mut spans, ident.to_string(), style);
            idx = end;
            continue;
        }

        let end = idx + ch.len_utf8();
        let style = if is_code_symbol(ch) {
            theme.code_symbol
        } else {
            theme.code_text
        };
        push_styled_text(&mut spans, line[idx..end].to_string(), style);
        idx = end;
    }

    spans
}

fn render_inline_markdown(
    text: &str,
    base_style: Style,
    theme: &MarkdownTheme,
) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    let mut plain = String::new();
    let mut idx = 0usize;

    while idx < text.len() {
        let rest = &text[idx..];

        if let Some((consumed, code)) = parse_inline_code(rest) {
            flush_plain_text(&mut spans, &mut plain, base_style);
            let marker_len = inline_code_marker_len(rest);
            push_styled_text(
                &mut spans,
                rest[..marker_len].to_string(),
                theme.markdown_markers,
            );
            push_styled_text(&mut spans, code.to_string(), theme.inline_code);
            push_styled_text(
                &mut spans,
                rest[consumed - marker_len..consumed].to_string(),
                theme.markdown_markers,
            );
            idx += consumed;
            continue;
        }

        if let Some((consumed, label, url)) = parse_inline_link(rest) {
            flush_plain_text(&mut spans, &mut plain, base_style);
            push_styled_text(&mut spans, "[".to_string(), theme.markdown_markers);
            spans.extend(render_inline_markdown(label, theme.link_text, theme));
            push_styled_text(&mut spans, "](".to_string(), theme.markdown_markers);
            push_styled_text(&mut spans, url.to_string(), theme.link_url);
            push_styled_text(&mut spans, ")".to_string(), theme.markdown_markers);
            idx += consumed;
            continue;
        }

        if let Some((consumed, inner)) = parse_wrapped_segment(rest, "~~") {
            flush_plain_text(&mut spans, &mut plain, base_style);
            push_styled_text(&mut spans, "~~".to_string(), theme.markdown_markers);
            spans.extend(render_inline_markdown(
                inner,
                base_style.patch(theme.strike),
                theme,
            ));
            push_styled_text(&mut spans, "~~".to_string(), theme.markdown_markers);
            idx += consumed;
            continue;
        }

        if let Some((consumed, inner)) = parse_wrapped_segment(rest, "**") {
            flush_plain_text(&mut spans, &mut plain, base_style);
            push_styled_text(&mut spans, "**".to_string(), theme.markdown_markers);
            spans.extend(render_inline_markdown(
                inner,
                base_style.patch(theme.strong),
                theme,
            ));
            push_styled_text(&mut spans, "**".to_string(), theme.markdown_markers);
            idx += consumed;
            continue;
        }

        if let Some((consumed, inner)) = parse_wrapped_segment(rest, "*") {
            flush_plain_text(&mut spans, &mut plain, base_style);
            push_styled_text(&mut spans, "*".to_string(), theme.markdown_markers);
            spans.extend(render_inline_markdown(
                inner,
                base_style.patch(theme.emphasis),
                theme,
            ));
            push_styled_text(&mut spans, "*".to_string(), theme.markdown_markers);
            idx += consumed;
            continue;
        }

        if let Some(ch) = rest.chars().next() {
            plain.push(ch);
            idx += ch.len_utf8();
        } else {
            break;
        }
    }

    flush_plain_text(&mut spans, &mut plain, base_style);
    spans
}

fn flush_plain_text(spans: &mut Vec<Span<'static>>, plain: &mut String, style: Style) {
    if plain.is_empty() {
        return;
    }
    push_styled_text(spans, mem::take(plain), style);
}

fn push_styled_text(spans: &mut Vec<Span<'static>>, text: String, style: Style) {
    if text.is_empty() {
        return;
    }

    if let Some(last) = spans.last_mut()
        && last.style == style
    {
        last.content.to_mut().push_str(&text);
        return;
    }

    spans.push(Span::styled(text, style));
}

fn parse_heading(line: &str) -> Option<(&str, usize, &str)> {
    let trimmed = line.trim_start_matches(' ');
    let indent_len = line.len() - trimmed.len();
    if indent_len > 3 {
        return None;
    }

    let marker_len = trimmed.chars().take_while(|&ch| ch == '#').count();
    if !(1..=6).contains(&marker_len) {
        return None;
    }
    let rest = &trimmed[marker_len..];
    let content = rest.strip_prefix(' ')?;
    Some((&line[..indent_len], marker_len, content))
}

fn parse_blockquote(line: &str) -> Option<(&str, &str)> {
    let trimmed = line.trim_start_matches(' ');
    let indent_len = line.len() - trimmed.len();
    if indent_len > 3 || !trimmed.starts_with('>') {
        return None;
    }

    let mut idx = indent_len;
    let bytes = line.as_bytes();
    while idx < line.len() && bytes[idx] == b'>' {
        idx += 1;
        if idx < line.len() && bytes[idx] == b' ' {
            idx += 1;
        }
    }

    Some((&line[..idx], &line[idx..]))
}

fn parse_list_marker(line: &str) -> Option<(&str, &str, &str)> {
    let trimmed = line.trim_start_matches(' ');
    let indent_len = line.len() - trimmed.len();
    let indent = &line[..indent_len];

    for marker in ["- ", "* ", "+ "] {
        if let Some(content) = trimmed.strip_prefix(marker) {
            let marker_start = indent_len;
            let marker_end = marker_start + marker.len();
            return Some((indent, &line[marker_start..marker_end], content));
        }
    }

    let digit_len = trimmed.chars().take_while(|ch| ch.is_ascii_digit()).count();
    if digit_len == 0 {
        return None;
    }
    let rest = &trimmed[digit_len..];
    if !(rest.starts_with(". ") || rest.starts_with(") ")) {
        return None;
    }

    let marker_start = indent_len;
    let marker_end = marker_start + digit_len + 2;
    Some((
        indent,
        &line[marker_start..marker_end],
        &trimmed[digit_len + 2..],
    ))
}

fn is_thematic_break(line: &str) -> bool {
    let trimmed = line.trim();
    if trimmed.len() < 3 {
        return false;
    }
    let marker = trimmed.chars().find(|ch| !ch.is_whitespace());
    let Some(marker) = marker else {
        return false;
    };
    if !matches!(marker, '-' | '*' | '_') {
        return false;
    }

    let marker_count = trimmed.chars().filter(|&ch| ch == marker).count();
    marker_count >= 3 && trimmed.chars().all(|ch| ch == marker || ch.is_whitespace())
}

fn is_table_rule(line: &str) -> bool {
    let trimmed = line.trim();
    if trimmed.is_empty() || !trimmed.contains('|') {
        return false;
    }

    trimmed
        .split('|')
        .filter(|part| !part.trim().is_empty())
        .all(|part| {
            let part = part.trim();
            let core = part.trim_start_matches(':').trim_end_matches(':');
            !core.is_empty() && core.chars().all(|ch| ch == '-')
        })
}

fn parse_fence(line: &str) -> Option<(char, usize, &str)> {
    let trimmed = line.trim_start_matches(' ');
    let indent_len = line.len() - trimmed.len();
    if indent_len > 3 {
        return None;
    }
    let first = trimmed.chars().next()?;
    if first != '`' && first != '~' {
        return None;
    }
    let marker_len = trimmed.chars().take_while(|&ch| ch == first).count();
    if marker_len < 3 {
        return None;
    }
    Some((first, marker_len, trimmed[marker_len..].trim()))
}

fn parse_code_language(info: &str) -> Option<CodeLanguage> {
    let language = info
        .split_whitespace()
        .next()?
        .trim_matches(|ch| ch == '{' || ch == '}')
        .to_ascii_lowercase();

    match language.as_str() {
        "rust" | "rs" => Some(CodeLanguage::Rust),
        "python" | "py" => Some(CodeLanguage::Python),
        "javascript" | "js" | "jsx" => Some(CodeLanguage::JavaScript),
        "typescript" | "ts" | "tsx" => Some(CodeLanguage::TypeScript),
        "json" => Some(CodeLanguage::Json),
        "bash" | "sh" | "zsh" | "shell" => Some(CodeLanguage::Shell),
        _ => None,
    }
}

fn parse_inline_code(text: &str) -> Option<(usize, &str)> {
    let marker_len = inline_code_marker_len(text);
    if marker_len == 0 {
        return None;
    }
    let marker = &text[..marker_len];
    let rest = &text[marker_len..];
    let close = rest.find(marker)?;
    Some((marker_len + close + marker_len, &rest[..close]))
}

fn inline_code_marker_len(text: &str) -> usize {
    text.chars().take_while(|&ch| ch == '`').count()
}

fn parse_inline_link(text: &str) -> Option<(usize, &str, &str)> {
    if !text.starts_with('[') {
        return None;
    }
    let label_end = text.find("](")?;
    let url_start = label_end + 2;
    let url_end = text[url_start..].find(')')? + url_start;
    Some((url_end + 1, &text[1..label_end], &text[url_start..url_end]))
}

fn parse_wrapped_segment<'a>(text: &'a str, delimiter: &str) -> Option<(usize, &'a str)> {
    if !text.starts_with(delimiter) {
        return None;
    }
    let rest = &text[delimiter.len()..];
    let close = rest.find(delimiter)?;
    if close == 0 {
        return None;
    }
    Some((delimiter.len() + close + delimiter.len(), &rest[..close]))
}

fn consume_quoted(line: &str, start: usize, quote: char) -> usize {
    let mut escaped = false;
    for (offset, ch) in line[start + quote.len_utf8()..].char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            continue;
        }
        if ch == quote {
            return start + quote.len_utf8() + offset + ch.len_utf8();
        }
    }
    line.len()
}

fn consume_identifier(line: &str, start: usize) -> Option<usize> {
    let mut chars = line[start..].char_indices();
    let (_, first) = chars.next()?;
    if !(first.is_ascii_alphabetic() || first == '_') {
        return None;
    }

    let mut end = start + first.len_utf8();
    for (offset, ch) in chars {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            end = start + offset + ch.len_utf8();
        } else {
            break;
        }
    }
    Some(end)
}

fn consume_number(line: &str, start: usize) -> Option<usize> {
    let first = line[start..].chars().next()?;
    if !first.is_ascii_digit() {
        return None;
    }

    let mut end = start + first.len_utf8();
    for (offset, ch) in line[start + first.len_utf8()..].char_indices() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '+' | '-') {
            end = start + first.len_utf8() + offset + ch.len_utf8();
        } else {
            break;
        }
    }
    Some(end)
}

fn next_non_whitespace_char(line: &str, start: usize) -> Option<char> {
    line[start..].chars().find(|ch| !ch.is_whitespace())
}

fn consume_shell_variable(line: &str, start: usize) -> usize {
    let mut end = start + 1;
    for (offset, ch) in line[end..].char_indices() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            end = start + 1 + offset + ch.len_utf8();
        } else {
            break;
        }
    }
    end.max(start + 1)
}

fn is_code_symbol(ch: char) -> bool {
    matches!(
        ch,
        '{' | '}'
            | '['
            | ']'
            | '('
            | ')'
            | ':'
            | ';'
            | ','
            | '.'
            | '='
            | '+'
            | '-'
            | '*'
            | '/'
            | '%'
            | '!'
            | '<'
            | '>'
            | '|'
            | '&'
    )
}

const RUST_KEYWORDS: [&str; 30] = [
    "as", "async", "await", "break", "const", "continue", "crate", "else", "enum", "false", "fn",
    "for", "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub", "return",
    "self", "Self", "struct", "trait", "true", "use", "while",
];

const JAVASCRIPT_KEYWORDS: [&str; 25] = [
    "async", "await", "break", "case", "catch", "class", "const", "continue", "default", "else",
    "export", "false", "finally", "for", "function", "if", "import", "let", "new", "null",
    "return", "switch", "throw", "true", "var",
];

const TYPESCRIPT_KEYWORDS: [&str; 31] = [
    "as",
    "async",
    "await",
    "break",
    "case",
    "catch",
    "class",
    "const",
    "continue",
    "default",
    "else",
    "enum",
    "export",
    "extends",
    "false",
    "finally",
    "for",
    "function",
    "if",
    "implements",
    "import",
    "interface",
    "let",
    "new",
    "null",
    "return",
    "throw",
    "true",
    "type",
    "var",
    "void",
];

const PYTHON_KEYWORDS: [&str; 25] = [
    "and", "as", "async", "await", "class", "def", "elif", "else", "False", "for", "from", "if",
    "import", "in", "is", "lambda", "None", "not", "or", "pass", "return", "self", "True", "while",
    "yield",
];

const SHELL_KEYWORDS: [&str; 15] = [
    "case", "do", "done", "echo", "elif", "else", "esac", "export", "fi", "for", "function", "if",
    "in", "then", "while",
];

fn record_match_occurrence_count(query: &str, rec: &MessageRecord) -> usize {
    rec.text
        .lines()
        .map(|line| search::find_match_ranges(query, line).len())
        .sum()
}

fn looks_like_codex_title_task_prompt(text: &str) -> bool {
    let t = text.trim_start();
    t.starts_with("You are a helpful assistant. You will be presented with a user prompt")
        && t.contains("\nUser prompt:")
}

fn extract_user_prompt_line_from_codex_title_task(text: &str) -> Option<String> {
    let (_, tail) = text.split_once("User prompt:")?;
    let mut last: Option<&str> = None;
    for line in tail.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let lower = line.to_ascii_lowercase();
        if lower.starts_with("images:")
            || lower.starts_with("local_images:")
            || lower.starts_with("text_elements:")
        {
            continue;
        }
        last = Some(line);
    }
    last.map(|s| s.to_string())
}

fn char_to_byte_pos(s: &str, char_pos: usize) -> usize {
    s.char_indices()
        .nth(char_pos)
        .map(|(i, _)| i)
        .unwrap_or(s.len())
}

fn prev_word_boundary(s: &str, char_pos: usize) -> usize {
    if char_pos == 0 {
        return 0;
    }
    let chars: Vec<char> = s.chars().collect();
    let mut pos = char_pos - 1;
    // skip whitespace
    while pos > 0 && chars[pos].is_whitespace() {
        pos -= 1;
    }
    // skip word chars
    while pos > 0 && !chars[pos - 1].is_whitespace() {
        pos -= 1;
    }
    pos
}

fn next_word_boundary(s: &str, char_pos: usize) -> usize {
    let chars: Vec<char> = s.chars().collect();
    let len = chars.len();
    if char_pos >= len {
        return len;
    }
    let mut pos = char_pos;
    // skip current word chars
    while pos < len && !chars[pos].is_whitespace() {
        pos += 1;
    }
    // skip whitespace
    while pos < len && chars[pos].is_whitespace() {
        pos += 1;
    }
    pos
}

fn dir_name_from_cwd(cwd: &str) -> String {
    let s = cwd.trim();
    let s = s.trim_end_matches('/');
    if s.is_empty() {
        return "-".to_string();
    }
    Path::new(s)
        .file_name()
        .and_then(|x| x.to_str())
        .filter(|x| !x.trim().is_empty())
        .unwrap_or(s)
        .to_string()
}

fn is_agents_instructions(text: &str) -> bool {
    let t = text.trim_start();
    let first = t.lines().next().unwrap_or("").trim_start();
    let lower = first.to_ascii_lowercase();
    lower.starts_with("# agents.md instructions for ")
        || lower.starts_with("agents.md instructions for ")
}

fn is_environment_context(text: &str) -> bool {
    let t = text.trim_start();
    let first = t.lines().next().unwrap_or("").trim_start();
    let lower = first.to_ascii_lowercase();
    lower.starts_with("<environment_context")
}

fn is_noise_user_message(text: &str) -> bool {
    is_agents_instructions(text) || is_environment_context(text)
}

fn ts_cmp_opt(a: Option<&str>, b: Option<&str>) -> cmp::Ordering {
    match (a, b) {
        (None, None) => cmp::Ordering::Equal,
        (None, Some(_)) => cmp::Ordering::Less,
        (Some(_), None) => cmp::Ordering::Greater,
        (Some(a), Some(b)) => ts_cmp_str(a, b),
    }
}

fn ts_cmp_str(a: &str, b: &str) -> cmp::Ordering {
    match (parse_timestamp_nanos(a), parse_timestamp_nanos(b)) {
        (Some(a), Some(b)) => a.cmp(&b),
        _ => a.cmp(b),
    }
}

fn parse_timestamp_nanos(s: &str) -> Option<i128> {
    if let Some(nanos) = parse_rfc3339_timestamp_nanos(s) {
        return Some(nanos);
    }

    let epoch = parse_epoch_number(s)?;
    epoch_to_unix_nanos(epoch)
}

fn parse_epoch_number(s: &str) -> Option<i64> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

fn epoch_to_unix_nanos(ts: i64) -> Option<i128> {
    let abs = ts.unsigned_abs();
    let nanos = if abs >= 1_000_000_000_000_000_000 {
        i128::from(ts)
    } else if abs >= 1_000_000_000_000_000 {
        i128::from(ts).checked_mul(1_000)?
    } else if abs >= 1_000_000_000_000 {
        i128::from(ts).checked_mul(1_000_000)?
    } else {
        i128::from(ts).checked_mul(1_000_000_000)?
    };
    Some(nanos)
}

fn display_timestamp(ts: &str) -> Option<String> {
    if parse_rfc3339_timestamp_nanos(ts).is_some() {
        return Some(ts.get(0..19).unwrap_or(ts).to_string());
    }

    let epoch = parse_epoch_number(ts)?;
    let nanos = epoch_to_unix_nanos(epoch)?;
    let secs = i64::try_from(nanos.div_euclid(1_000_000_000)).ok()?;
    Some(format_unix_seconds_short(secs))
}

fn parse_rfc3339_timestamp_nanos(s: &str) -> Option<i128> {
    let bytes = s.as_bytes();
    if bytes.len() < 20 {
        return None;
    }

    let year = parse_digits_i32(bytes, 0, 4)?;
    if bytes.get(4) != Some(&b'-') {
        return None;
    }
    let month = parse_digits_u32(bytes, 5, 2)?;
    if bytes.get(7) != Some(&b'-') {
        return None;
    }
    let day = parse_digits_u32(bytes, 8, 2)?;
    let sep = *bytes.get(10)?;
    if sep != b'T' && sep != b't' {
        return None;
    }
    let hour = parse_digits_u32(bytes, 11, 2)?;
    if bytes.get(13) != Some(&b':') {
        return None;
    }
    let minute = parse_digits_u32(bytes, 14, 2)?;
    if bytes.get(16) != Some(&b':') {
        return None;
    }
    let second = parse_digits_u32(bytes, 17, 2)?;

    let mut idx = 19usize;
    let mut nanos: i128 = 0;
    if bytes.get(idx) == Some(&b'.') {
        idx += 1;
        let frac_start = idx;
        while idx < bytes.len() && bytes[idx].is_ascii_digit() {
            idx += 1;
        }
        let frac = &s[frac_start..idx];
        if frac.is_empty() {
            return None;
        }
        nanos = parse_fractional_nanos(frac)?;
    }

    let offset_seconds: i32 = match *bytes.get(idx)? {
        b'Z' | b'z' => {
            idx += 1;
            0
        }
        b'+' | b'-' => {
            let sign = if bytes[idx] == b'+' { 1 } else { -1 };
            idx += 1;
            let off_hour = parse_digits_i32(bytes, idx, 2)?;
            idx += 2;
            if bytes.get(idx) != Some(&b':') {
                return None;
            }
            idx += 1;
            let off_min = parse_digits_i32(bytes, idx, 2)?;
            idx += 2;
            sign * (off_hour * 3_600 + off_min * 60)
        }
        _ => return None,
    };

    if idx != bytes.len() {
        return None;
    }

    let days = days_from_civil(year, month, day)?;
    let seconds = i128::from(days) * 86_400
        + i128::from(hour) * 3_600
        + i128::from(minute) * 60
        + i128::from(second)
        - i128::from(offset_seconds);
    Some(seconds * 1_000_000_000 + nanos)
}

fn parse_digits_i32(bytes: &[u8], start: usize, len: usize) -> Option<i32> {
    parse_digits_u32(bytes, start, len).and_then(|n| i32::try_from(n).ok())
}

fn parse_digits_u32(bytes: &[u8], start: usize, len: usize) -> Option<u32> {
    let end = start.checked_add(len)?;
    let slice = bytes.get(start..end)?;
    let mut out = 0u32;
    for &b in slice {
        if !b.is_ascii_digit() {
            return None;
        }
        out = out.checked_mul(10)?.checked_add(u32::from(b - b'0'))?;
    }
    Some(out)
}

fn parse_fractional_nanos(frac: &str) -> Option<i128> {
    let mut nanos = 0i128;
    let mut digits = 0usize;
    for b in frac.bytes() {
        if !b.is_ascii_digit() {
            return None;
        }
        if digits < 9 {
            nanos = nanos.checked_mul(10)?.checked_add(i128::from(b - b'0'))?;
        }
        digits += 1;
    }
    for _ in digits..9 {
        nanos = nanos.checked_mul(10)?;
    }
    Some(nanos)
}

fn days_from_civil(year: i32, month: u32, day: u32) -> Option<i64> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let year = i64::from(year) - if month <= 2 { 1 } else { 0 };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = i64::from(month);
    let day = i64::from(day);
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some(era * 146_097 + doe - 719_468)
}

fn format_unix_seconds_short(secs: i64) -> String {
    let days = secs.div_euclid(86_400);
    let secs_of_day = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = secs_of_day / 3_600;
    let minute = (secs_of_day % 3_600) / 60;
    let second = secs_of_day % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}")
}

fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    if month <= 2 {
        year += 1;
    }
    (year as i32, month as u32, day as u32)
}

#[derive(Debug)]
struct App {
    query: String,
    cursor_pos: usize,
    telemetry_query: String,
    telemetry_cursor_pos: usize,
    max_results: usize,
    active_tag_filters: Vec<TagFilter>,

    all: Vec<MessageRecord>,
    sessions: Vec<SessionSummary>,
    session_records: Vec<Vec<usize>>,
    filtered: Vec<SessionHit>,
    selected: usize,
    offset: usize,
    selected_preview_record_idx: Option<usize>,
    preview_scroll: usize,
    preview_scroll_reset_pending: bool,
    session_browser_start_scroll: usize,
    session_browser_end_scroll: usize,
    session_browser_active_pane: SessionBrowserPane,
    git_graph_scroll: usize,
    git_commit_scroll: usize,
    git_graph_visible: bool,
    git_commit_visible: bool,
    active_pane: ActivePane,
    active_split: Option<ActiveSplit>,
    layout_state: LayoutState,
    dragged_split: Option<ActiveSplit>,

    last_query: String,
    last_results: Vec<usize>,
    last_tag_filters: Vec<TagFilter>,

    indexing: IndexingProgress,
    ready: bool,
    telemetry_log_path: Option<PathBuf>,
    telemetry_sink: Option<telemetry::TelemetrySink>,
    telemetry_events: EventBuffer,
    cache_path: Option<PathBuf>,
    show_telemetry: bool,
    enabled_log_groups: HashSet<LogGroup>,
    log_groups_help_emitted: bool,
    ui_status: Option<String>,
    preview_bgcolor_target: Option<String>,
    preview_bgcolor: Option<Color>,
    preview_remote_style: bool,
    preview_bgcolor_cache: HashMap<String, Option<Color>>,
    ui_tags: config::UiTagConfig,
    remotes: HashMap<String, config::RemoteConfig>,
    git_repo_cache: HashMap<PathBuf, GitRepoContext>,
    git_commit_preview_cache: HashMap<(PathBuf, String), Vec<String>>,
}

pub fn run(args: RunArgs) -> anyhow::Result<()> {
    let rx = crate::indexer::spawn_indexer_from_args(args.clone());

    let mut stdout = io::stdout();
    enable_raw_mode().context("failed to enable raw mode")?;
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)
        .context("failed to switch screen")?;

    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend).context("failed to initialize terminal")?;
    terminal.clear().ok();

    let res = run_app(&mut terminal, rx, args);

    disable_raw_mode().ok();
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        cursor::Show,
        DisableMouseCapture
    )
    .ok();
    terminal.show_cursor().ok();

    res
}

fn run_app(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    rx: mpsc::Receiver<IndexerEvent>,
    args: RunArgs,
) -> anyhow::Result<()> {
    let app_config = config::load_config(args.scan.config.as_deref()).unwrap_or_default();
    let remotes = app_config
        .remotes
        .iter()
        .cloned()
        .map(|remote| (remote.name.clone(), remote))
        .collect();
    let telemetry_log_path = (!args.scan.no_telemetry).then(|| {
        args.scan
            .telemetry_log
            .clone()
            .unwrap_or_else(telemetry::default_log_path)
    });
    let mut telemetry_events = EventBuffer::new(EVENT_BUFFER_MAX_BYTES);
    if let Some(path) = telemetry_log_path.as_ref() {
        telemetry_events.seed(telemetry::read_recent_records(path, EVENT_BUFFER_MAX_BYTES));
    }
    let telemetry_sink = telemetry_log_path
        .as_ref()
        .and_then(|path| telemetry::TelemetrySink::open(path).ok());
    let initial_query = args.query.unwrap_or_default();
    let initial_cursor = initial_query.len();
    let mut app = App {
        query: initial_query,
        cursor_pos: initial_cursor,
        telemetry_query: String::new(),
        telemetry_cursor_pos: 0,
        max_results: args.max_results,
        active_tag_filters: Vec::new(),
        all: Vec::new(),
        sessions: Vec::new(),
        session_records: Vec::new(),
        filtered: Vec::new(),
        selected: 0,
        offset: 0,
        selected_preview_record_idx: None,
        preview_scroll: 0,
        preview_scroll_reset_pending: false,
        session_browser_start_scroll: 0,
        session_browser_end_scroll: usize::MAX,
        session_browser_active_pane: SessionBrowserPane::Start,
        git_graph_scroll: 0,
        git_commit_scroll: 0,
        git_graph_visible: false,
        git_commit_visible: false,
        active_pane: ActivePane::Results,
        active_split: None,
        layout_state: LayoutState::new(),
        dragged_split: None,
        last_query: String::new(),
        last_results: Vec::new(),
        last_tag_filters: Vec::new(),
        indexing: IndexingProgress::default(),
        ready: false,
        telemetry_log_path,
        telemetry_sink,
        telemetry_events,
        cache_path: (!args.scan.no_cache).then(cache::default_db_path),
        show_telemetry: false,
        enabled_log_groups: args.log_groups.iter().copied().collect(),
        log_groups_help_emitted: false,
        ui_status: None,
        preview_bgcolor_target: None,
        preview_bgcolor: None,
        preview_remote_style: false,
        preview_bgcolor_cache: HashMap::new(),
        ui_tags: app_config.ui.tags,
        remotes,
        git_repo_cache: HashMap::new(),
        git_commit_preview_cache: HashMap::new(),
    };

    if let Some(path) = app.telemetry_log_path.as_ref() {
        app.emit_event(
            "log_path_info",
            json!({
                "path": path.display().to_string(),
            }),
        );
        app.emit_event(
            "ui_session_started",
            json!({
                "pid": std::process::id(),
                "cwd": env::current_dir()
                    .map(|cwd| cwd.display().to_string())
                    .unwrap_or_default(),
            }),
        );
    }

    loop {
        while let Ok(ev) = rx.try_recv() {
            handle_indexer_event(&mut app, ev);
        }

        sync_preview_bgcolor(&mut app);

        terminal
            .draw(|f| ui(f, &mut app))
            .context("failed to render ui")?;

        if !event::poll(Duration::from_millis(50)).context("event poll failed")? {
            continue;
        }

        let ev = event::read().context("event read failed")?;
        match ev {
            Event::Key(key) => {
                if handle_key(terminal, &mut app, key)? {
                    break;
                }
            }
            Event::Mouse(mouse) => {
                handle_mouse(terminal, &mut app, mouse)?;
            }
            _ => {}
        }
    }

    Ok(())
}

fn handle_indexer_event(app: &mut App, ev: IndexerEvent) {
    match ev {
        IndexerEvent::Loaded { records } => {
            apply_records(app, records);
        }
        IndexerEvent::Discovered { total_files } => {
            app.indexing.total_files = total_files;
        }
        IndexerEvent::Progress {
            processed_files,
            total_files,
            records,
            sessions,
            current,
        } => {
            app.indexing.processed_files = processed_files;
            app.indexing.total_files = total_files;
            app.indexing.records = records;
            app.indexing.sessions = sessions;
            app.indexing.current = Some(current);
        }
        IndexerEvent::Warn { message } => {
            app.indexing.last_warn = Some(message);
        }
        IndexerEvent::Telemetry { record } => {
            app.append_event_record(record);
        }
        IndexerEvent::Done { records } => {
            apply_records(app, records);
        }
    }
}

fn apply_records(app: &mut App, records: Vec<MessageRecord>) {
    app.all = records;
    let (sessions, session_records) = build_session_index(&app.all);
    app.indexing.records = app.all.len();
    app.indexing.sessions = sessions.len();
    app.sessions = sessions;
    app.session_records = session_records;
    app.ready = true;
    app.update_results();
}

fn handle_key(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    app: &mut App,
    key: KeyEvent,
) -> anyhow::Result<bool> {
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        return Ok(true);
    }

    if key.code == KeyCode::Esc {
        return Ok(true);
    }

    // Accept query input even while indexing is still in progress.
    match key.code {
        KeyCode::Char('b') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            let preview_width = current_preview_inner_width(terminal, app)?;
            app.jump_preview_record(-1, preview_width);
            return Ok(false);
        }
        KeyCode::Char('f') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            let preview_width = current_preview_inner_width(terminal, app)?;
            app.jump_preview_record(1, preview_width);
            return Ok(false);
        }
        KeyCode::Char('t') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.toggle_telemetry_view();
            return Ok(false);
        }
        KeyCode::Char('v') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.toggle_git_graph();
            return Ok(false);
        }
        KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.toggle_git_commit();
            return Ok(false);
        }
        KeyCode::Char('l') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.cycle_layout_preset();
            return Ok(false);
        }
        KeyCode::Char('g')
            if key.modifiers.contains(KeyModifiers::CONTROL) && app.show_telemetry =>
        {
            app.toggle_log_group(LogGroup::Perf);
            return Ok(false);
        }
        KeyCode::Char('n') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            let width = current_preview_inner_width(terminal, app)?;
            app.jump_preview_match(1, width);
            return Ok(false);
        }
        KeyCode::Char('p') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            let width = current_preview_inner_width(terminal, app)?;
            app.jump_preview_match(-1, width);
            return Ok(false);
        }
        KeyCode::Left
            if key.modifiers.contains(KeyModifiers::ALT)
                && key.modifiers.contains(KeyModifiers::SHIFT) =>
        {
            app.adjust_active_split(-2);
            return Ok(false);
        }
        KeyCode::Right
            if key.modifiers.contains(KeyModifiers::ALT)
                && key.modifiers.contains(KeyModifiers::SHIFT) =>
        {
            app.adjust_active_split(2);
            return Ok(false);
        }
        KeyCode::Up
            if key.modifiers.contains(KeyModifiers::ALT)
                && key.modifiers.contains(KeyModifiers::SHIFT) =>
        {
            app.adjust_active_split(-2);
            return Ok(false);
        }
        KeyCode::Down
            if key.modifiers.contains(KeyModifiers::ALT)
                && key.modifiers.contains(KeyModifiers::SHIFT) =>
        {
            app.adjust_active_split(2);
            return Ok(false);
        }
        KeyCode::Backspace => {
            if app.show_telemetry {
                if app.telemetry_cursor_pos > 0 {
                    let byte_pos = char_to_byte_pos(&app.telemetry_query, app.telemetry_cursor_pos);
                    let prev_byte_pos =
                        char_to_byte_pos(&app.telemetry_query, app.telemetry_cursor_pos - 1);
                    app.telemetry_query
                        .replace_range(prev_byte_pos..byte_pos, "");
                    app.telemetry_cursor_pos -= 1;
                    app.reset_telemetry_search();
                }
            } else if app.cursor_pos > 0 {
                let byte_pos = char_to_byte_pos(&app.query, app.cursor_pos);
                let prev_byte_pos = char_to_byte_pos(&app.query, app.cursor_pos - 1);
                app.query.replace_range(prev_byte_pos..byte_pos, "");
                app.cursor_pos -= 1;
                app.update_results();
            }
            return Ok(false);
        }
        KeyCode::Delete => {
            if app.show_telemetry {
                let char_count = app.telemetry_query.chars().count();
                if app.telemetry_cursor_pos < char_count {
                    let byte_pos = char_to_byte_pos(&app.telemetry_query, app.telemetry_cursor_pos);
                    let next_byte_pos =
                        char_to_byte_pos(&app.telemetry_query, app.telemetry_cursor_pos + 1);
                    app.telemetry_query
                        .replace_range(byte_pos..next_byte_pos, "");
                    app.reset_telemetry_search();
                }
            } else {
                let char_count = app.query.chars().count();
                if app.cursor_pos < char_count {
                    let byte_pos = char_to_byte_pos(&app.query, app.cursor_pos);
                    let next_byte_pos = char_to_byte_pos(&app.query, app.cursor_pos + 1);
                    app.query.replace_range(byte_pos..next_byte_pos, "");
                }
                app.update_results();
            }
            return Ok(false);
        }
        KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            if app.show_telemetry {
                app.telemetry_query.clear();
                app.telemetry_cursor_pos = 0;
                app.reset_telemetry_search();
            } else {
                app.clear_query_and_filters();
            }
            return Ok(false);
        }
        KeyCode::Left => {
            if app.show_telemetry {
                if key.modifiers.contains(KeyModifiers::ALT) {
                    app.telemetry_cursor_pos =
                        prev_word_boundary(&app.telemetry_query, app.telemetry_cursor_pos);
                } else {
                    app.telemetry_cursor_pos = app.telemetry_cursor_pos.saturating_sub(1);
                }
            } else if key.modifiers.contains(KeyModifiers::ALT) {
                app.cursor_pos = prev_word_boundary(&app.query, app.cursor_pos);
            } else {
                app.cursor_pos = app.cursor_pos.saturating_sub(1);
            }
            return Ok(false);
        }
        KeyCode::Right => {
            if app.show_telemetry {
                let char_count = app.telemetry_query.chars().count();
                if key.modifiers.contains(KeyModifiers::ALT) {
                    app.telemetry_cursor_pos =
                        next_word_boundary(&app.telemetry_query, app.telemetry_cursor_pos);
                } else if app.telemetry_cursor_pos < char_count {
                    app.telemetry_cursor_pos += 1;
                }
            } else {
                let char_count = app.query.chars().count();
                if key.modifiers.contains(KeyModifiers::ALT) {
                    app.cursor_pos = next_word_boundary(&app.query, app.cursor_pos);
                } else if app.cursor_pos < char_count {
                    app.cursor_pos += 1;
                }
            }
            return Ok(false);
        }
        KeyCode::Home => {
            if app.show_telemetry {
                app.telemetry_cursor_pos = 0;
            } else {
                app.cursor_pos = 0;
            }
            return Ok(false);
        }
        KeyCode::End => {
            if app.show_telemetry {
                app.telemetry_cursor_pos = app.telemetry_query.chars().count();
            } else {
                app.cursor_pos = app.query.chars().count();
            }
            return Ok(false);
        }
        KeyCode::Char('a') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            if app.show_telemetry {
                app.telemetry_cursor_pos = 0;
            } else {
                app.cursor_pos = 0;
            }
            return Ok(false);
        }
        KeyCode::Char('e') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            if app.show_telemetry {
                app.telemetry_cursor_pos = app.telemetry_query.chars().count();
            } else {
                app.cursor_pos = app.query.chars().count();
            }
            return Ok(false);
        }
        KeyCode::Char(c) => {
            if !key.modifiers.contains(KeyModifiers::CONTROL)
                && !key.modifiers.contains(KeyModifiers::ALT)
            {
                if app.show_telemetry {
                    let byte_pos = char_to_byte_pos(&app.telemetry_query, app.telemetry_cursor_pos);
                    app.telemetry_query.insert(byte_pos, c);
                    app.telemetry_cursor_pos += 1;
                    app.reset_telemetry_search();
                } else {
                    let byte_pos = char_to_byte_pos(&app.query, app.cursor_pos);
                    app.query.insert(byte_pos, c);
                    app.cursor_pos += 1;
                    app.update_results();
                }
                return Ok(false);
            }
        }
        _ => {}
    }

    if !app.ready {
        return Ok(false);
    }

    match key.code {
        KeyCode::Char('o') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            if let Some(rec) = app.selected_record() {
                open_in_pager(terminal, app, rec)?;
            }
        }
        KeyCode::Enter => {
            if let Some(rec) = app.selected_record() {
                match resume_target_for_record(app, rec) {
                    Some(target) => {
                        let sid = rec.session_id.as_deref().unwrap_or("");
                        let status = run_with_tui_suspended(terminal, || {
                            let configured = configured_resume_command(&target)?;
                            let cleanup_dir = configured.zdotdir_cleanup.clone();
                            let mut cmd = configured.command;

                            for line in resume_loading_lines(&target, sid) {
                                eprintln!("{line}");
                            }

                            let mut child = match cmd.spawn() {
                                Ok(child) => child,
                                Err(err) => {
                                    if let Some(dir) = cleanup_dir {
                                        let _ = fs::remove_dir_all(dir);
                                    }
                                    return Err(err).context("failed to start resume shell");
                                }
                            };
                            let status = child
                                .wait()
                                .context("failed while waiting for resume shell")?;
                            if let Some(dir) = cleanup_dir {
                                let _ = fs::remove_dir_all(dir);
                            }
                            Ok(status)
                        });

                        match status {
                            Ok(st) if st.success() => {
                                app.indexing.last_warn = None;
                            }
                            Ok(st) => {
                                app.indexing.last_warn = Some(format!(
                                    "resume failed: {} (code={})",
                                    target.program,
                                    st.code()
                                        .map(|c| c.to_string())
                                        .unwrap_or_else(|| "?".to_string())
                                ));
                            }
                            Err(e) => {
                                app.indexing.last_warn =
                                    Some(format!("resume failed: {}: {e}", target.program));
                            }
                        }
                    }
                    None => {
                        app.indexing.last_warn =
                            Some("the selected row cannot be resumed".to_string());
                    }
                }
            }
        }
        KeyCode::Up => app.move_selection(-1),
        KeyCode::Down => app.move_selection(1),
        KeyCode::PageUp => {
            let preview_width = current_preview_inner_width(terminal, app)?;
            let preview_height = current_preview_inner_height(terminal, app)?;
            app.scroll_preview_page(-1, preview_height, preview_width);
        }
        KeyCode::PageDown => {
            let preview_width = current_preview_inner_width(terminal, app)?;
            let preview_height = current_preview_inner_height(terminal, app)?;
            app.scroll_preview_page(1, preview_height, preview_width);
        }
        KeyCode::Home => app.select_first(),
        KeyCode::End => app.select_last(),
        _ => {}
    }

    Ok(false)
}

fn selected_preview_bgcolor_target(app: &App) -> Option<&str> {
    let hit = app.selected_hit()?;
    let session = app.sessions.get(hit.session_idx)?;
    session.cwd.as_deref().filter(|cwd| !cwd.trim().is_empty())
}

fn selected_preview_is_remote(app: &App) -> bool {
    let Some(hit) = app.selected_hit() else {
        return false;
    };
    let Some(session) = app.sessions.get(hit.session_idx) else {
        return false;
    };
    session.origin != "local"
}

const EVENTS_PREVIEW_BGCOLOR: &str = "#202020";

fn events_preview_style() -> Style {
    parse_hex_color(EVENTS_PREVIEW_BGCOLOR)
        .map(|color| Style::default().bg(color))
        .unwrap_or_default()
}

fn parse_hex_color(hex: &str) -> Option<Color> {
    let hex = hex.trim();
    let digits = hex.strip_prefix('#')?;
    if digits.len() != 6 {
        return None;
    }
    let red = u8::from_str_radix(&digits[0..2], 16).ok()?;
    let green = u8::from_str_radix(&digits[2..4], 16).ok()?;
    let blue = u8::from_str_radix(&digits[4..6], 16).ok()?;
    Some(Color::Rgb(red, green, blue))
}

fn resolve_preview_bgcolor_for_target(target: &str) -> anyhow::Result<Option<Color>> {
    if target.starts_with('#') {
        return parse_hex_color(target)
            .map(Some)
            .ok_or_else(|| anyhow!("invalid preview background color: {target}"));
    }

    let Some(home) = env::var_os("HOME") else {
        return Ok(None);
    };
    let script = PathBuf::from(home).join("util/bgcolor.sh");
    if !script.is_file() {
        return Ok(None);
    }

    let output = Command::new(script)
        .arg("--format=hex")
        .arg(target)
        .output()
        .context("failed to resolve preview background color")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(anyhow!(
            "preview background resolver failed{}",
            if stderr.is_empty() {
                String::new()
            } else {
                format!(": {stderr}")
            }
        ));
    }

    let hex = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if hex.is_empty() {
        return Ok(None);
    }

    parse_hex_color(&hex)
        .map(Some)
        .ok_or_else(|| anyhow!("invalid preview background color: {hex}"))
}

fn sync_preview_bgcolor(app: &mut App) {
    let preview_remote_style = !app.show_telemetry && selected_preview_is_remote(app);
    let target = if app.show_telemetry || preview_remote_style {
        Some(EVENTS_PREVIEW_BGCOLOR.to_string())
    } else {
        selected_preview_bgcolor_target(app).map(str::to_owned)
    };
    if target == app.preview_bgcolor_target && preview_remote_style == app.preview_remote_style {
        return;
    }

    let color = match target.as_ref() {
        None => None,
        Some(target) => {
            if let Some(cached) = app.preview_bgcolor_cache.get(target) {
                *cached
            } else {
                match resolve_preview_bgcolor_for_target(target) {
                    Ok(color) => {
                        app.preview_bgcolor_cache.insert(target.clone(), color);
                        color
                    }
                    Err(err) => {
                        app.indexing.last_warn =
                            Some(format!("preview bgcolor update failed: {err}"));
                        return;
                    }
                }
            }
        }
    };

    app.preview_bgcolor_target = target;
    app.preview_bgcolor = color;
    app.preview_remote_style = preview_remote_style;
}

fn parse_git_commit_line(line: &str) -> Option<GitCommitEntry> {
    let mut parts = line.splitn(3, '\t');
    let hash = parts.next()?.trim();
    let epoch = parts.next()?.trim().parse::<i64>().ok()?;
    let summary = parts.next().unwrap_or("").trim().to_string();
    Some(GitCommitEntry {
        hash: hash.to_string(),
        epoch,
        summary,
    })
}

fn parse_git_graph_hash_field(field: &str) -> Option<&str> {
    field.split_whitespace().find(|token| {
        token.len() >= 7 && token.chars().all(|ch| ch.is_ascii_hexdigit())
    })
}

fn selected_git_commit_idx(commits: &[GitCommitEntry], anchor_ts: i64) -> Option<usize> {
    if commits.is_empty() {
        return None;
    }
    Some(
        commits
            .iter()
            .position(|commit| commit.epoch <= anchor_ts)
            .unwrap_or_else(|| commits.len().saturating_sub(1)),
    )
}

fn point_near_vertical_split(x: u16, rect: Rect) -> bool {
    rect.width > 0 && x == rect.x.saturating_sub(1)
}

fn point_near_horizontal_split(y: u16, rect: Rect) -> bool {
    rect.height > 0 && y == rect.y.saturating_sub(1)
}

fn with_anchor_indicator(doc: &PreviewDoc, anchor_record_idx: Option<usize>) -> Vec<Line<'static>> {
    let Some(anchor_record_idx) = anchor_record_idx else {
        return doc.lines.clone();
    };
    let mut lines = doc.lines.clone();
    let Some(line_idx) = doc
        .line_record_indices
        .iter()
        .position(|record_idx| *record_idx == Some(anchor_record_idx))
    else {
        return lines;
    };
    let mut spans = vec![Span::styled(
        "git anchor  ",
        Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::BOLD),
    )];
    spans.extend(lines[line_idx].spans.clone());
    lines[line_idx] = Line::from(spans);
    lines
}

fn with_selected_preview_line(
    doc: &PreviewDoc,
    selected_record_idx: Option<usize>,
    anchor_record_idx: Option<usize>,
) -> Vec<Line<'static>> {
    let mut lines = with_anchor_indicator(doc, anchor_record_idx);
    let Some(selected_record_idx) = selected_record_idx else {
        return lines;
    };
    let Some(line_idx) = doc
        .line_record_indices
        .iter()
        .position(|record_idx| *record_idx == Some(selected_record_idx))
    else {
        return lines;
    };
    let selected_style = Style::default()
        .bg(Color::DarkGray)
        .add_modifier(Modifier::BOLD);
    lines[line_idx] = Line::from(
        lines[line_idx]
            .spans
            .iter()
            .cloned()
            .map(|span| Span::styled(span.content, span.style.patch(selected_style)))
            .collect::<Vec<_>>(),
    );
    lines
}

fn app_geometry(area: Rect, app: &App) -> PaneGeometry {
    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints(
            [
                Constraint::Min(1),
                Constraint::Length(app.footer_height()),
            ]
            .as_ref(),
        )
        .split(area)[0];

    if app.show_telemetry || !app.git_graph_visible {
        let main = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(45), Constraint::Percentage(55)].as_ref())
            .split(root);
        let left = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Min(1)].as_ref())
            .split(main[0]);
        return PaneGeometry {
            root,
            query: left[0],
            results: left[1],
            git_graph: None,
            git_commit: None,
            turn_preview: main[1],
            turn_start: None,
            turn_end: None,
            browser_single: true,
        };
    }

    let mut results_pct = app.layout_state.results_pct.clamp(MIN_RESULTS_PCT, 100);
    let max_results = 100 - MIN_GIT_PCT - MIN_TURNS_PCT;
    results_pct = results_pct.min(max_results);
    let main = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(
            [
                Constraint::Percentage(results_pct),
                Constraint::Percentage(100 - results_pct),
            ]
            .as_ref(),
        )
        .split(root);
    let left = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(1)].as_ref())
        .split(main[0]);
    let left_body = left[1];
    let max_git = 100 - MIN_TURNS_PCT;
    let git_pct = app.layout_state.git_pct.clamp(MIN_GIT_PCT, max_git);
    let left_panes = Layout::default()
        .direction(Direction::Vertical)
        .constraints(
            [
                Constraint::Percentage(100 - git_pct),
                Constraint::Percentage(git_pct),
            ]
            .as_ref(),
        )
        .split(left_body);
    let git_column = left_panes[1];
    let (git_graph, git_commit) = if app.git_commit_visible {
        let graph_pct = app
            .layout_state
            .graph_pct
            .clamp(MIN_GRAPH_PCT, MAX_GRAPH_PCT);
        let panes = Layout::default()
            .direction(Direction::Vertical)
            .constraints(
                [
                    Constraint::Percentage(graph_pct),
                    Constraint::Percentage(100 - graph_pct),
                ]
                .as_ref(),
            )
            .split(git_column);
        (Some(panes[0]), Some(panes[1]))
    } else {
        (Some(git_column), None)
    };

    PaneGeometry {
        root,
        query: left[0],
        results: left_panes[0],
        git_graph,
        git_commit,
        turn_preview: main[1],
        turn_start: None,
        turn_end: None,
        browser_single: true,
    }
}

fn app_panes(area: Rect) -> (Rect, Rect) {
    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(1), Constraint::Length(1)].as_ref())
        .split(area)[0];
    let main = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(45), Constraint::Percentage(55)].as_ref())
        .split(root);
    let left = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(1)].as_ref())
        .split(main[0]);
    (left[1], main[1])
}

fn point_in_rect(x: u16, y: u16, rect: Rect) -> bool {
    x >= rect.x
        && x < rect.x.saturating_add(rect.width)
        && y >= rect.y
        && y < rect.y.saturating_add(rect.height)
}

fn visible_result_range(app: &App, results_area: Rect) -> (usize, usize) {
    let inner_height = results_area.height.saturating_sub(2) as usize;
    let total = app.filtered.len();
    let visible_start = cmp::min(app.offset, total);
    let visible_end = cmp::min(visible_start + inner_height, total);
    (visible_start, visible_end)
}

fn tag_filter_at_result_column(
    sess: &SessionSummary,
    tags: &config::UiTagConfig,
    content_x: usize,
) -> Option<TagFilter> {
    let ts_width = short_ts(sess.last_ts.as_deref()).chars().count();
    let specs = session_tag_specs(sess, tags);
    if specs.is_empty() {
        return None;
    }

    let mut cursor = ts_width + 1;
    for spec in specs {
        let width = spec.content.chars().count();
        if (cursor..cursor + width).contains(&content_x) {
            return Some(spec.filter);
        }
        cursor += width + 1;
    }

    None
}

fn handle_results_click(app: &mut App, results_area: Rect, mouse: MouseEvent) {
    if !point_in_rect(mouse.column, mouse.row, results_area) {
        return;
    }
    if mouse.row <= results_area.y
        || mouse.row >= results_area.y + results_area.height.saturating_sub(1)
    {
        return;
    }
    if mouse.column <= results_area.x
        || mouse.column >= results_area.x + results_area.width.saturating_sub(1)
    {
        return;
    }

    let (visible_start, visible_end) = visible_result_range(app, results_area);
    let row = visible_start + (mouse.row - results_area.y - 1) as usize;
    if row >= visible_end {
        return;
    }

    app.selected = row;
    app.reset_preview_scroll_to_match();

    let Some(hit) = app.filtered.get(row).copied() else {
        return;
    };
    let Some(sess) = app.sessions.get(hit.session_idx) else {
        return;
    };

    let content_x = (mouse.column - results_area.x - 1) as usize;
    if let Some(filter) = tag_filter_at_result_column(sess, &app.ui_tags, content_x) {
        app.toggle_tag_filter(filter);
    }
}

fn handle_mouse(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    app: &mut App,
    mouse: MouseEvent,
) -> anyhow::Result<()> {
    if !app.ready {
        return Ok(());
    }

    let area = terminal.size().context("failed to get terminal size")?;
    route_mouse(app, area.into(), mouse);
    Ok(())
}

fn route_mouse(app: &mut App, area: Rect, mouse: MouseEvent) {
    let geometry = app_geometry(area, app);
    let results_area = geometry.results;
    let preview_area = geometry.turn_preview;
    let preview_line_step: i32 = 3;
    let results_line_step = 1;
    let preview_hit_area = if app.show_telemetry {
        area
    } else {
        preview_area
    };

    match mouse.kind {
        MouseEventKind::Down(_) => {
            app.dragged_split = None;
            if !app.show_telemetry && app.git_graph_visible {
                if point_near_vertical_split(mouse.column, geometry.turn_preview) {
                    app.active_split = Some(ActiveSplit::ResultsGit);
                    app.dragged_split = Some(ActiveSplit::ResultsGit);
                } else if let Some(git_split_rect) = geometry.git_graph.or(geometry.git_commit)
                    && point_near_horizontal_split(mouse.row, git_split_rect)
                {
                    app.active_split = Some(ActiveSplit::GitTurns);
                    app.dragged_split = Some(ActiveSplit::GitTurns);
                } else if let Some(git_commit) = geometry.git_commit
                    && point_near_horizontal_split(mouse.row, git_commit)
                {
                    app.active_split = Some(ActiveSplit::GitColumnVertical);
                    app.dragged_split = Some(ActiveSplit::GitColumnVertical);
                }
            }
            if !app.show_telemetry {
                handle_results_click(app, results_area, mouse);
                if let Some(git_graph) = geometry.git_graph
                    && point_in_rect(mouse.column, mouse.row, git_graph)
                {
                    app.active_pane = ActivePane::GitGraph;
                } else if let Some(git_commit) = geometry.git_commit
                    && point_in_rect(mouse.column, mouse.row, git_commit)
                {
                    app.active_pane = ActivePane::GitCommit;
                } else if let Some(turn_start) = geometry.turn_start
                    && point_in_rect(mouse.column, mouse.row, turn_start)
                {
                    app.set_session_browser_active_pane(SessionBrowserPane::Start);
                } else if let Some(turn_end) = geometry.turn_end
                    && point_in_rect(mouse.column, mouse.row, turn_end)
                {
                    app.set_session_browser_active_pane(SessionBrowserPane::End);
                } else if point_in_rect(mouse.column, mouse.row, geometry.turn_preview) {
                    app.active_pane = ActivePane::TurnPreview;
                } else if point_in_rect(mouse.column, mouse.row, results_area) {
                    app.active_pane = ActivePane::Results;
                }
            }
        }
        MouseEventKind::Moved => {
            if !app.show_telemetry && point_in_rect(mouse.column, mouse.row, preview_area) {
                let preview_doc = if app.showing_session_browser() {
                    app.session_browser_doc()
                } else {
                    app.build_preview_doc()
                };
                let preview_width = preview_area.width.saturating_sub(2) as usize;
                if let Some(record_idx) = app.hovered_preview_record_idx(&preview_doc, preview_area, mouse) {
                    app.selected_preview_record_idx = Some(record_idx);
                    if preview_width > 0 {
                        app.preview_scroll_reset_pending = false;
                    }
                    app.active_pane = ActivePane::TurnPreview;
                }
            }
        }
        MouseEventKind::ScrollUp => {
            if let Some(git_graph) = geometry.git_graph
                && point_in_rect(mouse.column, mouse.row, git_graph)
            {
                app.active_pane = ActivePane::GitGraph;
                app.git_graph_scroll = app
                    .git_graph_scroll
                    .saturating_sub(preview_line_step as usize);
            } else if let Some(git_commit) = geometry.git_commit
                && point_in_rect(mouse.column, mouse.row, git_commit)
            {
                app.active_pane = ActivePane::GitCommit;
                app.git_commit_scroll = app
                    .git_commit_scroll
                    .saturating_sub(preview_line_step as usize);
            } else if app.showing_session_browser()
                && !geometry.browser_single
                && point_in_rect(mouse.column, mouse.row, preview_area)
            {
                let pane = if let Some(turn_start) = geometry.turn_start {
                    if point_in_rect(mouse.column, mouse.row, turn_start) {
                        SessionBrowserPane::Start
                    } else {
                        SessionBrowserPane::End
                    }
                } else {
                    SessionBrowserPane::Start
                };
                app.set_session_browser_active_pane(pane);
                app.scroll_preview_lines(
                    -preview_line_step,
                    preview_hit_area.width.saturating_sub(2) as usize,
                );
            } else if point_in_rect(mouse.column, mouse.row, preview_hit_area) {
                app.scroll_preview_lines(
                    -preview_line_step,
                    preview_hit_area.width.saturating_sub(2) as usize,
                );
            } else if !app.show_telemetry && point_in_rect(mouse.column, mouse.row, results_area) {
                app.move_selection(-results_line_step);
            }
        }
        MouseEventKind::ScrollDown => {
            if let Some(git_graph) = geometry.git_graph
                && point_in_rect(mouse.column, mouse.row, git_graph)
            {
                app.active_pane = ActivePane::GitGraph;
                app.git_graph_scroll = app
                    .git_graph_scroll
                    .saturating_add(preview_line_step as usize);
            } else if let Some(git_commit) = geometry.git_commit
                && point_in_rect(mouse.column, mouse.row, git_commit)
            {
                app.active_pane = ActivePane::GitCommit;
                app.git_commit_scroll = app
                    .git_commit_scroll
                    .saturating_add(preview_line_step as usize);
            } else if app.showing_session_browser()
                && !geometry.browser_single
                && point_in_rect(mouse.column, mouse.row, preview_area)
            {
                let pane = if let Some(turn_start) = geometry.turn_start {
                    if point_in_rect(mouse.column, mouse.row, turn_start) {
                        SessionBrowserPane::Start
                    } else {
                        SessionBrowserPane::End
                    }
                } else {
                    SessionBrowserPane::Start
                };
                app.set_session_browser_active_pane(pane);
                app.scroll_preview_lines(
                    preview_line_step,
                    preview_hit_area.width.saturating_sub(2) as usize,
                );
            } else if point_in_rect(mouse.column, mouse.row, preview_hit_area) {
                app.scroll_preview_lines(
                    preview_line_step,
                    preview_hit_area.width.saturating_sub(2) as usize,
                );
            } else if !app.show_telemetry && point_in_rect(mouse.column, mouse.row, results_area) {
                app.move_selection(results_line_step);
            }
        }
        MouseEventKind::Drag(MouseButton::Left) => {
            if let Some(split) = app.dragged_split {
                match split {
                    ActiveSplit::ResultsGit => {
                        let total = geometry.root.width.max(1);
                        let rel = mouse.column.saturating_sub(geometry.root.x) as u32;
                        let pct = ((rel * 100) / total as u32) as i16;
                        app.layout_state.results_pct = pct.clamp(
                            MIN_RESULTS_PCT as i16,
                            (100 - MIN_GIT_PCT - MIN_TURNS_PCT) as i16,
                        ) as u16;
                    }
                    ActiveSplit::GitTurns => {
                        let git_split_rect = geometry.git_graph.or(geometry.git_commit);
                        if let Some(git_split_rect) = git_split_rect {
                            let total =
                                geometry.results.height.saturating_add(git_split_rect.height).max(1);
                            let rel = mouse.row.saturating_sub(geometry.results.y) as u32;
                            let results_pct = ((rel * 100) / total as u32) as i16;
                            let git_pct = 100 - results_pct;
                            let max_git = 100 - MIN_TURNS_PCT;
                            app.layout_state.git_pct =
                                git_pct.clamp(MIN_GIT_PCT as i16, max_git as i16) as u16;
                        }
                    }
                    ActiveSplit::GitColumnVertical => {
                        if let Some(git_graph) = geometry.git_graph {
                            let total =
                                git_graph.height + geometry.git_commit.unwrap_or(git_graph).height;
                            let rel = mouse.row.saturating_sub(git_graph.y) as u32;
                            let pct = ((rel * 100) / total.max(1) as u32) as i16;
                            app.layout_state.graph_pct =
                                pct.clamp(MIN_GRAPH_PCT as i16, MAX_GRAPH_PCT as i16) as u16;
                        }
                    }
                }
            }
        }
        MouseEventKind::Up(MouseButton::Left) => {
            app.dragged_split = None;
        }
        _ => {}
    }
}

fn current_preview_area(
    terminal: &Terminal<CrosstermBackend<Stdout>>,
    app: &App,
) -> anyhow::Result<Rect> {
    let area = terminal.size().context("failed to get terminal size")?;
    let area: Rect = area.into();
    let geometry = app_geometry(area, app);
    let preview_area = if app.show_telemetry {
        Rect::new(
            geometry.root.x,
            geometry.root.y + 1,
            geometry.root.width,
            geometry.root.height.saturating_sub(1),
        )
    } else if let Some(area) = match app.active_pane {
        ActivePane::GitGraph => geometry.git_graph,
        ActivePane::GitCommit => geometry.git_commit,
        ActivePane::SessionBrowser(SessionBrowserPane::Start) => geometry.turn_start,
        ActivePane::SessionBrowser(SessionBrowserPane::End) => geometry.turn_end,
        _ => Some(geometry.turn_preview),
    } {
        area
    } else {
        geometry.turn_preview
    };
    Ok(preview_area)
}

fn current_preview_inner_width(
    terminal: &Terminal<CrosstermBackend<Stdout>>,
    app: &App,
) -> anyhow::Result<usize> {
    Ok(current_preview_area(terminal, app)?
        .width
        .saturating_sub(2) as usize)
}

fn current_preview_inner_height(
    terminal: &Terminal<CrosstermBackend<Stdout>>,
    app: &App,
) -> anyhow::Result<usize> {
    Ok(current_preview_area(terminal, app)?
        .height
        .saturating_sub(2) as usize)
}

impl App {
    fn current_preview_selection(&self, doc: &PreviewDoc, preview_width: usize) -> Option<usize> {
        self.selected_preview_record_idx
            .filter(|record_idx| doc.line_record_indices.contains(&Some(*record_idx)))
            .or_else(|| preview_selected_record_idx(doc, preview_width, self.preview_scroll))
    }

    fn select_preview_record(
        &mut self,
        doc: &PreviewDoc,
        preview_width: usize,
        record_idx: usize,
        align_to_top: bool,
    ) {
        self.selected_preview_record_idx = Some(record_idx);
        if align_to_top
            && let Some(raw_line) = doc
                .line_record_indices
                .iter()
                .position(|line_record_idx| *line_record_idx == Some(record_idx))
        {
            self.preview_scroll = preview_visual_line_offset(&doc.lines, raw_line, preview_width);
        }
        self.preview_scroll_reset_pending = false;
    }

    fn hovered_preview_record_idx(
        &self,
        doc: &PreviewDoc,
        preview_area: Rect,
        mouse: MouseEvent,
    ) -> Option<usize> {
        if mouse.column <= preview_area.x
            || mouse.column >= preview_area.x.saturating_add(preview_area.width).saturating_sub(1)
            || mouse.row <= preview_area.y
            || mouse.row >= preview_area.y.saturating_add(preview_area.height).saturating_sub(1)
        {
            return None;
        }
        let preview_width = preview_area.width.saturating_sub(2) as usize;
        if preview_width == 0 {
            return None;
        }
        let visual_offset = self
            .preview_scroll
            .saturating_add((mouse.row - preview_area.y - 1) as usize);
        let raw = preview_raw_line_index_for_visual_offset(&doc.lines, preview_width, visual_offset);
        doc.line_record_indices.get(raw).and_then(|idx| *idx)
    }

    fn showing_session_browser(&self) -> bool {
        self.ready
            && !self.show_telemetry
            && self.query.trim().is_empty()
            && self.selected_hit().is_some()
    }

    fn toggle_git_graph(&mut self) {
        self.git_graph_visible = !self.git_graph_visible;
        if !self.git_graph_visible {
            self.git_commit_visible = false;
            self.active_pane = ActivePane::TurnPreview;
        } else {
            self.active_pane = ActivePane::GitGraph;
        }
        self.active_split = None;
        self.dragged_split = None;
        self.reset_preview_scroll_to_match();
    }

    fn toggle_git_commit(&mut self) {
        if !self.git_graph_visible {
            return;
        }
        self.git_commit_visible = !self.git_commit_visible;
        if self.git_commit_visible {
            self.active_pane = ActivePane::GitCommit;
        } else if matches!(self.active_pane, ActivePane::GitCommit) {
            self.active_pane = ActivePane::GitGraph;
        }
    }

    fn cycle_layout_preset(&mut self) {
        self.layout_state.preset = self.layout_state.preset.next();
        self.layout_state.reset_to_preset();
        self.set_ui_status(format!(
            "layout preset {}",
            self.layout_state.preset.label()
        ));
    }

    fn adjust_active_split(&mut self, delta: i16) {
        let Some(split) = self.active_split else {
            return;
        };
        self.adjust_split(split, delta);
    }

    fn adjust_split(&mut self, split: ActiveSplit, delta: i16) {
        match split {
            ActiveSplit::ResultsGit => {
                let next = (self.layout_state.results_pct as i16 + delta).clamp(
                    MIN_RESULTS_PCT as i16,
                    (100 - MIN_GIT_PCT - MIN_TURNS_PCT) as i16,
                );
                self.layout_state.results_pct = next as u16;
            }
            ActiveSplit::GitTurns => {
                let max_git = 100 - self.layout_state.results_pct - MIN_TURNS_PCT;
                let next = (self.layout_state.git_pct as i16 + delta)
                    .clamp(MIN_GIT_PCT as i16, max_git as i16);
                self.layout_state.git_pct = next as u16;
            }
            ActiveSplit::GitColumnVertical => {
                let next = (self.layout_state.graph_pct as i16 + delta)
                    .clamp(MIN_GRAPH_PCT as i16, MAX_GRAPH_PCT as i16);
                self.layout_state.graph_pct = next as u16;
            }
        }
    }

    fn log_group_enabled(&self, group: LogGroup) -> bool {
        self.enabled_log_groups.contains(&group)
    }

    fn selected_git_match(
        &mut self,
        preview_width: usize,
        preview_height: usize,
    ) -> Option<GitMatch> {
        if self.show_telemetry || !self.git_graph_visible {
            return None;
        }
        let anchor_record_idx = self.selected_anchor_record_idx(preview_width, preview_height)?;
        let rec = self.all.get(anchor_record_idx)?;
        let cwd = rec.cwd.as_deref()?.trim();
        if cwd.is_empty() {
            return None;
        }
        let repo_root = self.resolve_repo_root(cwd)?;
        let anchor_ts = parse_timestamp_epoch(rec.timestamp.as_deref()?)?;
        let repo = self.git_repo_context(&repo_root).ok()?;
        let selected_commit_idx = selected_git_commit_idx(&repo.commits, anchor_ts)?;
        Some(GitMatch {
            anchor_record_idx,
            repo_root,
            anchor_ts,
            selected_commit_idx,
        })
    }

    fn selected_anchor_record_idx(
        &self,
        preview_width: usize,
        preview_height: usize,
    ) -> Option<usize> {
        if self.show_telemetry {
            return None;
        }
        if self.showing_session_browser() {
            let doc = self.session_browser_doc();
            return self.current_preview_selection(&doc, preview_width);
        }

        let doc = self.build_preview_doc();
        if self.query.trim().is_empty() {
            let raw = preview_center_raw_line(
                &doc.lines,
                preview_width,
                self.preview_scroll,
                preview_height,
            );
            return doc.line_record_indices.get(raw).and_then(|idx| *idx);
        }
        self.current_preview_selection(&doc, preview_width)
    }

    fn resolve_repo_root(&self, cwd: &str) -> Option<PathBuf> {
        let output = Command::new("git")
            .arg("-C")
            .arg(cwd)
            .arg("rev-parse")
            .arg("--show-toplevel")
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let repo_root = String::from_utf8_lossy(&output.stdout).trim().to_string();
        (!repo_root.is_empty()).then(|| PathBuf::from(repo_root))
    }

    fn git_repo_context(&mut self, repo_root: &Path) -> anyhow::Result<&GitRepoContext> {
        if !self.git_repo_cache.contains_key(repo_root) {
            let log_output = Command::new("git")
                .arg("-C")
                .arg(repo_root)
                .arg("log")
                .arg("--pretty=format:%H%x09%ct%x09%s")
                .output()
                .with_context(|| format!("failed to read git log from {}", repo_root.display()))?;
            if !log_output.status.success() {
                return Err(anyhow!("git log failed for {}", repo_root.display()));
            }
            let commits = String::from_utf8_lossy(&log_output.stdout)
                .lines()
                .filter_map(parse_git_commit_line)
                .collect::<Vec<_>>();

            let graph_output = Command::new("git")
                .arg("-C")
                .arg(repo_root)
                .arg("log")
                .arg("--graph")
                .arg("--decorate")
                .arg("--date=short")
                .arg("--pretty=format:%H%x09%h %cd %d %s")
                .output()
                .with_context(|| {
                    format!("failed to render git graph from {}", repo_root.display())
                })?;
            if !graph_output.status.success() {
                return Err(anyhow!("git graph failed for {}", repo_root.display()));
            }

            let mut graph_lines = Vec::new();
            let mut graph_line_commit_indices = Vec::new();
            for line in String::from_utf8_lossy(&graph_output.stdout).lines() {
                let mut parts = line.splitn(2, '\t');
                let hash = parse_git_graph_hash_field(parts.next().unwrap_or("")).unwrap_or("");
                let display = parts.next().unwrap_or(line).to_string();
                let commit_idx = commits.iter().position(|commit| commit.hash == hash);
                graph_lines.push(display);
                graph_line_commit_indices.push(commit_idx);
            }

            self.git_repo_cache.insert(
                repo_root.to_path_buf(),
                GitRepoContext {
                    repo_root: repo_root.to_path_buf(),
                    commits,
                    graph_lines,
                    graph_line_commit_indices,
                },
            );
        }
        self.git_repo_cache
            .get(repo_root)
            .ok_or_else(|| anyhow!("git repo cache missing for {}", repo_root.display()))
    }

    fn git_commit_preview_lines(
        &mut self,
        repo_root: &Path,
        hash: &str,
    ) -> anyhow::Result<&Vec<String>> {
        let key = (repo_root.to_path_buf(), hash.to_string());
        if !self.git_commit_preview_cache.contains_key(&key) {
            let output = Command::new("git")
                .arg("-C")
                .arg(repo_root)
                .arg("show")
                .arg("--stat")
                .arg("--summary")
                .arg("--format=medium")
                .arg("--color=never")
                .arg("--no-ext-diff")
                .arg(hash)
                .output()
                .with_context(|| {
                    format!("failed to render git show for {}", repo_root.display())
                })?;
            if !output.status.success() {
                return Err(anyhow!("git show failed for {}", repo_root.display()));
            }
            let lines = String::from_utf8_lossy(&output.stdout)
                .lines()
                .map(str::to_string)
                .collect::<Vec<_>>();
            self.git_commit_preview_cache.insert(key.clone(), lines);
        }
        self.git_commit_preview_cache.get(&key).ok_or_else(|| {
            anyhow!(
                "git commit preview cache missing for {}",
                repo_root.display()
            )
        })
    }

    fn events_title(&self) -> String {
        let mut groups = self.enabled_log_groups.iter().copied().collect::<Vec<_>>();
        groups.sort_by_key(|group| group.as_str());
        if groups.is_empty() {
            return "Events [no verbose groups]".to_string();
        }
        let joined = groups
            .into_iter()
            .map(log_group_label)
            .collect::<Vec<_>>()
            .join(" ");
        format!("Events [{joined}]")
    }

    fn selected_session_turn_count(&self) -> usize {
        self.selected_hit()
            .and_then(|hit| self.session_records.get(hit.session_idx))
            .map(Vec::len)
            .unwrap_or(0)
    }

    fn preview_title(&self, label: &str) -> String {
        let turns = self.selected_session_turn_count();
        let turn_label = if turns == 1 { "turn" } else { "turns" };
        format!("{label}  {turns} {turn_label}")
    }

    fn turn_position_in_selected_session(&self, record_idx: usize) -> Option<(usize, usize)> {
        let hit = self.selected_hit()?;
        let record_idxs = self.session_records.get(hit.session_idx)?;
        let turn_idx = record_idxs.iter().position(|&idx| idx == record_idx)?;
        Some((turn_idx + 1, record_idxs.len()))
    }

    fn query_preview_title(&self, label: &str, record_idx: Option<usize>) -> String {
        let Some(record_idx) = record_idx else {
            return self.preview_title(label);
        };
        let Some((turn_idx, total_turns)) = self.turn_position_in_selected_session(record_idx) else {
            return self.preview_title(label);
        };
        format!("{label}  turn {turn_idx}/{total_turns}")
    }

    fn turns_preview_title(&self, record_idx: Option<usize>) -> String {
        let Some(record_idx) = record_idx else {
            return self.preview_title("Turns");
        };
        let Some((turn_idx, total_turns)) = self.turn_position_in_selected_session(record_idx) else {
            return self.preview_title("Turns");
        };
        format!("Turns  turn {turn_idx}/{total_turns}")
    }

    fn git_graph_title(&self) -> String {
        if !self.git_graph_visible {
            return "Git Graph".to_string();
        }
        format!("Git Graph  {}", self.layout_state.preset.label())
    }

    fn git_commit_title(&self) -> String {
        "Commit Preview".to_string()
    }

    fn status_text(&self) -> String {
        if let Some(status) = self.ui_status.as_deref() {
            return format!("status: {status}");
        }
        self.indexing
            .last_warn
            .as_deref()
            .map(|s| format!("status: {s}"))
            .unwrap_or_default()
    }

    fn footer_height(&self) -> u16 {
        1
    }

    fn set_ui_status(&mut self, status: impl Into<String>) {
        self.ui_status = Some(status.into());
    }

    fn query_prompt_prefix(&self) -> String {
        if self.show_telemetry {
            return "events> ".to_string();
        }
        if self.active_tag_filters.is_empty() {
            return "> ".to_string();
        }

        let filters = self
            .active_tag_filters
            .iter()
            .map(|filter| match filter.kind {
                TagFilterKind::Provider => format!("provider={}", filter.value),
                TagFilterKind::Host => format!("host={}", filter.value),
                TagFilterKind::Project => format!("project={}", filter.value),
            })
            .collect::<Vec<_>>()
            .join("  ");
        format!("[{filters}]> ")
    }

    fn displayed_query(&self) -> &str {
        if self.show_telemetry {
            &self.telemetry_query
        } else {
            &self.query
        }
    }

    fn displayed_cursor_pos(&self) -> usize {
        if self.show_telemetry {
            self.telemetry_cursor_pos
        } else {
            self.cursor_pos
        }
    }

    fn reset_telemetry_search(&mut self) {
        self.preview_scroll = 0;
        self.preview_scroll_reset_pending = true;
    }

    fn append_event_record(&mut self, record: telemetry::EventRecord) {
        self.telemetry_events.push(record);
    }

    fn emit_event_record(&mut self, record: telemetry::EventRecord) {
        self.telemetry_events.push(record.clone());
        if let Some(sink) = self.telemetry_sink.as_mut()
            && let Err(err) = sink.emit_record(&record)
        {
            self.indexing.last_warn = Some(format!("telemetry log write failed: {err:#}"));
        }
    }

    fn emit_event(&mut self, kind: &str, data: Value) {
        self.emit_event_record(telemetry::EventRecord::new(None, kind, data));
    }

    fn emit_group_event(&mut self, group: LogGroup, kind: &str, data: Value) {
        self.emit_event_record(telemetry::EventRecord::new(
            Some(group.as_str()),
            kind,
            data,
        ));
    }

    fn matches_active_tag_filters(&self, sess: &SessionSummary) -> bool {
        self.active_tag_filters
            .iter()
            .all(|filter| match filter.kind {
                TagFilterKind::Provider => {
                    provider_label(sess.source, sess.account.as_deref()) == filter.value
                }
                TagFilterKind::Host => sess.machine_name == filter.value,
                TagFilterKind::Project => {
                    sess.project_slug.as_deref() == Some(filter.value.as_str())
                }
            })
    }

    fn toggle_tag_filter(&mut self, filter: TagFilter) {
        if let Some(existing_idx) = self
            .active_tag_filters
            .iter()
            .position(|existing| existing.kind == filter.kind)
        {
            if self.active_tag_filters[existing_idx].value == filter.value {
                self.active_tag_filters.remove(existing_idx);
            } else {
                self.active_tag_filters[existing_idx] = filter;
            }
        } else {
            self.active_tag_filters.push(filter);
        }
        self.update_results();
    }

    fn clear_query_and_filters(&mut self) {
        self.query.clear();
        self.cursor_pos = 0;
        self.active_tag_filters.clear();
        self.update_results();
    }

    fn matched_turn_count(&self) -> usize {
        self.filtered.iter().map(|hit| hit.hit_count).sum()
    }

    fn total_turn_count(&self) -> usize {
        self.all.len()
    }

    fn update_results(&mut self) {
        let q = self.query.trim().to_string();
        let started = Instant::now();
        let perf_enabled = self.log_group_enabled(LogGroup::Perf);
        let mut candidate_phase_started = perf_enabled.then(Instant::now);

        let prev_selected_global = self.filtered.get(self.selected).map(|hit| hit.session_idx);

        let max = self.max_results;
        let limit = |n: usize| -> bool { max != 0 && n >= max };

        let mut results: Vec<SessionHit> = Vec::new();
        let mut candidate_sessions = self.sessions.len();
        let mut reused_prefix_cache = false;
        let mut candidate_phase_duration_ms = 0u128;
        let mut match_phase_duration_ms = 0u128;

        if q.is_empty() {
            let mut i = 0usize;
            while i < self.sessions.len() && !limit(results.len()) {
                if self.matches_active_tag_filters(&self.sessions[i]) {
                    results.push(SessionHit {
                        session_idx: i,
                        matched_record_idx: None,
                        hit_count: 0,
                    });
                }
                i += 1;
            }
            if let Some(started) = candidate_phase_started.take() {
                candidate_phase_duration_ms = started.elapsed().as_millis();
            }
        } else {
            let use_cached_prefix = !self.last_query.is_empty()
                && q.starts_with(&self.last_query)
                && !self.last_results.is_empty()
                && self.active_tag_filters == self.last_tag_filters;
            reused_prefix_cache = use_cached_prefix;
            candidate_sessions = if use_cached_prefix {
                self.last_results.len()
            } else {
                self.sessions
                    .iter()
                    .filter(|sess| self.matches_active_tag_filters(sess))
                    .count()
            };
            if let Some(started) = candidate_phase_started.take() {
                candidate_phase_duration_ms = started.elapsed().as_millis();
            }
            let base: Box<dyn Iterator<Item = usize>> =
                if use_cached_prefix {
                    Box::new(self.last_results.iter().copied())
                } else {
                    Box::new(self.sessions.iter().enumerate().filter_map(|(idx, sess)| {
                        self.matches_active_tag_filters(sess).then_some(idx)
                    }))
                };

            let compiled = search::CompiledQuery::new(&q);
            let match_phase_started = perf_enabled.then(Instant::now);
            for idx in base {
                if let Some(hit) = self.session_match(idx, &compiled) {
                    results.push(hit);
                    if limit(results.len()) {
                        break;
                    }
                }
            }
            if let Some(started) = match_phase_started {
                match_phase_duration_ms = started.elapsed().as_millis();
            }
        }

        self.filtered = results;
        self.last_query = q.clone();
        self.last_results = self.filtered.iter().map(|hit| hit.session_idx).collect();
        self.last_tag_filters = self.active_tag_filters.clone();

        self.offset = 0;
        self.selected = 0;
        if let Some(prev) = prev_selected_global
            && let Some(pos) = self.filtered.iter().position(|hit| hit.session_idx == prev)
        {
            self.selected = pos;
        }
        self.reset_preview_scroll_to_match();

        if perf_enabled {
            self.emit_group_event(
                LogGroup::Perf,
                "perf_results_summary",
                json!({
                    "query": self.last_query,
                    "tag_filter_count": self.active_tag_filters.len(),
                    "duration_ms": started.elapsed().as_millis(),
                    "candidate_sessions": candidate_sessions,
                    "matched_sessions": self.filtered.len(),
                    "matched_turns": self.matched_turn_count(),
                    "total_sessions": self.sessions.len(),
                    "total_turns": self.total_turn_count(),
                    "reused_prefix_cache": reused_prefix_cache,
                }),
            );
            self.emit_group_event(
                LogGroup::Perf,
                "perf_results_phase",
                json!({
                    "phase": "candidate_enumeration",
                    "duration_ms": candidate_phase_duration_ms,
                    "candidate_sessions": candidate_sessions,
                    "matched_sessions": self.filtered.len(),
                }),
            );
            if !self.last_query.is_empty() {
                self.emit_group_event(
                    LogGroup::Perf,
                    "perf_results_phase",
                    json!({
                        "phase": "query_match_scan",
                        "duration_ms": match_phase_duration_ms,
                        "candidate_sessions": candidate_sessions,
                        "matched_sessions": self.filtered.len(),
                    }),
                );
            }
        }
    }

    fn session_match(
        &self,
        session_idx: usize,
        query: &search::CompiledQuery,
    ) -> Option<SessionHit> {
        let Some(record_idxs) = self.session_records.get(session_idx) else {
            return None;
        };
        let mut matched_record_idx: Option<usize> = None;
        let mut hit_count = 0usize;
        for &idx in record_idxs {
            if query.matches_record(&self.all[idx]) {
                hit_count = hit_count.saturating_add(1);
                if matched_record_idx.is_none() {
                    matched_record_idx = Some(idx);
                }
            }
        }
        matched_record_idx.map(|matched_record_idx| SessionHit {
            session_idx,
            matched_record_idx: Some(matched_record_idx),
            hit_count,
        })
    }

    fn selected_hit(&self) -> Option<SessionHit> {
        self.filtered.get(self.selected).copied()
    }

    fn selected_record(&self) -> Option<&MessageRecord> {
        let hit = self.selected_hit()?;
        if let Some(idx) = hit.matched_record_idx {
            return self.all.get(idx);
        }
        let session = self.sessions.get(hit.session_idx)?;
        self.all.get(session.first_user_idx)
    }

    fn reset_preview_scroll_to_match(&mut self) {
        self.selected_preview_record_idx = self.selected_hit().and_then(|hit| {
            hit.matched_record_idx.or_else(|| {
                self.sessions
                    .get(hit.session_idx)
                    .map(|session| session.first_user_idx)
            })
        });
        if self.showing_session_browser() {
            self.session_browser_start_scroll = 0;
            self.session_browser_end_scroll = usize::MAX;
            self.session_browser_active_pane = SessionBrowserPane::Start;
            if !matches!(self.active_pane, ActivePane::GitGraph | ActivePane::GitCommit) {
                self.active_pane = ActivePane::TurnPreview;
            }
            self.preview_scroll_reset_pending = true;
        } else {
            self.preview_scroll_reset_pending = true;
            if !matches!(
                self.active_pane,
                ActivePane::GitGraph | ActivePane::GitCommit
            ) {
                self.active_pane = ActivePane::TurnPreview;
            }
        }
        self.git_graph_scroll = 0;
        self.git_commit_scroll = 0;
    }

    fn session_browser_doc(&self) -> PreviewDoc {
        let Some(hit) = self.selected_hit() else {
            return PreviewDoc {
                lines: vec![Line::raw("(no match)")],
                first_match_line: 0,
                match_lines: Vec::new(),
                line_record_indices: vec![None],
            };
        };
        let Some(sess) = self.sessions.get(hit.session_idx) else {
            return PreviewDoc {
                lines: vec![Line::raw("(no match)")],
                first_match_line: 0,
                match_lines: Vec::new(),
                line_record_indices: vec![None],
            };
        };
        let Some(record_idxs) = self.session_records.get(hit.session_idx) else {
            return PreviewDoc {
                lines: vec![Line::raw("(no match)")],
                first_match_line: 0,
                match_lines: Vec::new(),
                line_record_indices: vec![None],
            };
        };

        let base_style = Style::default();
        let mut line_record_indices = Vec::new();

        let mut lines: Vec<Line<'static>> = vec![
            Line::raw(format!("session id: {}", sess.session_id)),
            Line::raw(format!(
                "account: {}",
                sess.account.as_deref().unwrap_or("")
            )),
            Line::raw(format!("host: {}", sess.machine_name)),
            Line::raw(format!("machine id: {}", sess.machine_id)),
            Line::raw(format!("origin: {}", sess.origin)),
            Line::raw(format!(
                "project: {}",
                sess.project_slug.as_deref().unwrap_or("")
            )),
            Line::raw(format!("source: {}", source_label(sess.source))),
            Line::raw(format!("turns: {}", record_idxs.len())),
            Line::raw(""),
        ];
        line_record_indices.resize(lines.len(), None);

        for (pos, &idx) in record_idxs.iter().enumerate() {
            let Some(rec) = self.all.get(idx) else {
                continue;
            };
            let role = match rec.role {
                Role::User => "user",
                Role::Assistant => "assistant",
                Role::System => "system",
                Role::Tool => "tool",
                Role::Unknown => "unknown",
            };

            lines.push(Line::raw(format!(
                "turn {}   {}   role: {role}   phase: {}",
                pos + 1,
                short_ts(rec.timestamp.as_deref()),
                rec.phase.as_deref().unwrap_or("")
            )));
            line_record_indices.push(Some(idx));
            lines.push(Line::raw(format!(
                "file: {}:{}",
                rec.file.display(),
                rec.line
            )));
            line_record_indices.push(Some(idx));
            if !rec.images.is_empty() {
                lines.push(Line::raw(format!("images: {}", rec.images.len())));
                line_record_indices.push(Some(idx));
                for path in materialize_record_images(rec) {
                    lines.push(Line::raw(format!("image file: {path}")));
                    line_record_indices.push(Some(idx));
                }
            }
            let rendered_lines = render_preview_message_lines(&rec.text, "", base_style);
            line_record_indices.extend(std::iter::repeat_n(Some(idx), rendered_lines.len()));
            lines.extend(rendered_lines);
            lines.push(Line::raw(""));
            line_record_indices.push(Some(idx));
        }

        PreviewDoc {
            lines,
            first_match_line: 0,
            match_lines: Vec::new(),
            line_record_indices,
        }
    }

    fn build_preview_doc(&self) -> PreviewDoc {
        if self.show_telemetry {
            return self.build_telemetry_preview_doc();
        }

        let query = self.query.trim();
        let base_style = Style::default();

        let Some(hit) = self.selected_hit() else {
            return PreviewDoc {
                lines: vec![Line::raw("(no match)")],
                first_match_line: 0,
                match_lines: Vec::new(),
                line_record_indices: vec![None],
            };
        };
        let Some(sess) = self.sessions.get(hit.session_idx) else {
            return PreviewDoc {
                lines: vec![Line::raw("(no match)")],
                first_match_line: 0,
                match_lines: Vec::new(),
                line_record_indices: vec![None],
            };
        };

        if query.is_empty() {
            let Some(rec) = self.selected_record() else {
                return PreviewDoc {
                    lines: vec![Line::raw("(no match)")],
                    first_match_line: 0,
                    match_lines: Vec::new(),
                    line_record_indices: vec![None],
                };
            };

            let role = match rec.role {
                Role::User => "user",
                Role::Assistant => "assistant",
                Role::System => "system",
                Role::Tool => "tool",
                Role::Unknown => "unknown",
            };

            let mut lines = vec![
                Line::raw(format!("timestamp: {}", short_ts(rec.timestamp.as_deref()))),
                Line::raw(format!("account: {}", rec.account.as_deref().unwrap_or(""))),
                Line::raw(format!("host: {}", rec.machine_name)),
                Line::raw(format!("machine id: {}", rec.machine_id)),
                Line::raw(format!("origin: {}", rec.origin)),
                Line::raw(format!(
                    "project: {}",
                    rec.project_slug.as_deref().unwrap_or("")
                )),
                Line::raw(format!(
                    "role: {role}   phase: {}",
                    rec.phase.as_deref().unwrap_or("")
                )),
                Line::raw(format!("cwd: {}", rec.cwd.as_deref().unwrap_or(""))),
                Line::raw(format!("file: {}:{}", rec.file.display(), rec.line)),
                Line::raw(format!("source: {}", source_label(rec.source))),
                Line::raw(""),
            ];
            let record_idx = hit.matched_record_idx.unwrap_or(sess.first_user_idx);
            if !rec.images.is_empty() {
                lines.push(Line::raw(format!("images: {}", rec.images.len())));
                for path in materialize_record_images(rec) {
                    lines.push(Line::raw(format!("image file: {path}")));
                }
                lines.push(Line::raw(""));
            }
            let mut line_record_indices = vec![Some(record_idx); lines.len()];
            let rendered_lines = render_preview_message_lines(&rec.text, query, base_style);
            line_record_indices.extend(std::iter::repeat_n(Some(record_idx), rendered_lines.len()));
            lines.extend(rendered_lines);

            return PreviewDoc {
                lines,
                first_match_line: 0,
                match_lines: Vec::new(),
                line_record_indices,
            };
        }

        let compiled = search::CompiledQuery::new(query);
        let Some(record_idxs) = self.session_records.get(hit.session_idx) else {
            return PreviewDoc {
                lines: vec![Line::raw("(no match)")],
                first_match_line: 0,
                match_lines: Vec::new(),
                line_record_indices: vec![None],
            };
        };

        let matched_indices: Vec<usize> = record_idxs
            .iter()
            .copied()
            .filter(|&idx| compiled.matches_record(&self.all[idx]))
            .collect();

        let total_matches = matched_indices.len();
        let total_occurrences: usize = matched_indices
            .iter()
            .map(|&idx| record_match_occurrence_count(query, &self.all[idx]))
            .sum();
        let shown_match_target = cmp::min(total_matches, PREVIEW_MAX_MATCHES);

        let mut lines: Vec<Line<'static>> = vec![
            Line::raw(format!("session id: {}", sess.session_id)),
            Line::raw(format!(
                "account: {}",
                sess.account.as_deref().unwrap_or("")
            )),
            Line::raw(format!("host: {}", sess.machine_name)),
            Line::raw(format!("machine id: {}", sess.machine_id)),
            Line::raw(format!("origin: {}", sess.origin)),
            Line::raw(format!(
                "project: {}",
                sess.project_slug.as_deref().unwrap_or("")
            )),
            Line::raw(format!("source: {}", source_label(sess.source))),
            Line::raw(format!("session opener: {}", sess.first_line)),
            Line::raw(format!(
                "showing {} of {} matching messages",
                shown_match_target, total_matches
            )),
            Line::raw(format!(
                "total query occurrences in shown message text: {total_occurrences}"
            )),
            Line::raw(format!(
                "preview limits: {} matches, {} lines",
                PREVIEW_MAX_MATCHES, PREVIEW_MAX_LINES
            )),
            Line::raw(""),
        ];
        let mut line_record_indices = vec![None; lines.len()];

        let first_match_line = lines.len();
        let mut match_lines = Vec::new();
        let mut shown_matches = 0usize;
        let mut lines_used = lines.len();
        let mut line_limited = false;

        for (i, &rec_idx) in matched_indices.iter().enumerate() {
            if shown_matches >= PREVIEW_MAX_MATCHES {
                break;
            }
            let rec = &self.all[rec_idx];
            let role = match rec.role {
                Role::User => "user",
                Role::Assistant => "assistant",
                Role::System => "system",
                Role::Tool => "tool",
                Role::Unknown => "unknown",
            };
            let occurrence_count = record_match_occurrence_count(query, rec);

            let (section, section_record_indices): (Vec<Line<'static>>, Vec<Option<usize>>) = {
                let mut section = vec![
                    Line::raw(format!("-- hit {}/{} --", i + 1, total_matches)),
                    Line::raw(format!("timestamp: {}", short_ts(rec.timestamp.as_deref()))),
                    Line::raw(format!("account: {}", rec.account.as_deref().unwrap_or(""))),
                    Line::raw(format!(
                        "role: {role}   phase: {}",
                        rec.phase.as_deref().unwrap_or("")
                    )),
                    Line::raw(format!("cwd: {}", rec.cwd.as_deref().unwrap_or(""))),
                    Line::raw(format!("file: {}:{}", rec.file.display(), rec.line)),
                    Line::raw(format!(
                        "query occurrences in this message: {occurrence_count}"
                    )),
                    Line::raw(format!("images: {}", rec.images.len())),
                    Line::raw(""),
                ];
                let mut section_record_indices = vec![Some(rec_idx); section.len()];
                if !rec.images.is_empty() {
                    for path in materialize_record_images(rec) {
                        section.push(Line::raw(format!("image file: {path}")));
                        section_record_indices.push(Some(rec_idx));
                    }
                    section.push(Line::raw(""));
                    section_record_indices.push(Some(rec_idx));
                }
                for raw_line in rec.text.lines() {
                    let raw_line_index = section.len();
                    let rendered_lines = render_preview_message_lines(raw_line, query, base_style);
                    section_record_indices
                        .extend(std::iter::repeat_n(Some(rec_idx), rendered_lines.len()));
                    section.extend(rendered_lines);
                    if !search::find_match_ranges(query, raw_line).is_empty() {
                        match_lines.push(lines_used + raw_line_index);
                    }
                }
                section.push(Line::raw(""));
                section_record_indices.push(Some(rec_idx));
                (section, section_record_indices)
            };

            if lines_used.saturating_add(section.len()) > PREVIEW_MAX_LINES {
                line_limited = true;
                break;
            }

            lines_used += section.len();
            lines.extend(section);
            line_record_indices.extend(section_record_indices);
            shown_matches += 1;
        }

        if shown_matches < total_matches || line_limited {
            let omitted_matches = total_matches.saturating_sub(shown_matches);
            let reason = if line_limited {
                format!("line limit ({PREVIEW_MAX_LINES})")
            } else {
                format!("match limit ({PREVIEW_MAX_MATCHES})")
            };
            lines.push(Line::raw(format!(
                "... truncated preview: {} more matching messages not shown due to {}",
                omitted_matches, reason
            )));
        }

        PreviewDoc {
            lines,
            first_match_line,
            match_lines,
            line_record_indices,
        }
    }

    fn build_telemetry_preview_doc(&self) -> PreviewDoc {
        let Some(path) = self.telemetry_log_path.as_ref() else {
            return PreviewDoc {
                lines: vec![Line::raw("events disabled")],
                first_match_line: 0,
                match_lines: Vec::new(),
                line_record_indices: vec![None],
            };
        };

        let mut lines: Vec<Line<'static>> = vec![
            Line::raw(format!("log: {}", path.display())),
            Line::raw(format!(
                "buffer cap: {} MB",
                EVENT_BUFFER_MAX_BYTES / (1024 * 1024)
            )),
        ];
        if let Ok(metadata) = fs::metadata(path) {
            lines.push(Line::raw(format!("bytes: {}", metadata.len())));
        }

        let buffered_records = self.telemetry_events.iter().collect::<Vec<_>>();
        if buffered_records.is_empty() {
            lines.push(Line::raw(""));
            lines.push(Line::raw("(no events yet)"));
            let line_count = lines.len();
            return PreviewDoc {
                lines,
                first_match_line: 0,
                match_lines: Vec::new(),
                line_record_indices: vec![None; line_count],
            };
        }

        let filter_query = self.telemetry_query.trim();
        let find_data = |kind: &str| {
            buffered_records
                .iter()
                .rev()
                .find(|record| record.kind == kind)
                .map(|record| &record.data)
        };

        if filter_query.is_empty() {
            lines.push(Line::raw(format!(
                "buffered events: {}",
                buffered_records.len()
            )));
            if let Some(data) = find_data("cache_open_finished") {
                lines.push(Line::raw(format!(
                    "cache open: {} ms   size: {} bytes",
                    telemetry_metric_u64(data, "duration_ms").unwrap_or(0),
                    telemetry_metric_u64(data, "cache_bytes").unwrap_or(0)
                )));
            }
            if let Some(data) = find_data("cache_load_finished") {
                lines.push(Line::raw(format!(
                    "cache load: {} ms   records: {}   sessions: {}",
                    telemetry_metric_u64(data, "duration_ms").unwrap_or(0),
                    telemetry_metric_u64(data, "records").unwrap_or(0),
                    telemetry_metric_u64(data, "sessions").unwrap_or(0)
                )));
            }
            if let Some(data) = find_data("refresh_finished") {
                lines.push(Line::raw(format!(
                    "refresh: {} ms   skipped: {}   refreshed: {}   failed: {}",
                    telemetry_metric_u64(data, "duration_ms").unwrap_or(0),
                    telemetry_metric_u64(data, "skipped_units").unwrap_or(0),
                    telemetry_metric_u64(data, "refreshed_units").unwrap_or(0),
                    telemetry_metric_u64(data, "failed_units").unwrap_or(0)
                )));
            }
            if let Some(data) = find_data("cache_reload_finished") {
                lines.push(Line::raw(format!(
                    "cache reload: {} ms   records: {}   sessions: {}",
                    telemetry_metric_u64(data, "duration_ms").unwrap_or(0),
                    telemetry_metric_u64(data, "records").unwrap_or(0),
                    telemetry_metric_u64(data, "sessions").unwrap_or(0)
                )));
            }
            if let Some(data) = find_data("indexer_finished") {
                lines.push(Line::raw(format!(
                    "total: {} ms",
                    telemetry_metric_u64(data, "duration_ms").unwrap_or(0),
                )));
            }
            if let Some(cache_path) = self.cache_path.as_ref()
                && let Ok(store) = cache::CacheStore::open(cache_path, false)
                && let Ok(states) = store.load_remote_sync_states()
                && !states.is_empty()
            {
                lines.push(Line::raw(""));
                lines.push(Line::raw("remote sync:"));
                for state in states {
                    lines.push(Line::raw(format!(
                        "{}@{} attempted={} success={} records={} sessions={} duration={}ms error={}",
                        state.remote_name,
                        state.host,
                        state.last_attempted_ms.unwrap_or(0),
                        state.last_success_ms.unwrap_or(0),
                        state.imported_records,
                        state.imported_sessions,
                        state.last_duration_ms.unwrap_or(0),
                        state.last_error.unwrap_or_default(),
                    )));
                }
            }
            lines.push(Line::raw(""));
        } else {
            lines.push(Line::raw(format!("search: {filter_query}")));
            lines.push(Line::raw(""));
        }
        lines.push(Line::raw(if filter_query.is_empty() {
            "events:"
        } else {
            "matching events:"
        }));
        lines.push(Line::raw(""));

        let mut first_match_line = lines.len();
        let mut match_lines = Vec::new();
        for record in buffered_records.iter().rev() {
            let rendered = format_telemetry_event_line(record);
            if !filter_query.is_empty()
                && search::find_match_ranges(filter_query, &rendered).is_empty()
            {
                continue;
            }
            if match_lines.is_empty() {
                first_match_line = lines.len();
            }
            match_lines.push(lines.len());
            lines.push(highlighted_line(&rendered, filter_query, Style::default()));
        }
        if filter_query.is_empty() && match_lines.is_empty() {
            first_match_line = lines.len();
        }
        if !filter_query.is_empty() && match_lines.is_empty() {
            lines.push(Line::raw("(no matching events)"));
        }

        let line_count = lines.len();
        PreviewDoc {
            lines,
            first_match_line,
            match_lines,
            line_record_indices: vec![None; line_count],
        }
    }

    fn build_git_graph_doc(&mut self, preview_width: usize, preview_height: usize) -> GitPaneDoc {
        let Some(git_match) = self.selected_git_match(preview_width, preview_height) else {
            return GitPaneDoc {
                lines: vec![
                    Line::raw("git graph unavailable"),
                    Line::raw("select a turn with a timestamp and git-backed cwd"),
                ],
            };
        };
        let anchor_ts = short_ts(
            self.all
                .get(git_match.anchor_record_idx)
                .and_then(|rec| rec.timestamp.as_deref()),
        );
        let Ok(repo) = self.git_repo_context(&git_match.repo_root) else {
            return GitPaneDoc {
                lines: vec![Line::raw("failed to load git graph")],
            };
        };
        let selected_commit = &repo.commits[git_match.selected_commit_idx];
        let selected_graph_line = repo
            .graph_line_commit_indices
            .iter()
            .position(|idx| *idx == Some(git_match.selected_commit_idx))
            .unwrap_or(0);
        let window_radius = preview_height.max(4);
        let start = selected_graph_line.saturating_sub(window_radius);
        let end = cmp::min(
            repo.graph_lines.len(),
            selected_graph_line + window_radius + 1,
        );
        let mut lines = vec![
            Line::raw(format!("repo: {}", repo.repo_root.display())),
            Line::raw(format!(
                "anchor: {}  selected commit: {}  {}",
                anchor_ts,
                &selected_commit.hash[..selected_commit.hash.len().min(12)],
                selected_commit.summary
            )),
            Line::raw(""),
        ];
        for (idx, line) in repo.graph_lines[start..end].iter().enumerate() {
            let absolute = start + idx;
            let style = if absolute == selected_graph_line {
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Yellow)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            lines.push(Line::from(vec![Span::styled(line.clone(), style)]));
        }
        GitPaneDoc { lines }
    }

    fn build_git_commit_doc(&mut self, preview_width: usize, preview_height: usize) -> GitPaneDoc {
        let Some(git_match) = self.selected_git_match(preview_width, preview_height) else {
            return GitPaneDoc {
                lines: vec![Line::raw("commit preview unavailable")],
            };
        };
        let (hash, summary) = {
            let Ok(repo) = self.git_repo_context(&git_match.repo_root) else {
                return GitPaneDoc {
                    lines: vec![Line::raw("failed to load commit preview")],
                };
            };
            let commit = &repo.commits[git_match.selected_commit_idx];
            (commit.hash.clone(), commit.summary.clone())
        };
        let Ok(preview_lines) = self.git_commit_preview_lines(&git_match.repo_root, &hash) else {
            return GitPaneDoc {
                lines: vec![Line::raw("failed to render commit preview")],
            };
        };
        let mut lines = vec![
            Line::raw(format!("repo: {}", git_match.repo_root.display())),
            Line::raw(format!(
                "commit: {}  anchor epoch: {}  {}",
                &hash[..hash.len().min(12)],
                git_match.anchor_ts,
                summary
            )),
            Line::raw(""),
        ];
        lines.extend(
            preview_lines
                .iter()
                .map(|line| highlighted_line(line, "", Style::default())),
        );
        GitPaneDoc { lines }
    }

    fn emit_preview_perf_summary(&mut self, mode: &str, doc: &PreviewDoc, duration_ms: u128) {
        if !self.log_group_enabled(LogGroup::Perf) || self.show_telemetry {
            return;
        }

        let Some(hit) = self.selected_hit() else {
            return;
        };
        let Some(sess) = self.sessions.get(hit.session_idx) else {
            return;
        };
        let record_idxs = self
            .session_records
            .get(hit.session_idx)
            .cloned()
            .unwrap_or_default();
        let session_records = record_idxs.len();
        let query = self.query.trim();
        let matched_record_idxs = if query.is_empty() {
            record_idxs.clone()
        } else {
            let compiled = search::CompiledQuery::new(query);
            record_idxs
                .iter()
                .copied()
                .filter(|&idx| compiled.matches_record(&self.all[idx]))
                .collect::<Vec<_>>()
        };
        let rendered_records = if query.is_empty() {
            matched_record_idxs.len()
        } else {
            matched_record_idxs.len().min(PREVIEW_MAX_MATCHES)
        };
        let processed_record_idxs = if query.is_empty() {
            matched_record_idxs
        } else {
            matched_record_idxs
                .into_iter()
                .take(PREVIEW_MAX_MATCHES)
                .collect::<Vec<_>>()
        };
        let raw_lines = processed_record_idxs
            .iter()
            .filter_map(|&idx| self.all.get(idx))
            .map(|rec| rec.text.lines().count())
            .sum::<usize>();
        let raw_bytes = processed_record_idxs
            .iter()
            .filter_map(|&idx| self.all.get(idx))
            .map(|rec| rec.text.len())
            .sum::<usize>();

        self.emit_group_event(
            LogGroup::Perf,
            "perf_preview_summary",
            json!({
                "mode": mode,
                "session_id": sess.session_id,
                "duration_ms": duration_ms,
                "session_records": session_records,
                "matched_records": if query.is_empty() { session_records } else { processed_record_idxs.len() },
                "rendered_records": rendered_records,
                "raw_lines": raw_lines,
                "raw_bytes": raw_bytes,
                "rendered_lines": doc.lines.len(),
                "first_match_line": doc.first_match_line,
                "truncated_by_match_limit": !query.is_empty() && processed_record_idxs.len() < self.session_records.get(hit.session_idx).map(|records| {
                    let compiled = search::CompiledQuery::new(query);
                    records.iter().filter(|&&idx| compiled.matches_record(&self.all[idx])).count()
                }).unwrap_or_default(),
                "truncated_by_line_limit": doc.lines.iter().count() >= PREVIEW_MAX_LINES,
            }),
        );
        self.emit_group_event(
            LogGroup::Perf,
            "perf_preview_phase",
            json!({
                "mode": mode,
                "phase": "document_build",
                "duration_ms": duration_ms,
                "records": rendered_records,
                "lines": doc.lines.len(),
                "bytes": raw_bytes,
            }),
        );
    }

    fn sync_selected_preview_record_from_scroll(&mut self, preview_width: usize) {
        if preview_width == 0 || (!self.showing_session_browser() && self.query.trim().is_empty()) {
            return;
        }
        let doc = if self.showing_session_browser() {
            self.session_browser_doc()
        } else {
            self.build_preview_doc()
        };
        self.selected_preview_record_idx =
            preview_selected_record_idx(&doc, preview_width, self.preview_scroll);
    }

    fn scroll_preview_lines(&mut self, delta: i32, preview_width: usize) {
        if self.show_telemetry {
            let cur = self.preview_scroll as i32;
            self.preview_scroll = cmp::max(0, cur + delta) as usize;
            self.preview_scroll_reset_pending = false;
            return;
        }
        match self.active_pane {
            ActivePane::GitGraph => {
                let cur = self.git_graph_scroll as i32;
                self.git_graph_scroll = cmp::max(0, cur + delta) as usize;
                return;
            }
            ActivePane::GitCommit => {
                let cur = self.git_commit_scroll as i32;
                self.git_commit_scroll = cmp::max(0, cur + delta) as usize;
                return;
            }
            _ => {}
        }
        if self.showing_session_browser() {
            let cur = self.preview_scroll as i32;
            self.preview_scroll = cmp::max(0, cur + delta) as usize;
            self.sync_selected_preview_record_from_scroll(preview_width);
            self.preview_scroll_reset_pending = false;
            return;
        }
        let cur = self.preview_scroll as i32;
        self.preview_scroll = cmp::max(0, cur + delta) as usize;
        self.sync_selected_preview_record_from_scroll(preview_width);
        self.preview_scroll_reset_pending = false;
    }

    fn scroll_preview_page(&mut self, dir: i32, page_height: usize, preview_width: usize) {
        let delta = cmp::max(1, page_height as i32 - 1) * dir;
        self.scroll_preview_lines(delta, preview_width);
    }

    fn jump_preview_record(&mut self, dir: i32, preview_width: usize) {
        if self.show_telemetry || preview_width == 0 {
            return;
        }

        let doc = if self.showing_session_browser() {
            self.session_browser_doc()
        } else {
            self.build_preview_doc()
        };
        let section_records = preview_section_record_indices(&doc);
        if section_records.len() <= 1 {
            return;
        }
        let current_record_idx = self
            .current_preview_selection(&doc, preview_width)
            .unwrap_or(section_records[0]);
        let current_pos = section_records
            .iter()
            .position(|&record_idx| record_idx == current_record_idx)
            .unwrap_or(0);
        let target_pos = if dir >= 0 {
            if current_pos + 1 < section_records.len() {
                current_pos + 1
            } else {
                0
            }
        } else {
            current_pos.checked_sub(1).unwrap_or(section_records.len() - 1)
        };
        self.select_preview_record(&doc, preview_width, section_records[target_pos], true);
    }

    fn jump_preview_match(&mut self, dir: i32, preview_width: usize) {
        if self.showing_session_browser() || self.show_telemetry || self.query.trim().is_empty() {
            return;
        }
        let doc = self.build_preview_doc();
        if doc.match_lines.is_empty() {
            return;
        }

        let current_raw = preview_raw_line_index_for_visual_offset(
            &doc.lines,
            preview_width,
            self.preview_scroll,
        );
        let target_raw = if dir >= 0 {
            doc.match_lines
                .iter()
                .copied()
                .find(|&line| line > current_raw)
                .unwrap_or(doc.match_lines[0])
        } else {
            doc.match_lines
                .iter()
                .copied()
                .rev()
                .find(|&line| line < current_raw)
                .unwrap_or(*doc.match_lines.last().unwrap())
        };

        self.preview_scroll =
            preview_visual_line_offset(&doc.lines, target_raw, preview_width).saturating_sub(2);
        self.preview_scroll_reset_pending = false;
    }

    fn move_selection(&mut self, delta: i32) {
        if self.filtered.is_empty() {
            self.selected = 0;
            return;
        }
        let max = (self.filtered.len() - 1) as i32;
        let cur = self.selected as i32;
        let next = cmp::min(max, cmp::max(0, cur + delta));
        self.selected = next as usize;
        self.reset_preview_scroll_to_match();
    }

    fn select_first(&mut self) {
        self.selected = 0;
        self.reset_preview_scroll_to_match();
    }

    fn select_last(&mut self) {
        if self.filtered.is_empty() {
            self.selected = 0;
            return;
        }
        self.selected = self.filtered.len() - 1;
        self.reset_preview_scroll_to_match();
    }

    fn toggle_telemetry_view(&mut self) {
        self.show_telemetry = !self.show_telemetry;
        if self.show_telemetry && !self.log_groups_help_emitted {
            self.emit_event(
                "log_groups_help",
                json!({
                    "summary": "While Events is open, Ctrl+g toggles perf logging.",
                }),
            );
            self.log_groups_help_emitted = true;
        }
        self.reset_preview_scroll_to_match();
    }

    fn toggle_log_group(&mut self, group: LogGroup) {
        let enabled = if self.enabled_log_groups.contains(&group) {
            self.enabled_log_groups.remove(&group);
            false
        } else {
            self.enabled_log_groups.insert(group);
            true
        };
        self.set_ui_status(format!(
            "{} logging {}",
            log_group_label(group),
            if enabled { "enabled" } else { "disabled" }
        ));
        self.emit_event(
            "log_group_toggle",
            json!({
                "group": group.as_str(),
                "enabled": enabled,
                "hotkey": log_group_toggle_key(group),
                "summary": format!("{} logging {}", log_group_label(group), if enabled { "enabled" } else { "disabled" }),
            }),
        );
    }

    fn set_session_browser_active_pane(&mut self, pane: SessionBrowserPane) {
        self.session_browser_active_pane = pane;
        self.active_pane = ActivePane::SessionBrowser(pane);
    }
}

fn ui(f: &mut ratatui::Frame, app: &mut App) {
    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints(
            [
                Constraint::Min(1),
                Constraint::Length(app.footer_height()),
            ]
            .as_ref(),
        )
        .split(f.area());

    let displayed_query = app.displayed_query().to_string();
    if !app.ready {
        let query_prefix = app.query_prompt_prefix();
        let query = Paragraph::new(format!("{query_prefix}{displayed_query}"))
            .style(Style::default().fg(Color::White));
        f.render_widget(query, Rect::new(root[0].x, root[0].y, root[0].width, 1));
        let cursor_x = root[0].x + query_prefix.chars().count() as u16 + app.displayed_cursor_pos() as u16;
        let cursor_y = root[0].y;
        f.set_cursor_position((cursor_x, cursor_y));

        let status_height: u16 = if app.show_telemetry { 6 } else { 3 };
        let main = Layout::default()
            .direction(Direction::Vertical)
            .constraints(if app.show_telemetry {
                vec![Constraint::Length(status_height), Constraint::Min(1)]
            } else {
                vec![Constraint::Length(3), Constraint::Min(1)]
            })
            .split(Rect::new(
                root[0].x,
                root[0].y + 1,
                root[0].width,
                root[0].height.saturating_sub(1),
            ));

        let pct = if app.indexing.total_files == 0 {
            0u16
        } else {
            ((app.indexing.processed_files.saturating_mul(100)) / app.indexing.total_files) as u16
        };

        if app.show_telemetry {
            // Compact status area: gauge + status text in a fixed block
            let status_split = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(3), Constraint::Min(1)].as_ref())
                .split(main[0]);

            let gauge = Gauge::default()
                .block(Block::default().borders(Borders::ALL).title("Indexing"))
                .gauge_style(Style::default().fg(Color::Cyan))
                .percent(pct);
            f.render_widget(gauge, status_split[0]);

            let mut status_lines = vec![Line::raw(format!(
                "files: {}/{}   records: {}   sessions: {}",
                app.indexing.processed_files,
                app.indexing.total_files,
                app.indexing.records,
                app.indexing.sessions
            ))];
            if let Some(cur) = app.indexing.current.as_ref() {
                status_lines.push(Line::raw(format!("current: {}", cur.display())));
            }
            if let Some(w) = app.indexing.last_warn.as_deref() {
                status_lines.push(Line::raw(format!("warn: {w}")));
            }
            let status_p = Paragraph::new(Text::from(status_lines)).wrap(Wrap { trim: false });
            f.render_widget(status_p, status_split[1]);

            // Events view fills remaining space
            let telemetry_area = main[1];
            let telemetry_inner_height = telemetry_area.height.saturating_sub(2) as usize;
            let telemetry_inner_width = telemetry_area.width.saturating_sub(2) as usize;
            let telemetry_doc = app.build_telemetry_preview_doc();
            if app.preview_scroll_reset_pending {
                let first_match_visual_line = preview_visual_line_offset(
                    &telemetry_doc.lines,
                    telemetry_doc.first_match_line,
                    telemetry_inner_width,
                );
                app.preview_scroll = first_match_visual_line.saturating_sub(2);
                app.preview_scroll_reset_pending = false;
            }
            let telemetry_total_lines =
                preview_visual_line_count(&telemetry_doc.lines, telemetry_inner_width);
            let telemetry_max_scroll = telemetry_total_lines.saturating_sub(telemetry_inner_height);
            app.preview_scroll = cmp::min(app.preview_scroll, telemetry_max_scroll);
            let telemetry_style = events_preview_style();
            let telemetry = Paragraph::new(Text::from(telemetry_doc.lines))
                .style(telemetry_style)
                .block(
                    Block::default()
                        .style(telemetry_style)
                        .borders(Borders::ALL)
                        .title(app.events_title()),
                )
                .scroll((app.preview_scroll as u16, 0))
                .wrap(Wrap { trim: false });
            f.render_widget(telemetry, telemetry_area);
        } else {
            let gauge = Gauge::default()
                .block(Block::default().borders(Borders::ALL).title("Indexing"))
                .gauge_style(Style::default().fg(Color::Cyan))
                .percent(pct);
            f.render_widget(gauge, main[0]);

            let mut lines = vec![Line::raw(format!(
                "files: {}/{}   records: {}   sessions: {}",
                app.indexing.processed_files,
                app.indexing.total_files,
                app.indexing.records,
                app.indexing.sessions
            ))];
            if let Some(cur) = app.indexing.current.as_ref() {
                lines.push(Line::raw(format!("current: {}", cur.display())));
            }
            if let Some(w) = app.indexing.last_warn.as_deref() {
                lines.push(Line::raw(format!("warn: {w}")));
            }
            let p = Paragraph::new(Text::from(lines))
                .block(Block::default().borders(Borders::ALL).title("Status"))
                .wrap(Wrap { trim: false });
            f.render_widget(p, main[1]);
        }

        render_footer(
            f,
            root[1],
            app.status_text(),
            if app.show_telemetry {
                format!(
                    "Esc/Ctrl+c: quit  Ctrl+t: events  Ctrl+g: perf  PgUp/PgDn: scroll page  wheel: scroll  Ctrl+u: clear query+tag filters  query: \"{}\"",
                    app.query.trim()
                )
            } else {
                "Esc/Ctrl+c: quit  Ctrl+t: events".to_string()
            },
        );
        return;
    }

    if app.show_telemetry {
        let query_prefix = app.query_prompt_prefix();
        let query = Paragraph::new(format!("{query_prefix}{displayed_query}"))
            .style(Style::default().fg(Color::White));
        f.render_widget(query, Rect::new(root[0].x, root[0].y, root[0].width, 1));
        let cursor_x = root[0].x + query_prefix.chars().count() as u16 + app.displayed_cursor_pos() as u16;
        let cursor_y = root[0].y;
        f.set_cursor_position((cursor_x, cursor_y));

        let telemetry_area = Rect::new(
            root[0].x,
            root[0].y + 1,
            root[0].width,
            root[0].height.saturating_sub(1),
        );
        let telemetry_inner_height = telemetry_area.height.saturating_sub(2) as usize;
        let telemetry_inner_width = telemetry_area.width.saturating_sub(2) as usize;
        let telemetry_doc = app.build_preview_doc();
        if app.preview_scroll_reset_pending {
            let first_match_visual_line = preview_visual_line_offset(
                &telemetry_doc.lines,
                telemetry_doc.first_match_line,
                telemetry_inner_width,
            );
            app.preview_scroll = first_match_visual_line.saturating_sub(2);
            app.preview_scroll_reset_pending = false;
        }
        let telemetry_total_lines =
            preview_visual_line_count(&telemetry_doc.lines, telemetry_inner_width);
        let telemetry_max_scroll = telemetry_total_lines.saturating_sub(telemetry_inner_height);
        app.preview_scroll = cmp::min(app.preview_scroll, telemetry_max_scroll);
        let telemetry_style = events_preview_style();
        let telemetry = Paragraph::new(Text::from(telemetry_doc.lines))
            .style(telemetry_style)
            .block(
                Block::default()
                    .style(telemetry_style)
                    .borders(Borders::ALL)
                    .title(app.events_title()),
            )
            .scroll((app.preview_scroll as u16, 0))
            .wrap(Wrap { trim: false });
        f.render_widget(telemetry, telemetry_area);

        render_footer(
            f,
            root[1],
            app.status_text(),
            format!(
                "Esc/Ctrl+c: quit  Ctrl+t: events  Ctrl+g: perf  PgUp/PgDn: scroll page  wheel: scroll  Ctrl+u: clear events filter  filter: \"{}\"",
                app.displayed_query().trim()
            ),
        );
        return;
    }

    let geometry = app_geometry(f.area(), app);
    let query_prefix = app.query_prompt_prefix();
    let query = Paragraph::new(format!("{query_prefix}{displayed_query}"))
        .style(Style::default().fg(Color::White));
    f.render_widget(query, geometry.query);
    let cursor_x = geometry.query.x + query_prefix.chars().count() as u16 + app.displayed_cursor_pos() as u16;
    let cursor_y = geometry.query.y;
    f.set_cursor_position((cursor_x, cursor_y));

    // Results pane (manual windowing)
    let results_area = geometry.results;
    let inner_height = results_area.height.saturating_sub(2) as usize; // borders
    let total = app.filtered.len();

    if app.selected >= total && total > 0 {
        app.selected = total - 1;
    }

    if total == 0 {
        app.selected = 0;
        app.offset = 0;
    }

    if total > 0 && inner_height > 0 {
        if app.selected < app.offset {
            app.offset = app.selected;
        } else if app.selected >= app.offset + inner_height {
            app.offset = app.selected + 1 - inner_height;
        }
    }

    let visible_start = cmp::min(app.offset, total);
    let visible_end = cmp::min(visible_start + inner_height, total);
    let query = app.query.trim().to_string();

    let mut lines: Vec<Line> = Vec::new();
    for hit in app.filtered[visible_start..visible_end].iter() {
        let Some(sess) = app.sessions.get(hit.session_idx) else {
            continue;
        };
        let matched = hit.matched_record_idx.and_then(|idx| app.all.get(idx));
        lines.push(result_line(
            sess,
            matched,
            hit.hit_count,
            &query,
            &app.ui_tags,
            Style::default(),
        ));
    }

    let results = Paragraph::new(Text::from(lines)).block(
        Block::default().borders(Borders::ALL).title(format!(
            "Results (sessions {}/{} turns {}/{})",
            app.filtered.len(),
            app.sessions.len(),
            app.matched_turn_count(),
            app.total_turn_count()
        )),
    );
    f.render_widget(results, results_area);

    // Preview
    let preview_area = geometry.turn_preview;
    let preview_inner_height = preview_area.height.saturating_sub(2) as usize;
    let preview_inner_width = preview_area.width.saturating_sub(2) as usize;
    let preview_style = app
        .preview_bgcolor
        .map(|color| Style::default().bg(color))
        .unwrap_or_default();
    let preview_border_style = if app.preview_remote_style {
        Style::default()
            .fg(REMOTE_PREVIEW_BORDER_FG)
            .bg(REMOTE_ACCENT)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default()
    };
    if let Some(git_graph_area) = geometry.git_graph {
        let graph_inner_height = git_graph_area.height.saturating_sub(2) as usize;
        let graph_inner_width = git_graph_area.width.saturating_sub(2) as usize;
        let graph_doc = app.build_git_graph_doc(graph_inner_width, preview_inner_height);
        let graph_total_lines = preview_visual_line_count(&graph_doc.lines, graph_inner_width);
        let graph_max_scroll = graph_total_lines.saturating_sub(graph_inner_height);
        app.git_graph_scroll = cmp::min(app.git_graph_scroll, graph_max_scroll);
        let border_style = if matches!(app.active_pane, ActivePane::GitGraph) {
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default()
        };
        let graph = Paragraph::new(Text::from(graph_doc.lines))
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(border_style)
                    .title(app.git_graph_title()),
            )
            .scroll((app.git_graph_scroll as u16, 0))
            .wrap(Wrap { trim: false });
        f.render_widget(graph, git_graph_area);
    }

    if let Some(git_commit_area) = geometry.git_commit {
        let commit_inner_height = git_commit_area.height.saturating_sub(2) as usize;
        let commit_inner_width = git_commit_area.width.saturating_sub(2) as usize;
        let commit_doc = app.build_git_commit_doc(commit_inner_width, preview_inner_height);
        let commit_total_lines = preview_visual_line_count(&commit_doc.lines, commit_inner_width);
        let commit_max_scroll = commit_total_lines.saturating_sub(commit_inner_height);
        app.git_commit_scroll = cmp::min(app.git_commit_scroll, commit_max_scroll);
        let border_style = if matches!(app.active_pane, ActivePane::GitCommit) {
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default()
        };
        let commit = Paragraph::new(Text::from(commit_doc.lines))
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(border_style)
                    .title(app.git_commit_title()),
            )
            .scroll((app.git_commit_scroll as u16, 0))
            .wrap(Wrap { trim: false });
        f.render_widget(commit, git_commit_area);
    }

    let turn_anchor_record_idx = if app.git_graph_visible {
        app.selected_anchor_record_idx(preview_inner_width, preview_inner_height)
    } else {
        None
    };

    let preview_started = Instant::now();
    let preview_doc = if app.showing_session_browser() {
        app.session_browser_doc()
    } else {
        app.build_preview_doc()
    };
    let preview_duration_ms = preview_started.elapsed().as_millis();
    let preview_mode = if app.showing_session_browser() {
        "session_browser_single"
    } else if query.is_empty() {
        "empty_query"
    } else {
        "query_preview"
    };
    app.emit_preview_perf_summary(preview_mode, &preview_doc, preview_duration_ms);
    if app.preview_scroll_reset_pending {
        let first_match_visual_line = preview_visual_line_offset(
            &preview_doc.lines,
            preview_doc.first_match_line,
            preview_inner_width,
        );
        app.preview_scroll = first_match_visual_line.saturating_sub(2);
        app.preview_scroll_reset_pending = false;
    }
    let layout_started = Instant::now();
    let preview_total_lines =
        preview_visual_line_count(&preview_doc.lines, preview_inner_width);
    let preview_max_scroll = preview_total_lines.saturating_sub(preview_inner_height);
    app.preview_scroll = cmp::min(app.preview_scroll, preview_max_scroll);
    if app.log_group_enabled(LogGroup::Perf) {
        app.emit_group_event(
            LogGroup::Perf,
            "perf_preview_phase",
            json!({
                "mode": preview_mode,
                "phase": "visual_layout",
                "duration_ms": layout_started.elapsed().as_millis(),
                "records": 0,
                "lines": preview_total_lines,
                "bytes": 0,
            }),
        );
    }
    let selected_preview_record_idx = if app.showing_session_browser() || !query.is_empty() {
        app.current_preview_selection(&preview_doc, preview_inner_width)
    } else {
        None
    };
    let preview_title = if !app.showing_session_browser() && !query.is_empty() {
        app.query_preview_title("Preview", selected_preview_record_idx)
    } else if app.showing_session_browser() {
        app.turns_preview_title(selected_preview_record_idx)
    } else {
        app.preview_title("Preview")
    };
    let preview = Paragraph::new(Text::from(with_selected_preview_line(
        &preview_doc,
        selected_preview_record_idx,
        turn_anchor_record_idx,
    )))
    .style(preview_style)
    .block(
        Block::default()
            .style(preview_style)
            .border_style(preview_border_style)
            .borders(Borders::ALL)
            .title(preview_title),
    )
    .scroll((app.preview_scroll as u16, 0))
    .wrap(Wrap { trim: false });
    f.render_widget(preview, preview_area);

    render_footer(
        f,
        root[1],
        app.status_text(),
        format!(
            "Esc/Ctrl+c: quit  Enter: resume  Ctrl+o: pager  Ctrl+t: events  Ctrl+v: git graph  Ctrl+d: commit  Ctrl+l: layout  ↑/↓: move  PgUp/PgDn: pane page  Ctrl+b/f: prev/next turn  Ctrl+n/p: next/prev match  Alt+Shift+arrows: resize  wheel: pane scroll  Backspace: delete  Ctrl+u: clear query+tag filters  query: \"{}\"",
            app.displayed_query().trim()
        ),
    );

    // Highlight the selected row by overdrawing it in the results pane.
    if total > 0 && inner_height > 0 && app.selected >= visible_start && app.selected < visible_end
    {
        let y = (app.selected - visible_start) as u16;
        let x = results_area.x + 1;
        let w = results_area.width.saturating_sub(2);
        let highlight_area = ratatui::layout::Rect {
            x,
            y: results_area.y + 1 + y,
            width: w,
            height: 1,
        };
        let hit = app.filtered[app.selected];
        let Some(sess) = app.sessions.get(hit.session_idx) else {
            return;
        };
        let matched = hit.matched_record_idx.and_then(|idx| app.all.get(idx));
        let selected_base_style = Style::default()
            .bg(Color::DarkGray)
            .add_modifier(Modifier::BOLD)
            .add_modifier(Modifier::UNDERLINED);
        let mut selected_line = result_line(
            sess,
            matched,
            hit.hit_count,
            &query,
            &app.ui_tags,
            selected_base_style,
        );
        let mut adjusted_spans = Vec::with_capacity(selected_line.spans.len());
        for span in selected_line.spans.drain(..) {
            if span.style.bg.is_some() {
                let content = span.content.into_owned();
                if content.starts_with(' ') && content.ends_with(' ') && content.len() >= 2 {
                    adjusted_spans.push(Span::styled(
                        " ".to_string(),
                        span.style.add_modifier(Modifier::BOLD),
                    ));
                    adjusted_spans.push(Span::styled(
                        content[1..content.len() - 1].to_string(),
                        span.style
                            .add_modifier(Modifier::BOLD)
                            .add_modifier(Modifier::UNDERLINED),
                    ));
                    adjusted_spans.push(Span::styled(
                        " ".to_string(),
                        span.style.add_modifier(Modifier::BOLD),
                    ));
                } else {
                    adjusted_spans.push(Span::styled(
                        content,
                        span.style
                            .add_modifier(Modifier::BOLD)
                            .add_modifier(Modifier::UNDERLINED),
                    ));
                }
            } else {
                adjusted_spans.push(span);
            }
        }
        selected_line.spans = adjusted_spans;
        let p = Paragraph::new(Text::from(vec![selected_line]));
        f.render_widget(p, highlight_area);
    }
}

fn render_footer(f: &mut ratatui::Frame, area: Rect, status_text: String, help_text: String) {
    let keys = Paragraph::new(help_text).style(Style::default().fg(Color::DarkGray));
    f.render_widget(keys, area);
    if !status_text.is_empty() {
        let status = Paragraph::new(Line::from(vec![Span::styled(
            status_text,
            Style::default().fg(Color::Yellow),
        )]));
        f.render_widget(status, area);
    }
}

fn open_in_pager(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    app: &App,
    rec: &MessageRecord,
) -> anyhow::Result<()> {
    let pager = env::var("PAGER").unwrap_or_else(|_| "less -R".to_string());
    let (file_path, cmd) = if rec.source == SourceKind::OpenCodeSession {
        let file_path = write_opencode_session_pager_file(app, rec)?;
        let file = shell_escape(&file_path.to_string_lossy());
        (Some(file_path), format!("nl -ba {file} | {pager}"))
    } else {
        let file = shell_escape(&rec.file.to_string_lossy());
        let start = rec.line.saturating_sub(40).max(1);
        let end = rec.line.saturating_add(200);
        (
            None,
            format!("nl -ba {file} | sed -n '{start},{end}p' | {pager}"),
        )
    };
    let _ = run_with_tui_suspended(terminal, || {
        Command::new("sh")
            .arg("-lc")
            .arg(cmd)
            .status()
            .context("pager起動に失敗しました")
    });
    if let Some(path) = file_path {
        let _ = fs::remove_file(path);
    }
    Ok(())
}

fn write_opencode_session_pager_file(app: &App, rec: &MessageRecord) -> anyhow::Result<PathBuf> {
    let body = build_opencode_session_pager_text(app, rec);
    let path = std::env::temp_dir().join(format!(
        "agent-history-opencode-session-{}-{}.txt",
        sanitize_for_filename(rec.session_id.as_deref().unwrap_or("session")),
        unix_now_nanos()
    ));
    fs::write(&path, body)
        .with_context(|| format!("pager temp write failed: {}", path.display()))?;
    Ok(path)
}

fn build_opencode_session_pager_text(app: &App, rec: &MessageRecord) -> String {
    let Some(hit) = app.selected_hit() else {
        return rec.text.clone();
    };
    let Some(sess) = app.sessions.get(hit.session_idx) else {
        return rec.text.clone();
    };
    let Some(indices) = app.session_records.get(hit.session_idx) else {
        return rec.text.clone();
    };

    let mut out = String::new();
    out.push_str(&format!("session id: {}\n", sess.session_id));
    out.push_str(&format!(
        "source: {}\n",
        source_label(SourceKind::OpenCodeSession)
    ));
    if let Some(account) = sess.account.as_deref() {
        out.push_str(&format!("account: {account}\n"));
    }
    out.push_str(&format!("session opener: {}\n", sess.first_line));
    out.push('\n');

    for (pos, &idx) in indices.iter().enumerate() {
        let Some(item) = app.all.get(idx) else {
            continue;
        };
        let role = match item.role {
            Role::User => "user",
            Role::Assistant => "assistant",
            Role::System => "system",
            Role::Tool => "tool",
            Role::Unknown => "unknown",
        };
        out.push_str(&format!("== message {} ==\n", pos + 1));
        out.push_str(&format!(
            "timestamp: {}\n",
            short_ts(item.timestamp.as_deref())
        ));
        out.push_str(&format!("role: {role}\n"));
        if let Some(phase) = item.phase.as_deref()
            && !phase.trim().is_empty()
        {
            out.push_str(&format!("phase: {phase}\n"));
        }
        if let Some(cwd) = item.cwd.as_deref()
            && !cwd.trim().is_empty()
        {
            out.push_str(&format!("cwd: {cwd}\n"));
        }
        out.push_str(&format!(
            "source file: {}:{}\n",
            item.file.display(),
            item.line
        ));
        if !item.images.is_empty() {
            out.push_str(&format!("images: {}\n", item.images.len()));
            for path in materialize_record_images(item) {
                out.push_str(&format!("image file: {path}\n"));
            }
        }
        out.push('\n');
        out.push_str(&item.text);
        if !item.text.ends_with('\n') {
            out.push('\n');
        }
        out.push('\n');
    }

    out
}

fn unix_now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default()
}

fn shell_escape(s: &str) -> String {
    let mut out = String::from("'");
    for ch in s.chars() {
        if ch == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(ch);
        }
    }
    out.push('\'');
    out
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResumeTarget {
    program: String,
    args: Vec<String>,
    current_dir: Option<PathBuf>,
    remote: Option<config::RemoteConfig>,
}

#[derive(Debug)]
struct ConfiguredResumeCommand {
    command: Command,
    zdotdir_cleanup: Option<PathBuf>,
}

const AGENT_HISTORY_RETURN_HINT_TEXT: &str = "[exit to agent-history]";

fn resume_command_string(target: &ResumeTarget) -> String {
    let mut parts = Vec::with_capacity(target.args.len() + 1);
    parts.push(shell_escape(&target.program));
    parts.extend(target.args.iter().map(|arg| shell_escape(arg)));
    parts.join(" ")
}

fn shell_program() -> String {
    env::var("SHELL")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "/bin/zsh".to_string())
}

fn shell_uses_zsh(shell: &str) -> bool {
    Path::new(shell).file_name().and_then(|name| name.to_str()) == Some("zsh")
}

fn write_agent_history_zsh_bootstrap(dir: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(dir)
        .with_context(|| format!("failed to create zsh bootstrap dir: {}", dir.display()))?;

    fs::write(
        dir.join(".zshenv"),
        "if [[ -r \"$HOME/.zshenv\" ]]; then\n  source \"$HOME/.zshenv\"\nfi\n",
    )
    .with_context(|| format!("failed to write {}", dir.join(".zshenv").display()))?;
    fs::write(
        dir.join(".zprofile"),
        "if [[ -r \"$HOME/.zprofile\" ]]; then\n  source \"$HOME/.zprofile\"\nfi\n",
    )
    .with_context(|| format!("failed to write {}", dir.join(".zprofile").display()))?;
    fs::write(
        dir.join(".zlogin"),
        "if [[ -r \"$HOME/.zlogin\" ]]; then\n  source \"$HOME/.zlogin\"\nfi\n",
    )
    .with_context(|| format!("failed to write {}", dir.join(".zlogin").display()))?;

    let zshrc = r#"if [[ -r "$HOME/.zshrc" ]]; then
  source "$HOME/.zshrc"
fi

if [[ -n "${AGENT_HISTORY_RETURN_HINT_TEXT:-}" ]]; then
  autoload -Uz add-zsh-hook 2>/dev/null || true
  _agent_history_prompt_precmd() {
    emulate -L zsh
    local hint="${AGENT_HISTORY_RETURN_HINT_TEXT:-}"
    [[ -n "$hint" ]] || return 0
    case "${PROMPT:-}" in
      *"$hint"*) ;;
      *) PROMPT="${PROMPT:-}${hint} " ;;
    esac
    case "${PS1:-}" in
      *"$hint"*) ;;
      *) PS1="${PS1:-}${hint} " ;;
    esac
  }
  add-zsh-hook precmd _agent_history_prompt_precmd 2>/dev/null || true
fi

if [[ -n "${AGENT_HISTORY_PREFILL_ONCE:-}" ]]; then
  autoload -Uz add-zle-hook-widget 2>/dev/null || true
  _agent_history_prefill_line() {
    emulate -L zsh
    if [[ -n "${AGENT_HISTORY_PREFILL_ONCE:-}" ]]; then
      BUFFER="${AGENT_HISTORY_PREFILL_ONCE}"
      CURSOR=${#BUFFER}
      unset AGENT_HISTORY_PREFILL_ONCE
    fi
    add-zle-hook-widget -d line-init _agent_history_prefill_line 2>/dev/null || true
  }
  add-zle-hook-widget line-init _agent_history_prefill_line 2>/dev/null || true
fi
"#;

    fs::write(dir.join(".zshrc"), zshrc)
        .with_context(|| format!("failed to write {}", dir.join(".zshrc").display()))?;

    Ok(())
}

fn configured_resume_command_for_shell(
    target: &ResumeTarget,
    shell: &str,
) -> anyhow::Result<ConfiguredResumeCommand> {
    let mut cmd = Command::new(shell);
    cmd.arg("-il");
    if let Some(cwd) = target.current_dir.as_ref() {
        cmd.current_dir(cwd);
    }
    cmd.env("AGENT_HISTORY_RETURN_HINT", "1");
    cmd.env(
        "AGENT_HISTORY_RETURN_HINT_TEXT",
        AGENT_HISTORY_RETURN_HINT_TEXT,
    );
    cmd.env(
        "AGENT_HISTORY_RESUME_COMMAND",
        resume_command_string(target),
    );
    cmd.env("AGENT_HISTORY_PREFILL_ONCE", resume_command_string(target));

    let zdotdir_cleanup = if shell_uses_zsh(shell) {
        let dir = std::env::temp_dir().join(format!("agent-history-zsh-boot-{}", unix_now_nanos()));
        write_agent_history_zsh_bootstrap(&dir)?;
        cmd.env("ZDOTDIR", &dir);
        Some(dir)
    } else {
        cmd.env(
            "PROMPT",
            format!("$PROMPT{} ", AGENT_HISTORY_RETURN_HINT_TEXT),
        );
        cmd.env("PS1", format!("$PS1{} ", AGENT_HISTORY_RETURN_HINT_TEXT));
        None
    };

    Ok(ConfiguredResumeCommand {
        command: cmd,
        zdotdir_cleanup,
    })
}

fn configured_resume_command(target: &ResumeTarget) -> anyhow::Result<ConfiguredResumeCommand> {
    if let Some(remote) = target.remote.as_ref() {
        return configured_remote_resume_command(target, remote);
    }
    configured_resume_command_for_shell(target, &shell_program())
}

fn configured_remote_resume_command(
    target: &ResumeTarget,
    remote: &config::RemoteConfig,
) -> anyhow::Result<ConfiguredResumeCommand> {
    let ssh_target = match remote.user.as_deref() {
        Some(user) => format!("{user}@{}", remote.host),
        None => remote.host.clone(),
    };
    let mut script = format!(
        "export AGENT_HISTORY_RETURN_HINT=1 AGENT_HISTORY_RETURN_HINT_TEXT={} AGENT_HISTORY_RESUME_COMMAND={}; ",
        shell_escape(AGENT_HISTORY_RETURN_HINT_TEXT),
        shell_escape(&resume_command_string(target)),
    );
    if let Some(cwd) = target.current_dir.as_ref() {
        script.push_str(&format!("cd {} && ", shell_escape(&cwd.to_string_lossy())));
    }
    script.push_str("exec \"${SHELL:-/bin/zsh}\" -il");

    let mut cmd = Command::new("ssh");
    cmd.arg("-t")
        .arg(ssh_target)
        .arg("/bin/sh")
        .arg("-lc")
        .arg(script);
    Ok(ConfiguredResumeCommand {
        command: cmd,
        zdotdir_cleanup: None,
    })
}

fn resume_loading_lines(target: &ResumeTarget, session_id: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    out.push(format!("resuming: {} {}", target.program, session_id));
    if let Some(cwd) = target.current_dir.as_ref() {
        out.push(format!("cwd: {}", cwd.display()));
    }
    out.push(format!("resume command: {}", resume_command_string(target)));
    out.push(String::new());
    out
}

fn resume_target_for_record(app: &App, rec: &MessageRecord) -> Option<ResumeTarget> {
    let sid = rec.session_id.as_deref()?;
    let cwd = existing_dir(
        rec.cwd
            .as_deref()
            .or_else(|| find_cwd_for_session_id(app, sid)),
    );

    match rec.source {
        SourceKind::CodexSessionJsonl | SourceKind::CodexHistoryJsonl => {
            let mut args = Vec::new();
            let program = if let Some(account) = rec.account.as_deref() {
                args.push(account.to_string());
                "codex-account".to_string()
            } else {
                "codex".to_string()
            };
            args.push("resume".to_string());
            if let Some(dir) = cwd.as_ref() {
                args.push("-C".to_string());
                args.push(dir.to_string_lossy().to_string());
            }
            args.push(sid.to_string());
            Some(ResumeTarget {
                program,
                args,
                current_dir: cwd,
                remote: remote_for_record(app, rec),
            })
        }
        SourceKind::ClaudeProjectJsonl => {
            let (program, mut args) = if let Some(account) = rec.account.as_deref() {
                (
                    "claude-account".to_string(),
                    vec![account.to_string(), "--resume".to_string()],
                )
            } else {
                ("claude".to_string(), vec!["--resume".to_string()])
            };
            args.push(sid.to_string());
            Some(ResumeTarget {
                program,
                args,
                current_dir: cwd,
                remote: remote_for_record(app, rec),
            })
        }
        SourceKind::OpenCodeSession => Some(ResumeTarget {
            program: "opencode".to_string(),
            args: vec!["--session".to_string(), sid.to_string()],
            current_dir: cwd,
            remote: remote_for_record(app, rec),
        }),
    }
}

fn remote_for_record(app: &App, rec: &MessageRecord) -> Option<config::RemoteConfig> {
    if rec.origin == "local" {
        return None;
    }
    app.remotes.get(&rec.origin).cloned()
}

fn find_cwd_for_session_id<'a>(app: &'a App, session_id: &str) -> Option<&'a str> {
    app.all
        .iter()
        .filter(|r| r.session_id.as_deref() == Some(session_id))
        .filter_map(|r| r.cwd.as_deref())
        .find(|s| !s.trim().is_empty())
}

fn existing_dir(path: Option<&str>) -> Option<PathBuf> {
    let s = path?.trim();
    if s.is_empty() {
        return None;
    }
    let p = PathBuf::from(s);
    match fs::metadata(&p) {
        Ok(m) if m.is_dir() => Some(p),
        _ => None,
    }
}

fn run_with_tui_suspended<R>(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    f: impl FnOnce() -> anyhow::Result<R>,
) -> anyhow::Result<R> {
    disable_raw_mode().ok();
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        cursor::Show,
        DisableMouseCapture
    )
    .ok();
    terminal.show_cursor().ok();

    let res = f();

    enable_raw_mode().ok();
    execute!(
        terminal.backend_mut(),
        EnterAlternateScreen,
        cursor::Hide,
        EnableMouseCapture
    )
    .ok();
    terminal.hide_cursor().ok();
    terminal.clear().ok();

    res
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(prefix: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let dir = std::env::temp_dir().join(format!("{prefix}-{nanos}-{}", std::process::id()));
            fs::create_dir_all(&dir).unwrap();
            Self { path: dir }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn empty_app() -> App {
        App {
            query: String::new(),
            cursor_pos: 0,
            telemetry_query: String::new(),
            telemetry_cursor_pos: 0,
            max_results: 0,
            active_tag_filters: Vec::new(),
            all: Vec::new(),
            sessions: Vec::new(),
            session_records: Vec::new(),
            filtered: vec![],
            selected: 0,
            offset: 0,
            selected_preview_record_idx: None,
            preview_scroll: 0,
            preview_scroll_reset_pending: false,
            session_browser_start_scroll: 0,
            session_browser_end_scroll: usize::MAX,
            session_browser_active_pane: SessionBrowserPane::Start,
            git_graph_scroll: 0,
            git_commit_scroll: 0,
            git_graph_visible: false,
            git_commit_visible: false,
            active_pane: ActivePane::Results,
            active_split: None,
            layout_state: LayoutState::new(),
            dragged_split: None,
            last_query: String::new(),
            last_results: vec![],
            last_tag_filters: Vec::new(),
            indexing: IndexingProgress::default(),
            ready: false,
            telemetry_log_path: None,
            telemetry_sink: None,
            telemetry_events: EventBuffer::new(EVENT_BUFFER_MAX_BYTES),
            cache_path: None,
            show_telemetry: false,
            enabled_log_groups: HashSet::new(),
            log_groups_help_emitted: false,
            ui_status: None,
            preview_bgcolor_target: None,
            preview_bgcolor: None,
            preview_remote_style: false,
            preview_bgcolor_cache: HashMap::new(),
            ui_tags: config::UiTagConfig::default(),
            remotes: HashMap::new(),
            git_repo_cache: HashMap::new(),
            git_commit_preview_cache: HashMap::new(),
        }
    }

    fn test_record(mut rec: MessageRecord) -> MessageRecord {
        rec.machine_id = "local".to_string();
        rec.machine_name = "local".to_string();
        rec.project_slug = rec.cwd.as_deref().map(dir_name_from_cwd);
        rec.origin = "local".to_string();
        rec
    }

    fn ready_app_with_data(
        all: Vec<MessageRecord>,
        sessions: Vec<SessionSummary>,
        session_records: Vec<Vec<usize>>,
    ) -> App {
        let mut app = empty_app();
        app.ready = true;
        app.all = all;
        app.sessions = sessions;
        app.session_records = session_records;
        app
    }

    fn ready_app_with_indexed_data(all: Vec<MessageRecord>) -> App {
        let (sessions, session_records) = build_session_index(&all);
        ready_app_with_data(all, sessions, session_records)
    }

    #[test]
    fn resume_target_for_codex_uses_codex_resume_with_cd_when_cwd_exists() {
        let tmp = TempDir::new("agent-history");
        let cwd = tmp.path.join("proj");
        fs::create_dir_all(&cwd).unwrap();

        let rec = test_record(MessageRecord {
            timestamp: None,
            role: Role::User,
            text: "x".to_string(),
            file: PathBuf::from("/tmp/x.jsonl"),
            line: 1,
            session_id: Some("019c5a97-1de5-7371-80ef-72ae0f764f43".to_string()),
            account: None,
            cwd: Some(cwd.to_string_lossy().to_string()),
            phase: None,
            images: Vec::new(),
            machine_id: String::new(),
            machine_name: String::new(),
            project_slug: None,
            origin: String::new(),
            source: SourceKind::CodexSessionJsonl,
        });

        let app = ready_app_with_data(vec![rec.clone()], Vec::new(), Vec::new());

        let target = resume_target_for_record(&app, &rec).unwrap();
        assert_eq!(target.program, "codex");
        assert_eq!(
            target.args,
            vec![
                "resume".to_string(),
                "-C".to_string(),
                cwd.to_string_lossy().to_string(),
                "019c5a97-1de5-7371-80ef-72ae0f764f43".to_string(),
            ]
        );
        assert_eq!(target.current_dir.as_deref(), Some(cwd.as_path()));
    }

    #[test]
    fn handle_indexer_event_loaded_then_done_replaces_results() {
        let cached = test_record(MessageRecord {
            timestamp: Some("2026-01-01T00:00:00.000Z".to_string()),
            role: Role::User,
            text: "cached text".to_string(),
            file: PathBuf::from("/tmp/cached.jsonl"),
            line: 1,
            session_id: Some("cached-session".to_string()),
            account: None,
            cwd: Some("/tmp/cached".to_string()),
            phase: None,
            images: Vec::new(),
            machine_id: String::new(),
            machine_name: String::new(),
            project_slug: None,
            origin: String::new(),
            source: SourceKind::CodexSessionJsonl,
        });
        let refreshed = test_record(MessageRecord {
            timestamp: Some("2026-01-02T00:00:00.000Z".to_string()),
            role: Role::User,
            text: "refreshed text".to_string(),
            file: PathBuf::from("/tmp/refreshed.jsonl"),
            line: 1,
            session_id: Some("refreshed-session".to_string()),
            account: None,
            cwd: Some("/tmp/refreshed".to_string()),
            phase: None,
            images: Vec::new(),
            machine_id: String::new(),
            machine_name: String::new(),
            project_slug: None,
            origin: String::new(),
            source: SourceKind::CodexSessionJsonl,
        });

        let mut app = empty_app();
        handle_indexer_event(
            &mut app,
            IndexerEvent::Loaded {
                records: vec![cached.clone()],
            },
        );
        assert!(app.ready);
        assert_eq!(app.all.len(), 1);
        assert_eq!(app.all[0].text, "cached text");
        assert_eq!(app.sessions.len(), 1);
        assert_eq!(app.indexing.records, 1);

        handle_indexer_event(
            &mut app,
            IndexerEvent::Done {
                records: vec![refreshed.clone()],
            },
        );
        assert_eq!(app.all.len(), 1);
        assert_eq!(app.all[0].text, "refreshed text");
        assert_eq!(app.sessions.len(), 1);
        assert_eq!(app.sessions[0].session_id, "refreshed-session");
        assert_eq!(app.indexing.records, 1);
    }

    #[test]
    fn resume_target_for_claude_uses_claude_resume() {
        let tmp = TempDir::new("agent-history");
        let cwd = tmp.path.join("proj");
        fs::create_dir_all(&cwd).unwrap();

        let rec = test_record(MessageRecord {
            timestamp: None,
            role: Role::User,
            text: "x".to_string(),
            file: PathBuf::from("/tmp/x.jsonl"),
            line: 1,
            session_id: Some("8adefc6b-d73e-4a0b-a330-9be4114a5bdb".to_string()),
            account: None,
            cwd: Some(cwd.to_string_lossy().to_string()),
            phase: None,
            images: Vec::new(),
            machine_id: String::new(),
            machine_name: String::new(),
            project_slug: None,
            origin: String::new(),
            source: SourceKind::ClaudeProjectJsonl,
        });

        let app = ready_app_with_data(vec![rec.clone()], Vec::new(), Vec::new());

        let target = resume_target_for_record(&app, &rec).unwrap();
        assert_eq!(target.program, "claude");
        assert_eq!(
            target.args,
            vec![
                "--resume".to_string(),
                "8adefc6b-d73e-4a0b-a330-9be4114a5bdb".to_string(),
            ]
        );
        assert_eq!(target.current_dir.as_deref(), Some(cwd.as_path()));
    }

    #[test]
    fn resume_target_for_account_scoped_codex_uses_wrapper() {
        let rec = test_record(MessageRecord {
            timestamp: None,
            role: Role::User,
            text: "x".to_string(),
            file: PathBuf::from("/tmp/x.jsonl"),
            line: 1,
            session_id: Some("sid".to_string()),
            account: Some("work".to_string()),
            cwd: None,
            phase: None,
            images: Vec::new(),
            machine_id: String::new(),
            machine_name: String::new(),
            project_slug: None,
            origin: String::new(),
            source: SourceKind::CodexSessionJsonl,
        });

        let app = ready_app_with_data(vec![rec.clone()], Vec::new(), Vec::new());

        let target = resume_target_for_record(&app, &rec).unwrap();
        assert_eq!(target.program, "codex-account");
        assert_eq!(target.args, vec!["work", "resume", "sid"]);
    }

    #[test]
    fn resume_target_for_account_scoped_claude_uses_wrapper() {
        let rec = test_record(MessageRecord {
            timestamp: None,
            role: Role::User,
            text: "x".to_string(),
            file: PathBuf::from("/tmp/x.jsonl"),
            line: 1,
            session_id: Some("sid".to_string()),
            account: Some("abc".to_string()),
            cwd: None,
            phase: None,
            images: Vec::new(),
            machine_id: String::new(),
            machine_name: String::new(),
            project_slug: None,
            origin: String::new(),
            source: SourceKind::ClaudeProjectJsonl,
        });

        let app = ready_app_with_data(vec![rec.clone()], Vec::new(), Vec::new());

        let target = resume_target_for_record(&app, &rec).unwrap();
        assert_eq!(target.program, "claude-account");
        assert_eq!(target.args, vec!["abc", "--resume", "sid"]);
    }

    #[test]
    fn resume_target_for_opencode_uses_session_flag() {
        let tmp = TempDir::new("agent-history");
        let cwd = tmp.path.join("proj");
        fs::create_dir_all(&cwd).unwrap();

        let rec = test_record(MessageRecord {
            timestamp: None,
            role: Role::User,
            text: "x".to_string(),
            file: PathBuf::from("/tmp/x.json"),
            line: 1,
            session_id: Some("ses_demo".to_string()),
            account: None,
            cwd: Some(cwd.to_string_lossy().to_string()),
            phase: Some("orchestrator".to_string()),
            images: Vec::new(),
            machine_id: String::new(),
            machine_name: String::new(),
            project_slug: None,
            origin: String::new(),
            source: SourceKind::OpenCodeSession,
        });

        let app = ready_app_with_data(vec![rec.clone()], Vec::new(), Vec::new());

        let target = resume_target_for_record(&app, &rec).unwrap();
        assert_eq!(target.program, "opencode");
        assert_eq!(
            target.args,
            vec!["--session".to_string(), "ses_demo".to_string()]
        );
        assert_eq!(target.current_dir.as_deref(), Some(cwd.as_path()));
    }

    #[test]
    fn resume_loading_lines_includes_command_and_optional_cwd() {
        let target = ResumeTarget {
            program: "codex".to_string(),
            args: vec![
                "resume".to_string(),
                "-C".to_string(),
                "/x".to_string(),
                "sid".to_string(),
            ],
            current_dir: Some(PathBuf::from("/x")),
            remote: None,
        };

        let lines = resume_loading_lines(&target, "sid");
        assert_eq!(lines[0], "resuming: codex sid");
        assert_eq!(lines[1], "cwd: /x");
        assert_eq!(lines[2], "resume command: 'codex' 'resume' '-C' '/x' 'sid'");
    }

    #[test]
    fn configured_resume_command_uses_login_shell_with_current_dir() {
        let target = ResumeTarget {
            program: "codex".to_string(),
            args: vec!["resume".to_string(), "sid".to_string()],
            current_dir: Some(PathBuf::from("/tmp/proj dir")),
            remote: None,
        };

        let configured = configured_resume_command_for_shell(&target, "/bin/zsh").unwrap();
        let cmd = &configured.command;
        let program = cmd.get_program().to_string_lossy().to_string();
        let args: Vec<String> = cmd
            .get_args()
            .map(|arg| arg.to_string_lossy().to_string())
            .collect();

        assert!(!program.is_empty());
        assert_eq!(args, vec!["-il"]);
        assert_eq!(
            cmd.get_current_dir()
                .map(|p| p.to_string_lossy().to_string()),
            Some("/tmp/proj dir".to_string())
        );
        let envs: std::collections::HashMap<String, Option<String>> = cmd
            .get_envs()
            .map(|(k, v)| {
                (
                    k.to_string_lossy().to_string(),
                    v.map(|v| v.to_string_lossy().to_string()),
                )
            })
            .collect();
        assert_eq!(
            envs.get("AGENT_HISTORY_RETURN_HINT")
                .and_then(|v| v.clone()),
            Some("1".to_string())
        );
        assert_eq!(
            envs.get("AGENT_HISTORY_RETURN_HINT_TEXT")
                .and_then(|v| v.clone()),
            Some(AGENT_HISTORY_RETURN_HINT_TEXT.to_string())
        );
        assert_eq!(
            envs.get("AGENT_HISTORY_PREFILL_ONCE")
                .and_then(|v| v.clone()),
            Some("'codex' 'resume' 'sid'".to_string())
        );
        assert_eq!(
            envs.get("AGENT_HISTORY_RESUME_COMMAND")
                .and_then(|v| v.clone()),
            Some("'codex' 'resume' 'sid'".to_string())
        );
        let zdotdir = envs
            .get("ZDOTDIR")
            .and_then(|v| v.clone())
            .expect("zsh launch should set ZDOTDIR");
        let bootstrap = PathBuf::from(&zdotdir);
        assert!(bootstrap.join(".zshenv").is_file());
        assert!(bootstrap.join(".zprofile").is_file());
        assert!(bootstrap.join(".zshrc").is_file());
        assert!(bootstrap.join(".zlogin").is_file());
        let zshrc = fs::read_to_string(bootstrap.join(".zshrc")).unwrap();
        assert!(zshrc.contains("AGENT_HISTORY_RETURN_HINT_TEXT"));
        assert!(zshrc.contains("AGENT_HISTORY_PREFILL_ONCE"));
        assert!(zshrc.contains("add-zle-hook-widget line-init _agent_history_prefill_line"));
        fs::remove_dir_all(bootstrap).unwrap();
    }

    #[test]
    fn configured_resume_command_non_zsh_falls_back_to_prompt_envs() {
        let target = ResumeTarget {
            program: "codex".to_string(),
            args: vec!["resume".to_string(), "sid".to_string()],
            current_dir: None,
            remote: None,
        };

        let configured = configured_resume_command_for_shell(&target, "/bin/bash").unwrap();
        let envs: std::collections::HashMap<String, Option<String>> = configured
            .command
            .get_envs()
            .map(|(k, v)| {
                (
                    k.to_string_lossy().to_string(),
                    v.map(|v| v.to_string_lossy().to_string()),
                )
            })
            .collect();

        assert_eq!(envs.get("ZDOTDIR").and_then(|v| v.clone()), None);
        assert_eq!(
            envs.get("PROMPT").and_then(|v| v.clone()),
            Some("$PROMPT[exit to agent-history] ".to_string())
        );
        assert_eq!(
            envs.get("PS1").and_then(|v| v.clone()),
            Some("$PS1[exit to agent-history] ".to_string())
        );
    }

    fn mr(
        ts: Option<&str>,
        role: Role,
        text: &str,
        session_id: &str,
        source: SourceKind,
    ) -> MessageRecord {
        test_record(MessageRecord {
            timestamp: ts.map(|s| s.to_string()),
            role,
            text: text.to_string(),
            file: PathBuf::from("/tmp/x.jsonl"),
            line: 1,
            session_id: Some(session_id.to_string()),
            account: None,
            cwd: None,
            phase: None,
            images: Vec::new(),
            machine_id: String::new(),
            machine_name: String::new(),
            project_slug: None,
            origin: String::new(),
            source,
        })
    }

    #[test]
    fn build_session_index_uses_first_user_message_and_sorts_by_last_activity() {
        let all = vec![
            mr(
                Some("2026-02-10T00:00:00Z"),
                Role::System,
                "sys",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-02-10T00:00:01Z"),
                Role::User,
                "first hello",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-02-11T00:00:00Z"),
                Role::Assistant,
                "needle",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-02-12T00:00:00Z"),
                Role::User,
                "yo",
                "b",
                SourceKind::ClaudeProjectJsonl,
            ),
            mr(
                Some("2026-02-13T00:00:00Z"),
                Role::Assistant,
                "ok",
                "b",
                SourceKind::ClaudeProjectJsonl,
            ),
        ];

        let (sessions, records) = build_session_index(&all);
        assert_eq!(sessions.len(), 2);
        assert_eq!(records.len(), 2);

        // The most recent session comes first.
        assert_eq!(sessions[0].session_id, "b");
        assert_eq!(sessions[0].source, SourceKind::ClaudeProjectJsonl);
        assert_eq!(all[sessions[0].first_user_idx].role, Role::User);
        assert_eq!(all[sessions[0].first_user_idx].text, "yo");
        assert_eq!(records[0].len(), 2);

        // The second session follows.
        assert_eq!(sessions[1].session_id, "a");
        assert_eq!(sessions[1].source, SourceKind::CodexSessionJsonl);
        assert_eq!(all[sessions[1].first_user_idx].role, Role::User);
        assert_eq!(all[sessions[1].first_user_idx].text, "first hello");
        assert_eq!(records[1].len(), 3);
    }

    #[test]
    fn build_session_index_keeps_same_session_id_separate_across_accounts() {
        let mut a = mr(
            Some("2026-02-10T00:00:01Z"),
            Role::User,
            "account a",
            "shared",
            SourceKind::CodexSessionJsonl,
        );
        a.account = Some("a".to_string());

        let mut b = mr(
            Some("2026-02-11T00:00:01Z"),
            Role::User,
            "account b",
            "shared",
            SourceKind::CodexSessionJsonl,
        );
        b.account = Some("b".to_string());

        let (sessions, _) = build_session_index(&[a, b]);
        assert_eq!(sessions.len(), 2);
        assert!(
            sessions
                .iter()
                .any(|sess| sess.account.as_deref() == Some("a"))
        );
        assert!(
            sessions
                .iter()
                .any(|sess| sess.account.as_deref() == Some("b"))
        );
    }

    #[test]
    fn update_results_matches_any_message_and_selected_record_is_the_first_hit() {
        let all = vec![
            mr(
                Some("2026-02-10T00:00:00Z"),
                Role::System,
                "sys",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-02-10T00:00:01Z"),
                Role::User,
                "first hello",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-02-11T00:00:00Z"),
                Role::Assistant,
                "hay needle stack",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-02-12T00:00:00Z"),
                Role::User,
                "yo",
                "b",
                SourceKind::ClaudeProjectJsonl,
            ),
            mr(
                Some("2026-02-13T00:00:00Z"),
                Role::Assistant,
                "ok",
                "b",
                SourceKind::ClaudeProjectJsonl,
            ),
        ];
        let mut app = ready_app_with_indexed_data(all);
        app.query = "needle".to_string();

        app.update_results();
        assert_eq!(app.filtered.len(), 1);

        let sess = app
            .sessions
            .get(app.selected_hit().unwrap().session_idx)
            .unwrap();
        assert_eq!(sess.session_id, "a");

        let hit = app.selected_hit().unwrap();
        assert_eq!(hit.hit_count, 1);

        let rec = app.selected_record().unwrap();
        assert_eq!(rec.role, Role::Assistant);
        assert_eq!(rec.text, "hay needle stack");
    }

    #[test]
    fn selected_record_uses_session_opener_when_query_is_empty() {
        let all = vec![
            mr(
                Some("2026-02-10T00:00:01Z"),
                Role::User,
                "first hello",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-02-11T00:00:00Z"),
                Role::Assistant,
                "hay needle stack",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
        ];
        let (sessions, session_records) = build_session_index(&all);
        let mut app = App {
            query: String::new(),
            cursor_pos: 0,
            telemetry_query: String::new(),
            telemetry_cursor_pos: 0,
            max_results: 0,
            active_tag_filters: Vec::new(),
            all,
            sessions,
            session_records,
            filtered: vec![],
            selected: 0,
            offset: 0,
            selected_preview_record_idx: None,
            preview_scroll: 0,
            preview_scroll_reset_pending: false,
            session_browser_start_scroll: 0,
            session_browser_end_scroll: usize::MAX,
            session_browser_active_pane: SessionBrowserPane::Start,
            git_graph_scroll: 0,
            git_commit_scroll: 0,
            git_graph_visible: false,
            git_commit_visible: false,
            active_pane: ActivePane::Results,
            active_split: None,
            layout_state: LayoutState::new(),
            dragged_split: None,
            last_query: String::new(),
            last_results: vec![],
            last_tag_filters: Vec::new(),
            indexing: IndexingProgress::default(),
            ready: true,
            telemetry_log_path: None,
            telemetry_sink: None,
            telemetry_events: EventBuffer::new(EVENT_BUFFER_MAX_BYTES),
            cache_path: None,
            show_telemetry: false,
            enabled_log_groups: HashSet::new(),
            log_groups_help_emitted: false,
            ui_status: None,
            preview_bgcolor_target: None,
            preview_bgcolor: None,
            preview_remote_style: false,
            preview_bgcolor_cache: HashMap::new(),
            ui_tags: config::UiTagConfig::default(),
            remotes: HashMap::new(),
            git_repo_cache: HashMap::new(),
            git_commit_preview_cache: HashMap::new(),
        };

        app.update_results();
        let rec = app.selected_record().unwrap();
        assert_eq!(rec.role, Role::User);
        assert_eq!(rec.text, "first hello");
    }

    #[test]
    fn update_results_sets_preview_scroll_near_first_matching_line() {
        let all = vec![mr(
            Some("2026-02-10T00:00:01Z"),
            Role::User,
            "line 1\nline 2\nneedle here\nline 4",
            "a",
            SourceKind::CodexSessionJsonl,
        )];
        let mut app = ready_app_with_indexed_data(all);
        app.query = "needle".to_string();

        app.update_results();
        let doc = app.build_preview_doc();
        assert_eq!(doc.first_match_line, 12);
        assert_eq!(app.preview_scroll, 0);
        assert!(app.preview_scroll_reset_pending);
    }

    #[test]
    fn preview_doc_includes_multiple_matching_records() {
        let all = vec![
            mr(
                Some("2026-02-10T00:00:01Z"),
                Role::User,
                "first needle needle",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-02-10T00:00:02Z"),
                Role::Assistant,
                "second needle",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
        ];
        let (sessions, session_records) = build_session_index(&all);
        let mut app = App {
            query: "needle".to_string(),
            cursor_pos: 6,
            telemetry_query: String::new(),
            telemetry_cursor_pos: 0,
            max_results: 0,
            active_tag_filters: Vec::new(),
            all,
            sessions,
            session_records,
            filtered: vec![],
            selected: 0,
            offset: 0,
            selected_preview_record_idx: None,
            preview_scroll: 0,
            preview_scroll_reset_pending: false,
            session_browser_start_scroll: 0,
            session_browser_end_scroll: usize::MAX,
            session_browser_active_pane: SessionBrowserPane::Start,
            git_graph_scroll: 0,
            git_commit_scroll: 0,
            git_graph_visible: false,
            git_commit_visible: false,
            active_pane: ActivePane::Results,
            active_split: None,
            layout_state: LayoutState::new(),
            dragged_split: None,
            last_query: String::new(),
            last_results: vec![],
            last_tag_filters: Vec::new(),
            indexing: IndexingProgress::default(),
            ready: true,
            telemetry_log_path: None,
            telemetry_sink: None,
            telemetry_events: EventBuffer::new(EVENT_BUFFER_MAX_BYTES),
            cache_path: None,
            show_telemetry: false,
            enabled_log_groups: HashSet::new(),
            log_groups_help_emitted: false,
            ui_status: None,
            preview_bgcolor_target: None,
            preview_bgcolor: None,
            preview_remote_style: false,
            preview_bgcolor_cache: HashMap::new(),
            ui_tags: config::UiTagConfig::default(),
            remotes: HashMap::new(),
            git_repo_cache: HashMap::new(),
            git_commit_preview_cache: HashMap::new(),
        };

        app.update_results();
        let doc = app.build_preview_doc();
        let rendered = doc
            .lines
            .iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(rendered.contains("showing 2 of 2 matching messages"));
        assert!(rendered.contains("total query occurrences in shown message text: 3"));
        assert!(rendered.contains("-- hit 1/2 --"));
        assert!(rendered.contains("-- hit 2/2 --"));
        assert!(rendered.contains("query occurrences in this message: 2"));
        assert!(rendered.contains("query occurrences in this message: 1"));
        assert!(rendered.contains("first needle"));
        assert!(rendered.contains("second needle"));
    }

    #[test]
    fn telemetry_preview_doc_renders_latest_run_summary() {
        let tmp = TempDir::new("agent-history-telemetry-preview");
        let log = tmp.path.join("events.jsonl");
        fs::write(
            &log,
            concat!(
                "{\"ts_ms\":1,\"kind\":\"indexer_started\",\"data\":{\"total_files\":10,\"cache_enabled\":true,\"rebuild_index\":true}}\n",
                "{\"ts_ms\":2,\"kind\":\"cache_open_finished\",\"data\":{\"duration_ms\":7,\"cache_bytes\":4096}}\n",
                "{\"ts_ms\":3,\"kind\":\"cache_load_finished\",\"data\":{\"duration_ms\":11,\"records\":12,\"sessions\":4}}\n",
                "{\"ts_ms\":4,\"kind\":\"refresh_finished\",\"data\":{\"duration_ms\":77,\"skipped_units\":8,\"refreshed_units\":2,\"failed_units\":1}}\n",
                "{\"ts_ms\":5,\"kind\":\"unit_reindexed\",\"data\":{\"source\":\"opencode\",\"path\":\"/tmp/ses.json\",\"duration_ms\":55,\"records\":1,\"message_files\":20,\"part_files\":300,\"text_parts\":200,\"part_parse_failures\":3}}\n",
                "{\"ts_ms\":6,\"kind\":\"cache_reload_finished\",\"data\":{\"duration_ms\":9,\"records\":13,\"sessions\":4,\"cache_bytes\":8192}}\n",
                "{\"ts_ms\":7,\"kind\":\"indexer_finished\",\"data\":{\"duration_ms\":120,\"records\":13,\"sessions\":4}}\n"
            ),
        )
        .unwrap();

        let mut app = empty_app();
        app.ready = true;
        app.telemetry_log_path = Some(log);
        app.telemetry_events.seed(telemetry::read_recent_records(
            app.telemetry_log_path.as_ref().unwrap(),
            EVENT_BUFFER_MAX_BYTES,
        ));
        app.show_telemetry = true;

        let doc = app.build_preview_doc();
        let rendered = doc
            .lines
            .iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n");

        assert!(rendered.contains("cache open: 7 ms"));
        assert!(rendered.contains("cache load: 11 ms"));
        assert!(rendered.contains("refresh: 77 ms"));
        assert!(rendered.contains("buffered events: 7"));
        assert!(rendered.contains("events:"));
        assert!(rendered.contains("unit_reindexed: ms=55"));
        assert!(rendered.contains("parts=300"));
    }

    #[test]
    fn telemetry_preview_doc_search_filters_events() {
        let tmp = TempDir::new("agent-history-telemetry-search");
        let log = tmp.path.join("events.jsonl");
        fs::write(
            &log,
            concat!(
                "{\"ts_ms\":1,\"kind\":\"log_path_info\",\"data\":{\"path\":\"/tmp/events.jsonl\"}}\n",
                "{\"ts_ms\":2,\"kind\":\"cache_open_finished\",\"data\":{\"duration_ms\":7,\"cache_bytes\":4096}}\n",
                "{\"ts_ms\":3,\"kind\":\"refresh_finished\",\"data\":{\"duration_ms\":77,\"skipped_units\":8,\"refreshed_units\":2,\"failed_units\":1}}\n"
            ),
        )
        .unwrap();

        let mut app = empty_app();
        app.ready = true;
        app.telemetry_log_path = Some(log.clone());
        app.telemetry_events
            .seed(telemetry::read_recent_records(&log, EVENT_BUFFER_MAX_BYTES));
        app.show_telemetry = true;
        app.telemetry_query = "refresh".to_string();

        let doc = app.build_preview_doc();
        let rendered = doc
            .lines
            .iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n");

        assert!(rendered.contains("search: refresh"));
        assert!(rendered.contains("refresh_finished: ms=77"));
        assert!(!rendered.contains("cache open: 7 ms"));
    }

    #[test]
    fn telemetry_preview_doc_renders_log_path_info_event() {
        let tmp = TempDir::new("agent-history-telemetry-log-path");
        let log = tmp.path.join("events.jsonl");
        fs::write(
            &log,
            format!(
                "{{\"ts_ms\":1,\"kind\":\"log_path_info\",\"data\":{{\"path\":\"{}\"}}}}\n",
                log.display()
            ),
        )
        .unwrap();

        let mut app = empty_app();
        app.ready = true;
        app.telemetry_log_path = Some(log.clone());
        app.telemetry_events
            .seed(telemetry::read_recent_records(&log, EVENT_BUFFER_MAX_BYTES));
        app.show_telemetry = true;

        let doc = app.build_preview_doc();
        let rendered = doc
            .lines
            .iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n");

        assert!(rendered.contains("events log path="));
        assert!(rendered.contains(&log.display().to_string()));
    }

    #[test]
    fn telemetry_event_line_formats_ui_session_started_banner() {
        let record = telemetry::EventRecord::new(
            None,
            "ui_session_started",
            json!({
                "pid": 4242,
                "cwd": "/Users/slu/agent-history",
            }),
        );

        let rendered = format_telemetry_event_line(&record);
        assert!(rendered.contains("agent-history ui started"));
        assert!(rendered.contains("pid=4242"));
        assert!(rendered.contains("cwd=/Users/slu/agent-history"));
        assert!(rendered.contains("================"));
    }

    #[test]
    fn build_opencode_session_pager_text_includes_whole_session() {
        let all = vec![
            test_record(MessageRecord {
                timestamp: Some("2026-02-10T00:00:01Z".to_string()),
                role: Role::User,
                text: "session opener".to_string(),
                file: PathBuf::from("/tmp/opencode/part/msg1/prt_1.json"),
                line: 1,
                session_id: Some("ses_demo".to_string()),
                account: None,
                cwd: Some("/tmp/project".to_string()),
                phase: Some("title".to_string()),
                images: Vec::new(),
                machine_id: String::new(),
                machine_name: String::new(),
                project_slug: None,
                origin: String::new(),
                source: SourceKind::OpenCodeSession,
            }),
            test_record(MessageRecord {
                timestamp: Some("2026-02-10T00:00:02Z".to_string()),
                role: Role::Assistant,
                text: "assistant reply".to_string(),
                file: PathBuf::from("/tmp/opencode/part/msg2/prt_1.json"),
                line: 1,
                session_id: Some("ses_demo".to_string()),
                account: None,
                cwd: Some("/tmp/project".to_string()),
                phase: Some("orchestrator".to_string()),
                images: Vec::new(),
                machine_id: String::new(),
                machine_name: String::new(),
                project_slug: None,
                origin: String::new(),
                source: SourceKind::OpenCodeSession,
            }),
        ];
        let (sessions, session_records) = build_session_index(&all);
        let mut app = empty_app();
        app.ready = true;
        app.all = all;
        app.sessions = sessions;
        app.session_records = session_records;
        app.filtered = vec![SessionHit {
            session_idx: 0,
            matched_record_idx: None,
            hit_count: 0,
        }];

        let rendered = build_opencode_session_pager_text(&app, &app.all[0]);
        assert!(rendered.contains("session id: ses_demo"));
        assert!(rendered.contains("== message 1 =="));
        assert!(rendered.contains("== message 2 =="));
        assert!(rendered.contains("session opener"));
        assert!(rendered.contains("assistant reply"));
        assert!(rendered.contains("/tmp/opencode/part/msg1/prt_1.json:1"));
        assert!(rendered.contains("/tmp/opencode/part/msg2/prt_1.json:1"));
    }

    #[test]
    fn file_url_for_path_percent_encodes_spaces() {
        let url = file_url_for_path(Path::new("/tmp/agent history/demo 1.png"));
        assert_eq!(url, "file:///tmp/agent%20history/demo%201.png");
    }

    #[test]
    fn materialize_record_images_returns_file_urls() {
        let rec = test_record(MessageRecord {
            timestamp: None,
            role: Role::User,
            text: "image".to_string(),
            file: PathBuf::from("/tmp/x.jsonl"),
            line: 7,
            session_id: Some("ses demo".to_string()),
            account: None,
            cwd: None,
            phase: None,
            images: vec![ImageAttachment {
                kind: ImageAttachmentKind::DataUrl {
                    media_type: "image/png".to_string(),
                    data_url: "data:image/png;base64,aGVsbG8=".to_string(),
                },
                label: Some("inline".to_string()),
            }],
            machine_id: String::new(),
            machine_name: String::new(),
            project_slug: None,
            origin: String::new(),
            source: SourceKind::CodexSessionJsonl,
        });

        let rendered = materialize_record_images(&rec);
        assert_eq!(rendered.len(), 1);
        assert!(rendered[0].contains("inline: "));
        assert!(rendered[0].contains("file:///"));
        assert!(rendered[0].contains("agent-history-images"));
    }

    #[test]
    fn toggle_telemetry_view_flips_preview_mode() {
        let mut app = empty_app();
        assert!(!app.show_telemetry);
        app.toggle_telemetry_view();
        assert!(app.show_telemetry);
        app.toggle_telemetry_view();
        assert!(!app.show_telemetry);
    }

    #[test]
    fn preview_title_includes_selected_session_turn_count() {
        let all = vec![
            mr(
                Some("2026-04-13T00:00:01Z"),
                Role::User,
                "first",
                "session-a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-04-13T00:00:02Z"),
                Role::Assistant,
                "second",
                "session-a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-04-13T00:00:03Z"),
                Role::User,
                "third",
                "session-a",
                SourceKind::CodexSessionJsonl,
            ),
        ];
        let mut app = ready_app_with_indexed_data(all);
        app.update_results();

        assert_eq!(app.preview_title("Start"), "Start  3 turns");
        assert_eq!(app.preview_title("Preview"), "Preview  3 turns");
    }

    #[test]
    fn query_preview_title_uses_matched_turn_position() {
        let all = vec![
            mr(
                Some("2026-04-13T00:00:01Z"),
                Role::User,
                "first",
                "session-a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-04-13T00:00:02Z"),
                Role::Assistant,
                "needle second",
                "session-a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-04-13T00:00:03Z"),
                Role::User,
                "third",
                "session-a",
                SourceKind::CodexSessionJsonl,
            ),
        ];
        let mut app = ready_app_with_indexed_data(all);
        app.query = "needle".to_string();
        app.update_results();

        let matched_record_idx = app.selected_record().and_then(|_| {
            app.selected_hit()
                .and_then(|hit| hit.matched_record_idx)
        });

        assert_eq!(
            app.query_preview_title("Preview", matched_record_idx),
            "Preview  turn 2/3"
        );
    }

    #[test]
    fn turns_preview_title_uses_selected_turn_position() {
        let all = vec![
            mr(
                Some("2026-04-13T00:00:01Z"),
                Role::User,
                "first",
                "session-a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-04-13T00:00:02Z"),
                Role::Assistant,
                "second",
                "session-a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-04-13T00:00:03Z"),
                Role::User,
                "third",
                "session-a",
                SourceKind::CodexSessionJsonl,
            ),
        ];
        let mut app = ready_app_with_indexed_data(all);
        app.update_results();

        assert_eq!(
            app.turns_preview_title(Some(1)),
            "Turns  turn 2/3"
        );
    }

    #[test]
    fn preview_selected_record_idx_uses_last_section_start_at_or_before_scroll() {
        let doc = PreviewDoc {
            lines: vec![
                Line::raw("header"),
                Line::raw("turn 1"),
                Line::raw("line 1"),
                Line::raw("turn 2"),
                Line::raw("line 2"),
            ],
            first_match_line: 0,
            match_lines: Vec::new(),
            line_record_indices: vec![None, Some(10), Some(10), Some(20), Some(20)],
        };

        assert_eq!(preview_selected_record_idx(&doc, 80, 0), Some(10));
        assert_eq!(preview_selected_record_idx(&doc, 80, 2), Some(10));
        assert_eq!(preview_selected_record_idx(&doc, 80, 3), Some(20));
    }

    #[test]
    fn parse_git_commit_line_extracts_hash_epoch_and_summary() {
        let commit = parse_git_commit_line("abc123\t1712966400\tship git context panes").unwrap();
        assert_eq!(commit.hash, "abc123");
        assert_eq!(commit.epoch, 1_712_966_400);
        assert_eq!(commit.summary, "ship git context panes");
    }

    #[test]
    fn parse_git_graph_hash_field_extracts_hash_after_graph_prefix() {
        assert_eq!(
            parse_git_graph_hash_field("*   898f62e0e9983baf7aaf9d420c9c71ddfaec0bf3"),
            Some("898f62e0e9983baf7aaf9d420c9c71ddfaec0bf3")
        );
        assert_eq!(parse_git_graph_hash_field("|\\  "), None);
    }

    #[test]
    fn selected_git_commit_idx_chooses_first_commit_at_or_before_anchor() {
        let commits = vec![
            GitCommitEntry {
                hash: "newest".to_string(),
                epoch: 300,
                summary: "newest".to_string(),
            },
            GitCommitEntry {
                hash: "middle".to_string(),
                epoch: 200,
                summary: "middle".to_string(),
            },
            GitCommitEntry {
                hash: "oldest".to_string(),
                epoch: 100,
                summary: "oldest".to_string(),
            },
        ];

        assert_eq!(selected_git_commit_idx(&commits, 350), Some(0));
        assert_eq!(selected_git_commit_idx(&commits, 250), Some(1));
        assert_eq!(selected_git_commit_idx(&commits, 150), Some(2));
        assert_eq!(selected_git_commit_idx(&commits, 50), Some(2));
    }

    #[test]
    fn with_anchor_indicator_marks_first_anchor_line() {
        let doc = PreviewDoc {
            lines: vec![
                Line::raw("header"),
                Line::raw("turn 1"),
                Line::raw("turn 2"),
            ],
            first_match_line: 0,
            match_lines: Vec::new(),
            line_record_indices: vec![None, Some(7), Some(8)],
        };

        let lines = with_anchor_indicator(&doc, Some(8));
        let rendered = lines[2]
            .spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect::<String>();
        assert!(rendered.starts_with("git anchor"));
        assert!(rendered.contains("turn 2"));
    }

    #[test]
    fn cycle_layout_preset_resets_ratios_to_new_default() {
        let mut app = empty_app();
        app.layout_state.results_pct = 33;
        app.layout_state.git_pct = 33;
        app.cycle_layout_preset();

        assert_eq!(app.layout_state.preset, LayoutPreset::TurnsWide);
        assert_eq!(app.layout_state.results_pct, 18);
        assert_eq!(app.layout_state.git_pct, 22);
    }

    #[test]
    fn preview_scroll_line_movement_clamps_at_zero() {
        let all = vec![mr(
            Some("2026-02-10T00:00:01Z"),
            Role::User,
            "hello",
            "a",
            SourceKind::CodexSessionJsonl,
        )];
        let mut app = ready_app_with_indexed_data(all);
        app.preview_scroll = 1;

        app.scroll_preview_lines(-10, 80);
        assert_eq!(app.preview_scroll, 0);
    }

    #[test]
    fn mouse_wheel_over_preview_scrolls_preview() {
        let all = vec![mr(
            Some("2026-02-10T00:00:01Z"),
            Role::User,
            "line 1\nline 2\nline 3",
            "a",
            SourceKind::CodexSessionJsonl,
        )];
        let mut app = ready_app_with_indexed_data(all);
        app.filtered = vec![SessionHit {
            session_idx: 0,
            matched_record_idx: None,
            hit_count: 0,
        }];

        route_mouse(
            &mut app,
            Rect::new(0, 0, 100, 30),
            MouseEvent {
                kind: MouseEventKind::ScrollDown,
                column: 80,
                row: 10,
                modifiers: KeyModifiers::empty(),
            },
        );

        assert_eq!(app.session_browser_active_pane, SessionBrowserPane::Start);
        assert_eq!(app.preview_scroll, 3);
        assert_eq!(app.selected, 0);
    }

    #[test]
    fn mouse_wheel_over_results_moves_selection() {
        let all = vec![
            mr(
                Some("2026-02-10T00:00:01Z"),
                Role::User,
                "first",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-02-10T00:00:02Z"),
                Role::User,
                "second",
                "b",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-02-10T00:00:03Z"),
                Role::User,
                "third",
                "c",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-02-10T00:00:04Z"),
                Role::User,
                "fourth",
                "d",
                SourceKind::CodexSessionJsonl,
            ),
        ];
        let mut app = ready_app_with_indexed_data(all);
        app.filtered = vec![
            SessionHit {
                session_idx: 0,
                matched_record_idx: None,
                hit_count: 0,
            },
            SessionHit {
                session_idx: 1,
                matched_record_idx: None,
                hit_count: 0,
            },
            SessionHit {
                session_idx: 2,
                matched_record_idx: None,
                hit_count: 0,
            },
            SessionHit {
                session_idx: 3,
                matched_record_idx: None,
                hit_count: 0,
            },
        ];

        route_mouse(
            &mut app,
            Rect::new(0, 0, 100, 30),
            MouseEvent {
                kind: MouseEventKind::ScrollDown,
                column: 10,
                row: 10,
                modifiers: KeyModifiers::empty(),
            },
        );

        assert_eq!(app.selected, 1);
    }

    #[test]
    fn mouse_wheel_over_left_side_does_not_move_selection_in_events_view() {
        let all = vec![
            mr(
                Some("2026-02-10T00:00:01Z"),
                Role::User,
                "first",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-02-10T00:00:02Z"),
                Role::User,
                "second",
                "b",
                SourceKind::CodexSessionJsonl,
            ),
        ];
        let mut app = ready_app_with_indexed_data(all);
        app.filtered = vec![
            SessionHit {
                session_idx: 0,
                matched_record_idx: None,
                hit_count: 0,
            },
            SessionHit {
                session_idx: 1,
                matched_record_idx: None,
                hit_count: 0,
            },
        ];
        app.show_telemetry = true;

        route_mouse(
            &mut app,
            Rect::new(0, 0, 100, 30),
            MouseEvent {
                kind: MouseEventKind::ScrollDown,
                column: 10,
                row: 10,
                modifiers: KeyModifiers::empty(),
            },
        );

        assert_eq!(app.selected, 0);
        assert_eq!(app.preview_scroll, 3);
    }

    #[test]
    fn clicking_project_tag_filters_results_without_touching_query() {
        let mut alpha = mr(
            Some("2026-02-10T00:00:01Z"),
            Role::User,
            "first",
            "session-alpha",
            SourceKind::CodexSessionJsonl,
        );
        alpha.cwd = Some("/tmp/alpha".to_string());
        alpha.project_slug = Some("alpha".to_string());

        let mut beta = mr(
            Some("2026-02-10T00:00:02Z"),
            Role::User,
            "second",
            "session-beta",
            SourceKind::CodexSessionJsonl,
        );
        beta.cwd = Some("/tmp/beta".to_string());
        beta.project_slug = Some("beta".to_string());

        let all = vec![alpha, beta];
        let mut app = ready_app_with_indexed_data(all);
        app.update_results();

        let area = Rect::new(0, 0, 100, 30);
        let (results_area, _) = app_panes(area);
        let sess = app.sessions.get(app.filtered[0].session_idx).unwrap();
        let expected_project = sess.project_slug.clone().unwrap();
        let rendered = result_line(sess, None, 0, "", &app.ui_tags, Style::default())
            .spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect::<String>();
        let project_offset = rendered.find(&format!(" {} ", expected_project)).unwrap() as u16 + 1;

        route_mouse(
            &mut app,
            area,
            MouseEvent {
                kind: MouseEventKind::Down(MouseButton::Left),
                column: results_area.x + 1 + project_offset,
                row: results_area.y + 1,
                modifiers: KeyModifiers::empty(),
            },
        );

        assert_eq!(app.query, "");
        assert_eq!(app.active_tag_filters.len(), 1);
        assert_eq!(app.active_tag_filters[0].kind, TagFilterKind::Project);
        assert_eq!(app.active_tag_filters[0].value, expected_project);
        assert_eq!(app.filtered.len(), 1);
        assert_eq!(
            app.sessions[app.filtered[0].session_idx].project_slug,
            Some(expected_project)
        );
    }

    #[test]
    fn result_line_includes_match_snippet_and_hit_count() {
        let sess = SessionSummary {
            source: SourceKind::CodexSessionJsonl,
            session_id: "s1".to_string(),
            account: None,
            first_user_idx: 0,
            last_ts: Some("2026-02-10T00:00:00Z".to_string()),
            cwd: Some("/tmp/proj".to_string()),
            dir: "proj".to_string(),
            first_line: "session opener".to_string(),
            machine_id: "local".to_string(),
            machine_name: "local".to_string(),
            origin: "local".to_string(),
            project_slug: Some("proj".to_string()),
        };
        let rec = test_record(MessageRecord {
            timestamp: None,
            role: Role::Assistant,
            text: "this contains the matching context".to_string(),
            file: PathBuf::from("/tmp/x.jsonl"),
            line: 1,
            session_id: Some("s1".to_string()),
            account: None,
            cwd: None,
            phase: None,
            images: Vec::new(),
            machine_id: String::new(),
            machine_name: String::new(),
            project_slug: None,
            origin: String::new(),
            source: SourceKind::CodexSessionJsonl,
        });

        let line = result_line(
            &sess,
            Some(&rec),
            3,
            "",
            &config::UiTagConfig::default(),
            Style::default(),
        );
        let rendered = line
            .spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect::<String>();
        assert!(rendered.contains("session opener"));
        assert!(rendered.contains("matching context"));
        assert!(rendered.contains("[3 hits]"));
    }

    #[test]
    fn matched_turn_count_sums_hits_across_filtered_sessions() {
        let all = vec![
            mr(
                Some("2026-02-10T00:00:01Z"),
                Role::User,
                "needle one",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-02-10T00:00:02Z"),
                Role::Assistant,
                "needle two",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-02-10T00:00:03Z"),
                Role::User,
                "haystack",
                "b",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-02-10T00:00:04Z"),
                Role::Assistant,
                "needle three",
                "b",
                SourceKind::CodexSessionJsonl,
            ),
        ];
        let mut app = ready_app_with_indexed_data(all);
        app.query = "needle".to_string();

        app.update_results();

        assert_eq!(app.filtered.len(), 2);
        assert_eq!(app.matched_turn_count(), 3);
        assert_eq!(app.total_turn_count(), 4);
    }

    #[test]
    fn jump_preview_match_moves_between_matching_lines() {
        let all = vec![mr(
            Some("2026-02-10T00:00:01Z"),
            Role::User,
            "alpha\nneedle one\nbeta\nneedle two",
            "a",
            SourceKind::CodexSessionJsonl,
        )];
        let mut app = ready_app_with_indexed_data(all);
        app.query = "needle".to_string();

        app.update_results();
        let initial_scroll = app.preview_scroll;

        app.jump_preview_match(1, 80);
        assert!(app.preview_scroll > initial_scroll);
    }

    #[test]
    fn jump_preview_record_moves_between_turns_in_session_browser() {
        let all = vec![
            mr(
                Some("2026-02-10T00:00:01Z"),
                Role::User,
                "first",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-02-10T00:00:02Z"),
                Role::Assistant,
                "second",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-02-10T00:00:03Z"),
                Role::User,
                "third",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
        ];
        let mut app = ready_app_with_indexed_data(all);
        app.update_results();
        assert!(app.showing_session_browser());

        app.jump_preview_record(1, 80);
        assert!(app.preview_scroll > 0);
    }

    #[test]
    fn jump_preview_record_aligns_target_turn_to_top_of_pane() {
        let all = vec![
            mr(
                Some("2026-02-10T00:00:01Z"),
                Role::User,
                "first",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-02-10T00:00:02Z"),
                Role::Assistant,
                "second",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-02-10T00:00:03Z"),
                Role::User,
                "third",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
        ];
        let mut app = ready_app_with_indexed_data(all);
        app.update_results();
        let doc = app.session_browser_doc();
        let starts = preview_section_start_lines(&doc.line_record_indices);
        app.preview_scroll = preview_visual_line_offset(&doc.lines, starts[0], 80);

        app.jump_preview_record(1, 80);

        assert_eq!(
            app.preview_scroll,
            preview_visual_line_offset(&doc.lines, starts[1], 80)
        );
    }

    #[test]
    fn jump_preview_record_wraps_forward_from_last_turn_to_first() {
        let all = vec![
            mr(
                Some("2026-02-10T00:00:01Z"),
                Role::User,
                "first",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-02-10T00:00:02Z"),
                Role::Assistant,
                "second",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-02-10T00:00:03Z"),
                Role::User,
                "third",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
        ];
        let mut app = ready_app_with_indexed_data(all);
        app.update_results();
        let doc = app.session_browser_doc();
        let starts = preview_section_start_lines(&doc.line_record_indices);
        app.preview_scroll = preview_visual_line_offset(&doc.lines, *starts.last().unwrap(), 80);
        app.sync_selected_preview_record_from_scroll(80);

        app.jump_preview_record(1, 80);

        assert_eq!(
            app.preview_scroll,
            preview_visual_line_offset(&doc.lines, starts[0], 80)
        );
    }

    #[test]
    fn mouse_hover_over_preview_updates_selected_turn() {
        let all = vec![
            mr(
                Some("2026-02-10T00:00:01Z"),
                Role::User,
                "first",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-02-10T00:00:02Z"),
                Role::Assistant,
                "second",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-02-10T00:00:03Z"),
                Role::User,
                "third",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
        ];
        let mut app = ready_app_with_indexed_data(all);
        app.update_results();
        let area = Rect::new(0, 0, 100, 30);
        let geometry = app_geometry(area, &app);
        let preview_area = geometry.turn_preview;
        let doc = app.session_browser_doc();
        let starts = preview_section_start_lines(&doc.line_record_indices);
        let second_turn_y = preview_area.y
            + 1
            + preview_visual_line_offset(
                &doc.lines,
                starts[1],
                preview_area.width.saturating_sub(2) as usize,
            ) as u16;
        route_mouse(
            &mut app,
            area,
            MouseEvent {
                kind: MouseEventKind::Moved,
                column: 80,
                row: second_turn_y,
                modifiers: KeyModifiers::empty(),
            },
        );

        assert_eq!(app.selected_preview_record_idx, Some(1));
    }

    #[test]
    fn scroll_preview_page_uses_provided_page_height() {
        let all = vec![mr(
            Some("2026-02-10T00:00:01Z"),
            Role::User,
            "line 1\nline 2\nline 3\nline 4\nline 5\nline 6\nline 7\nline 8",
            "a",
            SourceKind::CodexSessionJsonl,
        )];
        let mut app = ready_app_with_indexed_data(all);
        app.update_results();

        app.scroll_preview_page(1, 6, 80);
        assert_eq!(app.preview_scroll, 5);

        app.scroll_preview_page(-1, 6, 80);
        assert_eq!(app.preview_scroll, 0);
    }

    #[test]
    fn result_line_starts_with_timestamp_before_tags() {
        let sess = SessionSummary {
            source: SourceKind::CodexSessionJsonl,
            session_id: "s1".to_string(),
            account: Some("work".to_string()),
            first_user_idx: 0,
            last_ts: Some("2026-02-10T00:00:00Z".to_string()),
            cwd: Some("/tmp/proj".to_string()),
            dir: "proj".to_string(),
            first_line: "session opener".to_string(),
            machine_id: "mini".to_string(),
            machine_name: "Mini".to_string(),
            origin: "remote".to_string(),
            project_slug: Some("proj".to_string()),
        };

        let line = result_line(
            &sess,
            None,
            0,
            "",
            &config::UiTagConfig::default(),
            Style::default(),
        );
        let rendered = line
            .spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect::<String>();

        assert!(rendered.starts_with("2026-02-10T00:00:00 "));
        assert!(rendered.contains(" O work "));
        assert!(rendered.contains(" Mini "));
        assert!(rendered.contains(" proj "));
    }

    #[test]
    fn selected_preview_bgcolor_target_uses_selected_session_cwd() {
        let all = vec![
            mr(
                Some("2026-02-10T00:00:01Z"),
                Role::User,
                "first",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-02-10T00:00:02Z"),
                Role::User,
                "second",
                "b",
                SourceKind::CodexSessionJsonl,
            ),
        ];
        let (mut sessions, session_records) = build_session_index(&all);
        sessions[0].cwd = Some("/tmp/project-b".to_string());
        sessions[1].cwd = Some("/tmp/project-a".to_string());

        let app = App {
            query: String::new(),
            cursor_pos: 0,
            telemetry_query: String::new(),
            telemetry_cursor_pos: 0,
            max_results: 0,
            active_tag_filters: Vec::new(),
            all,
            sessions,
            session_records,
            filtered: vec![
                SessionHit {
                    session_idx: 0,
                    matched_record_idx: None,
                    hit_count: 0,
                },
                SessionHit {
                    session_idx: 1,
                    matched_record_idx: None,
                    hit_count: 0,
                },
            ],
            selected: 1,
            offset: 0,
            selected_preview_record_idx: None,
            preview_scroll: 0,
            preview_scroll_reset_pending: false,
            session_browser_start_scroll: 0,
            session_browser_end_scroll: usize::MAX,
            session_browser_active_pane: SessionBrowserPane::Start,
            git_graph_scroll: 0,
            git_commit_scroll: 0,
            git_graph_visible: false,
            git_commit_visible: false,
            active_pane: ActivePane::Results,
            active_split: None,
            layout_state: LayoutState::new(),
            dragged_split: None,
            last_query: String::new(),
            last_results: vec![],
            last_tag_filters: Vec::new(),
            indexing: IndexingProgress::default(),
            ready: true,
            telemetry_log_path: None,
            telemetry_sink: None,
            telemetry_events: EventBuffer::new(EVENT_BUFFER_MAX_BYTES),
            cache_path: None,
            show_telemetry: false,
            enabled_log_groups: HashSet::new(),
            log_groups_help_emitted: false,
            ui_status: None,
            preview_bgcolor_target: None,
            preview_bgcolor: None,
            preview_remote_style: false,
            preview_bgcolor_cache: HashMap::new(),
            ui_tags: config::UiTagConfig::default(),
            remotes: HashMap::new(),
            git_repo_cache: HashMap::new(),
            git_commit_preview_cache: HashMap::new(),
        };

        assert_eq!(
            selected_preview_bgcolor_target(&app),
            Some("/tmp/project-a")
        );
    }

    #[test]
    fn parse_hex_color_accepts_rgb_hex() {
        assert_eq!(
            parse_hex_color("#123abc"),
            Some(Color::Rgb(0x12, 0x3a, 0xbc))
        );
        assert_eq!(parse_hex_color("123abc"), None);
        assert_eq!(parse_hex_color("#123abz"), None);
    }

    #[test]
    fn highlighted_line_splits_matching_spans() {
        let line = highlighted_line("Hello there hello", "hello", Style::default());

        assert_eq!(line.spans.len(), 3);
        assert_eq!(line.spans[0].content.as_ref(), "Hello");
        assert_eq!(line.spans[1].content.as_ref(), " there ");
        assert_eq!(line.spans[2].content.as_ref(), "hello");
    }

    #[test]
    fn highlighted_line_uses_distinct_colors_for_distinct_query_terms() {
        let line = highlighted_line("alpha beta", "alpha beta", Style::default());
        let alpha = line
            .spans
            .iter()
            .find(|span| span.content.as_ref() == "alpha")
            .unwrap();
        let beta = line
            .spans
            .iter()
            .find(|span| span.content.as_ref() == "beta")
            .unwrap();

        assert_eq!(alpha.style, query_match_style_for(0));
        assert_eq!(beta.style, query_match_style_for(1));
        assert_ne!(alpha.style.bg, beta.style.bg);
    }

    #[test]
    fn preview_markdown_renders_headings_and_inline_code() {
        let lines = render_preview_message_lines("# Heading with `code`", "", Style::default());
        let theme = MarkdownTheme::new(Style::default());

        assert_eq!(lines.len(), 1);
        assert_eq!(line_text(&lines[0]), "# Heading with `code`");
        assert_eq!(lines[0].spans[0].style, theme.heading_markers);
        assert_eq!(lines[0].spans[1].style, theme.heading_style(1));
        assert_eq!(lines[0].spans[3].style, theme.inline_code);
    }

    #[test]
    fn preview_markdown_renders_links_without_losing_raw_text() {
        let lines =
            render_preview_message_lines("[docs](https://example.com)", "", Style::default());
        let theme = MarkdownTheme::new(Style::default());

        assert_eq!(line_text(&lines[0]), "[docs](https://example.com)");
        assert_eq!(lines[0].spans[1].style, theme.link_text);
        assert_eq!(lines[0].spans[3].style, theme.link_url);
    }

    #[test]
    fn preview_markdown_highlights_tagged_rust_fences() {
        let lines =
            render_preview_message_lines("```rust\nlet answer = 42;\n```", "", Style::default());
        let theme = MarkdownTheme::new(Style::default());

        assert_eq!(lines.len(), 3);
        assert_eq!(line_text(&lines[1]), "let answer = 42;");
        assert!(
            lines[1]
                .spans
                .iter()
                .any(|span| span.content.as_ref() == "let" && span.style == theme.code_keyword)
        );
        assert!(
            lines[1]
                .spans
                .iter()
                .any(|span| span.content.as_ref() == "42" && span.style == theme.code_number)
        );
    }

    #[test]
    fn preview_markdown_uses_generic_style_for_untagged_fences() {
        let lines = render_preview_message_lines("```\nplain text\n```", "", Style::default());
        let theme = MarkdownTheme::new(Style::default());

        assert_eq!(lines.len(), 3);
        assert_eq!(lines[1].spans.len(), 1);
        assert_eq!(line_text(&lines[1]), "plain text");
        assert_eq!(lines[1].spans[0].style, theme.code_text);
    }

    #[test]
    fn preview_markdown_query_highlight_overrides_code_style() {
        let lines =
            render_preview_message_lines("```rust\nlet value = 42;\n```", "42", Style::default());
        let theme = MarkdownTheme::new(Style::default());

        let highlight = lines[1]
            .spans
            .iter()
            .find(|span| span.content.as_ref() == "42")
            .unwrap();
        assert_eq!(
            highlight.style,
            theme.code_number.patch(query_match_style_for(0))
        );
    }

    #[test]
    fn preview_markdown_renders_blockquotes_and_lists() {
        let lines = render_preview_message_lines("> quoted\n- item", "", Style::default());
        let theme = MarkdownTheme::new(Style::default());

        assert_eq!(line_text(&lines[0]), "> quoted");
        assert_eq!(line_text(&lines[1]), "- item");
        assert_eq!(lines[0].spans[0].style, theme.quote_markers);
        assert_eq!(lines[0].spans[1].style, theme.quote_text);
        assert_eq!(lines[1].spans[0].style, theme.list_markers);
    }

    #[test]
    fn provider_label_distinguishes_supported_providers() {
        assert_eq!(provider_label(SourceKind::ClaudeProjectJsonl, None), "C");
        assert_eq!(provider_label(SourceKind::CodexSessionJsonl, None), "O");
        assert_eq!(provider_label(SourceKind::CodexHistoryJsonl, None), "O");
        assert_eq!(provider_label(SourceKind::OpenCodeSession, None), "OC");
        assert_eq!(
            provider_label(SourceKind::ClaudeProjectJsonl, Some("abc")),
            "C abc"
        );
    }

    fn line_text(line: &Line<'_>) -> String {
        line.spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect::<String>()
    }

    #[test]
    fn session_tag_spans_do_not_duplicate_account_when_provider_tag_is_visible() {
        let sess = SessionSummary {
            source: SourceKind::CodexSessionJsonl,
            session_id: "s1".to_string(),
            account: Some("work".to_string()),
            first_user_idx: 0,
            last_ts: None,
            cwd: None,
            dir: "proj".to_string(),
            first_line: "hello".to_string(),
            machine_id: "local".to_string(),
            machine_name: "MBP".to_string(),
            origin: "local".to_string(),
            project_slug: None,
        };

        let rendered = session_tag_spans(&sess, &config::UiTagConfig::default())
            .into_iter()
            .map(|span| span.content.into_owned())
            .collect::<String>();

        assert_eq!(rendered.matches("work").count(), 1);
    }

    #[test]
    fn session_tag_spans_do_not_render_standalone_account_tag() {
        let sess = SessionSummary {
            source: SourceKind::CodexSessionJsonl,
            session_id: "s1".to_string(),
            account: Some("work".to_string()),
            first_user_idx: 0,
            last_ts: None,
            cwd: None,
            dir: "proj".to_string(),
            first_line: "hello".to_string(),
            machine_id: "local".to_string(),
            machine_name: "MBP".to_string(),
            origin: "local".to_string(),
            project_slug: None,
        };
        let tags = config::UiTagConfig {
            show_provider: false,
            ..config::UiTagConfig::default()
        };

        let rendered = session_tag_spans(&sess, &tags)
            .into_iter()
            .map(|span| span.content.into_owned())
            .collect::<String>();

        assert!(!rendered.contains("work"));
    }

    #[test]
    fn session_tag_spans_hide_local_host_tag() {
        let sess = SessionSummary {
            source: SourceKind::CodexSessionJsonl,
            session_id: "s1".to_string(),
            account: None,
            first_user_idx: 0,
            last_ts: None,
            cwd: None,
            dir: "proj".to_string(),
            first_line: "hello".to_string(),
            machine_id: "local".to_string(),
            machine_name: "MBP M1 Max".to_string(),
            origin: "local".to_string(),
            project_slug: None,
        };

        let rendered = session_tag_spans(&sess, &config::UiTagConfig::default())
            .into_iter()
            .map(|span| span.content.into_owned())
            .collect::<String>();

        assert!(!rendered.contains("MBP M1 Max"));
    }

    #[test]
    fn session_tag_spans_show_remote_host_tag() {
        let sess = SessionSummary {
            source: SourceKind::CodexSessionJsonl,
            session_id: "s1".to_string(),
            account: None,
            first_user_idx: 0,
            last_ts: None,
            cwd: None,
            dir: "proj".to_string(),
            first_line: "hello".to_string(),
            machine_id: "mini".to_string(),
            machine_name: "Mini".to_string(),
            origin: "workstation".to_string(),
            project_slug: None,
        };

        let rendered = session_tag_spans(&sess, &config::UiTagConfig::default())
            .into_iter()
            .map(|span| span.content.into_owned())
            .collect::<String>();

        assert!(rendered.contains("Mini"));
    }

    #[test]
    fn short_ts_formats_epoch_timestamps_for_display() {
        assert_eq!(short_ts(Some("1704067200000")), "2024-01-01T00:00:00");
        assert_eq!(
            short_ts(Some("2026-02-10T00:00:00Z")),
            "2026-02-10T00:00:00"
        );
    }

    #[test]
    fn ts_cmp_str_sorts_epoch_and_rfc3339_timestamps_by_actual_time() {
        assert_eq!(
            ts_cmp_str("1704067200000", "2023-12-31T23:59:59Z"),
            cmp::Ordering::Greater
        );
        assert_eq!(
            ts_cmp_str("1704067200", "2024-01-01T00:00:00Z"),
            cmp::Ordering::Equal
        );
    }

    #[test]
    fn build_session_index_extracts_user_prompt_from_codex_title_task_prompt() {
        let all = vec![
            mr(
                Some("2026-02-10T00:00:00Z"),
                Role::User,
                "# AGENTS.md instructions for /x\n\n<INSTRUCTIONS>\n...",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-02-10T00:00:01Z"),
                Role::User,
                "<environment_context>\n  <cwd>/x</cwd>\n</environment_context>",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-02-10T00:00:02Z"),
                Role::User,
                "You are a helpful assistant. You will be presented with a user prompt.\n\nUser prompt:\nline1\nline2",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
        ];

        let (sessions, _) = build_session_index(&all);
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].first_line, "line2");
    }

    #[test]
    fn build_session_index_skips_agents_instructions_in_codex_sessions() {
        let all = vec![
            mr(
                Some("2026-02-10T00:00:00Z"),
                Role::User,
                "# AGENTS.md instructions for /x\n\n<INSTRUCTIONS>\n...",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-02-10T00:00:01Z"),
                Role::User,
                "real first",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-02-10T00:00:02Z"),
                Role::Assistant,
                "ok",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
        ];

        let (sessions, _) = build_session_index(&all);
        assert_eq!(sessions.len(), 1);

        let first = &all[sessions[0].first_user_idx];
        assert_eq!(first.role, Role::User);
        assert_eq!(first.text, "real first");
    }

    #[test]
    fn build_session_index_skips_environment_context_in_codex_sessions() {
        let all = vec![
            mr(
                Some("2026-02-10T00:00:00Z"),
                Role::User,
                "<environment_context>\n  <cwd>/x</cwd>\n  <shell>zsh</shell>\n</environment_context>",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-02-10T00:00:01Z"),
                Role::User,
                "real first",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
        ];

        let (sessions, _) = build_session_index(&all);
        assert_eq!(sessions.len(), 1);

        let first = &all[sessions[0].first_user_idx];
        assert_eq!(first.role, Role::User);
        assert_eq!(first.text, "real first");
    }

    #[test]
    fn build_session_index_skips_sessions_with_only_noise_user_messages() {
        let all = vec![
            mr(
                Some("2026-02-10T00:00:00Z"),
                Role::User,
                "# AGENTS.md instructions for /x\n\n<INSTRUCTIONS>\n...",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-02-10T00:00:01Z"),
                Role::User,
                "<environment_context>\n  <cwd>/x</cwd>\n</environment_context>",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
            mr(
                Some("2026-02-10T00:00:02Z"),
                Role::Assistant,
                "ok",
                "a",
                SourceKind::CodexSessionJsonl,
            ),
        ];

        let (sessions, _) = build_session_index(&all);
        assert!(sessions.is_empty());
    }

    #[test]
    fn build_session_index_sets_dir_from_cwd() {
        let mut r1 = mr(
            Some("2026-02-10T00:00:00Z"),
            Role::User,
            "hi",
            "a",
            SourceKind::CodexSessionJsonl,
        );
        r1.cwd = Some("/home/tizze/projects/myproj".to_string());
        let all = vec![r1];

        let (sessions, _) = build_session_index(&all);
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].dir, "myproj");
    }

    #[test]
    fn build_session_index_includes_codex_history_sessions() {
        let all = vec![
            mr(
                Some("100"),
                Role::Unknown,
                "hello",
                "h1",
                SourceKind::CodexHistoryJsonl,
            ),
            mr(
                Some("200"),
                Role::Unknown,
                "later",
                "h1",
                SourceKind::CodexHistoryJsonl,
            ),
        ];

        let (sessions, _) = build_session_index(&all);
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].source, SourceKind::CodexHistoryJsonl);
        assert_eq!(sessions[0].session_id, "h1");
        assert_eq!(all[sessions[0].first_user_idx].text, "hello");
    }

    #[test]
    fn dir_name_from_cwd_extracts_basename() {
        assert_eq!(
            dir_name_from_cwd("/home/tizze/ghq/github.com/tizze/agent-history"),
            "agent-history"
        );
        assert_eq!(dir_name_from_cwd("/home/tizze/x/y/"), "y");
        assert_eq!(dir_name_from_cwd(""), "-");
        assert_eq!(dir_name_from_cwd("   "), "-");
    }
}
