use crate::{
    args::Args,
    indexer::{
        ImageAttachment, ImageAttachmentKind, IndexerEvent, MessageRecord, Role, SourceKind,
    },
    search, telemetry,
};
use anyhow::Context as _;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use crossterm::{
    cursor,
    event::{
        self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEvent, KeyModifiers,
        MouseEvent, MouseEventKind,
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
use serde_json::Value;
use std::{
    cmp,
    collections::HashMap,
    env, fs,
    io::{self, Stdout, Write},
    path::{Path, PathBuf},
    process::Command,
    sync::mpsc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

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
}

const PREVIEW_MAX_MATCHES: usize = 100;
const PREVIEW_MAX_LINES: usize = 5000;

fn build_session_index(all: &[MessageRecord]) -> (Vec<SessionSummary>, Vec<Vec<usize>>) {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    struct SessionKeyRef<'a> {
        source: SourceKind,
        session_id: &'a str,
        account: Option<&'a str>,
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

fn short_ts(ts: Option<&str>) -> String {
    let ts = ts.unwrap_or("");
    if let Some(formatted) = display_timestamp(ts) {
        return formatted;
    }
    ts.get(0..19).unwrap_or(ts).to_string()
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

fn latest_lines(path: &Path, max_lines: usize) -> Vec<String> {
    let Ok(body) = fs::read_to_string(path) else {
        return Vec::new();
    };
    let mut lines: Vec<String> = body.lines().map(|line| line.to_string()).collect();
    if lines.len() > max_lines {
        let start = lines.len() - max_lines;
        lines.drain(0..start);
    }
    lines
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

fn format_telemetry_event_line(row: &Value) -> String {
    let ts = row
        .get("ts_ms")
        .map(|value| {
            value
                .as_u64()
                .map(|ts| short_ts(Some(&ts.to_string())))
                .unwrap_or_else(|| value.to_string())
        })
        .unwrap_or_default();
    let kind = row
        .get("kind")
        .and_then(|value| value.as_str())
        .unwrap_or("event");
    let data = row.get("data").unwrap_or(&Value::Null);

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
        _ => serde_json::to_string(data).unwrap_or_default(),
    };

    if ts.is_empty() {
        format!("{kind}: {summary}")
    } else {
        format!("{ts}  {kind}: {summary}")
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

fn result_line(sess: &SessionSummary, matched: Option<&MessageRecord>, hit_count: usize) -> String {
    let ts = short_ts(sess.last_ts.as_deref());
    let prefix = format!(
        "{} {} [{}] {}",
        ts,
        provider_label(sess.source, sess.account.as_deref()),
        sess.dir,
        sess.first_line,
    );

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

fn highlighted_line(
    text: &str,
    query: &str,
    base_style: Style,
    match_style: Style,
) -> Line<'static> {
    let ranges = search::find_match_ranges(query, text);
    if ranges.is_empty() {
        return Line::from(vec![Span::styled(text.to_string(), base_style)]);
    }

    let mut spans: Vec<Span<'static>> = Vec::new();
    let mut last = 0usize;
    for (start, end) in ranges {
        if start > last {
            spans.push(Span::styled(text[last..start].to_string(), base_style));
        }
        spans.push(Span::styled(text[start..end].to_string(), match_style));
        last = end;
    }
    if last < text.len() {
        spans.push(Span::styled(text[last..].to_string(), base_style));
    }

    Line::from(spans)
}

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
    max_results: usize,

    all: Vec<MessageRecord>,
    sessions: Vec<SessionSummary>,
    session_records: Vec<Vec<usize>>,
    filtered: Vec<SessionHit>,
    selected: usize,
    offset: usize,
    preview_scroll: usize,
    preview_scroll_reset_pending: bool,

    last_query: String,
    last_results: Vec<usize>,

    indexing: IndexingProgress,
    ready: bool,
    telemetry_log_path: Option<PathBuf>,
    show_telemetry: bool,
    last_bgcolor_target: Option<String>,
}

pub fn run(args: Args) -> anyhow::Result<()> {
    let rx = crate::indexer::spawn_indexer_from_args(args.clone());

    let mut stdout = io::stdout();
    enable_raw_mode().context("raw modeの有効化に失敗")?;
    execute!(
        stdout,
        EnterAlternateScreen,
        cursor::Hide,
        EnableMouseCapture
    )
    .context("画面切替に失敗")?;

    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend).context("Terminal初期化に失敗")?;
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
    args: Args,
) -> anyhow::Result<()> {
    let mut app = App {
        query: args.query.unwrap_or_default(),
        max_results: args.max_results,
        all: Vec::new(),
        sessions: Vec::new(),
        session_records: Vec::new(),
        filtered: Vec::new(),
        selected: 0,
        offset: 0,
        preview_scroll: 0,
        preview_scroll_reset_pending: false,
        last_query: String::new(),
        last_results: Vec::new(),
        indexing: IndexingProgress::default(),
        ready: false,
        telemetry_log_path: (!args.no_telemetry).then(|| {
            args.telemetry_log
                .clone()
                .unwrap_or_else(telemetry::default_log_path)
        }),
        show_telemetry: false,
        last_bgcolor_target: None,
    };

    loop {
        while let Ok(ev) = rx.try_recv() {
            handle_indexer_event(&mut app, ev);
        }

        sync_terminal_bgcolor(terminal, &mut app);

        terminal.draw(|f| ui(f, &mut app)).context("描画に失敗")?;

        if !event::poll(Duration::from_millis(50)).context("event pollに失敗")? {
            continue;
        }

        let ev = event::read().context("event readに失敗")?;
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
        KeyCode::Char('k') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.scroll_preview_lines(-1);
            return Ok(false);
        }
        KeyCode::Char('j') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.scroll_preview_lines(1);
            return Ok(false);
        }
        KeyCode::Char('b') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.scroll_preview_page(-1);
            return Ok(false);
        }
        KeyCode::Char('f') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.scroll_preview_page(1);
            return Ok(false);
        }
        KeyCode::Char('t') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.toggle_telemetry_view();
            return Ok(false);
        }
        KeyCode::Backspace => {
            app.query.pop();
            app.update_results();
            return Ok(false);
        }
        KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.query.clear();
            app.update_results();
            return Ok(false);
        }
        KeyCode::Char(c) => {
            if !key.modifiers.contains(KeyModifiers::CONTROL)
                && !key.modifiers.contains(KeyModifiers::ALT)
            {
                app.query.push(c);
                app.update_results();
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
                                    "resume失敗: {} (code={})",
                                    target.program,
                                    st.code()
                                        .map(|c| c.to_string())
                                        .unwrap_or_else(|| "?".to_string())
                                ));
                            }
                            Err(e) => {
                                app.indexing.last_warn =
                                    Some(format!("resume失敗: {}: {e}", target.program));
                            }
                        }
                    }
                    None => {
                        app.indexing.last_warn = Some("この行はresume対象にできません".to_string());
                    }
                }
            }
        }
        KeyCode::Up => app.move_selection(-1),
        KeyCode::Down => app.move_selection(1),
        KeyCode::PageUp => app.page(-1),
        KeyCode::PageDown => app.page(1),
        KeyCode::Home => app.select_first(),
        KeyCode::End => app.select_last(),
        _ => {}
    }

    Ok(false)
}

fn selected_bgcolor_target(app: &App) -> Option<&str> {
    let hit = app.selected_hit()?;
    let session = app.sessions.get(hit.session_idx)?;
    session.cwd.as_deref().filter(|cwd| !cwd.trim().is_empty())
}

const EVENTS_TERMINAL_BGCOLOR: &str = "#202020";

fn emit_terminal_bgcolor_hex(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    hex: &str,
) -> anyhow::Result<()> {
    let osc11 = format!("\u{1b}]11;{hex}\u{1b}\\");
    terminal
        .backend_mut()
        .write_all(osc11.as_bytes())
        .context("failed to emit terminal background escape sequence")?;
    terminal
        .backend_mut()
        .flush()
        .context("failed to flush terminal background escape sequence")?;
    Ok(())
}

fn emit_terminal_bgcolor_for_path(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    path: &str,
) -> anyhow::Result<()> {
    let Some(home) = env::var_os("HOME") else {
        return Ok(());
    };
    let script = PathBuf::from(home).join("util/bgcolor.sh");
    if !script.is_file() {
        return Ok(());
    }

    let output = Command::new(script)
        .arg("--format=osc11")
        .arg(path)
        .output()
        .context("failed to resolve terminal background color")?;
    if !output.status.success() || output.stdout.is_empty() {
        return Ok(());
    }

    terminal
        .backend_mut()
        .write_all(&output.stdout)
        .context("failed to emit terminal background escape sequence")?;
    terminal
        .backend_mut()
        .flush()
        .context("failed to flush terminal background escape sequence")?;
    Ok(())
}

fn sync_terminal_bgcolor(terminal: &mut Terminal<CrosstermBackend<Stdout>>, app: &mut App) {
    let target = if app.show_telemetry {
        Some(EVENTS_TERMINAL_BGCOLOR.to_string())
    } else {
        selected_bgcolor_target(app).map(str::to_owned)
    };
    if target == app.last_bgcolor_target {
        return;
    }

    if let Some(value) = target.as_deref() {
        let result = if value.starts_with('#') {
            emit_terminal_bgcolor_hex(terminal, value)
        } else {
            emit_terminal_bgcolor_for_path(terminal, value)
        };
        if let Err(err) = result {
            app.indexing.last_warn = Some(format!("bgcolor update failed: {err}"));
            return;
        }
    }

    app.last_bgcolor_target = target;
}

fn app_panes(area: Rect) -> (Rect, Rect) {
    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints(
            [
                Constraint::Length(3),
                Constraint::Min(1),
                Constraint::Length(2),
            ]
            .as_ref(),
        )
        .split(area);
    let main = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(45), Constraint::Percentage(55)].as_ref())
        .split(root[1]);
    (main[0], main[1])
}

fn point_in_rect(x: u16, y: u16, rect: Rect) -> bool {
    x >= rect.x
        && x < rect.x.saturating_add(rect.width)
        && y >= rect.y
        && y < rect.y.saturating_add(rect.height)
}

fn handle_mouse(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    app: &mut App,
    mouse: MouseEvent,
) -> anyhow::Result<()> {
    if !app.ready {
        return Ok(());
    }

    let area = terminal.size().context("terminal size取得に失敗")?;
    route_mouse(app, area.into(), mouse);
    Ok(())
}

fn route_mouse(app: &mut App, area: Rect, mouse: MouseEvent) {
    let (results_area, preview_area) = app_panes(area);
    let preview_line_step = 3;
    let results_line_step = 1;
    let preview_hit_area = if app.show_telemetry {
        area
    } else {
        preview_area
    };

    match mouse.kind {
        MouseEventKind::ScrollUp => {
            if point_in_rect(mouse.column, mouse.row, preview_hit_area) {
                app.scroll_preview_lines(-preview_line_step);
            } else if !app.show_telemetry && point_in_rect(mouse.column, mouse.row, results_area) {
                app.move_selection(-results_line_step);
            }
        }
        MouseEventKind::ScrollDown => {
            if point_in_rect(mouse.column, mouse.row, preview_hit_area) {
                app.scroll_preview_lines(preview_line_step);
            } else if !app.show_telemetry && point_in_rect(mouse.column, mouse.row, results_area) {
                app.move_selection(results_line_step);
            }
        }
        _ => {}
    }
}

impl App {
    fn update_results(&mut self) {
        let q = self.query.trim().to_string();

        let prev_selected_global = self.filtered.get(self.selected).map(|hit| hit.session_idx);

        let max = self.max_results;
        let limit = |n: usize| -> bool { max != 0 && n >= max };

        let mut results: Vec<SessionHit> = Vec::new();

        if q.is_empty() {
            let mut i = 0usize;
            while i < self.sessions.len() && !limit(results.len()) {
                results.push(SessionHit {
                    session_idx: i,
                    matched_record_idx: None,
                    hit_count: 0,
                });
                i += 1;
            }
        } else {
            let base: Box<dyn Iterator<Item = usize>> = if !self.last_query.is_empty()
                && q.starts_with(&self.last_query)
                && !self.last_results.is_empty()
            {
                Box::new(self.last_results.iter().copied())
            } else {
                Box::new(0..self.sessions.len())
            };

            let compiled = search::CompiledQuery::new(&q);
            for idx in base {
                if let Some(hit) = self.session_match(idx, &compiled) {
                    results.push(hit);
                    if limit(results.len()) {
                        break;
                    }
                }
            }
        }

        self.filtered = results;
        self.last_query = q;
        self.last_results = self.filtered.iter().map(|hit| hit.session_idx).collect();

        self.offset = 0;
        self.selected = 0;
        if let Some(prev) = prev_selected_global
            && let Some(pos) = self.filtered.iter().position(|hit| hit.session_idx == prev)
        {
            self.selected = pos;
        }
        self.reset_preview_scroll_to_match();
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
        self.preview_scroll = self.build_preview_doc().first_match_line.saturating_sub(2);
        self.preview_scroll_reset_pending = true;
    }

    fn build_preview_doc(&self) -> PreviewDoc {
        if self.show_telemetry {
            return self.build_telemetry_preview_doc();
        }

        let query = self.query.trim();
        let base_style = Style::default();
        let match_style = Style::default()
            .fg(Color::Black)
            .bg(Color::Yellow)
            .add_modifier(Modifier::BOLD);

        let Some(hit) = self.selected_hit() else {
            return PreviewDoc {
                lines: vec![Line::raw("(no match)")],
                first_match_line: 0,
            };
        };
        let Some(sess) = self.sessions.get(hit.session_idx) else {
            return PreviewDoc {
                lines: vec![Line::raw("(no match)")],
                first_match_line: 0,
            };
        };

        if query.is_empty() {
            let Some(rec) = self.selected_record() else {
                return PreviewDoc {
                    lines: vec![Line::raw("(no match)")],
                    first_match_line: 0,
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
                Line::raw(format!(
                    "role: {role}   phase: {}",
                    rec.phase.as_deref().unwrap_or("")
                )),
                Line::raw(format!("cwd: {}", rec.cwd.as_deref().unwrap_or(""))),
                Line::raw(format!("file: {}:{}", rec.file.display(), rec.line)),
                Line::raw(format!("source: {}", source_label(rec.source))),
                Line::raw(""),
            ];
            if !rec.images.is_empty() {
                lines.push(Line::raw(format!("images: {}", rec.images.len())));
                for path in materialize_record_images(rec) {
                    lines.push(Line::raw(format!("image file: {path}")));
                }
                lines.push(Line::raw(""));
            }
            lines.extend(
                rec.text
                    .lines()
                    .map(|l| highlighted_line(l, query, base_style, match_style)),
            );
            if rec.text.lines().count() == 0 {
                lines.push(Line::raw(""));
            }

            return PreviewDoc {
                lines,
                first_match_line: 0,
            };
        }

        let compiled = search::CompiledQuery::new(query);
        let Some(record_idxs) = self.session_records.get(hit.session_idx) else {
            return PreviewDoc {
                lines: vec![Line::raw("(no match)")],
                first_match_line: 0,
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

        let first_match_line = lines.len();
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

            let section: Vec<Line<'static>> = {
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
                if !rec.images.is_empty() {
                    for path in materialize_record_images(rec) {
                        section.push(Line::raw(format!("image file: {path}")));
                    }
                    section.push(Line::raw(""));
                }
                section.extend(
                    rec.text
                        .lines()
                        .map(|l| highlighted_line(l, query, base_style, match_style)),
                );
                if rec.text.lines().count() == 0 {
                    section.push(Line::raw(""));
                }
                section.push(Line::raw(""));
                section
            };

            if lines_used.saturating_add(section.len()) > PREVIEW_MAX_LINES {
                line_limited = true;
                break;
            }

            lines_used += section.len();
            lines.extend(section);
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
        }
    }

    fn build_telemetry_preview_doc(&self) -> PreviewDoc {
        let Some(path) = self.telemetry_log_path.as_ref() else {
            return PreviewDoc {
                lines: vec![Line::raw("events disabled")],
                first_match_line: 0,
            };
        };

        let mut lines: Vec<Line<'static>> = vec![Line::raw(format!("log: {}", path.display()))];
        if let Ok(metadata) = fs::metadata(path) {
            lines.push(Line::raw(format!("bytes: {}", metadata.len())));
        }

        let raw_lines = latest_lines(path, 400);
        if raw_lines.is_empty() {
            lines.push(Line::raw(""));
            lines.push(Line::raw("(no events yet)"));
            return PreviewDoc {
                lines,
                first_match_line: 0,
            };
        }

        let parsed: Vec<Value> = raw_lines
            .iter()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .collect();
        let latest_start = parsed
            .iter()
            .rposition(|row| {
                row.get("kind").and_then(|value| value.as_str()) == Some("indexer_started")
            })
            .unwrap_or(0);
        let run = &parsed[latest_start..];

        let find_data = |kind: &str| {
            run.iter()
                .rev()
                .find(|row| row.get("kind").and_then(|value| value.as_str()) == Some(kind))
                .and_then(|row| row.get("data"))
        };

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
        lines.push(Line::raw(""));
        lines.push(Line::raw("latest run events:"));
        lines.push(Line::raw(""));

        let first_match_line = lines.len();
        for row in run.iter().rev() {
            lines.push(Line::raw(format_telemetry_event_line(row)));
        }

        PreviewDoc {
            lines,
            first_match_line,
        }
    }

    fn scroll_preview_lines(&mut self, delta: i32) {
        let cur = self.preview_scroll as i32;
        self.preview_scroll = cmp::max(0, cur + delta) as usize;
        self.preview_scroll_reset_pending = false;
    }

    fn scroll_preview_page(&mut self, dir: i32) {
        let delta = 10i32 * dir;
        self.scroll_preview_lines(delta);
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

    fn page(&mut self, dir: i32) {
        if self.filtered.is_empty() {
            return;
        }
        let delta = 10i32 * dir;
        self.move_selection(delta);
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
        self.reset_preview_scroll_to_match();
    }
}

fn ui(f: &mut ratatui::Frame, app: &mut App) {
    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints(
            [
                Constraint::Length(3),
                Constraint::Min(1),
                Constraint::Length(2),
            ]
            .as_ref(),
        )
        .split(f.area());

    let query = Paragraph::new(format!("> {}", app.query))
        .block(Block::default().borders(Borders::ALL).title("Query"))
        .style(Style::default().fg(Color::White));
    f.render_widget(query, root[0]);

    if !app.ready {
        let main = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(3), Constraint::Min(1)].as_ref())
            .split(root[1]);

        let pct = if app.indexing.total_files == 0 {
            0u16
        } else {
            ((app.indexing.processed_files.saturating_mul(100)) / app.indexing.total_files) as u16
        };

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

        let footer = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Length(1)].as_ref())
            .split(root[2]);

        let status = Paragraph::new(
            app.indexing
                .last_warn
                .as_deref()
                .map(|s| format!("status: {s}"))
                .unwrap_or_default(),
        )
        .style(Style::default().fg(Color::Yellow));
        f.render_widget(status, footer[0]);

        let keys = Paragraph::new("Esc/Ctrl+c: quit").style(Style::default().fg(Color::DarkGray));
        f.render_widget(keys, footer[1]);
        return;
    }

    if app.show_telemetry {
        let telemetry_area = root[1];
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
        let telemetry = Paragraph::new(Text::from(telemetry_doc.lines))
            .block(Block::default().borders(Borders::ALL).title("Events"))
            .scroll((app.preview_scroll as u16, 0))
            .wrap(Wrap { trim: false });
        f.render_widget(telemetry, telemetry_area);

        let footer = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Length(1)].as_ref())
            .split(root[2]);

        let status = Paragraph::new(
            app.indexing
                .last_warn
                .as_deref()
                .map(|s| format!("status: {s}"))
                .unwrap_or_default(),
        )
        .style(Style::default().fg(Color::Yellow));
        f.render_widget(status, footer[0]);

        let keys = Paragraph::new(format!(
            "Esc/Ctrl+c: quit  Ctrl+t: events  Ctrl+j/k: scroll line  Ctrl+f/b: scroll page  wheel: scroll  query: \"{}\"",
            app.query.trim()
        ))
        .style(Style::default().fg(Color::DarkGray));
        f.render_widget(keys, footer[1]);
        return;
    }

    let main = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(45), Constraint::Percentage(55)].as_ref())
        .split(root[1]);

    // Results pane (manual windowing)
    let results_area = main[0];
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
    let query = app.query.trim();
    let result_match_style = Style::default()
        .fg(Color::Black)
        .bg(Color::Yellow)
        .add_modifier(Modifier::BOLD);

    let mut lines: Vec<Line> = Vec::new();
    for hit in app.filtered[visible_start..visible_end].iter() {
        let Some(sess) = app.sessions.get(hit.session_idx) else {
            continue;
        };
        let matched = hit.matched_record_idx.and_then(|idx| app.all.get(idx));
        lines.push(highlighted_line(
            &result_line(sess, matched, hit.hit_count),
            query,
            Style::default(),
            result_match_style,
        ));
    }

    let results = Paragraph::new(Text::from(lines)).block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!("Results ({})", app.filtered.len())),
    );
    f.render_widget(results, results_area);

    // Preview
    let preview_area = main[1];
    let preview_inner_height = preview_area.height.saturating_sub(2) as usize;
    let preview_inner_width = preview_area.width.saturating_sub(2) as usize;
    let preview_doc = app.build_preview_doc();
    if app.preview_scroll_reset_pending {
        let first_match_visual_line = preview_visual_line_offset(
            &preview_doc.lines,
            preview_doc.first_match_line,
            preview_inner_width,
        );
        app.preview_scroll = first_match_visual_line.saturating_sub(2);
        app.preview_scroll_reset_pending = false;
    }
    let preview_total_lines = preview_visual_line_count(&preview_doc.lines, preview_inner_width);
    let preview_max_scroll = preview_total_lines.saturating_sub(preview_inner_height);
    app.preview_scroll = cmp::min(app.preview_scroll, preview_max_scroll);
    let preview = Paragraph::new(Text::from(preview_doc.lines))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(if app.show_telemetry {
                    "Events"
                } else {
                    "Preview"
                }),
        )
        .scroll((app.preview_scroll as u16, 0))
        .wrap(Wrap { trim: false });
    f.render_widget(preview, preview_area);

    let footer = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Length(1)].as_ref())
        .split(root[2]);

    let status = Paragraph::new(
        app.indexing
            .last_warn
            .as_deref()
            .map(|s| format!("status: {s}"))
            .unwrap_or_default(),
    )
    .style(Style::default().fg(Color::Yellow));
    f.render_widget(status, footer[0]);

    let keys = Paragraph::new(format!(
        "Esc/Ctrl+c: quit  Enter: resume  Ctrl+o: pager  Ctrl+t: events  ↑/↓: move  Ctrl+j/k: preview line  Ctrl+f/b: preview page  wheel: pane scroll  Backspace: delete  Ctrl+u: clear  query: \"{}\"",
        app.query.trim()
    ))
    .style(Style::default().fg(Color::DarkGray));
    f.render_widget(keys, footer[1]);

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
            .fg(Color::Black)
            .bg(Color::LightCyan)
            .add_modifier(Modifier::BOLD);
        let selected_match_style = Style::default()
            .fg(Color::Black)
            .bg(Color::Yellow)
            .add_modifier(Modifier::BOLD);
        let p = Paragraph::new(Text::from(vec![highlighted_line(
            &result_line(sess, matched, hit.hit_count),
            query,
            selected_base_style,
            selected_match_style,
        )]));
        f.render_widget(p, highlight_area);
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
    configured_resume_command_for_shell(target, &shell_program())
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
            })
        }
        SourceKind::OpenCodeSession => Some(ResumeTarget {
            program: "opencode".to_string(),
            args: vec!["--session".to_string(), sid.to_string()],
            current_dir: cwd,
        }),
    }
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
            max_results: 0,
            all: Vec::new(),
            sessions: Vec::new(),
            session_records: Vec::new(),
            filtered: vec![],
            selected: 0,
            offset: 0,
            preview_scroll: 0,
            preview_scroll_reset_pending: false,
            last_query: String::new(),
            last_results: vec![],
            indexing: IndexingProgress::default(),
            ready: false,
            telemetry_log_path: None,
            show_telemetry: false,
            last_bgcolor_target: None,
        }
    }

    #[test]
    fn resume_target_for_codex_uses_codex_resume_with_cd_when_cwd_exists() {
        let tmp = TempDir::new("agent-history");
        let cwd = tmp.path.join("proj");
        fs::create_dir_all(&cwd).unwrap();

        let rec = MessageRecord {
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
            source: SourceKind::CodexSessionJsonl,
        };

        let app = App {
            query: String::new(),
            max_results: 0,
            all: vec![rec.clone()],
            sessions: Vec::new(),
            session_records: Vec::new(),
            filtered: vec![],
            selected: 0,
            offset: 0,
            preview_scroll: 0,
            preview_scroll_reset_pending: false,
            last_query: String::new(),
            last_results: vec![],
            indexing: IndexingProgress::default(),
            ready: true,
            telemetry_log_path: None,
            show_telemetry: false,
            last_bgcolor_target: None,
        };

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
        let cached = MessageRecord {
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
            source: SourceKind::CodexSessionJsonl,
        };
        let refreshed = MessageRecord {
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
            source: SourceKind::CodexSessionJsonl,
        };

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

        let rec = MessageRecord {
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
            source: SourceKind::ClaudeProjectJsonl,
        };

        let app = App {
            query: String::new(),
            max_results: 0,
            all: vec![rec.clone()],
            sessions: Vec::new(),
            session_records: Vec::new(),
            filtered: vec![],
            selected: 0,
            offset: 0,
            preview_scroll: 0,
            preview_scroll_reset_pending: false,
            last_query: String::new(),
            last_results: vec![],
            indexing: IndexingProgress::default(),
            ready: true,
            telemetry_log_path: None,
            show_telemetry: false,
            last_bgcolor_target: None,
        };

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
        let rec = MessageRecord {
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
            source: SourceKind::CodexSessionJsonl,
        };

        let app = App {
            query: String::new(),
            max_results: 0,
            all: vec![rec.clone()],
            sessions: Vec::new(),
            session_records: Vec::new(),
            filtered: vec![],
            selected: 0,
            offset: 0,
            preview_scroll: 0,
            preview_scroll_reset_pending: false,
            last_query: String::new(),
            last_results: vec![],
            indexing: IndexingProgress::default(),
            ready: true,
            telemetry_log_path: None,
            show_telemetry: false,
            last_bgcolor_target: None,
        };

        let target = resume_target_for_record(&app, &rec).unwrap();
        assert_eq!(target.program, "codex-account");
        assert_eq!(target.args, vec!["work", "resume", "sid"]);
    }

    #[test]
    fn resume_target_for_account_scoped_claude_uses_wrapper() {
        let rec = MessageRecord {
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
            source: SourceKind::ClaudeProjectJsonl,
        };

        let app = App {
            query: String::new(),
            max_results: 0,
            all: vec![rec.clone()],
            sessions: Vec::new(),
            session_records: Vec::new(),
            filtered: vec![],
            selected: 0,
            offset: 0,
            preview_scroll: 0,
            preview_scroll_reset_pending: false,
            last_query: String::new(),
            last_results: vec![],
            indexing: IndexingProgress::default(),
            ready: true,
            telemetry_log_path: None,
            show_telemetry: false,
            last_bgcolor_target: None,
        };

        let target = resume_target_for_record(&app, &rec).unwrap();
        assert_eq!(target.program, "claude-account");
        assert_eq!(target.args, vec!["abc", "--resume", "sid"]);
    }

    #[test]
    fn resume_target_for_opencode_uses_session_flag() {
        let tmp = TempDir::new("agent-history");
        let cwd = tmp.path.join("proj");
        fs::create_dir_all(&cwd).unwrap();

        let rec = MessageRecord {
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
            source: SourceKind::OpenCodeSession,
        };

        let app = App {
            query: String::new(),
            max_results: 0,
            all: vec![rec.clone()],
            sessions: Vec::new(),
            session_records: Vec::new(),
            filtered: vec![],
            selected: 0,
            offset: 0,
            preview_scroll: 0,
            preview_scroll_reset_pending: false,
            last_query: String::new(),
            last_results: vec![],
            indexing: IndexingProgress::default(),
            ready: true,
            telemetry_log_path: None,
            show_telemetry: false,
            last_bgcolor_target: None,
        };

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
        MessageRecord {
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
            source,
        }
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
        let (sessions, session_records) = build_session_index(&all);
        let mut app = App {
            query: "needle".to_string(),
            max_results: 0,
            all,
            sessions,
            session_records,
            filtered: vec![],
            selected: 0,
            offset: 0,
            preview_scroll: 0,
            preview_scroll_reset_pending: false,
            last_query: String::new(),
            last_results: vec![],
            indexing: IndexingProgress::default(),
            ready: true,
            telemetry_log_path: None,
            show_telemetry: false,
            last_bgcolor_target: None,
        };

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
            max_results: 0,
            all,
            sessions,
            session_records,
            filtered: vec![],
            selected: 0,
            offset: 0,
            preview_scroll: 0,
            preview_scroll_reset_pending: false,
            last_query: String::new(),
            last_results: vec![],
            indexing: IndexingProgress::default(),
            ready: true,
            telemetry_log_path: None,
            show_telemetry: false,
            last_bgcolor_target: None,
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
        let (sessions, session_records) = build_session_index(&all);
        let mut app = App {
            query: "needle".to_string(),
            max_results: 0,
            all,
            sessions,
            session_records,
            filtered: vec![],
            selected: 0,
            offset: 0,
            preview_scroll: 0,
            preview_scroll_reset_pending: false,
            last_query: String::new(),
            last_results: vec![],
            indexing: IndexingProgress::default(),
            ready: true,
            telemetry_log_path: None,
            show_telemetry: false,
            last_bgcolor_target: None,
        };

        app.update_results();
        let doc = app.build_preview_doc();
        assert_eq!(doc.first_match_line, 8);
        assert_eq!(app.preview_scroll, 6);
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
            max_results: 0,
            all,
            sessions,
            session_records,
            filtered: vec![],
            selected: 0,
            offset: 0,
            preview_scroll: 0,
            preview_scroll_reset_pending: false,
            last_query: String::new(),
            last_results: vec![],
            indexing: IndexingProgress::default(),
            ready: true,
            telemetry_log_path: None,
            show_telemetry: false,
            last_bgcolor_target: None,
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
        assert!(rendered.contains("latest run events:"));
        assert!(rendered.contains("unit_reindexed: ms=55"));
        assert!(rendered.contains("parts=300"));
    }

    #[test]
    fn build_opencode_session_pager_text_includes_whole_session() {
        let all = vec![
            MessageRecord {
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
                source: SourceKind::OpenCodeSession,
            },
            MessageRecord {
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
                source: SourceKind::OpenCodeSession,
            },
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
        let rec = MessageRecord {
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
            source: SourceKind::CodexSessionJsonl,
        };

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
    fn preview_scroll_line_movement_clamps_at_zero() {
        let all = vec![mr(
            Some("2026-02-10T00:00:01Z"),
            Role::User,
            "hello",
            "a",
            SourceKind::CodexSessionJsonl,
        )];
        let (sessions, session_records) = build_session_index(&all);
        let mut app = App {
            query: String::new(),
            max_results: 0,
            all,
            sessions,
            session_records,
            filtered: vec![],
            selected: 0,
            offset: 0,
            preview_scroll: 1,
            preview_scroll_reset_pending: false,
            last_query: String::new(),
            last_results: vec![],
            indexing: IndexingProgress::default(),
            ready: true,
            telemetry_log_path: None,
            show_telemetry: false,
            last_bgcolor_target: None,
        };

        app.scroll_preview_lines(-10);
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
        let (sessions, session_records) = build_session_index(&all);
        let mut app = App {
            query: String::new(),
            max_results: 0,
            all,
            sessions,
            session_records,
            filtered: vec![SessionHit {
                session_idx: 0,
                matched_record_idx: None,
                hit_count: 0,
            }],
            selected: 0,
            offset: 0,
            preview_scroll: 0,
            preview_scroll_reset_pending: false,
            last_query: String::new(),
            last_results: vec![],
            indexing: IndexingProgress::default(),
            ready: true,
            telemetry_log_path: None,
            show_telemetry: false,
            last_bgcolor_target: None,
        };

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
        let (sessions, session_records) = build_session_index(&all);
        let mut app = App {
            query: String::new(),
            max_results: 0,
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
            ],
            selected: 0,
            offset: 0,
            preview_scroll: 0,
            preview_scroll_reset_pending: false,
            last_query: String::new(),
            last_results: vec![],
            indexing: IndexingProgress::default(),
            ready: true,
            telemetry_log_path: None,
            show_telemetry: false,
            last_bgcolor_target: None,
        };

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
        let (sessions, session_records) = build_session_index(&all);
        let mut app = App {
            query: String::new(),
            max_results: 0,
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
            selected: 0,
            offset: 0,
            preview_scroll: 0,
            preview_scroll_reset_pending: false,
            last_query: String::new(),
            last_results: vec![],
            indexing: IndexingProgress::default(),
            ready: true,
            telemetry_log_path: None,
            show_telemetry: true,
            last_bgcolor_target: None,
        };

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
        };
        let rec = MessageRecord {
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
            source: SourceKind::CodexSessionJsonl,
        };

        let line = result_line(&sess, Some(&rec), 3);
        assert!(line.contains("session opener"));
        assert!(line.contains("matching context"));
        assert!(line.contains("[3 hits]"));
    }

    #[test]
    fn selected_bgcolor_target_uses_selected_session_cwd() {
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
            max_results: 0,
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
            preview_scroll: 0,
            preview_scroll_reset_pending: false,
            last_query: String::new(),
            last_results: vec![],
            indexing: IndexingProgress::default(),
            ready: true,
            telemetry_log_path: None,
            show_telemetry: false,
            last_bgcolor_target: None,
        };

        assert_eq!(selected_bgcolor_target(&app), Some("/tmp/project-a"));
    }

    #[test]
    fn highlighted_line_splits_matching_spans() {
        let line = highlighted_line(
            "Hello there hello",
            "hello",
            Style::default(),
            Style::default().fg(Color::Yellow),
        );

        assert_eq!(line.spans.len(), 3);
        assert_eq!(line.spans[0].content.as_ref(), "Hello");
        assert_eq!(line.spans[1].content.as_ref(), " there ");
        assert_eq!(line.spans[2].content.as_ref(), "hello");
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
