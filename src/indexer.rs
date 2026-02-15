use crate::args::Args;
use serde::Deserialize;
use serde_json::Value;
use std::{
    collections::HashSet,
    env,
    fs::File,
    io::{BufRead as _, BufReader},
    path::{Path, PathBuf},
    sync::mpsc,
    thread,
};
use walkdir::WalkDir;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    User,
    Assistant,
    System,
    Tool,
    Unknown,
}

impl Role {
    pub fn from_str(s: &str) -> Self {
        match s {
            "user" => Self::User,
            "assistant" => Self::Assistant,
            "system" => Self::System,
            "tool" => Self::Tool,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(clippy::enum_variant_names)]
pub enum SourceKind {
    CodexSessionJsonl,
    CodexHistoryJsonl,
    ClaudeProjectJsonl,
}

#[derive(Debug, Clone)]
pub struct MessageRecord {
    pub timestamp: Option<String>,
    pub role: Role,
    pub text: String,

    pub file: PathBuf,
    pub line: u32,

    pub session_id: Option<String>,
    pub cwd: Option<String>,
    pub phase: Option<String>,

    pub source: SourceKind,
}

#[derive(Debug, Clone)]
pub struct IndexerConfig {
    pub roots: Vec<PathBuf>,
    pub extra_files: Vec<PathBuf>,
}

#[derive(Debug)]
pub enum IndexerEvent {
    RootScanProgress {
        scanned_dirs: usize,
        found_roots: usize,
        found_files: usize,
        current: PathBuf,
    },
    Discovered {
        total_files: usize,
    },
    Progress {
        processed_files: usize,
        total_files: usize,
        records: usize,
        sessions: usize,
        current: PathBuf,
    },
    Warn {
        message: String,
    },
    Done {
        records: Vec<MessageRecord>,
    },
}

#[derive(Default, Debug, Clone)]
struct FileContext {
    session_id: Option<String>,
    cwd: Option<String>,
}

pub fn spawn_indexer_from_args(args: Args) -> mpsc::Receiver<IndexerEvent> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        if let Err(e) = run_indexer_from_args(args, &tx) {
            let _ = tx.send(IndexerEvent::Warn {
                message: format!("インデックス作成中にエラー: {e:#}"),
            });
            let _ = tx.send(IndexerEvent::Done { records: vec![] });
        }
    });
    rx
}

#[derive(Debug, Clone)]
struct RootScanProgress {
    scanned_dirs: usize,
    found_roots: usize,
    found_files: usize,
    current: PathBuf,
}

fn run_indexer_from_args(args: Args, tx: &mpsc::Sender<IndexerEvent>) -> anyhow::Result<()> {
    let home = env::var_os("HOME").map(PathBuf::from);

    let mut roots: Vec<PathBuf> = Vec::new();
    let mut extra_files: Vec<PathBuf> = Vec::new();

    if !args.no_default_roots
        && let Some(home) = home.as_ref()
    {
        let sessions = home.join(".codex/sessions");
        if sessions.is_dir() {
            roots.push(sessions);
        }
        let archived = home.join(".codex/archived_sessions");
        if archived.is_dir() {
            roots.push(archived);
        }

        let claude_projects = home.join(".claude/projects");
        if claude_projects.is_dir() {
            roots.push(claude_projects);
        }

        // プロジェクト配下にある `.codex/sessions` なども自動検出して追加する
        let (more_roots, more_files) = discover_project_codex_stores_with_progress(home, |p| {
            tx.send(IndexerEvent::RootScanProgress {
                scanned_dirs: p.scanned_dirs,
                found_roots: p.found_roots,
                found_files: p.found_files,
                current: p.current,
            })
            .ok();
        });
        roots.extend(more_roots);
        extra_files.extend(more_files);
    }

    for root in &args.roots {
        roots.push(expand_tilde(root, home.as_deref()));
    }

    if args.include_history {
        let Some(home) = home.as_ref() else {
            tx.send(IndexerEvent::Warn {
                message: "$HOME が見つかりませんでした（--history を使うには HOME が必要）"
                    .to_string(),
            })
            .ok();
            // 続行（history は取り込めないだけ）
            roots.sort();
            roots.dedup();
            extra_files.sort();
            extra_files.dedup();
            return run_indexer(IndexerConfig { roots, extra_files }, tx);
        };

        let history = home.join(".codex/history.jsonl");
        if history.is_file() {
            extra_files.push(history);
        }
    }

    roots.sort();
    roots.dedup();
    extra_files.sort();
    extra_files.dedup();

    run_indexer(IndexerConfig { roots, extra_files }, tx)
}

fn run_indexer(cfg: IndexerConfig, tx: &mpsc::Sender<IndexerEvent>) -> anyhow::Result<()> {
    let mut files = collect_jsonl_files(&cfg.roots, &cfg.extra_files);
    files.sort();

    tx.send(IndexerEvent::Discovered {
        total_files: files.len(),
    })
    .ok();

    let total_files = files.len();
    let mut out: Vec<MessageRecord> = Vec::new();
    let mut sessions: HashSet<(SourceKind, String)> = HashSet::new();

    for (processed_files, file) in files.into_iter().enumerate() {
        let processed_files = processed_files.saturating_add(1);
        match index_file(&file, &mut out, &mut sessions) {
            Ok(()) => {}
            Err(e) => {
                tx.send(IndexerEvent::Warn {
                    message: format!("読み取り失敗: {}: {e}", file.display()),
                })
                .ok();
            }
        }

        tx.send(IndexerEvent::Progress {
            processed_files,
            total_files,
            records: out.len(),
            sessions: sessions.len(),
            current: file,
        })
        .ok();
    }

    tx.send(IndexerEvent::Done { records: out }).ok();
    Ok(())
}

fn collect_jsonl_files(roots: &[PathBuf], extra_files: &[PathBuf]) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = Vec::new();

    for root in roots {
        if root.is_file() {
            if root.extension().and_then(|s| s.to_str()) == Some("jsonl") {
                out.push(root.to_path_buf());
            }
            continue;
        }

        for entry in WalkDir::new(root)
            .follow_links(false)
            .into_iter()
            .filter_entry(|e| {
                // `subagents` はノイズになりやすく、量も多いのでデフォルトでは除外する
                if e.file_type().is_dir() && e.file_name() == "subagents" {
                    return false;
                }
                true
            })
            .filter_map(Result::ok)
        {
            if !entry.file_type().is_file() {
                continue;
            }
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("jsonl") {
                continue;
            }
            out.push(path.to_path_buf());
        }
    }

    for file in extra_files {
        if file.is_file() {
            out.push(file.to_path_buf());
        }
    }

    out.sort();
    out.dedup();
    out
}

fn index_file(
    file: &Path,
    out: &mut Vec<MessageRecord>,
    sessions: &mut HashSet<(SourceKind, String)>,
) -> anyhow::Result<()> {
    let f = File::open(file)?;
    let mut reader = BufReader::new(f);
    let mut buf = String::new();

    let mut ctx = FileContext::default();
    let mut line_no: u32 = 0;

    loop {
        buf.clear();
        let n = reader.read_line(&mut buf)?;
        if n == 0 {
            break;
        }
        line_no = line_no.saturating_add(1);

        let line = buf.trim_end_matches(['\n', '\r']);
        if line.is_empty() {
            continue;
        }

        let Ok(v) = serde_json::from_str::<Value>(line) else {
            continue;
        };

        if let Some(rec) = extract_record(&v, file, line_no, &mut ctx) {
            if let Some(sid) = rec.session_id.as_deref() {
                sessions.insert((rec.source, sid.to_string()));
            }
            out.push(rec);
        }
    }

    Ok(())
}

fn expand_tilde(path: &Path, home: Option<&Path>) -> PathBuf {
    let Some(home) = home else {
        return path.to_path_buf();
    };
    let s = path.to_string_lossy();
    if s == "~" {
        return home.to_path_buf();
    }
    if let Some(rest) = s.strip_prefix("~/") {
        return home.join(rest);
    }
    path.to_path_buf()
}

fn discover_project_codex_stores_with_progress(
    home: &Path,
    mut on_progress: impl FnMut(RootScanProgress),
) -> (Vec<PathBuf>, Vec<PathBuf>) {
    const PROGRESS_EVERY_DIRS: usize = 500;

    // `~/.codex` は既にデフォルトで入れているので、ここでは「プロジェクト配下」の .codex を拾う。
    // ただしスキャン対象全体をHOME直下にしておくと、ユーザーの作業ディレクトリ構成に依存しない。
    let mut roots: Vec<PathBuf> = Vec::new();
    let mut files: Vec<PathBuf> = Vec::new();

    let user_codex = home.join(".codex");
    let walker = WalkDir::new(home)
        .follow_links(false)
        .max_depth(10)
        .into_iter()
        .filter_entry(|e| {
            if !e.file_type().is_dir() {
                return true;
            }
            // `~/.codex` は巨大になりやすいので探索対象から外す（既にデフォルトで別途読み込む）
            if e.path() == user_codex {
                return false;
            }
            let name = e.file_name().to_string_lossy();
            match name.as_ref() {
                // どの `.codex` 配下でも会話履歴の本体なので潜らない（`.codex` 自体だけ見えれば十分）
                "sessions" | "archived_sessions" => false,
                ".git" | "node_modules" | "target" | ".next" | ".turbo" | "dist" | "build"
                | ".venv" | ".cache" | ".local" | ".npm" | ".cargo" | ".rustup" | ".mozilla"
                | ".vscode-server" | ".cursor" | ".cursor-server" | "snap" | "google-cloud-sdk" => {
                    false
                }
                // HOME直下の `.claude` は別ルートで入れるので、ここでは潜らない（スキャン短縮）
                ".claude" => false,
                _ => true,
            }
        });

    let mut scanned_dirs: usize = 0;
    let mut last_progress: usize = 0;

    for entry in walker.filter_map(Result::ok) {
        if !entry.file_type().is_dir() {
            continue;
        }

        scanned_dirs = scanned_dirs.saturating_add(1);
        if scanned_dirs == 1 || scanned_dirs.saturating_sub(last_progress) >= PROGRESS_EVERY_DIRS {
            on_progress(RootScanProgress {
                scanned_dirs,
                found_roots: roots.len(),
                found_files: files.len(),
                current: entry.path().to_path_buf(),
            });
            last_progress = scanned_dirs;
        }

        if entry.file_name() != ".codex" {
            continue;
        }

        let dir = entry.path();
        // user-scope の ~/.codex は除外（重複回避）
        if dir == user_codex {
            continue;
        }

        let sessions = dir.join("sessions");
        if sessions.is_dir() {
            roots.push(sessions);
        }
        let archived = dir.join("archived_sessions");
        if archived.is_dir() {
            roots.push(archived);
        }
        let history = dir.join("history.jsonl");
        if history.is_file() {
            files.push(history);
        }
    }

    on_progress(RootScanProgress {
        scanned_dirs,
        found_roots: roots.len(),
        found_files: files.len(),
        current: home.to_path_buf(),
    });

    roots.sort();
    roots.dedup();
    files.sort();
    files.dedup();
    (roots, files)
}

fn extract_record(
    v: &Value,
    file: &Path,
    line: u32,
    ctx: &mut FileContext,
) -> Option<MessageRecord> {
    // 1) Codex session jsonl
    if let Some(rec) = extract_codex_session_record(v, file, line, ctx) {
        return Some(rec);
    }

    // 2) Claude project jsonl (~/.claude/projects/**.jsonl)
    if let Some(rec) = extract_claude_project_record(v, file, line) {
        return Some(rec);
    }

    // 3) Codex history jsonl (~/.codex/history.jsonl)
    extract_codex_history_record(v, file, line)
}

fn extract_content_text(payload: &Value) -> Option<String> {
    let content = payload.get("content")?.as_array()?;
    let mut parts: Vec<&str> = Vec::new();
    for item in content {
        let Some(text) = item.get("text").and_then(|x| x.as_str()) else {
            continue;
        };
        parts.push(text);
    }
    if parts.is_empty() {
        return None;
    }
    Some(parts.join("\n"))
}

fn extract_codex_session_record(
    v: &Value,
    file: &Path,
    line: u32,
    ctx: &mut FileContext,
) -> Option<MessageRecord> {
    let ty = v.get("type").and_then(|x| x.as_str())?;
    let payload = v.get("payload")?;

    if ty == "session_meta" {
        ctx.session_id = payload
            .get("id")
            .and_then(|x| x.as_str())
            .map(|s| s.to_string());
        ctx.cwd = payload
            .get("cwd")
            .and_then(|x| x.as_str())
            .map(|s| s.to_string());
        return None;
    }

    if ty != "response_item" {
        return None;
    }

    let payload_ty = payload.get("type").and_then(|x| x.as_str())?;
    if payload_ty != "message" {
        return None;
    }

    let role = payload
        .get("role")
        .and_then(|x| x.as_str())
        .map(Role::from_str)
        .unwrap_or(Role::Unknown);

    let timestamp = v
        .get("timestamp")
        .and_then(|x| x.as_str())
        .map(|s| s.to_string());

    let phase = payload
        .get("phase")
        .and_then(|x| x.as_str())
        .map(|s| s.to_string());

    let text = extract_content_text(payload)?;

    Some(MessageRecord {
        timestamp,
        role,
        text,
        file: file.to_path_buf(),
        line,
        session_id: ctx.session_id.clone(),
        cwd: ctx.cwd.clone(),
        phase,
        source: SourceKind::CodexSessionJsonl,
    })
}

fn extract_claude_project_record(v: &Value, file: &Path, line: u32) -> Option<MessageRecord> {
    // 例:
    // {"type":"user", ... , "message":{"role":"user","content":"..."}, "timestamp":"..."}
    // {"type":"assistant", ... , "message":{"role":"assistant","content":[{"type":"text","text":"..."}]}, "timestamp":"..."}
    let ty = v.get("type").and_then(|x| x.as_str())?;
    if ty != "user" && ty != "assistant" && ty != "system" && ty != "tool" {
        return None;
    }

    let message = v.get("message")?;
    let role = message
        .get("role")
        .and_then(|x| x.as_str())
        .map(Role::from_str)
        .unwrap_or(Role::Unknown);

    let timestamp = v
        .get("timestamp")
        .and_then(|x| x.as_str())
        .map(|s| s.to_string());
    let session_id = v
        .get("sessionId")
        .and_then(|x| x.as_str())
        .map(|s| s.to_string());
    let cwd = v.get("cwd").and_then(|x| x.as_str()).map(|s| s.to_string());

    let text = extract_claude_message_text(message)?;

    Some(MessageRecord {
        timestamp,
        role,
        text,
        file: file.to_path_buf(),
        line,
        session_id,
        cwd,
        phase: None,
        source: SourceKind::ClaudeProjectJsonl,
    })
}

fn extract_claude_message_text(message: &Value) -> Option<String> {
    let content = message.get("content")?;

    if let Some(s) = content.as_str() {
        let s = s.trim();
        return if s.is_empty() {
            None
        } else {
            Some(s.to_string())
        };
    }

    let arr = content.as_array()?;
    let mut parts: Vec<String> = Vec::new();
    for item in arr {
        if let Some(text) = item.get("text").and_then(|x| x.as_str()) {
            if !text.is_empty() {
                parts.push(text.to_string());
            }
            continue;
        }
        if let Some(thinking) = item.get("thinking").and_then(|x| x.as_str()) {
            if !thinking.is_empty() {
                parts.push(thinking.to_string());
            }
            continue;
        }
        if let Some(content) = item.get("content").and_then(|x| x.as_str()) {
            if !content.is_empty() {
                parts.push(content.to_string());
            }
            continue;
        }
        // tool_use などで input.args が文字列の場合だけ拾う（JSON全体は巨大化しがちなので避ける）
        if let Some(args) = item
            .get("input")
            .and_then(|x| x.as_object())
            .and_then(|o| o.get("args"))
            .and_then(|x| x.as_str())
            && !args.is_empty()
        {
            parts.push(args.to_string());
        }
    }

    if parts.is_empty() {
        return None;
    }
    Some(parts.join("\n"))
}

fn extract_codex_history_record(v: &Value, file: &Path, line: u32) -> Option<MessageRecord> {
    #[derive(Deserialize)]
    struct HistoryLine {
        session_id: Option<String>,
        ts: Option<i64>,
        text: Option<String>,
    }

    let Ok(h) = serde_json::from_value::<HistoryLine>(v.clone()) else {
        return None;
    };
    let ts = h.ts?;
    let session_id = h.session_id?;
    let text = h.text?;

    Some(MessageRecord {
        timestamp: Some(ts.to_string()),
        role: Role::Unknown,
        text,
        file: file.to_path_buf(),
        line,
        session_id: Some(session_id),
        cwd: None,
        phase: None,
        source: SourceKind::CodexHistoryJsonl,
    })
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

    #[test]
    fn extracts_session_message() {
        let line = r#"{"timestamp":"2026-02-11T14:16:44.023Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"了解。"}],"phase":"commentary"}}"#;
        let v: Value = serde_json::from_str(line).unwrap();
        let mut ctx = FileContext::default();
        let rec = extract_record(&v, Path::new("/tmp/x.jsonl"), 12, &mut ctx).unwrap();
        assert_eq!(rec.role, Role::Assistant);
        assert_eq!(rec.phase.as_deref(), Some("commentary"));
        assert_eq!(rec.text, "了解。");
        assert_eq!(rec.timestamp.as_deref(), Some("2026-02-11T14:16:44.023Z"));
    }

    #[test]
    fn session_meta_updates_context() {
        let meta = r#"{"timestamp":"2026-02-11T12:05:47.856Z","type":"session_meta","payload":{"id":"abc","cwd":"/home/tizze/x"}}"#;
        let v: Value = serde_json::from_str(meta).unwrap();
        let mut ctx = FileContext::default();
        let rec = extract_record(&v, Path::new("/tmp/x.jsonl"), 1, &mut ctx);
        assert!(rec.is_none());
        assert_eq!(ctx.session_id.as_deref(), Some("abc"));
        assert_eq!(ctx.cwd.as_deref(), Some("/home/tizze/x"));
    }

    #[test]
    fn extracts_history_line() {
        let line = r#"{"session_id":"s1","ts":123,"text":"hello"}"#;
        let v: Value = serde_json::from_str(line).unwrap();
        let mut ctx = FileContext::default();
        let rec = extract_record(&v, Path::new("/tmp/h.jsonl"), 99, &mut ctx).unwrap();
        assert_eq!(rec.text, "hello");
        assert_eq!(rec.session_id.as_deref(), Some("s1"));
        assert_eq!(rec.timestamp.as_deref(), Some("123"));
    }

    #[test]
    fn extracts_claude_project_message() {
        let line = r#"{"type":"user","cwd":"/x","sessionId":"s2","timestamp":"2026-01-01T00:00:00.000Z","message":{"role":"user","content":"hi"}}"#;
        let v: Value = serde_json::from_str(line).unwrap();
        let mut ctx = FileContext::default();
        let rec = extract_record(&v, Path::new("/tmp/c.jsonl"), 5, &mut ctx).unwrap();
        assert_eq!(rec.text, "hi");
        assert_eq!(rec.cwd.as_deref(), Some("/x"));
        assert_eq!(rec.session_id.as_deref(), Some("s2"));
        assert_eq!(rec.source, SourceKind::ClaudeProjectJsonl);
    }

    #[test]
    fn collect_jsonl_files_skips_subagents_dir() {
        let tmp = TempDir::new("agent-history-indexer");
        let root = &tmp.path;

        fs::write(root.join("a.jsonl"), "{}\n").unwrap();
        fs::create_dir_all(root.join("subagents")).unwrap();
        fs::write(root.join("subagents/b.jsonl"), "{}\n").unwrap();

        fs::create_dir_all(root.join("nested/subagents")).unwrap();
        fs::write(root.join("nested/subagents/c.jsonl"), "{}\n").unwrap();
        fs::write(root.join("nested/d.jsonl"), "{}\n").unwrap();

        let files = collect_jsonl_files(&[root.to_path_buf()], &[]);
        assert!(files.contains(&root.join("a.jsonl")));
        assert!(files.contains(&root.join("nested/d.jsonl")));
        assert!(!files.contains(&root.join("subagents/b.jsonl")));
        assert!(!files.contains(&root.join("nested/subagents/c.jsonl")));
    }

    #[test]
    fn discover_project_codex_stores_finds_project_level_codex_dirs() {
        let tmp = TempDir::new("agent-history-roots");
        let home = &tmp.path;

        // user-scope は除外されること（重複回避）
        fs::create_dir_all(home.join(".codex/sessions")).unwrap();

        // プロジェクト配下
        fs::create_dir_all(home.join("projects/p1/.codex/sessions")).unwrap();
        fs::create_dir_all(home.join("projects/p2/.codex/archived_sessions")).unwrap();
        fs::create_dir_all(home.join("projects/p3/.codex")).unwrap();
        fs::write(home.join("projects/p3/.codex/history.jsonl"), "{}\n").unwrap();

        // 除外ディレクトリ配下は検出しない
        fs::create_dir_all(home.join("projects/p4/.git/.codex/sessions")).unwrap();
        fs::create_dir_all(home.join("projects/p5/node_modules/.codex/sessions")).unwrap();
        fs::create_dir_all(home.join(".claude/projects/p6/.codex/sessions")).unwrap();

        let (roots, files) = discover_project_codex_stores_with_progress(home, |_| {});

        assert!(roots.contains(&home.join("projects/p1/.codex/sessions")));
        assert!(roots.contains(&home.join("projects/p2/.codex/archived_sessions")));
        assert!(files.contains(&home.join("projects/p3/.codex/history.jsonl")));

        // user-scope の ~/.codex は対象外
        assert!(!roots.contains(&home.join(".codex/sessions")));

        // 除外配下
        assert!(!roots.contains(&home.join("projects/p4/.git/.codex/sessions")));
        assert!(!roots.contains(&home.join("projects/p5/node_modules/.codex/sessions")));
        assert!(!roots.contains(&home.join(".claude/projects/p6/.codex/sessions")));
    }

    #[test]
    fn discover_project_codex_stores_reports_progress() {
        let tmp = TempDir::new("agent-history-roots-progress");
        let home = &tmp.path;

        fs::create_dir_all(home.join("p/.codex/sessions")).unwrap();
        fs::write(home.join("p/.codex/history.jsonl"), "{}\n").unwrap();

        let mut events: Vec<RootScanProgress> = Vec::new();
        let (roots, files) = discover_project_codex_stores_with_progress(home, |p| {
            events.push(p);
        });

        assert!(!events.is_empty());
        let last = events.last().unwrap();
        assert_eq!(last.found_roots, roots.len());
        assert_eq!(last.found_files, files.len());
        assert_eq!(last.current, home.to_path_buf());
        assert!(last.scanned_dirs >= 1);
    }

    #[test]
    fn expand_tilde_handles_home_and_subpaths() {
        let home = Path::new("/home/tizze");
        assert_eq!(
            expand_tilde(Path::new("~"), Some(home)),
            PathBuf::from(home)
        );
        assert_eq!(
            expand_tilde(Path::new("~/x/y"), Some(home)),
            PathBuf::from("/home/tizze/x/y")
        );
        assert_eq!(
            expand_tilde(Path::new("/abs/path"), Some(home)),
            PathBuf::from("/abs/path")
        );
    }
}
