use crate::{args::Args, cache, telemetry};
use anyhow::Context as _;
use serde::Deserialize;
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::{HashSet, VecDeque},
    env,
    fs::{self, File},
    io::{BufRead as _, BufReader},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, mpsc},
    thread,
    time::Instant,
    time::UNIX_EPOCH,
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
    OpenCodeSession,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageAttachment {
    pub kind: ImageAttachmentKind,
    pub label: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ImageAttachmentKind {
    DataUrl {
        media_type: String,
        data_url: String,
    },
    LocalPath {
        path: PathBuf,
    },
}

#[derive(Debug, Clone)]
pub struct MessageRecord {
    pub timestamp: Option<String>,
    pub role: Role,
    pub text: String,

    pub file: PathBuf,
    pub line: u32,

    pub session_id: Option<String>,
    pub account: Option<String>,
    pub cwd: Option<String>,
    pub phase: Option<String>,
    pub images: Vec<ImageAttachment>,

    pub source: SourceKind,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct JsonlInputRoot {
    pub path: PathBuf,
    pub account: Option<String>,
}

#[derive(Debug, Clone)]
pub struct IndexerConfig {
    pub roots: Vec<JsonlInputRoot>,
    pub extra_files: Vec<JsonlInputRoot>,
    pub opencode_storage_roots: Vec<PathBuf>,
}

#[derive(Debug)]
pub enum IndexerEvent {
    Loaded {
        records: Vec<MessageRecord>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AccountConfigKind {
    Codex,
    Claude,
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

fn run_indexer_from_args(args: Args, tx: &mpsc::Sender<IndexerEvent>) -> anyhow::Result<()> {
    let home = env::var_os("HOME").map(PathBuf::from);

    let mut roots: Vec<JsonlInputRoot> = Vec::new();
    let mut extra_files: Vec<JsonlInputRoot> = Vec::new();
    let mut opencode_storage_roots: Vec<PathBuf> = Vec::new();

    if !args.no_default_roots
        && let Some(home) = home.as_ref()
    {
        let sessions = home.join(".codex/sessions");
        if sessions.is_dir() {
            roots.push(JsonlInputRoot {
                path: sessions,
                account: None,
            });
        }
        let archived = home.join(".codex/archived_sessions");
        if archived.is_dir() {
            roots.push(JsonlInputRoot {
                path: archived,
                account: None,
            });
        }

        let claude_projects = home.join(".claude/projects");
        if claude_projects.is_dir() {
            roots.push(JsonlInputRoot {
                path: claude_projects,
                account: None,
            });
        }

        let opencode_storage = home.join(".local/share/opencode/storage");
        if opencode_storage.is_dir() {
            opencode_storage_roots.push(opencode_storage);
        }

        for (account, dir) in discover_account_config_dirs(home, AccountConfigKind::Codex) {
            let sessions = dir.join("sessions");
            if sessions.is_dir() {
                roots.push(JsonlInputRoot {
                    path: sessions,
                    account: Some(account.clone()),
                });
            }

            let archived = dir.join("archived_sessions");
            if archived.is_dir() {
                roots.push(JsonlInputRoot {
                    path: archived,
                    account: Some(account.clone()),
                });
            }
        }

        for (account, dir) in discover_account_config_dirs(home, AccountConfigKind::Claude) {
            let projects = dir.join("projects");
            if projects.is_dir() {
                roots.push(JsonlInputRoot {
                    path: projects,
                    account: Some(account),
                });
            }
        }
    }

    for root in &args.roots {
        roots.push(JsonlInputRoot {
            path: expand_tilde(root, home.as_deref()),
            account: None,
        });
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
            opencode_storage_roots.sort();
            opencode_storage_roots.dedup();
            return run_indexer(
                IndexerConfig {
                    roots,
                    extra_files,
                    opencode_storage_roots,
                },
                tx,
                None,
                false,
                (!args.no_telemetry).then(|| {
                    args.telemetry_log
                        .clone()
                        .unwrap_or_else(telemetry::default_log_path)
                }),
            );
        };

        let history = home.join(".codex/history.jsonl");
        if history.is_file() {
            extra_files.push(JsonlInputRoot {
                path: history,
                account: None,
            });
        }

        for (account, dir) in discover_account_config_dirs(home, AccountConfigKind::Codex) {
            let history = dir.join("history.jsonl");
            if history.is_file() {
                extra_files.push(JsonlInputRoot {
                    path: history,
                    account: Some(account),
                });
            }
        }
    }

    roots.sort();
    roots.dedup();
    extra_files.sort();
    extra_files.dedup();
    opencode_storage_roots.sort();
    opencode_storage_roots.dedup();

    run_indexer(
        IndexerConfig {
            roots,
            extra_files,
            opencode_storage_roots,
        },
        tx,
        (!args.no_cache).then(cache::default_db_path),
        args.rebuild_index,
        (!args.no_telemetry).then(|| {
            args.telemetry_log
                .clone()
                .unwrap_or_else(telemetry::default_log_path)
        }),
    )
}

fn run_indexer(
    cfg: IndexerConfig,
    tx: &mpsc::Sender<IndexerEvent>,
    cache_path: Option<PathBuf>,
    rebuild_index: bool,
    telemetry_path: Option<PathBuf>,
) -> anyhow::Result<()> {
    let indexer_started = Instant::now();
    let inputs = collect_index_inputs(&cfg.roots, &cfg.extra_files, &cfg.opencode_storage_roots);
    let unit_keys: Vec<String> = inputs
        .iter()
        .map(|input| cache::unit_key(input.path()))
        .collect();
    let total_files = inputs.len();
    let cache_path_string = cache_path.as_ref().map(|path| path.display().to_string());
    let telemetry_path_string = telemetry_path
        .as_ref()
        .map(|path| path.display().to_string());
    let mut telemetry_failed = false;
    let mut telemetry = match telemetry_path {
        Some(path) => match telemetry::TelemetrySink::open(&path) {
            Ok(sink) => Some(sink),
            Err(err) => {
                tx.send(IndexerEvent::Warn {
                    message: format!("telemetry unavailable: {err:#}"),
                })
                .ok();
                None
            }
        },
        None => None,
    };
    let mut emit = |kind: &str, data: Value| {
        if telemetry_failed {
            return;
        }
        if let Some(sink) = telemetry.as_mut()
            && let Err(err) = sink.emit(kind, data)
        {
            telemetry_failed = true;
            tx.send(IndexerEvent::Warn {
                message: format!("telemetry log write failed: {err:#}"),
            })
            .ok();
        }
    };

    emit(
        "indexer_started",
        json!({
            "total_files": total_files,
            "cache_enabled": cache_path_string.is_some(),
            "cache_path": cache_path_string,
            "rebuild_index": rebuild_index,
            "telemetry_path": telemetry_path_string,
        }),
    );

    tx.send(IndexerEvent::Discovered { total_files }).ok();

    if cache_path.is_none() {
        let full_scan_started = Instant::now();
        let out = run_full_scan(inputs, tx, total_files, &mut emit)?;
        emit(
            "full_scan_finished",
            json!({
                "duration_ms": full_scan_started.elapsed().as_millis(),
                "records": out.len(),
                "sessions": count_sessions(&out),
                "total_files": total_files,
            }),
        );
        emit(
            "indexer_finished",
            json!({
                "duration_ms": indexer_started.elapsed().as_millis(),
                "records": out.len(),
                "sessions": count_sessions(&out),
                "total_files": total_files,
            }),
        );
        tx.send(IndexerEvent::Done { records: out }).ok();
        return Ok(());
    }

    let mut records_count = 0usize;
    let mut sessions_count = 0usize;

    let mut cache_store = match cache_path {
        Some(path) => {
            let cache_open_started = Instant::now();
            match cache::CacheStore::open(&path, rebuild_index) {
                Ok(mut store) => {
                    let cache_bytes = fs::metadata(&path).ok().map(|metadata| metadata.len());
                    emit(
                        "cache_open_finished",
                        json!({
                            "path": path.display().to_string(),
                            "duration_ms": cache_open_started.elapsed().as_millis(),
                            "rebuild_index": rebuild_index,
                            "cache_bytes": cache_bytes,
                        }),
                    );

                    let prune_started = Instant::now();
                    let removed = store.prune_missing_units()?;
                    emit(
                        "cache_prune_finished",
                        json!({
                            "path": path.display().to_string(),
                            "duration_ms": prune_started.elapsed().as_millis(),
                            "removed_units": removed,
                        }),
                    );
                    if removed > 0 {
                        tx.send(IndexerEvent::Warn {
                            message: format!("removed {removed} stale cached sources"),
                        })
                        .ok();
                    }

                    let cache_load_started = Instant::now();
                    let cached_records = store.load_records(&unit_keys)?;
                    emit(
                        "cache_load_finished",
                        json!({
                            "path": path.display().to_string(),
                            "duration_ms": cache_load_started.elapsed().as_millis(),
                            "records": cached_records.len(),
                            "sessions": count_sessions(&cached_records),
                            "requested_units": unit_keys.len(),
                        }),
                    );
                    if !cached_records.is_empty() {
                        records_count = cached_records.len();
                        sessions_count = count_sessions(&cached_records);
                        tx.send(IndexerEvent::Loaded {
                            records: cached_records,
                        })
                        .ok();
                    }
                    Some(store)
                }
                Err(err) => {
                    emit(
                        "cache_open_failed",
                        json!({
                            "path": path.display().to_string(),
                            "duration_ms": cache_open_started.elapsed().as_millis(),
                            "error": format!("{err:#}"),
                        }),
                    );
                    tx.send(IndexerEvent::Warn {
                        message: format!("cache unavailable, falling back to full scan: {err:#}"),
                    })
                    .ok();
                    None
                }
            }
        }
        None => None,
    };
    if cache_store.is_none() {
        let full_scan_started = Instant::now();
        let out = run_full_scan(inputs, tx, total_files, &mut emit)?;
        emit(
            "full_scan_finished",
            json!({
                "duration_ms": full_scan_started.elapsed().as_millis(),
                "records": out.len(),
                "sessions": count_sessions(&out),
                "total_files": total_files,
                "reason": "cache_unavailable",
            }),
        );
        emit(
            "indexer_finished",
            json!({
                "duration_ms": indexer_started.elapsed().as_millis(),
                "records": out.len(),
                "sessions": count_sessions(&out),
                "total_files": total_files,
            }),
        );
        tx.send(IndexerEvent::Done { records: out }).ok();
        return Ok(());
    }

    let fingerprint_started = Instant::now();
    let cached_fingerprints = if let Some(store) = cache_store.as_ref() {
        store.fingerprints_for_keys(&unit_keys)?
    } else {
        Default::default()
    };
    emit(
        "fingerprint_scan_finished",
        json!({
            "duration_ms": fingerprint_started.elapsed().as_millis(),
            "total_units": unit_keys.len(),
            "cached_units": cached_fingerprints.len(),
        }),
    );
    let mut cache_changed = false;
    let mut processed_files = 0usize;
    let mut opencode_jobs: Vec<(usize, IndexInput)> = Vec::new();
    let mut opencode_meta: Vec<Option<(String, PathBuf, String)>> = vec![None; total_files];
    let mut skipped_units = 0usize;
    let mut refreshed_units = 0usize;
    let mut refreshed_jsonl_units = 0usize;
    let mut refreshed_opencode_units = 0usize;
    let mut failed_units = 0usize;
    let refresh_started = Instant::now();

    for (idx, input) in inputs.iter().cloned().enumerate() {
        let current = input.path().to_path_buf();
        let unit_key = cache::unit_key(&current);
        let fingerprint = fingerprint_for_input(&input)?;
        let source_kind = input.telemetry_source_kind();

        if cached_fingerprints.get(&unit_key) == Some(&fingerprint) {
            skipped_units = skipped_units.saturating_add(1);
            processed_files = processed_files.saturating_add(1);
            tx.send(IndexerEvent::Progress {
                processed_files,
                total_files,
                records: records_count,
                sessions: sessions_count,
                current,
            })
            .ok();
            continue;
        }

        if matches!(input, IndexInput::OpenCodeSession { .. }) {
            opencode_meta[idx] = Some((unit_key, current.clone(), fingerprint));
            opencode_jobs.push((idx, input));
            continue;
        }

        match index_input_to_chunk(input) {
            Ok(chunk) => {
                let current = chunk.current.clone();
                let records_added = chunk.records.len();
                let sessions_added = chunk.sessions.len();
                let cache_write_started = Instant::now();
                if let Some(store) = cache_store.as_mut() {
                    store.replace_unit(&unit_key, &current, &fingerprint, &chunk.records)?;
                }
                refreshed_units = refreshed_units.saturating_add(1);
                refreshed_jsonl_units = refreshed_jsonl_units.saturating_add(1);
                cache_changed = true;
                processed_files = processed_files.saturating_add(1);
                emit(
                    "unit_reindexed",
                    unit_reindexed_payload(
                        &current,
                        records_added,
                        sessions_added,
                        cache_write_started.elapsed().as_millis(),
                        &chunk.telemetry,
                    ),
                );
                tx.send(IndexerEvent::Progress {
                    processed_files,
                    total_files,
                    records: records_count,
                    sessions: sessions_count,
                    current,
                })
                .ok();
            }
            Err((current, e)) => {
                failed_units = failed_units.saturating_add(1);
                processed_files = processed_files.saturating_add(1);
                emit(
                    "unit_failed",
                    json!({
                        "source": source_kind,
                        "path": current.display().to_string(),
                        "error": e.to_string(),
                    }),
                );
                tx.send(IndexerEvent::Warn {
                    message: format!("読み取り失敗: {}: {e}", current.display()),
                })
                .ok();
                tx.send(IndexerEvent::Progress {
                    processed_files,
                    total_files,
                    records: records_count,
                    sessions: sessions_count,
                    current,
                })
                .ok();
            }
        }
    }

    if !opencode_jobs.is_empty() {
        let job_count = opencode_jobs.len();
        let (rx, handles) = spawn_opencode_index_workers(opencode_jobs);
        for _ in 0..job_count {
            let Ok(result) = rx.recv() else {
                break;
            };
            match result {
                IndexedInputResult::Indexed { index, chunk } => {
                    let Some((unit_key, current, fingerprint)) = opencode_meta[index].take() else {
                        continue;
                    };
                    let records_added = chunk.records.len();
                    let sessions_added = chunk.sessions.len();
                    let cache_write_started = Instant::now();
                    if let Some(store) = cache_store.as_mut() {
                        store.replace_unit(&unit_key, &current, &fingerprint, &chunk.records)?;
                    }
                    refreshed_units = refreshed_units.saturating_add(1);
                    refreshed_opencode_units = refreshed_opencode_units.saturating_add(1);
                    cache_changed = true;
                    processed_files = processed_files.saturating_add(1);
                    emit(
                        "unit_reindexed",
                        unit_reindexed_payload(
                            &current,
                            records_added,
                            sessions_added,
                            cache_write_started.elapsed().as_millis(),
                            &chunk.telemetry,
                        ),
                    );
                    tx.send(IndexerEvent::Progress {
                        processed_files,
                        total_files,
                        records: records_count,
                        sessions: sessions_count,
                        current,
                    })
                    .ok();
                }
                IndexedInputResult::Failed {
                    index,
                    current,
                    error,
                } => {
                    let _ = opencode_meta[index].take();
                    failed_units = failed_units.saturating_add(1);
                    processed_files = processed_files.saturating_add(1);
                    emit(
                        "unit_failed",
                        json!({
                            "source": "opencode",
                            "path": current.display().to_string(),
                            "error": error,
                        }),
                    );
                    tx.send(IndexerEvent::Warn {
                        message: format!("読み取り失敗: {}: {error}", current.display()),
                    })
                    .ok();
                    tx.send(IndexerEvent::Progress {
                        processed_files,
                        total_files,
                        records: records_count,
                        sessions: sessions_count,
                        current,
                    })
                    .ok();
                }
            }
        }
        for handle in handles {
            let _ = handle.join();
        }
    }

    emit(
        "refresh_finished",
        json!({
            "duration_ms": refresh_started.elapsed().as_millis(),
            "total_units": total_files,
            "skipped_units": skipped_units,
            "refreshed_units": refreshed_units,
            "refreshed_jsonl_units": refreshed_jsonl_units,
            "refreshed_opencode_units": refreshed_opencode_units,
            "failed_units": failed_units,
            "cache_changed": cache_changed,
        }),
    );

    let final_cache_load_started = Instant::now();
    let final_records = match cache_store.as_ref() {
        Some(store) => store.load_records(&unit_keys)?,
        None => Vec::new(),
    };
    let final_sessions = count_sessions(&final_records);
    emit(
        "cache_reload_finished",
        json!({
            "duration_ms": final_cache_load_started.elapsed().as_millis(),
            "records": final_records.len(),
            "sessions": final_sessions,
            "cache_bytes": cache_path_string
                .as_ref()
                .and_then(|path| fs::metadata(Path::new(path)).ok().map(|metadata| metadata.len())),
        }),
    );
    if cache_changed || records_count != final_records.len() {
        records_count = final_records.len();
        sessions_count = final_sessions;
        tx.send(IndexerEvent::Progress {
            processed_files: total_files,
            total_files,
            records: records_count,
            sessions: sessions_count,
            current: PathBuf::from("<cache>"),
        })
        .ok();
    }

    tx.send(IndexerEvent::Done {
        records: final_records,
    })
    .ok();
    emit(
        "indexer_finished",
        json!({
            "duration_ms": indexer_started.elapsed().as_millis(),
            "records": records_count,
            "sessions": sessions_count,
            "total_files": total_files,
        }),
    );
    Ok(())
}

fn count_sessions(records: &[MessageRecord]) -> usize {
    records
        .iter()
        .filter_map(|record| {
            record
                .session_id
                .as_ref()
                .map(|session_id| (record.source, session_id.clone(), record.account.clone()))
        })
        .collect::<HashSet<_>>()
        .len()
}

fn fingerprint_for_input(input: &IndexInput) -> anyhow::Result<String> {
    match input {
        IndexInput::Jsonl { path, .. } => fingerprint_for_path(path),
        IndexInput::OpenCodeSession { session_file, .. } => {
            fingerprint_for_opencode_session(session_file)
        }
    }
}

fn fingerprint_for_path(path: &Path) -> anyhow::Result<String> {
    let metadata =
        fs::metadata(path).with_context(|| format!("metadata read failed: {}", path.display()))?;
    let modified = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    Ok(format!("{}:{modified}", metadata.len()))
}

fn fingerprint_for_opencode_session(path: &Path) -> anyhow::Result<String> {
    let metadata =
        fs::metadata(path).with_context(|| format!("metadata read failed: {}", path.display()))?;
    let modified = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let session: OpenCodeSession = serde_json::from_reader(File::open(path)?)
        .with_context(|| format!("session fingerprint parse failed: {}", path.display()))?;
    let updated = session
        .time
        .as_ref()
        .and_then(|time| time.updated.or(time.created))
        .unwrap_or_default();
    Ok(format!(
        "{}:{modified}:{}:{updated}:{}:{}",
        metadata.len(),
        session.id,
        session.directory.as_deref().unwrap_or(""),
        session.title.as_deref().unwrap_or("")
    ))
}

fn run_full_scan(
    inputs: Vec<IndexInput>,
    tx: &mpsc::Sender<IndexerEvent>,
    total_files: usize,
    emit: &mut impl FnMut(&str, Value),
) -> anyhow::Result<Vec<MessageRecord>> {
    let mut indexed_inputs: Vec<Option<IndexedInputChunk>> = Vec::with_capacity(total_files);
    indexed_inputs.resize_with(total_files, || None);
    let mut sessions: HashSet<(SourceKind, String, Option<String>)> = HashSet::new();
    let mut records = 0usize;
    let mut processed_files = 0usize;
    let mut opencode_jobs: Vec<(usize, IndexInput)> = Vec::new();

    for (idx, input) in inputs.into_iter().enumerate() {
        if matches!(input, IndexInput::OpenCodeSession { .. }) {
            opencode_jobs.push((idx, input));
            continue;
        }

        match index_input_to_chunk(input) {
            Ok(chunk) => {
                records = records.saturating_add(chunk.records.len());
                sessions.extend(chunk.sessions.iter().cloned());
                let current = chunk.current.clone();
                emit(
                    "unit_reindexed",
                    unit_reindexed_payload(
                        &current,
                        chunk.records.len(),
                        chunk.sessions.len(),
                        0,
                        &chunk.telemetry,
                    ),
                );
                indexed_inputs[idx] = Some(chunk);
                processed_files = processed_files.saturating_add(1);
                tx.send(IndexerEvent::Progress {
                    processed_files,
                    total_files,
                    records,
                    sessions: sessions.len(),
                    current,
                })
                .ok();
            }
            Err((current, e)) => {
                emit(
                    "unit_failed",
                    json!({
                        "source": "jsonl",
                        "path": current.display().to_string(),
                        "error": e.to_string(),
                    }),
                );
                processed_files = processed_files.saturating_add(1);
                tx.send(IndexerEvent::Warn {
                    message: format!("読み取り失敗: {}: {e}", current.display()),
                })
                .ok();
                tx.send(IndexerEvent::Progress {
                    processed_files,
                    total_files,
                    records,
                    sessions: sessions.len(),
                    current,
                })
                .ok();
            }
        }
    }

    if !opencode_jobs.is_empty() {
        let job_count = opencode_jobs.len();
        let (rx, handles) = spawn_opencode_index_workers(opencode_jobs);
        for _ in 0..job_count {
            let Ok(result) = rx.recv() else {
                break;
            };
            match result {
                IndexedInputResult::Indexed { index, chunk } => {
                    records = records.saturating_add(chunk.records.len());
                    sessions.extend(chunk.sessions.iter().cloned());
                    let current = chunk.current.clone();
                    emit(
                        "unit_reindexed",
                        unit_reindexed_payload(
                            &current,
                            chunk.records.len(),
                            chunk.sessions.len(),
                            0,
                            &chunk.telemetry,
                        ),
                    );
                    indexed_inputs[index] = Some(chunk);
                    processed_files = processed_files.saturating_add(1);
                    tx.send(IndexerEvent::Progress {
                        processed_files,
                        total_files,
                        records,
                        sessions: sessions.len(),
                        current,
                    })
                    .ok();
                }
                IndexedInputResult::Failed { current, error, .. } => {
                    emit(
                        "unit_failed",
                        json!({
                            "source": "opencode",
                            "path": current.display().to_string(),
                            "error": error,
                        }),
                    );
                    processed_files = processed_files.saturating_add(1);
                    tx.send(IndexerEvent::Warn {
                        message: format!("読み取り失敗: {}: {error}", current.display()),
                    })
                    .ok();
                    tx.send(IndexerEvent::Progress {
                        processed_files,
                        total_files,
                        records,
                        sessions: sessions.len(),
                        current,
                    })
                    .ok();
                }
            }
        }
        for handle in handles {
            let _ = handle.join();
        }
    }

    let mut out: Vec<MessageRecord> = Vec::with_capacity(records);
    for chunk in indexed_inputs.into_iter().flatten() {
        out.extend(chunk.records);
    }

    Ok(out)
}

#[derive(Debug)]
struct IndexedInputChunk {
    current: PathBuf,
    records: Vec<MessageRecord>,
    sessions: HashSet<(SourceKind, String, Option<String>)>,
    telemetry: IndexTelemetry,
}

#[derive(Debug)]
enum IndexedInputResult {
    Indexed {
        index: usize,
        chunk: IndexedInputChunk,
    },
    Failed {
        index: usize,
        current: PathBuf,
        error: String,
    },
}

#[derive(Debug, Clone)]
struct IndexTelemetry {
    source: &'static str,
    duration_ms: u128,
    opencode: Option<OpenCodeIndexTelemetry>,
}

#[derive(Debug, Clone, Default)]
struct OpenCodeIndexTelemetry {
    message_files: usize,
    messages_with_text: usize,
    part_files: usize,
    text_parts: usize,
    part_parse_failures: usize,
    used_title_fallback: bool,
}

fn unit_reindexed_payload(
    path: &Path,
    records: usize,
    sessions: usize,
    cache_write_ms: u128,
    telemetry: &IndexTelemetry,
) -> Value {
    let mut payload = json!({
        "source": telemetry.source,
        "path": path.display().to_string(),
        "records": records,
        "sessions": sessions,
        "duration_ms": telemetry.duration_ms,
        "cache_write_ms": cache_write_ms,
    });

    if let Some(opencode) = telemetry.opencode.as_ref() {
        payload["message_files"] = json!(opencode.message_files);
        payload["messages_with_text"] = json!(opencode.messages_with_text);
        payload["part_files"] = json!(opencode.part_files);
        payload["text_parts"] = json!(opencode.text_parts);
        payload["part_parse_failures"] = json!(opencode.part_parse_failures);
        payload["used_title_fallback"] = json!(opencode.used_title_fallback);
    }

    payload
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum IndexInput {
    Jsonl {
        path: PathBuf,
        account: Option<String>,
    },
    OpenCodeSession {
        storage_root: PathBuf,
        session_file: PathBuf,
    },
}

impl IndexInput {
    fn path(&self) -> &Path {
        match self {
            Self::Jsonl { path, .. } => path,
            Self::OpenCodeSession { session_file, .. } => session_file,
        }
    }

    fn telemetry_source_kind(&self) -> &'static str {
        match self {
            Self::Jsonl { .. } => "jsonl",
            Self::OpenCodeSession { .. } => "opencode",
        }
    }
}

fn collect_index_inputs(
    roots: &[JsonlInputRoot],
    extra_files: &[JsonlInputRoot],
    opencode_storage_roots: &[PathBuf],
) -> Vec<IndexInput> {
    let mut out: Vec<IndexInput> = collect_jsonl_files(roots, extra_files)
        .into_iter()
        .map(|input| IndexInput::Jsonl {
            path: input.path,
            account: input.account,
        })
        .collect();
    out.extend(collect_opencode_session_files(opencode_storage_roots));
    out.sort();
    out.dedup();
    out
}

fn collect_jsonl_files(
    roots: &[JsonlInputRoot],
    extra_files: &[JsonlInputRoot],
) -> Vec<JsonlInputRoot> {
    let mut out: Vec<JsonlInputRoot> = Vec::new();

    for root in roots {
        if root.path.is_file() {
            if root.path.extension().and_then(|s| s.to_str()) == Some("jsonl") {
                out.push(root.clone());
            }
            continue;
        }

        for entry in WalkDir::new(&root.path)
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
            out.push(JsonlInputRoot {
                path: path.to_path_buf(),
                account: root.account.clone(),
            });
        }
    }

    for file in extra_files {
        if file.path.is_file() {
            out.push(file.clone());
        }
    }

    out.sort();
    out.dedup();
    out
}

fn collect_opencode_session_files(storage_roots: &[PathBuf]) -> Vec<IndexInput> {
    let mut out: Vec<IndexInput> = Vec::new();

    for storage_root in storage_roots {
        let session_root = storage_root.join("session");
        if !session_root.is_dir() {
            continue;
        }

        for entry in WalkDir::new(&session_root)
            .follow_links(false)
            .into_iter()
            .filter_map(Result::ok)
        {
            if !entry.file_type().is_file() {
                continue;
            }
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            out.push(IndexInput::OpenCodeSession {
                storage_root: storage_root.to_path_buf(),
                session_file: path.to_path_buf(),
            });
        }
    }

    out.sort();
    out.dedup();
    out
}

fn index_input(
    input: IndexInput,
    out: &mut Vec<MessageRecord>,
    sessions: &mut HashSet<(SourceKind, String, Option<String>)>,
) -> anyhow::Result<IndexTelemetry> {
    match input {
        IndexInput::Jsonl { path, account } => {
            let started = Instant::now();
            index_file(&path, account.as_deref(), out, sessions)?;
            Ok(IndexTelemetry {
                source: "jsonl",
                duration_ms: started.elapsed().as_millis(),
                opencode: None,
            })
        }
        IndexInput::OpenCodeSession {
            storage_root,
            session_file,
        } => {
            let started = Instant::now();
            let opencode =
                index_opencode_session_file(&storage_root, &session_file, out, sessions)?;
            Ok(IndexTelemetry {
                source: "opencode",
                duration_ms: started.elapsed().as_millis(),
                opencode: Some(opencode),
            })
        }
    }
}

fn index_input_to_chunk(input: IndexInput) -> Result<IndexedInputChunk, (PathBuf, anyhow::Error)> {
    let current = input.path().to_path_buf();
    let mut records = Vec::new();
    let mut sessions = HashSet::new();
    match index_input(input, &mut records, &mut sessions) {
        Ok(telemetry) => Ok(IndexedInputChunk {
            current,
            records,
            sessions,
            telemetry,
        }),
        Err(e) => Err((current, e)),
    }
}

fn spawn_opencode_index_workers(
    jobs: Vec<(usize, IndexInput)>,
) -> (
    mpsc::Receiver<IndexedInputResult>,
    Vec<thread::JoinHandle<()>>,
) {
    let worker_count = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .min(jobs.len())
        .max(1);
    let queue = Arc::new(Mutex::new(VecDeque::from(jobs)));
    let (tx, rx) = mpsc::channel();
    let mut handles = Vec::with_capacity(worker_count);

    for _ in 0..worker_count {
        let queue = Arc::clone(&queue);
        let tx = tx.clone();
        handles.push(thread::spawn(move || {
            loop {
                let next_job = {
                    let mut queue = queue.lock().expect("opencode work queue poisoned");
                    queue.pop_front()
                };
                let Some((index, input)) = next_job else {
                    break;
                };

                let result = match index_input_to_chunk(input) {
                    Ok(chunk) => IndexedInputResult::Indexed { index, chunk },
                    Err((current, error)) => IndexedInputResult::Failed {
                        index,
                        current,
                        error: format!("{error:#}"),
                    },
                };
                if tx.send(result).is_err() {
                    break;
                }
            }
        }));
    }
    drop(tx);

    (rx, handles)
}

fn index_file(
    file: &Path,
    account: Option<&str>,
    out: &mut Vec<MessageRecord>,
    sessions: &mut HashSet<(SourceKind, String, Option<String>)>,
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

        if let Some(rec) = extract_record(&v, file, line_no, &mut ctx, account) {
            if let Some(sid) = rec.session_id.as_deref() {
                sessions.insert((rec.source, sid.to_string(), rec.account.clone()));
            }
            out.push(rec);
        }
    }

    Ok(())
}

#[derive(Debug, Deserialize)]
struct OpenCodeSessionTime {
    created: Option<i64>,
    updated: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct OpenCodeSession {
    id: String,
    directory: Option<String>,
    title: Option<String>,
    time: Option<OpenCodeSessionTime>,
}

#[derive(Debug, Deserialize)]
struct OpenCodeMessageTime {
    created: Option<i64>,
    completed: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct OpenCodeMessagePath {
    cwd: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OpenCodeMessage {
    id: String,
    #[serde(rename = "sessionID")]
    session_id: String,
    role: String,
    time: Option<OpenCodeMessageTime>,
    path: Option<OpenCodeMessagePath>,
    mode: Option<String>,
    agent: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OpenCodePartTime {
    start: Option<i64>,
    end: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct OpenCodePart {
    #[serde(rename = "type")]
    part_type: String,
    text: Option<String>,
    time: Option<OpenCodePartTime>,
}

fn index_opencode_session_file(
    storage_root: &Path,
    session_file: &Path,
    out: &mut Vec<MessageRecord>,
    sessions: &mut HashSet<(SourceKind, String, Option<String>)>,
) -> anyhow::Result<OpenCodeIndexTelemetry> {
    let mut telemetry = OpenCodeIndexTelemetry::default();
    let session: OpenCodeSession = serde_json::from_reader(File::open(session_file)?)?;
    let message_dir = storage_root.join("message").join(&session.id);
    if !message_dir.is_dir() {
        return Ok(telemetry);
    }

    let session_cwd = session.directory.as_deref().map(str::to_string);
    let fallback_title = session
        .title
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let session_ts = session
        .time
        .as_ref()
        .and_then(|t| t.updated.or(t.created))
        .map(format_epoch_timestamp);

    let mut message_files: Vec<PathBuf> = fs::read_dir(&message_dir)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|s| s.to_str()) == Some("json"))
        .collect();
    message_files.sort();
    telemetry.message_files = message_files.len();

    let mut records_added = 0usize;

    for message_file in message_files {
        let message: OpenCodeMessage = serde_json::from_reader(File::open(&message_file)?)?;
        let part_dir = storage_root.join("part").join(&message.id);
        let part_result = extract_opencode_message_text(&part_dir)?;
        telemetry.part_files = telemetry.part_files.saturating_add(part_result.part_files);
        telemetry.text_parts = telemetry.text_parts.saturating_add(part_result.text_parts);
        telemetry.part_parse_failures = telemetry
            .part_parse_failures
            .saturating_add(part_result.part_parse_failures);

        let Some((text, text_file, timestamp)) = part_result.payload else {
            continue;
        };
        telemetry.messages_with_text = telemetry.messages_with_text.saturating_add(1);

        let role = Role::from_str(&message.role);
        let cwd = message
            .path
            .as_ref()
            .and_then(|p| p.cwd.as_deref())
            .or(session_cwd.as_deref())
            .map(|s| s.to_string());
        let timestamp = timestamp
            .or_else(|| {
                message
                    .time
                    .as_ref()
                    .and_then(|t| t.completed.or(t.created))
                    .map(format_epoch_timestamp)
            })
            .or_else(|| session_ts.clone());
        let phase = message.mode.clone().or(message.agent.clone());

        out.push(MessageRecord {
            timestamp,
            role,
            text,
            file: text_file,
            line: 1,
            session_id: Some(message.session_id.clone()),
            account: None,
            cwd,
            phase,
            images: Vec::new(),
            source: SourceKind::OpenCodeSession,
        });
        records_added = records_added.saturating_add(1);
    }

    if records_added == 0
        && let Some(title) = fallback_title
    {
        telemetry.used_title_fallback = true;
        out.push(MessageRecord {
            timestamp: session_ts,
            role: Role::User,
            text: title.to_string(),
            file: session_file.to_path_buf(),
            line: 1,
            session_id: Some(session.id.clone()),
            account: None,
            cwd: session_cwd,
            phase: Some("title".to_string()),
            images: Vec::new(),
            source: SourceKind::OpenCodeSession,
        });
        records_added = 1;
    }

    if records_added > 0 {
        sessions.insert((SourceKind::OpenCodeSession, session.id, None));
    }

    Ok(telemetry)
}

struct OpenCodeMessageTextResult {
    payload: Option<(String, PathBuf, Option<String>)>,
    part_files: usize,
    text_parts: usize,
    part_parse_failures: usize,
}

fn extract_opencode_message_text(part_dir: &Path) -> anyhow::Result<OpenCodeMessageTextResult> {
    if !part_dir.is_dir() {
        return Ok(OpenCodeMessageTextResult {
            payload: None,
            part_files: 0,
            text_parts: 0,
            part_parse_failures: 0,
        });
    }

    let mut part_files: Vec<PathBuf> = fs::read_dir(part_dir)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|s| s.to_str()) == Some("json"))
        .collect();
    part_files.sort();
    let part_file_count = part_files.len();

    let mut texts: Vec<String> = Vec::new();
    let mut first_text_file: Option<PathBuf> = None;
    let mut last_ts: Option<i64> = None;
    let mut text_parts = 0usize;
    let mut part_parse_failures = 0usize;

    for part_file in part_files {
        let part: OpenCodePart = match serde_json::from_reader(File::open(&part_file)?) {
            Ok(part) => part,
            Err(_) => {
                part_parse_failures = part_parse_failures.saturating_add(1);
                continue;
            }
        };

        if part.part_type != "text" {
            continue;
        }

        let Some(text) = part
            .text
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        else {
            continue;
        };
        if first_text_file.is_none() {
            first_text_file = Some(part_file.clone());
        }
        text_parts = text_parts.saturating_add(1);
        texts.push(text.to_string());

        if let Some(ts) = part.time.as_ref().and_then(|t| t.end.or(t.start)) {
            last_ts = Some(last_ts.map_or(ts, |cur| cur.max(ts)));
        }
    }

    if texts.is_empty() {
        return Ok(OpenCodeMessageTextResult {
            payload: None,
            part_files: part_file_count,
            text_parts,
            part_parse_failures,
        });
    }

    Ok(OpenCodeMessageTextResult {
        payload: Some((
            texts.join("\n"),
            first_text_file.unwrap_or_else(|| part_dir.to_path_buf()),
            last_ts.map(format_epoch_timestamp),
        )),
        part_files: part_file_count,
        text_parts,
        part_parse_failures,
    })
}

fn format_epoch_timestamp(ts: i64) -> String {
    let (secs, _) = match epoch_to_unix_seconds_and_nanos(ts) {
        Some(parts) => parts,
        None => return ts.to_string(),
    };
    format_unix_seconds_rfc3339(secs)
}

fn epoch_to_unix_seconds_and_nanos(ts: i64) -> Option<(i64, u32)> {
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
    let secs = nanos.div_euclid(1_000_000_000);
    let subsec_nanos = nanos.rem_euclid(1_000_000_000) as u32;
    Some((i64::try_from(secs).ok()?, subsec_nanos))
}

fn format_unix_seconds_rfc3339(secs: i64) -> String {
    let days = secs.div_euclid(86_400);
    let secs_of_day = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = secs_of_day / 3_600;
    let minute = (secs_of_day % 3_600) / 60;
    let second = secs_of_day % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
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

fn discover_account_config_dirs(home: &Path, kind: AccountConfigKind) -> Vec<(String, PathBuf)> {
    let prefix = match kind {
        AccountConfigKind::Codex => ".codex-",
        AccountConfigKind::Claude => ".claude-",
    };

    let mut out: Vec<(String, PathBuf)> = Vec::new();
    let Ok(entries) = fs::read_dir(home) else {
        return out;
    };

    for entry in entries.filter_map(Result::ok) {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if !file_type.is_dir() {
            continue;
        }

        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(account) = name.strip_prefix(prefix) else {
            continue;
        };
        let account = account.trim();
        if account.is_empty() {
            continue;
        }

        out.push((account.to_string(), entry.path()));
    }

    out.sort();
    out.dedup();
    out
}

fn extract_record(
    v: &Value,
    file: &Path,
    line: u32,
    ctx: &mut FileContext,
    account: Option<&str>,
) -> Option<MessageRecord> {
    // 1) Codex session jsonl
    if let Some(rec) = extract_codex_session_record(v, file, line, ctx, account) {
        return Some(rec);
    }

    // 2) Claude project jsonl (~/.claude/projects/**.jsonl)
    if let Some(rec) = extract_claude_project_record(v, file, line, account) {
        return Some(rec);
    }

    // 3) Codex history jsonl (~/.codex/history.jsonl)
    extract_codex_history_record(v, file, line, account)
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

fn extract_codex_content_images(payload: &Value) -> Vec<ImageAttachment> {
    let Some(content) = payload.get("content").and_then(|value| value.as_array()) else {
        return Vec::new();
    };

    let mut out = Vec::new();
    for item in content {
        let Some(item_type) = item.get("type").and_then(|value| value.as_str()) else {
            continue;
        };
        match item_type {
            "input_image" => {
                let Some(image_url) = item.get("image_url").and_then(|value| value.as_str()) else {
                    continue;
                };
                if let Some(media_type) = parse_data_url_media_type(image_url) {
                    out.push(ImageAttachment {
                        kind: ImageAttachmentKind::DataUrl {
                            media_type,
                            data_url: image_url.to_string(),
                        },
                        label: item
                            .get("name")
                            .and_then(|value| value.as_str())
                            .map(|value| value.to_string()),
                    });
                }
            }
            "local_image" => {
                let Some(path) = item
                    .get("path")
                    .or_else(|| item.get("local_path"))
                    .and_then(|value| value.as_str())
                else {
                    continue;
                };
                out.push(ImageAttachment {
                    kind: ImageAttachmentKind::LocalPath {
                        path: PathBuf::from(path),
                    },
                    label: item
                        .get("name")
                        .and_then(|value| value.as_str())
                        .map(|value| value.to_string()),
                });
            }
            _ => {}
        }
    }

    out
}

fn parse_data_url_media_type(data_url: &str) -> Option<String> {
    let rest = data_url.strip_prefix("data:")?;
    let (meta, _) = rest.split_once(',')?;
    let media_type = meta.split(';').next()?.trim();
    if media_type.is_empty() {
        return None;
    }
    Some(media_type.to_string())
}

fn extract_codex_session_record(
    v: &Value,
    file: &Path,
    line: u32,
    ctx: &mut FileContext,
    account: Option<&str>,
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
    let images = extract_codex_content_images(payload);

    Some(MessageRecord {
        timestamp,
        role,
        text,
        file: file.to_path_buf(),
        line,
        session_id: ctx.session_id.clone(),
        account: account.map(|s| s.to_string()),
        cwd: ctx.cwd.clone(),
        phase,
        images,
        source: SourceKind::CodexSessionJsonl,
    })
}

fn extract_claude_project_record(
    v: &Value,
    file: &Path,
    line: u32,
    account: Option<&str>,
) -> Option<MessageRecord> {
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
        account: account.map(|s| s.to_string()),
        cwd,
        phase: None,
        images: Vec::new(),
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

fn extract_codex_history_record(
    v: &Value,
    file: &Path,
    line: u32,
    account: Option<&str>,
) -> Option<MessageRecord> {
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
        account: account.map(|s| s.to_string()),
        cwd: None,
        phase: None,
        images: Vec::new(),
        source: SourceKind::CodexHistoryJsonl,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        sync::mpsc,
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

    fn contains_path(entries: &[JsonlInputRoot], path: &Path) -> bool {
        entries.iter().any(|entry| entry.path == path)
    }

    fn recv_events(rx: &mpsc::Receiver<IndexerEvent>) -> Vec<IndexerEvent> {
        let mut out = Vec::new();
        while let Ok(event) = rx.try_recv() {
            out.push(event);
        }
        out
    }

    fn basic_cfg_for_jsonl(path: &Path) -> IndexerConfig {
        IndexerConfig {
            roots: vec![JsonlInputRoot {
                path: path.to_path_buf(),
                account: None,
            }],
            extra_files: Vec::new(),
            opencode_storage_roots: Vec::new(),
        }
    }

    fn basic_cfg_for_opencode(storage: &Path) -> IndexerConfig {
        IndexerConfig {
            roots: Vec::new(),
            extra_files: Vec::new(),
            opencode_storage_roots: vec![storage.to_path_buf()],
        }
    }

    #[test]
    fn extracts_session_message() {
        let line = r#"{"timestamp":"2026-02-11T14:16:44.023Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"了解。"}],"phase":"commentary"}}"#;
        let v: Value = serde_json::from_str(line).unwrap();
        let mut ctx = FileContext::default();
        let rec = extract_record(&v, Path::new("/tmp/x.jsonl"), 12, &mut ctx, None).unwrap();
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
        let rec = extract_record(&v, Path::new("/tmp/x.jsonl"), 1, &mut ctx, None);
        assert!(rec.is_none());
        assert_eq!(ctx.session_id.as_deref(), Some("abc"));
        assert_eq!(ctx.cwd.as_deref(), Some("/home/tizze/x"));
    }

    #[test]
    fn extracts_history_line() {
        let line = r#"{"session_id":"s1","ts":123,"text":"hello"}"#;
        let v: Value = serde_json::from_str(line).unwrap();
        let mut ctx = FileContext::default();
        let rec = extract_record(&v, Path::new("/tmp/h.jsonl"), 99, &mut ctx, None).unwrap();
        assert_eq!(rec.text, "hello");
        assert_eq!(rec.session_id.as_deref(), Some("s1"));
        assert_eq!(rec.timestamp.as_deref(), Some("123"));
    }

    #[test]
    fn extracts_claude_project_message() {
        let line = r#"{"type":"user","cwd":"/x","sessionId":"s2","timestamp":"2026-01-01T00:00:00.000Z","message":{"role":"user","content":"hi"}}"#;
        let v: Value = serde_json::from_str(line).unwrap();
        let mut ctx = FileContext::default();
        let rec = extract_record(&v, Path::new("/tmp/c.jsonl"), 5, &mut ctx, None).unwrap();
        assert_eq!(rec.text, "hi");
        assert_eq!(rec.cwd.as_deref(), Some("/x"));
        assert_eq!(rec.session_id.as_deref(), Some("s2"));
        assert_eq!(rec.source, SourceKind::ClaudeProjectJsonl);
    }

    #[test]
    fn extract_record_preserves_account_namespace() {
        let line = r#"{"type":"user","cwd":"/x","sessionId":"s2","timestamp":"2026-01-01T00:00:00.000Z","message":{"role":"user","content":"hi"}}"#;
        let v: Value = serde_json::from_str(line).unwrap();
        let mut ctx = FileContext::default();
        let rec = extract_record(&v, Path::new("/tmp/c.jsonl"), 5, &mut ctx, Some("work")).unwrap();
        assert_eq!(rec.account.as_deref(), Some("work"));
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

        let files = collect_jsonl_files(
            &[JsonlInputRoot {
                path: root.to_path_buf(),
                account: None,
            }],
            &[],
        );
        assert!(contains_path(&files, &root.join("a.jsonl")));
        assert!(contains_path(&files, &root.join("nested/d.jsonl")));
        assert!(!contains_path(&files, &root.join("subagents/b.jsonl")));
        assert!(!contains_path(
            &files,
            &root.join("nested/subagents/c.jsonl")
        ));
    }

    #[test]
    fn collect_index_inputs_includes_opencode_sessions() {
        let tmp = TempDir::new("agent-history-opencode-inputs");
        let storage = tmp.path.join("storage");
        fs::create_dir_all(storage.join("session/global")).unwrap();
        fs::write(storage.join("session/global/ses_1.json"), "{}").unwrap();

        let inputs = collect_index_inputs(&[], &[], &[storage]);
        assert_eq!(inputs.len(), 1);
        match &inputs[0] {
            IndexInput::OpenCodeSession {
                storage_root,
                session_file,
            } => {
                assert!(storage_root.ends_with("storage"));
                assert!(session_file.ends_with("ses_1.json"));
            }
            other => panic!("unexpected input: {other:?}"),
        }
    }

    #[test]
    fn indexes_opencode_session_text_parts() {
        let tmp = TempDir::new("agent-history-opencode");
        let storage = tmp.path.join("storage");
        let sid = "ses_demo";
        let mid = "msg_demo";

        fs::create_dir_all(storage.join("session/global")).unwrap();
        fs::create_dir_all(storage.join(format!("message/{sid}"))).unwrap();
        fs::create_dir_all(storage.join(format!("part/{mid}"))).unwrap();

        fs::write(
            storage.join("session/global/ses_demo.json"),
            r#"{
  "id": "ses_demo",
  "directory": "/tmp/project",
  "title": "demo title",
  "time": { "created": 100, "updated": 300 }
}"#,
        )
        .unwrap();

        fs::write(
            storage.join(format!("message/{sid}/{mid}.json")),
            r#"{
  "id": "msg_demo",
  "sessionID": "ses_demo",
  "role": "assistant",
  "time": { "created": 120, "completed": 250 },
  "path": { "cwd": "/tmp/project" },
  "mode": "orchestrator"
}"#,
        )
        .unwrap();

        fs::write(
            storage.join(format!("part/{mid}/prt_1.json")),
            r#"{
  "type": "reasoning",
  "text": "ignored"
}"#,
        )
        .unwrap();
        fs::write(
            storage.join(format!("part/{mid}/prt_2.json")),
            r#"{
  "type": "text",
  "text": "hello from opencode",
  "time": { "start": 200, "end": 220 }
}"#,
        )
        .unwrap();

        let mut out = Vec::new();
        let mut sessions = HashSet::new();
        let telemetry = index_opencode_session_file(
            &storage,
            &storage.join("session/global/ses_demo.json"),
            &mut out,
            &mut sessions,
        )
        .unwrap();

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].source, SourceKind::OpenCodeSession);
        assert_eq!(out[0].role, Role::Assistant);
        assert_eq!(out[0].text, "hello from opencode");
        assert_eq!(out[0].cwd.as_deref(), Some("/tmp/project"));
        assert_eq!(out[0].session_id.as_deref(), Some("ses_demo"));
        assert_eq!(out[0].timestamp.as_deref(), Some("1970-01-01T00:03:40Z"));
        assert_eq!(out[0].phase.as_deref(), Some("orchestrator"));
        assert_eq!(telemetry.message_files, 1);
        assert_eq!(telemetry.messages_with_text, 1);
        assert_eq!(telemetry.part_files, 2);
        assert_eq!(telemetry.text_parts, 1);
        assert!(!telemetry.used_title_fallback);
        assert!(sessions.contains(&(SourceKind::OpenCodeSession, "ses_demo".to_string(), None)));
    }

    #[test]
    fn indexes_opencode_session_title_when_no_text_parts_exist() {
        let tmp = TempDir::new("agent-history-opencode-title");
        let storage = tmp.path.join("storage");
        let sid = "ses_demo";
        let mid = "msg_demo";

        fs::create_dir_all(storage.join("session/global")).unwrap();
        fs::create_dir_all(storage.join(format!("message/{sid}"))).unwrap();
        fs::create_dir_all(storage.join(format!("part/{mid}"))).unwrap();

        fs::write(
            storage.join("session/global/ses_demo.json"),
            r#"{
  "id": "ses_demo",
  "directory": "/tmp/project",
  "title": "fallback title",
  "time": { "updated": 300 }
}"#,
        )
        .unwrap();
        fs::write(
            storage.join(format!("message/{sid}/{mid}.json")),
            r#"{
  "id": "msg_demo",
  "sessionID": "ses_demo",
  "role": "assistant",
  "time": { "completed": 250 }
}"#,
        )
        .unwrap();
        fs::write(
            storage.join(format!("part/{mid}/prt_1.json")),
            r#"{
  "type": "patch"
}"#,
        )
        .unwrap();

        let mut out = Vec::new();
        let mut sessions = HashSet::new();
        let telemetry = index_opencode_session_file(
            &storage,
            &storage.join("session/global/ses_demo.json"),
            &mut out,
            &mut sessions,
        )
        .unwrap();

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].text, "fallback title");
        assert_eq!(out[0].role, Role::User);
        assert_eq!(out[0].file, storage.join("session/global/ses_demo.json"));
        assert_eq!(out[0].phase.as_deref(), Some("title"));
        assert_eq!(telemetry.message_files, 1);
        assert_eq!(telemetry.messages_with_text, 0);
        assert_eq!(telemetry.part_files, 1);
        assert_eq!(telemetry.text_parts, 0);
        assert!(telemetry.used_title_fallback);
    }

    #[test]
    fn discover_account_config_dirs_finds_suffix_profiles_with_sanity_checks() {
        let tmp = TempDir::new("agent-history-account-dirs");
        let home = &tmp.path;

        fs::create_dir_all(home.join(".codex-work")).unwrap();
        fs::create_dir_all(home.join(".claude-personal")).unwrap();
        fs::write(home.join(".codex-bad"), "not a dir").unwrap();
        fs::create_dir_all(home.join(".codex-")).unwrap();

        let codex = discover_account_config_dirs(home, AccountConfigKind::Codex);
        let claude = discover_account_config_dirs(home, AccountConfigKind::Claude);

        assert!(codex.contains(&("work".to_string(), home.join(".codex-work"))));
        assert!(!codex.iter().any(|(account, _)| account.is_empty()));
        assert!(claude.contains(&("personal".to_string(), home.join(".claude-personal"))));
    }

    #[test]
    fn format_epoch_timestamp_formats_seconds_and_millis_as_rfc3339() {
        assert_eq!(format_epoch_timestamp(220), "1970-01-01T00:03:40Z");
        assert_eq!(
            format_epoch_timestamp(1_704_067_200_000),
            "2024-01-01T00:00:00Z"
        );
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

    #[test]
    fn run_indexer_loads_cached_records_on_second_run() {
        let tmp = TempDir::new("agent-history-cache-jsonl");
        let jsonl = tmp.path.join("history.jsonl");
        let cache_db = tmp.path.join("index.sqlite");

        fs::write(
            &jsonl,
            concat!(
                "{\"timestamp\":\"2026-02-11T12:05:47.856Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"abc\",\"cwd\":\"/tmp/demo\"}}\n",
                "{\"timestamp\":\"2026-02-11T12:06:47.856Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"hello cache\"}]}}\n"
            ),
        )
        .unwrap();

        let cfg = basic_cfg_for_jsonl(&jsonl);

        let (tx1, rx1) = mpsc::channel();
        run_indexer(cfg.clone(), &tx1, Some(cache_db.clone()), false, None).unwrap();
        let first_events = recv_events(&rx1);
        assert!(
            first_events
                .iter()
                .any(|event| matches!(event, IndexerEvent::Done { .. }))
        );
        assert!(
            !first_events
                .iter()
                .any(|event| matches!(event, IndexerEvent::Loaded { .. }))
        );

        let (tx2, rx2) = mpsc::channel();
        run_indexer(cfg, &tx2, Some(cache_db), false, None).unwrap();
        let second_events = recv_events(&rx2);
        assert!(
            second_events.iter().any(
                |event| matches!(event, IndexerEvent::Loaded { records } if records.len() == 1)
            )
        );
        assert!(
            second_events
                .iter()
                .any(|event| matches!(event, IndexerEvent::Done { records } if records.len() == 1))
        );
    }

    #[test]
    fn run_indexer_writes_telemetry_log_with_cache_metrics() {
        let tmp = TempDir::new("agent-history-telemetry");
        let jsonl = tmp.path.join("history.jsonl");
        let cache_db = tmp.path.join("index.sqlite");
        let telemetry_log = tmp.path.join("events.jsonl");

        fs::write(
            &jsonl,
            concat!(
                "{\"timestamp\":\"2026-02-11T12:05:47.856Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"abc\",\"cwd\":\"/tmp/demo\"}}\n",
                "{\"timestamp\":\"2026-02-11T12:06:47.856Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"telemetry hello\"}]}}\n"
            ),
        )
        .unwrap();

        let cfg = basic_cfg_for_jsonl(&jsonl);
        let (tx, rx) = mpsc::channel();
        run_indexer(cfg, &tx, Some(cache_db), false, Some(telemetry_log.clone())).unwrap();
        let _ = recv_events(&rx);

        let body = fs::read_to_string(&telemetry_log).unwrap();
        let rows: Vec<serde_json::Value> = body
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let kinds: Vec<&str> = rows
            .iter()
            .filter_map(|row| row.get("kind").and_then(|value| value.as_str()))
            .collect();

        assert!(kinds.contains(&"indexer_started"));
        assert!(kinds.contains(&"cache_open_finished"));
        assert!(kinds.contains(&"cache_load_finished"));
        assert!(kinds.contains(&"fingerprint_scan_finished"));
        assert!(kinds.contains(&"refresh_finished"));
        assert!(kinds.contains(&"cache_reload_finished"));
        assert!(kinds.contains(&"indexer_finished"));

        let refresh = rows
            .iter()
            .find(|row| {
                row.get("kind").and_then(|value| value.as_str()) == Some("refresh_finished")
            })
            .unwrap();
        assert_eq!(refresh["data"]["refreshed_units"].as_u64(), Some(1));
        assert_eq!(refresh["data"]["failed_units"].as_u64(), Some(0));
    }

    #[test]
    fn run_indexer_skips_unchanged_opencode_session_children() {
        let tmp = TempDir::new("agent-history-cache-opencode");
        let storage = tmp.path.join("storage");
        let cache_db = tmp.path.join("index.sqlite");
        let sid = "ses_demo";
        let mid = "msg_demo";

        fs::create_dir_all(storage.join("session/global")).unwrap();
        fs::create_dir_all(storage.join(format!("message/{sid}"))).unwrap();
        fs::create_dir_all(storage.join(format!("part/{mid}"))).unwrap();

        fs::write(
            storage.join("session/global/ses_demo.json"),
            r#"{
  "id": "ses_demo",
  "directory": "/tmp/project",
  "title": "demo title",
  "time": { "created": 100, "updated": 300 }
}"#,
        )
        .unwrap();

        fs::write(
            storage.join(format!("message/{sid}/{mid}.json")),
            r#"{
  "id": "msg_demo",
  "sessionID": "ses_demo",
  "role": "assistant",
  "time": { "created": 120, "completed": 250 }
}"#,
        )
        .unwrap();

        let part_file = storage.join(format!("part/{mid}/prt_1.json"));
        fs::write(
            &part_file,
            r#"{
  "type": "text",
  "text": "hello from cache",
  "time": { "start": 200, "end": 220 }
}"#,
        )
        .unwrap();

        let cfg = basic_cfg_for_opencode(&storage);
        let (tx1, rx1) = mpsc::channel();
        run_indexer(cfg.clone(), &tx1, Some(cache_db.clone()), false, None).unwrap();
        let first_events = recv_events(&rx1);
        assert!(first_events.iter().any(|event| matches!(event, IndexerEvent::Done { records } if records.iter().any(|record| record.text == "hello from cache"))));

        fs::write(&part_file, "{not valid json").unwrap();

        let (tx2, rx2) = mpsc::channel();
        run_indexer(cfg, &tx2, Some(cache_db), false, None).unwrap();
        let second_events = recv_events(&rx2);
        assert!(second_events.iter().any(|event| matches!(event, IndexerEvent::Loaded { records } if records.iter().any(|record| record.text == "hello from cache"))));
        assert!(second_events.iter().any(|event| matches!(event, IndexerEvent::Done { records } if records.iter().any(|record| record.text == "hello from cache"))));
    }

    #[test]
    fn run_indexer_rebuild_index_ignores_existing_cache_load() {
        let tmp = TempDir::new("agent-history-cache-rebuild");
        let jsonl = tmp.path.join("history.jsonl");
        let cache_db = tmp.path.join("index.sqlite");

        fs::write(
            &jsonl,
            concat!(
                "{\"timestamp\":\"2026-02-11T12:05:47.856Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"abc\",\"cwd\":\"/tmp/demo\"}}\n",
                "{\"timestamp\":\"2026-02-11T12:06:47.856Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"hello rebuild\"}]}}\n"
            ),
        )
        .unwrap();

        let cfg = basic_cfg_for_jsonl(&jsonl);

        let (tx1, rx1) = mpsc::channel();
        run_indexer(cfg.clone(), &tx1, Some(cache_db.clone()), false, None).unwrap();
        let _ = recv_events(&rx1);

        let (tx2, rx2) = mpsc::channel();
        run_indexer(cfg, &tx2, Some(cache_db), true, None).unwrap();
        let events = recv_events(&rx2);
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, IndexerEvent::Loaded { .. }))
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, IndexerEvent::Done { records } if records.len() == 1))
        );
    }

    #[test]
    fn run_indexer_falls_back_to_full_scan_when_cache_open_fails() {
        let tmp = TempDir::new("agent-history-cache-fallback");
        let jsonl = tmp.path.join("history.jsonl");
        let bad_cache_path = tmp.path.join("not-a-db");

        fs::write(
            &jsonl,
            concat!(
                "{\"timestamp\":\"2026-02-11T12:05:47.856Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"abc\",\"cwd\":\"/tmp/demo\"}}\n",
                "{\"timestamp\":\"2026-02-11T12:06:47.856Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"hello fallback\"}]}}\n"
            ),
        )
        .unwrap();
        fs::create_dir_all(&bad_cache_path).unwrap();

        let cfg = basic_cfg_for_jsonl(&jsonl);
        let (tx, rx) = mpsc::channel();
        run_indexer(cfg, &tx, Some(bad_cache_path), false, None).unwrap();
        let events = recv_events(&rx);

        assert!(events.iter().any(
            |event| matches!(event, IndexerEvent::Warn { message } if message.contains("cache unavailable"))
        ));
        assert!(events.iter().any(
            |event| matches!(event, IndexerEvent::Done { records } if records.iter().any(|record| record.text == "hello fallback"))
        ));
    }

    #[test]
    fn run_indexer_refreshes_changed_jsonl_cache_contents() {
        let tmp = TempDir::new("agent-history-cache-refresh-jsonl");
        let jsonl = tmp.path.join("history.jsonl");
        let cache_db = tmp.path.join("index.sqlite");

        fs::write(
            &jsonl,
            concat!(
                "{\"timestamp\":\"2026-02-11T12:05:47.856Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"abc\",\"cwd\":\"/tmp/demo\"}}\n",
                "{\"timestamp\":\"2026-02-11T12:06:47.856Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"before change\"}]}}\n"
            ),
        )
        .unwrap();

        let cfg = basic_cfg_for_jsonl(&jsonl);
        let (tx1, rx1) = mpsc::channel();
        run_indexer(cfg.clone(), &tx1, Some(cache_db.clone()), false, None).unwrap();
        let _ = recv_events(&rx1);

        std::thread::sleep(std::time::Duration::from_millis(5));
        fs::write(
            &jsonl,
            concat!(
                "{\"timestamp\":\"2026-02-11T12:05:47.856Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"abc\",\"cwd\":\"/tmp/demo\"}}\n",
                "{\"timestamp\":\"2026-02-11T12:06:47.856Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"after change\"}]}}\n"
            ),
        )
        .unwrap();

        let (tx2, rx2) = mpsc::channel();
        run_indexer(cfg, &tx2, Some(cache_db), false, None).unwrap();
        let events = recv_events(&rx2);

        assert!(events.iter().any(
            |event| matches!(event, IndexerEvent::Loaded { records } if records.iter().any(|record| record.text == "before change"))
        ));
        assert!(events.iter().any(
            |event| matches!(event, IndexerEvent::Done { records } if records.iter().any(|record| record.text == "after change"))
        ));
    }

    #[test]
    fn run_indexer_refreshes_changed_opencode_session_contents() {
        let tmp = TempDir::new("agent-history-cache-refresh-opencode");
        let storage = tmp.path.join("storage");
        let cache_db = tmp.path.join("index.sqlite");
        let sid = "ses_demo";
        let mid = "msg_demo";
        let session_file = storage.join("session/global/ses_demo.json");
        let part_file = storage.join(format!("part/{mid}/prt_1.json"));

        fs::create_dir_all(storage.join("session/global")).unwrap();
        fs::create_dir_all(storage.join(format!("message/{sid}"))).unwrap();
        fs::create_dir_all(storage.join(format!("part/{mid}"))).unwrap();

        fs::write(
            &session_file,
            r#"{
  "id": "ses_demo",
  "directory": "/tmp/project",
  "title": "demo title",
  "time": { "created": 100, "updated": 300 }
}"#,
        )
        .unwrap();
        fs::write(
            storage.join(format!("message/{sid}/{mid}.json")),
            r#"{
  "id": "msg_demo",
  "sessionID": "ses_demo",
  "role": "assistant",
  "time": { "created": 120, "completed": 250 }
}"#,
        )
        .unwrap();
        fs::write(
            &part_file,
            r#"{
  "type": "text",
  "text": "before opencode refresh",
  "time": { "start": 200, "end": 220 }
}"#,
        )
        .unwrap();

        let cfg = basic_cfg_for_opencode(&storage);
        let (tx1, rx1) = mpsc::channel();
        run_indexer(cfg.clone(), &tx1, Some(cache_db.clone()), false, None).unwrap();
        let _ = recv_events(&rx1);

        std::thread::sleep(std::time::Duration::from_millis(5));
        fs::write(
            &part_file,
            r#"{
  "type": "text",
  "text": "after opencode refresh",
  "time": { "start": 200, "end": 220 }
}"#,
        )
        .unwrap();
        fs::write(
            &session_file,
            r#"{
  "id": "ses_demo",
  "directory": "/tmp/project",
  "title": "demo title",
  "time": { "created": 100, "updated": 301 }
}"#,
        )
        .unwrap();

        let (tx2, rx2) = mpsc::channel();
        run_indexer(cfg, &tx2, Some(cache_db), false, None).unwrap();
        let events = recv_events(&rx2);

        assert!(events.iter().any(
            |event| matches!(event, IndexerEvent::Loaded { records } if records.iter().any(|record| record.text == "before opencode refresh"))
        ));
        assert!(events.iter().any(
            |event| matches!(event, IndexerEvent::Done { records } if records.iter().any(|record| record.text == "after opencode refresh"))
        ));
    }

    #[test]
    fn run_indexer_prunes_deleted_jsonl_source_from_cache() {
        let tmp = TempDir::new("agent-history-cache-delete-jsonl");
        let jsonl = tmp.path.join("history.jsonl");
        let cache_db = tmp.path.join("index.sqlite");

        fs::write(
            &jsonl,
            concat!(
                "{\"timestamp\":\"2026-02-11T12:05:47.856Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"abc\",\"cwd\":\"/tmp/demo\"}}\n",
                "{\"timestamp\":\"2026-02-11T12:06:47.856Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"to be deleted\"}]}}\n"
            ),
        )
        .unwrap();

        let cfg = basic_cfg_for_jsonl(&jsonl);
        let (tx1, rx1) = mpsc::channel();
        run_indexer(cfg.clone(), &tx1, Some(cache_db.clone()), false, None).unwrap();
        let _ = recv_events(&rx1);

        fs::remove_file(&jsonl).unwrap();

        let (tx2, rx2) = mpsc::channel();
        run_indexer(cfg, &tx2, Some(cache_db), false, None).unwrap();
        let events = recv_events(&rx2);

        assert!(events.iter().any(
            |event| matches!(event, IndexerEvent::Warn { message } if message.contains("stale cached sources"))
        ));
        assert!(
            events
                .iter()
                .any(|event| matches!(event, IndexerEvent::Done { records } if records.is_empty()))
        );
    }
}
