# agent-history
Codex / Claude / OpenCode の会話履歴を、プロジェクト横断で全文検索できるTUIツール。

## このforkでの大きな変更
upstream の `origin/main` と比べると、このforkはかなり大きく拡張されています。

- OpenCode の履歴インデックス化と resume 対応を追加。
- OpenCode セッションのインデックス作成を並列化し、大きなストレージでも更新を速くしました。
- 検索結果を「ただセッションが出るだけ」から改善し、マッチした断片、マッチしたメッセージのプレビュー、実際のヒット位置を中心にした pager 表示に対応。
- 結果一覧とプレビューの両方でクエリハイライトを追加。
- プレビューを大幅強化し、キーボードスクロール、マウスホイールスクロール、左右ペインでのホイール振り分け、折り返し行を考慮したスクロール制御、複数ヒットの連続表示に対応。
- 巨大セッションでも状況を把握しやすいように、複数マッチの表示件数やクエリ出現回数の集計を出すよう改善。
- 結果一覧の表示形式と provider 表示を見直し、一覧性を向上。
- `~/.codex-work` や `~/.claude-work` のようなアカウント別ディレクトリを自動検出し、別 namespace として扱い、`codex-account <name>` / `claude-account <name>` で resume できるように拡張。
- 永続 SQLite キャッシュを追加し、差分更新・削除済みソースの prune・fingerprint による再利用で、毎回のフルスキャンを避けるようにしました。
- キャッシュ制御用に `--no-cache` と `--rebuild-index` を追加しました。
- インデックス処理とキャッシュ処理のライフサイクルを記録する JSONL telemetry を追加し、無効化フラグと出力先指定フラグも追加しました。
- Codex の画像添付を抽出してインデックスに保持し、プレビューに出せるようにしました。

要するに、もはや upstream の最初の検索TUIのままではなく、複数の agent 実装と複数アカウントを横断して扱えるローカル履歴ブラウザに近いものになっています。

- Codex: `~/.codex/sessions/**.jsonl` / `~/.codex/archived_sessions/**.jsonl`
- Codexアカウント: `~/.codex-<account>/{sessions,archived_sessions}/**.jsonl`
- Claude: `~/.claude/projects/**.jsonl`
- Claudeアカウント: `~/.claude-<account>/projects/**.jsonl`
- OpenCode: `~/.local/share/opencode/storage/{session,message,part}`
- 追加の場所を読みたい場合は `--root` で明示的に指定してください

## インストール
```bash
# リポジトリ内で
cargo install --path .
```

## 必要なもの
- Rust toolchain（ソースからビルド/インストールする場合）
- 任意: `codex` CLI（`Enter`でOpenAI/Codexセッションをresumeする場合）
- 任意: `codex-account` ラッパー（`~/.codex-work` のようなアカウント別Codexセッションを `Enter` でresumeする場合）
- 任意: `claude` CLI（`Enter`でClaudeセッションをresumeする場合）
- 任意: `claude-account` ラッパー（`~/.claude-work` のようなアカウント別Claudeセッションを `Enter` でresumeする場合）
- 任意: `opencode` CLI（`Enter`でOpenCodeセッションをresumeする場合）
- 任意: `$PAGER`（未指定なら`less -R`。`Ctrl+o`で使用）

※ `fzf` のような外部fuzzy finderは不要です。

## アーキテクチャメモ
- 永続キャッシュのデフォルト保存先は `~/.local/state/agent-history/index.sqlite` です。
- telemetry のデフォルト保存先は `~/.local/state/agent-history/events.jsonl` です。
- `AGENT_HISTORY_CACHE_DB` でキャッシュDBの保存先を上書きできます。
- `AGENT_HISTORY_TELEMETRY_LOG` で telemetry ログの保存先を上書きできます。
- キャッシュ対象の各ソースは fingerprint で判定し、変化していないファイル/セッションは再利用します。
- 消えたソースはキャッシュから自動で prune されます。
- OpenCode セッションの再インデックスは並列実行されます。
- Codex の画像添付はプレビューでパスとして表示され、埋め込み画像データは必要時にテンポラリディレクトリへ展開されます。

## 使い方
```bash
agent-history
```

起動直後からクエリ入力できます（インデックス作成は裏で進み、進捗が表示されます）。

開発中なら:
```bash
cargo run --release
```

### 表示
結果一覧は `日時(最終更新) C|O|OC[ account] [ディレクトリ名] 最初の発言(1行目)`。

- `C`: Claude
- `O`: OpenAI/Codex
- `OC`: OpenCode
- `~/.claude-work` や `~/.codex-work` のようなアカウント別ホームディレクトリは `C work` / `O work` のように表示します
- セッションのディレクトリ名は `[instrumenter]` のように角括弧で表示します

※ Codexのログは先頭に `AGENTS.md` や `<environment_context>` が入ることがあるので、一覧の「最初の発言」からは除外します。  
（それしか無いセッションは一覧に出しません）  
※ `subagents/` 配下のログは量が多くノイズになりやすいので、デフォルトでは探索対象から除外します。
※ マッチした Codex レコードに画像添付がある場合、プレビューにはそのファイルパスを表示します。

### オプション
```bash
# 追加の検索ルート（複数可）
agent-history --root /path/to/dir

# デフォルトルートを無効化して任意ルートだけ検索
agent-history --no-default-roots --root /path/to/dir

# 追加で ~/.codex/history.jsonl も取り込む（簡易テキスト）
agent-history --history

# 永続キャッシュを使わず毎回フルスキャン
agent-history --no-cache

# 永続キャッシュを破棄して再構築
agent-history --rebuild-index

# telemetry JSONL の出力先を指定
agent-history --telemetry-log /tmp/agent-history-events.jsonl

# telemetry ログを無効化
agent-history --no-telemetry

# 起動時クエリ
agent-history --query "DMARC"

# 検索はスペース区切りがAND（全部含む）
agent-history --query "cloud run"

# Smartcase: 大文字を含むトークンだけ大小区別
agent-history --query "GitHub Actions"
```

## キー操作
- 文字入力: クエリ更新（即時検索）
- `Backspace`: 1文字削除
- `Ctrl+u`: クエリ全消し
- `↑/↓`: 選択移動
- `PageUp/PageDown`: ページ移動
- `Enter`: 選択中の会話を `codex resume` / `claude --resume` / `opencode --session`、またはアカウント別セッションなら `codex-account <name>` / `claude-account <name>` で開く
- `Ctrl+o`: 選択中の該当ファイル周辺を `$PAGER`（未指定なら `less -R`）で開く
- `Esc` / `Ctrl+c`: 終了

## セキュリティ
脆弱性の報告は `SECURITY.md` を参照してください。

## ライセンス
本ソフトウェアはデュアルライセンスです。

- Apache License 2.0 (`LICENSE-APACHE`)
- MIT License (`LICENSE-MIT`)
