# agent-history
Codex / Claude の会話履歴を、プロジェクト横断で全文検索できるTUIツール。

- Codex: `~/.codex/sessions/**.jsonl` / `~/.codex/archived_sessions/**.jsonl`
- Claude: `~/.claude/projects/**.jsonl`
- 追加: `$HOME` 配下の `**/.codex/{sessions,archived_sessions}` と `**/.codex/history.jsonl` を自動検出（`.git`, `node_modules` などは除外）

## インストール
```bash
# リポジトリ内で
cargo install --path .
```

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
結果一覧は `日時(最終更新) ディレクトリ名 C|O 最初の発言(1行目)`。

- `C`: Claude
- `O`: OpenAI/Codex

※ Codexのログは先頭に `AGENTS.md` や `<environment_context>` が入ることがあるので、一覧の「最初の発言」からは除外します。  
（それしか無いセッションは一覧に出しません）  
※ `subagents/` 配下のログは量が多くノイズになりやすいので、デフォルトでは探索対象から除外します。

### オプション
```bash
# 追加の検索ルート（複数可）
agent-history --root /path/to/dir

# デフォルトルートを無効化して任意ルートだけ検索
agent-history --no-default-roots --root /path/to/dir

# 追加で ~/.codex/history.jsonl も取り込む（簡易テキスト）
agent-history --history

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
- `Enter`: 選択中の会話を `codex resume` / `claude --resume` で開く（CLIが必要）
- `Ctrl+o`: 選択中の該当ファイル周辺を `$PAGER`（未指定なら `less -R`）で開く
- `Esc` / `Ctrl+c`: 終了

## セキュリティ
脆弱性の報告は `SECURITY.md` を参照してください。

## ライセンス
本ソフトウェアはデュアルライセンスです。

- Apache License 2.0 (`LICENSE-APACHE`)
- MIT License (`LICENSE-MIT`)
