use crate::indexer::MessageRecord;

#[derive(Debug, Clone)]
pub struct CompiledQuery {
    tokens: Vec<Token>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TermMatchRange {
    pub start: usize,
    pub end: usize,
    pub term_index: usize,
}

#[derive(Debug, Clone)]
struct Token {
    raw: String,
    // Some: ASCII case-insensitive when the needle contains ASCII letters but no uppercase.
    // None: regular contains matching, which stays case-sensitive or non-ASCII aware.
    lower_ascii: Option<Vec<u8>>,
}

impl CompiledQuery {
    pub fn new(query: &str) -> Self {
        let query = query.trim();
        if query.is_empty() {
            return Self { tokens: vec![] };
        }

        let parts = parse_query_parts(query);
        let mut tokens: Vec<Token> = Vec::new();
        for t in &parts {
            if t.is_empty() {
                continue;
            }

            let bytes = t.as_bytes();
            let has_upper = bytes.iter().any(|&b| b.is_ascii_uppercase());
            let has_ascii_alpha = bytes.iter().any(|&b| b.is_ascii_alphabetic());

            let lower_ascii = if !has_upper && has_ascii_alpha {
                let mut lower: Vec<u8> = Vec::with_capacity(bytes.len());
                for &b in bytes {
                    lower.push(b.to_ascii_lowercase());
                }
                Some(lower)
            } else {
                None
            };

            tokens.push(Token {
                raw: t.to_string(),
                lower_ascii,
            });
        }

        Self { tokens }
    }

    pub fn matches_record(&self, rec: &MessageRecord) -> bool {
        if self.tokens.is_empty() {
            return true;
        }
        for token in &self.tokens {
            if !record_matches_token(rec, token) {
                return false;
            }
        }
        true
    }
}

pub fn find_match_ranges(query: &str, haystack: &str) -> Vec<(usize, usize)> {
    let compiled = CompiledQuery::new(query);
    if compiled.tokens.is_empty() {
        return vec![];
    }

    let mut ranges: Vec<(usize, usize)> = Vec::new();
    for token in &compiled.tokens {
        ranges.extend(find_token_ranges(haystack, token));
    }

    if ranges.is_empty() {
        return ranges;
    }

    ranges.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    let mut merged: Vec<(usize, usize)> = Vec::with_capacity(ranges.len());

    for (start, end) in ranges {
        if let Some((_, cur_end)) = merged.last_mut()
            && start <= *cur_end
        {
            *cur_end = (*cur_end).max(end);
            continue;
        }
        merged.push((start, end));
    }

    merged
}

pub fn find_term_match_ranges(query: &str, haystack: &str) -> Vec<TermMatchRange> {
    let compiled = CompiledQuery::new(query);
    if compiled.tokens.is_empty() {
        return vec![];
    }

    let mut ranges: Vec<TermMatchRange> = Vec::new();
    for (term_index, token) in compiled.tokens.iter().enumerate() {
        ranges.extend(
            find_token_ranges(haystack, token)
                .into_iter()
                .map(|(start, end)| TermMatchRange {
                    start,
                    end,
                    term_index,
                }),
        );
    }

    if ranges.is_empty() {
        return ranges;
    }

    ranges.sort_unstable_by(|a, b| {
        a.start
            .cmp(&b.start)
            .then(b.end.cmp(&a.end))
            .then(a.term_index.cmp(&b.term_index))
    });

    let mut resolved: Vec<TermMatchRange> = Vec::with_capacity(ranges.len());
    let mut covered_until = 0usize;
    for mut range in ranges {
        if range.end <= covered_until {
            continue;
        }
        if range.start < covered_until {
            range.start = covered_until;
        }
        covered_until = range.end;
        resolved.push(range);
    }

    resolved
}

fn record_matches_token(rec: &MessageRecord, token: &Token) -> bool {
    if contains_token(&rec.text, token) {
        return true;
    }
    if let Some(account) = rec.account.as_deref()
        && contains_token(account, token)
    {
        return true;
    }
    if let Some(cwd) = rec.cwd.as_deref()
        && contains_token(cwd, token)
    {
        return true;
    }
    if let Some(session_id) = rec.session_id.as_deref()
        && contains_token(session_id, token)
    {
        return true;
    }
    if contains_token(&rec.machine_name, token) || contains_token(&rec.machine_id, token) {
        return true;
    }
    if let Some(project_slug) = rec.project_slug.as_deref()
        && contains_token(project_slug, token)
    {
        return true;
    }
    if contains_token(&rec.origin, token) {
        return true;
    }
    let path = rec.file.to_string_lossy();
    if contains_token(&path, token) {
        return true;
    }
    false
}

fn find_token_ranges(haystack: &str, token: &Token) -> Vec<(usize, usize)> {
    if token.raw.is_empty() {
        return vec![];
    }

    match token.lower_ascii.as_deref() {
        Some(lower) => find_ascii_case_insensitive_ranges(haystack.as_bytes(), lower),
        None => haystack
            .match_indices(&token.raw)
            .map(|(start, matched)| (start, start + matched.len()))
            .collect(),
    }
}

fn contains_token(haystack: &str, token: &Token) -> bool {
    if token.raw.is_empty() {
        return true;
    }

    match token.lower_ascii.as_deref() {
        Some(lower) => contains_ascii_case_insensitive_bytes(haystack.as_bytes(), lower),
        None => haystack.contains(&token.raw),
    }
}

fn contains_ascii_case_insensitive_bytes(haystack: &[u8], needle_lower: &[u8]) -> bool {
    let n = needle_lower;
    if n.is_empty() {
        return true;
    }
    let h = haystack;
    if n.len() > h.len() {
        return false;
    }

    for i in 0..=h.len() - n.len() {
        if h[i].to_ascii_lowercase() != n[0] {
            continue;
        }
        let mut ok = true;
        for j in 1..n.len() {
            if h[i + j].to_ascii_lowercase() != n[j] {
                ok = false;
                break;
            }
        }
        if ok {
            return true;
        }
    }
    false
}

/// Parse a query string into parts, respecting double-quoted phrases.
/// `"hello world" foo` → ["hello world", "foo"]
fn parse_query_parts(query: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut chars = query.chars().peekable();
    while let Some(&ch) = chars.peek() {
        if ch.is_whitespace() {
            chars.next();
            continue;
        }
        if ch == '"' {
            chars.next(); // consume opening quote
            let mut phrase = String::new();
            loop {
                match chars.next() {
                    Some('"') | None => break,
                    Some(c) => phrase.push(c),
                }
            }
            if !phrase.is_empty() {
                parts.push(phrase);
            }
        } else {
            let mut word = String::new();
            while let Some(&c) = chars.peek() {
                if c.is_whitespace() {
                    break;
                }
                word.push(c);
                chars.next();
            }
            if !word.is_empty() {
                parts.push(word);
            }
        }
    }
    parts
}

fn find_ascii_case_insensitive_ranges(haystack: &[u8], needle_lower: &[u8]) -> Vec<(usize, usize)> {
    let n = needle_lower;
    if n.is_empty() || n.len() > haystack.len() {
        return vec![];
    }

    let mut ranges = Vec::new();
    for i in 0..=haystack.len() - n.len() {
        if haystack[i].to_ascii_lowercase() != n[0] {
            continue;
        }
        let mut ok = true;
        for j in 1..n.len() {
            if haystack[i + j].to_ascii_lowercase() != n[j] {
                ok = false;
                break;
            }
        }
        if ok {
            ranges.push((i, i + n.len()));
        }
    }
    ranges
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexer::{Role, SourceKind};
    use std::path::PathBuf;

    fn rec(text: &str) -> MessageRecord {
        MessageRecord {
            timestamp: Some("2026-01-01T00:00:00.000Z".to_string()),
            role: Role::User,
            text: text.to_string(),
            file: PathBuf::from("/tmp/x.jsonl"),
            line: 1,
            session_id: Some("s1".to_string()),
            account: None,
            cwd: Some("/home/tizze".to_string()),
            phase: None,
            images: Vec::new(),
            machine_id: "local".to_string(),
            machine_name: "local".to_string(),
            project_slug: Some("tizze".to_string()),
            origin: "local".to_string(),
            source: SourceKind::CodexSessionJsonl,
        }
    }

    #[test]
    fn smartcase_lower_is_case_insensitive() {
        let r = rec("Hello World");
        assert!(CompiledQuery::new("hello").matches_record(&r));
    }

    #[test]
    fn smartcase_upper_is_case_sensitive() {
        let r = rec("Hello World");
        assert!(!CompiledQuery::new("WORLD").matches_record(&r));
        assert!(CompiledQuery::new("World").matches_record(&r));
    }

    #[test]
    fn tokens_are_and() {
        let r = rec("abc def ghi");
        assert!(CompiledQuery::new("abc ghi").matches_record(&r));
        assert!(!CompiledQuery::new("abc xyz").matches_record(&r));
    }

    #[test]
    fn compiled_query_matches_as_expected() {
        let r = rec("Hello World");
        assert!(CompiledQuery::new("hello").matches_record(&r));
    }

    #[test]
    fn find_match_ranges_finds_ascii_case_insensitive_matches() {
        assert_eq!(
            find_match_ranges("hello", "Hello there hello"),
            vec![(0, 5), (12, 17)]
        );
    }

    #[test]
    fn find_match_ranges_merges_overlapping_matches() {
        assert_eq!(find_match_ranges("ell llo", "Hello"), vec![(1, 5)]);
    }

    #[test]
    fn quoted_phrase_matches_exact_sequence() {
        let r = rec("abc def ghi");
        assert!(CompiledQuery::new("\"abc def\"").matches_record(&r));
        assert!(!CompiledQuery::new("\"abc ghi\"").matches_record(&r));
    }

    #[test]
    fn quoted_phrase_with_other_tokens() {
        let r = rec("abc def ghi");
        assert!(CompiledQuery::new("\"abc def\" ghi").matches_record(&r));
        assert!(!CompiledQuery::new("\"abc def\" xyz").matches_record(&r));
    }

    #[test]
    fn quoted_phrase_highlight_ranges() {
        assert_eq!(
            find_match_ranges("\"hello world\"", "say hello world now"),
            vec![(4, 15)]
        );
    }

    #[test]
    fn parse_query_parts_handles_mixed_input() {
        assert_eq!(
            parse_query_parts("foo \"hello world\" bar"),
            vec!["foo", "hello world", "bar"]
        );
    }

    #[test]
    fn parse_query_parts_unclosed_quote() {
        assert_eq!(
            parse_query_parts("\"unclosed phrase"),
            vec!["unclosed phrase"]
        );
    }

    #[test]
    fn find_term_match_ranges_keeps_distinct_terms_separate() {
        assert_eq!(
            find_term_match_ranges("hello world", "hello world"),
            vec![
                TermMatchRange {
                    start: 0,
                    end: 5,
                    term_index: 0
                },
                TermMatchRange {
                    start: 6,
                    end: 11,
                    term_index: 1
                }
            ]
        );
    }

    #[test]
    fn find_term_match_ranges_clips_overlapping_terms() {
        assert_eq!(
            find_term_match_ranges("ell llo", "Hello"),
            vec![
                TermMatchRange {
                    start: 1,
                    end: 4,
                    term_index: 0
                },
                TermMatchRange {
                    start: 4,
                    end: 5,
                    term_index: 1
                }
            ]
        );
    }
}
