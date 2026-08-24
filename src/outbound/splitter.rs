//! Code-fence-aware long markdown splitter (Go `outbound/splitter.go` port).

/// Split a long markdown string into chunks under `limit` chars,
/// preserving code block integrity — fences are closed and reopened at boundaries.
pub fn split_with_code_fences(text: &str, limit: usize) -> Vec<String> {
    if limit == 0 || text.chars().count() <= limit {
        return vec![text.to_string()];
    }
    let lines: Vec<&str> = text.split('\n').collect();
    let mut out: Vec<String> = Vec::new();
    let mut buf: Vec<String> = Vec::new();
    let mut buf_len = 0usize;
    let mut fence_lang: Option<String> = None;

    macro_rules! flush {
        () => {
            if !buf.is_empty() {
                let mut chunk = buf.join("\n");
                if fence_lang.is_some() {
                    chunk.push_str("\n```");
                }
                out.push(chunk);
                buf.clear();
                buf_len = 0;
                if let Some(lang) = &fence_lang {
                    let reopen = format!("```{lang}");
                    buf_len = reopen.len();
                    buf.push(reopen);
                }
            }
        };
    }

    for line in &lines {
        let fence = parse_fence(line);
        let mut line_len = line.len();
        if !buf.is_empty() {
            line_len += 1;
        }
        let is_heading = is_heading_line(line);
        let near_full = (buf_len as f64) > (limit as f64) * 0.75;

        if buf_len + line_len > limit || (is_heading && near_full && !buf.is_empty()) {
            flush!();
        }
        buf.push((*line).to_string());
        buf_len += line_len;

        match fence {
            FenceParse::Open(lang) => fence_lang = Some(lang),
            FenceParse::Close => fence_lang = None,
            FenceParse::None => {}
        }
    }
    flush!();
    #[allow(unused_assignments)]
    let _ = buf_len;
    out
}

enum FenceParse {
    Open(String),
    Close,
    None,
}

/// Parse ```` ```lang ```` lines (Go regex `^```(\w*)$` semantics).
fn parse_fence(line: &str) -> FenceParse {
    if let Some(rest) = line.strip_prefix("```") {
        // Only word-char (or empty) suffixes match the Go pattern.
        let is_word = rest.chars().all(|c| c.is_alphanumeric() || c == '_');
        if is_word {
            if rest.is_empty() {
                return FenceParse::Close;
            }
            return FenceParse::Open(rest.to_string());
        }
        return FenceParse::None;
    }
    FenceParse::None
}

fn is_heading_line(line: &str) -> bool {
    let t = line.trim_start_matches([' ', '\t']);
    let hashes = t.chars().take_while(|c| *c == '#').count();
    (1..=6).contains(&hashes) && t[hashes..].starts_with(' ')
}

/// Rune-based newline-boundary splitter for plain text (Go `chunkText` fallback).
pub(crate) fn chunk_text(s: &str, limit: usize) -> Vec<String> {
    let total = s.chars().count();
    if limit == 0 || total <= limit {
        return vec![s.to_string()];
    }
    if s.contains("```") {
        return split_with_code_fences(s, limit);
    }
    let runes: Vec<char> = s.chars().collect();
    let mut chunks: Vec<String> = Vec::new();
    let mut pos = 0usize;
    while pos < runes.len() {
        if runes.len() - pos <= limit {
            chunks.push(runes[pos..].iter().collect());
            break;
        }
        let mut cut = 0usize;
        // Search backwards from `limit` down to `limit/2` for a newline boundary.
        for i in (limit / 2..=limit).rev() {
            if runes[i] == '\n' {
                cut = i + 1;
                break;
            }
        }
        if cut == 0 {
            cut = limit;
        }
        chunks.push(runes[pos..pos + cut].iter().collect());
        pos += cut;
    }
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_short_text_single() {
        assert_eq!(chunk_text("hello", 100), vec!["hello"]);
    }

    #[test]
    fn chunk_splits_and_keeps_content() {
        let s = "a\nb\nc\nd";
        let got = chunk_text(s, 4);
        // No content lost: concatenation equals the original.
        let joined: String = got.concat();
        assert_eq!(joined, s);
        assert!(got.iter().all(|c| c.chars().count() <= 4));
    }

    #[test]
    fn splitter_reopens_fences() {
        let s = "intro\n```rust\nfn a(){}\nfn b(){}\n```\ntail text here\nmore tail text";
        let parts = split_with_code_fences(s, 12);
        assert!(parts.len() >= 2);
        // Every chunk must have balanced fences.
        for p in &parts {
            let opens = p.matches("```").count();
            // opens includes reopens; each chunk should contain an even number of fence markers
            assert_eq!(opens % 2, 0, "unbalanced fences in {p:?}");
        }
        // Content preserved across chunks (modulo reopened fence markers).
        let stripped: String = parts
            .iter()
            .map(|p| p.replace("```rust\n", "").replace("\n```", ""))
            .collect::<Vec<_>>()
            .join("");
        assert!(stripped.contains("intro"));
        assert!(stripped.contains("tail"));
    }
}
