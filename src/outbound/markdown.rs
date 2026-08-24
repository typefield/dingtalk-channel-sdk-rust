//! Markdown normalization for DingTalk AI-card renderer (SPEC §7 / E10),
//! ported from the official connector.
//!
//! Rules: single `\n` → `<br>` outside code blocks; keep `\n` inside code
//! fences; keep `\n` before block-syntax lines (lists/tables/headings/rules);
//! merge consecutive quote lines joined by `<br>`; insert a blank line before
//! table divider rows when missing.

fn crlf_normalize(text: &str) -> String {
    text.replace("\r\n", "\n").replace('\r', "\n")
}

/// `^\s*\|?\s*:?-+:?\s*(\|?\s*:?-+:?\s*)+\|?\s*$`
fn is_table_divider(line: &str) -> bool {
    if line.is_empty() || !line.contains('|') {
        return false;
    }
    let t = line.trim();
    let bytes = t.as_bytes();
    let mut i = 0;
    // optional leading |
    if i < bytes.len() && bytes[i] == b'|' {
        i += 1;
    }
    // skip spaces
    while i < bytes.len() && (bytes[i] == b' ' || bytes[i] == b'\t') {
        i += 1;
    }
    // segments: :?-+ separated by | with optional spaces
    let mut segments = 0;
    loop {
        // optional leading ':'
        if i < bytes.len() && bytes[i] == b':' {
            i += 1;
        }
        let start = i;
        while i < bytes.len() && bytes[i] == b'-' {
            i += 1;
        }
        if i == start {
            return false; // need at least one '-'
        }
        if i < bytes.len() && bytes[i] == b':' {
            i += 1;
        }
        segments += 1;
        // skip trailing spaces
        while i < bytes.len() && (bytes[i] == b' ' || bytes[i] == b'\t') {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        if bytes[i] == b'|' {
            i += 1;
            while i < bytes.len() && (bytes[i] == b' ' || bytes[i] == b'\t') {
                i += 1;
            }
            continue;
        }
        return false;
    }
    segments >= 2
}

/// `^\s*\|?.*\|.*\|?\s*$` — a row that looks like a table row.
fn is_table_row(line: &str) -> bool {
    let t = line.trim();
    if !t.contains('|') {
        return false;
    }
    // count at least one '|' inside after optional leading '|'
    let stripped = t.strip_prefix('|').unwrap_or(t);
    stripped.contains('|')
}

/// `^\s{0,3}(```)` — code fence.
fn is_fence(line: &str) -> bool {
    let t = line.trim_start_matches([' ', '\t']);
    t.starts_with("```") && line.len() - line.trim_start().len() <= 3
}

/// Block-start syntax: list item, table cell, heading, horizontal rule.
fn is_block_start(line: &str) -> bool {
    let indent = line.len() - line.trim_start_matches([' ', '\t']).len();
    if indent > 3 {
        return false;
    }
    let t = &line[indent..];
    // list: - * + or digit+. / digit+)
    if t.starts_with("- ") || t.starts_with("* ") || t.starts_with("+ ") {
        return true;
    }
    for (idx, ch) in t.char_indices() {
        if ch.is_ascii_digit() {
            continue;
        } else if (ch == '.' || ch == ')') && idx > 0 {
            let rest = &t[idx + 1..];
            return rest.starts_with(' ');
        } else {
            break;
        }
    }
    // table cell
    if t.starts_with('|') {
        return true;
    }
    // heading
    let hashes = t.chars().take_while(|c| *c == '#').count();
    if (1..=6).contains(&hashes) && t[hashes..].starts_with(' ') {
        return true;
    }
    // horizontal rule: --- *** ___ (with optional spaces between)
    let compact: String = t.chars().filter(|c| !c.is_whitespace()).collect();
    if compact.len() >= 3 {
        let first = compact.chars().next().unwrap();
        if (first == '-' || first == '*' || first == '_') && compact.chars().all(|c| c == first) {
            return true;
        }
    }
    false
}

/// `^\s{0,3}>\s?` — quote prefix.
fn quote_prefix_len(line: &str) -> Option<usize> {
    let indent = line.len() - line.trim_start_matches([' ', '\t']).len();
    if indent > 3 {
        return None;
    }
    let rest = &line[indent..];
    if let Some(r) = rest.strip_prefix('>') {
        let extra = r
            .chars()
            .next()
            .map(|c| if c == ' ' { c.len_utf8() } else { 0 })
            .unwrap_or(0);
        return Some(indent + 1 + extra);
    }
    None
}

/// Insert a blank line before a table divider whose previous non-divider line is not blank/table.
pub(crate) fn ensure_table_blank_lines(text: &str) -> String {
    let normalized = crlf_normalize(text);
    let lines: Vec<&str> = normalized.split('\n').collect();
    let mut out: Vec<&str> = Vec::with_capacity(lines.len() + 4);
    for (i, cur) in lines.iter().enumerate() {
        let next = lines.get(i + 1).copied().unwrap_or("");
        if i > 0
            && is_table_row(cur)
            && is_table_divider(next)
            && !lines[i - 1].trim().is_empty()
            && !is_table_row(lines[i - 1])
        {
            out.push("");
        }
        out.push(cur);
    }
    out.join("\n")
}

/// Single `\n` → `<br>` per the card renderer conventions, honoring code fences,
/// quotes and block-syntax lines.
pub(crate) fn fix_newlines(text: &str) -> String {
    let normalized = crlf_normalize(text);
    let lines: Vec<&str> = normalized.split('\n').collect();

    // 1. Merge consecutive quote lines (outside code blocks): strip `>` prefixes, join with <br>.
    let mut merged: Vec<String> = Vec::with_capacity(lines.len());
    let mut pending: Vec<String> = Vec::new();
    let mut in_code = false;

    macro_rules! flush_pending {
        () => {
            if !pending.is_empty() {
                merged.push(pending.join("<br>"));
                pending.clear();
            }
        };
    }

    for line in &lines {
        let fence = is_fence(line);
        if in_code {
            flush_pending!();
            merged.push((*line).to_string());
            if fence {
                in_code = false;
            }
            continue;
        }
        if fence {
            flush_pending!();
            merged.push((*line).to_string());
            in_code = true;
            continue;
        }
        if let Some(plen) = quote_prefix_len(line) {
            if pending.is_empty() {
                pending.push((*line).to_string());
            } else {
                pending.push(line[plen.min(line.len())..].to_string());
            }
        } else {
            flush_pending!();
            merged.push((*line).to_string());
        }
    }
    flush_pending!();

    // 2. Choose separators: keep \n inside code blocks and before block-syntax lines, else <br>.
    let mut sb = String::new();
    let mut in_code = false;
    for (i, cur) in merged.iter().enumerate() {
        let next_in_code = if is_fence(cur) { !in_code } else { in_code };
        if i < merged.len() - 1 {
            let next = &merged[i + 1];
            let keep_nl = next_in_code
                || cur.is_empty()
                || next.is_empty()
                || is_fence(next)
                || is_block_start(next);
            sb.push_str(cur);
            if keep_nl {
                sb.push('\n');
            } else {
                sb.push_str("<br>");
            }
        } else {
            sb.push_str(cur);
        }
        in_code = next_in_code;
    }
    sb
}

/// Normalize markdown for the AI-card renderer.
pub fn normalize_for_card(content: &str) -> String {
    fix_newlines(&ensure_table_blank_lines(content))
}
