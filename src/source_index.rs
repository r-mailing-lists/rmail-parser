use crate::source_page::decode_entities;

/// One line of a period's index page at the source.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexEntry {
    /// The page number as the source writes it, leading zeros included.
    pub number: String,
    pub subject: String,
    pub author: String,
}

/// Reads the entries of a `date.html` index page, sorted by page number as a
/// number. The page lists them by date, which is not always the same order.
pub fn parse_index(html: &str) -> Vec<IndexEntry> {
    let mut entries = parse_index_by_date(html);
    entries.sort_by_key(|e| e.number.parse::<u64>().unwrap_or(u64::MAX));
    entries
}

/// Reads the entries of a `date.html` index page in the order it lists them,
/// which is by date.
pub fn parse_index_by_date(html: &str) -> Vec<IndexEntry> {
    const ENTRY: &str = "<LI><A HREF=\"";
    let mut entries = Vec::new();
    let mut rest = html;
    while let Some(at) = rest.find(ENTRY) {
        rest = &rest[at + ENTRY.len()..];
        if let Some(entry) = parse_entry(rest) {
            entries.push(entry);
        }
    }
    entries
}

/// Reads one entry from the text that follows `<LI><A HREF="`.
fn parse_entry(s: &str) -> Option<IndexEntry> {
    let (href, s) = s.split_once('"')?;
    let number = href.strip_suffix(".html")?;
    if number.is_empty() || !number.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let (_, s) = s.split_once('>')?;
    let (subject, s) = s.split_once("</A>")?;
    let (_, s) = s.split_once("<I>")?;
    let (author, _) = s.split_once("</I>")?;
    Some(IndexEntry {
        number: number.to_string(),
        subject: decode_entities(subject.trim()),
        author: decode_entities(author.trim()),
    })
}

/// Reduces a subject to what the index page and the text file agree on. The
/// two escape and wrap a subject differently, and the text file has `?` where
/// the page has the real character, so only ASCII letters and digits are kept,
/// after reply prefixes and bracketed list tags are taken off the front.
pub fn subject_key(subject: &str) -> String {
    let decoded = decode_entities(subject);
    let mut rest = decoded.trim_start();
    loop {
        let lower = rest.to_ascii_lowercase();
        let prefix = ["re:", "fwd:", "fw:"]
            .iter()
            .find(|p| lower.starts_with(**p));
        if let Some(prefix) = prefix {
            rest = rest[prefix.len()..].trim_start();
        } else if rest.starts_with('[') {
            match rest.find(']') {
                Some(end) => rest = rest[end + 1..].trim_start(),
                None => break,
            }
        } else {
            break;
        }
    }
    rest.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_lowercase())
        .collect()
}
