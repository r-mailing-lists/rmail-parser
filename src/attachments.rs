use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::OnceLock;

use anyhow::{Context, Result};
use encoding_rs::WINDOWS_1252;
use flate2::read::GzDecoder;
use flate2::{Compression, GzBuilder};
use regex::Regex;
use serde::{Deserialize, Serialize};

/// The first line of the notice Mailman leaves where it scrubbed a text.
pub const NOTICE: &str = "An embedded and charset-unspecified text was scrubbed...";

/// One notice found in a message body.
#[derive(Debug, Clone, PartialEq)]
pub struct Scrubbed {
    /// Byte offset of the notice line.
    pub start: usize,
    /// Byte offset of the end of the URL line, before its line end.
    pub end: usize,
    /// The URL as written.
    pub url: String,
    /// The part of the URL after `/attachments/`, which names the file.
    pub key: String,
}

/// What was fetched for one URL: the text, or why there is none.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredFile {
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encoding: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// `"not found"` or `"too large"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

/// Everything fetched for one raw file, by key. A `BTreeMap` so the keys are
/// written sorted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PeriodAttachments {
    pub period: String,
    pub files: BTreeMap<String, StoredFile>,
}

fn notice_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        let url = r"https?://[^\s<>]*?/attachments/(\d{8}/[0-9a-f]{8}/attachment(?:-\d+)?\.[A-Za-z0-9]+)";
        Regex::new(&format!(
            r"(?m)^{notice}\r?\nName: not available\r?\n(?:URL: <(?P<new>{url})>|Url: (?P<old>{url}))[ \t]*\r?$",
            notice = regex::escape(NOTICE),
            url = url,
        ))
        .expect("notice regex")
    })
}

fn address_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"([-+,.\w]+)@([-+.\w]+)").expect("address regex"))
}

/// Writes every `local@domain` as `local at domain`, the way Mailman's
/// archiver writes addresses in the text it keeps. The scrubbed files skip
/// that step, so restored text goes through here. The rule is applied again
/// until the text stops changing, so a chain such as `a@b@c` comes out as
/// `a at b at c` in one call. Text without such an `@` comes back unchanged,
/// and a second call changes nothing.
pub fn obscure_addresses(text: &str) -> String {
    let mut text = text.to_string();
    // Each pass that changes anything takes out at least one `@`, so this
    // ends.
    while let Cow::Owned(next) = address_regex().replace_all(&text, "${1} at ${2}") {
        text = next;
    }
    text
}

/// `text` with every `\r\n`, and every `\r` on its own, made `\n`, the
/// line end of every other body in the archive.
fn lf_line_ends(text: &str) -> String {
    text.replace("\r\n", "\n").replace('\r', "\n")
}

/// Finds the notices whose part has no name and whose URL points at a file
/// under `/attachments/`. A notice quoted with `>` is not at the start of
/// its line and is left out. Which host the URL belongs to is not checked.
pub fn find_scrubbed(text: &str) -> Vec<Scrubbed> {
    notice_regex()
        .captures_iter(text)
        .map(|c| {
            let whole = c.get(0).expect("whole match");
            let end = whole.end() - usize::from(whole.as_str().ends_with('\r'));
            let url = c.name("new").or_else(|| c.name("old")).expect("url");
            // The key group is the only group inside the URL; take the last
            // one set, which is the one of the form that matched.
            let key = c
                .iter()
                .skip(1)
                .flatten()
                .last()
                .expect("key")
                .as_str()
                .to_string();
            Scrubbed {
                start: whole.start(),
                end,
                url: url.as_str().to_string(),
                key,
            }
        })
        .collect()
}

/// Puts the stored text where each notice is. A notice is replaced when its
/// key is stored with a text that is not blank and the stored URL is the
/// notice's own (http and https alike); the text goes in with its addresses
/// written as `user at domain` (see `obscure_addresses`), its line ends made
/// `\n` and its trailing whitespace trimmed. Everything else stays as it was.
/// Returns the new body and the stored URL of each text put in, as stored
/// and in the order of the body (empty when nothing was replaced). The body
/// is not trimmed here: the caller trims it as `parse_message` trims every
/// body.
pub fn restore(body: &str, store: &PeriodAttachments) -> (String, Vec<String>) {
    let mut out = String::with_capacity(body.len());
    let mut last = 0;
    let mut urls = Vec::new();
    for s in find_scrubbed(body) {
        let file = match store.files.get(&s.key) {
            Some(f) if same_url(&f.url, &s.url) => f,
            _ => continue,
        };
        let Some(text) = file.text.as_deref().filter(|t| !t.trim().is_empty()) else {
            continue;
        };
        out.push_str(&body[last..s.start]);
        out.push_str(lf_line_ends(&obscure_addresses(text)).trim_end());
        last = s.end;
        urls.push(file.url.clone());
    }
    out.push_str(&body[last..]);
    (out, urls)
}

/// Turns fetched bytes into UTF-8: ASCII and valid UTF-8 are kept as they
/// are, anything else is read as Windows-1252. Returns the encoding used.
pub fn decode(bytes: &[u8]) -> (String, &'static str) {
    match std::str::from_utf8(bytes) {
        Ok(s) if s.is_ascii() => (s.to_string(), "ascii"),
        Ok(s) => (s.to_string(), "utf-8"),
        Err(_) => {
            let (s, _) = WINDOWS_1252.decode_without_bom_handling(bytes);
            (s.into_owned(), "windows-1252")
        }
    }
}

/// True when the two URLs are equal once `http://` or `https://` is taken
/// off both.
pub fn same_url(a: &str, b: &str) -> bool {
    fn bare(u: &str) -> &str {
        u.strip_prefix("https://")
            .or_else(|| u.strip_prefix("http://"))
            .unwrap_or(u)
    }
    bare(a) == bare(b)
}

/// Reads `<dir>/<period>.json.gz`. `None` when the period has no file yet.
/// An error when the file cannot be read, is damaged, or says it describes
/// another period.
pub fn load(dir: &Path, period: &str) -> Result<Option<PeriodAttachments>> {
    let path = dir.join(format!("{}.json.gz", period));
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(e).with_context(|| format!("Failed to read {}", path.display()));
        }
    };
    let mut json = String::new();
    GzDecoder::new(bytes.as_slice())
        .read_to_string(&mut json)
        .with_context(|| format!("Failed to decompress {}", path.display()))?;
    let stored: PeriodAttachments = serde_json::from_str(&json)
        .with_context(|| format!("Failed to parse {}", path.display()))?;
    if stored.period != period {
        anyhow::bail!(
            "{} describes period {}, not {}",
            path.display(),
            stored.period,
            period
        );
    }
    Ok(Some(stored))
}

/// Writes `<dir>/<period>.json.gz`: compact JSON with sorted keys and a final
/// newline, gzipped with header time 0, so the same data gives the same bytes
/// on any machine. The bytes go to a `.tmp` file that is then renamed over
/// the period's file, so a write cut short leaves the old file as it was.
pub fn save(dir: &Path, p: &PeriodAttachments) -> Result<()> {
    fs::create_dir_all(dir).with_context(|| format!("Failed to create {}", dir.display()))?;
    let path = dir.join(format!("{}.json.gz", p.period));
    let tmp = dir.join(format!("{}.json.gz.tmp", p.period));
    let mut json = serde_json::to_string(p).context("Failed to serialize PeriodAttachments")?;
    json.push('\n');
    let mut encoder = GzBuilder::new()
        .mtime(0)
        .write(Vec::new(), Compression::default());
    encoder
        .write_all(json.as_bytes())
        .context("Failed to compress PeriodAttachments")?;
    let gz = encoder
        .finish()
        .context("Failed to compress PeriodAttachments")?;
    let written = fs::write(&tmp, gz)
        .with_context(|| format!("Failed to write {}", tmp.display()))
        .and_then(|()| {
            fs::rename(&tmp, &path)
                .with_context(|| format!("Failed to move {} to {}", tmp.display(), path.display()))
        });
    if written.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    written
}
