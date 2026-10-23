use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// What the source's pages say about one raw file: which page each message
/// has, and the characters those pages still carry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PeriodSources {
    pub period: String,
    /// Page number of the i-th message in the raw file, counted the way
    /// `split_mbox` counts them. `None` where no page could be matched.
    pub articles: Vec<Option<String>>,
    /// Page number -> one character per `?` in that message's body. A key
    /// means the page has been fetched; an empty value means it had nothing
    /// to restore. A `BTreeMap` so the keys are written sorted.
    pub fixes: BTreeMap<String, String>,
    /// Page number -> the Message-ID that page names, without angle
    /// brackets, for every page that was fetched and turned out to belong to
    /// another message than the one it was paired with. Indexing the period
    /// again links such a page only to the message it names. The field has
    /// no default: a file written before it existed is refused, not read as
    /// a file with nothing rejected.
    pub rejected: BTreeMap<String, String>,
}

const MONTHS: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];

/// The period a raw file covers, which is its file stem: `2026-October`,
/// `2026q4` or `2026`. The source files its pages under the same name.
pub fn period_of(path: &Path) -> String {
    path.file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown")
        .to_string()
}

/// A key that sorts periods by time, whichever way the list is filed. Sort
/// on it in reverse to get the newest first. A quarter and a year sort at
/// their last month; a name that fits none of the three schemes sorts oldest.
pub fn period_sort_key(period: &str) -> (i32, u32) {
    if let Some((year, month)) = period.split_once('-') {
        let month = MONTHS.iter().position(|m| m.eq_ignore_ascii_case(month));
        if let (Ok(year), Some(month)) = (year.parse::<i32>(), month) {
            return (year, month as u32 + 1);
        }
    }
    if let Some((year, quarter)) = period.split_once('q') {
        if let (Ok(year), Ok(quarter @ 1..=4)) = (year.parse::<i32>(), quarter.parse::<u32>()) {
            return (year, quarter * 3);
        }
    }
    match period.parse::<i32>() {
        Ok(year) => (year, 12),
        Err(_) => (0, 0),
    }
}

/// Reads `<dir>/<period>.json`. `None` when the period has no file yet. An
/// error when the file cannot be read, is damaged, or says it describes
/// another period.
pub fn load(dir: &Path, period: &str) -> Result<Option<PeriodSources>> {
    let path = dir.join(format!("{}.json", period));
    let raw = match fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(e).with_context(|| format!("Failed to read {}", path.display()));
        }
    };
    let sources: PeriodSources = serde_json::from_str(&raw)
        .with_context(|| format!("Failed to parse {}", path.display()))?;
    // A file copied or renamed from another period would otherwise be saved
    // over, or have its pages linked into, a period it does not describe.
    if sources.period != period {
        anyhow::bail!(
            "{} describes period {}, not {}",
            path.display(),
            sources.period,
            period
        );
    }
    Ok(Some(sources))
}

/// Writes `<dir>/<period>.json`. The layout is fixed and the keys are sorted,
/// so saving a period that has not changed writes the same bytes.
///
/// The bytes go to `<period>.json.tmp` beside it, and that file is then
/// renamed over the period's file. A write that is cut short therefore
/// leaves the period's file as it was, and never half written: `load`
/// refuses a damaged file, and both commands stop on one.
pub fn save(dir: &Path, s: &PeriodSources) -> Result<()> {
    fs::create_dir_all(dir).with_context(|| format!("Failed to create {}", dir.display()))?;
    let path = dir.join(format!("{}.json", s.period));
    let tmp = dir.join(format!("{}.json.tmp", s.period));
    let mut json = serde_json::to_string_pretty(s).context("Failed to serialize PeriodSources")?;
    json.push('\n');
    let written = fs::write(&tmp, json)
        .with_context(|| format!("Failed to write {}", tmp.display()))
        .and_then(|()| {
            fs::rename(&tmp, &path)
                .with_context(|| format!("Failed to move {} to {}", tmp.display(), path.display()))
        });
    if written.is_err() {
        // Whatever went wrong, the temporary file is of no use now.
        let _ = fs::remove_file(&tmp);
    }
    written
}

/// Marks each message that repeats an earlier message of the same raw file,
/// by Message-ID. A source that archived a period a second time has every
/// message of it twice in the text file, the later copy sometimes with its
/// From line mangled, and still one page for each message. Only the first
/// copy stands for the message: a later one is never matched with a page.
/// `None` is a message that did not parse; it, and a message without a
/// Message-ID, is a copy of nothing.
pub fn later_copies<'a>(message_ids: impl IntoIterator<Item = Option<&'a str>>) -> Vec<bool> {
    let mut seen = HashSet::new();
    message_ids
        .into_iter()
        .map(|id| {
            let id = id
                .unwrap_or_default()
                .trim()
                .trim_start_matches('<')
                .trim_end_matches('>');
            !id.is_empty() && !seen.insert(id)
        })
        .collect()
}

/// True when the body has a `?` with a letter directly on both sides, which
/// is what a lost apostrophe or accented letter looks like. A `?` inside a
/// URL is a query string, so tokens that look like URLs are not considered.
pub fn needs_page(body: &str) -> bool {
    body.split_whitespace()
        .filter(|token| !token.contains("://") && !token.starts_with("www."))
        .any(|token| {
            token
                .as_bytes()
                .windows(3)
                .any(|w| w[1] == b'?' && w[0].is_ascii_alphabetic() && w[2].is_ascii_alphabetic())
        })
}

/// Replaces each `?` in `body` with the matching character of `fix`, which
/// has one character per `?`, in order. A `?` in `fix` leaves that one alone.
///
/// Returns `None` when `fix` does not have exactly one character per `?`:
/// the body is then not the text the fix was made from, and applying any of
/// it would put characters in the wrong places.
pub fn apply_fixes(body: &str, fix: &str) -> Option<String> {
    if body.matches('?').count() != fix.chars().count() {
        return None;
    }
    let mut fixes = fix.chars();
    Some(
        body.chars()
            .map(|c| {
                if c == '?' {
                    fixes.next().unwrap_or('?')
                } else {
                    c
                }
            })
            .collect(),
    )
}
