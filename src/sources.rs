use std::collections::BTreeMap;
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

/// Reads `<dir>/<period>.json`. `None` when the period has no file yet.
pub fn load(dir: &Path, period: &str) -> Result<Option<PeriodSources>> {
    let path = dir.join(format!("{}.json", period));
    let raw = match fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(e).with_context(|| format!("Failed to read {}", path.display()));
        }
    };
    let sources = serde_json::from_str(&raw)
        .with_context(|| format!("Failed to parse {}", path.display()))?;
    Ok(Some(sources))
}

/// Writes `<dir>/<period>.json`. The layout is fixed and the keys are sorted,
/// so saving a period that has not changed writes the same bytes.
pub fn save(dir: &Path, s: &PeriodSources) -> Result<()> {
    fs::create_dir_all(dir).with_context(|| format!("Failed to create {}", dir.display()))?;
    let path = dir.join(format!("{}.json", s.period));
    let mut json = serde_json::to_string_pretty(s).context("Failed to serialize PeriodSources")?;
    json.push('\n');
    fs::write(&path, json).with_context(|| format!("Failed to write {}", path.display()))?;
    Ok(())
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
