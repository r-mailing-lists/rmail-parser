use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};

use crate::attachments::{
    self, decode, find_scrubbed, obscure_addresses, PeriodAttachments, StoredFile,
};
use crate::pipeline::{discover_mbox_files, read_file_lossy};
use crate::source_fetch::HttpFetcher;
use crate::sources::{period_of, period_sort_key};

/// A period's file is written after this many new entries at the latest, so
/// a run that is cut short keeps what it fetched.
const SAVE_EVERY: usize = 50;

/// The one way out to the archive for scrubbed texts. Everything above it is
/// tested through this trait, with no network.
pub trait AttachmentFetcher {
    /// The bytes at `url`. `Ok(None)` is HTTP 404. An answer that is not the
    /// file asked for, such as a redirect or an HTML page, is an error.
    fn get_bytes(&mut self, url: &str) -> anyhow::Result<Option<Vec<u8>>>;
}

impl AttachmentFetcher for HttpFetcher {
    fn get_bytes(&mut self, url: &str) -> anyhow::Result<Option<Vec<u8>>> {
        HttpFetcher::get_bytes(self, url)
    }
}

/// Limits on one run.
#[derive(Debug, Clone)]
pub struct AttachmentsOptions {
    /// Requests the run may make, whatever their outcome.
    pub max_requests: usize,
    /// Failures in a row that end the run. A 404 counts as one, and a file
    /// that arrives starts the count again.
    pub max_failures: usize,
    /// Files longer than this are recorded as too large, without their text.
    pub max_bytes: usize,
}

impl Default for AttachmentsOptions {
    fn default() -> Self {
        Self {
            max_requests: 3000,
            max_failures: 10,
            max_bytes: 1_048_576,
        }
    }
}

/// What one run did.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct AttachmentsReport {
    /// Raw files looked at.
    pub periods: usize,
    /// Notices found, one per file per period.
    pub found: usize,
    /// Of those, already in the store when the run began.
    pub stored_before: usize,
    /// Files fetched and stored in this run.
    pub fetched: usize,
    /// Requests answered 404 and recorded as "not found".
    pub not_found: usize,
    /// Files over the size limit, recorded as "too large".
    pub too_large: usize,
    /// Requests that failed, and the 404s among the failures in a row that
    /// ended the run. Nothing is recorded for them.
    pub failed: usize,
    /// Notices whose URL is outside the archive, never asked for.
    pub refused: usize,
    /// Notices still to do: not refused, not stored, not answered.
    pub left: usize,
    /// Why the run ended while work was left, if it did.
    pub stopped: Option<String>,
    /// Requests for a file known to be there, made to tell a run of 404s
    /// from a source that went away. They count toward the requests a run
    /// may make, and nothing is stored for them.
    pub probes: usize,
}

/// `url` without `http://` or `https://`.
fn bare(url: &str) -> &str {
    url.strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .unwrap_or(url)
}

/// Fetches the text Mailman scrubbed out of the messages in the raw files in
/// `raw`, into `out/<period>.json.gz`. Periods are taken oldest first. Only
/// URLs that are exactly `<base_url>/attachments/` followed by a key are
/// asked for, at the base's scheme.
///
/// A 404 and a file over the size limit are recorded, so they are not asked
/// for again. A failed request records nothing and is asked for again on a
/// later run. A 404 counts as a failure in a row, though, as a source that
/// has moved its files or turns us away answers 404 to everything. When
/// `max_failures` of them come in a row, a file known to be there is asked
/// for once: the last one this run fetched with its text, or else one
/// stored with its text in a period loaded so far. If it arrives, the
/// source is there, the 404s stand and the run goes on. If it does not, or
/// no file is known, or the failures in a row hold an error, the run ends,
/// and the 404s since the last file that arrived are taken out of the store
/// again, in whichever period they were recorded. A period's file is
/// written when it has changed, and at least every 50 new entries.
///
/// Running out of requests or giving up on an archive that keeps failing is
/// not an error: the report says why the run stopped.
pub fn run_attachments(
    raw: &Path,
    out: &Path,
    base_url: &str,
    opts: &AttachmentsOptions,
    fetcher: &mut dyn AttachmentFetcher,
) -> Result<AttachmentsReport> {
    let mut files = discover_mbox_files(raw)?;
    if files.is_empty() {
        anyhow::bail!("No mbox files found in {}", raw.display());
    }
    files.sort_by(|a, b| {
        let (a, b) = (period_of(a), period_of(b));
        period_sort_key(&a)
            .cmp(&period_sort_key(&b))
            .then_with(|| a.cmp(&b))
    });

    let base = base_url.trim_end_matches('/');
    let prefix = format!("{}/attachments/", bare(base));
    let scheme = if base.starts_with("http://") {
        "http://"
    } else {
        "https://"
    };

    let mut report = AttachmentsReport::default();
    let mut requests = 0;
    let mut failures = 0;
    // The 404s since the last file that arrived, as (period, key).
    let mut streak: Vec<(String, String)> = Vec::new();
    let mut halt: Option<String> = None;
    // Files known to be there, at the base's scheme: the last one fetched
    // with its text in this run, and one stored with its text before.
    let mut last_fetched: Option<String> = None;
    let mut stored_with_text: Option<String> = None;

    for file_path in &files {
        report.periods += 1;
        let period = period_of(file_path);
        let text = read_file_lossy(file_path)?;
        // A notice is the archive's only when its URL is exactly the
        // archive's `attachments/` followed by the key: anyone can write a
        // notice, and a client resolves `..` in a URL. The others are set
        // aside before two notices are taken for one, so a refused notice
        // cannot hide a real one with the same key.
        let mut refused = HashSet::new();
        let mut seen = HashSet::new();
        let mut notices = Vec::new();
        for notice in find_scrubbed(&text) {
            if bare(&notice.url) != format!("{}{}", prefix, notice.key) {
                refused.insert(notice.url);
            } else if seen.insert(notice.key.clone()) {
                notices.push(notice);
            }
        }
        report.found += refused.len() + notices.len();
        report.refused += refused.len();
        if notices.is_empty() {
            continue;
        }
        let mut store = attachments::load(out, &period)?.unwrap_or_else(|| PeriodAttachments {
            period: period.clone(),
            files: Default::default(),
        });
        if stored_with_text.is_none() {
            // Only a URL that is the archive's own, as for a notice.
            stored_with_text = store
                .files
                .iter()
                .find(|(key, f)| f.text.is_some() && bare(&f.url) == format!("{}{}", prefix, key))
                .map(|(_, f)| format!("{}{}", scheme, bare(&f.url)));
        }
        // Whether the store differs from the period's file, and how many
        // entries were added since the file was last written.
        let mut changed = false;
        let mut unsaved = 0;

        for notice in notices {
            if store.files.contains_key(&notice.key) {
                report.stored_before += 1;
                continue;
            }
            if halt.is_none() && requests >= opts.max_requests {
                halt = Some(format!("request budget of {} spent", opts.max_requests));
            }
            if halt.is_some() {
                report.left += 1;
                continue;
            }

            let url = format!("{}{}", scheme, bare(&notice.url));
            requests += 1;
            let answer = fetcher.get_bytes(&url);
            let arrived = matches!(answer, Ok(Some(_)));
            let stored = match answer {
                Ok(Some(bytes)) if bytes.len() > opts.max_bytes => {
                    report.too_large += 1;
                    Some(StoredFile {
                        url,
                        encoding: None,
                        text: None,
                        status: Some("too large".to_string()),
                    })
                }
                Ok(Some(bytes)) => {
                    report.fetched += 1;
                    last_fetched = Some(url.clone());
                    let (text, encoding) = decode(&bytes);
                    Some(StoredFile {
                        url,
                        encoding: Some(encoding.to_string()),
                        text: Some(obscure_addresses(&text)),
                        status: None,
                    })
                }
                Ok(None) => {
                    report.not_found += 1;
                    streak.push((period.clone(), notice.key.clone()));
                    Some(StoredFile {
                        url,
                        encoding: None,
                        text: None,
                        status: Some("not found".to_string()),
                    })
                }
                Err(e) => {
                    report.failed += 1;
                    report.left += 1;
                    eprintln!("Warning: {}: {:#}", url, e);
                    None
                }
            };
            if let Some(stored) = stored {
                store.files.insert(notice.key, stored);
                changed = true;
                unsaved += 1;
                if unsaved >= SAVE_EVERY {
                    save_period(out, &store)?;
                    changed = false;
                    unsaved = 0;
                }
            }

            if arrived {
                // The source is there, so the 404s before this file are its
                // word on those files.
                failures = 0;
                streak.clear();
                continue;
            }
            failures += 1;
            if failures >= opts.max_failures {
                // Files deleted at the source, or a source that went away? A
                // file known to be there tells, when every failure in a row
                // is a 404 (the streak holds the 404s among them) and a
                // request is left to ask for it.
                let known = last_fetched.as_ref().or(stored_with_text.as_ref());
                let probe =
                    known.filter(|_| streak.len() == failures && requests < opts.max_requests);
                if let Some(known) = probe {
                    requests += 1;
                    report.probes += 1;
                    let answers = matches!(fetcher.get_bytes(known), Ok(Some(_)));
                    let word = if answers {
                        "it answers"
                    } else {
                        "it does not answer"
                    };
                    eprintln!(
                        "source checked after {} not found in a row: {}",
                        failures, word
                    );
                    if answers {
                        // So the 404s are its word on those files.
                        failures = 0;
                        streak.clear();
                        continue;
                    }
                }
                halt = Some(format!("{} requests in a row failed", failures));
                let (here, elsewhere) = forget(out, &mut streak, &mut store)?;
                if here > 0 {
                    changed = true;
                }
                report.not_found -= here + elsewhere;
                report.failed += here + elsewhere;
                report.left += here + elsewhere;
            }
        }
        if changed {
            write_period(out, &store)?;
        }
    }
    report.stopped = halt;
    Ok(report)
}

/// Takes the 404s in `streak` out of the store again: those of the period
/// being fetched out of `store`, which is written later, and those of an
/// earlier period out of its file. Returns how many came out of each.
fn forget(
    out: &Path,
    streak: &mut Vec<(String, String)>,
    store: &mut PeriodAttachments,
) -> Result<(usize, usize)> {
    let mut here = 0;
    let mut earlier: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (period, key) in streak.drain(..) {
        if period == store.period {
            here += usize::from(store.files.remove(&key).is_some());
        } else {
            earlier.entry(period).or_default().push(key);
        }
    }
    let mut elsewhere = 0;
    for (period, keys) in earlier {
        // An earlier period's file was written when that period was done.
        if let Some(mut file) = attachments::load(out, &period)? {
            let before = file.files.len();
            for key in keys {
                file.files.remove(&key);
            }
            if file.files.len() < before {
                elsewhere += before - file.files.len();
                write_period(out, &file)?;
            }
        }
    }
    Ok((here, elsewhere))
}

/// Writes a period's file, or removes it when nothing is left in it: a file
/// that held only 404s taken out again was written by this run, and the
/// period is left as it was before.
fn write_period(out: &Path, store: &PeriodAttachments) -> Result<()> {
    if !store.files.is_empty() {
        return save_period(out, store);
    }
    let path = out.join(format!("{}.json.gz", store.period));
    match fs::remove_file(&path) {
        Ok(()) => {
            eprintln!("Removed {}", path.display());
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("Failed to remove {}", path.display())),
    }
}

fn save_period(out: &Path, store: &PeriodAttachments) -> Result<()> {
    attachments::save(out, store)?;
    eprintln!(
        "Wrote {}",
        out.join(format!("{}.json.gz", store.period)).display()
    );
    Ok(())
}
