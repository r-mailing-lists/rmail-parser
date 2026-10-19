use std::collections::HashSet;
use std::path::Path;

use anyhow::Result;

use crate::attachments::{self, decode, find_scrubbed, PeriodAttachments, StoredFile};
use crate::pipeline::{discover_mbox_files, read_file_lossy};
use crate::source_fetch::HttpFetcher;
use crate::sources::{period_of, period_sort_key};

/// A period's file is written after this many new entries at the latest, so
/// a run that is cut short keeps what it fetched.
const SAVE_EVERY: usize = 50;

/// The one way out to the archive for scrubbed texts. Everything above it is
/// tested through this trait, with no network.
pub trait AttachmentFetcher {
    /// The bytes at `url`. `Ok(None)` is HTTP 404.
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
    /// Failures in a row that end the run. A 404 is an answer, not a failure.
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
    /// Requests answered 404, recorded as "not found".
    pub not_found: usize,
    /// Files over the size limit, recorded as "too large".
    pub too_large: usize,
    /// Requests that failed. Nothing is recorded for them.
    pub failed: usize,
    /// Notices whose URL is outside the archive, never asked for.
    pub refused: usize,
    /// Notices still to do: not refused, not stored, not answered.
    pub left: usize,
    /// Why the run ended while work was left, if it did.
    pub stopped: Option<String>,
}

/// `url` without `http://` or `https://`.
fn bare(url: &str) -> &str {
    url.strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .unwrap_or(url)
}

/// Fetches the text Mailman scrubbed out of the messages in the raw files in
/// `raw`, into `out/<period>.json.gz`. Periods are taken oldest first. Only
/// URLs under `<base_url>/attachments/` are asked for, at the base's scheme.
///
/// A 404 and a file over the size limit are recorded, so they are not asked
/// for again. A failed request records nothing and is asked for again on a
/// later run. A period's file is written when it has changed, and at least
/// every 50 new entries.
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
    let mut halt: Option<String> = None;

    for file_path in &files {
        report.periods += 1;
        let period = period_of(file_path);
        let text = read_file_lossy(file_path)?;
        let mut seen = HashSet::new();
        let notices: Vec<_> = find_scrubbed(&text)
            .into_iter()
            .filter(|n| seen.insert(n.key.clone()))
            .collect();
        if notices.is_empty() {
            continue;
        }
        let mut store = attachments::load(out, &period)?.unwrap_or_else(|| PeriodAttachments {
            period: period.clone(),
            files: Default::default(),
        });
        let mut unsaved = 0;

        for notice in notices {
            report.found += 1;
            if !bare(&notice.url).starts_with(&prefix) {
                report.refused += 1;
                continue;
            }
            if store.files.contains_key(&notice.key) {
                report.stored_before += 1;
                continue;
            }
            if halt.is_none() && failures >= opts.max_failures {
                halt = Some(format!("{} requests in a row failed", failures));
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
            let stored = match fetcher.get_bytes(&url) {
                Ok(Some(bytes)) if bytes.len() > opts.max_bytes => {
                    report.too_large += 1;
                    StoredFile {
                        url,
                        encoding: None,
                        text: None,
                        status: Some("too large".to_string()),
                    }
                }
                Ok(Some(bytes)) => {
                    report.fetched += 1;
                    let (text, encoding) = decode(&bytes);
                    StoredFile {
                        url,
                        encoding: Some(encoding.to_string()),
                        text: Some(text),
                        status: None,
                    }
                }
                Ok(None) => {
                    report.not_found += 1;
                    StoredFile {
                        url,
                        encoding: None,
                        text: None,
                        status: Some("not found".to_string()),
                    }
                }
                Err(e) => {
                    report.failed += 1;
                    failures += 1;
                    report.left += 1;
                    eprintln!("Warning: {}: {:#}", url, e);
                    continue;
                }
            };
            failures = 0;
            store.files.insert(notice.key, stored);
            unsaved += 1;
            if unsaved >= SAVE_EVERY {
                save_period(out, &store)?;
                unsaved = 0;
            }
        }
        if unsaved > 0 {
            save_period(out, &store)?;
        }
    }
    report.stopped = halt;
    Ok(report)
}

fn save_period(out: &Path, store: &PeriodAttachments) -> Result<()> {
    attachments::save(out, store)?;
    eprintln!(
        "Wrote {}",
        out.join(format!("{}.json.gz", store.period)).display()
    );
    Ok(())
}
