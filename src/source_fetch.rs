use std::path::Path;

use anyhow::Result;
use mailparse::MailHeaderMap;
use rayon::prelude::*;

use crate::mbox::split_mbox;
use crate::message::{parse_message, Message};
use crate::pipeline::{discover_mbox_files, read_file_lossy};
use crate::source_align::align;
use crate::source_index::{parse_index, subject_key, IndexEntry};
use crate::source_page::{fix_string, parse_article, ArticlePage};
use crate::sources::{self, needs_page, period_of, period_sort_key, PeriodSources};

/// A period's file is written after this many pages at the latest, so a run
/// that is cut short keeps what it fetched.
const SAVE_EVERY: usize = 50;

/// What the source answered.
pub enum Fetched {
    Body(String),
    /// HTTP 404: the source has no such page.
    NotFound,
}

/// The one way out to the source. Everything above it is tested against
/// saved pages through this trait, with no network.
pub trait Fetcher {
    fn get(&mut self, url: &str) -> anyhow::Result<Fetched>;
}

/// Limits on one run.
#[derive(Debug, Clone)]
pub struct SourcesOptions {
    /// Requests the run may make, whatever their outcome.
    pub max_requests: usize,
    /// Failures in a row that end the run.
    pub max_failures: usize,
}

impl Default for SourcesOptions {
    fn default() -> Self {
        Self {
            max_requests: 300,
            max_failures: 3,
        }
    }
}

/// What one run did.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct SourcesReport {
    /// Index pages fetched and aligned.
    pub periods_indexed: usize,
    /// Message pages fetched.
    pub pages_fetched: usize,
    /// `?` that a page had the real character for.
    pub chars_restored: usize,
    /// `?` in fetched messages left as they were.
    pub chars_left: usize,
    /// Pages naming a Message-ID that was not their message's.
    pub mismatches: usize,
    /// Characters beyond U+FFFF seen in fetched pages. The text file has two
    /// `?` for each, which this version does not restore.
    pub astral_chars: usize,
    /// Why the run ended before every period was up to date, if it did.
    pub stopped: Option<String>,
}

/// The outcome of asking the source for one page.
enum Asked<T> {
    Got(T),
    NotFound,
    /// The request failed, or what came back was not the page asked for.
    Failed,
    /// The run is over: the reason why.
    Stop(String),
}

/// The fetcher, with this run's request budget and failure count around it.
struct Budget<'a> {
    fetcher: &'a mut dyn Fetcher,
    opts: &'a SourcesOptions,
    requests: usize,
    failures: usize,
}

impl Budget<'_> {
    /// Asks for `url` and reads the answer with `parse`. An answer `parse`
    /// cannot read counts as a failure: a source that is down often says so
    /// with an error page and a 200.
    fn ask<T>(&mut self, url: &str, parse: impl FnOnce(&str) -> Option<T>) -> Asked<T> {
        if self.requests >= self.opts.max_requests {
            return Asked::Stop(format!(
                "request budget of {} spent",
                self.opts.max_requests
            ));
        }
        self.requests += 1;
        let problem = match self.fetcher.get(url) {
            Ok(Fetched::Body(body)) => match parse(&body) {
                Some(page) => {
                    self.failures = 0;
                    return Asked::Got(page);
                }
                None => "not the page expected".to_string(),
            },
            Ok(Fetched::NotFound) => {
                self.failures = 0;
                return Asked::NotFound;
            }
            Err(e) => format!("{:#}", e),
        };
        eprintln!("Warning: {}: {}", url, problem);
        self.failures += 1;
        if self.failures >= self.opts.max_failures {
            Asked::Stop(format!("{} requests in a row failed", self.failures))
        } else {
            Asked::Failed
        }
    }
}

/// Fetches what `out_dir` is missing for the raw files in `raw_dir`: the
/// index of every period that has none or has grown, and the page of every
/// message that may have a character to restore. Periods are taken newest
/// first, and each period's file is written as soon as there is something to
/// write, so the next run picks up where this one stopped.
///
/// Running out of budget or giving up on a source that keeps failing is not
/// an error: the report says why the run stopped.
pub fn run_sources(
    raw_dir: &Path,
    out_dir: &Path,
    base_url: &str,
    opts: &SourcesOptions,
    fetcher: &mut dyn Fetcher,
) -> Result<SourcesReport> {
    let mut files = discover_mbox_files(raw_dir)?;
    if files.is_empty() {
        anyhow::bail!("No mbox files found in {}", raw_dir.display());
    }
    files.sort_by(|a, b| {
        let (a, b) = (period_of(a), period_of(b));
        period_sort_key(&b)
            .cmp(&period_sort_key(&a))
            .then_with(|| a.cmp(&b))
    });

    let base = base_url.trim_end_matches('/');
    let mut report = SourcesReport::default();
    let mut budget = Budget {
        fetcher,
        opts,
        requests: 0,
        failures: 0,
    };

    for file_path in &files {
        // Read exactly as `parse` does, so both count the same messages.
        let content = read_file_lossy(file_path)?;
        let raw_messages = split_mbox(&content);
        if raw_messages.is_empty() {
            continue;
        }
        let period = period_of(file_path);
        let stop = update_period(
            &period,
            &raw_messages,
            out_dir,
            base,
            &mut budget,
            &mut report,
        )?;
        if stop.is_some() {
            report.stopped = stop;
            break;
        }
    }
    Ok(report)
}

/// Brings one period's file up to date. Returns the reason when the run has
/// to stop here.
fn update_period(
    period: &str,
    raw_messages: &[&str],
    out_dir: &Path,
    base: &str,
    budget: &mut Budget<'_>,
    report: &mut SourcesReport,
) -> Result<Option<String>> {
    let mut current = match sources::load(out_dir, period)? {
        Some(known) if known.articles.len() == raw_messages.len() => known,
        // No file yet, or the raw file has changed length since: index it.
        stale => {
            let url = format!("{}/{}/date.html", base, period);
            let articles = match budget.ask(&url, non_empty_index) {
                Asked::Got(index) => {
                    let keys: Vec<String> = raw_messages
                        .iter()
                        .map(|raw| subject_key(&raw_subject(raw)))
                        .collect();
                    report.periods_indexed += 1;
                    align(&keys, &index)
                }
                // Recorded as a period without pages, so it is not asked for again.
                Asked::NotFound => vec![None; raw_messages.len()],
                Asked::Failed => return Ok(None),
                Asked::Stop(reason) => return Ok(Some(reason)),
            };
            // Pages already fetched stay fetched, as long as they are still here.
            let mut fixes = stale.map(|s| s.fixes).unwrap_or_default();
            fixes.retain(|number, _| articles.iter().flatten().any(|a| a == number));
            let indexed = PeriodSources {
                period: period.to_string(),
                articles,
                fixes,
            };
            save_period(out_dir, &indexed)?;
            indexed
        }
    };

    // A message that does not parse keeps its place, as it does in `parse`.
    let parsed: Vec<Option<Message>> = raw_messages
        .par_iter()
        .map(|raw| parse_message(raw).ok())
        .collect();

    let mut unsaved = 0;
    let mut stop = None;
    for (i, msg) in parsed.iter().enumerate() {
        let (msg, number) = match (msg, current.articles.get(i)) {
            (Some(msg), Some(Some(number))) => (msg, number.clone()),
            _ => continue,
        };
        if current.fixes.contains_key(&number) || !needs_page(&msg.body_plain) {
            continue;
        }

        let url = format!("{}/{}/{}.html", base, period, number);
        match budget.ask(&url, parse_article) {
            Asked::Got(page) => {
                report.pages_fetched += 1;
                if names_another_message(&page, msg) {
                    eprintln!(
                        "Warning: {}/{}.html is not the page of message {} in the raw file, unlinking it",
                        period,
                        number,
                        i + 1
                    );
                    current.articles[i] = None;
                    report.mismatches += 1;
                } else {
                    let fix = fix_string(&msg.body_plain, &page.text);
                    let restored = fix.chars().filter(|c| *c != '?').count();
                    report.chars_restored += restored;
                    report.chars_left += fix.chars().count() - restored;
                    report.astral_chars += page.text.chars().filter(|c| *c > '\u{ffff}').count();
                    // Nothing to restore is stored as an empty string.
                    let fix = if restored == 0 { String::new() } else { fix };
                    current.fixes.insert(number, fix);
                }
            }
            Asked::NotFound => {
                eprintln!(
                    "Warning: {}/{}.html is listed in the index but is not there, unlinking it",
                    period, number
                );
                current.articles[i] = None;
            }
            Asked::Failed => continue,
            Asked::Stop(reason) => {
                stop = Some(reason);
                break;
            }
        }
        unsaved += 1;
        if unsaved >= SAVE_EVERY {
            save_period(out_dir, &current)?;
            unsaved = 0;
        }
    }
    if unsaved > 0 {
        save_period(out_dir, &current)?;
    }
    Ok(stop)
}

fn save_period(out_dir: &Path, period: &PeriodSources) -> Result<()> {
    sources::save(out_dir, period)?;
    eprintln!(
        "Wrote {}",
        out_dir.join(format!("{}.json", period.period)).display()
    );
    Ok(())
}

/// An index with no entries, for a raw file that has messages, is an error
/// page or a page cut short, not an index.
fn non_empty_index(html: &str) -> Option<Vec<IndexEntry>> {
    let index = parse_index(html);
    if index.is_empty() {
        None
    } else {
        Some(index)
    }
}

/// The Subject of a raw message, read the way `parse_message` reads it but
/// without needing the rest of the message to parse.
fn raw_subject(raw: &str) -> String {
    let start = if raw.starts_with("From ") {
        raw.find('\n').map(|p| p + 1).unwrap_or(raw.len())
    } else {
        0
    };
    mailparse::parse_headers(raw[start..].as_bytes())
        .ok()
        .and_then(|(headers, _)| headers.get_first_value("Subject"))
        .unwrap_or_default()
}

/// True when the page names a Message-ID and it is not this message's.
fn names_another_message(page: &ArticlePage, msg: &Message) -> bool {
    let own = msg
        .message_id
        .trim()
        .trim_start_matches('<')
        .trim_end_matches('>');
    match &page.message_id {
        Some(id) => !own.is_empty() && id != own,
        None => false,
    }
}
