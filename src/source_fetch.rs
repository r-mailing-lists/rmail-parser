use std::collections::HashSet;
use std::io::Read;
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use mailparse::MailHeaderMap;
use rayon::prelude::*;

use crate::mbox::split_mbox;
use crate::message::{parse_message, Message};
use crate::pipeline::{discover_mbox_files, read_file_lossy};
use crate::source_align::align;
use crate::source_index::{parse_index, subject_key, IndexEntry};
use crate::source_page::{fix_string, parse_article, ArticlePage};
use crate::sources::{self, needs_page, period_of, period_sort_key, PeriodSources};

const USER_AGENT: &str = "rmail-parser (+https://github.com/r-mailing-lists)";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// A period's file is written after this many pages at the latest, so a run
/// that is cut short keeps what it fetched.
const SAVE_EVERY: usize = 50;

/// What the source answered.
pub enum Fetched {
    Body(String),
    /// HTTP 404: the source says it has no such page. A run does not take
    /// its word for it.
    NotFound,
}

/// Why an answer with a 200 is not the file at `asked`, if it is not. A
/// server that has moved its files or turns a client away often says so
/// with a redirect to another page, or with an HTML page and a 200, and
/// neither is the file. So an answer that came from another URL than the
/// one asked for (`final_url`, after any redirect) is not the file, and nor
/// is a `text/html` answer for a URL that does not end in `.html` or `.htm`.
pub fn not_the_file(asked: &str, final_url: &str, content_type: &str) -> Option<String> {
    if final_url != asked {
        return Some(format!("redirected to {}", final_url));
    }
    let mime = content_type.split(';').next().unwrap_or("").trim();
    let path = asked.to_ascii_lowercase();
    if mime.eq_ignore_ascii_case("text/html") && !path.ends_with(".html") && !path.ends_with(".htm")
    {
        return Some("an HTML page, not the file".to_string());
    }
    None
}

/// The one way out to the source. Everything above it is tested against
/// saved pages through this trait, with no network.
pub trait Fetcher {
    fn get(&mut self, url: &str) -> anyhow::Result<Fetched>;
}

/// Fetches over HTTP, one request at a time, with a pause between requests.
pub struct HttpFetcher {
    agent: ureq::Agent,
    delay: Duration,
    last_request: Option<Instant>,
}

impl HttpFetcher {
    pub fn new(delay_ms: u64) -> Self {
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .user_agent(USER_AGENT)
            .build();
        Self {
            agent,
            delay: Duration::from_millis(delay_ms),
            last_request: None,
        }
    }

    /// Waits until the pause since the last request is over.
    fn pause(&self) {
        if let Some(last) = self.last_request {
            let since = last.elapsed();
            if since < self.delay {
                thread::sleep(self.delay - since);
            }
        }
    }

    /// Fetches `url` as bytes, with the same pause between requests as
    /// `get`. `Ok(None)` is HTTP 404; any other failure is an error, and so
    /// is an answer that is not the file asked for (see `not_the_file`).
    pub fn get_bytes(&mut self, url: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.pause();
        let request = self.agent.get(url);
        // The URL as the client writes it, which is how it reports where the
        // answer came from, so only a redirect makes the two differ.
        let asked = request
            .request_url()
            .map(|parsed| parsed.as_url().as_str().to_string())
            .unwrap_or_else(|_| url.to_string());
        let result = request.call();
        self.last_request = Some(Instant::now());
        match result {
            Ok(response) => {
                let content_type = response.header("content-type").unwrap_or("");
                if let Some(why) = not_the_file(&asked, response.get_url(), content_type) {
                    return Err(anyhow!("{}", why));
                }
                let mut bytes = Vec::new();
                response
                    .into_reader()
                    .read_to_end(&mut bytes)
                    .map_err(|e| anyhow!("could not read the response: {}", e))?;
                Ok(Some(bytes))
            }
            // A 404 at the end of a redirect says nothing about the file
            // asked for. The page of a 404 is HTML, so only the URL counts.
            Err(ureq::Error::Status(404, response)) => {
                match not_the_file(&asked, response.get_url(), "") {
                    Some(why) => Err(anyhow!("HTTP 404, {}", why)),
                    None => Ok(None),
                }
            }
            Err(ureq::Error::Status(code, _)) => Err(anyhow!("HTTP {}", code)),
            Err(e) => Err(anyhow!("{}", e)),
        }
    }
}

impl Fetcher for HttpFetcher {
    fn get(&mut self, url: &str) -> anyhow::Result<Fetched> {
        self.pause();
        let result = self.agent.get(url).call();
        self.last_request = Some(Instant::now());
        match result {
            Ok(response) => {
                // The pages are ASCII with numeric entities. A stray byte in
                // some other encoding comes out as U+FFFD, which is never
                // taken as a fix.
                let body = response
                    .into_string()
                    .map_err(|e| anyhow!("could not read the response: {}", e))?;
                Ok(Fetched::Body(body))
            }
            Err(ureq::Error::Status(404, _)) => Ok(Fetched::NotFound),
            Err(ureq::Error::Status(code, _)) => Err(anyhow!("HTTP {}", code)),
            Err(e) => Err(anyhow!("{}", e)),
        }
    }
}

/// Limits on one run.
#[derive(Debug, Clone)]
pub struct SourcesOptions {
    /// Requests the run may make, whatever their outcome.
    pub max_requests: usize,
    /// Failures in a row that end the run. A "not found" answer is one.
    pub max_failures: usize,
    /// Periods the run may look at, newest first. `None` is every period.
    /// A scheduled run passes 2, so it never walks into history.
    pub max_periods: Option<usize>,
}

impl Default for SourcesOptions {
    fn default() -> Self {
        Self {
            max_requests: 300,
            max_failures: 3,
            max_periods: None,
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
    /// Requests the source answered with "not found". Nothing is recorded
    /// for them, so the same pages are asked for again on a later run.
    pub not_found: usize,
    /// Requests that failed, or returned something that was not the page
    /// asked for.
    pub failed: usize,
    /// Periods indexed in this run whose index listed more pages than the
    /// raw file has messages: the index is live, the raw file a download.
    pub index_ahead: usize,
    /// Positions in the period files on disk after the run, for the periods
    /// the run looked at. A period whose index did not arrive counts with
    /// the file it already had; a period with no file counts for nothing.
    pub messages: usize,
    /// Of those positions, the ones with no page.
    pub unlinked: usize,
    /// Rejected pairings in those same files, from this run or earlier.
    pub rejected: usize,
    /// Older periods left alone because of `max_periods`.
    pub periods_skipped: usize,
    /// Why the run ended before every period was up to date, if it did.
    pub stopped: Option<String>,
}

/// The outcome of asking the source for one page.
enum Asked<T> {
    Got(T),
    /// The page did not arrive: the source said "not found", the request
    /// failed, or what came back was not the page asked for. None of these
    /// is taken to say anything about the page, so the caller records
    /// nothing and the page is asked for again on a later run.
    Failed,
    /// The run is over: the reason why.
    Stop(String),
}

/// The fetcher, with this run's request budget and failure count around it.
struct Budget<'a> {
    fetcher: &'a mut dyn Fetcher,
    opts: &'a SourcesOptions,
    requests: usize,
    /// Requests in a row that did not bring the page asked for.
    failures: usize,
    /// Requests answered "not found", over the whole run.
    not_found: usize,
    /// Requests that failed in any other way, over the whole run.
    failed: usize,
}

impl Budget<'_> {
    /// Asks for `url` and reads the answer with `parse`. An answer `parse`
    /// cannot read counts as a failure: a source that is down often says so
    /// with an error page and a 200. So does a "not found": from this source
    /// it is far more often a passing fault or a wrong address than a page
    /// that is gone, and believing it would write the fault down for good.
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
                None => {
                    self.failed += 1;
                    "not the page expected".to_string()
                }
            },
            Ok(Fetched::NotFound) => {
                self.not_found += 1;
                "not found".to_string()
            }
            Err(e) => {
                self.failed += 1;
                format!("{:#}", e)
            }
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
/// write, so the next run picks up where this one stopped. With
/// `max_periods` set, the run looks at that many periods and leaves the
/// older ones exactly as they are.
///
/// Only a page that arrived is ever written down. A request that fails, or
/// that the source answers with "not found", changes no file: the period or
/// the message is left as it was and asked for again on a later run.
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
        not_found: 0,
        failed: 0,
    };

    let mut looked_at = 0;
    for (at, file_path) in files.iter().enumerate() {
        // The older periods are not even read: a scheduled run stays with
        // the periods the workflow has just downloaded again.
        if opts.max_periods.is_some_and(|max| looked_at >= max) {
            report.periods_skipped = files.len() - at;
            break;
        }
        // Read exactly as `parse` does, so both count the same messages.
        let content = read_file_lossy(file_path)?;
        let raw_messages = split_mbox(&content);
        if raw_messages.is_empty() {
            continue;
        }
        looked_at += 1;
        // After a stop the walk goes on only to count the older periods
        // that a limited run leaves alone: no period file is read and no
        // request is sent.
        if report.stopped.is_some() {
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
            if opts.max_periods.is_none() {
                break;
            }
        }
    }
    report.not_found = budget.not_found;
    report.failed = budget.failed;
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
    // A message that does not parse keeps its place, as it does in `parse`.
    let parsed: Vec<Option<Message>> = raw_messages
        .par_iter()
        .map(|raw| parse_message(raw).ok())
        .collect();

    let mut current = match sources::load(out_dir, period)? {
        Some(known) if known.articles.len() == raw_messages.len() => known,
        // No file yet, or the raw file has changed length since: index it.
        stale => {
            let url = format!("{}/{}/date.html", base, period);
            let index = match budget.ask(&url, non_empty_index) {
                Asked::Got(index) => index,
                // No index, so nothing is known about this period: its file
                // is left as it is, or left unwritten, and the index is
                // asked for again on a later run.
                Asked::Failed => {
                    tally(report, stale.as_ref());
                    return Ok(None);
                }
                Asked::Stop(reason) => {
                    tally(report, stale.as_ref());
                    return Ok(Some(reason));
                }
            };
            report.periods_indexed += 1;
            if index.len() != raw_messages.len() {
                eprintln!(
                    "Note: {}: the index lists {} pages and the raw file has {} messages",
                    period,
                    index.len(),
                    raw_messages.len()
                );
            }
            if index.len() > raw_messages.len() {
                report.index_ahead += 1;
            }
            let keys: Vec<String> = raw_messages
                .iter()
                .map(|raw| subject_key(&raw_subject(raw)))
                .collect();
            let mut articles = align(&keys, &index);
            // A page already fetched stays fetched for as long as the index
            // lists it, whether or not a message is linked to it just now:
            // the index can be ahead of the raw file, and the messages it
            // unlinks are linked again once the raw file has caught up.
            let listed: HashSet<&str> = index.iter().map(|e| e.number.as_str()).collect();
            let (mut fixes, mut rejected) = match stale {
                Some(stale) => (stale.fixes, stale.rejected),
                None => Default::default(),
            };
            fixes.retain(|number, _| listed.contains(number.as_str()));
            rejected.retain(|number, _| listed.contains(number.as_str()));
            // A page that has said whose it is goes to that message only.
            // Pairing by subject would otherwise hand it back to the message
            // it was taken from, and nothing would look at it again.
            for (i, article) in articles.iter_mut().enumerate() {
                let named = article.as_ref().and_then(|number| rejected.get(number));
                if let Some(named) = named {
                    let own = parsed[i].as_ref().map(own_message_id);
                    if own != Some(named.as_str()) {
                        *article = None;
                    }
                }
            }
            let indexed = PeriodSources {
                period: period.to_string(),
                articles,
                fixes,
                rejected,
            };
            save_period(out_dir, &indexed)?;
            indexed
        }
    };

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
                if let Some(named) = other_message_named(&page, msg) {
                    eprintln!(
                        "Warning: {}/{}.html is not the page of message {} in the raw file, unlinking it",
                        period,
                        number,
                        i + 1
                    );
                    current.articles[i] = None;
                    // Written down, so that indexing the period again does
                    // not pair the two a second time.
                    current.rejected.insert(number, named);
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
            // The message keeps its page number and gets no fix, so its
            // page is asked for again on a later run.
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
    // What the period's file says now, whether or not this run changed it.
    tally(report, Some(&current));
    Ok(stop)
}

/// Adds what a period's file on disk says to the report's totals.
fn tally(report: &mut SourcesReport, on_file: Option<&PeriodSources>) {
    if let Some(file) = on_file {
        report.messages += file.articles.len();
        report.unlinked += file.articles.iter().filter(|a| a.is_none()).count();
        report.rejected += file.rejected.len();
    }
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

/// A message's own Message-ID, without angle brackets, as a page names it.
fn own_message_id(msg: &Message) -> &str {
    msg.message_id
        .trim()
        .trim_start_matches('<')
        .trim_end_matches('>')
}

/// The Message-ID the page names, when it names one and it is not this
/// message's. `None` when the page is this message's, or cannot say.
fn other_message_named(page: &ArticlePage, msg: &Message) -> Option<String> {
    let own = own_message_id(msg);
    match &page.message_id {
        Some(id) if !own.is_empty() && id != own => Some(id.clone()),
        _ => None,
    }
}
