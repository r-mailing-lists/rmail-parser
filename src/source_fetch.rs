use std::collections::{BTreeMap, HashSet};
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
use crate::source_index::{parse_index_by_date, subject_key, IndexEntry};
use crate::source_page::{fix_string, page_shows, parse_article, ArticlePage};
use crate::sources::{self, needs_page, period_of, period_sort_key, PeriodSources};

const USER_AGENT: &str = "rmail-parser (+https://github.com/r-mailing-lists)";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// The largest page `HttpFetcher::get` reads. ureq's own `into_string`
/// stops at 10 MB, and one message page of r-sig-mixed-models (2013q3,
/// 021019.html) is past that: its whole attachment is written into it.
const MAX_PAGE_BYTES: u64 = 64 * 1024 * 1024;

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
                let mut bytes = Vec::new();
                response
                    .into_reader()
                    .take(MAX_PAGE_BYTES + 1)
                    .read_to_end(&mut bytes)
                    .map_err(|e| anyhow!("could not read the response: {}", e))?;
                if bytes.len() as u64 > MAX_PAGE_BYTES {
                    return Err(anyhow!(
                        "could not read the response: over {} MB",
                        MAX_PAGE_BYTES / (1024 * 1024)
                    ));
                }
                Ok(Fetched::Body(String::from_utf8_lossy(&bytes).into_owned()))
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
    /// Pairings of periods archived more than once that wait for their page
    /// to be fetched and found to be the message's (`PeriodSources::pending`).
    pub pending: usize,
    /// Pairings of that kind this run found to be right, and linked.
    pub confirmed: usize,
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

    let later = sources::later_copies(
        parsed
            .iter()
            .map(|msg| msg.as_ref().map(|msg| msg.message_id.as_str())),
    );
    let firsts: Vec<usize> = (0..raw_messages.len()).filter(|&i| !later[i]).collect();
    // The messages the period's messages answer, as a page archived before
    // about 2008 names them.
    let parents: HashSet<&str> = parsed
        .iter()
        .flatten()
        .filter_map(|msg| msg.in_reply_to.as_deref())
        .map(bare_id)
        .collect();

    let mut current = match sources::load(out_dir, period)? {
        Some(known)
            if known.articles.len() == raw_messages.len()
                && (known.pending.is_empty() || known.pending.len() == raw_messages.len())
                && !known
                    .articles
                    .iter()
                    .zip(&later)
                    .any(|(a, later)| *later && a.is_some())
                && (known.parents_rechecked
                    || !known
                        .rejected
                        .values()
                        .any(|named| parents.contains(named.as_str()))) =>
        {
            known
        }
        // No file yet, the raw file has changed length since, the file gives
        // a later copy a page, as versions before 0.13.0 could, or it rejects
        // a page for naming a message's parent, as versions before 0.14.0
        // could: index it.
        stale => {
            let url = format!("{}/{}/date.html", base, period);
            let (index, by_date) = match budget.ask(&url, non_empty_index) {
                Asked::Got(both) => both,
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
            if index.len() != firsts.len() {
                eprintln!(
                    "Note: {}: the index lists {} pages and the raw file has {} messages",
                    period,
                    index.len(),
                    firsts.len()
                );
            }
            if index.len() > firsts.len() {
                report.index_ahead += 1;
            }
            // Only first copies are matched. A later copy has the same
            // subject as its first, and either could take the page; the
            // first is the message as it was sent.
            let keys: Vec<String> = firsts
                .iter()
                .map(|&i| subject_key(&raw_subject(raw_messages[i])))
                .collect();
            let mut articles = vec![None; raw_messages.len()];
            for (&i, article) in firsts.iter().zip(align(&keys, &index)) {
                articles[i] = article;
            }
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
            // A page archived before about 2008 names in its reply link the
            // message its own message answers. A rejection for naming the
            // parent of the message it is paired with again was such a page:
            // the pairing stands, and the page is fetched again and judged by
            // its text (`other_message_named`).
            for (i, article) in articles.iter().enumerate() {
                if let (Some(number), Some(msg)) = (article, &parsed[i]) {
                    let parent = msg.in_reply_to.as_deref().map(bare_id);
                    if parent.is_some() && rejected.get(number).map(String::as_str) == parent {
                        rejected.remove(number);
                    }
                }
            }
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
            // A period archived a second time can have its pages numbered
            // in another order than its raw file: the copies that made them
            // were archived later, among other periods' messages. Matched
            // in page order, its messages then go to the wrong pages about
            // as often as they find none. When more than a fifth of them
            // find none, the order is not the raw file's, and the period
            // gets no links at all: no link is better than a wrong one.
            //
            // Such a period's messages are paired again in the order the index
            // lists its pages, which is by date, and each pairing waits in
            // `pending` until its page has been fetched and found to be the
            // message's.
            let unlinked = firsts.iter().filter(|&&i| articles[i].is_none()).count();
            let mut pending = Vec::new();
            if firsts.len() < raw_messages.len() && unlinked * 5 > firsts.len() {
                articles.iter_mut().for_each(|article| *article = None);
                pending = by_date_pairs(&firsts, &parsed, raw_messages, &by_date, &rejected);
                eprintln!(
                    "Note: {}: archived more than once, and {} of its {} messages found no page in the index's order; {} paired by date, each linked once its page is found to be its own",
                    period,
                    unlinked,
                    firsts.len(),
                    pending.iter().filter(|p| p.is_some()).count()
                );
            }
            // Only a file with rejections needs to say they were looked at.
            let parents_rechecked = !rejected.is_empty();
            let indexed = PeriodSources {
                period: period.to_string(),
                articles,
                fixes,
                rejected,
                pending,
                parents_rechecked,
            };
            save_period(out_dir, &indexed)?;
            indexed
        }
    };

    let mut unsaved = 0;
    let mut stop = None;
    for (i, msg) in parsed.iter().enumerate() {
        let Some(msg) = msg else { continue };
        // A linked message is fetched when its page may have a character to
        // restore; a pairing that waits is fetched whatever its body needs,
        // to see whether the page is the message's.
        let linked = current.articles.get(i).cloned().flatten();
        let waiting = current.pending.get(i).cloned().flatten();
        let (number, confirming) = match (linked, waiting) {
            (Some(number), _) => {
                if current.fixes.contains_key(&number) || !needs_page(&msg.body_plain) {
                    continue;
                }
                (number, false)
            }
            (None, Some(number)) => (number, true),
            _ => continue,
        };

        let url = format!("{}/{}/{}.html", base, period, number);
        match budget.ask(&url, parse_article) {
            Asked::Got(page) => {
                report.pages_fetched += 1;
                let named = if confirming {
                    current.pending[i] = None;
                    not_its_page(&page, msg)
                } else {
                    other_message_named(&page, msg)
                };
                if let Some(named) = named {
                    eprintln!(
                        "Warning: {}/{}.html is not the page of message {} in the raw file, {}",
                        period,
                        number,
                        i + 1,
                        if confirming {
                            "leaving it unlinked"
                        } else {
                            "unlinking it"
                        }
                    );
                    current.articles[i] = None;
                    // Written down, so that indexing the period again does
                    // not pair the two a second time. Judged by this version,
                    // which already looked at a parent the page may name.
                    current.rejected.insert(number, named);
                    current.parents_rechecked = true;
                    report.mismatches += 1;
                } else {
                    if confirming {
                        current.articles[i] = Some(number.clone());
                        report.confirmed += 1;
                    }
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
    if current.pending.iter().all(Option::is_none) && !current.pending.is_empty() {
        current.pending.clear();
        unsaved += 1;
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
        report.pending += file.pending.iter().filter(|p| p.is_some()).count();
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
/// page or a page cut short, not an index. The entries by page number, and
/// as the page lists them, by date.
fn non_empty_index(html: &str) -> Option<(Vec<IndexEntry>, Vec<IndexEntry>)> {
    let by_date = parse_index_by_date(html);
    if by_date.is_empty() {
        return None;
    }
    let mut by_number = by_date.clone();
    by_number.sort_by_key(|e| e.number.parse::<u64>().unwrap_or(u64::MAX));
    Some((by_number, by_date))
}

/// Pairs the first copies of a period archived more than once with its
/// index in the order the index lists them, by date: the first copies are
/// taken in the order of their Date, and a message that does not parse is
/// left out. One entry per raw message, as `articles` has. A page that has
/// said whose it is goes to no other message.
fn by_date_pairs(
    firsts: &[usize],
    parsed: &[Option<Message>],
    raw_messages: &[&str],
    by_date: &[IndexEntry],
    rejected: &BTreeMap<String, String>,
) -> Vec<Option<String>> {
    let mut order: Vec<usize> = firsts
        .iter()
        .copied()
        .filter(|&i| parsed[i].is_some())
        .collect();
    order.sort_by_key(|&i| (parsed[i].as_ref().map(|msg| msg.date), i));
    let keys: Vec<String> = order
        .iter()
        .map(|&i| subject_key(&raw_subject(raw_messages[i])))
        .collect();
    let mut pending = vec![None; raw_messages.len()];
    for (&i, article) in order.iter().zip(align(&keys, by_date)) {
        if let Some(number) = article {
            let own = parsed[i].as_ref().map(own_message_id);
            match rejected.get(&number) {
                Some(named) if Some(named.as_str()) != own => {}
                _ => pending[i] = Some(number),
            }
        }
    }
    pending
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

/// A Message-ID without its angle brackets, as a page names it.
fn bare_id(id: &str) -> &str {
    id.trim().trim_start_matches('<').trim_end_matches('>')
}

/// A message's own Message-ID, without angle brackets, as a page names it.
fn own_message_id(msg: &Message) -> &str {
    bare_id(&msg.message_id)
}

/// The Message-ID the page names, when it names one and it is not this
/// message's. `None` when the page is this message's, or cannot say.
///
/// A page archived before about 2008 names in its reply link the message
/// its own message answers. A page that names this message's parent is taken
/// for its own when it shows its text (`page_shows`).
fn other_message_named(page: &ArticlePage, msg: &Message) -> Option<String> {
    let own = own_message_id(msg);
    match &page.message_id {
        Some(id) if !own.is_empty() && id != own => {
            let parent = msg.in_reply_to.as_deref().map(bare_id);
            if parent == Some(id.as_str()) && page_shows(&msg.body_plain, &page.text) {
                None
            } else {
                Some(id.clone())
            }
        }
        _ => None,
    }
}

/// For a pairing that waits to be confirmed: the Message-ID to reject the
/// page with, unless the page is shown to be this message's. A page that
/// names this message is its; one that names no message, or the one it
/// answers, is when it shows its text. Every other page is another's, and
/// so is one that cannot say and does not show it ("" when it named none).
fn not_its_page(page: &ArticlePage, msg: &Message) -> Option<String> {
    let own = own_message_id(msg);
    let parent = msg.in_reply_to.as_deref().map(bare_id);
    match page.message_id.as_deref() {
        Some(id) if !own.is_empty() && id == own => None,
        Some(id) if parent == Some(id) || own.is_empty() => {
            if page_shows(&msg.body_plain, &page.text) {
                None
            } else {
                Some(id.to_string())
            }
        }
        Some(id) => Some(id.to_string()),
        None => {
            if page_shows(&msg.body_plain, &page.text) {
                None
            } else {
                Some(String::new())
            }
        }
    }
}
