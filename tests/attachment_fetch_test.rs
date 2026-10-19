mod common;

use std::fs;
use std::path::{Path, PathBuf};

use common::{raw_message, scratch_dir, scrubbed_notice, FakeAttachments, BASE};
use rmail_parser::attachment_fetch::{run_attachments, AttachmentsOptions, AttachmentsReport};
use rmail_parser::attachments;
use rmail_parser::source_fetch::not_the_file;

fn url(rest: &str) -> String {
    format!("{}/attachments/{}", BASE, rest)
}

const A: &str = "20050301/aaaaaaaa/attachment.txt";
const B: &str = "20100210/75317b7f/attachment.pl";

/// A raw directory with one file per `(period, notice URLs)`.
fn raw_dir(name: &str, periods: &[(&str, &[String])]) -> PathBuf {
    let raw = scratch_dir(name);
    for (period, urls) in periods {
        let body: Vec<String> = urls.iter().map(|u| scrubbed_notice(u)).collect();
        let text = raw_message(1, "A subject", &body.join("\n"));
        fs::write(raw.join(format!("{}.mbox", period)), text).unwrap();
    }
    raw
}

fn run_with(
    raw: &Path,
    out: &Path,
    base: &str,
    opts: &AttachmentsOptions,
    source: &mut FakeAttachments,
) -> AttachmentsReport {
    run_attachments(raw, out, base, opts, source).unwrap()
}

fn run(raw: &Path, out: &Path, source: &mut FakeAttachments) -> AttachmentsReport {
    run_with(raw, out, BASE, &AttachmentsOptions::default(), source)
}

fn two_periods(name: &str) -> PathBuf {
    raw_dir(
        name,
        &[("2010-February", &[url(B)]), ("2005-March", &[url(A)])],
    )
}

#[test]
fn fetches_what_is_missing_oldest_period_first() {
    let raw = two_periods("att-first-raw");
    let out = scratch_dir("att-first-out");
    let mut source = FakeAttachments::new();
    source.serve(&url(A), b"plain text");
    source.serve(&url(B), "caf\u{e9}".as_bytes());

    // A trailing slash on the base, as the command line passes it.
    let base = format!("{}/", BASE);
    let report = run_with(
        &raw,
        &out,
        &base,
        &AttachmentsOptions::default(),
        &mut source,
    );

    assert_eq!(source.asked, [url(A), url(B)]);
    assert_eq!(report.periods, 2);
    assert_eq!(report.found, 2);
    assert_eq!(report.fetched, 2);
    assert_eq!(report.left, 0);
    assert_eq!(report.stopped, None);

    let march = attachments::load(&out, "2005-March").unwrap().unwrap();
    let file = &march.files[A];
    assert_eq!(file.text.as_deref(), Some("plain text"));
    assert_eq!(file.encoding.as_deref(), Some("ascii"));
    assert_eq!(file.url, url(A));
    let feb = attachments::load(&out, "2010-February").unwrap().unwrap();
    assert_eq!(feb.files[B].text.as_deref(), Some("caf\u{e9}"));
    assert_eq!(feb.files[B].encoding.as_deref(), Some("utf-8"));
}

#[test]
fn a_second_run_asks_for_nothing() {
    let raw = two_periods("att-second-raw");
    let out = scratch_dir("att-second-out");
    let mut source = FakeAttachments::new();
    source.serve(&url(A), b"one");
    source.serve(&url(B), b"two");
    run(&raw, &out, &mut source);
    let before = fs::read(out.join("2005-March.json.gz")).unwrap();

    let mut again = FakeAttachments::new();
    let report = run(&raw, &out, &mut again);

    assert!(again.asked.is_empty());
    assert_eq!(report.stored_before, 2);
    assert_eq!(report.fetched, 0);
    assert_eq!(report.left, 0);
    assert_eq!(fs::read(out.join("2005-March.json.gz")).unwrap(), before);
}

#[test]
fn the_cap_is_kept() {
    let urls: Vec<String> = ["1", "2", "3"]
        .iter()
        .map(|n| url(&format!("2010021{}/75317b7f/attachment.txt", n)))
        .collect();
    let raw = raw_dir("att-cap-raw", &[("2010-February", &urls)]);
    let out = scratch_dir("att-cap-out");
    let mut source = FakeAttachments::new();
    for u in &urls {
        source.serve(u, b"x");
    }
    let opts = AttachmentsOptions {
        max_requests: 2,
        ..AttachmentsOptions::default()
    };

    let report = run_with(&raw, &out, BASE, &opts, &mut source);

    assert_eq!(source.asked.len(), 2);
    assert_eq!(report.fetched, 2);
    assert_eq!(report.left, 1);
    assert!(report.stopped.is_some());
    let feb = attachments::load(&out, "2010-February").unwrap().unwrap();
    assert_eq!(feb.files.len(), 2);
}

#[test]
fn not_found_is_recorded_and_not_asked_again() {
    let cut = url("20100210/75317b7f/attachment.p");
    let raw = raw_dir("att-404-raw", &[("2010-February", &[url(B), cut.clone()])]);
    let out = scratch_dir("att-404-out");
    let mut source = FakeAttachments::new();

    let report = run(&raw, &out, &mut source);

    assert_eq!(source.asked.len(), 2);
    assert_eq!(report.not_found, 2);
    assert_eq!(report.left, 0);
    assert_eq!(report.stopped, None);
    let feb = attachments::load(&out, "2010-February").unwrap().unwrap();
    for key in [B, "20100210/75317b7f/attachment.p"] {
        assert_eq!(feb.files[key].status.as_deref(), Some("not found"));
        assert_eq!(feb.files[key].text, None);
    }

    let mut again = FakeAttachments::new();
    let report = run(&raw, &out, &mut again);
    assert!(again.asked.is_empty());
    assert_eq!(report.stored_before, 2);
}

#[test]
fn a_failure_is_not_recorded() {
    let raw = raw_dir("att-fail-raw", &[("2010-February", &[url(B)])]);
    let out = scratch_dir("att-fail-out");
    let mut source = FakeAttachments::new();
    source.serve(&url(B), b"text");
    source.fail(&url(B));

    let report = run(&raw, &out, &mut source);

    assert_eq!(report.failed, 1);
    assert_eq!(report.left, 1);
    assert_eq!(report.stopped, None);
    assert!(attachments::load(&out, "2010-February").unwrap().is_none());

    source.stop_failing();
    let report = run(&raw, &out, &mut source);
    assert_eq!(source.asked.len(), 2);
    assert_eq!(report.fetched, 1);
    let feb = attachments::load(&out, "2010-February").unwrap().unwrap();
    assert_eq!(feb.files[B].text.as_deref(), Some("text"));
}

#[test]
fn ten_failures_in_a_row_stop_the_run() {
    let urls: Vec<String> = (0..11)
        .map(|n| url(&format!("201002{:02}/75317b7f/attachment.txt", n + 1)))
        .collect();
    let raw = raw_dir("att-ten-raw", &[("2010-February", &urls)]);
    let out = scratch_dir("att-ten-out");
    let mut source = FakeAttachments::new();
    for u in &urls {
        source.fail(u);
    }

    let report = run(&raw, &out, &mut source);

    assert_eq!(source.asked.len(), 10);
    assert_eq!(report.failed, 10);
    assert_eq!(report.left, 11);
    assert!(report.stopped.unwrap().contains("failed"));
}

#[test]
fn too_large_is_recorded() {
    let raw = raw_dir("att-large-raw", &[("2010-February", &[url(B)])]);
    let out = scratch_dir("att-large-out");
    let mut source = FakeAttachments::new();
    source.serve(&url(B), &[b'x'; 11]);
    let opts = AttachmentsOptions {
        max_bytes: 10,
        ..AttachmentsOptions::default()
    };

    let report = run_with(&raw, &out, BASE, &opts, &mut source);

    assert_eq!(report.too_large, 1);
    assert_eq!(report.left, 0);
    let feb = attachments::load(&out, "2010-February").unwrap().unwrap();
    assert_eq!(feb.files[B].status.as_deref(), Some("too large"));
    assert_eq!(feb.files[B].text, None);
}

#[test]
fn outside_the_archive_is_refused() {
    let elsewhere = format!("https://elsewhere.example/attachments/{}", B);
    let raw = raw_dir("att-refuse-raw", &[("2010-February", &[elsewhere])]);
    let out = scratch_dir("att-refuse-out");
    let mut source = FakeAttachments::new();

    let report = run(&raw, &out, &mut source);

    assert!(source.asked.is_empty());
    assert_eq!(report.found, 1);
    assert_eq!(report.refused, 1);
    assert_eq!(report.left, 0);
    assert!(attachments::load(&out, "2010-February").unwrap().is_none());
}

#[test]
fn http_and_https_are_one_archive() {
    let http = format!(
        "http://lists.example.org/pipermail/r-devel/attachments/{}",
        B
    );
    let raw = raw_dir("att-scheme-raw", &[("2010-February", &[http])]);
    let out = scratch_dir("att-scheme-out");
    let mut source = FakeAttachments::new();
    source.serve(&url(B), b"text");

    let report = run(&raw, &out, &mut source);

    assert_eq!(source.asked, [url(B)]);
    assert_eq!(report.refused, 0);
    assert_eq!(report.fetched, 1);
    let feb = attachments::load(&out, "2010-February").unwrap().unwrap();
    assert_eq!(feb.files[B].url, url(B));
}

#[test]
fn a_quoted_notice_asks_nothing() {
    let raw = scratch_dir("att-quoted-raw");
    let quoted: String = scrubbed_notice(&url(B))
        .lines()
        .map(|l| format!("> {}\n", l))
        .collect();
    fs::write(
        raw.join("2010-February.mbox"),
        raw_message(1, "Re: a subject", &quoted),
    )
    .unwrap();
    let out = scratch_dir("att-quoted-out");
    let mut source = FakeAttachments::new();

    let report = run(&raw, &out, &mut source);

    assert!(source.asked.is_empty());
    assert_eq!(report.periods, 1);
    assert_eq!(report.found, 0);
}

#[test]
fn the_store_holds_no_whole_address() {
    use std::io::Read;

    let raw = raw_dir("att-addr-raw", &[("2010-February", &[url(B)])]);
    let out = scratch_dir("att-addr-out");
    let mut source = FakeAttachments::new();
    source.serve(&url(B), b"On Tue, Al <al.smith@example.org> wrote:");
    run(&raw, &out, &mut source);

    let feb = attachments::load(&out, "2010-February").unwrap().unwrap();
    assert_eq!(
        feb.files[B].text.as_deref(),
        Some("On Tue, Al <al.smith at example.org> wrote:")
    );
    let gz = fs::read(out.join("2010-February.json.gz")).unwrap();
    let mut json = String::new();
    flate2::read::GzDecoder::new(gz.as_slice())
        .read_to_string(&mut json)
        .unwrap();
    assert!(!json.contains("@example.org"));
}

/// `n` notice URLs of the right shape, all different, in one period.
fn many(n: usize) -> Vec<String> {
    (1..=n)
        .map(|i| url(&format!("{:08}/75317b7f/attachment.txt", 20100000 + i)))
        .collect()
}

fn not_found_entries(p: &attachments::PeriodAttachments) -> usize {
    p.files
        .values()
        .filter(|f| f.status.as_deref() == Some("not found"))
        .count()
}

#[test]
fn ten_not_found_in_a_row_stop_the_run_and_record_nothing() {
    let urls = many(11);
    let raw = raw_dir("att-ten-404-raw", &[("2010-February", &urls)]);
    let out = scratch_dir("att-ten-404-out");
    // Serves nothing: every request is a 404, as from a source that has
    // moved its files or turned us away.
    let mut source = FakeAttachments::new();

    let report = run(&raw, &out, &mut source);

    assert_eq!(source.asked.len(), 10);
    assert_eq!(
        report.stopped.as_deref(),
        Some("10 requests in a row failed")
    );
    assert_eq!(report.not_found, 0);
    assert_eq!(report.failed, 10);
    assert_eq!(report.left, 11);
    assert!(attachments::load(&out, "2010-February").unwrap().is_none());

    // So the next run asks for every one of them again.
    let mut again = FakeAttachments::new();
    for u in &urls {
        again.serve(u, b"back");
    }
    let report = run(&raw, &out, &mut again);
    assert_eq!(again.asked.len(), 11);
    assert_eq!(report.fetched, 11);
}

#[test]
fn nine_not_found_then_a_file_are_recorded_and_the_run_goes_on() {
    let urls = many(11);
    let raw = raw_dir("att-nine-404-raw", &[("2010-February", &urls)]);
    let out = scratch_dir("att-nine-404-out");
    let mut source = FakeAttachments::new();
    source.serve(&urls[9], b"found");
    source.serve(&urls[10], b"found too");

    let report = run(&raw, &out, &mut source);

    assert_eq!(source.asked.len(), 11);
    assert_eq!(report.stopped, None);
    assert_eq!(report.not_found, 9);
    assert_eq!(report.fetched, 2);
    assert_eq!(report.left, 0);
    let feb = attachments::load(&out, "2010-February").unwrap().unwrap();
    assert_eq!(not_found_entries(&feb), 9);
    assert_eq!(feb.files.len(), 11);
}

#[test]
fn a_file_starts_the_count_of_not_found_again() {
    let urls = many(20);
    let raw = raw_dir("att-reset-raw", &[("2010-February", &urls)]);
    let out = scratch_dir("att-reset-out");
    let mut source = FakeAttachments::new();
    source.serve(&urls[9], b"one");
    source.serve(&urls[19], b"two");

    let report = run(&raw, &out, &mut source);

    assert_eq!(source.asked.len(), 20);
    assert_eq!(report.stopped, None);
    assert_eq!(report.not_found, 18);
    assert_eq!(report.fetched, 2);
}

#[test]
fn a_run_of_not_found_across_two_periods_is_forgotten_in_both() {
    let march: Vec<String> = (1..=6)
        .map(|i| url(&format!("200503{:02}/aaaaaaaa/attachment.txt", i)))
        .collect();
    let feb = many(6);
    let raw = raw_dir(
        "att-across-raw",
        &[("2005-March", &march), ("2010-February", &feb)],
    );
    let out = scratch_dir("att-across-out");
    let mut source = FakeAttachments::new();
    // March, taken first, starts with a file; its five 404s are written with
    // it when the period is done, before the run of 404s ends in February.
    source.serve(&march[0], b"kept");

    let report = run(&raw, &out, &mut source);

    assert_eq!(source.asked.len(), 11);
    assert_eq!(
        report.stopped.as_deref(),
        Some("10 requests in a row failed")
    );
    assert_eq!(report.fetched, 1);
    assert_eq!(report.not_found, 0);
    assert_eq!(report.left, 11);
    let stored_march = attachments::load(&out, "2005-March").unwrap().unwrap();
    assert_eq!(stored_march.files.len(), 1);
    assert_eq!(
        stored_march.files["20050301/aaaaaaaa/attachment.txt"]
            .text
            .as_deref(),
        Some("kept")
    );
    assert!(attachments::load(&out, "2010-February").unwrap().is_none());
}

#[test]
fn a_period_file_that_held_only_forgotten_not_found_is_removed() {
    let march: Vec<String> = (1..=5)
        .map(|i| url(&format!("200503{:02}/aaaaaaaa/attachment.txt", i)))
        .collect();
    let raw = raw_dir(
        "att-emptied-raw",
        &[("2005-March", &march), ("2010-February", &many(5))],
    );
    let out = scratch_dir("att-emptied-out");
    let mut source = FakeAttachments::new();

    let report = run(&raw, &out, &mut source);

    assert_eq!(source.asked.len(), 10);
    assert!(report.stopped.is_some());
    assert_eq!(fs::read_dir(&out).unwrap().count(), 0);
}

#[test]
fn a_run_of_not_found_already_written_mid_period_is_taken_out() {
    // A period's file is written every 50 new entries: here after 45 files
    // and the first five of the 404s.
    let urls = many(56);
    let raw = raw_dir("att-midsave-raw", &[("2010-February", &urls)]);
    let out = scratch_dir("att-midsave-out");
    let mut source = FakeAttachments::new();
    for u in &urls[..45] {
        source.serve(u, b"x");
    }

    let report = run(&raw, &out, &mut source);

    assert_eq!(source.asked.len(), 55);
    assert!(report.stopped.is_some());
    assert_eq!(report.fetched, 45);
    assert_eq!(report.left, 11);
    let feb = attachments::load(&out, "2010-February").unwrap().unwrap();
    assert_eq!(feb.files.len(), 45);
    assert_eq!(not_found_entries(&feb), 0);
}

#[test]
fn the_store_keeps_the_text_as_fetched() {
    let raw = raw_dir("att-crlf-raw", &[("2010-February", &[url(B)])]);
    let out = scratch_dir("att-crlf-out");
    let mut source = FakeAttachments::new();
    source.serve(&url(B), b"one\r\ntwo\r\n");

    run(&raw, &out, &mut source);

    let feb = attachments::load(&out, "2010-February").unwrap().unwrap();
    assert_eq!(feb.files[B].text.as_deref(), Some("one\r\ntwo\r\n"));
}

#[test]
fn a_url_that_climbs_out_of_the_archive_is_refused() {
    let climbing = format!("{}/attachments/../../other/attachments/{}", BASE, B);
    let raw = raw_dir("att-climb-raw", &[("2010-February", &[climbing])]);
    let out = scratch_dir("att-climb-out");
    let mut source = FakeAttachments::new();

    let report = run(&raw, &out, &mut source);

    assert!(source.asked.is_empty(), "{:?}", source.asked);
    assert_eq!(report.found, 1);
    assert_eq!(report.refused, 1);
    assert_eq!(report.left, 0);
    assert!(attachments::load(&out, "2010-February").unwrap().is_none());
}

#[test]
fn a_nested_attachments_url_is_refused() {
    let nested = format!("{}/attachments/x/attachments/{}", BASE, B);
    let raw = raw_dir("att-nested-raw", &[("2010-February", &[nested])]);
    let out = scratch_dir("att-nested-out");
    let mut source = FakeAttachments::new();

    let report = run(&raw, &out, &mut source);

    assert!(source.asked.is_empty(), "{:?}", source.asked);
    assert_eq!(report.refused, 1);
}

#[test]
fn a_url_of_the_wrong_shape_is_not_found_at_all() {
    let wrong = [
        url("20100210/75317B7F/attachment.pl"),
        url("2010021/75317b7f/attachment.pl"),
    ];
    let raw = raw_dir("att-shape-raw", &[("2010-February", &wrong)]);
    let out = scratch_dir("att-shape-out");
    let mut source = FakeAttachments::new();

    let report = run(&raw, &out, &mut source);

    assert!(source.asked.is_empty());
    assert_eq!(report.periods, 1);
    assert_eq!(report.found, 0);
    assert_eq!(report.refused, 0);
}

#[test]
fn a_refused_notice_does_not_hide_a_real_one_with_its_key() {
    let climbing = format!("{}/attachments/../../other/attachments/{}", BASE, B);
    let raw = raw_dir(
        "att-same-key-raw",
        &[("2010-February", &[climbing, url(B)])],
    );
    let out = scratch_dir("att-same-key-out");
    let mut source = FakeAttachments::new();
    source.serve(&url(B), b"the real one");

    let report = run(&raw, &out, &mut source);

    assert_eq!(source.asked, [url(B)]);
    assert_eq!(report.found, 2);
    assert_eq!(report.refused, 1);
    assert_eq!(report.fetched, 1);
    let feb = attachments::load(&out, "2010-February").unwrap().unwrap();
    assert_eq!(feb.files[B].url, url(B));
    assert_eq!(feb.files[B].text.as_deref(), Some("the real one"));
}

// What the real fetcher takes for the file it asked for. It cannot be faked,
// so its rule is tested on its own.

const ASKED: &str =
    "https://lists.example.org/pipermail/r-devel/attachments/20100210/75317b7f/attachment.pl";

#[test]
fn an_answer_from_the_url_asked_for_is_the_file() {
    for content_type in [
        "text/plain",
        "text/plain; charset=us-ascii",
        "application/octet-stream",
        "",
    ] {
        assert_eq!(
            not_the_file(ASKED, ASKED, content_type),
            None,
            "{content_type}"
        );
    }
}

#[test]
fn a_redirect_is_not_the_file() {
    let elsewhere = "https://lists.example.org/pipermail/r-devel/";
    let why = not_the_file(ASKED, elsewhere, "text/plain").unwrap();
    assert!(why.contains(elsewhere), "{why}");
}

#[test]
fn an_html_page_for_a_file_that_is_not_one_is_not_the_file() {
    for content_type in [
        "text/html",
        "text/html; charset=utf-8",
        "Text/HTML ; charset=x",
    ] {
        assert!(
            not_the_file(ASKED, ASKED, content_type).is_some(),
            "{content_type}"
        );
    }
    for asked in [
        ASKED.replace("attachment.pl", "attachment.html"),
        ASKED.replace("attachment.pl", "attachment.htm"),
    ] {
        assert_eq!(not_the_file(&asked, &asked, "text/html"), None, "{asked}");
    }
}
