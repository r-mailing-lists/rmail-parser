mod common;

use std::fs;
use std::path::{Path, PathBuf};

use common::{raw_message, scratch_dir, scrubbed_notice, FakeAttachments, BASE};
use rmail_parser::attachment_fetch::{run_attachments, AttachmentsOptions, AttachmentsReport};
use rmail_parser::attachments;

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
