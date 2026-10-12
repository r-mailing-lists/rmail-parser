mod common;

use std::fs;
use std::path::{Path, PathBuf};

use common::{article_page, fixture_path, index_page, raw_message, scratch_dir, FakeFetcher, BASE};
use rmail_parser::source_fetch::{run_sources, Fetched, Fetcher, SourcesOptions, SourcesReport};
use rmail_parser::sources::{self, PeriodSources};

fn run(raw: &Path, out: &Path, source: &mut FakeFetcher) -> SourcesReport {
    run_sources(raw, out, BASE, &SourcesOptions::default(), source).unwrap()
}

fn saved(out: &Path, period: &str) -> PeriodSources {
    sources::load(out, period).unwrap().unwrap()
}

fn pages(saved: &PeriodSources) -> Vec<Option<&str>> {
    saved.articles.iter().map(|a| a.as_deref()).collect()
}

/// A raw directory holding the October fixture.
fn october_raw(name: &str) -> PathBuf {
    let raw = scratch_dir(name);
    fs::copy(
        fixture_path("raw/2026-October.mbox"),
        raw.join("2026-October.mbox"),
    )
    .unwrap();
    raw
}

fn october_source() -> FakeFetcher {
    let mut source = FakeFetcher::new();
    source.serve_fixture("2026-October/date.html");
    source.serve_fixture("2026-October/084715.html");
    source.serve_fixture("2026-October/084716.html");
    source
}

#[test]
fn test_first_run_indexes_the_period_and_fetches_only_the_pages_it_needs() {
    let raw = october_raw("first-raw");
    let out = scratch_dir("first-out");
    let mut source = october_source();

    // A trailing slash on the base URL, as the workflows write it.
    let base = format!("{}/", BASE);
    let report = run_sources(&raw, &out, &base, &SourcesOptions::default(), &mut source).unwrap();

    // Messages 1 and 4 have no `?` between letters, so their pages are not asked for.
    assert_eq!(
        source.asked_paths(),
        [
            "2026-October/date.html",
            "2026-October/084715.html",
            "2026-October/084716.html"
        ]
    );
    assert_eq!(
        fs::read_to_string(out.join("2026-October.json")).unwrap(),
        "{\n  \"period\": \"2026-October\",\n  \"articles\": [\n    \"084714\",\n    \"084715\",\n    \"084716\",\n    \"084717\"\n  ],\n  \"fixes\": {\n    \"084715\": \"\",\n    \"084716\": \"\u{f1}\u{e4}\u{2019}??\"\n  },\n  \"rejected\": {}\n}\n"
    );
    assert_eq!(
        report,
        SourcesReport {
            periods_indexed: 1,
            pages_fetched: 2,
            chars_restored: 3,
            chars_left: 4,
            mismatches: 0,
            astral_chars: 0,
            not_found: 0,
            failed: 0,
            index_ahead: 0,
            messages: 4,
            unlinked: 0,
            rejected: 0,
            periods_skipped: 0,
            stopped: None,
        }
    );
}

#[test]
fn test_second_run_asks_for_nothing_and_changes_nothing() {
    let raw = october_raw("second-raw");
    let out = scratch_dir("second-out");
    run(&raw, &out, &mut october_source());
    let before = fs::read(out.join("2026-October.json")).unwrap();

    let mut source = october_source();
    let report = run(&raw, &out, &mut source);

    assert!(source.asked.is_empty());
    // Nothing was done; the period's four messages are still counted.
    assert_eq!(
        report,
        SourcesReport {
            messages: 4,
            ..SourcesReport::default()
        }
    );
    assert!(before == fs::read(out.join("2026-October.json")).unwrap());
}

#[test]
fn test_periods_are_taken_newest_first_until_the_budget_is_spent() {
    let raw = scratch_dir("budget-raw");
    let out = scratch_dir("budget-out");
    let mut source = FakeFetcher::new();
    for period in ["2025-December", "2026-April", "2026-March"] {
        fs::write(
            raw.join(format!("{period}.mbox")),
            raw_message(1, "[Rd] Topic", "Plain text."),
        )
        .unwrap();
        source.serve(
            &format!("{period}/date.html"),
            &index_page(&[("000101", "[Rd] Topic")]),
        );
    }

    let opts = SourcesOptions {
        max_requests: 2,
        ..SourcesOptions::default()
    };
    let report = run_sources(&raw, &out, BASE, &opts, &mut source).unwrap();

    // April before March: by time, not by name.
    assert_eq!(
        source.asked_paths(),
        ["2026-April/date.html", "2026-March/date.html"]
    );
    assert_eq!(report.stopped.as_deref(), Some("request budget of 2 spent"));
    assert_eq!(report.periods_indexed, 2);
    assert!(out.join("2026-April.json").exists());
    assert!(out.join("2026-March.json").exists());
    assert!(!out.join("2025-December.json").exists());

    // The next run picks up the period that was left.
    let mut source2 = FakeFetcher::new();
    source2.serve(
        "2025-December/date.html",
        &index_page(&[("000101", "[Rd] Topic")]),
    );
    let report = run(&raw, &out, &mut source2);
    assert_eq!(source2.asked_paths(), ["2025-December/date.html"]);
    assert_eq!(report.stopped, None);
}

#[test]
fn test_quarterly_and_annual_periods_use_their_own_names() {
    for (name, periods, expected) in [
        (
            "quarterly",
            ["2025q4", "2026q1"],
            ["2026q1/date.html", "2025q4/date.html"],
        ),
        (
            "annual",
            ["1998", "1999"],
            ["1999/date.html", "1998/date.html"],
        ),
    ] {
        let raw = scratch_dir(&format!("{name}-raw"));
        let out = scratch_dir(&format!("{name}-out"));
        let mut source = FakeFetcher::new();
        for period in periods {
            fs::write(
                raw.join(format!("{period}.mbox")),
                raw_message(1, "[Rd] Topic", "Plain text."),
            )
            .unwrap();
            source.serve(
                &format!("{period}/date.html"),
                &index_page(&[("000101", "[Rd] Topic")]),
            );
        }
        run(&raw, &out, &mut source);
        assert_eq!(source.asked_paths(), expected);
        assert_eq!(pages(&saved(&out, periods[0])), [Some("000101")]);
    }
}

#[test]
fn test_an_index_that_is_not_found_writes_no_file_and_is_asked_for_again() {
    let raw = scratch_dir("no-index-raw");
    let out = scratch_dir("no-index-out");
    let mbox = raw_message(1, "[Rd] Topic A", "na?ve") + &raw_message(2, "[Rd] Topic B", "Plain.");
    fs::write(raw.join("2026-October.mbox"), mbox).unwrap();

    let mut source = FakeFetcher::new();
    let report = run(&raw, &out, &mut source);
    assert_eq!(source.asked_paths(), ["2026-October/date.html"]);
    assert_eq!(fs::read_dir(&out).unwrap().count(), 0);
    assert_eq!(
        report,
        SourcesReport {
            not_found: 1,
            ..SourcesReport::default()
        }
    );

    // The next run asks again. The source has the index by then, and the
    // period is indexed as it would have been the first time.
    let mut source = FakeFetcher::new();
    source.serve(
        "2026-October/date.html",
        &index_page(&[("000101", "[Rd] Topic A"), ("000102", "[Rd] Topic B")]),
    );
    source.serve(
        "2026-October/000101.html",
        &article_page("m1@example.org", "na&#239;ve"),
    );
    let report = run(&raw, &out, &mut source);
    assert_eq!(
        source.asked_paths(),
        ["2026-October/date.html", "2026-October/000101.html"]
    );
    let indexed = saved(&out, "2026-October");
    assert_eq!(pages(&indexed), [Some("000101"), Some("000102")]);
    assert_eq!(indexed.fixes["000101"], "\u{ef}");
    assert_eq!(report.periods_indexed, 1);
    assert_eq!(report.not_found, 0);
    assert_eq!(report.stopped, None);
}

/// Six messages that each need their page, with the index and pages for them.
fn six_damaged_messages(raw: &Path) -> FakeFetcher {
    damaged_messages(raw, 6, &[1, 2, 3, 4, 5, 6])
}

/// `count` messages that each need their page, written to `raw` as
/// `2026-February.mbox`, and a source that has the index for all of them
/// but only the pages of the messages numbered in `served`.
fn damaged_messages(raw: &Path, count: usize, served: &[usize]) -> FakeFetcher {
    let mut mbox = String::new();
    let mut entries = Vec::new();
    let mut source = FakeFetcher::new();
    for n in 1..=count {
        mbox.push_str(&raw_message(
            n,
            &format!("[Rd] Topic {n}"),
            &format!("A na?ve question, number {n}."),
        ));
        entries.push((format!("00010{n}"), format!("[Rd] Topic {n}")));
        if served.contains(&n) {
            source.serve(
                &format!("2026-February/00010{n}.html"),
                &article_page(
                    &format!("m{n}@example.org"),
                    &format!("A na&#239;ve question, number {n}."),
                ),
            );
        }
    }
    fs::write(raw.join("2026-February.mbox"), mbox).unwrap();
    let entries: Vec<(&str, &str)> = entries
        .iter()
        .map(|(n, s)| (n.as_str(), s.as_str()))
        .collect();
    source.serve("2026-February/date.html", &index_page(&entries));
    source
}

#[test]
fn test_source_going_down_part_way_through_a_period_keeps_what_was_fetched() {
    let raw = scratch_dir("down-raw");
    let out = scratch_dir("down-out");
    let mut source = six_damaged_messages(&raw);
    // The index and two pages arrive, then the source stops answering.
    source.down_after = Some(3);

    let report = run(&raw, &out, &mut source);

    // Three failures in a row end the run. That is not an error.
    assert_eq!(
        source.asked_paths(),
        [
            "2026-February/date.html",
            "2026-February/000101.html",
            "2026-February/000102.html",
            "2026-February/000103.html",
            "2026-February/000104.html",
            "2026-February/000105.html",
        ]
    );
    assert_eq!(
        report.stopped.as_deref(),
        Some("3 requests in a row failed")
    );
    assert_eq!(report.pages_fetched, 2);
    assert_eq!(report.failed, 3);
    assert_eq!(report.not_found, 0);

    // The period's file is valid and holds the two pages that did arrive.
    let partial = saved(&out, "2026-February");
    assert_eq!(partial.articles.len(), 6);
    assert_eq!(
        partial.fixes.keys().collect::<Vec<_>>(),
        ["000101", "000102"]
    );

    // The next run asks only for what is missing: no index, no page twice.
    let mut source = six_damaged_messages(&raw);
    let report = run(&raw, &out, &mut source);
    assert_eq!(
        source.asked_paths(),
        [
            "2026-February/000103.html",
            "2026-February/000104.html",
            "2026-February/000105.html",
            "2026-February/000106.html",
        ]
    );
    assert_eq!(report.stopped, None);
    assert_eq!(saved(&out, "2026-February").fixes.len(), 6);
}

#[test]
fn test_an_error_page_served_with_a_200_records_nothing() {
    let raw = scratch_dir("error-page-raw");
    let out = scratch_dir("error-page-out");
    let mut source = FakeFetcher::new();
    for period in ["2026-January", "2026-February", "2026-March", "2026-April"] {
        fs::write(
            raw.join(format!("{period}.mbox")),
            raw_message(1, "[Rd] Topic", "Plain text."),
        )
        .unwrap();
        source.serve(
            &format!("{period}/date.html"),
            "<html><body>Service unavailable</body></html>",
        );
    }

    let report = run(&raw, &out, &mut source);

    assert_eq!(source.asked.len(), 3);
    assert_eq!(
        report.stopped.as_deref(),
        Some("3 requests in a row failed")
    );
    assert_eq!(report.failed, 3);
    assert_eq!(report.not_found, 0);
    assert_eq!(fs::read_dir(&out).unwrap().count(), 0);
}

#[test]
fn test_a_raw_file_that_is_not_utf8_is_read_the_way_parse_reads_it() {
    let raw = scratch_dir("latin1-raw");
    let out = scratch_dir("latin1-out");
    // "cafe" with its last letter as the Latin-1 byte 0xE9, which on its own
    // is not valid UTF-8.
    let mut bytes = raw_message(1, "[R] Topic A", "Meet at the caf").into_bytes();
    let at = bytes.len() - 2;
    bytes.insert(at, 0xE9);
    bytes.extend(raw_message(2, "[R] Topic B", "A na?ve question.").into_bytes());
    fs::write(raw.join("2003-May.mbox"), bytes).unwrap();

    let mut source = FakeFetcher::new();
    source.serve(
        "2003-May/date.html",
        &index_page(&[("000101", "[R] Topic A"), ("000102", "[R] Topic B")]),
    );
    source.serve(
        "2003-May/000102.html",
        &article_page("", "A na&#239;ve question."),
    );

    let report = run(&raw, &out, &mut source);

    assert_eq!(report.stopped, None);
    let saved = saved(&out, "2003-May");
    assert_eq!(pages(&saved), [Some("000101"), Some("000102")]);
    assert_eq!(saved.fixes["000102"], "\u{ef}");
}

#[test]
fn test_a_message_with_an_unparseable_date_keeps_its_place() {
    let raw = scratch_dir("bad-date-raw");
    let out = scratch_dir("bad-date-out");
    let middle = raw_message(2, "[Rd] Topic B", "A na?ve question that cannot be dated.").replace(
        "Date: Mon, 2 Feb 2026 12:02:00 +0000",
        "Date: sometime last week",
    );
    let mbox = raw_message(1, "[Rd] Topic A", "Plain text.")
        + &middle
        + &raw_message(3, "[Rd] Topic C", "A na?ve question.");
    fs::write(raw.join("2026-February.mbox"), mbox).unwrap();

    let mut source = FakeFetcher::new();
    source.serve(
        "2026-February/date.html",
        &index_page(&[
            ("000101", "[Rd] Topic A"),
            ("000102", "[Rd] Topic B"),
            ("000103", "[Rd] Topic C"),
        ]),
    );
    source.serve(
        "2026-February/000102.html",
        &article_page(
            "m2@example.org",
            "A na&#239;ve question that cannot be dated.",
        ),
    );
    source.serve(
        "2026-February/000103.html",
        &article_page("m3@example.org", "A na&#239;ve question."),
    );

    let report = run(&raw, &out, &mut source);

    // The third message is still the third page. The second is not fetched:
    // it will not be in the output, so there is nothing to restore.
    assert_eq!(
        source.asked_paths(),
        ["2026-February/date.html", "2026-February/000103.html"]
    );
    let saved = saved(&out, "2026-February");
    assert_eq!(
        pages(&saved),
        [Some("000101"), Some("000102"), Some("000103")]
    );
    assert_eq!(saved.fixes.keys().collect::<Vec<_>>(), ["000103"]);
    assert_eq!(report.mismatches, 0);
}

#[test]
fn test_a_page_naming_another_message_is_unlinked_and_gives_no_fix() {
    let raw = scratch_dir("mismatch-raw");
    let out = scratch_dir("mismatch-out");
    fs::write(
        raw.join("2026-February.mbox"),
        raw_message(1, "[Rd] Topic A", "A na?ve question."),
    )
    .unwrap();
    let mut source = FakeFetcher::new();
    source.serve(
        "2026-February/date.html",
        &index_page(&[("000101", "[Rd] Topic A")]),
    );
    source.serve(
        "2026-February/000101.html",
        &article_page("someone-else@example.org", "A na&#239;ve question."),
    );

    let report = run(&raw, &out, &mut source);

    let saved_period = saved(&out, "2026-February");
    assert_eq!(pages(&saved_period), [None]);
    assert!(saved_period.fixes.is_empty());
    // The pairing is written down, with the message the page says it is.
    assert_eq!(
        saved_period.rejected,
        [("000101".to_string(), "someone-else@example.org".to_string())].into()
    );
    assert_eq!(report.mismatches, 1);
    assert_eq!(report.chars_restored, 0);

    // Unlinked, so not asked for again.
    let mut again = FakeFetcher::new();
    run(&raw, &out, &mut again);
    assert!(again.asked.is_empty());
}

#[test]
fn test_a_page_that_names_no_message_id_is_taken_on_trust() {
    let raw = scratch_dir("no-id-raw");
    let out = scratch_dir("no-id-out");
    fs::write(
        raw.join("2006-March.mbox"),
        raw_message(1, "[Rd] Topic A", "A na?ve question."),
    )
    .unwrap();
    let mut source = FakeFetcher::new();
    source.serve(
        "2006-March/date.html",
        &index_page(&[("036687", "[Rd] Topic A")]),
    );
    source.serve(
        "2006-March/036687.html",
        &article_page("", "A na&#239;ve question."),
    );

    let report = run(&raw, &out, &mut source);

    assert_eq!(saved(&out, "2006-March").fixes["036687"], "\u{ef}");
    assert_eq!(report.mismatches, 0);
}

#[test]
fn test_a_message_page_that_is_not_found_stays_linked_and_is_asked_for_again() {
    let raw = scratch_dir("no-page-raw");
    let out = scratch_dir("no-page-out");
    // The index lists three pages, and the source does not have the second.
    let mut source = damaged_messages(&raw, 3, &[1, 3]);

    let report = run(&raw, &out, &mut source);

    // The pages after it are fetched all the same.
    assert_eq!(
        source.asked_paths(),
        [
            "2026-February/date.html",
            "2026-February/000101.html",
            "2026-February/000102.html",
            "2026-February/000103.html",
        ]
    );
    let after = saved(&out, "2026-February");
    assert_eq!(
        pages(&after),
        [Some("000101"), Some("000102"), Some("000103")]
    );
    assert_eq!(after.fixes.keys().collect::<Vec<_>>(), ["000101", "000103"]);
    assert_eq!(report.pages_fetched, 2);
    assert_eq!(report.not_found, 1);
    assert_eq!(report.failed, 0);
    assert_eq!(report.stopped, None);

    // Still not there on the next run: asked for again, and nothing changes.
    let before = fs::read(out.join("2026-February.json")).unwrap();
    let mut source = damaged_messages(&raw, 3, &[1, 3]);
    let report = run(&raw, &out, &mut source);
    assert_eq!(source.asked_paths(), ["2026-February/000102.html"]);
    assert_eq!(report.not_found, 1);
    assert!(before == fs::read(out.join("2026-February.json")).unwrap());

    // The source has it now, and it is fetched like any other page.
    let mut source = damaged_messages(&raw, 3, &[1, 2, 3]);
    let report = run(&raw, &out, &mut source);
    assert_eq!(source.asked_paths(), ["2026-February/000102.html"]);
    assert_eq!(saved(&out, "2026-February").fixes["000102"], "\u{ef}");
    assert_eq!(report.pages_fetched, 1);
    assert_eq!(report.not_found, 0);
}

#[test]
fn test_message_pages_that_are_not_found_stop_the_run_and_stay_linked() {
    let raw = scratch_dir("no-pages-raw");
    let out = scratch_dir("no-pages-out");
    // The index arrives, and every page it lists is "not found".
    let mut source = damaged_messages(&raw, 6, &[]);

    let report = run(&raw, &out, &mut source);

    assert_eq!(
        source.asked_paths(),
        [
            "2026-February/date.html",
            "2026-February/000101.html",
            "2026-February/000102.html",
            "2026-February/000103.html",
        ]
    );
    assert_eq!(
        report,
        SourcesReport {
            periods_indexed: 1,
            not_found: 3,
            messages: 6,
            stopped: Some("3 requests in a row failed".to_string()),
            ..SourcesReport::default()
        }
    );
    let after = saved(&out, "2026-February");
    assert_eq!(
        pages(&after),
        [
            Some("000101"),
            Some("000102"),
            Some("000103"),
            Some("000104"),
            Some("000105"),
            Some("000106"),
        ]
    );
    assert!(after.fixes.is_empty());
}

#[test]
fn test_a_source_that_has_nothing_stops_the_run_and_writes_no_file() {
    let raw = scratch_dir("nothing-raw");
    let out = scratch_dir("nothing-out");
    for period in [
        "2026-January",
        "2026-February",
        "2026-March",
        "2026-April",
        "2026-May",
    ] {
        fs::write(
            raw.join(format!("{period}.mbox")),
            raw_message(1, "[Rd] Topic", "A na?ve question."),
        )
        .unwrap();
    }

    // What a mistyped base URL looks like: every answer is "not found".
    let mut source = FakeFetcher::new();
    let report = run(&raw, &out, &mut source);

    assert_eq!(source.asked.len(), SourcesOptions::default().max_failures);
    assert_eq!(
        source.asked_paths(),
        [
            "2026-May/date.html",
            "2026-April/date.html",
            "2026-March/date.html",
        ]
    );
    assert_eq!(
        report,
        SourcesReport {
            not_found: 3,
            stopped: Some("3 requests in a row failed".to_string()),
            ..SourcesReport::default()
        }
    );
    assert_eq!(fs::read_dir(&out).unwrap().count(), 0);
}

#[test]
fn test_not_found_and_failed_requests_in_a_row_add_up_to_the_limit() {
    let raw = scratch_dir("mixed-raw");
    let out = scratch_dir("mixed-out");
    let mut source = FakeFetcher::new();
    for period in ["2026-January", "2026-February", "2026-March", "2026-April"] {
        fs::write(
            raw.join(format!("{period}.mbox")),
            raw_message(1, "[Rd] Topic", "Plain text."),
        )
        .unwrap();
    }
    // April answers with an error page, March is not found, and by February
    // the connection is refused. January would have been fine.
    source.serve(
        "2026-April/date.html",
        "<html><body>Service unavailable</body></html>",
    );
    for period in ["2026-February", "2026-January"] {
        source.serve(
            &format!("{period}/date.html"),
            &index_page(&[("000101", "[Rd] Topic")]),
        );
    }
    source.down_after = Some(2);

    let report = run(&raw, &out, &mut source);

    assert_eq!(
        source.asked_paths(),
        [
            "2026-April/date.html",
            "2026-March/date.html",
            "2026-February/date.html",
        ]
    );
    assert_eq!(
        report,
        SourcesReport {
            not_found: 1,
            failed: 2,
            stopped: Some("3 requests in a row failed".to_string()),
            ..SourcesReport::default()
        }
    );
    assert_eq!(fs::read_dir(&out).unwrap().count(), 0);
}

#[test]
fn test_an_answer_after_a_not_found_starts_the_count_again() {
    let raw = scratch_dir("reset-raw");
    let out = scratch_dir("reset-out");
    let mut source = FakeFetcher::new();
    for period in [
        "2026-January",
        "2026-February",
        "2026-March",
        "2026-April",
        "2026-May",
    ] {
        fs::write(
            raw.join(format!("{period}.mbox")),
            raw_message(1, "[Rd] Topic", "Plain text."),
        )
        .unwrap();
    }
    // Two periods not found, one that is there, two more not found: four in
    // all, but never three in a row.
    source.serve(
        "2026-March/date.html",
        &index_page(&[("000101", "[Rd] Topic")]),
    );

    let report = run(&raw, &out, &mut source);

    assert_eq!(
        source.asked_paths(),
        [
            "2026-May/date.html",
            "2026-April/date.html",
            "2026-March/date.html",
            "2026-February/date.html",
            "2026-January/date.html",
        ]
    );
    assert_eq!(
        report,
        SourcesReport {
            periods_indexed: 1,
            not_found: 4,
            messages: 1,
            ..SourcesReport::default()
        }
    );
    // Only the period that was answered has a file.
    assert_eq!(fs::read_dir(&out).unwrap().count(), 1);
    assert_eq!(pages(&saved(&out, "2026-March")), [Some("000101")]);
}

#[test]
fn test_a_not_found_answer_counts_against_the_request_budget() {
    let raw = scratch_dir("budget-404-raw");
    let out = scratch_dir("budget-404-out");
    let mut source = FakeFetcher::new();
    for period in ["2026-January", "2026-February", "2026-March"] {
        fs::write(
            raw.join(format!("{period}.mbox")),
            raw_message(1, "[Rd] Topic", "Plain text."),
        )
        .unwrap();
    }
    // March, the first asked for, is not found.
    for period in ["2026-January", "2026-February"] {
        source.serve(
            &format!("{period}/date.html"),
            &index_page(&[("000101", "[Rd] Topic")]),
        );
    }

    let opts = SourcesOptions {
        max_requests: 2,
        ..SourcesOptions::default()
    };
    let report = run_sources(&raw, &out, BASE, &opts, &mut source).unwrap();

    assert_eq!(
        source.asked_paths(),
        ["2026-March/date.html", "2026-February/date.html"]
    );
    assert_eq!(
        report,
        SourcesReport {
            periods_indexed: 1,
            not_found: 1,
            messages: 1,
            stopped: Some("request budget of 2 spent".to_string()),
            ..SourcesReport::default()
        }
    );
    assert_eq!(fs::read_dir(&out).unwrap().count(), 1);
    assert!(out.join("2026-February.json").exists());
}

#[test]
fn test_a_period_that_has_grown_is_indexed_again_and_keeps_its_fixes() {
    let raw = scratch_dir("grown-raw");
    let out = scratch_dir("grown-out");
    let first = raw_message(1, "[Rd] Topic A", "A na?ve question.");
    fs::write(raw.join("2026-February.mbox"), &first).unwrap();
    let mut source = FakeFetcher::new();
    source.serve(
        "2026-February/date.html",
        &index_page(&[("000101", "[Rd] Topic A")]),
    );
    source.serve(
        "2026-February/000101.html",
        &article_page("m1@example.org", "A na&#239;ve question."),
    );
    run(&raw, &out, &mut source);

    // A day later the text file has one more message.
    let grown = first + &raw_message(2, "Re: [Rd] Topic A", "Not na?ve at all.");
    fs::write(raw.join("2026-February.mbox"), grown).unwrap();
    let mut source = FakeFetcher::new();
    source.serve(
        "2026-February/date.html",
        &index_page(&[("000101", "[Rd] Topic A"), ("000102", "[Rd] Topic A")]),
    );
    source.serve(
        "2026-February/000102.html",
        &article_page("m2@example.org", "Not na&#239;ve at all."),
    );
    let report = run(&raw, &out, &mut source);

    // The index again, and only the new message's page.
    assert_eq!(
        source.asked_paths(),
        ["2026-February/date.html", "2026-February/000102.html"]
    );
    assert_eq!(report.periods_indexed, 1);
    let saved = saved(&out, "2026-February");
    assert_eq!(pages(&saved), [Some("000101"), Some("000102")]);
    assert_eq!(saved.fixes.len(), 2);
}

/// An index of `count` pages numbered from 000101, all in one thread.
fn one_thread_index(count: usize) -> String {
    let numbers: Vec<String> = (1..=count).map(|n| format!("00010{n}")).collect();
    let entries: Vec<(&str, &str)> = numbers
        .iter()
        .map(|number| (number.as_str(), "[Rd] Topic A"))
        .collect();
    index_page(&entries)
}

#[test]
fn test_a_fix_is_kept_while_its_message_is_unlinked() {
    let raw = scratch_dir("ahead-raw");
    let out = scratch_dir("ahead-out");
    // Two messages of one thread, both linked and both pages fetched.
    let two = raw_message(1, "[Rd] Topic A", "A na?ve question.")
        + &raw_message(2, "Re: [Rd] Topic A", "Not na?ve at all.");
    fs::write(raw.join("2026-February.mbox"), &two).unwrap();
    let mut source = FakeFetcher::new();
    source.serve("2026-February/date.html", &one_thread_index(2));
    source.serve(
        "2026-February/000101.html",
        &article_page("m1@example.org", "A na&#239;ve question."),
    );
    source.serve(
        "2026-February/000102.html",
        &article_page("m2@example.org", "Not na&#239;ve at all."),
    );
    run(&raw, &out, &mut source);
    let linked = saved(&out, "2026-February");
    assert_eq!(pages(&linked), [Some("000101"), Some("000102")]);
    assert_eq!(
        linked.fixes.keys().collect::<Vec<_>>(),
        ["000101", "000102"]
    );

    // The thread goes on. The raw file is a download and has one more reply;
    // the index is live and already has two. Four pages for three messages
    // with one subject: nothing says which page is whose, so the whole run
    // is unlinked.
    let three = two + &raw_message(3, "Re: [Rd] Topic A", "Plain text.");
    fs::write(raw.join("2026-February.mbox"), &three).unwrap();
    let mut source = FakeFetcher::new();
    source.serve("2026-February/date.html", &one_thread_index(4));
    let report = run(&raw, &out, &mut source);
    assert_eq!(source.asked_paths(), ["2026-February/date.html"]);
    assert_eq!(report.periods_indexed, 1);
    let unlinked = saved(&out, "2026-February");
    assert_eq!(pages(&unlinked), [None, None, None]);
    // Both pages are still in the index, so what they gave is kept.
    assert_eq!(unlinked.fixes, linked.fixes);

    // The raw file catches up. Every message is linked again, and neither
    // of the two pages is asked for a second time.
    let four = three + &raw_message(4, "Re: [Rd] Topic A", "Still na?ve.");
    fs::write(raw.join("2026-February.mbox"), four).unwrap();
    let mut source = FakeFetcher::new();
    source.serve("2026-February/date.html", &one_thread_index(4));
    source.serve(
        "2026-February/000104.html",
        &article_page("m4@example.org", "Still na&#239;ve."),
    );
    let report = run(&raw, &out, &mut source);
    assert_eq!(
        source.asked_paths(),
        ["2026-February/date.html", "2026-February/000104.html"]
    );
    assert_eq!(report.pages_fetched, 1);
    let caught_up = saved(&out, "2026-February");
    assert_eq!(
        pages(&caught_up),
        [
            Some("000101"),
            Some("000102"),
            Some("000103"),
            Some("000104")
        ]
    );
    assert_eq!(
        caught_up.fixes.keys().collect::<Vec<_>>(),
        ["000101", "000102", "000104"]
    );
    assert_eq!(caught_up.fixes["000101"], linked.fixes["000101"]);
    assert_eq!(caught_up.fixes["000102"], linked.fixes["000102"]);
}

#[test]
fn test_a_fix_for_a_page_the_index_no_longer_lists_is_dropped() {
    let raw = scratch_dir("delisted-raw");
    let out = scratch_dir("delisted-out");
    // On file: one message, linked to page 000099, which has been fetched.
    sources::save(
        &out,
        &PeriodSources {
            period: "2026-February".to_string(),
            articles: vec![Some("000099".to_string())],
            fixes: [("000099".to_string(), "\u{ef}".to_string())].into(),
            // ... and page 000098, which had named another message.
            rejected: [("000098".to_string(), "gone@example.org".to_string())].into(),
        },
    )
    .unwrap();

    // The raw file has grown, and the index has no page 000099 any more.
    let mbox = raw_message(1, "[Rd] Topic A", "A na?ve question.")
        + &raw_message(2, "[Rd] Topic B", "Plain text.");
    fs::write(raw.join("2026-February.mbox"), mbox).unwrap();
    let mut source = FakeFetcher::new();
    source.serve(
        "2026-February/date.html",
        &index_page(&[("000101", "[Rd] Topic A"), ("000102", "[Rd] Topic B")]),
    );
    source.serve(
        "2026-February/000101.html",
        &article_page("m1@example.org", "A na&#239;ve question."),
    );
    run(&raw, &out, &mut source);

    assert_eq!(
        source.asked_paths(),
        ["2026-February/date.html", "2026-February/000101.html"]
    );
    let after = saved(&out, "2026-February");
    assert_eq!(pages(&after), [Some("000101"), Some("000102")]);
    assert_eq!(after.fixes.keys().collect::<Vec<_>>(), ["000101"]);
    // What was known about a page that is gone goes with it.
    assert!(after.rejected.is_empty());
}

#[test]
fn test_characters_beyond_the_bmp_are_counted_and_left() {
    let raw = scratch_dir("astral-raw");
    let out = scratch_dir("astral-out");
    fs::write(
        raw.join("2026-February.mbox"),
        raw_message(1, "[Rd] Topic A", "Thanks ?? for the na?ve fix"),
    )
    .unwrap();
    let mut source = FakeFetcher::new();
    source.serve(
        "2026-February/date.html",
        &index_page(&[("000101", "[Rd] Topic A")]),
    );
    source.serve(
        "2026-February/000101.html",
        &article_page("m1@example.org", "Thanks &#128512; for the na&#239;ve fix"),
    );

    let report = run(&raw, &out, &mut source);

    assert_eq!(saved(&out, "2026-February").fixes["000101"], "??\u{ef}");
    assert_eq!(report.chars_restored, 1);
    assert_eq!(report.chars_left, 2);
    assert_eq!(report.astral_chars, 1);
}

/// Wraps a fetcher and notes, at every request, how many fixes the period's
/// file on disk already holds.
struct Watching {
    inner: FakeFetcher,
    out: PathBuf,
    fixes_on_disk: Vec<usize>,
}

impl Fetcher for Watching {
    fn get(&mut self, url: &str) -> anyhow::Result<Fetched> {
        let on_disk = sources::load(&self.out, "2026-February").unwrap();
        self.fixes_on_disk
            .push(on_disk.map(|s| s.fixes.len()).unwrap_or(0));
        self.inner.get(url)
    }
}

#[test]
fn test_a_long_period_is_saved_as_it_goes() {
    let raw = scratch_dir("long-raw");
    let out = scratch_dir("long-out");
    let mut mbox = String::new();
    let mut entries = Vec::new();
    let mut inner = FakeFetcher::new();
    for n in 1..=60 {
        mbox.push_str(&raw_message(
            n,
            &format!("[Rd] Topic {n}"),
            "A na?ve question.",
        ));
        entries.push((format!("{:06}", 100 + n), format!("[Rd] Topic {n}")));
        inner.serve(
            &format!("2026-February/{:06}.html", 100 + n),
            &article_page(&format!("m{n}@example.org"), "A na&#239;ve question."),
        );
    }
    fs::write(raw.join("2026-February.mbox"), mbox).unwrap();
    let entries: Vec<(&str, &str)> = entries
        .iter()
        .map(|(n, s)| (n.as_str(), s.as_str()))
        .collect();
    inner.serve("2026-February/date.html", &index_page(&entries));

    let mut source = Watching {
        inner,
        out: out.clone(),
        fixes_on_disk: Vec::new(),
    };
    run_sources(&raw, &out, BASE, &SourcesOptions::default(), &mut source).unwrap();

    // By the time the last page is asked for, the first fifty are on disk:
    // a run that is killed loses at most fifty pages of work.
    assert_eq!(source.fixes_on_disk.len(), 61);
    assert_eq!(source.fixes_on_disk[60], 50);
    assert_eq!(saved(&out, "2026-February").fixes.len(), 60);
}

#[test]
fn test_a_raw_directory_without_messages() {
    let raw = scratch_dir("empty-raw");
    let out = scratch_dir("empty-out");
    let mut source = FakeFetcher::new();
    let err = run_sources(&raw, &out, BASE, &SourcesOptions::default(), &mut source).unwrap_err();
    assert!(err.to_string().contains("No mbox files found"));

    // A raw file with no messages in it is skipped without a request.
    fs::write(raw.join("2026-February.mbox"), "").unwrap();
    let report = run(&raw, &out, &mut source);
    assert!(source.asked.is_empty());
    assert_eq!(report, SourcesReport::default());
}

#[test]
fn test_an_index_404_does_not_erase_a_period_that_has_links() {
    let raw = scratch_dir("keep-links-raw");
    let out = scratch_dir("keep-links-out");
    let first = raw_message(1, "[Rd] Topic A", "A na?ve question.");
    fs::write(raw.join("2026-February.mbox"), &first).unwrap();
    let page_1 = article_page("m1@example.org", "A na&#239;ve question.");
    let mut source = FakeFetcher::new();
    source.serve(
        "2026-February/date.html",
        &index_page(&[("000101", "[Rd] Topic A")]),
    );
    source.serve("2026-February/000101.html", &page_1);
    run(&raw, &out, &mut source);
    let kept = fs::read(out.join("2026-February.json")).unwrap();
    assert_eq!(saved(&out, "2026-February").fixes["000101"], "\u{ef}");

    // The raw file grows, and the source answers 404 for the index.
    let grown = first + &raw_message(2, "Re: [Rd] Topic A", "Not na?ve at all.");
    fs::write(raw.join("2026-February.mbox"), &grown).unwrap();
    let mut source = FakeFetcher::new();
    let report = run(&raw, &out, &mut source);
    assert_eq!(source.asked_paths(), ["2026-February/date.html"]);
    assert!(kept == fs::read(out.join("2026-February.json")).unwrap());
    assert_eq!(report.periods_indexed, 0);
    assert_eq!(report.pages_fetched, 0);

    // The source is back: the period is indexed, and the earlier fix stays.
    let mut source = FakeFetcher::new();
    source.serve(
        "2026-February/date.html",
        &index_page(&[("000101", "[Rd] Topic A"), ("000102", "[Rd] Topic A")]),
    );
    source.serve("2026-February/000101.html", &page_1);
    source.serve(
        "2026-February/000102.html",
        &article_page("m2@example.org", "Not na&#239;ve at all."),
    );
    let report = run(&raw, &out, &mut source);
    assert_eq!(report.periods_indexed, 1);
    let again = saved(&out, "2026-February");
    assert_eq!(pages(&again), [Some("000101"), Some("000102")]);
    assert_eq!(again.fixes.len(), 2);
}

#[test]
fn test_an_index_that_is_not_found_leaves_a_file_that_links_nothing_as_it_is() {
    let raw = scratch_dir("links-nothing-raw");
    let out = scratch_dir("links-nothing-out");
    let first = raw_message(1, "[Rd] Topic A", "A na?ve question.");
    fs::write(raw.join("2026-February.mbox"), &first).unwrap();
    // The period has been indexed, and no entry matched its one message.
    let mut source = FakeFetcher::new();
    source.serve(
        "2026-February/date.html",
        &index_page(&[("000101", "[Rd] Another topic")]),
    );
    run(&raw, &out, &mut source);
    assert_eq!(pages(&saved(&out, "2026-February")), [None]);
    let kept = fs::read(out.join("2026-February.json")).unwrap();

    // The raw file grows, and the index is not found.
    let grown = first + &raw_message(2, "[Rd] Topic B", "Plain.");
    fs::write(raw.join("2026-February.mbox"), grown).unwrap();
    let mut source = FakeFetcher::new();
    let report = run(&raw, &out, &mut source);
    assert_eq!(source.asked_paths(), ["2026-February/date.html"]);
    assert!(kept == fs::read(out.join("2026-February.json")).unwrap());
    assert_eq!(report.periods_indexed, 0);
    assert_eq!(report.not_found, 1);

    // The next run asks for it again, and again writes nothing.
    let mut source = FakeFetcher::new();
    run(&raw, &out, &mut source);
    assert_eq!(source.asked_paths(), ["2026-February/date.html"]);
    assert!(kept == fs::read(out.join("2026-February.json")).unwrap());
}

#[test]
fn test_a_period_file_that_cannot_be_read_stops_the_run_and_is_left_alone() {
    let raw = scratch_dir("unreadable-raw");
    let out = scratch_dir("unreadable-out");
    fs::write(
        raw.join("2026-February.mbox"),
        raw_message(1, "[Rd] Topic A", "A na?ve question."),
    )
    .unwrap();
    fs::write(out.join("2026-February.json"), "this is not json").unwrap();

    let mut source = FakeFetcher::new();
    let err = run_sources(&raw, &out, BASE, &SourcesOptions::default(), &mut source).unwrap_err();

    assert!(format!("{:#}", err).contains("2026-February.json"));
    assert!(source.asked.is_empty());
    assert_eq!(
        fs::read(out.join("2026-February.json")).unwrap(),
        b"this is not json"
    );
}

#[test]
fn test_a_body_line_starting_with_from_takes_a_position_of_its_own() {
    let raw = scratch_dir("from-line-raw");
    let out = scratch_dir("from-line-out");
    // The first message has a body line that begins with "From ", which
    // `split_mbox` takes for the start of a message. Positions are counted
    // its way, so the raw file has three of them for two real messages.
    let mbox = raw_message(
        1,
        "[Rd] Topic A",
        "I heard it yesterday.\nFrom what I can tell, it is the same bug.",
    ) + &raw_message(2, "[Rd] Topic B", "A na?ve question.");
    fs::write(raw.join("2026-February.mbox"), mbox).unwrap();

    let mut source = FakeFetcher::new();
    source.serve(
        "2026-February/date.html",
        &index_page(&[("000101", "[Rd] Topic A"), ("000102", "[Rd] Topic B")]),
    );
    source.serve(
        "2026-February/000102.html",
        &article_page("m2@example.org", "A na&#239;ve question."),
    );

    run(&raw, &out, &mut source);

    // The second real message sits at the third position, and that is where
    // its page is recorded: `parse` counts the same way and finds it there.
    let saved = saved(&out, "2026-February");
    assert_eq!(pages(&saved), [Some("000101"), None, Some("000102")]);
    assert_eq!(saved.fixes["000102"], "\u{ef}");
}

#[test]
fn test_a_rejected_pairing_is_not_linked_again_when_the_period_is_indexed_again() {
    let raw = scratch_dir("rejected-raw");
    let out = scratch_dir("rejected-out");
    let first = raw_message(1, "[Rd] Topic A", "A na?ve question.");
    fs::write(raw.join("2026-February.mbox"), &first).unwrap();
    let mut source = FakeFetcher::new();
    source.serve(
        "2026-February/date.html",
        &index_page(&[("000101", "[Rd] Topic A")]),
    );
    // The page with the right subject is another message's.
    source.serve(
        "2026-February/000101.html",
        &article_page("someone-else@example.org", "A na&#239;ve question."),
    );
    run(&raw, &out, &mut source);
    assert_eq!(pages(&saved(&out, "2026-February")), [None]);

    // The raw file grows, so the period is indexed again. The run has one
    // request to spend: it ends straight after the index, before it could
    // fetch any page to find the mistake a second time.
    let grown = first + &raw_message(2, "[Rd] Topic B", "Plain text.");
    fs::write(raw.join("2026-February.mbox"), grown).unwrap();
    let mut source = FakeFetcher::new();
    source.serve(
        "2026-February/date.html",
        &index_page(&[("000101", "[Rd] Topic A"), ("000102", "[Rd] Topic B")]),
    );
    let opts = SourcesOptions {
        max_requests: 1,
        ..SourcesOptions::default()
    };
    run_sources(&raw, &out, BASE, &opts, &mut source).unwrap();

    assert_eq!(source.asked_paths(), ["2026-February/date.html"]);
    let again = saved(&out, "2026-February");
    // Same subject, same place, and still not its page.
    assert_eq!(pages(&again), [None, Some("000102")]);
    assert_eq!(again.rejected["000101"], "someone-else@example.org");

    // With requests to spare the page is not asked for again either.
    let mut source = FakeFetcher::new();
    let report = run(&raw, &out, &mut source);
    assert!(source.asked.is_empty());
    assert_eq!(report.mismatches, 0);
}

#[test]
fn test_a_rejected_page_goes_to_the_message_it_names_once_that_message_arrives() {
    let raw = scratch_dir("named-raw");
    let out = scratch_dir("named-out");
    // The index is ahead of the raw file: it lists the page of message 1,
    // and the raw file holds only message 2, a reply with the same subject.
    let reply = raw_message(2, "Re: [Rd] Topic A", "Not na?ve at all.");
    fs::write(raw.join("2026-February.mbox"), &reply).unwrap();
    let page_1 = article_page("m1@example.org", "A na&#239;ve question.");
    let page_2 = article_page("m2@example.org", "Not na&#239;ve at all.");
    let mut source = FakeFetcher::new();
    source.serve(
        "2026-February/date.html",
        &index_page(&[("000101", "[Rd] Topic A")]),
    );
    source.serve("2026-February/000101.html", &page_1);
    run(&raw, &out, &mut source);
    let before = saved(&out, "2026-February");
    assert_eq!(pages(&before), [None]);
    assert_eq!(before.rejected["000101"], "m1@example.org");

    // The raw file catches up and now starts with message 1.
    let both = raw_message(1, "[Rd] Topic A", "A na?ve question.") + &reply;
    fs::write(raw.join("2026-February.mbox"), both).unwrap();
    let mut source = FakeFetcher::new();
    source.serve(
        "2026-February/date.html",
        &index_page(&[("000101", "[Rd] Topic A"), ("000102", "[Rd] Topic A")]),
    );
    source.serve("2026-February/000101.html", &page_1);
    source.serve("2026-February/000102.html", &page_2);
    let report = run(&raw, &out, &mut source);

    // Page 000101 names message 1, so message 1 may have it.
    let after = saved(&out, "2026-February");
    assert_eq!(pages(&after), [Some("000101"), Some("000102")]);
    assert_eq!(after.fixes.keys().collect::<Vec<_>>(), ["000101", "000102"]);
    assert_eq!(report.mismatches, 0);
}

/// One message, and its index, for each of `periods`.
fn one_message_periods(raw: &Path, periods: &[&str]) -> FakeFetcher {
    let mut source = FakeFetcher::new();
    for period in periods {
        fs::write(
            raw.join(format!("{period}.mbox")),
            raw_message(1, "[Rd] Topic", "Plain text."),
        )
        .unwrap();
        source.serve(
            &format!("{period}/date.html"),
            &index_page(&[("000101", "[Rd] Topic")]),
        );
    }
    source
}

#[test]
fn test_max_periods_keeps_a_run_out_of_history() {
    let raw = scratch_dir("max-periods-raw");
    let out = scratch_dir("max-periods-out");
    let periods = [
        "2025-December",
        "2026-January",
        "2026-February",
        "2026-March",
    ];
    let mut source = one_message_periods(&raw, &periods);

    // What a scheduled run passes: the two newest periods, and budget to spare.
    let opts = SourcesOptions {
        max_periods: Some(2),
        ..SourcesOptions::default()
    };
    let report = run_sources(&raw, &out, BASE, &opts, &mut source).unwrap();

    assert_eq!(
        source.asked_paths(),
        ["2026-March/date.html", "2026-February/date.html"]
    );
    assert_eq!(report.periods_indexed, 2);
    assert_eq!(report.periods_skipped, 2);
    // Leaving history alone is not stopping early.
    assert_eq!(report.stopped, None);
    assert!(!out.join("2026-January.json").exists());
    assert!(!out.join("2025-December.json").exists());

    // The same run the next day asks for nothing: it does not creep back a
    // period at a time.
    let mut source = one_message_periods(&raw, &periods);
    let report = run_sources(&raw, &out, BASE, &opts, &mut source).unwrap();
    assert!(source.asked.is_empty());
    assert_eq!(report.periods_skipped, 2);

    // A run without the limit, which is what a backfill is, takes the rest.
    let mut source = one_message_periods(&raw, &periods);
    let report = run(&raw, &out, &mut source);
    assert_eq!(
        source.asked_paths(),
        ["2026-January/date.html", "2025-December/date.html"]
    );
    assert_eq!(report.periods_skipped, 0);
}

#[test]
fn test_max_periods_counts_periods_that_have_messages() {
    let raw = scratch_dir("max-periods-empty-raw");
    let out = scratch_dir("max-periods-empty-out");
    let mut source = one_message_periods(&raw, &["2026-January", "2026-February", "2026-March"]);
    // The newest file has no messages in it yet.
    fs::write(raw.join("2026-April.mbox"), "").unwrap();

    let opts = SourcesOptions {
        max_periods: Some(2),
        ..SourcesOptions::default()
    };
    let report = run_sources(&raw, &out, BASE, &opts, &mut source).unwrap();

    assert_eq!(
        source.asked_paths(),
        ["2026-March/date.html", "2026-February/date.html"]
    );
    assert_eq!(report.periods_skipped, 1);
}

#[test]
fn test_max_periods_of_zero_looks_at_nothing() {
    let raw = scratch_dir("max-periods-zero-raw");
    let out = scratch_dir("max-periods-zero-out");
    let mut source = one_message_periods(&raw, &["2026-February", "2026-March"]);

    let opts = SourcesOptions {
        max_periods: Some(0),
        ..SourcesOptions::default()
    };
    let report = run_sources(&raw, &out, BASE, &opts, &mut source).unwrap();

    assert!(source.asked.is_empty());
    assert_eq!(report.periods_skipped, 2);
    assert_eq!(fs::read_dir(&out).unwrap().count(), 0);
}

#[test]
fn test_the_report_counts_what_is_on_file_for_the_periods_looked_at() {
    let raw = scratch_dir("counts-raw");
    let out = scratch_dir("counts-out");
    // Three messages: one whose page turns out to be another message's, one
    // that is linked, and one the index has no entry for.
    let mbox = raw_message(1, "[Rd] Topic A", "A na?ve question.")
        + &raw_message(2, "[Rd] Topic B", "Plain text.")
        + &raw_message(3, "[Rd] Topic C", "Plain text.");
    fs::write(raw.join("2026-February.mbox"), mbox).unwrap();
    let mut source = FakeFetcher::new();
    source.serve(
        "2026-February/date.html",
        &index_page(&[("000101", "[Rd] Topic A"), ("000102", "[Rd] Topic B")]),
    );
    source.serve(
        "2026-February/000101.html",
        &article_page("someone-else@example.org", "A na&#239;ve question."),
    );

    let report = run(&raw, &out, &mut source);

    assert_eq!(report.messages, 3);
    assert_eq!(report.unlinked, 2);
    assert_eq!(report.rejected, 1);
    assert_eq!(report.mismatches, 1);
    // Two pages for three messages: the index is behind, not ahead.
    assert_eq!(report.index_ahead, 0);

    // A run that has nothing to do reports the same state. `mismatches`
    // counts what a run found, `rejected` what is written down.
    let mut source = FakeFetcher::new();
    let report = run(&raw, &out, &mut source);
    assert!(source.asked.is_empty());
    assert_eq!(report.messages, 3);
    assert_eq!(report.unlinked, 2);
    assert_eq!(report.rejected, 1);
    assert_eq!(report.mismatches, 0);
}

#[test]
fn test_an_index_ahead_of_the_raw_file_is_counted_when_the_period_is_indexed() {
    let raw = scratch_dir("ahead-count-raw");
    let out = scratch_dir("ahead-count-out");
    fs::write(
        raw.join("2026-February.mbox"),
        raw_message(1, "[Rd] Topic A", "Plain text."),
    )
    .unwrap();
    // The index is live and already lists a message the raw file, a
    // download, does not hold yet.
    let index = index_page(&[("000101", "[Rd] Topic A"), ("000102", "[Rd] Topic B")]);
    let mut source = FakeFetcher::new();
    source.serve("2026-February/date.html", &index);

    let report = run(&raw, &out, &mut source);

    assert_eq!(report.index_ahead, 1);
    assert_eq!(report.messages, 1);
    assert_eq!(report.unlinked, 0);
    assert_eq!(pages(&saved(&out, "2026-February")), [Some("000101")]);

    // It is counted by the run that fetched the index, and not again by a
    // run that had no reason to fetch it.
    let mut source = FakeFetcher::new();
    source.serve("2026-February/date.html", &index);
    let report = run(&raw, &out, &mut source);
    assert!(source.asked.is_empty());
    assert_eq!(report.index_ahead, 0);
}

#[test]
fn test_a_period_whose_index_did_not_arrive_is_not_counted() {
    let raw = scratch_dir("uncounted-raw");
    let out = scratch_dir("uncounted-out");
    fs::write(
        raw.join("2026-February.mbox"),
        raw_message(1, "[Rd] Topic A", "Plain text."),
    )
    .unwrap();

    // Not found: the period has no file, so nothing is known about it.
    let mut source = FakeFetcher::new();
    let report = run(&raw, &out, &mut source);

    assert_eq!(report.not_found, 1);
    assert_eq!(report.messages, 0);
    assert_eq!(report.unlinked, 0);
}

/// Four periods with messages, the newest of which has three whose pages
/// the source does not have. Only the newest period's index is served.
fn four_periods_the_newest_cannot_be_finished(raw: &Path) -> FakeFetcher {
    let mut source = FakeFetcher::new();
    for period in ["2025-December", "2026-January", "2026-February"] {
        fs::write(
            raw.join(format!("{period}.mbox")),
            raw_message(1, "[Rd] Topic", "Plain text."),
        )
        .unwrap();
    }
    let mbox = raw_message(1, "[Rd] Topic A", "A na?ve question.")
        + &raw_message(2, "[Rd] Topic B", "A na?ve question.")
        + &raw_message(3, "[Rd] Topic C", "A na?ve question.");
    fs::write(raw.join("2026-March.mbox"), mbox).unwrap();
    source.serve(
        "2026-March/date.html",
        &index_page(&[
            ("000101", "[Rd] Topic A"),
            ("000102", "[Rd] Topic B"),
            ("000103", "[Rd] Topic C"),
        ]),
    );
    source
}

#[test]
fn test_older_periods_are_counted_when_a_limited_run_runs_out_of_budget() {
    let raw = scratch_dir("skipped-budget-raw");
    let out = scratch_dir("skipped-budget-out");
    let mut source = four_periods_the_newest_cannot_be_finished(&raw);
    let opts = SourcesOptions {
        max_requests: 1,
        max_periods: Some(2),
        ..SourcesOptions::default()
    };

    let report = run_sources(&raw, &out, BASE, &opts, &mut source).unwrap();

    assert_eq!(
        report.stopped,
        Some("request budget of 1 spent".to_string())
    );
    assert_eq!(report.periods_skipped, 2);
    assert_eq!(source.asked_paths(), ["2026-March/date.html"]);
}

#[test]
fn test_older_periods_are_counted_when_a_limited_run_gives_up_on_the_source() {
    let raw = scratch_dir("skipped-failures-raw");
    let out = scratch_dir("skipped-failures-out");
    let mut source = four_periods_the_newest_cannot_be_finished(&raw);
    let opts = SourcesOptions {
        max_periods: Some(2),
        ..SourcesOptions::default()
    };

    let report = run_sources(&raw, &out, BASE, &opts, &mut source).unwrap();

    assert_eq!(
        report.stopped,
        Some("3 requests in a row failed".to_string())
    );
    assert_eq!(report.periods_skipped, 2);
    assert_eq!(
        source.asked_paths(),
        [
            "2026-March/date.html",
            "2026-March/000101.html",
            "2026-March/000102.html",
            "2026-March/000103.html"
        ]
    );
}

#[test]
fn test_an_empty_newest_file_takes_no_slot_when_a_limited_run_stops() {
    let raw = scratch_dir("skipped-empty-raw");
    let out = scratch_dir("skipped-empty-out");
    let mut source = four_periods_the_newest_cannot_be_finished(&raw);
    fs::remove_file(raw.join("2025-December.mbox")).unwrap();
    fs::write(raw.join("2026-April.mbox"), "").unwrap();
    let opts = SourcesOptions {
        max_requests: 1,
        max_periods: Some(2),
        ..SourcesOptions::default()
    };

    let report = run_sources(&raw, &out, BASE, &opts, &mut source).unwrap();

    assert!(report.stopped.is_some());
    // March and February are the window; only January is older.
    assert_eq!(report.periods_skipped, 1);
    assert_eq!(source.asked_paths(), ["2026-March/date.html"]);
}

#[test]
fn test_a_period_on_file_is_counted_when_its_index_does_not_arrive() {
    let raw = scratch_dir("onfile-raw");
    let out = scratch_dir("onfile-out");
    // January: two messages, one page. February: one message, linked.
    fs::write(
        raw.join("2026-January.mbox"),
        raw_message(1, "[Rd] Topic A", "Plain text.")
            + &raw_message(2, "[Rd] Topic B", "Plain text."),
    )
    .unwrap();
    let feb = raw_message(1, "[Rd] Topic C", "Plain text.");
    fs::write(raw.join("2026-February.mbox"), &feb).unwrap();
    let mut source = FakeFetcher::new();
    source.serve(
        "2026-January/date.html",
        &index_page(&[("000101", "[Rd] Topic A")]),
    );
    source.serve(
        "2026-February/date.html",
        &index_page(&[("000201", "[Rd] Topic C")]),
    );
    let report = run(&raw, &out, &mut source);
    assert_eq!((report.messages, report.unlinked), (3, 1));

    // February's raw file grows and its index is not found.
    let grown = feb + &raw_message(2, "[Rd] Topic D", "Plain text.");
    fs::write(raw.join("2026-February.mbox"), &grown).unwrap();
    let mut source = FakeFetcher::new();
    let report = run(&raw, &out, &mut source);
    assert_eq!(source.asked_paths(), ["2026-February/date.html"]);
    assert_eq!(report.not_found, 1);
    assert_eq!(report.periods_indexed, 0);
    // The file on disk still has one position for February.
    assert_eq!(report.messages, 3);
    assert_eq!(report.unlinked, 1);

    // The same when the budget runs out at the index. January is not looked
    // at, since the run stops first.
    let mut source = FakeFetcher::new();
    let opts = SourcesOptions {
        max_requests: 0,
        ..SourcesOptions::default()
    };
    let report = run_sources(&raw, &out, BASE, &opts, &mut source).unwrap();
    assert!(source.asked.is_empty());
    assert!(report.stopped.is_some());
    assert_eq!(report.messages, 1);
    assert_eq!(report.unlinked, 0);
}
