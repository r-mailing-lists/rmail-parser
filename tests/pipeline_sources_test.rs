mod common;

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use common::{fixture_path, raw_message, scratch_dir, BASE};
use rmail_parser::message::Message;
use rmail_parser::output::MonthArchive;
use rmail_parser::pipeline::run_parse;
use rmail_parser::sources::{self, PeriodSources};

fn period(name: &str, articles: &[Option<&str>], fixes: &[(&str, &str)]) -> PeriodSources {
    PeriodSources {
        period: name.to_string(),
        articles: articles.iter().map(|a| a.map(String::from)).collect(),
        fixes: fixes
            .iter()
            .map(|(number, fix)| (number.to_string(), fix.to_string()))
            .collect::<BTreeMap<_, _>>(),
        rejected: BTreeMap::new(),
    }
}

/// Parses `raw` with `sources` and returns one month of the output.
fn parse_month(raw: &Path, sources_dir: &Path, name: &str, month: &str) -> Vec<Message> {
    let output = scratch_dir(&format!("{name}-output"));
    run_parse(
        raw,
        &output,
        "r-devel",
        false,
        None,
        Some((sources_dir, BASE)),
        false,
        None,
    )
    .unwrap();
    let json = fs::read_to_string(output.join(format!("{month}.json"))).unwrap();
    let archive: MonthArchive = serde_json::from_str(&json).unwrap();
    archive.messages
}

fn by_message_id<'a>(messages: &'a [Message], message_id: &str) -> &'a Message {
    messages
        .iter()
        .find(|m| m.message_id == message_id)
        .unwrap()
}

#[test]
fn test_parse_with_sources_links_each_message_and_restores_its_characters() {
    let sources_dir = scratch_dir("links-sources");
    sources::save(
        &sources_dir,
        &period(
            "2026-October",
            &[
                Some("084714"),
                Some("084715"),
                Some("084716"),
                Some("084717"),
            ],
            &[("084715", ""), ("084716", "\u{f1}\u{e4}\u{2019}??")],
        ),
    )
    .unwrap();

    let messages = parse_month(
        &fixture_path("raw/2026-October.mbox"),
        &sources_dir,
        "links",
        "2026-10",
    );

    // The output is in date order, which is not the raw file's order: the
    // link follows the message, not its place in the output.
    let order: Vec<&str> = messages.iter().map(|m| m.message_id.as_str()).collect();
    assert_eq!(
        order,
        [
            "<msg-084714@example.org>",
            "<msg-084715@example.org>",
            "<msg-084717@example.org>",
            "<msg-084716@example.org>"
        ]
    );
    for number in ["084714", "084715", "084716", "084717"] {
        let msg = by_message_id(&messages, &format!("<msg-{number}@example.org>"));
        assert_eq!(
            msg.source_url.as_deref(),
            Some(format!("{BASE}/2026-October/{number}.html").as_str())
        );
    }

    let restored = by_message_id(&messages, "<msg-084716@example.org>");
    assert_eq!(
        restored.body_plain,
        "I\u{f1}aki and I saw this at the Universit\u{e4}t last week, and it\u{2019}s the same\n\
         there. Does that help?\n\
         \n\
         The notes are at https://example.org/history?id=1666 if you want them."
    );
    // The snippet is cut from the restored body.
    assert!(restored
        .body_snippet
        .starts_with("I\u{f1}aki and I saw this at the Universit\u{e4}t"));

    // A page that was fetched and had nothing to restore leaves the body alone.
    let untouched = by_message_id(&messages, "<msg-084715@example.org>");
    assert!(untouched.body_plain.contains("Bessel?s correction"));
}

#[test]
fn test_parse_with_sources_keeps_message_and_thread_ids() {
    let raw = fixture_path("raw/2026-October.mbox");
    let sources_dir = scratch_dir("ids-sources");
    sources::save(
        &sources_dir,
        &period(
            "2026-October",
            &[
                Some("084714"),
                Some("084715"),
                Some("084716"),
                Some("084717"),
            ],
            &[("084716", "\u{f1}\u{e4}\u{2019}??")],
        ),
    )
    .unwrap();
    let with = parse_month(&raw, &sources_dir, "ids-with", "2026-10");
    let without = parse_month(
        &raw,
        &scratch_dir("ids-no-sources"),
        "ids-without",
        "2026-10",
    );

    let ids = |messages: &[Message]| -> Vec<(String, String)> {
        messages
            .iter()
            .map(|m| (m.id.clone(), m.thread_id.clone()))
            .collect()
    };
    assert_eq!(ids(&with), ids(&without));
}

#[test]
fn test_parse_with_an_empty_sources_directory_writes_what_it_writes_without_one() {
    let raw = fixture_path("raw/2026-October.mbox");
    let plain = scratch_dir("empty-plain");
    let empty = scratch_dir("empty-with");
    run_parse(&raw, &plain, "r-devel", true, None, None, false, None).unwrap();
    run_parse(
        &raw,
        &empty,
        "r-devel",
        true,
        None,
        Some((&scratch_dir("empty-sources"), BASE)),
        false,
        None,
    )
    .unwrap();

    for file in [
        "2026-10.json",
        "meta.json",
        "index.json",
        "message-order.json",
        "contributors.json",
    ] {
        assert!(
            fs::read(plain.join(file)).unwrap() == fs::read(empty.join(file)).unwrap(),
            "{file} differs"
        );
    }
    // No trace of the new field.
    assert!(!fs::read_to_string(plain.join("2026-10.json"))
        .unwrap()
        .contains("source_url"));
}

#[test]
fn test_a_message_with_an_unparseable_date_does_not_shift_the_links_after_it() {
    let raw = scratch_dir("bad-date-raw");
    let middle = raw_message(2, "[Rd] Topic B", "This one cannot be dated.").replace(
        "Date: Mon, 2 Feb 2026 12:02:00 +0000",
        "Date: sometime last week",
    );
    let mbox = raw_message(1, "[Rd] Topic A", "Plain text.")
        + &middle
        + &raw_message(3, "[Rd] Topic C", "A na?ve question.");
    fs::write(raw.join("2026-February.mbox"), mbox).unwrap();
    let sources_dir = scratch_dir("bad-date-sources");
    sources::save(
        &sources_dir,
        &period(
            "2026-February",
            &[Some("000101"), Some("000102"), Some("000103")],
            &[("000103", "\u{ef}")],
        ),
    )
    .unwrap();

    let messages = parse_month(&raw, &sources_dir, "bad-date", "2026-02");

    // The undated message is dropped from the output, as before, but it
    // still counts: the third message is still the third page.
    assert_eq!(messages.len(), 2);
    let third = by_message_id(&messages, "<m3@example.org>");
    assert_eq!(
        third.source_url.as_deref(),
        Some(format!("{BASE}/2026-February/000103.html").as_str())
    );
    assert_eq!(third.body_plain, "A na\u{ef}ve question.");
    let first = by_message_id(&messages, "<m1@example.org>");
    assert_eq!(
        first.source_url.as_deref(),
        Some(format!("{BASE}/2026-February/000101.html").as_str())
    );
}

#[test]
fn test_a_fix_that_no_longer_fits_its_message_is_not_applied() {
    let raw = scratch_dir("stale-fix-raw");
    fs::write(
        raw.join("2026-February.mbox"),
        raw_message(1, "[Rd] Topic A", "Is this na?ve? Maybe."),
    )
    .unwrap();
    let sources_dir = scratch_dir("stale-fix-sources");
    // Made when the body had one `?`. It has two now.
    sources::save(
        &sources_dir,
        &period("2026-February", &[Some("000101")], &[("000101", "\u{ef}")]),
    )
    .unwrap();

    let messages = parse_month(&raw, &sources_dir, "stale-fix", "2026-02");

    assert_eq!(messages[0].body_plain, "Is this na?ve? Maybe.");
    assert_eq!(messages[0].body_snippet, "Is this na?ve? Maybe.");
    // The link does not depend on the fix.
    assert_eq!(
        messages[0].source_url.as_deref(),
        Some(format!("{BASE}/2026-February/000101.html").as_str())
    );
}

#[test]
fn test_a_message_in_two_period_files_appears_once_with_the_page_that_was_found() {
    let raw = scratch_dir("twice-raw");
    // A message sent at the turn of the month, filed under both. February's
    // file is read first: the files are taken in name order.
    let boundary = raw_message(7, "[Rd] Topic A", "A na?ve question.");
    fs::write(raw.join("2026-February.mbox"), &boundary).unwrap();
    fs::write(
        raw.join("2026-January.mbox"),
        raw_message(1, "[Rd] Earlier", "Plain text.") + &boundary,
    )
    .unwrap();
    let sources_dir = scratch_dir("twice-sources");
    sources::save(&sources_dir, &period("2026-February", &[None], &[])).unwrap();
    sources::save(
        &sources_dir,
        &period(
            "2026-January",
            &[Some("000100"), Some("000101")],
            &[("000101", "\u{ef}")],
        ),
    )
    .unwrap();

    let messages = parse_month(&raw, &sources_dir, "twice", "2026-02");

    let copies: Vec<&Message> = messages
        .iter()
        .filter(|m| m.message_id == "<m7@example.org>")
        .collect();
    assert_eq!(copies.len(), 1);
    assert_eq!(
        copies[0].source_url.as_deref(),
        Some(format!("{BASE}/2026-January/000101.html").as_str())
    );
    assert_eq!(copies[0].body_plain, "A na\u{ef}ve question.");
}

#[test]
fn test_a_message_linked_in_both_period_files_keeps_the_first() {
    let raw = scratch_dir("both-raw");
    let boundary = raw_message(7, "[Rd] Topic A", "Plain text.");
    fs::write(raw.join("2026-February.mbox"), &boundary).unwrap();
    fs::write(raw.join("2026-January.mbox"), &boundary).unwrap();
    let sources_dir = scratch_dir("both-sources");
    sources::save(
        &sources_dir,
        &period("2026-February", &[Some("000201")], &[]),
    )
    .unwrap();
    sources::save(
        &sources_dir,
        &period("2026-January", &[Some("000101")], &[]),
    )
    .unwrap();

    let messages = parse_month(&raw, &sources_dir, "both", "2026-02");

    assert_eq!(messages.len(), 1);
    assert_eq!(
        messages[0].source_url.as_deref(),
        Some(format!("{BASE}/2026-February/000201.html").as_str())
    );
}

#[test]
fn test_a_later_copy_in_the_same_file_neither_takes_a_page_nor_replaces_the_first() {
    let raw = scratch_dir("copy-raw");
    // The period was archived twice: the text file has the message again,
    // with the address in its From line mangled.
    let first = raw_message(7, "[Rd] Topic A", "A na?ve question.");
    let copy = first.replace("(User 7)", "(U|er 7)");
    fs::write(raw.join("2026-February.mbox"), first + &copy).unwrap();
    let sources_dir = scratch_dir("copy-sources");
    // Written by an earlier version, which gave the page to the copy.
    sources::save(
        &sources_dir,
        &period(
            "2026-February",
            &[None, Some("000101")],
            &[("000101", "\u{ef}")],
        ),
    )
    .unwrap();

    let messages = parse_month(&raw, &sources_dir, "copy", "2026-02");

    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].from_name, "User 7");
    assert_eq!(messages[0].source_url, None);
    assert_eq!(messages[0].body_plain, "A na?ve question.");
}

#[test]
fn test_a_sources_file_for_a_longer_raw_file_is_ignored() {
    let raw = scratch_dir("shrunk-raw");
    fs::write(
        raw.join("2026-February.mbox"),
        raw_message(2, "[Rd] Topic B", "Plain text."),
    )
    .unwrap();
    let sources_dir = scratch_dir("shrunk-sources");
    // Written when the raw file had two messages. Which one went is unknown.
    sources::save(
        &sources_dir,
        &period("2026-February", &[Some("000101"), Some("000102")], &[]),
    )
    .unwrap();

    let messages = parse_month(&raw, &sources_dir, "shrunk", "2026-02");

    assert_eq!(messages[0].source_url, None);
}

#[test]
fn test_messages_past_the_end_of_a_sources_file_get_no_link() {
    let raw = scratch_dir("grown-raw");
    let mbox = raw_message(1, "[Rd] Topic A", "Plain text.")
        + &raw_message(2, "[Rd] Topic B", "Plain text.");
    fs::write(raw.join("2026-February.mbox"), mbox).unwrap();
    let sources_dir = scratch_dir("grown-sources");
    // Written before the second message arrived.
    sources::save(
        &sources_dir,
        &period("2026-February", &[Some("000101")], &[]),
    )
    .unwrap();

    let messages = parse_month(&raw, &sources_dir, "grown", "2026-02");

    assert!(by_message_id(&messages, "<m1@example.org>")
        .source_url
        .is_some());
    assert_eq!(
        by_message_id(&messages, "<m2@example.org>").source_url,
        None
    );
}

#[test]
fn test_a_damaged_sources_file_stops_the_parse_and_names_the_file() {
    let raw = scratch_dir("damaged-raw");
    fs::write(
        raw.join("2026-February.mbox"),
        raw_message(1, "[Rd] Topic A", "Plain text."),
    )
    .unwrap();
    let sources_dir = scratch_dir("damaged-sources");
    fs::write(sources_dir.join("2026-February.json"), "{\"period\": ").unwrap();

    let output = scratch_dir("damaged-output");
    let err = run_parse(
        &raw,
        &output,
        "r-devel",
        false,
        None,
        Some((&sources_dir, BASE)),
        false,
        None,
    )
    .unwrap_err();
    assert!(format!("{:#}", err).contains("2026-February.json"));
}

#[test]
fn test_a_sources_file_from_before_rejected_pairings_stops_the_parse_and_names_the_file() {
    let raw = scratch_dir("old-format-raw");
    fs::write(
        raw.join("2026-February.mbox"),
        raw_message(1, "[Rd] Topic A", "Plain text."),
    )
    .unwrap();
    let sources_dir = scratch_dir("old-format-sources");
    // What v0.9.0 wrote: no `rejected` field. It is not carried over.
    fs::write(
        sources_dir.join("2026-February.json"),
        "{\n  \"period\": \"2026-February\",\n  \"articles\": [\n    \"000101\"\n  ],\n  \"fixes\": {}\n}\n",
    )
    .unwrap();

    let output = scratch_dir("old-format-output");
    let err = run_parse(
        &raw,
        &output,
        "r-devel",
        false,
        None,
        Some((&sources_dir, BASE)),
        false,
        None,
    )
    .unwrap_err();

    let message = format!("{:#}", err);
    assert!(message.contains("2026-February.json"), "{message}");
    assert!(message.contains("missing field `rejected`"), "{message}");
}
