mod common;

use std::fs;
use std::path::Path;

use common::{raw_message, scratch_dir};
use rmail_parser::attachments::{save, PeriodAttachments, StoredFile, NOTICE};
use rmail_parser::message::Message;
use rmail_parser::output::MonthArchive;
use rmail_parser::pipeline::run_parse;

const URL: &str =
    "https://lists.example.org/pipermail/r-devel/attachments/20100210/75317b7f/attachment.txt";
const KEY: &str = "20100210/75317b7f/attachment.txt";

fn notice() -> String {
    format!("{NOTICE}\nName: not available\nURL: <{URL}>")
}

fn raw_dir(name: &str) -> std::path::PathBuf {
    let raw = scratch_dir(name);
    fs::write(
        raw.join("2010-February.mbox"),
        raw_message(1, "[Rd] Topic", &notice()),
    )
    .unwrap();
    raw
}

fn store_with(name: &str, text: &str) -> std::path::PathBuf {
    let dir = scratch_dir(name);
    let mut p = PeriodAttachments {
        period: "2010-February".to_string(),
        files: Default::default(),
    };
    p.files.insert(
        KEY.to_string(),
        StoredFile {
            url: URL.to_string(),
            encoding: Some("utf-8".to_string()),
            text: Some(text.to_string()),
            status: None,
        },
    );
    save(&dir, &p).unwrap();
    dir
}

fn parse(raw: &Path, name: &str, hide: bool, attachments: Option<&Path>) -> std::path::PathBuf {
    let output = scratch_dir(name);
    run_parse(raw, &output, "r-devel", true, None, None, hide, attachments).unwrap();
    output
}

fn first_message(output: &Path) -> Message {
    let json = fs::read_to_string(output.join("2026-02.json")).unwrap();
    let archive: MonthArchive = serde_json::from_str(&json).unwrap();
    archive.messages.into_iter().next().unwrap()
}

fn read_all(dir: &Path) -> Vec<(String, Vec<u8>)> {
    let mut files: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .map(|e| {
            let e = e.unwrap();
            (
                e.file_name().to_string_lossy().into_owned(),
                fs::read(e.path()).unwrap(),
            )
        })
        .collect();
    files.sort();
    files
}

#[test]
fn parse_restores_body_and_snippet() {
    let raw = raw_dir("restore-raw");
    let store = store_with("restore-store", "Restored words here.\n");
    let out = parse(&raw, "restore-out", false, Some(&store));
    let msg = first_message(&out);
    assert!(
        msg.body_plain.starts_with("Restored words here."),
        "{}",
        msg.body_plain
    );
    assert!(
        msg.body_snippet.starts_with("Restored words here."),
        "{}",
        msg.body_snippet
    );
    assert!(!msg.body_plain.contains(NOTICE));
}

#[test]
fn parse_without_the_flag_is_unchanged() {
    let raw = raw_dir("same-raw");
    let plain = parse(&raw, "same-plain", false, None);
    let empty = parse(
        &raw,
        "same-empty",
        false,
        Some(&scratch_dir("same-empty-store")),
    );
    assert_eq!(read_all(&plain), read_all(&empty));
    assert!(first_message(&plain).body_plain.contains(NOTICE));
}

#[test]
fn hidden_addresses_cover_restored_text() {
    let raw = raw_dir("hide-raw");
    let store = store_with("hide-store", "Write to al.smith@example.org today.\n");
    let out = parse(&raw, "hide-out", true, Some(&store));
    for (name, bytes) in read_all(&out) {
        let text = String::from_utf8_lossy(&bytes);
        assert!(!text.contains("al.smith@example.org"), "{name}");
    }
    assert!(first_message(&out).body_plain.starts_with("Write to"));
}

#[test]
fn a_damaged_store_stops_the_parse() {
    let raw = raw_dir("damaged-raw");
    let store = scratch_dir("damaged-store");
    fs::write(store.join("2010-February.json.gz"), "garbage").unwrap();
    let err = run_parse(
        &raw,
        &scratch_dir("damaged-out"),
        "r-devel",
        false,
        None,
        None,
        false,
        Some(&store),
    )
    .unwrap_err();
    assert!(format!("{:#}", err).contains("2010-February.json.gz"));
}

#[test]
fn restored_bodies_hold_no_whole_address() {
    let raw = raw_dir("obscure-raw");
    let store = store_with("obscure-store", "Write to al.smith@example.org today.\n");
    let out = parse(&raw, "obscure-out", false, Some(&store));
    for (name, bytes) in read_all(&out) {
        let text = String::from_utf8_lossy(&bytes);
        assert!(!text.contains("al.smith@example.org"), "{name}");
    }
    assert!(first_message(&out)
        .body_plain
        .contains("al.smith at example.org"));
}

#[test]
fn a_restored_body_is_cleaned_like_any_other() {
    let raw = raw_dir("clean-raw");
    let store = store_with("clean-store", "\n\n  \nFirst words.\r\nSecond line.\r\n");
    let out = parse(&raw, "clean-out", false, Some(&store));
    let msg = first_message(&out);
    assert_eq!(msg.body_plain, "First words.\nSecond line.");
    assert_eq!(msg.body_snippet, "First words.\nSecond line.");
}

#[test]
fn a_restored_message_says_where_its_text_came_from() {
    let raw = raw_dir("from-raw");
    let store = store_with("from-store", "Restored words here.\n");
    let out = parse(&raw, "from-out", false, Some(&store));
    assert_eq!(first_message(&out).restored_from, [URL]);
}

#[test]
fn an_unrestored_message_has_no_restored_from_key() {
    let raw = raw_dir("unrestored-raw");
    // Only whitespace is stored, so the notice stays.
    let store = store_with("unrestored-store", "  \n");
    let out = parse(&raw, "unrestored-out", false, Some(&store));
    assert!(first_message(&out).body_plain.contains(NOTICE));
    let json = fs::read_to_string(out.join("2026-02.json")).unwrap();
    assert!(!json.contains("restored_from"), "{json}");
}
