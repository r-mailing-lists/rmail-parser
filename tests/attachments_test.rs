mod common;

use std::collections::BTreeMap;
use std::fs;

use rmail_parser::attachments::{
    decode, find_scrubbed, load, restore, same_url, save, PeriodAttachments, StoredFile, NOTICE,
};

const NEW_URL: &str =
    "https://stat.ethz.ch/pipermail/r-help/attachments/20100210/75317b7f/attachment.pl";
const OLD_URL: &str =
    "https://stat.ethz.ch/pipermail/r-help/attachments/20050329/1d7eba7e/attachment-0001.pl";

fn new_form(eol: &str) -> String {
    format!("{NOTICE}{eol}Name: not available{eol}URL: <{NEW_URL}>{eol}after")
}

#[test]
fn finds_the_new_and_the_old_form() {
    let text = format!("Hi\n{}", new_form("\n"));
    let found = find_scrubbed(&text);
    assert_eq!(found.len(), 1);
    let s = &found[0];
    assert_eq!(s.key, "20100210/75317b7f/attachment.pl");
    assert_eq!(s.url, NEW_URL);
    assert!(text[s.start..s.end].starts_with(NOTICE));
    assert!(text[s.start..s.end].ends_with(".pl>"));

    let text = format!("{NOTICE}\nName: not available\nUrl: {OLD_URL}  \nafter");
    let found = find_scrubbed(&text);
    assert_eq!(found.len(), 1);
    let s = &found[0];
    assert_eq!(s.key, "20050329/1d7eba7e/attachment-0001.pl");
    assert_eq!(s.url, OLD_URL);
    assert!(text[s.start..s.end].starts_with(NOTICE));
    assert!(text[s.start..s.end]
        .trim_end()
        .ends_with("attachment-0001.pl"));
}

#[test]
fn a_named_part_is_left_out() {
    let text = format!("{NOTICE}\nName: data.txt\nURL: <{NEW_URL}>\n");
    assert!(find_scrubbed(&text).is_empty());
}

#[test]
fn a_quoted_notice_is_left_out() {
    let text = format!("> {NOTICE}\n> Name: not available\n> URL: <{NEW_URL}>\n");
    assert!(find_scrubbed(&text).is_empty());
}

#[test]
fn crlf_line_ends() {
    let text = new_form("\r\n");
    let found = find_scrubbed(&text);
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].key, "20100210/75317b7f/attachment.pl");
    assert!(text[found[0].end..].starts_with("\r\nafter"));
}

#[test]
fn two_notices_in_one_text() {
    let text = format!(
        "{}\nmiddle\n{NOTICE}\nName: not available\nUrl: {OLD_URL}\n",
        new_form("\n")
    );
    let found = find_scrubbed(&text);
    assert_eq!(found.len(), 2);
    assert!(found[0].end < found[1].start);
    assert_eq!(found[0].key, "20100210/75317b7f/attachment.pl");
    assert_eq!(found[1].key, "20050329/1d7eba7e/attachment-0001.pl");
}

#[test]
fn decodes_ascii_utf8_and_windows_1252() {
    assert_eq!(decode(b"plain"), ("plain".to_string(), "ascii"));
    assert_eq!(decode("Québec".as_bytes()), ("Québec".to_string(), "utf-8"));
    assert_eq!(
        decode(b"Qu\xe9bec don\x92t \x81"),
        ("Québec don\u{2019}t \u{81}".to_string(), "windows-1252")
    );
}

#[test]
fn urls_match_across_http_and_https() {
    assert!(same_url("http://a/b", "https://a/b"));
    assert!(!same_url("http://a/b", "https://a/c"));
}

fn sample(period: &str) -> PeriodAttachments {
    let mut files = BTreeMap::new();
    files.insert(
        "20100210/75317b7f/attachment.pl".to_string(),
        StoredFile {
            url: NEW_URL.to_string(),
            encoding: Some("utf-8".to_string()),
            text: Some("print 1;\n".to_string()),
            status: None,
        },
    );
    files.insert(
        "20050329/1d7eba7e/attachment-0001.pl".to_string(),
        StoredFile {
            url: OLD_URL.to_string(),
            encoding: None,
            text: None,
            status: Some("not found".to_string()),
        },
    );
    PeriodAttachments {
        period: period.to_string(),
        files,
    }
}

#[test]
fn save_then_load_round_trips_and_is_stable() {
    let dir = common::scratch_dir("attachments-roundtrip");
    assert_eq!(load(&dir, "2010-February").unwrap(), None);
    let p = sample("2010-February");
    save(&dir, &p).unwrap();
    assert_eq!(load(&dir, "2010-February").unwrap(), Some(p.clone()));
    let path = dir.join("2010-February.json.gz");
    let first = fs::read(&path).unwrap();
    save(&dir, &p).unwrap();
    assert_eq!(fs::read(&path).unwrap(), first);
    assert!(!dir.join("2010-February.json.gz.tmp").exists());
}

#[test]
fn a_damaged_file_stops_with_its_name() {
    let dir = common::scratch_dir("attachments-damaged");
    fs::write(dir.join("2010-February.json.gz"), b"garbage").unwrap();
    let err = load(&dir, "2010-February").unwrap_err();
    assert!(format!("{err:#}").contains("2010-February.json.gz"));
}

#[test]
fn a_file_for_another_period_is_refused() {
    let dir = common::scratch_dir("attachments-other-period");
    save(&dir, &sample("2010-March")).unwrap();
    fs::rename(
        dir.join("2010-March.json.gz"),
        dir.join("2010-February.json.gz"),
    )
    .unwrap();
    assert!(load(&dir, "2010-February").is_err());
}

const KEY: &str = "20100210/75317b7f/attachment.pl";

fn stored(entries: &[(&str, StoredFile)]) -> PeriodAttachments {
    PeriodAttachments {
        period: "2010-February".to_string(),
        files: entries
            .iter()
            .map(|(k, f)| (k.to_string(), f.clone()))
            .collect(),
    }
}

fn with_text(url: &str, text: &str) -> StoredFile {
    StoredFile {
        url: url.to_string(),
        encoding: Some("ascii".to_string()),
        text: Some(text.to_string()),
        status: None,
    }
}

fn with_status(url: &str, status: &str) -> StoredFile {
    StoredFile {
        url: url.to_string(),
        encoding: None,
        text: None,
        status: Some(status.to_string()),
    }
}

#[test]
fn restores_a_stored_text_in_place() {
    let body = format!("Hi\n{NOTICE}\nName: not available\nURL: <{NEW_URL}>\n-- \nfooter");
    let store = stored(&[(KEY, with_text(NEW_URL, "Hello\n\n"))]);
    let (out, n) = restore(&body, &store);
    assert_eq!(out, "Hi\nHello\n-- \nfooter");
    assert_eq!(n, 1);
}

#[test]
fn leaves_what_it_cannot_restore() {
    let body = format!("Hi\n{NOTICE}\nName: not available\nURL: <{NEW_URL}>\nend");
    let other = "https://other.example.org/pipermail/r-help/attachments/20100210/75317b7f/attachment.pl";
    let cases = [
        stored(&[]),
        stored(&[(KEY, with_status(NEW_URL, "not found"))]),
        stored(&[(KEY, with_text(other, "Hello"))]),
        stored(&[(KEY, with_text(NEW_URL, "  \n"))]),
        stored(&[(KEY, with_text(NEW_URL, ""))]),
    ];
    for store in cases {
        let (out, n) = restore(&body, &store);
        assert_eq!(out, body);
        assert_eq!(n, 0);
    }
}

#[test]
fn restores_a_stored_text_whichever_scheme_was_fetched() {
    let body = format!("{NOTICE}\nName: not available\nURL: <{NEW_URL}>\nend");
    let http = NEW_URL.replacen("https://", "http://", 1);
    let store = stored(&[(KEY, with_text(&http, "Hello"))]);
    assert_eq!(restore(&body, &store), ("Hello\nend".to_string(), 1));
}

#[test]
fn restores_two_and_leaves_a_named_part() {
    let second = "https://stat.ethz.ch/pipermail/r-help/attachments/20100210/75317b7f/attachment-0001.pl";
    let named = format!(
        "An embedded and charset-unspecified text was scrubbed...\nName: x.R\nURL: <{NEW_URL}>"
    );
    let body = format!(
        "{NOTICE}\nName: not available\nURL: <{NEW_URL}>\n\nmid\n\n{named}\n\n{NOTICE}\nName: not available\nURL: <{second}>\n"
    );
    let store = stored(&[
        (KEY, with_text(NEW_URL, "one")),
        ("20100210/75317b7f/attachment-0001.pl", with_text(second, "two\n")),
    ]);
    let (out, n) = restore(&body, &store);
    assert_eq!(out, format!("one\n\nmid\n\n{named}\n\ntwo\n"));
    assert_eq!(n, 2);
}
