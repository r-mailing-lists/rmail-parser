//! Whole runs of `parse` over `tests/fixtures/members/`, a sample of a list
//! whose archive only its members can read. Its five messages name seven
//! people, and hold an address in every form these archives have: Mailman's
//! text form on one line and broken over a line end, each of the source's
//! four lossy separators, the plain sign, the sign escaped in three kinds
//! of link, and `(at)`. Each Message-ID is at its sender's own domain, and
//! two of them have the sender's mailbox name in them.

mod common;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use common::{fixture_path, scratch_dir};
use rmail_parser::message::Message;
use rmail_parser::output::{ContributorEntry, MonthArchive};

/// The domains of the fixture's seven people, as written and as the lossy
/// forms write them. A run that hides addresses writes none of them
/// anywhere: not in a text, and not in an identifier either.
const DOMAINS: [&str; 10] = [
    "uni-example.edu",
    "inst-example.org",
    "|n@t-ex@mp|e@org",
    "lab-example.net",
    "uni-example.de",
    "col-example.org",
    "coi-ex@mpie.org",
    "dept-example.org",
    "dept-ex@mple.org",
    "u@it-ex@mple.@et",
];

/// Their mailbox names, but for `jane.roe`: she has no name beside her
/// address, so that is what she is known by.
const MAILBOXES: [&str; 9] = [
    "m@rk@poe",
    "lee.vane",
    "j?rg.m?ller",
    "p@t.kim",
    "pat.kim",
    "@@m.r@y",
    "sam.ray",
    "kim.lee",
    "jane.roe@",
];

/// The fixture's five Message-IDs, in the order of its messages.
const MESSAGE_IDS: [&str; 5] = [
    "<20260302101500.jane.roe@uni-example.edu>",
    "<CAF8xyz01@inst-example.org>",
    "<m3.20260303@lab-example.net>",
    "<4a5b6c.pat.kim@col-example.org>",
    "<e7f8.20260304@dept-example.org>",
];

/// What each of them is written as: `<`, the first 32 hex characters of
/// its SHA-256, `@hidden>`.
const HIDDEN_IDS: [&str; 5] = [
    "<a3567c8d6e93c810677b8cd3b143e3c6@hidden>",
    "<ceafe7cc22069a77dcd7aede2cb8ef95@hidden>",
    "<5ddae1225d36c82e0d9db276195ed3da@hidden>",
    "<ee61581635513858f5b16fb5392cc12a@hidden>",
    "<295d4473a10ecd970a3263c6ddd8ce43@hidden>",
];

fn rmail_parser() -> Command {
    Command::new(env!("CARGO_BIN_EXE_rmail-parser"))
}

/// Runs `rmail-parser parse` over `input` into a fresh directory and
/// returns that directory.
fn parse_into(name: &str, input: &Path, extra: &[&str]) -> PathBuf {
    let output = scratch_dir(name);
    let run = rmail_parser()
        .args(["parse", "--list", "r-sig-phylo", "--input"])
        .arg(input)
        .arg("--output")
        .arg(&output)
        .args(extra)
        .output()
        .unwrap();
    assert!(
        run.status.success(),
        "{}",
        String::from_utf8_lossy(&run.stderr)
    );
    output
}

/// Runs `rmail-parser parse` over the fixture and returns every file it
/// wrote, keyed by its path under the output directory.
fn parse_members(name: &str, extra: &[&str]) -> BTreeMap<String, Vec<u8>> {
    snapshot(&parse_into(name, &fixture_path("members"), extra))
}

/// Every file under `dir`, keyed by its path relative to `dir`.
fn snapshot(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    fn walk(base: &Path, dir: &Path, out: &mut BTreeMap<String, Vec<u8>>) {
        for entry in fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(base, &path, out);
            } else {
                let rel = path.strip_prefix(base).unwrap().to_string_lossy();
                out.insert(rel.to_string(), fs::read(&path).unwrap());
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(dir, dir, &mut out);
    out
}

/// The names of the files that hold `needle`, read as bytes.
fn files_with<'a>(files: &'a BTreeMap<String, Vec<u8>>, needle: &str) -> Vec<&'a str> {
    files
        .iter()
        .filter(|(_, bytes)| {
            bytes
                .windows(needle.len())
                .any(|window| window == needle.as_bytes())
        })
        .map(|(name, _)| name.as_str())
        .collect()
}

/// Fails if any file holds a domain, a mailbox name or a Message-ID of the
/// fixture.
fn assert_nothing_of_an_address_in(files: &BTreeMap<String, Vec<u8>>) {
    for needle in DOMAINS.iter().chain(&MAILBOXES).chain(&MESSAGE_IDS) {
        assert_eq!(files_with(files, needle), [""; 0], "{needle:?}");
    }
}

/// The messages of the fixture's one month, in the order they were written.
fn messages(files: &BTreeMap<String, Vec<u8>>) -> Vec<Message> {
    let archive: MonthArchive = serde_json::from_slice(&files["2026-03.json"]).unwrap();
    archive.messages
}

/// The name and slug of every contributor, sorted.
fn contributor_names(files: &BTreeMap<String, Vec<u8>>) -> Vec<(String, String)> {
    let contributors: Vec<ContributorEntry> =
        serde_json::from_slice(&files["contributors.json"]).unwrap();
    let mut names: Vec<(String, String)> =
        contributors.into_iter().map(|c| (c.name, c.slug)).collect();
    names.sort();
    names
}

/// `tests/fixtures/golden/members/` holds what the 0.10.0 binary wrote for
/// the fixture, before `--hide-addresses` existed. A run without the flag
/// must keep writing exactly those bytes.
#[test]
fn without_the_flag_parse_writes_what_0_10_0_wrote() {
    let golden = snapshot(&fixture_path("golden/members"));
    let written = parse_members("members-plain", &[]);

    assert_eq!(
        golden.keys().collect::<Vec<_>>(),
        written.keys().collect::<Vec<_>>()
    );
    for (name, bytes) in &golden {
        assert!(bytes == &written[name], "{name} differs from 0.10.0");
    }
    // The fixture is of use only if the unhidden output is full of what a
    // hidden one must not have: every one of the needles.
    for needle in DOMAINS.iter().chain(&MAILBOXES).chain(&MESSAGE_IDS) {
        assert_ne!(files_with(&written, needle), [""; 0], "{needle:?}");
    }
}

#[test]
fn with_the_flag_no_file_holds_an_address() {
    let plain = parse_members("scan-plain", &[]);
    let hidden = parse_members("scan-hidden", &["--hide-addresses"]);

    // Every file the run wrote is looked at, as bytes.
    assert_eq!(
        hidden.keys().collect::<Vec<_>>(),
        [
            "2026-03.json",
            "contributors.json",
            "index.json",
            "message-order.json",
            "meta.json"
        ]
    );
    assert_nothing_of_an_address_in(&hidden);
    // The slug 0.10.0 makes of a sender without a name is the address too.
    assert_eq!(
        files_with(&plain, "jane-roe-at-uni-example-edu"),
        ["contributors.json"]
    );
    for slug in ["uni-example-edu", "coi-ex-mpie-org"] {
        assert_ne!(files_with(&plain, slug), [""; 0], "{slug:?}");
        assert_eq!(files_with(&hidden, slug), [""; 0], "{slug:?}");
    }
}

#[test]
fn with_the_flag_the_text_is_the_same_but_for_its_addresses() {
    let hidden = messages(&parse_members("text-hidden", &["--hide-addresses"]));

    // Mailman's text form; a web address and a list's address stay.
    assert_eq!(
        hidden[0].body_plain,
        "Dear list,\n\
         \n\
         How do I root a tree with ape? Please reply to the list or to me\n\
         ([address hidden]). My notes are available at www.example.org/phylo\n\
         and the code at github.com/example/rooting.\n\
         \n\
         Jane\n\
         \n\
         _______________________________________________\n\
         R-sig-phylo mailing list - R-sig-phylo at r-project.org\n\
         https://stat.ethz.ch/mailman/listinfo/r-sig-phylo\n\
         Searchable archive at http://www.mail-archive.com/r-sig-phylo at r-project.org/"
    );
    // The plain sign and the first lossy separator; the footer stays.
    assert_eq!(
        hidden[1].body_plain,
        "On Mon, 2 Mar 2026, [address hidden] wrote:\n\
         > How do I root a tree with ape? Please reply to the list or to me\n\
         > ([address hidden]).\n\
         \n\
         Use root() with an outgroup. Lee Vane <[address hidden]> wrote\n\
         about this last year. I have moved: [address hidden]\n\
         is my address now.\n\
         \n\
         Mark\n\
         \n\
         _______________________________________________\n\
         R-@|g-phy|o m@|||ng ||@t - R-@|g-phy|o @end|ng |rom r-project@org\n\
         https://stat.ethz.ch/mailman/listinfo/r-sig-phylo"
    );
    assert_eq!(
        hidden[2].body_plain,
        "Dear all,\n\
         \n\
         J?rg M?ller ([address hidden]) and I are planning a workshop\n\
         at the Universit?t in June. Write to either of us to take part.\n\
         \n\
         Lee"
    );
    // An address broken over a line end, whose line end and quote mark
    // stay; `(at)`; and Mailman's link to a member's options.
    assert_eq!(
        hidden[3].body_plain,
        "On Tue, 3 Mar 2026 at 09:05, Lee Vane <[address hidden]\n\
         > > wrote:\n\
         > Write to either of us to take part.\n\
         \n\
         Count me in. My other address is [address hidden], and my\n\
         list settings are at\n\
         https://stat.ethz.ch/mailman/options/r-sig-phylo/[address hidden]\n\
         \n\
         Pat"
    );
    // A link rewritten by a mail service, a `mailto:` link, and two more
    // of the lossy separators.
    assert_eq!(
        hidden[4].body_plain,
        "The slides of the last meeting are behind this link:\n\
         https://eur01.safelinks.protection.outlook.com/?url=https%3A%2F%2Fwww.example.org%2Fslides&data=05%7C01%7C[address hidden]%7C8d2f%7C0&reserved=0\n\
         \n\
         Questions go to me (mailto:[address hidden]?subject=Slides) or to\n\
         Kim Lee, [address hidden], who gave the talk.\n\
         \n\
         Sam <[address hidden]>"
    );
    for msg in &hidden[2..4] {
        assert_eq!(
            msg.subject_clean, "Workshop: write to [address hidden]",
            "{}",
            msg.subject
        );
    }
    assert_eq!(
        hidden[3].subject,
        "Re: [R-sig-phylo] Workshop: write to [address hidden]"
    );
}

#[test]
fn with_the_flag_the_snippet_is_the_start_of_the_cleaned_text() {
    let hidden = messages(&parse_members("snippet-hidden", &["--hide-addresses"]));

    // The first 200 characters of the second message, as cleaned.
    assert_eq!(
        hidden[1].body_snippet,
        "On Mon, 2 Mar 2026, [address hidden] wrote:\n\
         > How do I root a tree with ape? Please reply to the list or to me\n\
         > ([address hidden]).\n\
         \n\
         Use root() with an outgroup. Lee Vane <[address hidden]> wrote\n\
         abo"
    );
    for msg in &hidden {
        assert!(msg.body_plain.starts_with(&msg.body_snippet), "{}", msg.id);
    }
}

#[test]
fn with_the_flag_a_sender_without_a_name_is_known_by_the_mailbox_name() {
    let files = parse_members("names-hidden", &["--hide-addresses"]);

    let senders: Vec<String> = messages(&files).into_iter().map(|m| m.from_name).collect();
    // The fourth sender's line is in a lossy form, which has lost a letter
    // of the mailbox name: what is left of it is the name.
    assert_eq!(
        senders,
        ["jane.roe", "Mark Poe", "Lee Vane", "pt.kim", "Sam Ray"]
    );
    let names = contributor_names(&files);
    let names: Vec<(&str, &str)> = names
        .iter()
        .map(|(name, slug)| (name.as_str(), slug.as_str()))
        .collect();
    assert_eq!(
        names,
        [
            ("Lee Vane", "lee-vane"),
            ("Mark Poe", "mark-poe"),
            ("Sam Ray", "sam-ray"),
            ("jane.roe", "jane-roe"),
            ("pt.kim", "pt-kim")
        ]
    );
}

#[test]
fn with_the_flag_threads_are_made_from_the_cleaned_messages() {
    let files = parse_members("threads-hidden", &["--hide-addresses"]);
    let archive: MonthArchive = serde_json::from_slice(&files["2026-03.json"]).unwrap();

    assert_eq!(archive.threads.len(), 3);
    assert_eq!(archive.threads[0].participants, ["Mark Poe", "jane.roe"]);
    assert_eq!(archive.threads[1].participants, ["Lee Vane", "pt.kim"]);
    assert_eq!(
        archive.threads[1].subject,
        "Workshop: write to [address hidden]"
    );
}

#[test]
fn with_the_flag_what_identifies_a_message_or_its_sender_is_unchanged() {
    let plain = parse_members("ids-plain", &[]);
    let hidden = parse_members("ids-hidden", &["--hide-addresses"]);

    // The files made of ids and dates alone.
    for file in ["index.json", "message-order.json", "meta.json"] {
        assert!(plain[file] == hidden[file], "{file} differs");
    }
    // The threads have the shape they have without the flag.
    let threads = |files: &BTreeMap<String, Vec<u8>>| -> Vec<(String, usize, String, String)> {
        let archive: MonthArchive = serde_json::from_slice(&files["2026-03.json"]).unwrap();
        archive
            .threads
            .into_iter()
            .map(|t| (t.id, t.message_count, t.root_message_id, t.last_reply))
            .collect()
    };
    assert_eq!(threads(&plain), threads(&hidden));
    assert_eq!(threads(&hidden).len(), 3);

    let (plain, hidden) = (messages(&plain), messages(&hidden));
    assert_eq!(plain.len(), 5);
    assert_eq!(hidden.len(), 5);
    for (plain, hidden) in plain.iter().zip(&hidden) {
        assert_eq!(hidden.from_email_hash, plain.from_email_hash);
        assert_eq!(hidden.id, plain.id);
        assert_eq!(hidden.thread_id, plain.thread_id);
        assert_eq!(hidden.thread_depth, plain.thread_depth);
        assert_eq!(hidden.date, plain.date);
        assert_eq!(hidden.month, plain.month);
        assert_eq!(hidden.source_url, plain.source_url);
    }
    // The hash is not that of an empty or a hidden address, and the id is
    // still the one made from the Message-ID as the source has it.
    assert_eq!(
        hidden[0].from_email_hash,
        "4767bde5b58adf52e7677cc95211d11df090aabc3926a901c8f3119e2ec4d7be"
    );
    let ids: Vec<&str> = hidden.iter().map(|m| m.id.as_str()).collect();
    assert_eq!(
        ids,
        [
            "msg-a3567c8d6e93",
            "msg-ceafe7cc2206",
            "msg-5ddae1225d36",
            "msg-ee6158163551",
            "msg-295d4473a10e"
        ]
    );
    let depths: Vec<u32> = hidden.iter().map(|m| m.thread_depth).collect();
    assert_eq!(depths, [0, 1, 0, 1, 0]);
}

#[test]
fn with_the_flag_identifiers_are_written_as_hashes_and_a_reply_still_names_its_parent() {
    let plain = messages(&parse_members("identifiers-plain", &[]));
    let files = parse_members("identifiers-hidden", &["--hide-addresses"]);
    let hidden = messages(&files);

    let written: Vec<&str> = plain.iter().map(|m| m.message_id.as_str()).collect();
    assert_eq!(written, MESSAGE_IDS);
    let written: Vec<&str> = hidden.iter().map(|m| m.message_id.as_str()).collect();
    assert_eq!(written, HIDDEN_IDS);

    // The second message answers the first, and the fourth the third.
    for (reply, parent) in [(1, 0), (3, 2)] {
        assert_eq!(
            plain[reply].in_reply_to.as_deref(),
            Some(MESSAGE_IDS[parent])
        );
        assert_eq!(
            hidden[reply].in_reply_to.as_deref(),
            Some(HIDDEN_IDS[parent])
        );
        assert_eq!(hidden[reply].references, [HIDDEN_IDS[parent]]);
        assert_eq!(
            hidden[reply].in_reply_to.as_deref(),
            Some(hidden[parent].message_id.as_str())
        );
    }
    for message in [0, 2, 4] {
        assert_eq!(hidden[message].in_reply_to, None);
        assert!(hidden[message].references.is_empty());
    }
    // No file holds an identifier as the source has it.
    for original in MESSAGE_IDS {
        assert_eq!(files_with(&files, original), [""; 0], "{original:?}");
    }
}

/// A `sources/` file for the fixture: a page for each message, and the five
/// characters the third one's page restores.
fn members_sources(name: &str) -> PathBuf {
    let dir = scratch_dir(name);
    fs::write(
        dir.join("2026-March.json"),
        "{\n  \"period\": \"2026-March\",\n  \"articles\": [\n    \"000101\",\n    \"000102\",\n    \"000103\",\n    \"000104\",\n    \"000105\"\n  ],\n  \"fixes\": {\n    \"000103\": \"\u{f6}\u{fc}\u{f6}\u{fc}\u{e4}\"\n  },\n  \"rejected\": {}\n}\n",
    )
    .unwrap();
    dir
}

const BASE_URL: &str = "https://lists.example.org/pipermail/r-sig-phylo/";

#[test]
fn with_the_flag_text_restored_from_a_source_page_is_cleaned_too() {
    let sources = members_sources("sources-hidden-sources");
    let files = parse_members(
        "sources-hidden",
        &[
            "--hide-addresses",
            "--sources",
            sources.to_str().unwrap(),
            "--base-url",
            BASE_URL,
        ],
    );

    // The page's characters are in the text, so the text was restored, and
    // the address they completed is not, so it was cleaned after that.
    let restored = &messages(&files)[2];
    assert_eq!(
        restored.body_plain,
        "Dear all,\n\
         \n\
         J\u{f6}rg M\u{fc}ller ([address hidden]) and I are planning a workshop\n\
         at the Universit\u{e4}t in June. Write to either of us to take part.\n\
         \n\
         Lee"
    );
    assert_eq!(restored.body_snippet, restored.body_plain);
    assert_eq!(
        restored.source_url.as_deref(),
        Some("https://lists.example.org/pipermail/r-sig-phylo/2026-March/000103.html")
    );
    assert_eq!(
        files_with(&files, "j\u{f6}rg.m\u{fc}ller"),
        [""; 0],
        "the restored mailbox name"
    );
    assert_nothing_of_an_address_in(&files);
}

#[test]
fn without_the_flag_text_restored_from_a_source_page_keeps_its_address() {
    let sources = members_sources("sources-plain-sources");
    let files = parse_members(
        "sources-plain",
        &[
            "--sources",
            sources.to_str().unwrap(),
            "--base-url",
            BASE_URL,
        ],
    );

    assert!(messages(&files)[2]
        .body_plain
        .contains("(j\u{f6}rg.m\u{fc}ller at uni-example.de)"));
}

/// An alias file as `--aliases` takes it. Some of the names such a file
/// gives to people are addresses: here the first is one as a whole, the
/// second holds one, and the third is a name.
fn members_aliases(name: &str) -> PathBuf {
    let path = scratch_dir(name).join("aliases.json");
    fs::write(
        &path,
        r#"{
  "aliases": [
    {
      "canonical_name": "jane.roe at uni-example.edu",
      "email_hashes": ["4767bde5b58adf52e7677cc95211d11df090aabc3926a901c8f3119e2ec4d7be"]
    },
    {
      "canonical_name": "Lee Vane <lee.vane@lab-example.net>",
      "email_hashes": ["300647a7d0e225928dd9e8db6d8ae7be33718d452d1bfed181852e7b54d98389"]
    },
    {
      "canonical_name": "Mark A. Poe",
      "email_hashes": ["aa674cdb29fabcc5fa20c05d13e7e2512d9fbf15b6b8259420fd6dc6e39c450f"]
    }
  ]
}
"#,
    )
    .unwrap();
    path
}

#[test]
fn with_the_flag_a_name_from_the_alias_file_is_cleaned_too() {
    let aliases = members_aliases("aliases-hidden-file");
    let files = parse_members(
        "aliases-hidden",
        &["--hide-addresses", "--aliases", aliases.to_str().unwrap()],
    );

    let names = contributor_names(&files);
    let names: Vec<(&str, &str)> = names
        .iter()
        .map(|(name, slug)| (name.as_str(), slug.as_str()))
        .collect();
    assert_eq!(
        names,
        [
            ("Lee Vane <[address hidden]>", "lee-vane-address-hidden"),
            ("Mark A. Poe", "mark-a-poe"),
            ("Sam Ray", "sam-ray"),
            ("jane.roe", "jane-roe"),
            ("pt.kim", "pt-kim")
        ]
    );
    assert_nothing_of_an_address_in(&files);
}

#[test]
fn without_the_flag_a_name_from_the_alias_file_is_written_as_it_is() {
    let aliases = members_aliases("aliases-plain-file");
    let files = parse_members("aliases-plain", &["--aliases", aliases.to_str().unwrap()]);

    let names = contributor_names(&files);
    let names: Vec<&str> = names.iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(
        names,
        [
            "Lee Vane <lee.vane@lab-example.net>",
            "Mark A. Poe",
            "Sam Ray",
            "jane.roe at uni-example.edu",
            "p@t.kim m@iii@g oii coi-ex@mpie.org"
        ]
    );
}

#[test]
fn with_the_flag_and_no_stats_the_one_file_written_holds_no_address() {
    let files = parse_members("no-stats-hidden", &["--hide-addresses", "--no-stats"]);

    assert_eq!(files.keys().collect::<Vec<_>>(), ["2026-03.json"]);
    assert_nothing_of_an_address_in(&files);
    assert_eq!(messages(&files).len(), 5);
}

#[test]
fn with_the_flag_a_message_in_two_files_is_written_once_and_clean() {
    // The source files a message sent at the turn of a period under both.
    // Here the second file holds the fixture's last two messages again.
    let raw = scratch_dir("twice-hidden-raw");
    let march = fs::read_to_string(fixture_path("members/2026-March.mbox")).unwrap();
    let again = march.find("From p@t.kim").unwrap();
    fs::write(raw.join("2026-March.mbox"), &march).unwrap();
    fs::write(raw.join("2026-April.mbox"), &march[again..]).unwrap();

    let once = parse_members("twice-hidden-once", &["--hide-addresses"]);
    let twice = snapshot(&parse_into("twice-hidden", &raw, &["--hide-addresses"]));

    // Every file is what the fixture alone gives, so each message is there
    // once, cleaned, under the id and in the thread it has there.
    assert_eq!(
        once.keys().collect::<Vec<_>>(),
        twice.keys().collect::<Vec<_>>()
    );
    for (name, bytes) in &once {
        assert!(bytes == &twice[name], "{name} differs");
    }
    assert_eq!(messages(&twice).len(), 5);
    assert_nothing_of_an_address_in(&twice);
}

#[test]
fn stats_over_what_a_hidden_parse_wrote_brings_no_address_back() {
    let parsed = parse_into(
        "stats-hidden-parsed",
        &fixture_path("members"),
        &["--hide-addresses", "--no-stats"],
    );
    let output = scratch_dir("stats-hidden");
    let run = rmail_parser()
        .args(["stats", "--list", "r-sig-phylo", "--input"])
        .arg(&parsed)
        .arg("--output")
        .arg(&output)
        .output()
        .unwrap();
    assert!(
        run.status.success(),
        "{}",
        String::from_utf8_lossy(&run.stderr)
    );

    let files = snapshot(&output);
    assert_eq!(
        files.keys().collect::<Vec<_>>(),
        [
            "contributors.json",
            "index.json",
            "message-order.json",
            "meta.json"
        ]
    );
    assert_nothing_of_an_address_in(&files);
    // And it is what the hidden parse writes itself when it makes them.
    let direct = parse_members("stats-hidden-direct", &["--hide-addresses"]);
    for (name, bytes) in &files {
        assert!(bytes == &direct[name], "{name} differs");
    }
}
