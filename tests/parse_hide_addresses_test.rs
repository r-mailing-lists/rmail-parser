//! Whole runs of `parse` over `tests/fixtures/members/`, a sample of a list
//! whose archive only its members can read. Its three messages name four
//! people, in the three forms an address takes in these archives.

mod common;

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::process::Command;

use common::{fixture_path, scratch_dir};
use rmail_parser::message::Message;
use rmail_parser::output::{ContributorEntry, MonthArchive};

/// The four addresses of the fixture, each in the three forms. Not every
/// form of every address is in the fixture, and none may be in the output.
const ADDRESSES: [[&str; 3]; 4] = [
    [
        "jane.roe at uni-example.edu",
        "j@ne@roe @end|ng |rom un|-ex@mp|e@edu",
        "jane.roe@uni-example.edu",
    ],
    [
        "mark.poe at inst-example.org",
        "m@rk@poe @end|ng |rom |n@t-ex@mp|e@org",
        "mark.poe@inst-example.org",
    ],
    [
        "lee.vane at lab-example.net",
        "|ee@v@ne @end|ng |rom |@b-ex@mp|e@net",
        "lee.vane@lab-example.net",
    ],
    [
        "j?rg.m?ller at uni-example.de",
        "j?rg@m?||er @end|ng |rom un|-ex@mp|e@de",
        "j?rg.m?ller@uni-example.de",
    ],
];

/// What would be left of those addresses if only the name were hidden: a
/// separator and the domain after it. The fixture's Message-IDs are at hosts
/// under these domains, `mail.uni-example.edu` for one, and are kept.
const DOMAINS: [&str; 12] = [
    " at uni-example.edu",
    "|rom un|-ex@mp|e@edu",
    "@uni-example.edu",
    " at inst-example.org",
    "|rom |n@t-ex@mp|e@org",
    "@inst-example.org",
    " at lab-example.net",
    "|rom |@b-ex@mp|e@net",
    "@lab-example.net",
    " at uni-example.de",
    "|rom un|-ex@mp|e@de",
    "@uni-example.de",
];

/// Runs `rmail-parser parse` over the fixture into a fresh directory and
/// returns every file it wrote, keyed by its path under that directory.
fn parse_members(name: &str, extra: &[&str]) -> BTreeMap<String, Vec<u8>> {
    let output = scratch_dir(name);
    let run = Command::new(env!("CARGO_BIN_EXE_rmail-parser"))
        .args(["parse", "--list", "r-sig-phylo", "--input"])
        .arg(fixture_path("members"))
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
    snapshot(&output)
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

/// True when `needle` is somewhere in `bytes`.
fn holds(bytes: &[u8], needle: &str) -> bool {
    bytes
        .windows(needle.len())
        .any(|window| window == needle.as_bytes())
}

/// The names of the files that hold `needle`.
fn files_with<'a>(files: &'a BTreeMap<String, Vec<u8>>, needle: &str) -> Vec<&'a str> {
    files
        .iter()
        .filter(|(_, bytes)| holds(bytes, needle))
        .map(|(name, _)| name.as_str())
        .collect()
}

/// The messages of the fixture's one month, in the order they were written.
fn messages(files: &BTreeMap<String, Vec<u8>>) -> Vec<Message> {
    let archive: MonthArchive = serde_json::from_slice(&files["2026-03.json"]).unwrap();
    archive.messages
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
    // The fixture is of use only if the unhidden output is full of addresses.
    let month = String::from_utf8(written["2026-03.json"].clone()).unwrap();
    assert!(month.contains("jane.roe at uni-example.edu"));
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
    for forms in ADDRESSES {
        // The scan means something only if a run without the flag fails it.
        assert!(
            forms
                .iter()
                .any(|form| !files_with(&plain, form).is_empty()),
            "{forms:?} is not in the fixture"
        );
        for form in forms {
            assert_eq!(files_with(&hidden, form), [""; 0], "{form:?}");
        }
    }
    for domain in DOMAINS {
        assert_eq!(files_with(&hidden, domain), [""; 0], "{domain:?}");
    }
    // The slug 0.10.0 makes of a sender without a name is the address too.
    assert_eq!(
        files_with(&plain, "jane-roe-at-uni-example-edu"),
        ["contributors.json"]
    );
    assert_eq!(files_with(&hidden, "uni-example-edu"), [""; 0]);
}

#[test]
fn with_the_flag_the_text_is_the_same_but_for_its_addresses() {
    let hidden = messages(&parse_members("text-hidden", &["--hide-addresses"]));

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
    assert_eq!(
        hidden[2].subject,
        "[R-sig-phylo] Workshop: write to [address hidden]"
    );
    assert_eq!(
        hidden[2].subject_clean,
        "Workshop: write to [address hidden]"
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

    assert_eq!(messages(&files)[0].from_name, "jane.roe");
    let contributors: Vec<ContributorEntry> =
        serde_json::from_slice(&files["contributors.json"]).unwrap();
    let mut names: Vec<(&str, &str)> = contributors
        .iter()
        .map(|c| (c.name.as_str(), c.slug.as_str()))
        .collect();
    names.sort();
    assert_eq!(
        names,
        [
            ("Lee Vane", "lee-vane"),
            ("Mark Poe", "mark-poe"),
            ("jane.roe", "jane-roe")
        ]
    );
}

#[test]
fn with_the_flag_threads_are_made_from_the_cleaned_messages() {
    let files = parse_members("threads-hidden", &["--hide-addresses"]);
    let archive: MonthArchive = serde_json::from_slice(&files["2026-03.json"]).unwrap();

    assert_eq!(archive.threads.len(), 2);
    assert_eq!(archive.threads[0].participants, ["Mark Poe", "jane.roe"]);
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
    let (plain, hidden) = (messages(&plain), messages(&hidden));
    assert_eq!(plain.len(), 3);
    assert_eq!(hidden.len(), 3);
    for (plain, hidden) in plain.iter().zip(&hidden) {
        assert_eq!(hidden.from_email_hash, plain.from_email_hash);
        assert_eq!(hidden.id, plain.id);
        assert_eq!(hidden.message_id, plain.message_id);
        assert_eq!(hidden.in_reply_to, plain.in_reply_to);
        assert_eq!(hidden.references, plain.references);
        assert_eq!(hidden.thread_id, plain.thread_id);
        assert_eq!(hidden.thread_depth, plain.thread_depth);
        assert_eq!(hidden.date, plain.date);
        assert_eq!(hidden.month, plain.month);
        assert_eq!(hidden.source_url, plain.source_url);
    }
    // The hash is not that of an empty or a hidden address.
    assert_eq!(
        hidden[0].from_email_hash,
        "4767bde5b58adf52e7677cc95211d11df090aabc3926a901c8f3119e2ec4d7be"
    );
    assert_eq!(
        hidden[1].in_reply_to.as_deref(),
        Some("<20260302101500.1234@mail.uni-example.edu>")
    );
}

/// A `sources/` file for the fixture: a page for each message, and the five
/// characters the third one's page restores.
fn members_sources(name: &str) -> std::path::PathBuf {
    let dir = scratch_dir(name);
    fs::write(
        dir.join("2026-March.json"),
        "{\n  \"period\": \"2026-March\",\n  \"articles\": [\n    \"000101\",\n    \"000102\",\n    \"000103\"\n  ],\n  \"fixes\": {\n    \"000103\": \"\u{f6}\u{fc}\u{f6}\u{fc}\u{e4}\"\n  },\n  \"rejected\": {}\n}\n",
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
    for needle in [
        "j\u{f6}rg.m\u{fc}ller at uni-example.de",
        " at uni-example.de",
        "j?rg.m?ller",
    ] {
        assert_eq!(files_with(&files, needle), [""; 0], "{needle:?}");
    }
    for domain in DOMAINS {
        assert_eq!(files_with(&files, domain), [""; 0], "{domain:?}");
    }
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
fn members_aliases(name: &str) -> std::path::PathBuf {
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

/// The name and slug of every contributor, sorted.
fn contributor_names(files: &BTreeMap<String, Vec<u8>>) -> Vec<(String, String)> {
    let contributors: Vec<ContributorEntry> =
        serde_json::from_slice(&files["contributors.json"]).unwrap();
    let mut names: Vec<(String, String)> =
        contributors.into_iter().map(|c| (c.name, c.slug)).collect();
    names.sort();
    names
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
            ("jane.roe", "jane-roe")
        ]
    );
    for forms in ADDRESSES {
        for form in forms {
            assert_eq!(files_with(&files, form), [""; 0], "{form:?}");
        }
    }
    for domain in DOMAINS {
        assert_eq!(files_with(&files, domain), [""; 0], "{domain:?}");
    }
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
            "jane.roe at uni-example.edu"
        ]
    );
}
