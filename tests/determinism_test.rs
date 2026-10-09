//! Parsing the same archive twice must write byte-identical files.
//!
//! The archive repositories re-parse every day and commit whatever changed,
//! and the Parquet build downstream inherits the message order. Several sorts
//! had no tie-break and were fed from hash maps, so messages sharing a
//! timestamp, threads starting in the same second, and contributors with equal
//! counts came out in a different order on every run. Lists with no new mail
//! were rewritten daily as a result.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use rmail_parser::pipeline;

/// Hash-map iteration order is re-seeded per map, so a handful of runs in one
/// process is enough to expose an order that depends on it.
const RUNS: usize = 12;

fn scratch_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "rmail-parser-determinism-{}-{}",
        std::process::id(),
        name
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

/// An archive built to tie everywhere: forty messages sent in the same second,
/// each starting its own thread, from four senders with ten messages apiece,
/// each sender using two spellings of their name five times.
fn tied_mbox() -> String {
    let mut mbox = String::new();
    for i in 0..40 {
        let sender = i % 4;
        let name = if (i / 4) % 2 == 0 {
            format!("Sender {sender}")
        } else {
            format!("S. Ender {sender}")
        };
        mbox.push_str(&format!(
            "From user{sender}@example.com  Mon Feb  2 12:00:00 2026\n\
             From: {name} <user{sender}@example.com>\n\
             Date: Mon, 2 Feb 2026 12:00:00 +0000\n\
             Subject: [R] topic {i}\n\
             Message-ID: <tie{i:03}@example.com>\n\
             Content-Type: text/plain; charset=UTF-8\n\
             \n\
             Body of message {i}.\n\
             \n"
        ));
    }
    mbox
}

/// Every file under `dir`, keyed by its path relative to `dir`.
fn snapshot(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    fn walk(base: &Path, dir: &Path, out: &mut BTreeMap<String, Vec<u8>>) {
        for entry in fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(base, &path, out);
            } else {
                let rel = path.strip_prefix(base).unwrap().to_string_lossy().to_string();
                out.insert(rel, fs::read(&path).unwrap());
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(dir, dir, &mut out);
    out
}

/// Panics naming the first file that differs between two snapshots.
fn assert_same(first: &BTreeMap<String, Vec<u8>>, other: &BTreeMap<String, Vec<u8>>, run: usize) {
    assert_eq!(
        first.keys().collect::<Vec<_>>(),
        other.keys().collect::<Vec<_>>(),
        "run {run} wrote a different set of files"
    );
    for (name, bytes) in first {
        assert!(bytes == &other[name], "run {run} wrote a different {name}");
    }
}

#[test]
fn parsing_the_same_archive_twice_writes_identical_files() {
    let input = scratch_dir("parse-input");
    fs::write(input.join("2026-February.mbox"), tied_mbox()).unwrap();

    let mut first: Option<BTreeMap<String, Vec<u8>>> = None;
    for run in 0..RUNS {
        let output = scratch_dir(&format!("parse-output-{run}"));
        pipeline::run_parse(&input, &output, "tied-list", true, None).unwrap();
        let snap = snapshot(&output);
        fs::remove_dir_all(&output).unwrap();

        let meta: serde_json::Value = serde_json::from_slice(&snap["meta.json"]).unwrap();
        assert_eq!(meta["total_messages"], 40, "the fixture should parse in full");

        match &first {
            None => first = Some(snap),
            Some(expected) => assert_same(expected, &snap, run),
        }
    }
    fs::remove_dir_all(&input).unwrap();
}

#[test]
fn aggregating_the_same_lists_twice_writes_identical_files() {
    // Two lists with the same four equally active contributors, so every
    // aggregated contributor and every per-list count ties.
    let parse_input = scratch_dir("aggregate-parse-input");
    fs::write(parse_input.join("2026-February.mbox"), tied_mbox()).unwrap();
    let parsed = scratch_dir("aggregate-parsed");
    pipeline::run_parse(&parse_input, &parsed, "tied-list", true, None).unwrap();

    let lists = scratch_dir("aggregate-lists");
    for list in ["list-a", "list-b"] {
        fs::create_dir_all(lists.join(list)).unwrap();
        fs::copy(parsed.join("contributors.json"), lists.join(list).join("contributors.json")).unwrap();
    }

    let out_dir = scratch_dir("aggregate-output");
    let mut first: Option<Vec<u8>> = None;
    for run in 0..RUNS {
        let output = out_dir.join(format!("contributors-{run}.json"));
        pipeline::run_aggregate(&lists, &output, None).unwrap();
        let bytes = fs::read(&output).unwrap();
        match &first {
            None => first = Some(bytes),
            Some(expected) => assert!(expected == &bytes, "run {run} wrote a different aggregate"),
        }
    }
    for dir in [parse_input, parsed, lists, out_dir] {
        fs::remove_dir_all(&dir).unwrap();
    }
}
