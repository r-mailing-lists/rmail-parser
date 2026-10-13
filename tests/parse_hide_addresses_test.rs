//! Whole runs of `parse` over `tests/fixtures/members/`, a sample of a list
//! whose archive only its members can read. Its three messages name four
//! people, in the three forms an address takes in these archives.

mod common;

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::process::Command;

use common::{fixture_path, scratch_dir};

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
                let rel = path.strip_prefix(base).unwrap().to_string_lossy().to_string();
                out.insert(rel, fs::read(&path).unwrap());
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(dir, dir, &mut out);
    out
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
