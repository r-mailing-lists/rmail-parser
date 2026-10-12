use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use rmail_parser::sources::{
    apply_fixes, load, needs_page, period_of, period_sort_key, save, PeriodSources,
};

fn scratch_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "rmail-parser-sources-{}-{}",
        std::process::id(),
        name
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn sample() -> PeriodSources {
    let mut fixes = BTreeMap::new();
    // Inserted out of order: the file must still list them sorted.
    fixes.insert("084716".to_string(), "?\u{2019}?\u{e4}".to_string());
    fixes.insert("084715".to_string(), String::new());
    PeriodSources {
        period: "2026-October".to_string(),
        articles: vec![
            Some("084714".to_string()),
            Some("084715".to_string()),
            None,
            Some("084716".to_string()),
        ],
        fixes,
    }
}

#[test]
fn test_period_of_is_the_file_stem() {
    assert_eq!(
        period_of(Path::new("raw/2026-October.mbox")),
        "2026-October"
    );
    assert_eq!(period_of(Path::new("raw/2026q4.mbox")), "2026q4");
    assert_eq!(period_of(Path::new("raw/2026.mbox")), "2026");
    assert_eq!(period_of(Path::new("2003-May.txt")), "2003-May");
}

#[test]
fn test_period_sort_key_orders_all_three_schemes() {
    // Alphabetical order is wrong for months: April sorts before March.
    let mut monthly = vec!["2026-April", "2025-December", "2026-March", "2026-October"];
    monthly.sort_by_key(|p| std::cmp::Reverse(period_sort_key(p)));
    assert_eq!(
        monthly,
        ["2026-October", "2026-April", "2026-March", "2025-December"]
    );

    let mut quarterly = vec!["2025q4", "2026q2", "2026q1", "2026q4"];
    quarterly.sort_by_key(|p| std::cmp::Reverse(period_sort_key(p)));
    assert_eq!(quarterly, ["2026q4", "2026q2", "2026q1", "2025q4"]);

    let mut annual = vec!["1998", "2026", "2011"];
    annual.sort_by_key(|p| std::cmp::Reverse(period_sort_key(p)));
    assert_eq!(annual, ["2026", "2011", "1998"]);

    // A list that changed scheme part way through still sorts by time.
    assert!(period_sort_key("2026q1") > period_sort_key("2025"));
    assert!(period_sort_key("2026-April") > period_sort_key("2026q1"));
    // A name that fits no scheme goes last when the newest comes first.
    assert!(period_sort_key("notes") < period_sort_key("1997"));
    assert!(period_sort_key("2026q7") < period_sort_key("1997"));
}

#[test]
fn test_save_writes_a_fixed_layout() {
    let dir = scratch_dir("layout");
    save(&dir, &sample()).unwrap();
    let written = fs::read_to_string(dir.join("2026-October.json")).unwrap();
    let expected = "{\n  \"period\": \"2026-October\",\n  \"articles\": [\n    \"084714\",\n    \"084715\",\n    null,\n    \"084716\"\n  ],\n  \"fixes\": {\n    \"084715\": \"\",\n    \"084716\": \"?\u{2019}?\u{e4}\"\n  }\n}\n";
    assert_eq!(written, expected);
    fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn test_load_returns_what_save_wrote_and_saving_again_changes_nothing() {
    let dir = scratch_dir("round-trip");
    save(&dir, &sample()).unwrap();
    let first = fs::read(dir.join("2026-October.json")).unwrap();

    let loaded = load(&dir, "2026-October").unwrap().unwrap();
    assert_eq!(loaded, sample());

    save(&dir, &loaded).unwrap();
    assert!(first == fs::read(dir.join("2026-October.json")).unwrap());
    fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn test_load_of_a_period_without_a_file_is_none() {
    let dir = scratch_dir("missing");
    assert!(load(&dir, "2026-October").unwrap().is_none());
    // A directory that does not exist at all is the same case.
    assert!(load(&dir.join("nowhere"), "2026-October")
        .unwrap()
        .is_none());
    fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn test_load_of_a_damaged_file_is_an_error_naming_it() {
    let dir = scratch_dir("damaged");
    fs::write(dir.join("2026-October.json"), "{\"period\": \"2026-Oct").unwrap();
    let err = load(&dir, "2026-October").unwrap_err();
    assert!(format!("{:#}", err).contains("2026-October.json"));
    fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn test_needs_page() {
    assert!(needs_page("The current var() applies Bessel?s correction."));
    assert!(needs_page("an der Universit?t Z?rich"));
    // A real question mark has no letter after it.
    assert!(!needs_page("Is this a bug? I think so."));
    assert!(!needs_page("What?! No."));
    // Query strings are not lost characters.
    assert!(!needs_page(
        "see https://bugs.r-project.org/show_bug.cgi?id=19144"
    ));
    assert!(!needs_page("see <https://example.org/a?b=1>"));
    assert!(!needs_page("see www.example.org/search?q=lme4 for more"));
    // ... but a lost character elsewhere in the same body still counts.
    assert!(needs_page(
        "see https://example.org/a?b=1 for Bessel?s correction"
    ));
    assert!(!needs_page(""));
    assert!(!needs_page("?"));
}

#[test]
fn test_apply_fixes_replaces_each_question_mark_in_order() {
    assert_eq!(
        apply_fixes("Is it Bessel?s or Universit?t?", "\u{2019}\u{e4}?").unwrap(),
        "Is it Bessel\u{2019}s or Universit\u{e4}t?"
    );
    // A fix made only of `?` changes nothing.
    assert_eq!(apply_fixes("why? how?", "??").unwrap(), "why? how?");
    assert_eq!(apply_fixes("no marks here", "").unwrap(), "no marks here");
}

#[test]
fn test_apply_fixes_refuses_a_fix_of_the_wrong_length() {
    // The body has gained a `?` since the fix was made: every later
    // character would land one place early, so nothing is applied.
    assert_eq!(
        apply_fixes("Why? Bessel?s Universit?t", "\u{2019}\u{e4}"),
        None
    );
    // ... and the same when it has lost one.
    assert_eq!(apply_fixes("Bessel?s", "\u{2019}\u{e4}"), None);
    assert_eq!(apply_fixes("Bessel?s", ""), None);
}
