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
    // The third message was paired with page 084717, and that page named
    // another message.
    let mut rejected = BTreeMap::new();
    rejected.insert("084717".to_string(), "msg-084799@example.org".to_string());
    PeriodSources {
        period: "2026-October".to_string(),
        articles: vec![
            Some("084714".to_string()),
            Some("084715".to_string()),
            None,
            Some("084716".to_string()),
        ],
        fixes,
        rejected,
        pending: Vec::new(),
        parents_rechecked: false,
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
    let expected = "{\n  \"period\": \"2026-October\",\n  \"articles\": [\n    \"084714\",\n    \"084715\",\n    null,\n    \"084716\"\n  ],\n  \"fixes\": {\n    \"084715\": \"\",\n    \"084716\": \"?\u{2019}?\u{e4}\"\n  },\n  \"rejected\": {\n    \"084717\": \"msg-084799@example.org\"\n  }\n}\n";
    assert_eq!(written, expected);
    fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn test_save_writes_rejected_even_when_nothing_was_rejected() {
    let dir = scratch_dir("nothing-rejected");
    let mut nothing = sample();
    nothing.rejected.clear();
    save(&dir, &nothing).unwrap();
    let written = fs::read_to_string(dir.join("2026-October.json")).unwrap();
    assert!(
        written.ends_with("  },\n  \"rejected\": {}\n}\n"),
        "{written}"
    );
    assert_eq!(load(&dir, "2026-October").unwrap().unwrap(), nothing);
    fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn test_load_refuses_a_file_written_before_rejected_pairings_were_recorded() {
    let dir = scratch_dir("old-format");
    // What v0.9.0 wrote: no `rejected` field.
    let old = "{\n  \"period\": \"2026-October\",\n  \"articles\": [\n    \"084714\"\n  ],\n  \"fixes\": {}\n}\n";
    fs::write(dir.join("2026-October.json"), old).unwrap();

    let err = load(&dir, "2026-October").unwrap_err();
    let message = format!("{:#}", err);
    assert!(message.contains("2026-October.json"), "{message}");
    assert!(message.contains("missing field `rejected`"), "{message}");
    // The file is left as it was, for a person to look at.
    assert_eq!(
        fs::read_to_string(dir.join("2026-October.json")).unwrap(),
        old
    );
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

/// The names of everything in `dir`, sorted.
fn names_in(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

#[test]
fn test_save_replaces_the_file_that_was_there_and_leaves_no_temporary_file() {
    let dir = scratch_dir("replace");
    save(&dir, &sample()).unwrap();
    assert_eq!(names_in(&dir), ["2026-October.json"]);

    let mut changed = sample();
    changed.articles.push(Some("084717".to_string()));
    changed.fixes.remove("084715");
    save(&dir, &changed).unwrap();

    assert_eq!(load(&dir, "2026-October").unwrap().unwrap(), changed);
    assert_eq!(names_in(&dir), ["2026-October.json"]);
    fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn test_save_puts_a_new_file_in_place_and_never_writes_into_the_old_one() {
    let dir = scratch_dir("in-place");
    save(&dir, &sample()).unwrap();
    let path = dir.join("2026-October.json");
    let before = fs::read(&path).unwrap();
    // A second name for the file as it is now. Writing into the file would
    // show under this name too. Putting a new file in its place does not,
    // so at no moment is the period's file half written.
    let old = dir.join("as-it-was");
    fs::hard_link(&path, &old).unwrap();

    let mut changed = sample();
    changed.articles.push(None);
    save(&dir, &changed).unwrap();

    assert!(before == fs::read(&old).unwrap());
    assert_eq!(load(&dir, "2026-October").unwrap().unwrap(), changed);
    fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn test_save_that_cannot_write_leaves_the_file_as_it_was() {
    let dir = scratch_dir("cannot-write");
    save(&dir, &sample()).unwrap();
    let before = fs::read(dir.join("2026-October.json")).unwrap();
    // A directory where the temporary file goes, so writing it fails.
    fs::create_dir(dir.join("2026-October.json.tmp")).unwrap();

    let mut changed = sample();
    changed.articles.push(None);
    let err = save(&dir, &changed).unwrap_err();

    assert!(format!("{:#}", err).contains("2026-October.json"));
    assert!(before == fs::read(dir.join("2026-October.json")).unwrap());
    fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn test_save_that_cannot_put_the_file_in_place_leaves_no_temporary_file() {
    let dir = scratch_dir("cannot-rename");
    // A directory where the period's file goes: the temporary file is
    // written, and then cannot be renamed over it.
    fs::create_dir(dir.join("2026-October.json")).unwrap();

    let err = save(&dir, &sample()).unwrap_err();

    assert!(format!("{:#}", err).contains("2026-October.json"));
    assert_eq!(names_in(&dir), ["2026-October.json"]);
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
fn test_load_refuses_a_file_that_describes_another_period() {
    let dir = scratch_dir("another-period");
    save(&dir, &sample()).unwrap();
    // October's file under September's name, as a copy or a rename leaves it.
    fs::copy(
        dir.join("2026-October.json"),
        dir.join("2026-September.json"),
    )
    .unwrap();

    let err = load(&dir, "2026-September").unwrap_err();
    let message = format!("{:#}", err);
    assert!(message.contains("2026-September.json"), "{message}");
    assert!(
        message.contains("describes period 2026-October, not 2026-September"),
        "{message}"
    );

    // The file it was copied from is read as before.
    assert_eq!(load(&dir, "2026-October").unwrap().unwrap(), sample());
    fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn test_load_fails_when_the_file_cannot_be_read() {
    let dir = scratch_dir("unreadable");
    // Making the directory a regular file means opening the period file
    // fails, and not because it is missing.
    let not_a_dir = dir.join("not-a-dir");
    fs::write(&not_a_dir, "").unwrap();
    let err = load(&not_a_dir, "2026-October").unwrap_err();
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
