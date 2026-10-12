use std::path::Path;
use std::process::{Command, Output};

fn rmail_parser(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rmail-parser"))
        .args(args)
        .output()
        .unwrap()
}

#[test]
fn test_version_flag_prints_the_crate_version() {
    let run = rmail_parser(&["--version"]);
    assert!(run.status.success());
    assert_eq!(
        String::from_utf8_lossy(&run.stdout).trim(),
        format!("rmail-parser {}", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn test_parse_refuses_sources_without_base_url() {
    let run = rmail_parser(&[
        "parse",
        "--input",
        "raw",
        "--output",
        "processed",
        "--list",
        "r-devel",
        "--sources",
        "sources",
    ]);
    assert!(!run.status.success());
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(
        stderr.contains("required arguments were not provided"),
        "{stderr}"
    );
    assert!(stderr.contains("--base-url <BASE_URL>"), "{stderr}");
}

#[test]
fn test_parse_refuses_base_url_without_sources() {
    let run = rmail_parser(&[
        "parse",
        "--input",
        "raw",
        "--output",
        "processed",
        "--list",
        "r-devel",
        "--base-url",
        "https://lists.example.org/pipermail/r-devel/",
    ]);
    assert!(!run.status.success());
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(
        stderr.contains("required arguments were not provided"),
        "{stderr}"
    );
    assert!(stderr.contains("--sources <SOURCES>"), "{stderr}");
}

#[test]
fn test_sources_defaults_to_300_requests_a_second_apart() {
    let run = rmail_parser(&["sources", "--help"]);
    assert!(run.status.success());
    let help = String::from_utf8_lossy(&run.stdout);
    for expected in [
        "--raw",
        "--out",
        "--base-url",
        "--max-requests",
        "--delay-ms",
        "--max-periods",
    ] {
        assert!(help.contains(expected), "{expected} missing from:\n{help}");
    }
    assert!(help.contains("[default: 300]"), "{help}");
    assert!(help.contains("[default: 1000]"), "{help}");
}

#[test]
fn test_sources_that_stops_early_says_so_and_exits_zero() {
    let raw = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/raw");
    let out = std::env::temp_dir().join(format!("rmail-parser-cli-{}-out", std::process::id()));
    let _ = std::fs::remove_dir_all(&out);

    // A budget of zero stops the run before its first request, so this
    // exercises the real fetcher's wiring without sending anything.
    let run = rmail_parser(&[
        "sources",
        "--raw",
        raw.to_str().unwrap(),
        "--out",
        out.to_str().unwrap(),
        "--base-url",
        "https://lists.example.org/pipermail/r-devel/",
        "--max-requests",
        "0",
    ]);

    assert!(run.status.success());
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(
        stderr.contains("Stopped early: request budget of 0 spent"),
        "{stderr}"
    );
    assert!(!out.exists());
}

#[test]
fn test_sources_with_nothing_left_to_fetch_says_every_period_is_up_to_date() {
    let raw = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/raw");
    let out =
        std::env::temp_dir().join(format!("rmail-parser-cli-{}-complete", std::process::id()));
    let _ = std::fs::remove_dir_all(&out);
    std::fs::create_dir_all(&out).unwrap();
    // The fixture's period, with both of the pages it needs already fetched.
    let complete = "{\n  \"period\": \"2026-October\",\n  \"articles\": [\n    \"084714\",\n    \"084715\",\n    \"084716\",\n    \"084717\"\n  ],\n  \"fixes\": {\n    \"084715\": \"\",\n    \"084716\": \"\"\n  },\n  \"rejected\": {}\n}\n";
    std::fs::write(out.join("2026-October.json"), complete).unwrap();

    // A budget of zero again: were anything asked for, the run would stop
    // early and say so.
    let run = rmail_parser(&[
        "sources",
        "--raw",
        raw.to_str().unwrap(),
        "--out",
        out.to_str().unwrap(),
        "--base-url",
        "https://lists.example.org/pipermail/r-devel/",
        "--max-requests",
        "0",
    ]);

    assert!(run.status.success());
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(
        stderr.contains("Requests that failed:          0\n"),
        "{stderr}"
    );
    assert!(
        stderr.contains("Pages the source did not have: 0\n"),
        "{stderr}"
    );
    assert!(stderr.contains("Every period is up to date"), "{stderr}");
    // The lines the pilot's measurements are read from.
    for line in [
        "Indexes ahead of the raw file: 0\n",
        "Messages in periods looked at: 4\n",
        "Messages without a link:       0\n",
        "Rejected pairings on file:     0\n",
        "Older periods not looked at:   0\n",
    ] {
        assert!(stderr.contains(line), "{line:?} missing from:\n{stderr}");
    }
    assert_eq!(
        std::fs::read_to_string(out.join("2026-October.json")).unwrap(),
        complete
    );
    std::fs::remove_dir_all(&out).unwrap();
}

#[test]
fn test_sources_that_could_not_fetch_everything_does_not_say_up_to_date() {
    let raw = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/raw");
    let out = std::env::temp_dir().join(format!("rmail-parser-cli-{}-failed", std::process::id()));
    let _ = std::fs::remove_dir_all(&out);

    // The base URL is not a URL, so the one request this run may make fails
    // before a connection is opened: nothing is sent anywhere.
    let run = rmail_parser(&[
        "sources",
        "--raw",
        raw.to_str().unwrap(),
        "--out",
        out.to_str().unwrap(),
        "--base-url",
        "not a url",
        "--max-requests",
        "1",
        "--delay-ms",
        "0",
    ]);

    // Not a stop, and not an error: the period is left for the next run.
    assert!(run.status.success());
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(
        stderr.contains("Requests that failed:          1\n"),
        "{stderr}"
    );
    assert!(
        stderr.contains("Pages the source did not have: 0\n"),
        "{stderr}"
    );
    assert!(
        stderr.contains("Not everything was fetched; the next run will ask again"),
        "{stderr}"
    );
    assert!(!stderr.contains("Every period is up to date"), "{stderr}");
    assert!(!stderr.contains("Stopped early"), "{stderr}");
    assert!(!out.exists());
}

#[test]
fn test_sources_with_max_periods_does_not_claim_every_period_is_up_to_date() {
    let raw = std::env::temp_dir().join(format!(
        "rmail-parser-cli-{}-periods-raw",
        std::process::id()
    ));
    let out = std::env::temp_dir().join(format!(
        "rmail-parser-cli-{}-periods-out",
        std::process::id()
    ));
    for dir in [&raw, &out] {
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).unwrap();
    }
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    std::fs::copy(
        fixtures.join("raw/2026-October.mbox"),
        raw.join("2026-October.mbox"),
    )
    .unwrap();
    // An older period that has never been indexed.
    std::fs::copy(fixtures.join("sample.mbox"), raw.join("2026-February.mbox")).unwrap();
    let complete = "{\n  \"period\": \"2026-October\",\n  \"articles\": [\n    \"084714\",\n    \"084715\",\n    \"084716\",\n    \"084717\"\n  ],\n  \"fixes\": {\n    \"084715\": \"\",\n    \"084716\": \"\"\n  },\n  \"rejected\": {}\n}\n";
    std::fs::write(out.join("2026-October.json"), complete).unwrap();

    // A budget of zero: were February looked at, the run would stop early.
    let run = rmail_parser(&[
        "sources",
        "--raw",
        raw.to_str().unwrap(),
        "--out",
        out.to_str().unwrap(),
        "--base-url",
        "https://lists.example.org/pipermail/r-devel/",
        "--max-requests",
        "0",
        "--max-periods",
        "1",
    ]);

    assert!(run.status.success());
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(
        stderr.contains("Older periods not looked at:   1\n"),
        "{stderr}"
    );
    assert!(
        stderr.contains("The periods looked at are up to date"),
        "{stderr}"
    );
    assert!(!stderr.contains("Every period is up to date"), "{stderr}");
    assert!(!stderr.contains("Stopped early"), "{stderr}");
    assert!(!out.join("2026-February.json").exists());
    std::fs::remove_dir_all(&raw).unwrap();
    std::fs::remove_dir_all(&out).unwrap();
}

#[test]
fn test_sources_prints_each_count_on_its_own_line() {
    let raw = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/raw");
    let out = std::env::temp_dir().join(format!("rmail-parser-cli-{}-counts", std::process::id()));
    let _ = std::fs::remove_dir_all(&out);
    std::fs::create_dir_all(&out).unwrap();
    // The fixture's period as a file with different numbers on every line:
    // four positions, two without a page, one rejected pairing. The raw file
    // has four messages, so the run asks for nothing and the budget of zero
    // is not spent.
    let written = "{\n  \"period\": \"2026-October\",\n  \"articles\": [\n    \"084714\",\n    null,\n    null,\n    \"084717\"\n  ],\n  \"fixes\": {},\n  \"rejected\": {\n    \"084716\": \"msg-084799@example.org\"\n  }\n}\n";
    std::fs::write(out.join("2026-October.json"), written).unwrap();

    let run = rmail_parser(&[
        "sources",
        "--raw",
        raw.to_str().unwrap(),
        "--out",
        out.to_str().unwrap(),
        "--base-url",
        "https://lists.example.org/pipermail/r-devel/",
        "--max-requests",
        "0",
    ]);

    let stderr = String::from_utf8_lossy(&run.stderr);
    for line in [
        "Messages in periods looked at: 4\n",
        "Messages without a link:       2\n",
        "Rejected pairings on file:     1\n",
    ] {
        assert!(stderr.contains(line), "{line:?} missing from:\n{stderr}");
    }
    std::fs::remove_dir_all(&out).unwrap();
}
