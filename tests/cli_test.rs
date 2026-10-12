use std::path::Path;
use std::process::{Command, Output};

fn rmail_parser(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rmail-parser"))
        .args(args)
        .output()
        .unwrap()
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
