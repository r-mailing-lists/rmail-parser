use rmail_parser::output::MonthArchive;
use rmail_parser::message::parse_message;
use rmail_parser::threading::reconstruct_threads;

#[test]
fn test_month_archive_serialization() {
    let raw = r#"From u@e.com  Mon Feb  1 20:13:59 2026
From: Alice <alice@example.com>
Date: Sun, 1 Feb 2026 20:13:59 -0500
Subject: Test
Message-ID: <t1@example.com>
Content-Type: text/plain

Hello world."#;

    let mut messages = vec![parse_message(raw).unwrap()];
    let threads = reconstruct_threads(&mut messages);

    let archive = MonthArchive {
        list: "r-help".to_string(),
        description: "The main R mailing list".to_string(),
        month: "2026-02".to_string(),
        messages,
        threads,
    };

    let json = serde_json::to_string_pretty(&archive).unwrap();
    assert!(json.contains("\"list\": \"r-help\""));
    assert!(json.contains("\"month\": \"2026-02\""));
    assert!(json.contains("Hello world"));

    // Verify round-trip
    let deserialized: MonthArchive = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.messages.len(), 1);
    assert_eq!(deserialized.threads.len(), 1);
}
