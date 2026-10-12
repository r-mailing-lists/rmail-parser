use std::path::Path;

#[test]
fn test_split_mbox_into_raw_messages() {
    let mbox_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/sample.mbox");
    let content = std::fs::read_to_string(&mbox_path).unwrap();
    let messages = rmail_parser::mbox::split_mbox(&content);
    assert_eq!(messages.len(), 3);
    assert!(messages[0].contains("weighted.residuals()"));
    assert!(messages[1].contains("Good catch"));
    assert!(messages[2].contains("ggplot2 facets"));
}

// The period files in `sources/` give each message its page by the message's
// position in the raw file, counted the way `split_mbox` counts. The three
// tests below hold that count still.

#[test]
fn test_a_body_line_starting_with_from_starts_a_new_message() {
    let content = "From ada at example.org  Mon Feb  2 12:00:00 2026\n\
                   Subject: One\n\
                   \n\
                   I heard it yesterday.\n\
                   From what I can tell, it is the same bug.\n\
                   \n\
                   From ben at example.org  Mon Feb  2 12:01:00 2026\n\
                   Subject: Two\n\
                   \n\
                   Thanks.\n";
    let messages = rmail_parser::mbox::split_mbox(content);
    assert_eq!(messages.len(), 3);
    assert_eq!(
        messages[0],
        "From ada at example.org  Mon Feb  2 12:00:00 2026\nSubject: One\n\nI heard it yesterday."
    );
    // No blank line before it, and no escaping: it still counts as a message.
    assert_eq!(messages[1], "From what I can tell, it is the same bug.");
    assert_eq!(
        messages[2],
        "From ben at example.org  Mon Feb  2 12:01:00 2026\nSubject: Two\n\nThanks."
    );
}

#[test]
fn test_lines_that_only_look_like_a_separator_do_not_split() {
    // A header, an escaped line, a word that begins with "From", and "From "
    // in the middle of a line.
    let content = "From ada at example.org  Mon Feb  2 12:00:00 2026\n\
                   From: ada at example.org (Ada)\n\
                   Subject: One\n\
                   \n\
                   >From what I can tell, it is the same bug.\n\
                   Fromage is cheese.\n\
                   It came From the list.\n";
    assert_eq!(rmail_parser::mbox::split_mbox(content).len(), 1);
}

#[test]
fn test_text_before_the_first_separator_is_not_a_message() {
    let content = "This file was exported on Monday.\n\
                   From ada at example.org  Mon Feb  2 12:00:00 2026\n\
                   Subject: One\n\
                   \n\
                   Body.\n";
    let messages = rmail_parser::mbox::split_mbox(content);
    assert_eq!(messages.len(), 1);
    assert!(messages[0].starts_with("From ada at example.org"));
}
