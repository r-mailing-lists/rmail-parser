use rmail_parser::source_align::align;
use rmail_parser::source_index::{subject_key, IndexEntry};

fn entry(number: &str, subject: &str) -> IndexEntry {
    IndexEntry {
        number: number.to_string(),
        subject: subject.to_string(),
        author: "Someone".to_string(),
    }
}

fn keys(subjects: &[&str]) -> Vec<String> {
    subjects.iter().map(|s| subject_key(s)).collect()
}

fn pages(aligned: &[Option<String>]) -> Vec<Option<&str>> {
    aligned.iter().map(|a| a.as_deref()).collect()
}

#[test]
fn test_align_pairs_by_position_when_everything_agrees() {
    let raw = keys(&["[Rd] Topic A", "[Rd] Topic B", "Re: [Rd] Topic A"]);
    let index = [
        entry("000101", "[Rd] Topic A"),
        entry("000102", "[Rd] Topic B"),
        entry("000103", "[Rd] Topic A"),
    ];
    assert_eq!(
        pages(&align(&raw, &index)),
        [Some("000101"), Some("000102"), Some("000103")]
    );
}

#[test]
fn test_align_skips_a_message_the_index_does_not_have() {
    // The second raw message is junk the source never gave a page.
    let raw = keys(&[
        "[Rd] Topic A",
        "Cheap watches",
        "[Rd] Topic B",
        "[Rd] Topic C",
    ]);
    let index = [
        entry("000101", "[Rd] Topic A"),
        entry("000102", "[Rd] Topic B"),
        entry("000103", "[Rd] Topic C"),
    ];
    assert_eq!(
        pages(&align(&raw, &index)),
        [Some("000101"), None, Some("000102"), Some("000103")]
    );
}

#[test]
fn test_align_skips_an_index_entry_the_raw_file_does_not_have() {
    let raw = keys(&["[Rd] Topic A", "[Rd] Topic C"]);
    let index = [
        entry("000101", "[Rd] Topic A"),
        entry("000102", "[Rd] Topic B"),
        entry("000103", "[Rd] Topic C"),
    ];
    assert_eq!(
        pages(&align(&raw, &index)),
        [Some("000101"), Some("000103")]
    );
}

#[test]
fn test_align_falls_back_to_sequence_alignment_when_counts_match_but_subjects_do_not() {
    // Same length, but one message on each side has no partner on the other.
    let raw = keys(&["[Rd] Topic A", "Cheap watches", "[Rd] Topic C"]);
    let index = [
        entry("000101", "[Rd] Topic A"),
        entry("000102", "[Rd] Topic C"),
        entry("000103", "[Rd] Topic D"),
    ];
    assert_eq!(
        pages(&align(&raw, &index)),
        [Some("000101"), None, Some("000102")]
    );
}

#[test]
fn test_align_keeps_order_within_a_thread_of_identical_subjects() {
    // Nothing tells replies with one subject apart, so they pair in order and
    // the one left over is the last of the run.
    let raw = keys(&[
        "[Rd] Topic A",
        "Re: [Rd] Topic A",
        "Re: [Rd] Topic A",
        "[Rd] Topic B",
    ]);
    let index = [
        entry("000101", "[Rd] Topic A"),
        entry("000102", "[Rd] Topic A"),
        entry("000103", "[Rd] Topic B"),
    ];
    assert_eq!(
        pages(&align(&raw, &index)),
        [Some("000101"), Some("000102"), None, Some("000103")]
    );
}

#[test]
fn test_align_with_nothing_on_one_side() {
    let raw = keys(&["[Rd] Topic A", "[Rd] Topic B"]);
    assert_eq!(pages(&align(&raw, &[])), [None, None]);
    assert!(align(&[], &[entry("000101", "[Rd] Topic A")]).is_empty());
    assert!(align(&[], &[]).is_empty());
}

#[test]
fn test_align_handles_a_month_of_four_thousand_messages() {
    // The busiest months hold about 4,000 messages. One missing index entry
    // near the start forces the full table.
    let subjects: Vec<String> = (0..4000)
        .map(|i| format!("[R] question {}", i % 900))
        .collect();
    let raw: Vec<String> = subjects.iter().map(|s| subject_key(s)).collect();
    let mut index: Vec<IndexEntry> = subjects
        .iter()
        .enumerate()
        .map(|(i, s)| entry(&format!("{:06}", i + 1), s))
        .collect();
    index.remove(7);

    let aligned = align(&raw, &index);
    assert_eq!(aligned.len(), 4000);
    assert_eq!(aligned[6].as_deref(), Some("000007"));
    assert_eq!(aligned[7], None);
    assert_eq!(aligned[8].as_deref(), Some("000009"));
    assert_eq!(aligned[3999].as_deref(), Some("004000"));
    assert_eq!(aligned.iter().filter(|a| a.is_none()).count(), 1);
}
