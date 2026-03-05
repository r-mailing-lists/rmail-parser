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
