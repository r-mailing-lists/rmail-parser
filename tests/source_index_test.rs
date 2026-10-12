use std::path::Path;

use rmail_parser::source_index::{parse_index, subject_key, IndexEntry};

fn fixture(rel: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(rel);
    std::fs::read_to_string(&path).unwrap()
}

#[test]
fn test_parse_index_reads_every_entry_in_page_number_order() {
    let index = parse_index(&fixture("pipermail/2026-October/date.html"));
    // The page lists 084717 before 084716; the result is in number order.
    let numbers: Vec<&str> = index.iter().map(|e| e.number.as_str()).collect();
    assert_eq!(numbers, ["084714", "084715", "084716", "084717"]);
    assert_eq!(
        index[0],
        IndexEntry {
            number: "084714".to_string(),
            subject:
                "[Rd] Treat multiline expressions as a single block in R Console history #1666"
                    .to_string(),
            author: "Ada Example".to_string(),
        }
    );
    assert_eq!(index[2].author, "Cat Example");
    assert_eq!(
        index[3].subject,
        "[Rd]  [PATCH] Include Linux distribution identifiers in the default HTTP User-Agent"
    );
}

#[test]
fn test_parse_index_decodes_entities_and_sorts_by_number_not_text() {
    let html = "<ul>\n\
        <LI><A HREF=\"1000000.html\">[Rd] gsub(replacement=&quot;\\\\X&quot;) -&gt; &quot;X&quot;\n\
        </A><A NAME=\"1000000\">&nbsp;</A>\n\
        <I>Jos&#233; Example\n\
        </I>\n\
        \n\
        <LI><A HREF=\"999999.html\">[Rd] (no subject)\n\
        </A><A NAME=\"999999\">&nbsp;</A>\n\
        <I>eve at example.org\n\
        </I>\n\
        </ul>\n";
    let index = parse_index(html);
    assert_eq!(index.len(), 2);
    // As text, "1000000" sorts before "999999".
    assert_eq!(index[0].number, "999999");
    assert_eq!(index[1].number, "1000000");
    assert_eq!(
        index[1].subject,
        "[Rd] gsub(replacement=\"\\\\X\") -> \"X\""
    );
    assert_eq!(index[1].author, "Jos\u{e9} Example");
}

#[test]
fn test_parse_index_of_a_page_that_is_not_an_index_is_empty() {
    assert!(parse_index("<html><body>Service unavailable</body></html>").is_empty());
    assert!(parse_index("").is_empty());
    // A page cut off in the middle of an entry gives the entries before it.
    let cut = "<LI><A HREF=\"000001.html\">[Rd] one\n</A><A NAME=\"1\">&nbsp;</A>\n<I>Ada\n</I>\n\
               <LI><A HREF=\"000002.html\">[Rd] tw";
    assert_eq!(parse_index(cut).len(), 1);
}

#[test]
fn test_subject_key_ignores_what_the_two_renderings_disagree_on() {
    // The page has the character, the text file has `?`.
    assert_eq!(
        subject_key("[Rd] Universit&#228;t"),
        subject_key("Re: [Rd] Universit?t")
    );
    assert_eq!(subject_key("[Rd] Universit&#228;t"), "universitt");
    // The text file folds a long subject; the page joins it with two spaces.
    assert_eq!(
        subject_key("[Rd]  [PATCH] Include Linux distribution identifiers"),
        subject_key("[Rd] [PATCH] Include Linux\tdistribution identifiers")
    );
    // Escaping.
    assert_eq!(
        subject_key("[R] a &lt; b &amp;&amp; c"),
        subject_key("[R] a < b && c")
    );
}

#[test]
fn test_subject_key_strips_prefixes_and_tags_repeatedly() {
    assert_eq!(
        subject_key("Re: [R] RE: Fwd: [R-sig-Geo] FW: Topic A"),
        "topica"
    );
    assert_eq!(subject_key("  [BioC] topic a"), "topica");
    // Only at the front: a bracket later on is part of the subject.
    assert_eq!(subject_key("[R] x[1] and Re: y"), "x1andrey");
    assert_eq!(subject_key("[Rd] (no subject)"), "nosubject");
    assert_eq!(subject_key("[unclosed tag"), "unclosedtag");
    assert_eq!(subject_key(""), "");
    assert_eq!(subject_key("[Rd]"), "");
}

#[test]
fn test_subject_key_tells_different_subjects_apart() {
    assert_ne!(
        subject_key("[Rd] R-Forge status"),
        subject_key("[Rd] R-Forge statistics")
    );
}
