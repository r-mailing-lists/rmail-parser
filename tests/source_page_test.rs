use std::path::Path;

use rmail_parser::source_page::{decode_entities, fix_string, parse_article};

fn fixture(rel: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(rel);
    std::fs::read_to_string(&path).unwrap()
}

#[test]
fn test_decode_entities() {
    assert_eq!(decode_entities("Bessel&#8217;s"), "Bessel\u{2019}s");
    assert_eq!(decode_entities("Universit&#228;t"), "Universit\u{e4}t");
    assert_eq!(
        decode_entities("Universit&#xE4;t &#Xe4;"),
        "Universit\u{e4}t \u{e4}"
    );
    assert_eq!(
        decode_entities("&lt;a&gt; &amp; &quot;b&quot; &apos;c&apos;&nbsp;d"),
        "<a> & \"b\" 'c'\u{a0}d"
    );
    // One pass only: an escaped entity comes out as the text of an entity.
    assert_eq!(decode_entities("&amp;lt;"), "&lt;");
    // A character outside the Basic Multilingual Plane is one character.
    assert_eq!(decode_entities("&#128512;"), "\u{1f600}");
}

#[test]
fn test_decode_entities_leaves_what_it_does_not_know() {
    assert_eq!(
        decode_entities("AT&T and R&D; x && y"),
        "AT&T and R&D; x && y"
    );
    assert_eq!(
        decode_entities("&copy; &#; &#x; &#12a; &"),
        "&copy; &#; &#x; &#12a; &"
    );
    assert_eq!(decode_entities("fish &#38 chips"), "fish &#38 chips");
    // Not characters: zero, half of a surrogate pair, past the end of Unicode.
    assert_eq!(
        decode_entities("&#0;&#55357;&#1114112;"),
        "\u{fffd}\u{fffd}\u{fffd}"
    );
}

#[test]
fn test_parse_article_takes_the_text_between_the_markers() {
    let page = parse_article(&fixture("pipermail/2026-October/084716.html")).unwrap();
    assert_eq!(
        page.text,
        "I\u{f1}aki and I saw this at the Universit\u{e4}t last week, and it\u{2019}s the same\n\
         there. Does that help?\n\
         \n\
         The notes are at https://example.org/history?id=1666 if you want them."
    );
    assert_eq!(page.message_id.as_deref(), Some("msg-084716@example.org"));
}

#[test]
fn test_parse_article_keeps_the_replacement_character_the_source_has() {
    let page = parse_article(&fixture("pipermail/2026-October/084715.html")).unwrap();
    assert!(page.text.contains("Bessel\u{fffd}s correction"));
    assert!(page.text.contains("method = \"ML\")."));
    assert!(page.text.contains(
        "discussion: https://stat.ethz.ch/pipermail/r-devel/2019-February/077300.html\n"
    ));
    assert_eq!(page.message_id.as_deref(), Some("msg-084715@example.org"));
}

#[test]
fn test_parse_article_from_2006_has_no_message_id() {
    let page = parse_article(&fixture("pipermail/2006-March/036687.html")).unwrap();
    assert!(page.text.starts_with("Version: 2.2.1\nOS: XP Pro\n"));
    assert!(page
        .text
        .contains("browser \"()\", made a couple of \"plot\"'s within"));
    assert_eq!(page.message_id, None);
}

#[test]
fn test_parse_article_without_markers_is_none() {
    assert_eq!(
        parse_article("<html><body>Service unavailable</body></html>"),
        None
    );
    assert_eq!(parse_article("<!--beginarticle--><PRE>cut off"), None);
    assert_eq!(parse_article(""), None);
}

#[test]
fn test_fix_string_restores_what_the_page_has() {
    let body = "I?aki and I saw this at the Universit?t last week, and it?s the same\n\
                there. Does that help?\n\
                \n\
                The notes are at https://example.org/history?id=1666 if you want them.";
    let page = parse_article(&fixture("pipermail/2026-October/084716.html")).unwrap();
    // One character per `?`: three restored, then a real question mark and
    // a query string, which stay as they are.
    assert_eq!(fix_string(body, &page.text), "\u{f1}\u{e4}\u{2019}??");
}

#[test]
fn test_fix_string_leaves_a_character_the_source_lost_too() {
    let body = "The current var() applies Bessel?s correction. There is a precedent in\n\
                cov.wt(..., method = \"ML\").\n\
                I found a related discussion: https://stat.ethz.ch/pipermail/r-devel/2019-February/077300.html\n\
                The argument name could follow R Core?s preferences.";
    let page = parse_article(&fixture("pipermail/2026-October/084715.html")).unwrap();
    assert_eq!(fix_string(body, &page.text), "??");
}

#[test]
fn test_fix_string_tolerates_the_two_renderings_differing() {
    // The text file and the page obscure the address differently, and the
    // page is indented. Position by position these texts do not line up.
    let body =
        "Write to j@d@m@on @end|ng |rom ex@mp|e@org about Bessel?s correction\nand the caf? menu.";
    let page = "  Write to jadamson at example.org about Bessel\u{2019}s correction\nand the caf\u{e9} menu.";
    assert_eq!(fix_string(body, page), "\u{2019}\u{e9}");
}

#[test]
fn test_fix_string_matches_a_repeated_phrase_in_order() {
    let body = "R Core?s view and then R Core?s view again";
    let page = "R Core\u{2019}s view and then R Core\u{fffd}s view again";
    assert_eq!(fix_string(body, page), "\u{2019}?");
}

#[test]
fn test_fix_string_treats_neighbouring_question_marks_as_wildcards() {
    let body = "na?ve caf?? na?ve";
    let page = "na\u{ef}ve caf\u{e9}? na\u{ef}ve";
    assert_eq!(fix_string(body, page), "\u{ef}\u{e9}?\u{ef}");
}

#[test]
fn test_fix_string_leaves_both_halves_of_an_astral_character() {
    // The text file has two `?` for one character beyond U+FFFF, so the
    // context never lines up with the page. Left alone in this version.
    let body = "Thanks ?? for the quick fix";
    let page = "Thanks \u{1f600} for the quick fix";
    assert_eq!(fix_string(body, page), "??");
}

#[test]
fn test_fix_string_does_not_guess_without_context() {
    // No letters to anchor on: the page's characters may be anything.
    assert_eq!(fix_string("?", "\u{e9}"), "?");
    assert_eq!(
        fix_string("??? ????", "\u{43f}\u{440}\u{438} \u{432}\u{435}\u{442}!"),
        "???????"
    );
    // Context that is not in the page at all.
    assert_eq!(
        fix_string("Bessel?s correction", "a different message"),
        "?"
    );
    assert_eq!(fix_string("Bessel?s correction", ""), "?");
    assert_eq!(fix_string("no marks", "anything"), "");
}
