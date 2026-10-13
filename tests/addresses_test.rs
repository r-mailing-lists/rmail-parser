//! What `parse --hide-addresses` does to a text, a subject and a display
//! name. Each table row is an input and what it must become.

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use rmail_parser::addresses::{hide_in_message, hide_in_name, hide_in_text};
use rmail_parser::message::parse_message;

/// The list the tables are checked for.
const LIST: &str = "r-sig-phylo";

/// What an address becomes.
const HIDDEN: &str = "[address hidden]";

/// One address in the three forms these archives hold: Mailman's text
/// form, the source's lossy form, and the plain one.
const FORMS: [&str; 3] = [
    "jane at uni.edu",
    "j@ne @end|ng |rom un|@edu",
    "jane@uni.edu",
];

/// Checks every row and reports all that are wrong, not only the first.
fn check_text(rows: &[(&str, &str)]) {
    let wrong: Vec<String> = rows
        .iter()
        .filter_map(|(text, want)| {
            let got = hide_in_text(text, LIST);
            (got != *want).then(|| format!("{text:?}\n   gave {got:?}\n   want {want:?}"))
        })
        .collect();
    assert!(
        wrong.is_empty(),
        "{} of {} rows are wrong:\n{}",
        wrong.len(),
        rows.len(),
        wrong.join("\n")
    );
}

/// Checks each template with `ADDR` written in each of the three forms:
/// the address, and nothing else, must become the marker.
fn check_each_form(templates: &[&str]) {
    let rows: Vec<(String, String)> = templates
        .iter()
        .flat_map(|template| {
            FORMS.iter().map(move |form| {
                (
                    template.replace("ADDR", form),
                    template.replace("ADDR", HIDDEN),
                )
            })
        })
        .collect();
    let rows: Vec<(&str, &str)> = rows
        .iter()
        .map(|(text, want)| (text.as_str(), want.as_str()))
        .collect();
    check_text(&rows);
}

#[test]
fn an_address_in_any_of_the_three_forms_becomes_the_marker() {
    check_each_form(&[
        // Alone.
        "ADDR",
        // Inside angle brackets and inside parentheses.
        "Jane Doe <ADDR> wrote:",
        "write to me (ADDR) soon",
        // At the start and at the end of a line.
        "first line\nADDR is hers\nlast line",
        "first line\nhers is ADDR\nlast line",
        // Two on one line.
        "from ADDR to ADDR",
        "ADDR ADDR",
        // A header line quoted in a reply.
        "From: ADDR (Jane Doe)",
        "> From: ADDR (Jane Doe)",
    ]);
}

#[test]
fn punctuation_after_an_address_is_not_part_of_it() {
    check_each_form(&[
        "to ADDR. Then",
        "to ADDR, then",
        "to ADDR; then",
        "to ADDR: then",
        "to ADDR> then",
        "to ADDR) then",
        "ADDR.",
        "ADDR,",
        "ADDR;",
        "ADDR:",
        "ADDR>",
        "ADDR)",
        "ADDR).",
        "ADDR>,",
    ]);
}

#[test]
fn text_around_an_address_keeps_every_character_it_had() {
    check_each_form(&[
        // Letters beyond ASCII before and after, with blanks between.
        "Gr\u{fc}\u{df}e an ADDR f\u{fc}r \u{65e5}\u{672c}\u{8a9e}",
        // Quotation marks beyond ASCII directly on both sides.
        "\u{e9}t\u{e9} \u{2018}ADDR\u{2019} \u{fc}ber",
        "\u{300c}ADDR\u{300d}",
    ]);
}

/// Checks that each text comes back exactly as it went in.
fn check_unchanged(texts: &[&str]) {
    let rows: Vec<(&str, &str)> = texts.iter().map(|text| (*text, *text)).collect();
    check_text(&rows);
}

#[test]
fn text_without_an_address_comes_back_unchanged() {
    check_unchanged(&[
        "",
        "Dear all,\n\nthanks for the help.",
        // `at` in a sentence, with no domain after it.
        "The tree is rooted at the base, and we met at noon.",
        // What Mailman's text form makes of `tree@edge` in R code.
        "str(tree at edge)",
        "tree@edge",
        "obj@data$x",
        // A number is not a domain, and neither is a time or a frequency.
        "It was significant at 0.05 once, and at 1.5.2 it was fixed.",
        "See you at 5.30pm.",
        "Timed on one core at 2.4GHz.",
        // A file name that starts with a full stop.
        "The option is placed at .Rprofile for now.",
        // Separators with nothing to separate.
        "@",
        "a @ b",
        "@param x The tree.",
        " at ",
        "ends with at",
        "@end|ng |rom",
        " @end|ng |rom ",
        // Text beyond ASCII, with `at` in it.
        "Universit\u{e4}t Z\u{fc}rich \u{2014} \u{65e5}\u{672c}\u{8a9e} at \u{5927}\u{5b66}",
        // The marker itself.
        "[address hidden]",
    ]);
}

#[test]
fn a_mailing_list_s_own_address_stays() {
    check_unchanged(&[
        "r-sig-phylo at r-project.org",
        "R-sig-phylo-request at r-project.org",
        "r-help at stat.math.ethz.ch",
        "bioc-devel at r-project.org",
        // The other two domains lists live at.
        "r-devel at lists.r-project.org",
        "r-forge-list at lists.r-forge.r-project.org",
        // Without regard to case.
        "R-SIG-Phylo at R-project.org",
        "Bioc-devel at R-Project.ORG",
        // In a sentence, in a link, and in the footer of a message.
        "Write to r-help at r-project.org.",
        "<mailto:r-help at r-project.org>",
        "R-sig-phylo mailing list - R-sig-phylo at r-project.org\n\
         https://stat.ethz.ch/mailman/listinfo/r-sig-phylo\n\
         Searchable archive at http://www.mail-archive.com/r-sig-phylo at r-project.org/",
        // The plain form.
        "r-sig-phylo@r-project.org",
        "<R-help@stat.math.ethz.ch>",
        // The lossy form, where `@` is an a, an s or a full stop that was
        // lost, and `|` an i, an l or an f.
        "R-@|g-phy|o m@|||ng ||@t - R-@|g-phy|o @end|ng |rom r-project@org",
        "r-he|p @end|ng |rom @t@t@m@th@ethz@ch",
        "b|oc-deve| @end|ng |rom r-project@org",
        "r-deve| @end|ng |rom ||@t@@r-project@org",
    ]);
}

#[test]
fn an_address_named_after_the_list_being_parsed_stays() {
    let text = "ask StatList-owner at r-project.org first";
    assert_eq!(hide_in_text(text, "statlist"), text);
    // It is not this list's address in a run for another list.
    assert_eq!(hide_in_text(text, LIST), "ask [address hidden] first");
    // A list without a name vouches for no address.
    assert_eq!(
        hide_in_text("ask jane at r-project.org first", ""),
        "ask [address hidden] first"
    );
}

#[test]
fn an_address_that_only_resembles_a_list_s_is_hidden() {
    check_text(&[
        ("Jane.Doe at r-project.org", HIDDEN),
        ("r-help at example.org", HIDDEN),
        // A list's domain with something before or after it, and the
        // host of the archive, where no list has its address.
        ("r-help at mail.r-project.org", HIDDEN),
        ("r-help at r-project.org.example.com", HIDDEN),
        ("r-help at stat.ethz.ch", HIDDEN),
        // The start of a list's name in the middle of a mailbox name.
        ("for-r-help at r-project.org", HIDDEN),
        ("abioc at r-project.org", HIDDEN),
        // The plain form.
        ("Jane.Doe@r-project.org", HIDDEN),
        ("r-help@example.org", HIDDEN),
        // The lossy form. A lost character is not any character: the two
        // lost at the start of `@|mon` cannot have been `r-`.
        ("j@ne@doe @end|ng |rom r-project@org", HIDDEN),
        ("@|mon@roe @end|ng |rom r-project@org", HIDDEN),
        ("r-he|p @end|ng |rom ex@mp|e@org", HIDDEN),
        ("r-he|p @end|ng |rom r-project|org", HIDDEN),
        ("b@oc-deve| @end|ng |rom r-project@org", HIDDEN),
    ]);
}

#[test]
fn a_web_address_written_in_a_sentence_stays() {
    check_unchanged(&[
        // The part after ` at ` starts with `www.`.
        "Read more at www.example.org",
        "Read more at www.example.org.",
        "The notes are available at www.example.org",
        // Or it is directly followed by a slash.
        "the code at github.com/user/repo",
        "a mirror at cran.example.org/web/packages/ape, I think",
        "the archive at example.org/",
    ]);
}

#[test]
fn an_address_that_only_resembles_a_web_address_is_hidden() {
    check_text(&[
        ("info at example.org", HIDDEN),
        (
            "Write to info at example.org.",
            "Write to [address hidden].",
        ),
        // `www` further on, or without its full stop.
        ("info at mail.www.example.org", HIDDEN),
        ("info at wwwexample.org", HIDDEN),
        // A slash that does not follow directly.
        ("info at example.org /pub", "[address hidden] /pub"),
        ("info at example.org, or /pub", "[address hidden], or /pub"),
        // The other two forms are never a web address in a sentence.
        ("webmaster@www.example.org", HIDDEN),
        ("git@github.com/user/repo", "[address hidden]/user/repo"),
        ("webm@@ter @end|ng |rom www@ex@mp|e@org", HIDDEN),
    ]);
}

#[test]
fn an_ordinary_word_before_at_is_not_a_mailbox_name() {
    // Each with a bare domain after it, which is no web address by itself.
    let texts: Vec<String> = [
        "look",
        "looking",
        "available",
        "found",
        "here",
        "located",
        "hosted",
        "are",
        "was",
        "and",
        "up",
        "now",
        "left",
        "indexed",
        "see",
    ]
    .iter()
    .map(|word| format!("which {word} at cran.example.org today"))
    .collect();
    let texts: Vec<&str> = texts.iter().map(String::as_str).collect();
    check_unchanged(&texts);
}

#[test]
fn an_ordinary_word_is_one_in_any_case_and_after_punctuation() {
    check_unchanged(&[
        "Look at example.org",
        "It WAS at example.org",
        "(see at example.org)",
        "the mirror, now at example.org.",
    ]);
}

#[test]
fn a_word_that_is_not_on_the_list_of_ordinary_words_is_a_mailbox_name() {
    check_text(&[
        ("me at example.org", HIDDEN),
        ("write me at example.org", "write [address hidden]"),
        // Two short words that a department may well have as its mailbox.
        ("is at example.org", HIDDEN),
        (
            "My address is at example.org",
            "My address [address hidden]",
        ),
        ("it at example.org", HIDDEN),
        ("IT at uni.edu", HIDDEN),
        // A longer word that starts or ends with an ordinary one.
        ("looks at example.org", HIDDEN),
        ("this at example.org", HIDDEN),
        ("is.it at example.org", HIDDEN),
        // Only Mailman's text form can be a sentence.
        ("see@example.org", HIDDEN),
        ("see @end|ng |rom ex@mp|e@org", HIDDEN),
    ]);
}

#[test]
fn a_mailbox_name_is_hidden_in_full_whatever_characters_it_has() {
    check_text(&[
        ("o'brien at uni.edu", HIDDEN),
        ("jane_doe-roe+lists at uni.edu", HIDDEN),
        ("jane=doe%relay at uni.edu", HIDDEN),
        ("jane&joe~x!y#1$z*a^b`c{d}e|f at uni.edu", HIDDEN),
        ("o'brien@uni.edu", HIDDEN),
        ("jane=doe%relay@uni.edu", HIDDEN),
        // Letters beyond ASCII, and the `?` the source leaves for them.
        ("j\u{f6}rg.m\u{fc}ller at uni.de", HIDDEN),
        ("j?rg.m?ller at uni.de", HIDDEN),
        ("j\u{f6}rg.m\u{fc}ller@uni.de", HIDDEN),
        ("j?rg.m?||er @end|ng |rom un|@de", HIDDEN),
        // Such a `?` may be the first character of the name.
        ("(?zg?r at uni.edu)", "([address hidden])"),
        ("<?zg?r@uni.edu>", "<[address hidden]>"),
    ]);
}

#[test]
fn punctuation_before_a_mailbox_name_is_not_part_of_it() {
    check_text(&[
        ("'jane at uni.edu'", "'[address hidden]'"),
        ("*jane@uni.edu*", "*[address hidden]*"),
        ("`jane at uni.edu`", "`[address hidden]`"),
        ("--jane at uni.edu", "--[address hidden]"),
        ("...jane at uni.edu", "...[address hidden]"),
        ("mailto:jane at uni.edu", "mailto:[address hidden]"),
        ("a,jane at uni.edu", "a,[address hidden]"),
        ("'j@ne @end|ng |rom un|@edu'", "'[address hidden]'"),
        // In the lossy form a name may start with a lost character.
        ("<@|mon@roe @end|ng |rom ex@mp|e@org>", "<[address hidden]>"),
    ]);
    // And it does not stand between a word and what lets the word stay.
    check_unchanged(&[
        "'r-help at r-project.org'",
        "*available at example.org*",
        "...see at example.org",
    ]);
}

#[test]
fn a_domain_is_hidden_in_full_whatever_characters_it_has() {
    check_text(&[
        ("jane at my_uni.edu", HIDDEN),
        ("jane at dept+lab.uni.edu", HIDDEN),
        ("jane@my_uni.edu", HIDDEN),
        // Letters beyond ASCII, and the `?` the source leaves for them.
        ("jane at universit\u{e4}t.de", HIDDEN),
        ("jane at universit?t.de", HIDDEN),
        ("jane@universit?t.de", HIDDEN),
        ("j@ne @end|ng |rom un|ver@|t?t@de", HIDDEN),
        // Such a `?` may be the first character of the domain.
        ("jane at ?bung.de", HIDDEN),
        ("jane@?bung.de", HIDDEN),
        // A question mark that ends the sentence is not part of it.
        ("Is it jane at uni.edu?", "Is it [address hidden]?"),
        ("Is it jane@uni.edu?", "Is it [address hidden]?"),
        (
            "Is it j@ne @end|ng |rom un|@edu?",
            "Is it [address hidden]?",
        ),
    ]);
}

#[test]
fn a_domain_after_a_sign_is_hidden_even_without_a_mailbox_name() {
    check_text(&[
        // A name in quotation marks is not read as a name, so the domain
        // must go: left in place it would complete the address.
        ("\"jane doe\"@uni.edu", "\"jane doe\"[address hidden]"),
        ("@uni.edu", HIDDEN),
        ("everyone @uni.edu knows", "everyone [address hidden] knows"),
        (
            "\"jane doe\" @end|ng |rom un|@edu",
            "\"jane doe\" [address hidden]",
        ),
        ("< @end|ng |rom un|@edu>", "< [address hidden]>"),
    ]);
}

#[test]
fn the_lossy_separator_marks_an_address_however_it_is_spaced() {
    check_text(&[
        // No blank before it, or a line break there.
        ("j@ne@end|ng |rom un|@edu", HIDDEN),
        ("<@end|ng |rom un|@edu>", "<[address hidden]>"),
        ("j@ne\n@end|ng |rom un|@edu", "j@ne\n[address hidden]"),
        // No domain after it on the same line.
        ("j@ne @end|ng |rom\nun|@edu", "[address hidden]\nun|@edu"),
        ("j@ne @end|ng |rom", HIDDEN),
        ("j@ne @end|ng |rom <un|@edu>", "[address hidden] <un|@edu>"),
    ]);
}

#[test]
fn an_address_after_one_that_stays_is_still_hidden() {
    check_text(&[
        // `jane.doe` reads as the domain of `available at jane.doe`.
        (
            "available at jane.doe at uni.edu",
            "available at [address hidden]",
        ),
        (
            "r-help at r-project.org, jane at uni.edu",
            "r-help at r-project.org, [address hidden]",
        ),
        (
            "more at www.example.org or from jane at uni.edu",
            "more at www.example.org or from [address hidden]",
        ),
        (
            "r-he|p @end|ng |rom r-project@org j@ne @end|ng |rom un|@edu",
            "r-he|p @end|ng |rom r-project@org [address hidden]",
        ),
    ]);
}

#[test]
fn what_may_be_an_address_is_hidden() {
    check_text(&[
        // What Mailman's text form makes of `tree@edge.length` in R code.
        ("str(tree at edge.length)", "str([address hidden])"),
        // A domain in a sentence, after a word that is not on the list.
        ("download at cran.example.org", HIDDEN),
        // The plain form is never a web address.
        (
            "git clone git@github.com:user/repo.git",
            "git clone [address hidden]:user/repo.git",
        ),
    ]);
}

/// One megabyte.
const MEGABYTE: usize = 1 << 20;

/// Cleans `text`, and fails if that has not ended after a second. A scan
/// that is linear in the length of the text takes a fraction of a second
/// for a megabyte, and one that goes back over the line at every separator
/// takes minutes. The work is done on a thread of its own, so that such a
/// scan fails the test after that second and does not hold up the run.
fn hide_in_time(text: &str) -> String {
    let (done, result) = mpsc::channel();
    let owned = text.to_string();
    thread::spawn(move || done.send(hide_in_text(&owned, LIST)));
    result
        .recv_timeout(Duration::from_secs(1))
        .unwrap_or_else(|_| panic!("{} bytes took more than a second", text.len()))
}

#[test]
fn a_megabyte_without_a_blank_or_a_sign_is_cleaned_in_well_under_a_second() {
    let line = "a".repeat(MEGABYTE);
    assert!(hide_in_time(&line) == line);
    let line = "ab.".repeat(MEGABYTE / 3);
    assert!(hide_in_time(&line) == line);
    let line = "\u{e9}".repeat(MEGABYTE / 2);
    assert!(hide_in_time(&line) == line);
}

#[test]
fn a_megabyte_without_a_blank_but_with_signs_is_cleaned_in_well_under_a_second() {
    // Signs with nothing that could be a domain after them.
    for unit in [
        "@", "a@", "|@", "a.@", "@end|ng", "m", "m@", "m@iii@g", "%", "%40", "a%40", "%7Ca%40",
        "(", "(at)", "a(at)", "-", "--at--", "[dot]",
    ] {
        let line = unit.repeat(MEGABYTE / unit.len());
        assert!(hide_in_time(&line) == line, "{unit:?} repeated");
    }

    // One long name and one long domain around a single sign.
    let half = "ab.".repeat(MEGABYTE / 6);
    assert_eq!(
        hide_in_time(&format!("<{half}c@{half}c>")),
        "<[address hidden]>"
    );

    // An address at every sign: the first takes the name before it, each
    // later one finds its name already hidden, and the last has no domain.
    let count = MEGABYTE / 4;
    let line = "a.b@".repeat(count);
    let want = HIDDEN.repeat(count - 1) + "@";
    assert!(hide_in_time(&line) == want);

    // The lossy separator over and over, and after one long name.
    let line = "@end|ng |rom".repeat(MEGABYTE / 12).replace(' ', "_");
    assert!(hide_in_time(&line) == line);
    let line = "j@ne|".repeat(MEGABYTE / 5) + "@end|ng@|rom";
    assert!(hide_in_time(&line) == line);
}

#[test]
fn a_megabyte_of_separators_with_blanks_is_cleaned_in_well_under_a_second() {
    // Separators with no domain after them, and line ends with nothing
    // that is broken over them.
    for unit in [
        "x at ",
        "x AT ",
        "x (at) ",
        "x at a dot 5 ",
        "x at\n",
        "x at \r\n> ",
        "x\nat ",
        "\n",
        "\n> ",
        "> ",
        " ",
    ] {
        let text = unit.repeat(MEGABYTE / unit.len());
        assert!(hide_in_time(&text) == text, "{unit:?} repeated");
    }

    // An address in each form, a list's address and a web address, again
    // and again on one line.
    let unit = "jane at uni.edu r-help at r-project.org j@ne @end|ng |rom un|@edu \
                more at www.example.org jane@uni.edu ";
    let cleaned = "[address hidden] r-help at r-project.org [address hidden] \
                   more at www.example.org [address hidden] ";
    let count = MEGABYTE / unit.len();
    assert!(hide_in_time(&unit.repeat(count)) == cleaned.repeat(count));

    // One long name before each kind of separator, and one long domain
    // after it.
    let long = "ab.".repeat(MEGABYTE / 6);
    for separator in [" at ", " @end|ng |rom ", "@"] {
        assert_eq!(
            hide_in_time(&format!("<{long}c{separator}{long}c>")),
            "<[address hidden]>",
            "around {separator:?}"
        );
    }
}

/// Checks every display name and reports all that are wrong.
fn check_names(rows: &[(&str, &str)]) {
    let wrong: Vec<String> = rows
        .iter()
        .filter_map(|(name, want)| {
            let got = hide_in_name(name, LIST);
            (got != *want).then(|| format!("{name:?}\n   gave {got:?}\n   want {want:?}"))
        })
        .collect();
    assert!(
        wrong.is_empty(),
        "{} of {} rows are wrong:\n{}",
        wrong.len(),
        rows.len(),
        wrong.join("\n")
    );
}

#[test]
fn a_display_name_that_is_an_address_becomes_its_mailbox_name() {
    check_names(&[
        ("jane at uni.edu", "jane"),
        ("jane@uni.edu", "jane"),
        ("Jane.Doe_2 at uni.edu", "Jane.Doe_2"),
        // The lossy form gives the name without the characters it lost.
        ("j@ne|doe @end|ng |rom un|@edu", "jnedoe"),
        // It is an address as a whole with blanks, quotation marks or
        // brackets around it, as a `From:` line may leave them.
        (" jane at uni.edu ", "jane"),
        ("\"jane at uni.edu\"", "jane"),
        ("'jane@uni.edu'", "jane"),
        ("<jane at uni.edu>", "jane"),
        ("jane at uni.edu ()", "jane"),
        ("\"j@ne @end|ng |rom un|@edu\"", "jne"),
        // What lets an address stay in a text does not make it a name.
        ("r-sig-phylo at r-project.org", "r-sig-phylo"),
        ("r-he|p @end|ng |rom r-project@org", "r-hep"),
        ("info at www.example.org", "info"),
        ("is at example.org", "is"),
    ]);
}

#[test]
fn a_display_name_without_an_address_stays() {
    check_names(&[
        ("Jane Doe", "Jane Doe"),
        ("\"Doe, Jane\"", "\"Doe, Jane\""),
        ("J\u{f6}rg M\u{fc}ller", "J\u{f6}rg M\u{fc}ller"),
        ("Jane Doe at Uni", "Jane Doe at Uni"),
        ("jane", "jane"),
        ("unknown", "unknown"),
        ("", ""),
        (" ", " "),
    ]);
}

#[test]
fn a_display_name_with_an_address_among_other_words_gets_the_marker() {
    check_names(&[
        ("Jane Doe jane at uni.edu", "Jane Doe [address hidden]"),
        ("Jane Doe <jane@uni.edu>", "Jane Doe <[address hidden]>"),
        (
            "jane at uni.edu via R-sig-phylo",
            "[address hidden] via R-sig-phylo",
        ),
        (
            "J@ne Doe j@ne @end|ng |rom un|@edu",
            "J@ne Doe [address hidden]",
        ),
        // Among other words a list's address stays, as it does in a text.
        (
            "R-sig-phylo list r-sig-phylo at r-project.org",
            "R-sig-phylo list r-sig-phylo at r-project.org",
        ),
    ]);
}

#[test]
fn a_display_name_with_nothing_left_to_show_becomes_unknown() {
    check_names(&[
        // An address with an empty mailbox name: none at all, or one the
        // lossy form lost every character of.
        ("@uni.edu", "unknown"),
        ("@end|ng |rom ex@mp|e@org", "unknown"),
        ("@| @end|ng |rom ex@mp|e@org", "unknown"),
        // Nothing but the marker and blanks.
        ("jane at uni.edu jane@uni.edu", "unknown"),
        (" jane at uni.edu  j@ne @end|ng |rom un|@edu ", "unknown"),
    ]);
}

/// A message whose sender has no name beside the address, with an address
/// in its subject and one at the start of a long text.
fn message_with_addresses() -> String {
    format!(
        "From jane.roe at uni-example.edu  Mon Mar  2 10:15:00 2026\n\
         From: jane.roe at uni-example.edu\n\
         Date: Mon, 2 Mar 2026 10:15:00 +0000\n\
         Subject: Re: [R-sig-phylo] Mail from mark.poe at inst-example.org\n\
         In-Reply-To: <a1@mail.inst-example.org>\n\
         References: <a0@mail.inst-example.org> <a1@mail.inst-example.org>\n\
         Message-ID: <b1@mail.uni-example.edu>\n\
         \n\
         Write to mark.poe at inst-example.org today. {}\n",
        "x".repeat(300)
    )
}

#[test]
fn hiding_in_a_message_cleans_its_name_subject_and_text() {
    let mut msg = parse_message(&message_with_addresses()).unwrap();
    assert_eq!(msg.from_name, "jane.roe at uni-example.edu");

    hide_in_message(&mut msg, LIST);

    assert_eq!(msg.from_name, "jane.roe");
    assert_eq!(msg.subject, "Re: [R-sig-phylo] Mail from [address hidden]");
    assert_eq!(msg.subject_clean, "Mail from [address hidden]");
    assert_eq!(
        msg.body_plain,
        format!("Write to [address hidden] today. {}", "x".repeat(300))
    );
}

#[test]
fn hiding_in_a_message_keeps_the_cleaned_subject_the_subject_without_its_prefix() {
    for (subject, want, want_clean) in [
        (
            "Re: [R-sig-phylo] jane at uni.edu wrote",
            "Re: [R-sig-phylo] [address hidden] wrote",
            "[address hidden] wrote",
        ),
        // An address directly after the prefix, with no blank between.
        (
            "Re:jane at uni.edu",
            "Re:[address hidden]",
            "[address hidden]",
        ),
        ("[R]@uni.edu", "[R][address hidden]", "[address hidden]"),
        (
            "Fwd: [R] j@ne @end|ng |rom un|@edu",
            "Fwd: [R] [address hidden]",
            "[address hidden]",
        ),
        // What stays in the one stays in the other.
        (
            "Re: [R-sig-phylo] r-sig-phylo at r-project.org is down",
            "Re: [R-sig-phylo] r-sig-phylo at r-project.org is down",
            "r-sig-phylo at r-project.org is down",
        ),
        (
            "Fwd: [R] nothing to hide",
            "Fwd: [R] nothing to hide",
            "nothing to hide",
        ),
    ] {
        let raw = message_with_addresses().replace(
            "Re: [R-sig-phylo] Mail from mark.poe at inst-example.org",
            subject,
        );
        let mut msg = parse_message(&raw).unwrap();
        assert_eq!(msg.subject, subject);

        hide_in_message(&mut msg, LIST);

        assert_eq!(msg.subject, want);
        assert_eq!(msg.subject_clean, want_clean, "for {subject:?}");
    }
}

#[test]
fn hiding_in_a_message_cuts_the_snippet_from_the_cleaned_text() {
    let mut msg = parse_message(&message_with_addresses()).unwrap();

    hide_in_message(&mut msg, LIST);

    // 200 characters: the 33 up to the first `x`, and 167 of them.
    assert_eq!(
        msg.body_snippet,
        format!("Write to [address hidden] today. {}", "x".repeat(167))
    );
}

#[test]
fn hiding_in_a_message_keeps_what_identifies_it() {
    let mut plain = parse_message(&message_with_addresses()).unwrap();
    plain.thread_id = "thread-0123456789ab".to_string();
    plain.thread_depth = 2;
    plain.source_url =
        Some("https://lists.example.org/pipermail/r-sig-phylo/2026-March/000101.html".to_string());
    let mut hidden = plain.clone();

    hide_in_message(&mut hidden, LIST);

    // The hash is that of the real address: it is what joins a person's
    // messages across lists.
    assert_eq!(hidden.from_email_hash, plain.from_email_hash);
    assert_eq!(hidden.id, plain.id);
    assert_eq!(hidden.message_id, "<b1@mail.uni-example.edu>");
    assert_eq!(
        hidden.in_reply_to.as_deref(),
        Some("<a1@mail.inst-example.org>")
    );
    assert_eq!(
        hidden.references,
        ["<a0@mail.inst-example.org>", "<a1@mail.inst-example.org>"]
    );
    assert_eq!(hidden.thread_id, plain.thread_id);
    assert_eq!(hidden.thread_depth, plain.thread_depth);
    assert_eq!(hidden.date, plain.date);
    assert_eq!(hidden.month, plain.month);
    assert_eq!(hidden.source_url, plain.source_url);
    // And it did do its work.
    assert_ne!(hidden.body_plain, plain.body_plain);
}

/// The same numbers on every run, so that a failure can be run again.
struct Numbers(u64);

impl Numbers {
    fn below(&mut self, limit: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % limit as u64) as usize
    }
}

#[test]
fn any_text_is_cleaned_on_character_boundaries_and_gains_nothing_but_markers() {
    // What makes and what breaks an address, with characters of two, three
    // and four bytes among it. A cut inside one of those is a panic.
    const PIECES: [&str; 47] = [
        " at ",
        " at",
        "at ",
        " AT ",
        "(at)",
        "--at--",
        " dot ",
        "[dot]",
        "%40",
        "%7C",
        "m@iii@g oii",
        "> ",
        "@",
        "@end|ng |rom",
        " @end|ng |rom ",
        " ",
        "\n",
        ".",
        "..",
        "-",
        "_",
        "?",
        "|",
        "'",
        "<",
        ">",
        "(",
        ")",
        "/",
        ":",
        ",",
        "jane",
        "uni",
        ".edu",
        "www.",
        "r-",
        "bioc",
        "r-project.org",
        "stat.math.ethz.ch",
        "r-sig-phylo",
        "is",
        "5",
        "\u{e9}",
        "\u{a0}",
        "\u{65e5}",
        "\u{2019}",
        "\u{1f600}",
    ];
    let mut numbers = Numbers(0x9E37_79B9_7F4A_7C15);
    for _ in 0..20_000 {
        let text: String = (0..1 + numbers.below(12))
            .map(|_| PIECES[numbers.below(PIECES.len())])
            .collect();

        let cleaned = hide_in_text(&text, LIST);

        // What is left beside the markers is the text's own characters, in
        // their order. They need not be neighbours in the text: of an
        // address broken over two lines the line end stays, and the domain
        // after it goes.
        let mut rest = text.chars();
        for kept in cleaned.split(HIDDEN).flat_map(str::chars) {
            assert!(
                rest.any(|c| c == kept),
                "{text:?} became {cleaned:?}, which has a {kept:?} of its own"
            );
        }
        // A name is cleaned by other code, on the same boundaries.
        hide_in_name(&text, LIST);
    }
}

/// The four separators the source writes for the `@` of an address.
const LOSSY_SEPARATORS: [&str; 4] = ["@end|ng |rom", "@ending from", "m@iii@g oii", "m@ili@g off"];

#[test]
fn each_of_the_source_s_four_lossy_separators_marks_an_address() {
    let rows: Vec<(String, String)> = LOSSY_SEPARATORS
        .iter()
        .flat_map(|separator| {
            [
                "ADDR",
                "Jane Doe <ADDR> wrote:",
                "write to me (ADDR) soon",
                "first line\nADDR is hers\nlast line",
                "from ADDR to ADDR",
                "to ADDR. Then",
                "From: ADDR (Jane Doe)",
            ]
            .map(|template| {
                (
                    template.replace("ADDR", &format!("j@ne.doe {separator} un|-ex@mple.edu")),
                    template.replace("ADDR", HIDDEN),
                )
            })
        })
        .collect();
    let rows: Vec<(&str, &str)> = rows
        .iter()
        .map(|(text, want)| (text.as_str(), want.as_str()))
        .collect();
    check_text(&rows);
}

#[test]
fn each_lossy_separator_marks_an_address_with_a_part_or_a_blank_missing() {
    check_text(&[
        ("j@ne@ending from uni.edu", HIDDEN),
        ("<@ending from uni.edu>", "<[address hidden]>"),
        ("j@ne @ending from\nuni.edu", "[address hidden]\nuni.edu"),
        ("j@nem@iii@g oii uni.edu", HIDDEN),
        ("< m@iii@g oii uni.edu>", "< [address hidden]>"),
        ("j@ne m@iii@g oii", HIDDEN),
        (
            "\"jane doe\" m@ili@g off uni.edu",
            "\"jane doe\" [address hidden]",
        ),
        ("j@ne m@ili@g off <uni.edu>", "[address hidden] <uni.edu>"),
    ]);
    // The separators alone, and words that only start like one.
    check_unchanged(&[
        "@ending from",
        " m@iii@g oii ",
        "m@ili@g off",
        "a m@iii@g list",
        "the ending from here",
    ]);
}

#[test]
fn a_list_s_address_stays_after_each_lossy_separator() {
    check_unchanged(&[
        "R-@ig-phylo m@iling li@t - R-@ig-phylo @ending from r-project.org",
        "r-heip m@iii@g oii r-project.org",
        "r-help m@ili@g off r-project.org",
        "r-devel @ending from @t@t.m@th.ethz.ch",
    ]);
    check_text(&[
        ("j@ne.doe @ending from r-project.org", HIDDEN),
        ("r-heip m@iii@g oii ex@mpie.org", HIDDEN),
        ("j@ne m@ili@g off r-project.org", HIDDEN),
    ]);
}

#[test]
fn a_display_name_in_any_lossy_form_becomes_its_name_without_the_marks() {
    check_names(&[
        ("j@ne|doe @end|ng |rom un|@edu", "jnedoe"),
        ("j@ne.doe @ending from uni.edu", "jne.doe"),
        ("p@t.kim m@iii@g oii coi-ex@mpie.org", "pt.kim"),
        ("\"p@t.kim m@ili@g off col-ex@mple.org\"", "pt.kim"),
        ("@|@ m@iii@g oii coi-ex@mpie.org", "unknown"),
        (
            "P@t Kim p@t.kim m@iii@g oii coi-ex@mpie.org",
            "P@t Kim [address hidden]",
        ),
    ]);
}

#[test]
fn a_domain_with_digits_or_markup_in_its_last_part_is_still_a_domain() {
    check_text(&[
        ("jane.doe at uni.edu.2", HIDDEN),
        ("see jane.doe at uni.edu.1", "see [address hidden]"),
        ("jane.doe@uni.edu.2026", HIDDEN),
        ("<jane.doe at dept.uni.__edu", "<[address hidden]"),
        ("mailto:jane.doe at __uni.edu", "mailto:[address hidden]"),
        ("jane.doe at uni.**edu", HIDDEN),
        ("jane.doe@uni.__edu", HIDDEN),
        ("jane.doe at uni.-edu", HIDDEN),
        ("jane.doe at uni.+edu", HIDDEN),
        // Markup after the last letter is not part of the domain.
        ("**jane.doe at uni.edu**", "**[address hidden]**"),
        ("__jane.doe@uni.edu__", "__[address hidden]__"),
    ]);
}

#[test]
fn a_number_a_time_or_a_file_name_is_still_no_domain() {
    check_unchanged(&[
        "significant at 0.05",
        "fixed in version 1.5.2, or at 1.5.2",
        "See you at 5.30pm.",
        "Timed on one core at 2.4GHz.",
        "placed at .Rprofile",
        "str(tree at edge)",
        "tested at 0.05_level",
        "weights at 0.5*x and 0.25**2",
        "x@0.5",
    ]);
}

#[test]
fn a_question_mark_belongs_to_a_domain_only_before_a_full_stop() {
    // The list's address stays, and so does the rest of the link.
    check_unchanged(&[
        "<mailto:r-sig-phylo-request at r-project.org?subject=unsubscribe>",
        "<mailto:r-sig-phylo-request@r-project.org?subject=unsubscribe>",
        "Is it r-sig-phylo at r-project.org?",
    ]);
    check_text(&[
        (
            "<mailto:jane.doe at uni.edu?subject=hello>",
            "<mailto:[address hidden]?subject=hello>",
        ),
        ("jane@uni.edu?subject=a.b", "[address hidden]?subject=a.b"),
        (
            "j@ne @end|ng |rom un|@edu?subject=x",
            "[address hidden]?subject=x",
        ),
        // Before a full stop it is the letter the source lost.
        ("jane at universit?t.de", HIDDEN),
        ("jane at ?bung.de?", "[address hidden]?"),
        ("j@ne @end|ng |rom un|ver@|t?t@de", HIDDEN),
    ]);
}

#[test]
fn a_combining_mark_belongs_to_the_letter_before_it() {
    check_text(&[
        ("jose\u{301}.garci\u{301}a at uni.es", HIDDEN),
        ("jose\u{301}.garci\u{301}a@uni.es", HIDDEN),
        ("jose\u{301} at uni.es", HIDDEN),
        ("jose\u{301} @end|ng |rom un|@es", HIDDEN),
        ("jane at universita\u{308}t.de", HIDDEN),
        ("jane at uni.cafe\u{301} now", "[address hidden] now"),
    ]);
    check_names(&[(
        "jose\u{301}.garci\u{301}a at uni.es",
        "jose\u{301}.garci\u{301}a",
    )]);
}

#[test]
fn an_address_in_a_link_is_hidden_and_the_rest_of_the_link_stays() {
    check_text(&[
        // Mailman's own link to a member's options.
        (
            "https://stat.ethz.ch/mailman/options/r-sig-phylo/jane.doe%40uni.edu",
            "https://stat.ethz.ch/mailman/options/r-sig-phylo/[address hidden]",
        ),
        // A link rewritten by the recipient's mail service, which puts the
        // recipient into a parameter between two escaped bars.
        (
            "https://eur01.safelinks.protection.outlook.com/?url=http%3A%2F%2Fx.org&data=05%7C01%7Cjane.doe%40uni.edu%7Cabc&reserved=0",
            "https://eur01.safelinks.protection.outlook.com/?url=http%3A%2F%2Fx.org&data=05%7C01%7C[address hidden]%7Cabc&reserved=0",
        ),
        (
            "<mailto:jane.doe%40uni.edu?subject=hi>",
            "<mailto:[address hidden]?subject=hi>",
        ),
        // The name ends at what a link separates its parts with.
        (
            "https://example.org/u?email=jane.doe%40uni.edu&x=1",
            "https://example.org/u?email=[address hidden]&x=1",
        ),
        (
            "https://example.org/u?a=1&jane.doe%40uni.edu",
            "https://example.org/u?a=1&[address hidden]",
        ),
        ("x;jane.doe%40uni.edu;y", "x;[address hidden];y"),
        ("to|jane.doe%40uni.edu|cc", "to|[address hidden]|cc"),
        ("u=x%3ajane.doe%40uni.edu", "u=x%3a[address hidden]"),
        // The domain ends at an escape and at the start of a query.
        ("jane.doe%40uni.edu%3Fx", "[address hidden]%3Fx"),
        ("jane.doe%40uni.edu?x.y=1", "[address hidden]?x.y=1"),
        ("at jane.doe%40uni.edu.", "at [address hidden]."),
        // No name is read before it, as after a sign.
        ("https://example.org/%40uni.edu", "https://example.org/[address hidden]"),
        // A link inside a link has the sign escaped twice.
        (
            "u=https%253A%252F%252Fx.org%252Fjane.doe%2540uni.edu%26y",
            "u=https%253A%252F%252Fx.org%252F[address hidden]%26y",
        ),
    ]);
    check_unchanged(&[
        // A list's address stays in a link too.
        "https://stat.ethz.ch/mailman/options/r-sig-phylo/r-sig-phylo%40r-project.org",
        "mailto:r-help%40r-project.org?subject=help",
        // An escaped sign with no domain after it.
        "100%40 of them",
        "x%40y",
        "a 50%400.5 split",
    ]);
}

#[test]
fn an_address_spelled_with_at_in_brackets_or_dashes_is_hidden() {
    let rows: Vec<(String, String)> = [
        "(at)", "[at]", "<at>", "{at}", "(AT)", "[At]", "--at--", "--AT--",
    ]
    .iter()
    .flat_map(|at| {
        [
            ("jane.doe AT uni.edu", "[address hidden]"),
            ("jane.doeATuni.edu", "[address hidden]"),
            ("jane.doe ATuni.edu", "[address hidden]"),
            ("jane.doeAT uni.edu", "[address hidden]"),
            (
                "Jane Doe <jane.doe AT uni.edu> writes:",
                "Jane Doe <[address hidden]> writes:",
            ),
            (
                "write to jane.doeATuni.edu, please",
                "write to [address hidden], please",
            ),
        ]
        .map(|(text, want)| (text.replace("AT", at), want.to_string()))
    })
    .collect();
    let rows: Vec<(&str, &str)> = rows
        .iter()
        .map(|(text, want)| (text.as_str(), want.as_str()))
        .collect();
    check_text(&rows);
}

#[test]
fn an_address_spelled_with_at_in_capitals_is_hidden() {
    check_text(&[
        ("jane.doe AT uni.edu", HIDDEN),
        (
            "Jane Doe <jane.doe AT uni.edu> wrote:",
            "Jane Doe <[address hidden]> wrote:",
        ),
        (
            "https://stat.ethz.ch/mailman/options/r-sig-phylo/jane.doe--at--uni.edu",
            "https://stat.ethz.ch/mailman/options/r-sig-phylo/[address hidden]",
        ),
        // The dashes of this spelling are not read as part of a domain, and
        // the two dashes in the middle of another domain still are.
        ("jane--at--uni--at--x.org", HIDDEN),
        ("jane at xn--mller-kva.de", HIDDEN),
        ("jane--at--xn--mller-kva.de", HIDDEN),
    ]);
}

#[test]
fn what_lets_a_sentence_stay_does_not_apply_to_another_spelling_of_at() {
    check_text(&[
        ("look (at) example.org", HIDDEN),
        ("available AT www.example.org", HIDDEN),
        (
            "code [at] github.com/user/repo",
            "[address hidden]/user/repo",
        ),
        ("see--at--example.org", HIDDEN),
    ]);
    // A list's address stays however the sign is spelled.
    check_unchanged(&[
        "r-help (at) r-project.org",
        "R-sig-phylo AT r-project.org",
        "r-help--at--r-project.org",
    ]);
}

#[test]
fn at_in_brackets_or_capitals_without_two_parts_is_left_alone() {
    check_unchanged(&[
        "predict(at)",
        "x[at] <- 1",
        "f(at) x",
        "(at) uni.edu",
        "jane (at) work",
        "jane AT work",
        "AT uni.edu",
        // More than one blank on a side.
        "look  (at)  example.org",
        "jane.doe  AT  uni.edu",
        // Brackets that are not a pair, and other words in them.
        "jane.doe (at] uni.edu",
        "jane.doe (cat) uni.edu",
        "jane.doe -at- uni.edu",
    ]);
}

#[test]
fn a_domain_spelled_with_dot_between_its_parts_is_hidden() {
    check_text(&[
        ("jane at uni dot edu", HIDDEN),
        ("jane.doe AT uni DOT edu", HIDDEN),
        ("jane (at) uni (dot) edu", HIDDEN),
        ("jane[at]uni[dot]edu", HIDDEN),
        ("jane [at] uni [DOT] edu", HIDDEN),
        ("jane@uni dot edu", HIDDEN),
        (
            "jane at dept dot uni.ac dot uk, I think",
            "[address hidden], I think",
        ),
        // The word must stand between two parts of a domain.
        ("jane at uni.edu dot", "[address hidden] dot"),
        ("jane at uni.edu dot .", "[address hidden] dot ."),
    ]);
    check_unchanged(&[
        "it points at the dot",
        "a dot at the top",
        "jane at uni dot 5",
        "jane at uni  dot  edu",
        "jane at uni Dot edu",
        "jane at uni (dot edu",
    ]);
}

#[test]
fn an_address_broken_over_a_line_end_is_hidden_and_the_lines_stay() {
    check_text(&[
        (
            "On Mon, Jane Doe <jane.doe at\n> uni.edu> wrote:",
            "On Mon, Jane Doe <[address hidden]\n> > wrote:",
        ),
        (
            "On Mon, Jane Doe <jane.doe\n> at uni.edu> wrote:",
            "On Mon, Jane Doe <[address hidden]\n> > wrote:",
        ),
        ("jane.doe at\nuni.edu", "[address hidden]\n"),
        ("jane.doe\nat uni.edu", "[address hidden]\n"),
        (
            "> > jane.doe at\n> > uni.edu, I think",
            "> > [address hidden]\n> > , I think",
        ),
        ("| jane.doe at\n| uni.edu", "| [address hidden]\n| "),
        ("jane.doe at\n   uni.edu", "[address hidden]\n   "),
        // Blanks and a carriage return at the end of the line.
        ("jane.doe at \r\n> uni.edu", "[address hidden] \r\n> "),
        ("jane.doe \r\n> at uni.edu", "[address hidden] \r\n> "),
    ]);
}

#[test]
fn a_sentence_broken_over_a_line_end_stays_as_it_does_on_one_line() {
    check_unchanged(&[
        "the notes are available at\n> www.example.org/phylo",
        "the code at\ngithub.com/user/repo",
        "the files are hosted\n> at example.org",
        "R-sig-phylo mailing list - R-sig-phylo at\n> r-project.org",
        // No domain on the next line, or an empty line between.
        "we met at\nnoon",
        "first line\nat last",
        "jane.doe at\n\nuni.edu",
        "jane.doe\n\nat uni.edu",
        "jane.doe at\n> \n> uni.edu",
    ]);
}

#[test]
fn a_display_name_in_another_spelling_becomes_its_mailbox_name() {
    check_names(&[
        ("jane.doe (at) uni.edu", "jane.doe"),
        ("jane.doe[at]uni.edu", "jane.doe"),
        ("jane.doe AT uni DOT edu", "jane.doe"),
        ("jane.doe%40uni.edu", "jane.doe"),
        ("jane.doe--at--uni.edu", "jane.doe"),
    ]);
}
