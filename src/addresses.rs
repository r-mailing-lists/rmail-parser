//! Hiding mailbox addresses, for `parse --hide-addresses`.
//!
//! The archive of a list may be open to its members only, so that the
//! addresses of those who write to it cannot be collected. Its messages may
//! then be published, its addresses may not. This module takes them out of
//! a message before anything is derived from it.
//!
//! An address is written in three ways in these archives:
//!
//! - `mailbox at domain.tld`, Mailman's text form, by far the commonest;
//! - `mailbox @end|ng |rom domain`, the source's lossy form, see
//!   [`deobfuscate_email`](crate::message::deobfuscate_email), which has
//!   three more separators of the same kind;
//! - `mailbox@domain.tld`.
//!
//! Each is found by its separator, and the name before it and the domain
//! after it are read outwards from there. A name is read back no further
//! than the nearest character that cannot be in one, and a blank or the
//! separator before it always is such a character. A domain is read on in
//! the same way. No part of a text is therefore read more than a few times,
//! however long a line without a blank is, and the work grows with the
//! length of the text and no faster. All of it is done on character
//! boundaries: the separators are ASCII, and everything around them is read
//! one character at a time.

use std::cmp::Ordering;
use std::ops::Range;

use crate::message::{make_snippet, Message};

/// What an address is replaced with.
pub const HIDDEN: &str = "[address hidden]";

/// What a display name becomes when nothing of it may be shown.
const UNKNOWN: &str = "unknown";

/// What the source's text files have where a letter beyond ASCII was. It is
/// read as a letter inside a name or a domain, and at the start of either.
const LOST_LETTER: char = '?';

/// Mailman's text form writes the `@` of an address as this.
const AT: &str = " at ";

/// The source's lossy form writes it as one of these, with a blank on each
/// side. All four are in the files the source offers today.
const LOSSY: [&str; 4] = ["@end|ng |rom", "@ending from", "m@iii@g oii", "m@ili@g off"];

/// The ways an address is written in these archives.
#[derive(Clone, Copy, PartialEq)]
enum Form {
    /// `mailbox at domain.tld`
    At,
    /// `mailbox @end|ng |rom domain`, or another of the lossy separators
    Lossy,
    /// `mailbox@domain.tld`
    Sign,
}

/// The domains the R mailing lists have their addresses at.
const LIST_DOMAINS: [&str; 4] = [
    "r-project.org",
    "stat.math.ethz.ch",
    "lists.r-project.org",
    "lists.r-forge.r-project.org",
];

/// What the name of an R mailing list starts with.
const LIST_PREFIXES: [&str; 2] = ["r-", "bioc"];

/// Words that stand before ` at ` in a sentence and are not mailbox names:
/// "the files are hosted at example.org". `is` and `it` are not among
/// them: a department may well have either as its mailbox.
const ORDINARY_WORDS: [&str; 15] = [
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
];

/// What stands between a mailbox name and a domain, as found in a text.
struct Separator {
    form: Form,
    /// Its own text, without a blank beside it.
    core: Range<usize>,
    /// Where a mailbox name before it ends.
    name_end: usize,
    /// Where a domain after it starts.
    domain_start: usize,
}

/// An address found in a text, as byte ranges of that text.
struct Address {
    form: Form,
    /// Empty when no name stands before the separator.
    mailbox: Range<usize>,
    /// Empty when no domain stands after the separator.
    domain: Range<usize>,
    /// Where its separator ends.
    separator_end: usize,
    /// What the marker takes the place of.
    hidden: Range<usize>,
}

/// Replaces every mailbox address in `text` with a marker. The address of
/// an R mailing list stays: `list` is the name of the list being parsed.
pub fn hide_in_text(text: &str, list: &str) -> String {
    let mut out = String::with_capacity(text.len());
    // Everything before `copied` is already in `out`.
    let mut copied = 0;
    let mut from = 0;
    let lossy = lossy_addresses(text);
    while let Some(found) = next_address(text, from, &lossy) {
        if stays(text, &found, list) {
            // Only the separator is passed over. What follows it is looked
            // at again, in case it is the mailbox name of a next address.
            from = found.separator_end;
            continue;
        }
        out.push_str(&text[copied..found.hidden.start]);
        out.push_str(HIDDEN);
        copied = found.hidden.end;
        from = copied;
    }
    out.push_str(&text[copied..]);
    out
}

/// Hides the addresses in everything of `msg` that is shown to a reader:
/// the sender's name, the subject and the text, and with them the two
/// fields cut from those, the cleaned subject and the snippet.
///
/// What identifies the message or its sender is left as it is. The hash is
/// that of the real address, and it is what joins a person's messages
/// across lists. The ids, the references and the thread are what replies
/// are matched by.
pub fn hide_in_message(msg: &mut Message, list: &str) {
    msg.from_name = hide_in_name(&msg.from_name, list);
    msg.subject = hide_in_text(&msg.subject, list);
    // The cleaned subject is the subject without what stands before its
    // first word, `Re:` and `[R]`, so an address is hidden in it as it is
    // in the subject. Cleaning the field itself does not rest on that, and
    // spares `clean_subject`, which compiles its pattern on every call.
    msg.subject_clean = hide_in_text(&msg.subject_clean, list);
    msg.body_plain = hide_in_text(&msg.body_plain, list);
    msg.body_snippet = make_snippet(&msg.body_plain);
}

/// What `name`, the display name of a sender, may show.
///
/// A name that is an address as a whole becomes the mailbox name of that
/// address, which for the lossy form is the name without the characters it
/// lost. That holds for a list's address too: what lets an address stay in
/// a text does not make it a name. A name with an address among other words
/// is treated as a text. Where that leaves nothing to show, the name is
/// `unknown`.
pub fn hide_in_name(name: &str, list: &str) -> String {
    // A `From:` line may leave blanks, quotation marks or brackets around
    // the address. A lost character at either end belongs to it.
    let whole = name.trim_matches(|c: char| !(c.is_alphanumeric() || is_lost(c)));
    if let Some(found) = next_address(whole, 0, &lossy_addresses(whole)) {
        if found.hidden == (0..whole.len()) {
            let mailbox = &whole[found.mailbox];
            let shown: String = match found.form {
                Form::Lossy => mailbox.chars().filter(|c| !is_lost(*c)).collect(),
                Form::At | Form::Sign => mailbox.to_string(),
            };
            if shown.is_empty() {
                return UNKNOWN.to_string();
            }
            return shown;
        }
    }
    let hidden = hide_in_text(name, list);
    let left = hidden.replace(HIDDEN, "");
    if left.len() < hidden.len() && left.trim().is_empty() {
        return UNKNOWN.to_string();
    }
    hidden
}

/// The first address whose separator is at or after `from`. Its mailbox
/// name does not reach back before `from` either: what is before `from` is
/// an address that was hidden, or the separator of one that stays.
fn next_address(text: &str, from: usize, lossy: &[Range<usize>]) -> Option<Address> {
    (from..text.len()).find_map(|at| {
        let separator = separator_at(text, at, from)?;
        // A `@` in the name or the domain of a lossy address is the mark of
        // a lost character there, and not the sign of an address of its own.
        if separator.form == Form::Sign && within_any(lossy, at) {
            return None;
        }
        address(text, &separator, from)
    })
}

/// The separator that starts at the byte `at`, if one does. Every separator
/// starts with an ASCII character, so looking for one byte by byte never
/// lands inside a longer character.
fn separator_at(text: &str, at: usize, floor: usize) -> Option<Separator> {
    match text.as_bytes()[at] {
        b' ' if text[at..].starts_with(AT) => Some(Separator {
            form: Form::At,
            core: at + 1..at + AT.len() - 1,
            name_end: at,
            domain_start: at + AT.len(),
        }),
        b'@' | b'm' => lossy_separator(text, at, floor).or_else(|| {
            text[at..].starts_with('@').then_some(Separator {
                form: Form::Sign,
                core: at..at + 1,
                name_end: at,
                domain_start: at + 1,
            })
        }),
        _ => None,
    }
}

/// The lossy separator whose own text starts at `at`. The blank on each
/// side of it may be missing.
fn lossy_separator(text: &str, at: usize, floor: usize) -> Option<Separator> {
    let bytes = text.as_bytes();
    // All four start with `@e` or `m@`. Most characters of a text are told
    // apart from a separator by that alone.
    if !matches!(&bytes[at..], [b'@', b'e', ..] | [b'm', b'@', ..]) {
        return None;
    }
    let core = LOSSY.iter().find(|core| text[at..].starts_with(*core))?;
    let core = at..at + core.len();
    let blank_before = core.start > floor && bytes[core.start - 1] == b' ';
    let blank_after = bytes.get(core.end) == Some(&b' ');
    Some(Separator {
        form: Form::Lossy,
        name_end: core.start - usize::from(blank_before),
        domain_start: core.end + usize::from(blank_after),
        core,
    })
}

/// Every address of `text` in the lossy form, from the start of its name to
/// the end of its domain, in the order they stand in.
fn lossy_addresses(text: &str) -> Vec<Range<usize>> {
    let bytes = text.as_bytes();
    let mut found = Vec::new();
    let mut floor = 0;
    let mut at = 0;
    while at < bytes.len() {
        let separator = match bytes[at] {
            b'@' | b'm' => lossy_separator(text, at, floor),
            _ => None,
        };
        match separator {
            Some(separator) => {
                let name = mailbox_before(text, separator.name_end, floor, true);
                let domain = domain_after(text, separator.domain_start, true).range;
                floor = domain.end.max(separator.core.end);
                found.push(name.start..floor);
                at = floor;
            }
            None => at += 1,
        }
    }
    found
}

/// True when `at` lies in one of `ranges`, which are in ascending order and
/// do not overlap.
fn within_any(ranges: &[Range<usize>], at: usize) -> bool {
    ranges
        .binary_search_by(|range| {
            if range.end <= at {
                Ordering::Less
            } else if range.start > at {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        })
        .is_ok()
}

/// The address around `separator`, if what stands on its two sides makes
/// one. Its mailbox name does not reach back before `floor`.
///
/// ` at ` is a sentence as often as an address, so both parts must be there.
/// After a sign the domain is hidden even when no name is read before it:
/// the name may be one this module does not know how to read, as
/// `"jane doe"@uni.edu` is. No sentence has the words of a lossy separator
/// in it, so whatever stands on either side of one is hidden with it, be it
/// only a name or only a domain.
fn address(text: &str, separator: &Separator, floor: usize) -> Option<Address> {
    let lossy = separator.form == Form::Lossy;
    let Domain {
        range: domain,
        named,
    } = domain_after(text, separator.domain_start, lossy);
    // The lossy form has few full stops left: it writes most as `@`.
    if !lossy && !named {
        return None;
    }
    let mailbox = mailbox_before(text, separator.name_end, floor, lossy);
    let missing = match separator.form {
        Form::At => mailbox.is_empty(),
        Form::Sign => false,
        Form::Lossy => mailbox.is_empty() && domain.is_empty(),
    };
    if missing {
        return None;
    }
    // A part that is missing leaves the blank beside the separator alone.
    let start = if mailbox.is_empty() {
        separator.core.start
    } else {
        mailbox.start
    };
    let end = if domain.is_empty() {
        separator.core.end
    } else {
        domain.end
    };
    Some(Address {
        form: separator.form,
        hidden: start..end,
        mailbox,
        domain,
        separator_end: separator.domain_start,
    })
}

/// True for an address that is left as it is.
fn stays(text: &str, found: &Address, list: &str) -> bool {
    let lossy = found.form == Form::Lossy;
    let mailbox = &text[found.mailbox.clone()];
    let domain = &text[found.domain.clone()];
    // An R mailing list's own address.
    let of_a_list = LIST_PREFIXES
        .iter()
        .chain([&list])
        .any(|prefix| !prefix.is_empty() && rest_after(mailbox, prefix, lossy).is_some())
        && LIST_DOMAINS
            .iter()
            .any(|name| rest_after(domain, name, lossy) == Some(""));
    if of_a_list || found.form != Form::At {
        return of_a_list;
    }
    // Only Mailman's text form can be a sentence as well: "the code at
    // github.com/user/repo" is a web address written in prose, and so is
    // what follows an ordinary word. Anything else is taken for an address:
    // a hidden word is a small loss, a published address cannot be taken
    // back.
    domain.starts_with("www.")
        || text[found.domain.end..].starts_with('/')
        || ORDINARY_WORDS
            .iter()
            .any(|word| mailbox.eq_ignore_ascii_case(word))
}

/// What is left of `written` after `expected` at its start, compared
/// without regard to case, or `None` when it does not start with it. In
/// the lossy form a `@` stands where an a, an s or a full stop was, and a
/// `|` where an i, an l or an f was, so there each of the two also matches
/// the letters it may have been. It does not match any other: `@|mon` is
/// not a name that starts with `r-`.
fn rest_after<'a>(written: &'a str, expected: &str, lossy: bool) -> Option<&'a str> {
    let mut rest = written.chars();
    for e in expected.chars() {
        let w = rest.next()?;
        let lost = match w {
            '@' => matches!(e.to_ascii_lowercase(), 'a' | 's' | '.'),
            '|' => matches!(e.to_ascii_lowercase(), 'i' | 'l' | 'f'),
            _ => false,
        };
        if !(w.eq_ignore_ascii_case(&e) || lossy && lost) {
            return None;
        }
    }
    Some(rest.as_str())
}

/// The run of mailbox characters that ends at `end` and starts no earlier
/// than `floor`, from the first letter or digit in it: a quotation mark or
/// a dash before a name is a mailbox character too, but it is not part of
/// the name. A character that stands for a lost letter is.
fn mailbox_before(text: &str, end: usize, floor: usize, lossy: bool) -> Range<usize> {
    let mut start = end;
    for (at, c) in text[floor..end].char_indices().rev() {
        if !(is_mailbox_char(c) || lossy && is_lost(c)) {
            break;
        }
        start = floor + at;
    }
    let opens = |c: char| c.is_alphanumeric() || c == LOST_LETTER || lossy && is_lost(c);
    let name = text[start..end].find(opens).unwrap_or(end - start);
    start + name..end
}

/// True for a character a mailbox name may hold: a letter or a digit of
/// any script, with the marks that combine with it, or one of the signs the
/// mail standards allow in a name. The slash is one of those signs and is
/// left out, because it ends the web address before a name far more often
/// than it is part of a name.
fn is_mailbox_char(c: char) -> bool {
    c.is_alphanumeric() || is_mark(c) || ".!#$%&'*+-=?^_`{|}~".contains(c)
}

/// True for an accent that is written after its letter, as a character of
/// its own. It belongs to that letter: `e` and U+0301 are one `e` with an
/// acute accent, and must not split a name in two.
fn is_mark(c: char) -> bool {
    matches!(
        c,
        '\u{300}'..='\u{36f}'
            | '\u{1ab0}'..='\u{1aff}'
            | '\u{1dc0}'..='\u{1dff}'
            | '\u{20d0}'..='\u{20ff}'
            | '\u{fe20}'..='\u{fe2f}'
    )
}

/// What stands after a separator.
struct Domain {
    /// The run of domain characters, cut back to the last letter or digit
    /// in it: a full stop that ends the sentence is a domain character too,
    /// but it is not part of the address.
    range: Range<usize>,
    /// True for `domain.tld`: a letter or a digit, followed somewhere by a
    /// full stop and then a letter. Signs of markup may stand between the
    /// two, as in `uni.__edu`, and anything may follow, as in `uni.edu.2`.
    /// A number is not one ("significant at 0.05"), nor is a time, nor a
    /// file name that starts with a full stop.
    named: bool,
}

/// Reads the domain that starts at `start`.
fn domain_after(text: &str, start: usize, lossy: bool) -> Domain {
    let mut end = start;
    let mut named = false;
    // A letter or a digit has been read.
    let mut begun = false;
    // A full stop has been read after that, and only markup since.
    let mut stopped = false;
    // What `end` and `named` were at the first question mark that no full
    // stop has followed yet.
    let mut question = None;
    for (at, c) in text[start..].char_indices() {
        let after = start + at + c.len_utf8();
        if c.is_alphanumeric() || lossy && is_lost(c) {
            named |= stopped && c.is_alphabetic();
            (begun, stopped, end) = (true, false, after);
            // In the lossy form a `@` is a full stop as often as not.
            if lossy && c == '@' {
                question = None;
            }
        } else if is_mark(c) && end + c.len_utf8() == after {
            end = after;
        } else if c == '.' {
            stopped = begun;
            question = None;
        } else if c == LOST_LETTER {
            // It is the letter the source lost when a full stop follows it
            // in the domain, as in `universit?t.de`. When none does it is
            // the sentence's own, or the start of a link's query.
            question = question.or(Some((end, named)));
            (begun, stopped) = (true, false);
        } else if !matches!(c, '-' | '_' | '+' | '*') {
            // Mailman takes `_` and `+` for part of a domain, and `*` and
            // `_` are what a mail program puts around emphasised text.
            break;
        }
    }
    if let Some(before) = question {
        (end, named) = before;
    }
    Domain {
        range: start..end,
        named,
    }
}

/// True for the two characters the lossy form puts where it lost one.
fn is_lost(c: char) -> bool {
    matches!(c, '@' | '|')
}
