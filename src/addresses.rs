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
//!   [`deobfuscate_email`](crate::message::deobfuscate_email);
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

/// The source's lossy form writes it as this, with a blank on each side.
const LOSSY: &str = "@end|ng |rom";

/// The three ways an address is written in these archives.
#[derive(Clone, Copy, PartialEq)]
enum Form {
    /// `mailbox at domain.tld`
    At,
    /// `mailbox @end|ng |rom domain`
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
/// "it is hosted at example.org".
const ORDINARY_WORDS: [&str; 17] = [
    "look",
    "looking",
    "available",
    "found",
    "here",
    "located",
    "hosted",
    "is",
    "are",
    "was",
    "it",
    "and",
    "up",
    "now",
    "left",
    "indexed",
    "see",
];

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
    while let Some(found) = next_address(text, from) {
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
    if let Some(found) = next_address(whole, 0) {
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
fn next_address(text: &str, from: usize) -> Option<Address> {
    let bytes = text.as_bytes();
    // The separators are ASCII, so looking for them byte by byte never
    // lands inside a longer character.
    (from..bytes.len()).find_map(|at| match bytes[at] {
        b' ' if text[at..].starts_with(AT) => at_form(text, at, from),
        b'@' if text[at..].starts_with(LOSSY) => lossy_form(text, at, from),
        b'@' => sign_form(text, at, from),
        _ => None,
    })
}

/// `mailbox at domain.tld`, with its separator starting at `separator`. The
/// words are a sentence as often as an address, so both parts must be there.
fn at_form(text: &str, separator: usize, floor: usize) -> Option<Address> {
    let separator_end = separator + AT.len();
    let domain = domain_after(text, separator_end, false);
    if !is_domain(&text[domain.clone()]) {
        return None;
    }
    let mailbox = mailbox_before(text, separator, floor, false);
    if mailbox.is_empty() {
        return None;
    }
    Some(Address {
        form: Form::At,
        hidden: mailbox.start..domain.end,
        mailbox,
        domain,
        separator_end,
    })
}

/// `mailbox@domain.tld`, with its sign at `sign`. A domain after the sign is
/// hidden even when no name is read before it: the name may be one this
/// module does not know how to read, as `"jane doe"@uni.edu` is.
fn sign_form(text: &str, sign: usize, floor: usize) -> Option<Address> {
    let separator_end = sign + 1;
    let domain = domain_after(text, separator_end, false);
    if !is_domain(&text[domain.clone()]) {
        return None;
    }
    let mailbox = mailbox_before(text, sign, floor, false);
    Some(Address {
        form: Form::Sign,
        hidden: mailbox.start..domain.end,
        mailbox,
        domain,
        separator_end,
    })
}

/// `mailbox @end|ng |rom domain`, with the separator's own text starting at
/// `core`. No sentence has these words in it, so whatever stands on either
/// side is hidden with them, be it only a name or only a domain, and the
/// blank on each side may be missing.
fn lossy_form(text: &str, core: usize, floor: usize) -> Option<Address> {
    let bytes = text.as_bytes();
    let core_end = core + LOSSY.len();
    let blank_before = core > floor && bytes[core - 1] == b' ';
    let blank_after = bytes.get(core_end) == Some(&b' ');
    let separator_end = core_end + usize::from(blank_after);
    let mailbox = mailbox_before(text, core - usize::from(blank_before), floor, true);
    let domain = domain_after(text, separator_end, true);
    if mailbox.is_empty() && domain.is_empty() {
        return None;
    }
    // A part that is missing leaves its blank where it was.
    let start = if mailbox.is_empty() {
        core
    } else {
        mailbox.start
    };
    let end = if domain.is_empty() {
        core_end
    } else {
        domain.end
    };
    Some(Address {
        form: Form::Lossy,
        hidden: start..end,
        mailbox,
        domain,
        separator_end,
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

/// True for `domain.tld`: something that starts with a letter or a digit
/// and whose last part, after the last full stop, starts with a letter.
/// That leaves numbers alone ("significant at 0.05"), and times, and file
/// names that start with a full stop.
fn is_domain(text: &str) -> bool {
    text.starts_with(|c: char| c.is_alphanumeric() || c == LOST_LETTER)
        && text
            .rsplit_once('.')
            .is_some_and(|(_, tld)| tld.starts_with(char::is_alphabetic))
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
/// any script, or one of the signs the mail standards allow in a name. The
/// slash is one of those signs and is left out, because it ends the web
/// address before a name far more often than it is part of a name.
fn is_mailbox_char(c: char) -> bool {
    c.is_alphanumeric() || ".!#$%&'*+-=?^_`{|}~".contains(c)
}

/// The run of domain characters that starts at `start`, cut back to the
/// last letter or digit in it: a full stop or a question mark that ends the
/// sentence is a domain character too, but it is not part of the address.
fn domain_after(text: &str, start: usize, lossy: bool) -> Range<usize> {
    let mut end = start;
    for (at, c) in text[start..].char_indices() {
        let closes = c.is_alphanumeric() || lossy && is_lost(c);
        // Mailman takes `_` and `+` for part of a domain.
        if !(closes || matches!(c, '.' | '-' | '_' | '+' | LOST_LETTER)) {
            break;
        }
        if closes {
            end = start + at + c.len_utf8();
        }
    }
    start..end
}

/// True for the two characters the lossy form puts where it lost one.
fn is_lost(c: char) -> bool {
    matches!(c, '@' | '|')
}
