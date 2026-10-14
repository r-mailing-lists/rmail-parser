//! Hiding mailbox addresses, for `parse --hide-addresses`.
//!
//! The archive of a list may be open to its members only, so that the
//! addresses of those who write to it cannot be collected. Its messages may
//! then be published, its addresses may not. This module takes them out of
//! a message before anything is derived from it, and writes the message's
//! identifiers as hashes when the message itself is written.
//!
//! An address is a mailbox name, a separator and a domain. These archives
//! have the separator in many spellings:
//!
//! - ` at `, Mailman's text form, by far the commonest, on one line or
//!   broken over a line end by a mail program that wrapped the line;
//! - ` @end|ng |rom ` and three more of the same kind, the source's lossy
//!   forms, see [`deobfuscate_email`](crate::message::deobfuscate_email);
//! - `@`;
//! - `%40`, in a link;
//! - `(at)`, `--at--`, ` AT ` and the like, which people write to keep an
//!   address from being collected.
//!
//! Each is found by its first character, and the name before it and the
//! domain after it are read outwards from there. A name is read back no
//! further than the nearest character that cannot be in one, and never
//! past the last address that was found. A domain is read on in the same
//! way, and ends where the next separator starts. No part of a text is
//! therefore read more than a few times, however long a line without a
//! blank is, and the work grows with the length of the text and no faster.
//! All of it is done on character boundaries: every separator starts with
//! an ASCII character, and everything around it is read one character at a
//! time.

use std::cmp::Ordering;
use std::ops::Range;

use sha2::{Digest, Sha256};

use crate::message::{clean_subject, make_snippet, Message};

/// What an address is replaced with.
pub const HIDDEN: &str = "[address hidden]";

/// What a display name becomes when nothing of it may be shown.
const UNKNOWN: &str = "unknown";

/// What the source's text files have where a letter beyond ASCII was. It is
/// read as a letter inside a name or a domain, and at the start of either.
const LOST_LETTER: char = '?';

/// The source's lossy form writes the `@` of an address as one of these,
/// with a blank on each side. All four are in the files the source offers
/// today.
const LOSSY: [&str; 4] = ["@end|ng |rom", "@ending from", "m@iii@g oii", "m@ili@g off"];

/// Whether a byte is the first of one of them. Nearly every character of a
/// text is told apart from a lossy separator by that alone, which is far
/// cheaper than to compare with all four at every character. The table is
/// made from the list, so that a separator added there is found without
/// anything else to change.
const STARTS_LOSSY: [bool; 256] = {
    let mut starts = [false; 256];
    let mut i = 0;
    while i < LOSSY.len() {
        starts[LOSSY[i].as_bytes()[0] as usize] = true;
        i += 1;
    }
    starts
};

/// A link writes it as the first of these, and a link inside a link as the
/// second.
const ESCAPED_SIGNS: [&str; 2] = ["%40", "%2540"];

/// The ways an address is written in these archives.
#[derive(Clone, Copy, PartialEq)]
enum Form {
    /// `mailbox at domain.tld`, Mailman's text form, on one line or broken
    /// over a line end. It is the one form that is a sentence as often as
    /// an address.
    At,
    /// `mailbox AT domain.tld`, `mailbox (at) domain.tld` and the other
    /// spellings people use to keep an address from being collected.
    Spelled,
    /// `mailbox @end|ng |rom domain`, or another of the lossy separators.
    Lossy,
    /// `mailbox@domain.tld`
    Sign,
    /// `mailbox%40domain.tld`, in a link.
    Link,
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
    /// The line end inside it, with the quote marks that start the next
    /// line. Empty for a separator on one line.
    kept: Range<usize>,
}

impl Separator {
    /// A separator with nothing but its own text between name and domain.
    fn bare(form: Form, core: Range<usize>) -> Self {
        Separator {
            form,
            name_end: core.start,
            domain_start: core.end,
            kept: core.end..core.end,
            core,
        }
    }

    /// A separator that may have one blank on either side of its text.
    fn padded(text: &str, form: Form, core: Range<usize>, floor: usize) -> Self {
        let bytes = text.as_bytes();
        let before = core.start > floor && bytes[core.start - 1] == b' ';
        let after = bytes.get(core.end) == Some(&b' ');
        Separator {
            form,
            name_end: core.start - usize::from(before),
            domain_start: core.end + usize::from(after),
            kept: core.end..core.end,
            core,
        }
    }
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
    /// The part of that which is written again after the marker: the line
    /// end of an address broken over two lines.
    kept: Range<usize>,
    /// True when the name has a full stop that is spelled out, as in
    /// `jane dot doe`.
    name_spelled: bool,
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
        out.push_str(&text[found.kept.clone()]);
        copied = found.hidden.end;
        // "contact me at jane.doe at uni.edu": the name of the address was
        // read as the domain of the words before it, and its own domain is
        // still to come. That goes too, and the one marker stands for all.
        while let Some((kept, end)) = tail(text, copied) {
            out.push_str(&text[kept]);
            copied = end;
        }
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
/// across lists. The Message-ID and the references are what threads are
/// built from: [`hide_identifiers`] sees to those once that is done.
pub fn hide_in_message(msg: &mut Message, list: &str) {
    msg.from_name = hide_in_name(&msg.from_name, list);
    msg.subject = hide_in_text(&msg.subject, list);
    // The cleaned subject is the subject without what stands before its
    // first word, `Re:` and `[R]`, so it is cleaned as the subject is and
    // comes out as the end of the cleaned subject. It does not where what
    // stood before it made the difference: in `[R]@uni.edu` the domain is
    // hidden after the bracket, and by itself `@uni.edu` would stay. Then
    // it is cut from the cleaned subject again. That is rare, which
    // matters: `clean_subject` compiles its pattern on every call.
    let cleaned = hide_in_text(&msg.subject_clean, list);
    msg.subject_clean = if msg.subject.trim_end().ends_with(&cleaned) {
        cleaned
    } else {
        hide_in_text(&clean_subject(&msg.subject), list)
    };
    msg.body_plain = hide_in_text(&msg.body_plain, list);
    msg.body_snippet = make_snippet(&msg.body_plain);
}

/// Writes what names a message among others, its Message-ID and the
/// identifiers of the messages it answers, as hashes.
///
/// A mail program makes a Message-ID out of what it has at hand, which is
/// often the sender's own mail domain and sometimes the sender's mailbox
/// name, so these fields can hold an address or most of one. Each is
/// written as `<` the first 32 hex characters of its SHA-256 `@hidden>`.
/// Equal identifiers give equal hashes, so a reply still names the message
/// it answers.
///
/// This is for the moment a message is written, and no earlier: the `id`
/// of a message and the threads are made from the identifiers as the
/// source has them, and must be what a run that hides nothing makes.
pub fn hide_identifiers(msg: &mut Message) {
    msg.message_id = hidden_identifier(&msg.message_id);
    if let Some(parent) = &mut msg.in_reply_to {
        *parent = hidden_identifier(parent);
    }
    for reference in &mut msg.references {
        *reference = hidden_identifier(reference);
    }
}

/// What one of those fields is written as. An `In-Reply-To:` line may name
/// two messages, or have words beside the identifier: each `<...>` in it
/// is hashed on its own, so that it equals the hash of the Message-ID it
/// names, and nothing else of the line is written. A value without any
/// `<...>` is hashed as a whole. An empty one stays as it is.
fn hidden_identifier(value: &str) -> String {
    let hash = |text: &str| format!("<{}@hidden>", &hex::encode(Sha256::digest(text))[..32]);
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return value.to_string();
    }
    let mut hashes = Vec::new();
    let mut rest = trimmed;
    while let Some(close) = rest.find('>') {
        // Of doubled brackets the inner pair holds the identifier.
        if let Some(open) = rest[..close].rfind('<') {
            hashes.push(hash(&rest[open..=close]));
        }
        rest = &rest[close + 1..];
    }
    if hashes.is_empty() {
        return hash(trimmed);
    }
    hashes.join(" ")
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
                _ => mailbox.to_string(),
            };
            // A list's bounce address carries a member's address after one
            // of these signs: `list-bounces+jane.doe=uni.edu`.
            let shown = shown.split(['+', '=', '%']).next().unwrap_or_default();
            if shown.is_empty() {
                return UNKNOWN.to_string();
            }
            return shown.to_string();
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
    if let Some(len) = lossy_at(text, at) {
        return Some(Separator::padded(text, Form::Lossy, at..at + len, floor));
    }
    match text.as_bytes()[at] {
        b' ' => word_separator(text, at),
        b'\n' => separator_after_line_end(text, at, floor),
        b'@' => Some(sign_separator(text, at, floor)),
        b'%' => ESCAPED_SIGNS
            .iter()
            .find(|sign| text[at..].starts_with(*sign))
            .map(|sign| Separator::bare(Form::Link, at..at + sign.len())),
        b'(' | b'[' | b'<' | b'{' | b'-' => spelled_separator(text, at, floor),
        _ => None,
    }
}

/// The sign at `at`. A mail program that wraps a long line may end it with
/// `=` and go on with the sign on the next:
///
/// ```text
/// <mailto:jane.doe=
/// @uni.edu>
/// ```
///
/// The name then ends before that mark, and the mark and the line end stay
/// in the text.
fn sign_separator(text: &str, at: usize, floor: usize) -> Separator {
    let before = &text.as_bytes()[floor..at];
    let soft_break = [&b"=\r\n"[..], b"=\n"]
        .into_iter()
        .find(|line_end| before.ends_with(line_end))
        .map_or(0, <[u8]>::len);
    Separator {
        form: Form::Sign,
        core: at..at + 1,
        name_end: at - soft_break,
        domain_start: at + 1,
        kept: at - soft_break..at,
    }
}

/// ` at ` or ` AT `, with the blank before the word at `at`. After the lower
/// case word the line may end, and the domain then stands on the next line
/// after its quote marks:
///
/// ```text
/// ... Jane Doe <jane.doe at
/// > uni.edu> wrote:
/// ```
fn word_separator(text: &str, at: usize) -> Option<Separator> {
    let bytes = text.as_bytes();
    let word = at + 1..at + 3;
    let blank_after = bytes.get(word.end) == Some(&b' ');
    match bytes.get(word.clone())? {
        b"AT" if blank_after => Some(Separator {
            form: Form::Spelled,
            name_end: at,
            domain_start: word.end + 1,
            kept: word.end..word.end,
            core: word,
        }),
        b"at" => {
            let mut line_end = word.end + count(&bytes[word.end..], |b| b == b' ');
            line_end += usize::from(bytes.get(line_end) == Some(&b'\r'));
            if bytes.get(line_end) == Some(&b'\n') {
                let domain_start = line_end + 1 + count(&bytes[line_end + 1..], is_quote_mark);
                Some(Separator {
                    form: Form::At,
                    name_end: at,
                    domain_start,
                    kept: word.end..domain_start,
                    core: word,
                })
            } else if blank_after {
                Some(Separator {
                    form: Form::At,
                    name_end: at,
                    domain_start: word.end + 1,
                    kept: word.end..word.end,
                    core: word,
                })
            } else {
                None
            }
        }
        _ => None,
    }
}

/// `at ` at the start of the line that follows the line end at `at`, after
/// that line's quote marks. The mailbox name then ends the line before:
///
/// ```text
/// ... Jane Doe <jane.doe
/// > at uni.edu> wrote:
/// ```
fn separator_after_line_end(text: &str, at: usize, floor: usize) -> Option<Separator> {
    let bytes = text.as_bytes();
    let word = at + 1 + count(&bytes[at + 1..], is_quote_mark);
    if !bytes[word..].starts_with(b"at ") {
        return None;
    }
    // Blanks and a carriage return before the line end belong to it.
    let mut name_end = at;
    name_end -= usize::from(name_end > floor && bytes[name_end - 1] == b'\r');
    while name_end > floor && bytes[name_end - 1] == b' ' {
        name_end -= 1;
    }
    Some(Separator {
        form: Form::At,
        name_end,
        domain_start: word + 3,
        kept: name_end..word,
        core: word..word + 2,
    })
}

/// How many bytes at the start of `bytes` pass `test`.
fn count(bytes: &[u8], test: impl Fn(u8) -> bool) -> usize {
    bytes.iter().take_while(|byte| test(**byte)).count()
}

/// True for what a mail program starts a quoted line with.
fn is_quote_mark(byte: u8) -> bool {
    matches!(byte, b'>' | b'|' | b' ')
}

/// `(at)`, `[at]`, `<at>`, `{at}` or `--at--` at `at`, in any letter case,
/// with or without a blank on either side.
fn spelled_separator(text: &str, at: usize, floor: usize) -> Option<Separator> {
    let len = spelled_at(text, at)?;
    Some(Separator::padded(text, Form::Spelled, at..at + len, floor))
}

/// The length of `(at)`, `[at]`, `<at>`, `{at}` or `--at--` at `at`.
fn spelled_at(text: &str, at: usize) -> Option<usize> {
    let is_at = |a: &u8, t: &u8| a.eq_ignore_ascii_case(&b'a') && t.eq_ignore_ascii_case(&b't');
    match &text.as_bytes()[at..] {
        [b'(', a, t, b')', ..]
        | [b'[', a, t, b']', ..]
        | [b'<', a, t, b'>', ..]
        | [b'{', a, t, b'}', ..]
            if is_at(a, t) =>
        {
            Some(4)
        }
        [b'-', b'-', a, t, b'-', b'-', ..] if is_at(a, t) => Some(6),
        _ => None,
    }
}

/// The length of the lossy separator whose own text starts at the byte
/// `at`, if one does.
fn lossy_at(text: &str, at: usize) -> Option<usize> {
    let rest = &text.as_bytes()[at..];
    if !STARTS_LOSSY[usize::from(*rest.first()?)] {
        return None;
    }
    let separator = LOSSY
        .iter()
        .find(|known| rest.starts_with(known.as_bytes()))?;
    Some(separator.len())
}

/// Every address of `text` in the lossy form, from the start of its name to
/// the end of its domain, in the order they stand in.
fn lossy_addresses(text: &str) -> Vec<Range<usize>> {
    let mut found = Vec::new();
    let mut floor = 0;
    let mut at = 0;
    while at < text.len() {
        let Some(len) = lossy_at(text, at) else {
            at += 1;
            continue;
        };
        let separator = Separator::padded(text, Form::Lossy, at..at + len, floor);
        let name = mailbox_before(text, &separator, floor);
        let domain = domain_after(text, separator.domain_start, Form::Lossy).range;
        floor = domain.end.max(separator.core.end);
        found.push(name.start..floor);
        at = floor;
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
/// ` at ` is a sentence as often as an address, so both parts must be there,
/// and the same is asked of its other spellings. No sentence has the words
/// of a lossy separator in it, so whatever stands on either side of one is
/// hidden with it, be it only a name or only a domain.
fn address(text: &str, separator: &Separator, floor: usize) -> Option<Address> {
    let form = separator.form;
    let mut found = domain_after(text, separator.domain_start, form);
    // The lossy form has few full stops left: it writes most as `@`.
    if form != Form::Lossy && !found.named {
        return None;
    }
    let mut mailbox = mailbox_before(text, separator, floor);
    if mailbox.is_empty() && matches!(form, Form::Sign | Form::Link) && found.spelled {
        // After a sign with no name before it `dot` is a word, and the
        // domain is what stands before that word.
        found = plain_domain_after(text, separator.domain_start, form);
        if !found.named {
            return None;
        }
    }
    let domain = found.range;
    let is_one = match form {
        Form::At if !separator.kept.is_empty() => {
            !mailbox.is_empty() && looks_like_one(text, &mailbox, &domain)
        }
        Form::At | Form::Spelled => !mailbox.is_empty(),
        // A sign with nothing before it is a slot of an R object or a
        // handle on a social network far more often than the rest of an
        // address. Where something stands before it that ends a name, a
        // closing quotation mark as in `"jane doe"@uni.edu`, the name is
        // one this module cannot read, and the domain is hidden. The same
        // is done after an escaped sign, which only a link has.
        Form::Sign => !mailbox.is_empty() || !stands_alone(text, separator),
        Form::Link => true,
        Form::Lossy => !(mailbox.is_empty() && domain.is_empty()),
    };
    if !is_one {
        return None;
    }
    let mut name_spelled = false;
    if found.spelled {
        // `jane dot doe at uni dot edu`: where the domain is spelled out,
        // the name may be too, and its parts belong to it.
        while let Some(len) = spelled_dot_before(text, mailbox.start, floor) {
            let part = name_before(text, mailbox.start - len, floor, form);
            if part.is_empty() {
                break;
            }
            mailbox.start = part.start;
            name_spelled = true;
        }
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
    // With no name before it, what the separator keeps is not hidden at
    // all, and is not written a second time.
    let kept = if mailbox.is_empty() {
        start..start
    } else {
        separator.kept.clone()
    };
    Some(Address {
        form,
        hidden: start..end,
        kept,
        mailbox,
        domain,
        separator_end: separator.domain_start,
        name_spelled,
    })
}

/// True when a name and a domain on two lines look like an address and not
/// like a sentence that happens to break at `at`: "I took a quick stab at"
/// and "droplevels.data.table" on the next line are no address. They do
/// when the name holds a full stop, or stands directly after an opening
/// bracket or `mailto:`, or when the domain stands directly before a
/// closing bracket, as in "Jane Doe <jane.doe at" and "> uni.edu> wrote:".
fn looks_like_one(text: &str, mailbox: &Range<usize>, domain: &Range<usize>) -> bool {
    let before = &text[..mailbox.start];
    text[mailbox.clone()].contains('.')
        || before.ends_with(['<', '(', '['])
        || before.ends_with("mailto:")
        || text[domain.end..].starts_with(['>', ')', ']'])
}

/// True when the sign of `separator` has nothing before it that a name
/// could have ended with: it stands at the start of the text or of a line,
/// after a blank, or after an opening bracket or quotation mark. It does
/// not stand alone at the start of a line that a mail program has wrapped,
/// which the `=` at the end of the line before tells.
fn stands_alone(text: &str, separator: &Separator) -> bool {
    let opens = |c: char| {
        c.is_whitespace() || "([{<\u{ab}\u{2018}\u{201a}\u{201c}\u{201e}\u{2039}".contains(c)
    };
    if !separator.kept.is_empty() {
        return false;
    }
    let mut before = text[..separator.core.start].chars().rev();
    match before.next() {
        None => true,
        Some(c) if opens(c) => true,
        // These open a quotation and close one. Here it is opened when the
        // mark itself stands where a word may start.
        Some('"' | '\'' | '`') => before.next().is_none_or(opens),
        Some(_) => false,
    }
}

/// What follows a hidden address that ends at `end`, when the text goes on
/// there with a separator and a domain: the end of that domain, and the
/// part of the separator that stays in the text, a line end if there is one.
fn tail(text: &str, end: usize) -> Option<(Range<usize>, usize)> {
    let bytes = text.as_bytes();
    // A separator may start with its blank at `end`, or after one blank,
    // or at the line end after the blanks that end this line.
    let line_end = end + count(&bytes[end..], |byte| matches!(byte, b' ' | b'\r'));
    [end, end + 1, line_end].into_iter().find_map(|at| {
        let separator = (at < bytes.len())
            .then(|| separator_at(text, at, end))
            .flatten()
            .filter(|separator| separator.name_end == end)?;
        let domain = domain_after(text, separator.domain_start, separator.form);
        let follows = match separator.form {
            Form::Lossy => !domain.range.is_empty(),
            _ => domain.named,
        };
        follows.then(|| (separator.kept, domain.range.end))
    })
}

/// True for an address that is left as it is.
fn stays(text: &str, found: &Address, list: &str) -> bool {
    let lossy = found.form == Form::Lossy;
    let mailbox = &text[found.mailbox.clone()];
    let domain = &text[found.domain.clone()];
    // An R mailing list's own address. A name with a full stop in it is
    // never one, nor is one with any of three signs: a list's bounce
    // address carries a member's address after those, as in
    // `list-bounces+jane.doe=uni.edu`. In a link such a sign ends the name,
    // so there a name that follows one, or an escaped one, is not a list's
    // either.
    let follows_a_sign = found.form == Form::Link
        && (text[..found.mailbox.start].ends_with(['+', '=']) || follows_escape(text, found));
    let of_a_list = !mailbox.contains(['+', '=', '%', '.'])
        && !follows_a_sign
        && LIST_PREFIXES
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
        || is_path(text, found.domain.end) && !(mailbox.contains('.') || found.name_spelled)
        || ORDINARY_WORDS
            .iter()
            .any(|word| mailbox.eq_ignore_ascii_case(word))
}

/// True when the name of an address in a link stands directly after an
/// escaped character, as in `%2Bjane.doe%40`, or after one escaped twice,
/// as in `%252Bjane.doe%2540`.
fn follows_escape(text: &str, found: &Address) -> bool {
    let before = &text.as_bytes()[..found.mailbox.start];
    [2, 4].into_iter().any(|digits| {
        before.len() > digits
            && before[before.len() - digits - 1] == b'%'
            && before[before.len() - digits..]
                .iter()
                .all(u8::is_ascii_hexdigit)
    })
}

/// True when a web address goes on at `at` with a slash and its path. It
/// does not when an address stands in what follows the slash up to the next
/// blank, or starts there: in `a.b at x.org/c.d at y.org` the slash parts
/// two addresses. A separator with no domain after it is a word of the
/// sentence: "the code at github.com/user/repo at the moment".
fn is_path(text: &str, at: usize) -> bool {
    let Some(path) = text[at..].strip_prefix('/') else {
        return false;
    };
    let start = at + 1;
    let blank = start + path.find(char::is_whitespace).unwrap_or(path.len());
    !(start..=blank).any(|at| {
        at < text.len()
            && separator_at(text, at, start)
                .is_some_and(|separator| address(text, &separator, start).is_some())
    })
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

/// The mailbox name before `separator`, which starts no earlier than
/// `floor`.
fn mailbox_before(text: &str, separator: &Separator, floor: usize) -> Range<usize> {
    let name = name_before(text, separator.name_end, floor, separator.form);
    if separator.form != Form::Link {
        return name;
    }
    // In a link the name may follow an escaped character, as in
    // `%7Cjane.doe%40`, and the digits of that escape were read as part of
    // it. There are two of them, or four where the link is inside a link
    // (`%257C`): as many as the escaped sign has, or else two.
    let bytes = text.as_bytes();
    let run = run_before(text, name.end, floor, separator.form);
    if run == floor || bytes[run - 1] != b'%' {
        return name;
    }
    let digits = [separator.core.len() - 1, 2].into_iter().find(|digits| {
        bytes[run..name.end]
            .get(..*digits)
            .is_some_and(|escape| escape.iter().all(u8::is_ascii_hexdigit))
    });
    match digits {
        Some(digits) => trimmed(text, run + digits..name.end, separator.form),
        None => name,
    }
}

/// The name that ends at `end`: the run of mailbox characters that ends
/// there and starts no earlier than `floor`, from the first letter or digit
/// in it. A quotation mark or a dash before a name is a mailbox character
/// too, but it is not part of the name. A character that stands for a lost
/// letter is.
fn name_before(text: &str, end: usize, floor: usize, form: Form) -> Range<usize> {
    trimmed(text, run_before(text, end, floor, form)..end, form)
}

/// Where the run of mailbox characters that ends at `end` starts.
fn run_before(text: &str, end: usize, floor: usize, form: Form) -> usize {
    let mut start = end;
    for (at, c) in text[floor..end].char_indices().rev() {
        if !is_mailbox_char(c, form) {
            break;
        }
        start = floor + at;
    }
    start
}

/// `run` from the first character in it that a name may start with.
fn trimmed(text: &str, run: Range<usize>, form: Form) -> Range<usize> {
    let opens =
        |c: char| c.is_alphanumeric() || c == LOST_LETTER || form == Form::Lossy && is_lost(c);
    let name = text[run.clone()].find(opens).unwrap_or(run.len());
    run.start + name..run.end
}

/// True for a character a mailbox name may hold: a letter or a digit of
/// any script, with the marks that combine with it, or one of the signs the
/// mail standards allow in a name. The slash is one of those signs and is
/// left out, because it ends the web address before a name far more often
/// than it is part of a name. In a link, the other signs that a link parts
/// its pieces with are left out as well.
fn is_mailbox_char(c: char, form: Form) -> bool {
    let signs = match form {
        Form::Link => ".!$'*+-^_`{}~",
        _ => ".!#$%&'*+-=?^_`{|}~",
    };
    c.is_alphanumeric() || is_mark(c) || signs.contains(c) || form == Form::Lossy && c == '@'
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
    /// True when a full stop of it is spelled out, as in `uni dot edu`.
    spelled: bool,
}

/// Reads the domain that starts at `start`. A full stop of it may be
/// spelled out.
fn domain_after(text: &str, start: usize, form: Form) -> Domain {
    read_domain(text, start, form, true)
}

/// Reads the domain that starts at `start` up to the first spelled full
/// stop: what follows a sign with no name before it.
fn plain_domain_after(text: &str, start: usize, form: Form) -> Domain {
    read_domain(text, start, form, false)
}

/// Reads the domain that starts at `start`, with the full stops that are
/// spelled out if `spelled_dots` says so.
fn read_domain(text: &str, start: usize, form: Form, spelled_dots: bool) -> Domain {
    let lossy = form == Form::Lossy;
    let mut end = start;
    let mut named = false;
    let mut spelled = false;
    // A letter or a digit has been read.
    let mut begun = false;
    // A full stop has been read after that, and only markup since.
    let mut stopped = false;
    // What `end` and `named` were at the first question mark that no full
    // stop has followed yet.
    let mut question = None;
    let mut at = start;
    while let Some(c) = text[at..].chars().next() {
        let after = at + c.len_utf8();
        if lossy_at(text, at).is_some() {
            // A domain ends where the next lossy separator starts, also
            // where no blank parts the two: `github.comm@ili@g off`.
            break;
        }
        if c.is_alphanumeric() || lossy && is_lost(c) {
            named |= stopped && c.is_alphabetic();
            (begun, stopped, end) = (true, false, after);
            // In the lossy form a `@` is a full stop as often as not.
            if lossy && c == '@' {
                question = None;
            }
        } else if is_mark(c) && end == at {
            end = after;
        } else if c == '.' {
            stopped = begun;
            question = None;
        } else if c == LOST_LETTER && form != Form::Link {
            // It is the letter the source lost when a full stop follows it
            // in the domain, as in `universit?t.de`. When none does it is
            // the sentence's own, or the start of a link's query, which is
            // what it always is in a link.
            question = question.or(Some((end, named)));
            (begun, stopped) = (true, false);
        } else if let Some(len) =
            spelled_dot(text, at).filter(|_| spelled_dots && begun && end == at)
        {
            // `uni dot edu`: the word stands for a full stop where it has a
            // part of the domain on either side.
            if !text[at + len..].starts_with(char::is_alphanumeric) {
                break;
            }
            (stopped, spelled) = (true, true);
            question = None;
            at += len;
            continue;
        } else if !matches!(c, '-' | '_' | '+' | '*') || spelled_at(text, at).is_some() {
            // Mailman takes `_` and `+` for part of a domain, and `*` and
            // `_` are what a mail program puts around emphasised text. The
            // dashes of `--at--` are the next separator and no part of this
            // domain: were they read as one, a line of such separators
            // would be read to its end from each of them.
            break;
        }
        at = after;
    }
    if let Some(before) = question {
        (end, named) = before;
    }
    Domain {
        range: start..end,
        named,
        spelled,
    }
}

/// The length of the spelled full stop that ends at `end`, if one does and
/// starts no earlier than `floor`. It is spelled as `spelled_dot` reads it.
fn spelled_dot_before(text: &str, end: usize, floor: usize) -> Option<usize> {
    let before = &text.as_bytes()[floor..end];
    if before.ends_with(b" dot ") || before.ends_with(b" DOT ") {
        return Some(5);
    }
    let after = usize::from(before.last() == Some(&b' '));
    match &before[..before.len() - after] {
        [word @ .., b'(', d, o, t, b')'] | [word @ .., b'[', d, o, t, b']']
            if [*d, *o, *t].eq_ignore_ascii_case(b"dot") =>
        {
            Some(usize::from(word.last() == Some(&b' ')) + 5 + after)
        }
        _ => None,
    }
}

/// The length of ` dot `, ` DOT `, `(dot)` or `[dot]` at `at`, the two in
/// brackets in any letter case and with or without a blank on either side.
fn spelled_dot(text: &str, at: usize) -> Option<usize> {
    let rest = &text.as_bytes()[at..];
    if rest.starts_with(b" dot ") || rest.starts_with(b" DOT ") {
        return Some(5);
    }
    let before = usize::from(rest.first() == Some(&b' '));
    match &rest[before..] {
        [b'(', d, o, t, b')', after @ ..] | [b'[', d, o, t, b']', after @ ..]
            if [*d, *o, *t].eq_ignore_ascii_case(b"dot") =>
        {
            Some(before + 5 + usize::from(after.first() == Some(&b' ')))
        }
        _ => None,
    }
}

/// True for the two characters the lossy form puts where it lost one.
fn is_lost(c: char) -> bool {
    matches!(c, '@' | '|')
}
