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

use crate::message::{make_snippet, Message};

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
    // first word, `Re:` and `[R]`, so an address is hidden in it as it is
    // in the subject. Cleaning the field itself does not rest on that, and
    // spares `clean_subject`, which compiles its pattern on every call.
    msg.subject_clean = hide_in_text(&msg.subject_clean, list);
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
    match text.as_bytes()[at] {
        b' ' => word_separator(text, at),
        b'\n' => separator_after_line_end(text, at, floor),
        b'@' | b'm' => lossy_separator(text, at, floor).or_else(|| {
            text[at..]
                .starts_with('@')
                .then(|| Separator::bare(Form::Sign, at..at + 1))
        }),
        b'%' => ESCAPED_SIGNS
            .iter()
            .find(|sign| text[at..].starts_with(*sign))
            .map(|sign| Separator::bare(Form::Link, at..at + sign.len())),
        b'(' | b'[' | b'<' | b'{' | b'-' => spelled_separator(text, at, floor),
        _ => None,
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

/// The lossy separator whose own text starts at `at`. The blank on each
/// side of it may be missing.
fn lossy_separator(text: &str, at: usize, floor: usize) -> Option<Separator> {
    // All four start with `@e` or `m@`. Most characters of a text are told
    // apart from a separator by that alone.
    if !matches!(&text.as_bytes()[at..], [b'@', b'e', ..] | [b'm', b'@', ..]) {
        return None;
    }
    let core = LOSSY.iter().find(|core| text[at..].starts_with(*core))?;
    Some(Separator::padded(
        text,
        Form::Lossy,
        at..at + core.len(),
        floor,
    ))
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
                let name = mailbox_before(text, &separator, floor);
                let domain = domain_after(text, separator.domain_start, Form::Lossy).range;
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
/// ` at ` is a sentence as often as an address, so both parts must be there,
/// and the same is asked of its other spellings. After a sign, written or
/// escaped, the domain is hidden even when no name is read before it: the
/// name may be one this module does not know how to read, as
/// `"jane doe"@uni.edu` is. No sentence has the words of a lossy separator
/// in it, so whatever stands on either side of one is hidden with it, be it
/// only a name or only a domain.
fn address(text: &str, separator: &Separator, floor: usize) -> Option<Address> {
    let form = separator.form;
    let Domain {
        range: domain,
        named,
    } = domain_after(text, separator.domain_start, form);
    // The lossy form has few full stops left: it writes most as `@`.
    if form != Form::Lossy && !named {
        return None;
    }
    let mailbox = mailbox_before(text, separator, floor);
    let missing = match form {
        Form::At | Form::Spelled => mailbox.is_empty(),
        Form::Sign | Form::Link => false,
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
        form,
        hidden: start..end,
        kept: separator.kept.clone(),
        mailbox,
        domain,
        separator_end: separator.domain_start,
    })
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
    // An R mailing list's own address. A name with one of the three signs
    // in it is never one: a list's bounce address carries a member's
    // address there, as in `list-bounces+jane.doe=uni.edu`.
    let of_a_list = !mailbox.contains(['+', '=', '%'])
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
        || is_path(text, found.domain.end) && !mailbox.contains('.')
        || ORDINARY_WORDS
            .iter()
            .any(|word| mailbox.eq_ignore_ascii_case(word))
}

/// True when a web address goes on at `at` with a slash and its path. It
/// does not when a separator stands in what follows the slash up to the
/// next blank, or right after that: in `a.b at x.org/c.d at y.org` the
/// slash parts two addresses.
fn is_path(text: &str, at: usize) -> bool {
    let Some(path) = text[at..].strip_prefix('/') else {
        return false;
    };
    let blank = at + 1 + path.find(char::is_whitespace).unwrap_or(path.len());
    !(at + 1..=blank).any(|at| at < text.len() && separator_at(text, at, at).is_some())
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

/// The mailbox name before `separator`: the run of mailbox characters that
/// ends there and starts no earlier than `floor`, from the first letter or
/// digit in it. A quotation mark or a dash before a name is a mailbox
/// character too, but it is not part of the name. A character that stands
/// for a lost letter is.
fn mailbox_before(text: &str, separator: &Separator, floor: usize) -> Range<usize> {
    let form = separator.form;
    let end = separator.name_end;
    let mut start = end;
    for (at, c) in text[floor..end].char_indices().rev() {
        if !is_mailbox_char(c, form) {
            break;
        }
        start = floor + at;
    }
    if form == Form::Link && start > floor && text.as_bytes()[start - 1] == b'%' {
        // The name follows an escaped character, as in `%7Cjane.doe%40`,
        // and the digits of that escape are not part of it. There are two
        // of them, or four where the link is inside a link (`%257C`): as
        // many as the escaped sign has.
        let digits = separator.core.len() - 1;
        let escape = text.as_bytes()[start..end].get(..digits);
        if escape.is_some_and(|escape| escape.iter().all(u8::is_ascii_hexdigit)) {
            start += digits;
        }
    }
    let opens =
        |c: char| c.is_alphanumeric() || c == LOST_LETTER || form == Form::Lossy && is_lost(c);
    let name = text[start..end].find(opens).unwrap_or(end - start);
    start + name..end
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
}

/// Reads the domain that starts at `start`.
fn domain_after(text: &str, start: usize, form: Form) -> Domain {
    let lossy = form == Form::Lossy;
    let mut end = start;
    let mut named = false;
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
        } else if let Some(len) = spelled_dot(text, at).filter(|_| end == at) {
            // `uni dot edu`: the word stands for a full stop where it has a
            // part of the domain on either side.
            if !text[at + len..].starts_with(char::is_alphanumeric) {
                break;
            }
            stopped = true;
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
