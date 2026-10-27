/// One message page at the source, reduced to what the parser uses.
#[derive(Debug, Clone, PartialEq)]
pub struct ArticlePage {
    /// The message text as the page shows it: tags removed, entities decoded.
    pub text: String,
    /// The Message-ID the page's reply link names, without angle brackets.
    /// Pages from the 2000s do not name one.
    pub message_id: Option<String>,
}

const BEGIN_ARTICLE: &str = "<!--beginarticle-->";
const END_ARTICLE: &str = "<!--endarticle-->";

/// How many characters either side of a `?` are compared with the page.
const CONTEXT: usize = 6;

/// A context needs this many characters that are neither `?` nor whitespace
/// before finding it in the page means anything.
const MIN_ANCHORS: usize = 4;

/// Reads a message page. `None` when the article markers are missing, which
/// means the response is not a message page at all.
pub fn parse_article(html: &str) -> Option<ArticlePage> {
    let start = html.find(BEGIN_ARTICLE)? + BEGIN_ARTICLE.len();
    let end = start + html[start..].find(END_ARTICLE)?;
    let text = decode_entities(&strip_tags(&html[start..end]));
    Some(ArticlePage {
        text: text.trim().to_string(),
        message_id: page_message_id(html),
    })
}

/// Decodes numeric character references, decimal (`&#8217;`) and hexadecimal
/// (`&#xE4;`), and the six named ones pipermail writes: `lt`, `gt`, `amp`,
/// `quot`, `apos` and `nbsp`. Each has to end in `;`. Anything else that
/// starts with `&` is left as it is.
///
/// A number from 128 to 159 is read as the HTML standard reads it, as the
/// character Windows-1252 has at that byte: `&#146;` is U+2019, the right
/// single quote a browser shows for it. The five bytes Windows-1252 does not
/// use (129, 141, 143, 144 and 157) stay the control characters U+0081 and
/// so on. Zero, half of a surrogate pair and a number past U+10FFFF become
/// U+FFFD. Every other number is the character with that code point.
pub fn decode_entities(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(amp) = rest.find('&') {
        out.push_str(&rest[..amp]);
        rest = &rest[amp..];
        match decode_entity(rest) {
            Some((ch, len)) => {
                out.push(ch);
                rest = &rest[len..];
            }
            None => {
                out.push('&');
                rest = &rest[1..];
            }
        }
    }
    out.push_str(rest);
    out
}

/// Decodes the entity `s` starts with: its character, and its length in bytes.
fn decode_entity(s: &str) -> Option<(char, usize)> {
    // The longest entity accepted is `&#x10FFFF;`, so the `;` is close by.
    let end = s.bytes().take(12).position(|b| b == b';')?;
    let name = &s[1..end];
    let ch = if let Some(hex) = name.strip_prefix("#x").or_else(|| name.strip_prefix("#X")) {
        char_from_code(u32::from_str_radix(hex, 16).ok()?)
    } else if let Some(decimal) = name.strip_prefix('#') {
        char_from_code(decimal.parse().ok()?)
    } else {
        match name {
            "lt" => '<',
            "gt" => '>',
            "amp" => '&',
            "quot" => '"',
            "apos" => '\'',
            "nbsp" => '\u{a0}',
            _ => return None,
        }
    };
    Some((ch, end + 1))
}

/// The character a numeric reference names. Mailman writes the code point
/// Python decoded, and mail labelled ISO-8859-1 that holds Windows-1252
/// quotes and dashes decodes to 128 to 159. Those are read as the byte they
/// stand for, by the table the HTML standard gives.
fn char_from_code(code: u32) -> char {
    match code {
        0x80 => '\u{20ac}',
        0x82 => '\u{201a}',
        0x83 => '\u{192}',
        0x84 => '\u{201e}',
        0x85 => '\u{2026}',
        0x86 => '\u{2020}',
        0x87 => '\u{2021}',
        0x88 => '\u{2c6}',
        0x89 => '\u{2030}',
        0x8a => '\u{160}',
        0x8b => '\u{2039}',
        0x8c => '\u{152}',
        0x8e => '\u{17d}',
        0x91 => '\u{2018}',
        0x92 => '\u{2019}',
        0x93 => '\u{201c}',
        0x94 => '\u{201d}',
        0x95 => '\u{2022}',
        0x96 => '\u{2013}',
        0x97 => '\u{2014}',
        0x98 => '\u{2dc}',
        0x99 => '\u{2122}',
        0x9a => '\u{161}',
        0x9b => '\u{203a}',
        0x9c => '\u{153}',
        0x9e => '\u{17e}',
        0x9f => '\u{178}',
        _ => match char::from_u32(code) {
            Some(ch) if ch != '\0' => ch,
            _ => '\u{fffd}',
        },
    }
}

/// Drops everything between `<` and `>`. Pipermail escapes both characters in
/// message text, so a bare `<` is always its own markup.
fn strip_tags(html: &str) -> String {
    let mut out = String::with_capacity(html.len());
    let mut in_tag = false;
    for ch in html.chars() {
        match ch {
            '<' => in_tag = true,
            '>' if in_tag => in_tag = false,
            _ if !in_tag => out.push(ch),
            _ => {}
        }
    }
    out
}

/// The Message-ID in the `In-Reply-To=` parameter of the page's reply link.
fn page_message_id(html: &str) -> Option<String> {
    const PARAM: &str = "In-Reply-To=";
    let start = html.find(PARAM)? + PARAM.len();
    let rest = &html[start..];
    let end = rest
        .find(|c: char| c == '"' || c == '&' || c == '>' || c.is_whitespace())
        .unwrap_or(rest.len());
    let decoded = percent_decode(&rest[..end]);
    let id = decoded.trim().trim_start_matches('<').trim_end_matches('>');
    if id.is_empty() {
        None
    } else {
        Some(id.to_string())
    }
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = if bytes[i] == b'%' {
            s.get(i + 1..i + 3)
        } else {
            None
        };
        match hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
            Some(byte) => {
                out.push(byte);
                i += 3;
            }
            None => {
                out.push(bytes[i]);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Works out what each `?` in `body` should be, from the page's text. The
/// result has one character per `?`, in order: the character to restore, or
/// `?` to leave it alone. A page character is taken only when it is not
/// ASCII, not U+FFFD and not a control character.
///
/// The two texts are not compared position by position, because the page and
/// the text file obscure addresses differently. Each `?` is looked up by the
/// characters around it instead, where every `?` in that context matches
/// anything. The search only moves forward through the page, so a phrase
/// that occurs twice is matched in order.
pub fn fix_string(body: &str, page_text: &str) -> String {
    let body: Vec<char> = body.chars().collect();
    let page: Vec<char> = page_text.chars().collect();
    let mut fix = String::new();
    // The earliest place in the page the next `?` may sit.
    let mut cursor = 0;

    for pos in (0..body.len()).filter(|&i| body[i] == '?') {
        let left = &body[pos.saturating_sub(CONTEXT)..pos];
        let right = &body[pos + 1..(pos + 1 + CONTEXT).min(body.len())];
        let found = find_spot(&page, cursor, left, right);
        if let Some(spot) = found {
            cursor = spot + 1;
        }
        match found.map(|spot| page[spot]) {
            // U+FFFD means the source lost the character too, and a control
            // character is nothing a reader of the page sees.
            Some(ch) if !ch.is_ascii() && ch != '\u{fffd}' && !ch.is_control() => fix.push(ch),
            _ => fix.push('?'),
        }
    }
    fix
}

/// Finds the first place at or after `from` where the page has `left` just
/// before it and `right` just after it.
fn find_spot(page: &[char], from: usize, left: &[char], right: &[char]) -> Option<usize> {
    let anchors = left
        .iter()
        .chain(right)
        .filter(|c| **c != '?' && !c.is_whitespace())
        .count();
    if anchors < MIN_ANCHORS {
        return None;
    }
    let fits =
        |want: &[char], have: &[char]| want.iter().zip(have).all(|(w, h)| *w == '?' || w == h);
    let first = from.max(left.len());
    let last = page.len().checked_sub(right.len() + 1)?;
    (first..=last).find(|&spot| {
        fits(left, &page[spot - left.len()..spot])
            && fits(right, &page[spot + 1..spot + 1 + right.len()])
    })
}

/// How long a line of a body has to be, spaces folded, to count in
/// `page_shows`: shorter ones ("Thanks,", a signature) are in many messages.
const SHOWN_LINE: usize = 20;

/// The most lines of a body `page_shows` looks for.
const SHOWN_LINES: usize = 10;

/// Whether `page_text` shows the message whose body is `body`: at least half
/// of the body's own lines of `SHOWN_LINE` characters or more (quoted lines
/// left out, at most `SHOWN_LINES` of them) are in the page, with every `?`
/// of the body matching any one character. A body with no such line is
/// looked for whole, if it has that many characters. The page and the text
/// file write addresses differently, which is why half the lines and not all
/// of them are asked for.
///
/// A page archived before about 2008 names in its reply link the Message-ID
/// the message answers, not its own, so it cannot say whose it is. Its text
/// can.
pub fn page_shows(body: &str, page_text: &str) -> bool {
    let fold = |s: &str| s.split_whitespace().collect::<Vec<_>>().join(" ");
    let page: Vec<char> = fold(page_text).chars().collect();
    let mut lines: Vec<Vec<char>> = body
        .lines()
        .filter(|line| !line.trim_start().starts_with('>'))
        .map(fold)
        .filter(|line| line.chars().count() >= SHOWN_LINE)
        .map(|line| line.chars().collect())
        .take(SHOWN_LINES)
        .collect();
    if lines.is_empty() {
        let whole = fold(body);
        if whole.chars().count() < SHOWN_LINE {
            return false;
        }
        lines.push(whole.chars().collect());
    }
    let found = lines
        .iter()
        .filter(|line| contains_with_wildcards(&page, line))
        .count();
    found * 2 >= lines.len()
}

/// Whether `needle` occurs in `haystack`, a `?` in the needle matching any
/// one character.
fn contains_with_wildcards(haystack: &[char], needle: &[char]) -> bool {
    if needle.len() > haystack.len() {
        return false;
    }
    (0..=haystack.len() - needle.len()).any(|start| {
        needle
            .iter()
            .zip(&haystack[start..])
            .all(|(n, h)| *n == '?' || n == h)
    })
}
