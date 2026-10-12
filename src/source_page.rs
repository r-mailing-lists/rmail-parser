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

/// Decodes numeric entities (`&#8217;`, `&#xE4;`) and the six named ones
/// pipermail writes. Anything else that starts with `&` is left as it is.
/// A number that is not a character becomes U+FFFD, as it does in a browser.
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

fn char_from_code(code: u32) -> char {
    match char::from_u32(code) {
        Some(ch) if ch != '\0' => ch,
        _ => '\u{fffd}',
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
/// `?` to leave it alone.
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
            // U+FFFD means the source lost the character too.
            Some(ch) if !ch.is_ascii() && ch != '\u{fffd}' => fix.push(ch),
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
