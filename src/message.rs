use anyhow::{Context, Result};
use chrono::{DateTime, FixedOffset};
use mailparse::{parse_mail, MailHeaderMap};
use regex::Regex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Serialize DateTime<FixedOffset> as UTC RFC 3339 so that lexicographic
/// string ordering matches chronological ordering (avoids timezone-offset
/// comparison bugs in SQLite ORDER BY and PHP strtotime).
mod date_as_utc {
    use chrono::{DateTime, FixedOffset, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(date: &DateTime<FixedOffset>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let utc: DateTime<Utc> = date.with_timezone(&Utc);
        serializer.serialize_str(&utc.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<FixedOffset>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        DateTime::parse_from_rfc3339(&s).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: String,                    // "msg-<12-char-sha256-of-message_id>"
    pub message_id: String,            // original RFC 2822 Message-ID
    pub from_name: String,             // display name extracted from From header
    pub from_email_hash: String,       // sha256 of de-obfuscated email
    #[serde(with = "date_as_utc")]
    pub date: DateTime<FixedOffset>,   // parsed date (serialized as UTC)
    pub subject: String,               // original subject
    pub subject_clean: String,         // subject with [R], Re:, Fwd: stripped
    pub in_reply_to: Option<String>,   // parent message ID
    pub references: Vec<String>,       // ancestor message IDs
    pub body_plain: String,            // message body
    pub body_snippet: String,          // first 200 chars
    pub thread_id: String,             // empty - filled later by threading
    pub thread_depth: u32,             // 0 - filled later by threading
    pub month: String,                 // "YYYY-MM" from date
}

/// Reverses Mailman's pipermail email obfuscation.
///
/// Mailman obfuscates emails using the pattern:
///   `local_obfuscated @end|ng |rom domain_obfuscated`
///
/// In the local part: `|` and `@` represent removed characters (stripped out).
/// In the domain part: `@` represents `.`, `|` represents removed characters.
///
/// If the input doesn't contain the ` @end|ng |rom ` separator, it's returned as-is.
pub fn deobfuscate_email(obfuscated: &str) -> String {
    let separator = " @end|ng |rom ";
    if let Some(sep_pos) = obfuscated.find(separator) {
        let local = &obfuscated[..sep_pos];
        let domain = &obfuscated[sep_pos + separator.len()..];

        // Local part: remove | and @ (they represent removed/obfuscated chars)
        let clean_local: String = local.chars().filter(|c| *c != '|' && *c != '@').collect();

        // Domain part: replace @ with . (dots were obfuscated as @), remove |
        let clean_domain: String = domain
            .chars()
            .filter_map(|c| match c {
                '@' => Some('.'),
                '|' => None,
                _ => Some(c),
            })
            .collect();

        // Collapse consecutive dots and trim leading/trailing dots (artifacts of obfuscation)
        let clean_domain = collapse_dots(&clean_domain);
        let clean_domain = clean_domain.trim_matches('.');

        format!("{}@{}", clean_local, clean_domain)
    } else {
        // Not obfuscated, return as-is
        obfuscated.to_string()
    }
}

/// SHA-256 hash of a (de-obfuscated) email address.
pub fn hash_email(email: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(email.as_bytes());
    let result = hasher.finalize();
    hex::encode(result)
}

/// Strips list prefixes like [R], [Rd], [R-pkgs], and Re:/Fwd: markers from a subject line.
pub fn clean_subject(subject: &str) -> String {
    let re = Regex::new(r"(?i)^(\s*(Re:\s*|Fwd?:\s*|\[R(-pkgs|-sig-\w+|d)?\]\s*))+").unwrap();
    re.replace(subject, "").trim().to_string()
}

/// Extracts the display name from a From header value.
///
/// Handles formats:
///   - "Name <email>" -> "Name"
///   - "email (Name)" -> "Name"
///   - "email" -> "email" (fallback)
pub fn extract_name(from: &str) -> String {
    let from = from.trim();

    // "Name <email>" format
    if let Some(angle_pos) = from.find('<') {
        let name = from[..angle_pos].trim();
        if !name.is_empty() {
            return name.to_string();
        }
    }

    // "email (Name)" format
    if let (Some(paren_open), Some(paren_close)) = (from.find('('), from.rfind(')')) {
        if paren_open < paren_close {
            let name = from[paren_open + 1..paren_close].trim();
            if !name.is_empty() {
                return name.to_string();
            }
        }
    }

    // Fallback: return the whole string
    from.to_string()
}

/// Extracts the obfuscated email from a From header and de-obfuscates it.
///
/// Handles:
///   - "Name <obfuscated_email>" -> extracts between angle brackets
///   - "email (Name)" -> extracts the email before the parens
///   - plain email
pub fn extract_email_for_hash(from: &str) -> String {
    let from = from.trim();

    // "Name <email>" format
    if let (Some(start), Some(end)) = (from.find('<'), from.find('>')) {
        if start < end {
            let email_part = from[start + 1..end].trim();
            return deobfuscate_email(email_part);
        }
    }

    // "email (Name)" format
    if let Some(paren_pos) = from.find('(') {
        let email_part = from[..paren_pos].trim();
        return deobfuscate_email(email_part);
    }

    // Plain email
    deobfuscate_email(from)
}

/// Parses a raw email string (from the mbox splitter) into a structured Message.
///
/// The raw string may start with a "From " mbox envelope line, which is skipped.
/// Uses the `mailparse` crate for RFC 2822 header parsing.
pub fn parse_message(raw: &str) -> Result<Message> {
    // Skip the mbox "From " separator line if present
    let mail_content = if raw.starts_with("From ") {
        // Find the end of the first line
        match raw.find('\n') {
            Some(pos) => &raw[pos + 1..],
            None => return Err(anyhow::anyhow!("Message contains only a From line")),
        }
    } else {
        raw
    };

    let parsed = parse_mail(mail_content.as_bytes())
        .context("Failed to parse email")?;

    let headers = &parsed.headers;

    // Extract headers
    let from_header = headers
        .get_first_value("From")
        .unwrap_or_default();

    let message_id = headers
        .get_first_value("Message-ID")
        .or_else(|| headers.get_first_value("Message-Id"))
        .unwrap_or_default();

    let date_str = headers
        .get_first_value("Date")
        .unwrap_or_default();

    let subject = headers
        .get_first_value("Subject")
        .unwrap_or_default();

    let in_reply_to = headers.get_first_value("In-Reply-To");

    let references_header = headers
        .get_first_value("References")
        .unwrap_or_default();

    // Parse references: split on whitespace, filter for message IDs (contain angle brackets)
    let references: Vec<String> = if references_header.is_empty() {
        Vec::new()
    } else {
        parse_message_id_list(&references_header)
    };

    // Parse date
    let date = parse_date(&date_str)
        .context(format!("Failed to parse date: {}", date_str))?;

    // Extract name and email hash from From header
    let from_name = extract_name(&from_header);
    let email = extract_email_for_hash(&from_header);
    let from_email_hash = hash_email(&email);

    // Clean subject
    let subject_clean = clean_subject(&subject);

    // Generate message ID hash
    let id = generate_message_id(&message_id);

    // Extract body
    let body_plain = parsed
        .get_body()
        .unwrap_or_default()
        .trim()
        .to_string();

    // Generate snippet (first 200 characters, UTF-8 safe)
    let body_snippet: String = body_plain.chars().take(200).collect();

    // Extract month
    let month = date.format("%Y-%m").to_string();

    Ok(Message {
        id,
        message_id,
        from_name,
        from_email_hash,
        date,
        subject,
        subject_clean,
        in_reply_to,
        references,
        body_plain,
        body_snippet,
        thread_id: String::new(),
        thread_depth: 0,
        month,
    })
}

/// Generates a message ID by hashing the RFC 2822 Message-ID with SHA-256
/// and taking the first 12 hex characters.
fn generate_message_id(message_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(message_id.as_bytes());
    let result = hasher.finalize();
    let hex = hex::encode(result);
    format!("msg-{}", &hex[..12])
}

/// Parses a date string, trying RFC 2822 format first, then common alternatives.
fn parse_date(date_str: &str) -> Result<DateTime<FixedOffset>> {
    // Try RFC 2822 first
    if let Ok(dt) = DateTime::parse_from_rfc2822(date_str) {
        return Ok(dt);
    }

    // Try common alternative formats
    // Some emails have extra whitespace or slightly non-standard formats
    let cleaned = date_str.trim();

    // Try with extra whitespace removed (double spaces in day)
    let normalized = cleaned
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if let Ok(dt) = DateTime::parse_from_rfc2822(&normalized) {
        return Ok(dt);
    }

    // Try common strftime formats (with timezone)
    let formats = [
        "%a, %d %b %Y %H:%M:%S %z",
        "%d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S %Z",
    ];

    for fmt in &formats {
        if let Ok(dt) = DateTime::parse_from_str(cleaned, fmt) {
            return Ok(dt);
        }
        if let Ok(dt) = DateTime::parse_from_str(&normalized, fmt) {
            return Ok(dt);
        }
    }

    // Try formats without timezone — assume UTC
    use chrono::NaiveDateTime;
    let naive_formats = [
        // asctime/ctime: "Thu Jan  2 13:54:37 2003"
        "%a %b %e %H:%M:%S %Y",
        "%a %b %d %H:%M:%S %Y",
        // RFC 2822 without timezone: "Mon, 13 Jul 1998 12:47:53"
        "%a, %d %b %Y %H:%M:%S",
        // DD-Mon-YYYY: "04-Dec-2008 17:34:26 GMT"
        "%d-%b-%Y %H:%M:%S GMT",
        "%d-%b-%Y %H:%M:%S",
    ];
    for fmt in &naive_formats {
        if let Ok(naive) = NaiveDateTime::parse_from_str(&normalized, fmt) {
            return Ok(naive.and_utc().fixed_offset());
        }
        if let Ok(naive) = NaiveDateTime::parse_from_str(cleaned, fmt) {
            return Ok(naive.and_utc().fixed_offset());
        }
    }

    // Try 2-digit year formats: "Tue, 5 Aug 97 10:54:36 BST"
    // Strip named timezone suffixes (BST, EST, CST, etc.) and parse with 2-digit year
    let tz_stripped = regex::Regex::new(r"\s+[A-Z]{2,5}\s*$")
        .unwrap()
        .replace(&normalized, "")
        .to_string();
    let two_digit_formats = [
        "%a, %d %b %y %H:%M:%S",
        "%d %b %y %H:%M:%S",
    ];
    for fmt in &two_digit_formats {
        if let Ok(naive) = NaiveDateTime::parse_from_str(&tz_stripped, fmt) {
            return Ok(naive.and_utc().fixed_offset());
        }
        if let Ok(naive) = NaiveDateTime::parse_from_str(&normalized, fmt) {
            return Ok(naive.and_utc().fixed_offset());
        }
    }

    Err(anyhow::anyhow!("Unable to parse date: {}", date_str))
}

/// Collapses consecutive dots into a single dot.
fn collapse_dots(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut prev_dot = false;
    for ch in s.chars() {
        if ch == '.' {
            if !prev_dot {
                result.push('.');
            }
            prev_dot = true;
        } else {
            result.push(ch);
            prev_dot = false;
        }
    }
    result
}

/// Parses a space-separated list of message IDs (as found in References header).
/// Message IDs are enclosed in angle brackets: <id@domain>
fn parse_message_id_list(header_value: &str) -> Vec<String> {
    let mut ids = Vec::new();
    let mut current_id = String::new();
    let mut in_angle = false;

    for ch in header_value.chars() {
        match ch {
            '<' => {
                in_angle = true;
                current_id.push(ch);
            }
            '>' => {
                current_id.push(ch);
                if in_angle {
                    ids.push(current_id.clone());
                    current_id.clear();
                    in_angle = false;
                }
            }
            _ => {
                if in_angle {
                    current_id.push(ch);
                }
            }
        }
    }

    ids
}
