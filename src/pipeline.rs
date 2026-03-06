use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use encoding_rs::WINDOWS_1252;
use rayon::prelude::*;

use crate::mbox::split_mbox;
use crate::message::{parse_message, Message};
use crate::output::{ListMeta, MonthArchive};
use crate::threading::reconstruct_threads;

/// Reads a file as a UTF-8 string, falling back to Windows-1252 (Latin-1 superset)
/// if the file is not valid UTF-8. This handles old mbox files that contain
/// raw 8-bit characters from various European encodings.
fn read_file_lossy(path: &Path) -> Result<String> {
    let bytes = fs::read(path)
        .with_context(|| format!("Failed to read {}", path.display()))?;

    // Try UTF-8 first (zero-copy if valid)
    match String::from_utf8(bytes) {
        Ok(s) => Ok(s),
        Err(e) => {
            eprintln!(
                "Warning: {} is not valid UTF-8, falling back to Windows-1252",
                path.display()
            );
            let bytes = e.into_bytes();
            let (cow, _encoding, had_errors) = WINDOWS_1252.decode(&bytes);
            if had_errors {
                eprintln!(
                    "Warning: some characters in {} could not be decoded",
                    path.display()
                );
            }
            Ok(cow.into_owned())
        }
    }
}

/// If input is a file, return it in a vec. If a directory, find all `.mbox`
/// and `.txt` files inside it, sorted alphabetically.
pub fn discover_mbox_files(input: &Path) -> Result<Vec<PathBuf>> {
    if input.is_file() {
        return Ok(vec![input.to_path_buf()]);
    }

    if !input.is_dir() {
        anyhow::bail!("Input path does not exist: {}", input.display());
    }

    let mut files: Vec<PathBuf> = fs::read_dir(input)
        .context("Failed to read input directory")?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| {
            path.is_file()
                && matches!(
                    path.extension().and_then(|e| e.to_str()),
                    Some("mbox") | Some("txt")
                )
        })
        .collect();

    files.sort();
    Ok(files)
}

/// Extract a YYYY-MM month string from a filename.
///
/// Handles patterns:
///   - `2026-February.mbox` -> `2026-02`
///   - `2026q1.mbox`        -> `2026-01` (quarter start month)
///   - Unknown              -> file stem as-is
pub fn month_from_filename(path: &Path) -> String {
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown");

    // Try pattern: YYYY-MonthName (e.g. "2026-February")
    if let Some((year, month_name)) = stem.split_once('-') {
        if year.len() == 4 && year.chars().all(|c| c.is_ascii_digit()) {
            if let Some(month_num) = month_name_to_number(month_name) {
                return format!("{}-{:02}", year, month_num);
            }
        }
    }

    // Try pattern: YYYYqN (e.g. "2026q1")
    if stem.len() == 6 {
        let (year_part, q_part) = stem.split_at(4);
        if year_part.chars().all(|c| c.is_ascii_digit()) && q_part.starts_with('q') {
            if let Ok(quarter) = q_part[1..].parse::<u32>() {
                let start_month = match quarter {
                    1 => 1,
                    2 => 4,
                    3 => 7,
                    4 => 10,
                    _ => 1,
                };
                return format!("{}-{:02}", year_part, start_month);
            }
        }
    }

    // Fallback: use stem as-is
    stem.to_string()
}

/// Main parse pipeline.
///
/// - Discovers mbox files from `input`
/// - For each file: reads content, splits into messages, parses them (in parallel
///   via rayon), reconstructs threads, and writes a MonthArchive JSON file.
/// - One JSON file per month: `{output}/{month}.json`
pub fn run_parse(input: &Path, output: &Path, list_name: &str) -> Result<()> {
    let files = discover_mbox_files(input)?;

    if files.is_empty() {
        anyhow::bail!("No mbox files found in {}", input.display());
    }

    fs::create_dir_all(output).context("Failed to create output directory")?;

    for file_path in &files {
        let content = read_file_lossy(file_path)?;

        let raw_messages = split_mbox(&content);

        // Parse messages in parallel using rayon, skip failures
        let mut messages: Vec<Message> = raw_messages
            .par_iter()
            .filter_map(|raw| match parse_message(raw) {
                Ok(msg) => Some(msg),
                Err(e) => {
                    eprintln!(
                        "Warning: skipping message in {}: {}",
                        file_path.display(),
                        e
                    );
                    None
                }
            })
            .collect();

        if messages.is_empty() {
            eprintln!(
                "Warning: no valid messages in {}, skipping",
                file_path.display()
            );
            continue;
        }

        // Deduplicate by message_id (some pipermail archives contain 3x duplicates)
        let pre_dedup = messages.len();
        let mut seen_ids: HashSet<String> = HashSet::new();
        messages.retain(|msg| seen_ids.insert(msg.message_id.clone()));
        if messages.len() < pre_dedup {
            eprintln!(
                "Deduped {}: {} -> {} messages",
                file_path.display(),
                pre_dedup,
                messages.len()
            );
        }

        // Group messages by month
        let mut by_month: HashMap<String, Vec<Message>> = HashMap::new();
        for msg in messages.drain(..) {
            by_month
                .entry(msg.month.clone())
                .or_default()
                .push(msg);
        }

        // If there's only one month group, use the filename-derived month if
        // messages all share the same month anyway. Otherwise respect per-message months.
        // Either way, process each month group.
        for (month, mut month_messages) in by_month {
            // Sort messages by date within the month
            month_messages.sort_by(|a, b| a.date.cmp(&b.date));

            let threads = reconstruct_threads(&mut month_messages);

            let archive = MonthArchive {
                list: list_name.to_string(),
                description: String::new(),
                month: month.clone(),
                messages: month_messages,
                threads,
            };

            let json = serde_json::to_string_pretty(&archive)
                .context("Failed to serialize MonthArchive")?;

            let out_file = output.join(format!("{}.json", month));
            fs::write(&out_file, json)
                .with_context(|| format!("Failed to write {}", out_file.display()))?;

            eprintln!("Wrote {}", out_file.display());
        }
    }

    Ok(())
}

/// Generate ListMeta from mbox files and write meta.json.
///
/// Counts messages and months from the input mbox files.
pub fn run_stats(input: &Path, output: &Path, list_name: &str) -> Result<()> {
    let files = discover_mbox_files(input)?;

    if files.is_empty() {
        anyhow::bail!("No mbox files found in {}", input.display());
    }

    if let Some(parent) = output.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).context("Failed to create output directory")?;
        }
    }

    let mut total_messages: usize = 0;
    let mut total_threads: usize = 0;
    let mut months: Vec<String> = Vec::new();
    let mut first_date: Option<String> = None;
    let mut last_date: Option<String> = None;

    for file_path in &files {
        let content = read_file_lossy(file_path)?;

        let raw_messages = split_mbox(&content);

        let mut messages: Vec<Message> = raw_messages
            .par_iter()
            .filter_map(|raw| parse_message(raw).ok())
            .collect();

        if messages.is_empty() {
            continue;
        }

        // Deduplicate by message_id
        let mut seen_ids: HashSet<String> = HashSet::new();
        messages.retain(|msg| seen_ids.insert(msg.message_id.clone()));

        messages.sort_by(|a, b| a.date.cmp(&b.date));

        // Track first/last message dates
        if let Some(first_msg) = messages.first() {
            let date_str = first_msg.date.to_rfc3339();
            if first_date.is_none() || date_str < *first_date.as_ref().unwrap() {
                first_date = Some(date_str);
            }
        }
        if let Some(last_msg) = messages.last() {
            let date_str = last_msg.date.to_rfc3339();
            if last_date.is_none() || date_str > *last_date.as_ref().unwrap() {
                last_date = Some(date_str);
            }
        }

        total_messages += messages.len();

        // Collect unique months
        let month = month_from_filename(file_path);
        if !months.contains(&month) {
            months.push(month);
        }

        // Count threads
        let threads = reconstruct_threads(&mut messages);
        total_threads += threads.len();
    }

    months.sort();

    let meta = ListMeta {
        list: list_name.to_string(),
        description: String::new(),
        source_url: String::new(),
        total_messages,
        first_message: first_date.unwrap_or_default(),
        last_message: last_date.unwrap_or_default(),
        total_threads,
        months_available: months,
    };

    let json =
        serde_json::to_string_pretty(&meta).context("Failed to serialize ListMeta")?;

    fs::write(output, &json)
        .with_context(|| format!("Failed to write {}", output.display()))?;

    eprintln!("Wrote {}", output.display());

    Ok(())
}

/// Converts a month name (full or abbreviated) to its number (1-12).
fn month_name_to_number(name: &str) -> Option<u32> {
    match name.to_lowercase().as_str() {
        "january" | "jan" => Some(1),
        "february" | "feb" => Some(2),
        "march" | "mar" => Some(3),
        "april" | "apr" => Some(4),
        "may" => Some(5),
        "june" | "jun" => Some(6),
        "july" | "jul" => Some(7),
        "august" | "aug" => Some(8),
        "september" | "sep" => Some(9),
        "october" | "oct" => Some(10),
        "november" | "nov" => Some(11),
        "december" | "dec" => Some(12),
        _ => None,
    }
}
