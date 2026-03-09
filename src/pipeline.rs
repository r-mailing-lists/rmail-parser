use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use encoding_rs::WINDOWS_1252;
use rayon::prelude::*;
use serde::Deserialize;

use crate::mbox::split_mbox;
use crate::message::{parse_message, Message};
use crate::output::{
    AggregatedContributor, ContributorEntry, ListCount, ListIndex, ListMeta, MonthArchive,
};
use crate::threading::reconstruct_threads;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Reads a file as a UTF-8 string, falling back to Windows-1252 (Latin-1 superset)
/// if the file is not valid UTF-8. This handles old mbox files that contain
/// raw 8-bit characters from various European encodings.
fn read_file_lossy(path: &Path) -> Result<String> {
    let bytes =
        fs::read(path).with_context(|| format!("Failed to read {}", path.display()))?;

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

/// Generate a URL-safe slug from a name.
fn slugify(name: &str) -> String {
    let lower = name.to_lowercase();
    let slug: String = lower
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    // Collapse consecutive hyphens
    let mut result = String::with_capacity(slug.len());
    let mut prev_hyphen = false;
    for ch in slug.chars() {
        if ch == '-' {
            if !prev_hyphen {
                result.push('-');
            }
            prev_hyphen = true;
        } else {
            result.push(ch);
            prev_hyphen = false;
        }
    }
    result.trim_matches('-').to_string()
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

// ---------------------------------------------------------------------------
// Aliases — merge multiple email hashes into one canonical identity
// ---------------------------------------------------------------------------

/// An alias entry linking multiple email hashes to one canonical identity.
#[derive(Debug, Deserialize)]
struct AliasEntry {
    /// Canonical display name for this person
    canonical_name: String,
    /// All email hashes belonging to this person
    email_hashes: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct AliasFile {
    aliases: Vec<AliasEntry>,
}

/// Maps each email hash to its canonical hash (the first one in the group).
/// Also stores canonical_name overrides.
#[derive(Debug, Default)]
pub struct AliasMap {
    /// email_hash → canonical_email_hash
    hash_to_canonical: HashMap<String, String>,
    /// canonical_email_hash → forced display name
    canonical_names: HashMap<String, String>,
}

impl AliasMap {
    /// Load aliases from a JSON file. Returns an empty map if path is None.
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let path = match path {
            Some(p) if p.exists() => p,
            _ => return Ok(Self::default()),
        };

        let raw = fs::read_to_string(path)
            .with_context(|| format!("Failed to read aliases file: {}", path.display()))?;
        let file: AliasFile = serde_json::from_str(&raw)
            .with_context(|| format!("Failed to parse aliases file: {}", path.display()))?;

        let mut map = Self::default();
        for entry in &file.aliases {
            if entry.email_hashes.is_empty() {
                continue;
            }
            let canonical = &entry.email_hashes[0];
            map.canonical_names
                .insert(canonical.clone(), entry.canonical_name.clone());
            for hash in &entry.email_hashes {
                map.hash_to_canonical
                    .insert(hash.clone(), canonical.clone());
            }
        }

        eprintln!(
            "Loaded {} alias groups from {}",
            file.aliases.len(),
            path.display()
        );
        Ok(map)
    }

    /// Resolve an email hash to its canonical hash.
    fn resolve<'a>(&'a self, hash: &'a str) -> &'a str {
        self.hash_to_canonical
            .get(hash)
            .map(|s| s.as_str())
            .unwrap_or(hash)
    }

    /// Get the canonical name override for a hash, if one exists.
    fn canonical_name(&self, canonical_hash: &str) -> Option<&str> {
        self.canonical_names.get(canonical_hash).map(|s| s.as_str())
    }
}

// ---------------------------------------------------------------------------
// Stats generation from parsed messages
// ---------------------------------------------------------------------------

/// Accumulates stats across all parsed messages and writes output files.
struct StatsAccumulator {
    list_name: String,
    total_messages: usize,
    total_threads: usize,
    months: Vec<String>,
    first_date: Option<String>,
    last_date: Option<String>,
    /// msg_id → month
    msg_index: BTreeMap<String, String>,
    /// thread_id → month
    thread_index: BTreeMap<String, String>,
    /// [[msg_id, month, date_rfc3339], ...]
    message_order: Vec<(String, String, String)>,
    /// email_hash → ContributorAccum
    contributors: HashMap<String, ContributorAccum>,
    /// Alias map for merging multiple email hashes
    alias_map: AliasMap,
}

struct ContributorAccum {
    email_hash: String,
    /// name variant → count, to pick the most-used display name
    name_variants: HashMap<String, usize>,
    count: usize,
    first_date: Option<String>,
    last_date: Option<String>,
    yearly: HashMap<String, usize>,
}

impl ContributorAccum {
    /// Returns the display name with the highest message count.
    fn canonical_name(&self) -> &str {
        self.name_variants
            .iter()
            .max_by_key(|(_, count)| *count)
            .map(|(name, _)| name.as_str())
            .unwrap_or("")
    }
}

impl StatsAccumulator {
    fn new(list_name: &str, alias_map: AliasMap) -> Self {
        Self {
            list_name: list_name.to_string(),
            total_messages: 0,
            total_threads: 0,
            months: Vec::new(),
            first_date: None,
            last_date: None,
            msg_index: BTreeMap::new(),
            thread_index: BTreeMap::new(),
            message_order: Vec::new(),
            contributors: HashMap::new(),
            alias_map,
        }
    }

    /// Accumulate stats from one month's parsed data.
    fn accumulate(&mut self, month: &str, messages: &[Message], thread_count: usize) {
        if !self.months.contains(&month.to_string()) {
            self.months.push(month.to_string());
        }

        self.total_messages += messages.len();
        self.total_threads += thread_count;

        for msg in messages {
            let date_str = msg.date.to_rfc3339();

            // First/last date
            if self.first_date.is_none() || date_str < *self.first_date.as_ref().unwrap() {
                self.first_date = Some(date_str.clone());
            }
            if self.last_date.is_none() || date_str > *self.last_date.as_ref().unwrap() {
                self.last_date = Some(date_str.clone());
            }

            // Msg index
            self.msg_index.insert(msg.id.clone(), month.to_string());

            // Thread index
            if !msg.thread_id.is_empty() {
                self.thread_index
                    .entry(msg.thread_id.clone())
                    .or_insert_with(|| month.to_string());
            }

            // Message order
            self.message_order
                .push((msg.id.clone(), month.to_string(), date_str.clone()));

            // Contributors — keyed by canonical email hash to merge name variants
            let year = msg.date.format("%Y").to_string();
            let canonical_hash = self.alias_map.resolve(&msg.from_email_hash).to_string();
            let entry = self
                .contributors
                .entry(canonical_hash.clone())
                .or_insert_with(|| ContributorAccum {
                    email_hash: canonical_hash,
                    name_variants: HashMap::new(),
                    count: 0,
                    first_date: None,
                    last_date: None,
                    yearly: HashMap::new(),
                });
            entry.count += 1;
            *entry.name_variants.entry(msg.from_name.clone()).or_insert(0) += 1;
            if entry.first_date.is_none()
                || date_str < *entry.first_date.as_ref().unwrap()
            {
                entry.first_date = Some(date_str.clone());
            }
            if entry.last_date.is_none()
                || date_str > *entry.last_date.as_ref().unwrap()
            {
                entry.last_date = Some(date_str.clone());
            }
            *entry.yearly.entry(year).or_insert(0) += 1;
        }
    }

    /// Write all stats/index files to the output directory.
    fn write(&mut self, output: &Path) -> Result<()> {
        self.months.sort();

        // meta.json
        let meta = ListMeta {
            list: self.list_name.clone(),
            description: String::new(),
            source_url: String::new(),
            total_messages: self.total_messages,
            first_message: self.first_date.clone().unwrap_or_default(),
            last_message: self.last_date.clone().unwrap_or_default(),
            total_threads: self.total_threads,
            months_available: self.months.clone(),
        };
        let meta_path = output.join("meta.json");
        let json = serde_json::to_string_pretty(&meta)
            .context("Failed to serialize ListMeta")?;
        fs::write(&meta_path, json)
            .with_context(|| format!("Failed to write {}", meta_path.display()))?;
        eprintln!("Wrote {}", meta_path.display());

        // index.json
        let index = ListIndex {
            messages: self.msg_index.clone(),
            threads: self.thread_index.clone(),
        };
        let index_path = output.join("index.json");
        let json = serde_json::to_string(&index)
            .context("Failed to serialize ListIndex")?;
        fs::write(&index_path, json)
            .with_context(|| format!("Failed to write {}", index_path.display()))?;
        eprintln!("Wrote {}", index_path.display());

        // message-order.json — [[msg_id, month], ...] sorted by date
        self.message_order.sort_by(|a, b| a.2.cmp(&b.2));
        let order: Vec<[&str; 2]> = self
            .message_order
            .iter()
            .map(|(id, month, _)| [id.as_str(), month.as_str()])
            .collect();
        let order_path = output.join("message-order.json");
        let json = serde_json::to_string(&order)
            .context("Failed to serialize message-order")?;
        fs::write(&order_path, json)
            .with_context(|| format!("Failed to write {}", order_path.display()))?;
        eprintln!("Wrote {}", order_path.display());

        // contributors.json
        let mut contributors: Vec<ContributorEntry> = self
            .contributors
            .values()
            .map(|c| {
                // Use alias canonical_name if set, otherwise most-used name variant
                let name = self
                    .alias_map
                    .canonical_name(&c.email_hash)
                    .unwrap_or_else(|| c.canonical_name())
                    .to_string();
                let yearly: BTreeMap<String, usize> =
                    c.yearly.iter().map(|(k, &v)| (k.clone(), v)).collect();
                ContributorEntry {
                    slug: slugify(&name),
                    email_hash: c.email_hash.clone(),
                    name,
                    message_count: c.count,
                    first_date: c.first_date.clone(),
                    last_date: c.last_date.clone(),
                    yearly_activity: yearly,
                }
            })
            .collect();
        contributors.sort_by(|a, b| b.message_count.cmp(&a.message_count));

        let contrib_path = output.join("contributors.json");
        let json = serde_json::to_string_pretty(&contributors)
            .context("Failed to serialize contributors")?;
        fs::write(&contrib_path, json)
            .with_context(|| format!("Failed to write {}", contrib_path.display()))?;
        eprintln!(
            "Wrote {} ({} contributors)",
            contrib_path.display(),
            contributors.len()
        );

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// run_parse
// ---------------------------------------------------------------------------

/// Main parse pipeline.
///
/// - Discovers mbox files from `input`
/// - For each file: reads content, splits into messages, parses them (in parallel
///   via rayon), reconstructs threads, and writes a MonthArchive JSON file.
/// - One JSON file per month: `{output}/{month}.json`
/// - If `generate_stats` is true, also writes meta.json, index.json,
///   message-order.json, and contributors.json to the output directory.
pub fn run_parse(
    input: &Path,
    output: &Path,
    list_name: &str,
    generate_stats: bool,
    aliases_path: Option<&Path>,
) -> Result<()> {
    let files = discover_mbox_files(input)?;

    if files.is_empty() {
        anyhow::bail!("No mbox files found in {}", input.display());
    }

    fs::create_dir_all(output).context("Failed to create output directory")?;

    let mut stats = if generate_stats {
        let alias_map = AliasMap::load(aliases_path)?;
        Some(StatsAccumulator::new(list_name, alias_map))
    } else {
        None
    };

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

        // Group messages by month
        let mut by_month: HashMap<String, Vec<Message>> = HashMap::new();
        for msg in messages.drain(..) {
            by_month.entry(msg.month.clone()).or_default().push(msg);
        }

        for (month, mut month_messages) in by_month {
            // Sort messages by date within the month
            month_messages.sort_by(|a, b| a.date.cmp(&b.date));

            let threads = reconstruct_threads(&mut month_messages);

            // Accumulate stats before moving messages into the archive
            if let Some(ref mut s) = stats {
                s.accumulate(&month, &month_messages, threads.len());
            }

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

    // Write stats if enabled
    if let Some(mut s) = stats {
        s.write(output)?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// run_stats (standalone — reads processed JSON, not mbox)
// ---------------------------------------------------------------------------

/// Generate stats from already-parsed processed JSON files.
///
/// Reads `{input}/*.json` (month archives), generates meta.json, index.json,
/// message-order.json, and contributors.json in the output directory.
pub fn run_stats(input: &Path, output: &Path, list_name: &str, aliases_path: Option<&Path>) -> Result<()> {
    if !input.is_dir() {
        anyhow::bail!(
            "Input must be a directory of processed JSON files: {}",
            input.display()
        );
    }

    fs::create_dir_all(output).context("Failed to create output directory")?;

    // Find month JSON files (exclude meta.json, index.json, etc.)
    let mut month_files: Vec<PathBuf> = fs::read_dir(input)
        .context("Failed to read input directory")?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            if let Some(ext) = p.extension().and_then(|e| e.to_str()) {
                if ext != "json" {
                    return false;
                }
            } else {
                return false;
            }
            let stem = p
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("");
            !matches!(
                stem,
                "meta" | "index" | "message-order" | "contributors"
            )
        })
        .collect();

    month_files.sort();

    if month_files.is_empty() {
        anyhow::bail!("No month JSON files found in {}", input.display());
    }

    let alias_map = AliasMap::load(aliases_path)?;
    let mut stats = StatsAccumulator::new(list_name, alias_map);

    for file_path in &month_files {
        let raw = fs::read_to_string(file_path)
            .with_context(|| format!("Failed to read {}", file_path.display()))?;

        let archive: MonthArchive = serde_json::from_str(&raw)
            .with_context(|| format!("Failed to parse {}", file_path.display()))?;

        // We need thread count — count unique thread IDs from messages
        let mut thread_ids: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for msg in &archive.messages {
            if !msg.thread_id.is_empty() {
                thread_ids.insert(&msg.thread_id);
            }
        }

        stats.accumulate(&archive.month, &archive.messages, thread_ids.len());
    }

    stats.write(output)?;

    Ok(())
}

// ---------------------------------------------------------------------------
// run_aggregate
// ---------------------------------------------------------------------------

/// Aggregate per-list contributors.json files into a unified _contributors.json.
///
/// Reads `{input}/{list}/contributors.json` for each list subdirectory,
/// merges by contributor name, and writes the combined result.
pub fn run_aggregate(input: &Path, output: &Path, aliases_path: Option<&Path>) -> Result<()> {
    if !input.is_dir() {
        anyhow::bail!(
            "Input must be a directory containing list subdirectories: {}",
            input.display()
        );
    }

    // Find list directories that contain contributors.json
    let mut list_dirs: Vec<PathBuf> = fs::read_dir(input)
        .context("Failed to read input directory")?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_dir() && p.join("contributors.json").exists())
        .collect();

    list_dirs.sort();

    if list_dirs.is_empty() {
        anyhow::bail!(
            "No list directories with contributors.json found in {}",
            input.display()
        );
    }

    let alias_map = AliasMap::load(aliases_path)?;

    // canonical_email_hash → aggregated data (merges name variants across lists)
    let mut merged: HashMap<String, MergeAccum> = HashMap::new();

    for list_dir in &list_dirs {
        let list_slug = list_dir
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string();

        let contrib_path = list_dir.join("contributors.json");
        let raw = fs::read_to_string(&contrib_path)
            .with_context(|| format!("Failed to read {}", contrib_path.display()))?;

        let entries: Vec<ContributorEntry> = serde_json::from_str(&raw)
            .with_context(|| format!("Failed to parse {}", contrib_path.display()))?;

        eprintln!(
            "Read {} contributors from {}",
            entries.len(),
            list_slug
        );

        for entry in entries {
            let canonical_hash = alias_map.resolve(&entry.email_hash).to_string();
            let accum = merged
                .entry(canonical_hash.clone())
                .or_insert_with(|| MergeAccum {
                    name: entry.name.clone(),
                    email_hash: canonical_hash,
                    name_count: entry.message_count,
                    total_count: 0,
                    lists: Vec::new(),
                    first_date: None,
                    last_date: None,
                    yearly: HashMap::new(),
                });

            accum.total_count += entry.message_count;

            // Use the name variant with the most messages as canonical
            if entry.message_count > accum.name_count {
                accum.name = entry.name.clone();
                accum.name_count = entry.message_count;
            }

            accum.lists.push(ListCount {
                slug: list_slug.clone(),
                count: entry.message_count,
            });

            // Min first_date
            if let Some(ref d) = entry.first_date {
                if accum.first_date.is_none()
                    || d < accum.first_date.as_ref().unwrap()
                {
                    accum.first_date = Some(d.clone());
                }
            }

            // Max last_date
            if let Some(ref d) = entry.last_date {
                if accum.last_date.is_none()
                    || d > accum.last_date.as_ref().unwrap()
                {
                    accum.last_date = Some(d.clone());
                }
            }

            // Merge yearly activity
            for (year, count) in &entry.yearly_activity {
                *accum.yearly.entry(year.clone()).or_insert(0) += count;
            }
        }
    }

    // Convert to output format
    let mut contributors: Vec<AggregatedContributor> = merged
        .into_values()
        .map(|a| {
            let mut lists = a.lists;
            lists.sort_by(|x, y| y.count.cmp(&x.count));

            let yearly: BTreeMap<String, usize> =
                a.yearly.into_iter().collect();

            // Use alias canonical_name if set, otherwise most-used name variant
            let name = alias_map
                .canonical_name(&a.email_hash)
                .unwrap_or(&a.name)
                .to_string();

            AggregatedContributor {
                slug: slugify(&name),
                email_hash: a.email_hash,
                name,
                message_count: a.total_count,
                lists,
                first_date: a.first_date,
                last_date: a.last_date,
                yearly_activity: yearly,
            }
        })
        .collect();

    contributors.sort_by(|a, b| b.message_count.cmp(&a.message_count));

    let json = serde_json::to_string_pretty(&contributors)
        .context("Failed to serialize aggregated contributors")?;

    // Ensure parent directory exists
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }

    fs::write(output, json)
        .with_context(|| format!("Failed to write {}", output.display()))?;

    eprintln!(
        "Wrote {} ({} contributors from {} lists)",
        output.display(),
        contributors.len(),
        list_dirs.len()
    );

    Ok(())
}

struct MergeAccum {
    name: String,
    email_hash: String,
    /// Message count of the current canonical name (for picking best variant)
    name_count: usize,
    total_count: usize,
    lists: Vec<ListCount>,
    first_date: Option<String>,
    last_date: Option<String>,
    yearly: HashMap<String, usize>,
}
