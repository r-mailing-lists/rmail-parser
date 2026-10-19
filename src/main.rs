use std::io::{self, BufRead};
use std::path::Path;

use clap::{Parser, Subcommand};

use rmail_parser::message::{deobfuscate_email, extract_email_for_hash, hash_email};
use rmail_parser::attachment_fetch::{self, AttachmentsOptions, AttachmentsReport};
use rmail_parser::pipeline;
use rmail_parser::source_fetch::{self, HttpFetcher, SourcesOptions, SourcesReport};

#[derive(Parser)]
#[command(name = "rmail-parser")]
#[command(version)]
#[command(about = "Parse Mailman pipermail mbox archives into structured JSON")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Parse mbox files into JSON (generates stats by default)
    Parse {
        /// Input path (file or directory of .mbox files)
        #[arg(short, long)]
        input: String,
        /// Output directory for JSON files
        #[arg(short, long)]
        output: String,
        /// Mailing list name (e.g. "r-help")
        #[arg(short, long)]
        list: String,
        /// Skip stats generation (meta.json, index.json, contributors.json, etc.)
        #[arg(long, default_value_t = false)]
        no_stats: bool,
        /// Path to aliases.json for contributor merging
        #[arg(long)]
        aliases: Option<String>,
        /// Directory of sources/ files written by `sources` (needs --base-url)
        #[arg(long, requires = "base_url")]
        sources: Option<String>,
        /// The list's archive at the source, as given to `sources` (needs --sources)
        #[arg(long, requires = "sources")]
        base_url: Option<String>,
        /// Hide mailbox addresses in names, subjects and text, and write Message-IDs as hashes (for a list whose archive only its members can read)
        #[arg(long, default_value_t = false)]
        hide_addresses: bool,
        /// Directory of attachments/ files written by `attachments`; the text Mailman scrubbed is put back into the bodies
        #[arg(long)]
        attachments: Option<String>,
    },
    /// Fetch the text Mailman scrubbed out of messages into attachments/ files
    Attachments {
        /// Directory of raw .mbox files
        #[arg(long)]
        raw: String,
        /// Output directory for the per-period attachments files
        #[arg(long)]
        out: String,
        /// The list's archive at the source (e.g. "https://stat.ethz.ch/pipermail/r-devel/")
        #[arg(long)]
        base_url: String,
        /// Stop after this many requests
        #[arg(long, default_value_t = 3000)]
        max_requests: usize,
        /// Pause between requests, in milliseconds
        #[arg(long, default_value_t = 1000)]
        delay_ms: u64,
    },
    /// Fetch the source's index and message pages into sources/ files
    Sources {
        /// Directory of raw .mbox files
        #[arg(long)]
        raw: String,
        /// Output directory for the per-period JSON files
        #[arg(long)]
        out: String,
        /// The list's archive at the source (e.g. "https://stat.ethz.ch/pipermail/r-devel/")
        #[arg(long)]
        base_url: String,
        /// Stop after this many requests
        #[arg(long, default_value_t = 300)]
        max_requests: usize,
        /// Pause between requests, in milliseconds
        #[arg(long, default_value_t = 1000)]
        delay_ms: u64,

        /// Look at this many periods only, newest first (default: every period)
        #[arg(long)]
        max_periods: Option<usize>,
    },
    /// Generate stats from already-parsed processed JSON (no mbox re-parsing)
    Stats {
        /// Input directory of processed JSON files
        #[arg(short, long)]
        input: String,
        /// Output directory for stats files
        #[arg(short, long)]
        output: String,
        /// Mailing list name
        #[arg(short, long)]
        list: String,
        /// Path to aliases.json for contributor merging
        #[arg(long)]
        aliases: Option<String>,
    },
    /// Aggregate per-list contributors.json files into a unified file
    Aggregate {
        /// Input directory containing list subdirectories
        #[arg(short, long)]
        input: String,
        /// Output file path for the aggregated _contributors.json
        #[arg(short, long)]
        output: String,
        /// Path to aliases.json for contributor merging
        #[arg(long)]
        aliases: Option<String>,
    },
    /// Hash email addresses (reads from arguments or stdin, one per line)
    HashEmail {
        /// Email addresses to hash
        emails: Vec<String>,
    },
}

fn main() {
    let cli = Cli::parse();
    let result = match cli.command {
        Commands::Parse {
            input,
            output,
            list,
            no_stats,
            aliases,
            sources,
            base_url,
            hide_addresses,
            attachments,
        } => {
            // clap has already made sure the two come together.
            let source_pages = match (&sources, &base_url) {
                (Some(dir), Some(base)) => Some((Path::new(dir), base.as_str())),
                _ => None,
            };
            pipeline::run_parse(
                Path::new(&input),
                Path::new(&output),
                &list,
                !no_stats,
                aliases.as_deref().map(Path::new),
                source_pages,
                hide_addresses,
                attachments.as_deref().map(Path::new),
            )
        }
        Commands::Attachments {
            raw,
            out,
            base_url,
            max_requests,
            delay_ms,
        } => {
            let opts = AttachmentsOptions {
                max_requests,
                ..AttachmentsOptions::default()
            };
            let mut fetcher = HttpFetcher::new(delay_ms);
            attachment_fetch::run_attachments(
                Path::new(&raw),
                Path::new(&out),
                &base_url,
                &opts,
                &mut fetcher,
            )
            .map(|report| print_attachments_report(&report))
        }
        Commands::Sources {
            raw,
            out,
            base_url,
            max_requests,
            delay_ms,
            max_periods,
        } => {
            let opts = SourcesOptions {
                max_requests,
                max_periods,
                ..SourcesOptions::default()
            };
            let mut fetcher = HttpFetcher::new(delay_ms);
            source_fetch::run_sources(
                Path::new(&raw),
                Path::new(&out),
                &base_url,
                &opts,
                &mut fetcher,
            )
            .map(|report| print_sources_report(&report))
        }
        Commands::Stats {
            input,
            output,
            list,
            aliases,
        } => pipeline::run_stats(
            Path::new(&input),
            Path::new(&output),
            &list,
            aliases.as_deref().map(Path::new),
        ),
        Commands::Aggregate {
            input,
            output,
            aliases,
        } => pipeline::run_aggregate(
            Path::new(&input),
            Path::new(&output),
            aliases.as_deref().map(Path::new),
        ),
        Commands::HashEmail { emails } => {
            run_hash_email(&emails);
            Ok(())
        }
    };

    if let Err(e) = result {
        eprintln!("Error: {:#}", e);
        std::process::exit(1);
    }
}

/// Prints what an `attachments` run did, one count per line. A run that
/// stopped early says so and still succeeds: the next run carries on.
fn print_attachments_report(report: &AttachmentsReport) {
    eprintln!("Periods looked at:    {}", report.periods);
    eprintln!("Notices found:        {}", report.found);
    eprintln!("Stored before:        {}", report.stored_before);
    eprintln!("Fetched:              {}", report.fetched);
    eprintln!("Not found:            {}", report.not_found);
    eprintln!("Too large:            {}", report.too_large);
    eprintln!("Failed:               {}", report.failed);
    eprintln!("Outside the archive:  {}", report.refused);
    eprintln!("Left:                 {}", report.left);
    if let Some(reason) = &report.stopped {
        eprintln!("warning: {}", reason);
    }
}

/// Prints what a `sources` run did. A run that stopped early says so here
/// and still succeeds: the next run carries on from the files it wrote. A
/// run that reached the end but did not get everything it asked for says
/// that, and not that every period is up to date. Nor does a run that was
/// told to leave the older periods alone.
fn print_sources_report(report: &SourcesReport) {
    eprintln!("Periods indexed:               {}", report.periods_indexed);
    eprintln!("Pages fetched:                 {}", report.pages_fetched);
    eprintln!("Characters restored:           {}", report.chars_restored);
    eprintln!("? left as they were:           {}", report.chars_left);
    eprintln!("Pages naming another message:  {}", report.mismatches);
    eprintln!("Characters beyond U+FFFF seen: {}", report.astral_chars);
    eprintln!("Requests that failed:          {}", report.failed);
    eprintln!("Pages the source did not have: {}", report.not_found);
    eprintln!("Indexes ahead of the raw file: {}", report.index_ahead);
    eprintln!("Messages in periods looked at: {}", report.messages);
    eprintln!("Messages without a link:       {}", report.unlinked);
    eprintln!("Rejected pairings on file:     {}", report.rejected);
    eprintln!("Older periods not looked at:   {}", report.periods_skipped);
    let complete = report.failed == 0 && report.not_found == 0;
    match &report.stopped {
        Some(reason) => eprintln!("Stopped early: {}", reason),
        None if complete && report.periods_skipped == 0 => {
            eprintln!("Every period is up to date")
        }
        None if complete => eprintln!("The periods looked at are up to date"),
        None => eprintln!("Not everything was fetched; the next run will ask again"),
    }
}

/// Hash email addresses and print results.
/// If no emails given as args, reads from stdin (one per line).
fn run_hash_email(emails: &[String]) {
    let process = |email: &str| {
        let email = email.trim();
        if email.is_empty() {
            return;
        }
        // Handle "Name <email>" format too
        let clean = if email.contains('<') || email.contains("@end|ng") {
            extract_email_for_hash(email)
        } else {
            deobfuscate_email(email)
        };
        let hash = hash_email(&clean);
        println!("{}\t{}\t{}", email, clean, hash);
    };

    if emails.is_empty() {
        // Read from stdin
        eprintln!("Reading email addresses from stdin (one per line, Ctrl-D to finish):");
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            if let Ok(line) = line {
                process(&line);
            }
        }
    } else {
        for email in emails {
            process(email);
        }
    }
}
