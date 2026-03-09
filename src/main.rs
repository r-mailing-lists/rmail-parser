use std::io::{self, BufRead};
use std::path::Path;

use clap::{Parser, Subcommand};

use rmail_parser::message::{deobfuscate_email, extract_email_for_hash, hash_email};
use rmail_parser::pipeline;

#[derive(Parser)]
#[command(name = "rmail-parser")]
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
        } => pipeline::run_parse(
            Path::new(&input),
            Path::new(&output),
            &list,
            !no_stats,
            aliases.as_deref().map(Path::new),
        ),
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
