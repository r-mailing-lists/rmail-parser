use std::path::Path;

use clap::{Parser, Subcommand};

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
    },
    /// Aggregate per-list contributors.json files into a unified file
    Aggregate {
        /// Input directory containing list subdirectories
        #[arg(short, long)]
        input: String,
        /// Output file path for the aggregated _contributors.json
        #[arg(short, long)]
        output: String,
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
        } => pipeline::run_parse(
            Path::new(&input),
            Path::new(&output),
            &list,
            !no_stats,
        ),
        Commands::Stats {
            input,
            output,
            list,
        } => pipeline::run_stats(Path::new(&input), Path::new(&output), &list),
        Commands::Aggregate { input, output } => {
            pipeline::run_aggregate(Path::new(&input), Path::new(&output))
        }
    };

    if let Err(e) = result {
        eprintln!("Error: {:#}", e);
        std::process::exit(1);
    }
}
