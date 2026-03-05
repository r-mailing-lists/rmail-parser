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
    /// Parse mbox files into JSON
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
    },
    /// Generate list metadata/stats
    Stats {
        /// Input directory of .mbox files
        #[arg(short, long)]
        input: String,
        /// Output file for meta.json
        #[arg(short, long)]
        output: String,
        /// Mailing list name
        #[arg(short, long)]
        list: String,
    },
}

fn main() {
    let cli = Cli::parse();
    let result = match cli.command {
        Commands::Parse { input, output, list } => {
            pipeline::run_parse(Path::new(&input), Path::new(&output), &list)
        }
        Commands::Stats { input, output, list } => {
            pipeline::run_stats(Path::new(&input), Path::new(&output), &list)
        }
    };

    if let Err(e) = result {
        eprintln!("Error: {:#}", e);
        std::process::exit(1);
    }
}
