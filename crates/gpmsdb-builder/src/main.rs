use std::path::PathBuf;

use clap::Parser;

use gpmsdb_builder::{build_database, BuildOptions, ProgressMode};

#[derive(Debug, Parser)]
#[command(
    name = "gpmsdb-builder",
    about = "Rust-native builder scaffold for GPMsDB binary artifacts"
)]
struct Cli {
    #[arg(long)]
    source_root: PathBuf,
    #[arg(long)]
    out_dir: PathBuf,
    #[arg(long, default_value_t = 100)]
    bin_width_milli_mz: u32,
    #[arg(long, default_value_t = 64)]
    shard_count: u32,
    #[arg(long, default_value_t = 4096)]
    shard_flush_threshold: usize,
    #[arg(long, value_enum, default_value_t = ProgressMode::Jsonl)]
    progress: ProgressMode,
    #[arg(long)]
    genes_path: Option<PathBuf>,
    #[arg(long)]
    names_path: Option<PathBuf>,
    #[arg(long)]
    taxonomy_path: Option<PathBuf>,
    #[arg(long, default_value_t = 100)]
    progress_interval_ms: u64,
}

fn main() {
    let cli = Cli::parse();
    let options = BuildOptions {
        source_root: cli.source_root,
        out_dir: cli.out_dir,
        bin_width_milli_mz: cli.bin_width_milli_mz,
        shard_count: cli.shard_count,
        shard_flush_threshold: cli.shard_flush_threshold,
        progress: cli.progress,
        genes_path: cli.genes_path,
        names_path: cli.names_path,
        taxonomy_path: cli.taxonomy_path,
        progress_callback: None,
        progress_interval_ms: cli.progress_interval_ms,
    };

    if let Err(error) = build_database(&options) {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
