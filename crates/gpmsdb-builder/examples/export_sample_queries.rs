use std::{
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

use clap::Parser;
use gpmsdb_builder::stream_mass_all_db;

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long)]
    source: PathBuf,
    #[arg(long)]
    out_dir: PathBuf,
    #[arg(long, default_value_t = 3)]
    count: usize,
}

fn main() {
    let cli = Cli::parse();
    if let Err(error) = export_sample_queries(&cli.source, &cli.out_dir, cli.count) {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn export_sample_queries(source: &Path, out_dir: &Path, count: usize) -> Result<(), String> {
    if count == 0 {
        return Err("count must be > 0".to_string());
    }

    fs::create_dir_all(out_dir)
        .map_err(|error| format!("failed to create {}: {error}", out_dir.display()))?;

    let mut written = 0usize;
    stream_mass_all_db(source, |genome_id, peaks| {
        if written >= count {
            return;
        }

        let stem = format!("{:02}-{}", written + 1, sanitize_filename(&genome_id));
        let path = if written + 1 == count {
            out_dir.join(format!("{stem}.mgf"))
        } else {
            out_dir.join(format!("{stem}.txt"))
        };

        if path.extension().and_then(|ext| ext.to_str()) == Some("mgf") {
            write_mgf_query(&path, &genome_id, &peaks)
                .unwrap_or_else(|error| panic!("failed to write {}: {error}", path.display()));
        } else {
            write_text_query(&path, &genome_id, &peaks)
                .unwrap_or_else(|error| panic!("failed to write {}: {error}", path.display()));
        }

        written += 1;
    })
    .map_err(|error| error.to_string())?;

    if written == 0 {
        return Err(format!("no entries exported from {}", source.display()));
    }

    println!(
        "exported {written} sample quer{} into {}",
        if written == 1 { "y" } else { "ies" },
        out_dir.display()
    );
    Ok(())
}

fn write_text_query(path: &Path, genome_id: &str, peaks: &[f64]) -> Result<(), String> {
    let file = File::create(path)
        .map_err(|error| format!("failed to create {}: {error}", path.display()))?;
    let mut writer = BufWriter::new(file);
    writeln!(writer, "COM={genome_id}")
        .map_err(|error| format!("failed to write {}: {error}", path.display()))?;
    writeln!(writer, "# generated from all.db")
        .map_err(|error| format!("failed to write {}: {error}", path.display()))?;
    for peak in peaks {
        writeln!(writer, "{peak:.4} 1.0")
            .map_err(|error| format!("failed to write {}: {error}", path.display()))?;
    }
    writer
        .flush()
        .map_err(|error| format!("failed to flush {}: {error}", path.display()))
}

fn write_mgf_query(path: &Path, genome_id: &str, peaks: &[f64]) -> Result<(), String> {
    let file = File::create(path)
        .map_err(|error| format!("failed to create {}: {error}", path.display()))?;
    let mut writer = BufWriter::new(file);
    writeln!(writer, "BEGIN IONS")
        .map_err(|error| format!("failed to write {}: {error}", path.display()))?;
    writeln!(writer, "TITLE={genome_id}")
        .map_err(|error| format!("failed to write {}: {error}", path.display()))?;
    writeln!(writer, "COM={genome_id}")
        .map_err(|error| format!("failed to write {}: {error}", path.display()))?;
    for peak in peaks {
        writeln!(writer, "{peak:.4} 1.0")
            .map_err(|error| format!("failed to write {}: {error}", path.display()))?;
    }
    writeln!(writer, "END IONS")
        .map_err(|error| format!("failed to write {}: {error}", path.display()))?;
    writer
        .flush()
        .map_err(|error| format!("failed to flush {}: {error}", path.display()))
}

fn sanitize_filename(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}
