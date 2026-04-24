use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use gpmsdb_builder::{build_database, BuildOptions, ProgressMode};
use gpmsdb_format::MappedDatabase;
use tempfile::tempdir;

#[test]
fn builds_fixture_into_runtime_artifacts() {
    let out_dir = tempdir().expect("create temp output dir");
    let source_root = fixture_source_root();

    build_database(&BuildOptions {
        source_root,
        out_dir: out_dir.path().to_path_buf(),
        bin_width_milli_mz: 100,
        shard_count: 4,
        shard_flush_threshold: 1,
        progress: ProgressMode::None,
        genes_path: None,
        names_path: None,
        taxonomy_path: None,
        progress_callback: None,
        progress_interval_ms: 0,
    })
    .expect("builder should materialize fixture artifacts");

    assert!(out_dir.path().join("header.bin").exists());
    assert!(out_dir.path().join("mass_index.bin").exists());
    assert!(out_dir.path().join("genome_peaks.bin").exists());
    assert!(out_dir.path().join("meta.bin").exists());

    let db = MappedDatabase::open(out_dir.path()).expect("open built database");
    assert_eq!(db.header().bin_width_milli_mz, 100);
    assert_eq!(db.header().genome_count, 2);
    assert_eq!(db.header().total_peak_count, 4);
    assert_eq!(db.genome_peaks(0), Some(&[1_000_000, 1_200_000][..]));
    assert_eq!(db.genome_peaks(1), Some(&[1_001_000, 1_400_000][..]));
    assert_eq!(
        db.postings_for_milli_mz(1_000_000),
        &[gpmsdb_format::index::Posting {
            genome_id: 0,
            local_peak_idx: 0,
        }]
    );
    assert_eq!(
        db.postings_for_milli_mz(1_001_000),
        &[gpmsdb_format::index::Posting {
            genome_id: 1,
            local_peak_idx: 0,
        }]
    );

    let meta = fs::read(out_dir.path().join("meta.bin")).expect("read meta");
    assert!(meta
        .windows("Genome Zero".len())
        .any(|w| w == b"Genome Zero"));
    assert!(meta
        .windows("d__Bacteria;p__Shared".len())
        .any(|w| w == b"d__Bacteria;p__Shared"));
}

#[test]
fn builds_fixture_when_source_input_is_direct_all_db_file() {
    let out_dir = tempdir().expect("create temp output dir");
    let source_root = fixture_source_root().join("all.db");

    build_database(&BuildOptions {
        source_root,
        out_dir: out_dir.path().to_path_buf(),
        bin_width_milli_mz: 100,
        shard_count: 4,
        shard_flush_threshold: 1,
        progress: ProgressMode::None,
        genes_path: None,
        names_path: None,
        taxonomy_path: None,
        progress_callback: None,
        progress_interval_ms: 0,
    })
    .expect("builder should accept a direct all.db file path");

    let db = MappedDatabase::open(out_dir.path()).expect("open built database");
    assert_eq!(db.header().genome_count, 2);
    assert_eq!(db.genome_peaks(0), Some(&[1_000_000, 1_200_000][..]));
    assert_eq!(db.genome_peaks(1), Some(&[1_001_000, 1_400_000][..]));
}

#[test]
fn builds_fixture_when_source_input_is_related_db_file() {
    let out_dir = tempdir().expect("create temp output dir");
    let source_root = fixture_source_root().join("genes.db");

    build_database(&BuildOptions {
        source_root,
        out_dir: out_dir.path().to_path_buf(),
        bin_width_milli_mz: 100,
        shard_count: 4,
        shard_flush_threshold: 1,
        progress: ProgressMode::None,
        genes_path: None,
        names_path: None,
        taxonomy_path: None,
        progress_callback: None,
        progress_interval_ms: 0,
    })
    .expect("builder should accept a related db file and discover all.db nearby");

    let db = MappedDatabase::open(out_dir.path()).expect("open built database");
    assert_eq!(db.header().genome_count, 2);
    assert_eq!(db.genome_peaks(0), Some(&[1_000_000, 1_200_000][..]));
}

#[test]
fn cli_emits_jsonl_progress_while_building() {
    let out_dir = tempdir().expect("create temp output dir");
    let source_root = fixture_source_root();

    let result = Command::new(env!("CARGO_BIN_EXE_gpmsdb-builder"))
        .arg("--source-root")
        .arg(&source_root)
        .arg("--out-dir")
        .arg(out_dir.path())
        .arg("--progress")
        .arg("jsonl")
        .arg("--shard-flush-threshold")
        .arg("1")
        .output()
        .expect("run builder cli");

    assert!(
        result.status.success(),
        "stdout:\n{}\n\nstderr:\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );

    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(stderr.contains("\"event\":\"start\""));
    assert!(stderr.contains("\"event\":\"genome\""));
    assert!(stderr.contains("\"event\":\"done\""));
    assert!(stderr.contains("\"genomes_processed\":2"));
}

fn fixture_source_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/small_source")
}
