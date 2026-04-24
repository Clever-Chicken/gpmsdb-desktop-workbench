use gpmsdb_builder::{build_database, BuildOptions, ProgressMode};
use gpmsdb_format::MappedDatabase;
use std::path::{Path, PathBuf};

fn fixture_source_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/small_source")
}

fn build_runtime_fixture() -> tempfile::TempDir {
    let out_dir = tempfile::tempdir().expect("create temp output dir");
    build_database(&BuildOptions {
        source_root: fixture_source_root(),
        out_dir: out_dir.path().to_path_buf(),
        progress: ProgressMode::None,
        ..BuildOptions::default()
    })
    .expect("build fixture runtime database");
    out_dir
}

#[test]
fn reads_genome_metadata_from_small_fixture() {
    let fixture_dir = build_runtime_fixture();
    let db = MappedDatabase::open(fixture_dir.path()).unwrap();

    let genome0 = db.genome_metadata(0).expect("genome 0 metadata");
    assert_eq!(genome0.gene_count, 900);
    assert_eq!(genome0.taxonomy_id, 101);
    assert_eq!(genome0.display_name, "Genome Zero");
    assert_eq!(genome0.taxonomy, "d__Bacteria;p__Shared");
    assert_eq!(genome0.total_peaks, 2);

    let genome1 = db.genome_metadata(1).expect("genome 1 metadata");
    assert_eq!(genome1.gene_count, 1100);
    assert_eq!(genome1.display_name, "Genome One");
    assert_eq!(genome1.total_peaks, 2);
}
