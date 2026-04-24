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
fn opens_small_fixture_database() {
    let fixture_dir = build_runtime_fixture();
    let db = MappedDatabase::open(fixture_dir.path()).unwrap();
    assert_eq!(db.header().genome_count, 2);
}
