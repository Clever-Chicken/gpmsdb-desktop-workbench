use gpmsdb_format::MappedDatabase;
use std::path::Path;

#[test]
fn opens_small_fixture_database() {
    let fixture_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/small-db");
    let db = MappedDatabase::open(&fixture_dir).unwrap();
    assert_eq!(db.header().genome_count, 2);
}
