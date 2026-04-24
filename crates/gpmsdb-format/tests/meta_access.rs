use gpmsdb_format::MappedDatabase;
use std::path::Path;

#[test]
fn reads_genome_metadata_from_small_fixture() {
    let fixture_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/small-db");
    let db = MappedDatabase::open(&fixture_dir).unwrap();

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
