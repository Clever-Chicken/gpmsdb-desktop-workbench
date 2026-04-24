use std::{fs, path::PathBuf};

use gpmsdb_builder::stream_mass_all_db;

#[test]
fn streams_protocol4_mass_all_fixture() {
    let fixture =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/small_source/all.db");

    let mut entries = Vec::new();
    stream_mass_all_db(&fixture, |genome_id, peaks| {
        entries.push((genome_id, peaks));
    })
    .expect("streaming parser should decode the fixture");

    assert_eq!(
        entries,
        vec![
            ("g0".to_string(), vec![1000.0, 1200.0]),
            ("g1".to_string(), vec![1001.0, 1400.0]),
        ],
    );
}

#[test]
fn streams_chunked_appends_for_one_list_before_emitting() {
    let fixture =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/test-fixtures/chunked-appends.db");
    let bytes = pickle_with_chunked_appends();

    if let Some(parent) = fixture.parent() {
        fs::create_dir_all(parent).expect("create fixture directory");
    }
    fs::write(&fixture, bytes).expect("write pickle fixture");

    let mut entries = Vec::new();
    stream_mass_all_db(&fixture, |genome_id, peaks| {
        entries.push((genome_id, peaks));
    })
    .expect("streaming parser should stitch repeated APPENDS into one list");

    assert_eq!(
        entries,
        vec![
            ("g0".to_string(), vec![1.0, 2.0, 3.0]),
            ("g1".to_string(), vec![5.0]),
        ],
    );
}

#[test]
fn reports_unknown_opcode_with_offset() {
    let fixture =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/test-fixtures/unknown-opcode.db");
    let bytes = vec![0x80, 0x04, b'N', b'.'];

    if let Some(parent) = fixture.parent() {
        fs::create_dir_all(parent).expect("create fixture directory");
    }
    fs::write(&fixture, bytes).expect("write pickle fixture");

    let error = stream_mass_all_db(&fixture, |_genome_id, _peaks| {})
        .expect_err("unsupported opcode should fail loudly");
    let rendered = error.to_string();

    assert!(rendered.contains("offset 2"));
    assert!(rendered.contains("0x4e"));
}

fn pickle_with_chunked_appends() -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&[0x80, 0x04]);
    bytes.push(b'}');
    bytes.push(0x94);
    bytes.push(b'(');

    push_short_binunicode(&mut bytes, "g0");
    bytes.push(0x94);
    bytes.push(b']');
    bytes.push(0x94);

    bytes.push(b'(');
    push_binfloat(&mut bytes, 1.0);
    push_binfloat(&mut bytes, 2.0);
    bytes.push(b'e');

    bytes.push(b'(');
    push_binfloat(&mut bytes, 3.0);
    bytes.push(b'e');

    push_short_binunicode(&mut bytes, "g1");
    bytes.push(0x94);
    bytes.push(b']');
    bytes.push(0x94);

    bytes.push(b'(');
    push_binfloat(&mut bytes, 5.0);
    bytes.push(b'e');

    bytes.push(b'u');
    bytes.push(b'.');
    bytes
}

fn push_short_binunicode(bytes: &mut Vec<u8>, value: &str) {
    bytes.push(0x8c);
    bytes.push(value.len() as u8);
    bytes.extend_from_slice(value.as_bytes());
}

fn push_binfloat(bytes: &mut Vec<u8>, value: f64) {
    bytes.push(b'G');
    bytes.extend_from_slice(&value.to_be_bytes());
}
