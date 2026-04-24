use std::path::Path;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use gpmsdb_engine::{identify, QueryPeak};
use gpmsdb_format::MappedDatabase;

fn search_hot_path(c: &mut Criterion) {
    let fixture_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/small-db");
    let db = MappedDatabase::open(&fixture_dir).expect("small-db fixture should open");
    let query = vec![
        QueryPeak {
            milli_mz: 1_000_000,
            intensity: 0.5,
        },
        QueryPeak {
            milli_mz: 1_001_000,
            intensity: 0.3,
        },
        QueryPeak {
            milli_mz: 1_200_000,
            intensity: 0.2,
        },
    ];

    c.bench_function("identify/small-db", |b| {
        b.iter(|| identify(&db, black_box(&query), black_box(8), black_box(50)))
    });
}

criterion_group!(benches, search_hot_path);
criterion_main!(benches);
