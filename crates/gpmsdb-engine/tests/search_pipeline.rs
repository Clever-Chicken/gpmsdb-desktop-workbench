use gpmsdb_engine::{
    identify, run_batch_for_test, run_batch_with_progress_for_test, search_coarse_into,
    BatchProgressEvent, CandidateHit, QueryPeak, SearchBuffer,
};
use gpmsdb_format::MappedDatabase;
use std::path::Path;

#[test]
fn coarse_search_returns_expected_top_candidate() {
    let fixture_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/small-db");
    let db = MappedDatabase::open(&fixture_dir).unwrap();
    let query = vec![
        QueryPeak {
            milli_mz: 1_000_000,
            intensity: 0.4,
        },
        QueryPeak {
            milli_mz: 1_200_000,
            intensity: 0.3,
        },
        QueryPeak {
            milli_mz: 1_001_000,
            intensity: 0.2,
        },
    ];

    let genome_count = db.header().genome_count as usize;
    let mut buf = SearchBuffer::new(genome_count);
    search_coarse_into(&db, &query, 8, &mut buf);
    let candidates = &buf.hits;

    assert_eq!(
        candidates[0],
        CandidateHit {
            genome_id: 0,
            hit_count: 2,
        }
    );
    assert_eq!(
        candidates[1],
        CandidateHit {
            genome_id: 1,
            hit_count: 1,
        }
    );
}

#[test]
fn rerank_prefers_tighter_mass_matches() {
    let fixture_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/small-db");
    let db = MappedDatabase::open(&fixture_dir).unwrap();
    let query = vec![
        QueryPeak {
            milli_mz: 1_000_000,
            intensity: 0.5,
        },
        QueryPeak {
            milli_mz: 1_200_000,
            intensity: 0.2,
        },
        QueryPeak {
            milli_mz: 1_001_000,
            intensity: 0.3,
        },
    ];

    let results = identify(&db, &query, 8, 50);

    assert_eq!(results[0].genome_id, 0);
    assert!(results[0].score > results[1].score);
}

#[test]
fn batch_executor_reports_progress_in_order() {
    let fixture_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/small-db");
    let db = MappedDatabase::open(&fixture_dir).unwrap();
    let queries = vec![
        vec![
            QueryPeak {
                milli_mz: 1_000_000,
                intensity: 0.5,
            },
            QueryPeak {
                milli_mz: 1_200_000,
                intensity: 0.5,
            },
        ],
        vec![
            QueryPeak {
                milli_mz: 1_001_000,
                intensity: 0.6,
            },
            QueryPeak {
                milli_mz: 1_400_000,
                intensity: 0.4,
            },
        ],
    ];

    let (_results, events) = run_batch_for_test(&db, &queries, 8, 50);

    assert_eq!(
        events,
        vec![
            BatchProgressEvent {
                processed: 1,
                total: 2,
            },
            BatchProgressEvent {
                processed: 2,
                total: 2,
            },
        ]
    );
}

#[test]
fn batch_executor_streams_progress_through_callback() {
    let fixture_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/small-db");
    let db = MappedDatabase::open(&fixture_dir).unwrap();
    let queries = vec![
        vec![
            QueryPeak {
                milli_mz: 1_000_000,
                intensity: 0.5,
            },
            QueryPeak {
                milli_mz: 1_200_000,
                intensity: 0.5,
            },
        ],
        vec![
            QueryPeak {
                milli_mz: 1_001_000,
                intensity: 0.6,
            },
            QueryPeak {
                milli_mz: 1_400_000,
                intensity: 0.4,
            },
        ],
    ];

    let streamed = run_batch_with_progress_for_test(&db, &queries, 8, 50, 1);

    assert_eq!(
        streamed,
        vec![
            BatchProgressEvent {
                processed: 1,
                total: 2,
            },
            BatchProgressEvent {
                processed: 2,
                total: 2,
            },
        ]
    );
}
