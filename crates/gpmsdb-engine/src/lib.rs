mod batch;
mod coarse;
mod rerank;
mod types;

pub use batch::{
    run_batch_for_test, run_batch_with_progress, run_batch_with_progress_for_test,
    BatchProgressEvent,
};
pub use coarse::{search_coarse_into, SearchBuffer};
pub use rerank::rerank_into;
pub use types::{CandidateHit, QueryPeak, RankedResult};

use gpmsdb_format::MappedDatabase;

pub fn identify(
    db: &MappedDatabase,
    query: &[QueryPeak],
    coarse_limit: usize,
    ppm: u32,
) -> Vec<RankedResult> {
    let genome_count = db.header().genome_count as usize;
    let mut buf = SearchBuffer::new(genome_count);
    search_coarse_into(db, query, coarse_limit, &mut buf);
    rerank_into(db, query, ppm, &mut buf);
    std::mem::take(&mut buf.ranked)
}

pub fn identify_with_buffer(
    db: &MappedDatabase,
    query: &[QueryPeak],
    coarse_limit: usize,
    ppm: u32,
    buf: &mut SearchBuffer,
) {
    search_coarse_into(db, query, coarse_limit, buf);
    rerank_into(db, query, ppm, buf);
    // Results are in buf.ranked — caller reads them directly, zero copy.
}
