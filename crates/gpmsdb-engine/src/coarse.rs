use gpmsdb_format::MappedDatabase;

use crate::types::{CandidateHit, QueryPeak, RankedResult};

/// Per-query scratch space. Allocate once per thread, reuse across queries.
pub struct SearchBuffer {
    /// Flat hit counter indexed by genome_id. Length == genome_count.
    pub(crate) counts: Vec<u32>,
    /// Scratch space for coarse candidates.
    pub hits: Vec<CandidateHit>,
    /// Scratch space for ranked results (reused by rerank).
    pub(crate) ranked: Vec<RankedResult>,
    /// Genome IDs that received at least one hit this query (for fast reset).
    pub(crate) dirty: Vec<u32>,
}

impl SearchBuffer {
    pub fn new(genome_count: usize) -> Self {
        Self {
            counts: vec![0u32; genome_count],
            hits: Vec::new(),
            ranked: Vec::new(),
            dirty: Vec::new(),
        }
    }

    /// Reset only the cells that were written — O(hits) not O(genome_count).
    pub(crate) fn reset_counts(&mut self) {
        for &id in &self.dirty {
            if let Some(cell) = self.counts.get_mut(id as usize) {
                *cell = 0;
            }
        }
        self.dirty.clear();
    }
}

/// Fills `buf.hits` with the top-`limit` coarse candidates.
/// Resets `buf.counts` and `buf.dirty` before returning.
pub fn search_coarse_into(
    db: &MappedDatabase,
    query: &[QueryPeak],
    limit: usize,
    buf: &mut SearchBuffer,
) {
    buf.hits.clear();

    for peak in query {
        for posting in db.postings_for_milli_mz(peak.milli_mz) {
            let id = posting.genome_id as usize;
            if id < buf.counts.len() {
                if buf.counts[id] == 0 {
                    buf.dirty.push(posting.genome_id);
                }
                buf.counts[id] += 1;
            }
        }
    }

    buf.hits
        .extend(buf.dirty.iter().map(|&genome_id| CandidateHit {
            genome_id,
            hit_count: buf.counts[genome_id as usize],
        }));

    buf.hits.sort_unstable_by(|a, b| {
        b.hit_count
            .cmp(&a.hit_count)
            .then_with(|| a.genome_id.cmp(&b.genome_id))
    });
    buf.hits.truncate(limit);

    buf.reset_counts();
}
