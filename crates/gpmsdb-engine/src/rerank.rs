use gpmsdb_format::MappedDatabase;

use crate::{
    coarse::SearchBuffer,
    types::{QueryPeak, RankedResult},
};

/// Fills `buf.ranked` with scored results for the candidates in `buf.hits`.
/// Clears `buf.ranked` before writing.
pub fn rerank_into(db: &MappedDatabase, query: &[QueryPeak], ppm: u32, buf: &mut SearchBuffer) {
    buf.ranked.clear();

    for candidate in &buf.hits {
        let Some(peaks) = db.genome_peaks(candidate.genome_id) else {
            continue;
        };

        let mut score = 0.0_f32;
        let mut matched_total = 0_u32;

        for query_peak in query {
            if let Some(best_delta_ppm) = best_match_delta_ppm(peaks, query_peak.milli_mz, ppm) {
                matched_total += 1;
                let closeness = 1.0_f32 - (best_delta_ppm as f32 / ppm as f32);
                score += query_peak.intensity * closeness.max(0.0);
            }
        }

        buf.ranked.push(RankedResult {
            genome_id: candidate.genome_id,
            score,
            matched_ribosomal: candidate.hit_count,
            matched_total,
        });
    }

    buf.ranked.sort_unstable_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then_with(|| b.matched_total.cmp(&a.matched_total))
            .then_with(|| a.genome_id.cmp(&b.genome_id))
    });
}

fn best_match_delta_ppm(peaks: &[u32], query_peak: u32, ppm: u32) -> Option<u32> {
    if peaks.is_empty() || ppm == 0 || query_peak == 0 {
        return None;
    }

    let lower_bound = ppm_window_lower(query_peak, ppm);
    let mut idx = peaks.partition_point(|peak| *peak < lower_bound);
    let mut best: Option<u32> = None;

    while idx < peaks.len() {
        let peak = peaks[idx];
        let delta_ppm = ppm_distance(query_peak, peak);
        if delta_ppm > ppm {
            if peak > query_peak {
                break;
            }
            idx += 1;
            continue;
        }

        best = Some(match best {
            Some(current) => current.min(delta_ppm),
            None => delta_ppm,
        });
        idx += 1;
    }

    best
}

fn ppm_window_lower(query_peak: u32, ppm: u32) -> u32 {
    let query = u64::from(query_peak);
    let tolerance = (query * u64::from(ppm)) / 1_000_000;
    query.saturating_sub(tolerance) as u32
}

fn ppm_distance(query_peak: u32, candidate_peak: u32) -> u32 {
    let query = u64::from(query_peak);
    let candidate = u64::from(candidate_peak);
    let delta = query.abs_diff(candidate);
    ((delta * 1_000_000) / query) as u32
}
