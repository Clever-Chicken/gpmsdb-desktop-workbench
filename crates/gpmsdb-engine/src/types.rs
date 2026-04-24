#[derive(Clone, Copy, Debug)]
pub struct QueryPeak {
    pub milli_mz: u32,
    pub intensity: f32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CandidateHit {
    pub genome_id: u32,
    pub hit_count: u32,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RankedResult {
    pub genome_id: u32,
    pub score: f32,
    pub matched_ribosomal: u32,
    pub matched_total: u32,
}
