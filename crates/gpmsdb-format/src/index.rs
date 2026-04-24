use bytemuck::{Pod, Zeroable};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
pub struct Posting {
    pub genome_id: u32,
    pub local_peak_idx: u32,
}
