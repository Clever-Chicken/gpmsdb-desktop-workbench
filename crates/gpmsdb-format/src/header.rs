use bytemuck::{Pod, Zeroable};

pub const HEADER_SIZE: usize = 256;
pub const MAGIC_PREFIX: [u8; 6] = *b"GPMDB\0";
pub const MAGIC: [u8; 8] = [b'G', b'P', b'M', b'D', b'B', 0, 1, 0];
pub const SCHEMA_VERSION: u32 = 1;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Header {
    pub magic: [u8; 8],
    pub schema_version: u32,
    pub bin_width_milli_mz: u32,
    pub genome_count: u64,
    pub total_peak_count: u64,
    pub mass_index_offset: u64,
    pub mass_index_len: u64,
    pub genome_peaks_offset: u64,
    pub genome_peaks_len: u64,
    pub meta_offset: u64,
    pub meta_len: u64,
    pub crc32_header: u32,
    pub crc32_mass_index: u32,
    pub crc32_genome_peaks: u32,
    pub crc32_meta: u32,
    pub reserved: [u8; 160],
}

const _: [(); HEADER_SIZE] = [(); core::mem::size_of::<Header>()];
