use bytemuck::try_cast_slice;

use crate::{read_u64_le, MappedDatabase};

impl MappedDatabase {
    pub fn genome_peaks(&self, genome_id: u32) -> Option<&[u32]> {
        let genome_id = usize::try_from(genome_id).ok()?;
        let genome_count = usize::try_from(self.header().genome_count).ok()?;
        if genome_id >= genome_count {
            return None;
        }

        let start = read_u64_le(&self.meta_mmap, genome_id).ok()? as usize;
        let end = read_u64_le(&self.meta_mmap, genome_id + 1).ok()? as usize;
        let peaks = try_cast_slice::<u8, u32>(&self.genome_peaks_mmap)
            .expect("genome_peaks payload validated in open");
        peaks.get(start..end)
    }
}
