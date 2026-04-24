use std::str;

use crate::{read_u64_le, MappedDatabase};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GenomeMetadata<'a> {
    pub genome_id: u32,
    pub total_peaks: u64,
    pub gene_count: u32,
    pub taxonomy_id: u32,
    pub display_name: &'a str,
    pub taxonomy: &'a str,
}

impl MappedDatabase {
    pub fn genome_metadata(&self, genome_id: u32) -> Option<GenomeMetadata<'_>> {
        let genome_count = usize::try_from(self.header().genome_count).ok()?;
        let genome_index = usize::try_from(genome_id).ok()?;
        if genome_index >= genome_count {
            return None;
        }

        let layout = MetaLayout::read(&self.meta_mmap, genome_count)?;
        let start = read_u64_le(&self.meta_mmap, genome_index).ok()?;
        let end = read_u64_le(&self.meta_mmap, genome_index + 1).ok()?;
        let gene_count = read_u32_le(&self.meta_mmap, layout.gene_counts_offset, genome_index)?;
        let taxonomy_id = read_u32_le(&self.meta_mmap, layout.taxonomy_ids_offset, genome_index)?;
        let name_dict_id =
            read_u32_le(&self.meta_mmap, layout.genome_name_ids_offset, genome_index)? as usize;
        let taxonomy_dict_id = read_u32_le(
            &self.meta_mmap,
            layout.genome_taxonomy_ids_offset,
            genome_index,
        )? as usize;

        Some(GenomeMetadata {
            genome_id,
            total_peaks: end.checked_sub(start)?,
            gene_count,
            taxonomy_id,
            display_name: read_dictionary_entry(
                &self.meta_mmap,
                layout.name_offsets_offset,
                layout.name_blob_offset,
                layout.name_blob_len,
                name_dict_id,
            )?,
            taxonomy: read_dictionary_entry(
                &self.meta_mmap,
                layout.taxonomy_offsets_offset,
                layout.taxonomy_blob_offset,
                layout.taxonomy_blob_len,
                taxonomy_dict_id,
            )?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct MetaLayout {
    gene_counts_offset: usize,
    taxonomy_ids_offset: usize,
    name_offsets_offset: usize,
    taxonomy_offsets_offset: usize,
    genome_name_ids_offset: usize,
    genome_taxonomy_ids_offset: usize,
    name_blob_offset: usize,
    name_blob_len: usize,
    taxonomy_blob_offset: usize,
    taxonomy_blob_len: usize,
}

impl MetaLayout {
    fn read(bytes: &[u8], genome_count: usize) -> Option<Self> {
        let genome_offsets_bytes = (genome_count + 1).checked_mul(8)?;
        let gene_counts_offset = genome_offsets_bytes;
        let taxonomy_ids_offset = gene_counts_offset.checked_add(genome_count.checked_mul(4)?)?;
        let dict_counts_offset = taxonomy_ids_offset.checked_add(genome_count.checked_mul(4)?)?;
        let name_dict_count = read_scalar_u32(bytes, dict_counts_offset)? as usize;
        let taxonomy_dict_count = read_scalar_u32(bytes, dict_counts_offset + 4)? as usize;

        let name_offsets_offset = dict_counts_offset + 8;
        let name_offsets_bytes = (name_dict_count + 1).checked_mul(8)?;
        let taxonomy_offsets_offset = name_offsets_offset.checked_add(name_offsets_bytes)?;
        let taxonomy_offsets_bytes = (taxonomy_dict_count + 1).checked_mul(8)?;
        let genome_name_ids_offset = taxonomy_offsets_offset.checked_add(taxonomy_offsets_bytes)?;
        let genome_taxonomy_ids_offset =
            genome_name_ids_offset.checked_add(genome_count.checked_mul(4)?)?;
        let blob_lengths_offset =
            genome_taxonomy_ids_offset.checked_add(genome_count.checked_mul(4)?)?;
        let name_blob_len = read_scalar_u64(bytes, blob_lengths_offset)? as usize;
        let taxonomy_blob_len = read_scalar_u64(bytes, blob_lengths_offset + 8)? as usize;
        let name_blob_offset = blob_lengths_offset + 16;
        let taxonomy_blob_offset = name_blob_offset.checked_add(name_blob_len)?;

        let required_len = taxonomy_blob_offset.checked_add(taxonomy_blob_len)?;
        if bytes.len() < required_len {
            return None;
        }

        Some(Self {
            gene_counts_offset,
            taxonomy_ids_offset,
            name_offsets_offset,
            taxonomy_offsets_offset,
            genome_name_ids_offset,
            genome_taxonomy_ids_offset,
            name_blob_offset,
            name_blob_len,
            taxonomy_blob_offset,
            taxonomy_blob_len,
        })
    }
}

fn read_dictionary_entry<'a>(
    bytes: &'a [u8],
    offsets_offset: usize,
    blob_offset: usize,
    blob_len: usize,
    entry_id: usize,
) -> Option<&'a str> {
    let start = read_scalar_u64(bytes, offsets_offset + entry_id.checked_mul(8)?)? as usize;
    let end = read_scalar_u64(bytes, offsets_offset + (entry_id + 1).checked_mul(8)?)? as usize;
    if start > end || end > blob_len {
        return None;
    }

    let blob = bytes.get(blob_offset + start..blob_offset + end)?;
    str::from_utf8(blob).ok()
}

fn read_u32_le(bytes: &[u8], base_offset: usize, index: usize) -> Option<u32> {
    read_scalar_u32(bytes, base_offset + index.checked_mul(4)?)
}

fn read_scalar_u32(bytes: &[u8], offset: usize) -> Option<u32> {
    let chunk = bytes.get(offset..offset + 4)?;
    Some(u32::from_le_bytes(chunk.try_into().ok()?))
}

fn read_scalar_u64(bytes: &[u8], offset: usize) -> Option<u64> {
    let chunk = bytes.get(offset..offset + 8)?;
    Some(u64::from_le_bytes(chunk.try_into().ok()?))
}
