pub mod header;
pub mod index;
pub mod meta;
pub mod peaks;

use std::fs::File;
use std::path::{Path, PathBuf};

use bytemuck::{checked, try_cast_slice};
use header::{Header, MAGIC, SCHEMA_VERSION};
use index::Posting;
use memmap2::Mmap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum OpenError {
    #[error("failed to open {path}: {source}")]
    OpenFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to mmap {path}: {source}")]
    MapFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("header.bin is too small: expected at least {expected} bytes, got {actual}")]
    HeaderTooSmall { expected: usize, actual: usize },
    #[error("header layout mismatch: {0:?}")]
    HeaderLayout(checked::CheckedCastError),
    #[error("mass_index.bin is too small: expected at least {expected} bytes, got {actual}")]
    IndexTooSmall { expected: usize, actual: usize },
    #[error("mass_index layout mismatch: {0}")]
    IndexLayout(&'static str),
    #[error("genome_peaks.bin is too small: expected at least {expected} bytes, got {actual}")]
    PeaksTooSmall { expected: usize, actual: usize },
    #[error("genome_peaks layout mismatch: {0}")]
    PeaksLayout(&'static str),
    #[error("meta.bin is too small: expected at least {expected} bytes, got {actual}")]
    MetaTooSmall { expected: usize, actual: usize },
    #[error("header magic mismatch")]
    MagicMismatch,
    #[error("header schema version mismatch: expected {expected}, got {actual}")]
    SchemaVersionMismatch { expected: u32, actual: u32 },
}

pub struct MappedDatabase {
    #[allow(dead_code)]
    root: PathBuf,
    header_mmap: Mmap,
    mass_index_mmap: Mmap,
    genome_peaks_mmap: Mmap,
    meta_mmap: Mmap,
    bin_count: usize,
    postings_offset: usize,
}

impl MappedDatabase {
    pub fn open(root: &Path) -> Result<Self, OpenError> {
        let root = root.to_path_buf();
        let header_path = root.join("header.bin");
        let header_file = File::open(&header_path).map_err(|source| OpenError::OpenFile {
            path: header_path.clone(),
            source,
        })?;
        let header_mmap =
            unsafe { Mmap::map(&header_file) }.map_err(|source| OpenError::MapFile {
                path: header_path.clone(),
                source,
            })?;

        if header_mmap.len() < core::mem::size_of::<Header>() {
            return Err(OpenError::HeaderTooSmall {
                expected: core::mem::size_of::<Header>(),
                actual: header_mmap.len(),
            });
        }

        let header =
            checked::try_from_bytes::<Header>(&header_mmap[..core::mem::size_of::<Header>()])
                .map_err(OpenError::HeaderLayout)?;
        if header.magic != MAGIC {
            return Err(OpenError::MagicMismatch);
        }
        if header.schema_version != SCHEMA_VERSION {
            return Err(OpenError::SchemaVersionMismatch {
                expected: SCHEMA_VERSION,
                actual: header.schema_version,
            });
        }

        let mass_index_path = root.join("mass_index.bin");
        let mass_index_file =
            File::open(&mass_index_path).map_err(|source| OpenError::OpenFile {
                path: mass_index_path.clone(),
                source,
            })?;
        let mass_index_mmap =
            unsafe { Mmap::map(&mass_index_file) }.map_err(|source| OpenError::MapFile {
                path: mass_index_path.clone(),
                source,
            })?;

        if mass_index_mmap.len() < core::mem::size_of::<u32>() {
            return Err(OpenError::IndexTooSmall {
                expected: core::mem::size_of::<u32>(),
                actual: mass_index_mmap.len(),
            });
        }

        let bin_count = *checked::try_from_bytes::<u32>(&mass_index_mmap[..4])
            .map_err(OpenError::HeaderLayout)? as usize;
        let postings_offset = 4 + (bin_count + 1) * core::mem::size_of::<u64>();
        if mass_index_mmap.len() < postings_offset {
            return Err(OpenError::IndexTooSmall {
                expected: postings_offset,
                actual: mass_index_mmap.len(),
            });
        }

        let posting_count = read_u64_le(&mass_index_mmap[4..postings_offset], bin_count)? as usize;
        let expected_len = postings_offset + posting_count * core::mem::size_of::<Posting>();
        if mass_index_mmap.len() != expected_len {
            return Err(OpenError::IndexTooSmall {
                expected: expected_len,
                actual: mass_index_mmap.len(),
            });
        }
        try_cast_slice::<u8, Posting>(&mass_index_mmap[postings_offset..])
            .map_err(|_| OpenError::IndexLayout("posting payload is not valid Posting slice"))?;

        let genome_peaks_path = root.join("genome_peaks.bin");
        let genome_peaks_file =
            File::open(&genome_peaks_path).map_err(|source| OpenError::OpenFile {
                path: genome_peaks_path.clone(),
                source,
            })?;
        let genome_peaks_mmap =
            unsafe { Mmap::map(&genome_peaks_file) }.map_err(|source| OpenError::MapFile {
                path: genome_peaks_path.clone(),
                source,
            })?;
        let expected_peaks_bytes = usize::try_from(header.total_peak_count)
            .unwrap_or(usize::MAX)
            .saturating_mul(core::mem::size_of::<u32>());
        if genome_peaks_mmap.len() < expected_peaks_bytes {
            return Err(OpenError::PeaksTooSmall {
                expected: expected_peaks_bytes,
                actual: genome_peaks_mmap.len(),
            });
        }
        try_cast_slice::<u8, u32>(&genome_peaks_mmap[..expected_peaks_bytes])
            .map_err(|_| OpenError::PeaksLayout("genome_peaks payload is not valid u32 slice"))?;

        let meta_path = root.join("meta.bin");
        let meta_file = File::open(&meta_path).map_err(|source| OpenError::OpenFile {
            path: meta_path.clone(),
            source,
        })?;
        let meta_mmap = unsafe { Mmap::map(&meta_file) }.map_err(|source| OpenError::MapFile {
            path: meta_path.clone(),
            source,
        })?;
        let required_meta_prefix = (usize::try_from(header.genome_count).unwrap_or(usize::MAX) + 1)
            .saturating_mul(core::mem::size_of::<u64>());
        if meta_mmap.len() < required_meta_prefix {
            return Err(OpenError::MetaTooSmall {
                expected: required_meta_prefix,
                actual: meta_mmap.len(),
            });
        }

        Ok(Self {
            root,
            header_mmap,
            mass_index_mmap,
            genome_peaks_mmap,
            meta_mmap,
            bin_count,
            postings_offset,
        })
    }

    pub fn header(&self) -> &Header {
        checked::from_bytes::<Header>(&self.header_mmap[..core::mem::size_of::<Header>()])
    }

    pub fn postings_for_milli_mz(&self, milli_mz: u32) -> &[Posting] {
        let bin_width = self.header().bin_width_milli_mz;
        if bin_width == 0 {
            return &[];
        }

        let bin_id = (milli_mz / bin_width) as usize;
        if bin_id >= self.bin_count {
            return &[];
        }

        let offset_bytes = &self.mass_index_mmap[4..self.postings_offset];
        let start =
            read_u64_le(offset_bytes, bin_id).expect("offset table validated in open") as usize;
        let end =
            read_u64_le(offset_bytes, bin_id + 1).expect("offset table validated in open") as usize;
        &self.postings()[start..end]
    }

    fn postings(&self) -> &[Posting] {
        try_cast_slice::<u8, Posting>(&self.mass_index_mmap[self.postings_offset..])
            .expect("posting payload validated in open")
    }
}

fn read_u64_le(bytes: &[u8], index: usize) -> Result<u64, OpenError> {
    let start = index
        .checked_mul(core::mem::size_of::<u64>())
        .ok_or(OpenError::IndexLayout("offset index overflow"))?;
    let end = start + core::mem::size_of::<u64>();
    let chunk = bytes
        .get(start..end)
        .ok_or(OpenError::IndexLayout("offset table is truncated"))?;
    let mut raw = [0u8; 8];
    raw.copy_from_slice(chunk);
    Ok(u64::from_le_bytes(raw))
}
