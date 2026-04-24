use std::{
    cell::Cell,
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Error as IoError, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use clap::ValueEnum;
use crc32fast::Hasher;
use gpmsdb_format::{
    header::{HEADER_SIZE, MAGIC, SCHEMA_VERSION},
    index::Posting,
};
use memmap2::{Mmap, MmapMut};
use serde::Deserialize;
use serde_pickle::{de::DeOptions, from_reader};
use thiserror::Error;

use crate::try_stream_mass_all_db;

const DEFAULT_SHARD_COUNT: u32 = 64;
const DEFAULT_SHARD_FLUSH_THRESHOLD: usize = 4096;
const DEFAULT_PROGRESS_INTERVAL_MS: u64 = 100;
const IO_BUFFER_CAPACITY: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ProgressMode {
    None,
    Jsonl,
}

#[derive(Debug, Clone)]
pub enum BuildProgress {
    Start {
        all_db: PathBuf,
    },
    Genome {
        accession: String,
        genomes_processed: u64,
        peaks_processed: u64,
    },
    IndexStart,
    MetaStart,
    FinalizeStart,
    Done {
        genome_count: u64,
        total_peak_count: u64,
        bin_count: u32,
    },
}

#[derive(Clone)]
pub struct ProgressCallback(Arc<dyn Fn(BuildProgress) + Send + Sync>);

impl ProgressCallback {
    pub fn new<F>(callback: F) -> Self
    where
        F: Fn(BuildProgress) + Send + Sync + 'static,
    {
        Self(Arc::new(callback))
    }

    fn invoke(&self, event: BuildProgress) {
        (self.0)(event);
    }
}

impl std::fmt::Debug for ProgressCallback {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProgressCallback").finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
pub struct BuildOptions {
    pub source_root: PathBuf,
    pub out_dir: PathBuf,
    pub bin_width_milli_mz: u32,
    pub shard_count: u32,
    pub shard_flush_threshold: usize,
    pub progress: ProgressMode,
    pub genes_path: Option<PathBuf>,
    pub names_path: Option<PathBuf>,
    pub taxonomy_path: Option<PathBuf>,
    pub progress_callback: Option<ProgressCallback>,
    pub progress_interval_ms: u64,
}

impl Default for BuildOptions {
    fn default() -> Self {
        Self {
            source_root: PathBuf::new(),
            out_dir: PathBuf::new(),
            bin_width_milli_mz: 100,
            shard_count: DEFAULT_SHARD_COUNT,
            shard_flush_threshold: DEFAULT_SHARD_FLUSH_THRESHOLD,
            progress: ProgressMode::Jsonl,
            genes_path: None,
            names_path: None,
            taxonomy_path: None,
            progress_callback: None,
            progress_interval_ms: DEFAULT_PROGRESS_INTERVAL_MS,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuildReport {
    pub genome_count: u64,
    pub total_peak_count: u64,
    pub bin_count: u32,
}

#[derive(Debug, Error)]
pub enum BuildError {
    #[error("invalid build options: {0}")]
    InvalidOptions(&'static str),
    #[error("failed to create output directory {path}: {source}")]
    CreateOutputDir {
        path: PathBuf,
        #[source]
        source: IoError,
    },
    #[error("missing required all.db source under {root}")]
    MissingAllDb { root: PathBuf },
    #[error("failed to open metadata pickle {path}: {source}")]
    OpenMetadata {
        path: PathBuf,
        #[source]
        source: IoError,
    },
    #[error("failed to decode metadata pickle {path}: {source}")]
    DecodeMetadata {
        path: PathBuf,
        #[source]
        source: serde_pickle::Error,
    },
    #[error("failed to stream all.db source: {0}")]
    DecodeAllDb(#[from] crate::BuilderError),
    #[error("failed during streaming build callback: {0}")]
    Callback(#[from] Box<BuildError>),
    #[error("peak value {value} for genome {genome} is not a finite non-negative mass")]
    InvalidPeakValue { genome: String, value: f64 },
    #[error("encoded peak value overflow for genome {genome}: {value}")]
    PeakValueOverflow { genome: String, value: f64 },
    #[error("failed to create {path}: {source}")]
    CreateFile {
        path: PathBuf,
        #[source]
        source: IoError,
    },
    #[error("failed to write {path}: {source}")]
    WriteFile {
        path: PathBuf,
        #[source]
        source: IoError,
    },
    #[error("failed to seek {path}: {source}")]
    SeekFile {
        path: PathBuf,
        #[source]
        source: IoError,
    },
    #[error("failed to read {path}: {source}")]
    ReadFile {
        path: PathBuf,
        #[source]
        source: IoError,
    },
    #[error("failed to memory-map {path}: {source}")]
    MapFile {
        path: PathBuf,
        #[source]
        source: IoError,
    },
    #[error("too many genomes to fit into u32 ids")]
    GenomeIdOverflow,
    #[error("too many peaks in genome {genome} to fit local indices into u32")]
    LocalPeakIndexOverflow { genome: String },
    #[error("too many bins to fit into u32")]
    BinCountOverflow,
    #[error("temporary shard directory cleanup failed at {path}: {source}")]
    CleanupShardDir {
        path: PathBuf,
        #[source]
        source: IoError,
    },
}

pub fn build_database(options: &BuildOptions) -> Result<BuildReport, BuildError> {
    if options.bin_width_milli_mz == 0 {
        return Err(BuildError::InvalidOptions("bin_width_milli_mz must be > 0"));
    }
    if options.shard_count == 0 {
        return Err(BuildError::InvalidOptions("shard_count must be > 0"));
    }
    if options.shard_flush_threshold == 0 {
        return Err(BuildError::InvalidOptions(
            "shard_flush_threshold must be > 0",
        ));
    }

    let paths = ResolvedSourcePaths::discover(options)?;
    let metadata = MetadataSources::load(&paths)?;
    fs::create_dir_all(&options.out_dir).map_err(|source| BuildError::CreateOutputDir {
        path: options.out_dir.clone(),
        source,
    })?;

    let genome_peaks_path = options.out_dir.join("genome_peaks.bin");
    let mass_index_path = options.out_dir.join("mass_index.bin");
    let meta_path = options.out_dir.join("meta.bin");
    let header_path = options.out_dir.join("header.bin");
    let shard_dir = options.out_dir.join(".gpmsdb-builder-shards");
    fs::create_dir_all(&shard_dir).map_err(|source| BuildError::CreateOutputDir {
        path: shard_dir.clone(),
        source,
    })?;

    let genome_peaks_file =
        File::create(&genome_peaks_path).map_err(|source| BuildError::CreateFile {
            path: genome_peaks_path.clone(),
            source,
        })?;
    let mut genome_peaks_writer = BufWriter::with_capacity(IO_BUFFER_CAPACITY, genome_peaks_file);
    let mut shard_spool = ShardSpool::new(
        &shard_dir,
        options.shard_count,
        options.shard_flush_threshold,
    )?;
    let progress = ProgressReporter::new(
        options.progress,
        options.progress_callback.clone(),
        Duration::from_millis(options.progress_interval_ms),
    );
    let mut state = BuildState::new(options.bin_width_milli_mz, metadata);

    progress.emit(BuildProgress::Start {
        all_db: paths.all_db.clone(),
    });

    let stream_result = try_stream_mass_all_db(&paths.all_db, |genome_accession, peaks| {
        state
            .process_entry(
                genome_accession,
                peaks,
                &mut genome_peaks_writer,
                &mut shard_spool,
                &progress,
            )
            .map_err(Box::new)
    });

    match stream_result {
        Ok(()) => {}
        Err(crate::StreamError::Decode(error)) => return Err(BuildError::DecodeAllDb(error)),
        Err(crate::StreamError::Callback(error)) => return Err(*error),
    }

    genome_peaks_writer
        .flush()
        .map_err(|source| BuildError::WriteFile {
            path: genome_peaks_path.clone(),
            source,
        })?;
    shard_spool.flush_all()?;

    progress.emit(BuildProgress::IndexStart);
    let bin_count = write_mass_index(&mass_index_path, &state.bin_counts, &mut shard_spool)?;
    progress.emit(BuildProgress::MetaStart);
    write_meta(&meta_path, &state)?;

    progress.emit(BuildProgress::FinalizeStart);
    let mass_index_crc32 = crc32_file(&mass_index_path)?;
    let genome_peaks_crc32 = crc32_file(&genome_peaks_path)?;
    let meta_crc32 = crc32_file(&meta_path)?;

    let mass_index_len = fs::metadata(&mass_index_path)
        .map_err(|source| BuildError::ReadFile {
            path: mass_index_path.clone(),
            source,
        })?
        .len();
    let genome_peaks_len = fs::metadata(&genome_peaks_path)
        .map_err(|source| BuildError::ReadFile {
            path: genome_peaks_path.clone(),
            source,
        })?
        .len();
    let meta_len = fs::metadata(&meta_path)
        .map_err(|source| BuildError::ReadFile {
            path: meta_path.clone(),
            source,
        })?
        .len();

    let header_bytes = build_header_bytes(HeaderFields {
        bin_width_milli_mz: options.bin_width_milli_mz,
        genome_count: state.genome_count(),
        total_peak_count: state.total_peak_count,
        mass_index_len,
        genome_peaks_len,
        meta_len,
        crc32_mass_index: mass_index_crc32,
        crc32_genome_peaks: genome_peaks_crc32,
        crc32_meta: meta_crc32,
    });
    fs::write(&header_path, header_bytes).map_err(|source| BuildError::WriteFile {
        path: header_path.clone(),
        source,
    })?;

    fs::remove_dir_all(&shard_dir).map_err(|source| BuildError::CleanupShardDir {
        path: shard_dir.clone(),
        source,
    })?;

    let report = BuildReport {
        genome_count: state.genome_count(),
        total_peak_count: state.total_peak_count,
        bin_count,
    };
    progress.emit(BuildProgress::Done {
        genome_count: report.genome_count,
        total_peak_count: report.total_peak_count,
        bin_count: report.bin_count,
    });
    Ok(report)
}

#[derive(Debug)]
struct BuildState {
    bin_width_milli_mz: u32,
    metadata: MetadataSources,
    total_peak_count: u64,
    genome_offsets: Vec<u64>,
    gene_counts: Vec<u32>,
    taxonomy_ids: Vec<u32>,
    genome_name_dict_ids: Vec<u32>,
    genome_taxonomy_dict_ids: Vec<u32>,
    name_dict: StringDictionaryBuilder,
    taxonomy_dict: StringDictionaryBuilder,
    bin_counts: Vec<u64>,
}

impl BuildState {
    fn new(bin_width_milli_mz: u32, metadata: MetadataSources) -> Self {
        Self {
            bin_width_milli_mz,
            metadata,
            total_peak_count: 0,
            genome_offsets: vec![0],
            gene_counts: Vec::new(),
            taxonomy_ids: Vec::new(),
            genome_name_dict_ids: Vec::new(),
            genome_taxonomy_dict_ids: Vec::new(),
            name_dict: StringDictionaryBuilder::new(),
            taxonomy_dict: StringDictionaryBuilder::new(),
            bin_counts: Vec::new(),
        }
    }

    fn process_entry(
        &mut self,
        genome_accession: String,
        peaks: Vec<f64>,
        genome_peaks_writer: &mut BufWriter<File>,
        shard_spool: &mut ShardSpool,
        progress: &ProgressReporter,
    ) -> Result<(), BuildError> {
        let genome_id =
            u32::try_from(self.gene_counts.len()).map_err(|_| BuildError::GenomeIdOverflow)?;
        let gene_count = self.metadata.gene_count(&genome_accession);
        let taxonomy = self.metadata.taxonomy(&genome_accession);
        let display_name = self.metadata.name(&genome_accession);
        let name_dict_id = self.name_dict.intern(&display_name)?;
        let taxonomy_dict_id = self.taxonomy_dict.intern(&taxonomy.text)?;

        for (local_peak_idx, peak) in peaks.into_iter().enumerate() {
            let local_peak_idx =
                u32::try_from(local_peak_idx).map_err(|_| BuildError::LocalPeakIndexOverflow {
                    genome: genome_accession.clone(),
                })?;
            let peak_value = encode_peak_value(&genome_accession, peak)?;
            genome_peaks_writer
                .write_all(&peak_value.to_le_bytes())
                .map_err(|source| BuildError::WriteFile {
                    path: PathBuf::from("genome_peaks.bin"),
                    source,
                })?;

            let bin_id = peak_value / self.bin_width_milli_mz;
            let bin_index = usize::try_from(bin_id).map_err(|_| BuildError::BinCountOverflow)?;
            if self.bin_counts.len() <= bin_index {
                self.bin_counts.resize(bin_index + 1, 0);
            }
            self.bin_counts[bin_index] += 1;
            shard_spool.push(bin_id, genome_id, local_peak_idx)?;
            self.total_peak_count += 1;
        }

        self.genome_offsets.push(self.total_peak_count);
        self.gene_counts.push(gene_count);
        self.taxonomy_ids.push(taxonomy.id);
        self.genome_name_dict_ids.push(name_dict_id);
        self.genome_taxonomy_dict_ids.push(taxonomy_dict_id);

        progress.emit(BuildProgress::Genome {
            accession: genome_accession,
            genomes_processed: self.genome_count(),
            peaks_processed: self.total_peak_count,
        });
        Ok(())
    }

    fn genome_count(&self) -> u64 {
        self.gene_counts.len() as u64
    }
}

#[derive(Debug, Clone)]
struct MetadataSources {
    genes: HashMap<String, u32>,
    names: HashMap<String, String>,
    taxonomy: HashMap<String, TaxonomyInfo>,
}

impl MetadataSources {
    fn load(paths: &ResolvedSourcePaths) -> Result<Self, BuildError> {
        let genes = match &paths.genes {
            Some(path) => load_genes(path)?,
            None => HashMap::new(),
        };
        let names = match &paths.names {
            Some(path) => load_names(path)?,
            None => HashMap::new(),
        };
        let taxonomy = match &paths.taxonomy {
            Some(path) => load_taxonomy(path)?,
            None => HashMap::new(),
        };

        Ok(Self {
            genes,
            names,
            taxonomy,
        })
    }

    fn gene_count(&self, genome_accession: &str) -> u32 {
        self.genes.get(genome_accession).copied().unwrap_or(0)
    }

    fn name(&self, genome_accession: &str) -> String {
        self.names
            .get(genome_accession)
            .cloned()
            .unwrap_or_else(|| genome_accession.to_string())
    }

    fn taxonomy(&self, genome_accession: &str) -> TaxonomyInfo {
        self.taxonomy
            .get(genome_accession)
            .cloned()
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, Default)]
struct TaxonomyInfo {
    id: u32,
    text: String,
}

#[derive(Debug)]
struct ResolvedSourcePaths {
    all_db: PathBuf,
    genes: Option<PathBuf>,
    names: Option<PathBuf>,
    taxonomy: Option<PathBuf>,
}

impl ResolvedSourcePaths {
    fn discover(options: &BuildOptions) -> Result<Self, BuildError> {
        let source_input = &options.source_root;
        let all_db = resolve_all_db(source_input).ok_or_else(|| BuildError::MissingAllDb {
            root: source_input.clone(),
        })?;
        let search_roots = source_search_roots(source_input, &all_db);

        let genes = options.genes_path.clone().or_else(|| {
            find_existing_in_roots(
                &search_roots,
                &[
                    "mass/all_genes.db",
                    "all_genes.db",
                    "genes.db",
                    "custom/custom_genes.db",
                ],
            )
        });
        let names = options.names_path.clone().or_else(|| {
            find_existing_in_roots(&search_roots, &["names.db", "custom/custom_names.db"])
        });
        let taxonomy = options.taxonomy_path.clone().or_else(|| {
            find_existing_in_roots(
                &search_roots,
                &[
                    "taxonomy/gtdb_taxonomy.db",
                    "taxonomy/ncbi_taxonomy.db",
                    "taxonomy/ssu_silva_taxonomy.db",
                    "taxonomy/ssu_gg_taxonomy.db",
                    "taxonomy.db",
                    "custom/custom_taxonomy.db",
                ],
            )
        });

        Ok(Self {
            all_db,
            genes,
            names,
            taxonomy,
        })
    }
}

fn find_existing(candidates: Vec<PathBuf>) -> Option<PathBuf> {
    candidates.into_iter().find(|path| path.is_file())
}

fn resolve_all_db(source_input: &Path) -> Option<PathBuf> {
    for root in lineage_search_roots(source_input) {
        if let Some(found) = find_existing(vec![root.join("all.db"), root.join("mass/all.db")]) {
            return Some(found);
        }
    }

    None
}

fn source_search_roots(source_input: &Path, all_db: &Path) -> Vec<PathBuf> {
    let mut roots = Vec::new();

    for root in lineage_search_roots(source_input) {
        push_unique_path(&mut roots, Some(root));
    }
    for root in lineage_search_roots(all_db) {
        push_unique_path(&mut roots, Some(root));
    }

    roots
}

fn lineage_search_roots(path: &Path) -> Vec<PathBuf> {
    let anchor = if path.is_dir() {
        Some(path)
    } else {
        path.parent()
    };

    let mut roots = Vec::new();
    let Some(anchor) = anchor else {
        return roots;
    };

    for ancestor in anchor.ancestors() {
        roots.push(ancestor.to_path_buf());
    }

    roots
}

fn push_unique_path(paths: &mut Vec<PathBuf>, candidate: Option<PathBuf>) {
    let Some(candidate) = candidate else {
        return;
    };
    if !paths.iter().any(|path| path == &candidate) {
        paths.push(candidate);
    }
}

fn find_existing_in_roots(search_roots: &[PathBuf], suffixes: &[&str]) -> Option<PathBuf> {
    for root in search_roots {
        let candidates = suffixes
            .iter()
            .map(|suffix| root.join(suffix))
            .collect::<Vec<_>>();
        if let Some(found) = find_existing(candidates) {
            return Some(found);
        }
    }

    None
}

#[derive(Debug)]
struct ShardSpool {
    shard_flush_threshold: usize,
    writers: Vec<ShardWriter>,
}

impl ShardSpool {
    fn new(
        shard_dir: &Path,
        shard_count: u32,
        shard_flush_threshold: usize,
    ) -> Result<Self, BuildError> {
        let mut writers = Vec::with_capacity(
            usize::try_from(shard_count).map_err(|_| BuildError::BinCountOverflow)?,
        );
        for shard_index in 0..shard_count {
            let path = shard_dir.join(format!("shard-{shard_index:04}.bin"));
            let file = File::create(&path).map_err(|source| BuildError::CreateFile {
                path: path.clone(),
                source,
            })?;
            writers.push(ShardWriter {
                path,
                writer: BufWriter::with_capacity(IO_BUFFER_CAPACITY, file),
                buffer: Vec::new(),
            });
        }

        Ok(Self {
            shard_flush_threshold,
            writers,
        })
    }

    fn push(&mut self, bin_id: u32, genome_id: u32, local_peak_idx: u32) -> Result<(), BuildError> {
        let shard_index = usize::try_from(bin_id % (self.writers.len() as u32))
            .map_err(|_| BuildError::BinCountOverflow)?;
        let writer = &mut self.writers[shard_index];
        writer.buffer.push(ShardRecord {
            bin_id,
            genome_id,
            local_peak_idx,
        });
        if writer.buffer.len() >= self.shard_flush_threshold {
            writer.flush()?;
        }
        Ok(())
    }

    fn flush_all(&mut self) -> Result<(), BuildError> {
        for writer in &mut self.writers {
            writer.flush()?;
        }
        Ok(())
    }

    fn shard_paths(&self) -> impl Iterator<Item = &PathBuf> {
        self.writers.iter().map(|writer| &writer.path)
    }
}

#[derive(Debug)]
struct ShardWriter {
    path: PathBuf,
    writer: BufWriter<File>,
    buffer: Vec<ShardRecord>,
}

impl ShardWriter {
    fn flush(&mut self) -> Result<(), BuildError> {
        if self.buffer.is_empty() {
            self.writer
                .flush()
                .map_err(|source| BuildError::WriteFile {
                    path: self.path.clone(),
                    source,
                })?;
            return Ok(());
        }

        for record in self.buffer.drain(..) {
            self.writer
                .write_all(&record.bin_id.to_le_bytes())
                .and_then(|_| self.writer.write_all(&record.genome_id.to_le_bytes()))
                .and_then(|_| self.writer.write_all(&record.local_peak_idx.to_le_bytes()))
                .map_err(|source| BuildError::WriteFile {
                    path: self.path.clone(),
                    source,
                })?;
        }
        self.writer.flush().map_err(|source| BuildError::WriteFile {
            path: self.path.clone(),
            source,
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct ShardRecord {
    bin_id: u32,
    genome_id: u32,
    local_peak_idx: u32,
}

#[derive(Debug)]
struct StringDictionaryBuilder {
    ids: HashMap<String, u32>,
    entries: Vec<String>,
}

impl StringDictionaryBuilder {
    fn new() -> Self {
        let mut ids = HashMap::new();
        ids.insert(String::new(), 0);
        Self {
            ids,
            entries: vec![String::new()],
        }
    }

    fn intern(&mut self, value: &str) -> Result<u32, BuildError> {
        if let Some(existing) = self.ids.get(value) {
            return Ok(*existing);
        }

        let id = u32::try_from(self.entries.len()).map_err(|_| BuildError::BinCountOverflow)?;
        let owned = value.to_string();
        self.ids.insert(owned.clone(), id);
        self.entries.push(owned);
        Ok(id)
    }

    fn finalize(&self) -> Result<(Vec<u64>, Vec<u8>), BuildError> {
        let mut offsets = Vec::with_capacity(self.entries.len() + 1);
        let mut blob = Vec::new();
        offsets.push(0);

        for entry in &self.entries {
            blob.extend_from_slice(entry.as_bytes());
            offsets.push(u64::try_from(blob.len()).map_err(|_| BuildError::BinCountOverflow)?);
        }

        Ok((offsets, blob))
    }
}

fn encode_peak_value(genome_accession: &str, mz: f64) -> Result<u32, BuildError> {
    if !mz.is_finite() || mz < 0.0 {
        return Err(BuildError::InvalidPeakValue {
            genome: genome_accession.to_string(),
            value: mz,
        });
    }

    let encoded = (mz * 1000.0 + 0.5).floor();
    if encoded > u32::MAX as f64 {
        return Err(BuildError::PeakValueOverflow {
            genome: genome_accession.to_string(),
            value: mz,
        });
    }

    Ok(encoded as u32)
}

fn write_mass_index(
    path: &Path,
    bin_counts: &[u64],
    shard_spool: &mut ShardSpool,
) -> Result<u32, BuildError> {
    let bin_count_usize = bin_counts.len().max(1);
    let bin_count = u32::try_from(bin_count_usize).map_err(|_| BuildError::BinCountOverflow)?;

    let mut offsets = Vec::with_capacity(bin_count_usize + 1);
    let mut running = 0_u64;
    offsets.push(0);
    if bin_counts.is_empty() {
        offsets.push(0);
    } else {
        for count in bin_counts {
            running += *count;
            offsets.push(running);
        }
    }

    let postings_base = 4_u64 + (u64::from(bin_count) + 1) * 8;
    let postings_bytes = running * (std::mem::size_of::<Posting>() as u64);
    let total_len = postings_base + postings_bytes;

    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .truncate(true)
        .open(path)
        .map_err(|source| BuildError::CreateFile {
            path: path.to_path_buf(),
            source,
        })?;

    file.set_len(total_len)
        .map_err(|source| BuildError::WriteFile {
            path: path.to_path_buf(),
            source,
        })?;
    let mut mmap = unsafe { MmapMut::map_mut(&file) }.map_err(|source| BuildError::MapFile {
        path: path.to_path_buf(),
        source,
    })?;

    let mut header_cursor = 0;
    push_u32(&mut mmap, &mut header_cursor, bin_count);
    for offset in &offsets {
        push_u64(&mut mmap, &mut header_cursor, *offset);
    }

    let mut cursors = offsets[..bin_count_usize].to_vec();
    let postings_slice = &mut mmap[usize::try_from(postings_base).expect("postings base usize")..];
    let posting_size = std::mem::size_of::<Posting>();
    for shard_path in shard_spool.shard_paths() {
        let shard_file = File::open(shard_path).map_err(|source| BuildError::OpenMetadata {
            path: shard_path.clone(),
            source,
        })?;
        let mut reader = BufReader::new(shard_file);
        let mut record_bytes = [0_u8; 12];
        loop {
            match reader.read_exact(&mut record_bytes) {
                Ok(()) => {
                    let bin_id =
                        u32::from_le_bytes(record_bytes[0..4].try_into().expect("slice size"));
                    let genome_id =
                        u32::from_le_bytes(record_bytes[4..8].try_into().expect("slice size"));
                    let local_peak_idx =
                        u32::from_le_bytes(record_bytes[8..12].try_into().expect("slice size"));
                    let bin_index =
                        usize::try_from(bin_id).map_err(|_| BuildError::BinCountOverflow)?;
                    let cursor = &mut cursors[bin_index];
                    let position = usize::try_from(*cursor)
                        .map_err(|_| BuildError::BinCountOverflow)?
                        * posting_size;
                    postings_slice[position..position + 4]
                        .copy_from_slice(&genome_id.to_le_bytes());
                    postings_slice[position + 4..position + posting_size]
                        .copy_from_slice(&local_peak_idx.to_le_bytes());
                    *cursor += 1;
                }
                Err(source) if source.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(source) => {
                    return Err(BuildError::ReadFile {
                        path: shard_path.clone(),
                        source,
                    });
                }
            }
        }
    }

    mmap.flush().map_err(|source| BuildError::WriteFile {
        path: path.to_path_buf(),
        source,
    })?;

    Ok(bin_count)
}

fn write_meta(path: &Path, state: &BuildState) -> Result<(), BuildError> {
    let (name_offsets, name_blob) = state.name_dict.finalize()?;
    let (taxonomy_offsets, taxonomy_blob) = state.taxonomy_dict.finalize()?;

    let file = File::create(path).map_err(|source| BuildError::CreateFile {
        path: path.to_path_buf(),
        source,
    })?;
    let mut writer = BufWriter::with_capacity(IO_BUFFER_CAPACITY, file);

    for value in &state.genome_offsets {
        writer
            .write_all(&value.to_le_bytes())
            .map_err(|source| BuildError::WriteFile {
                path: path.to_path_buf(),
                source,
            })?;
    }
    for value in &state.gene_counts {
        writer
            .write_all(&value.to_le_bytes())
            .map_err(|source| BuildError::WriteFile {
                path: path.to_path_buf(),
                source,
            })?;
    }
    for value in &state.taxonomy_ids {
        writer
            .write_all(&value.to_le_bytes())
            .map_err(|source| BuildError::WriteFile {
                path: path.to_path_buf(),
                source,
            })?;
    }

    let name_dict_count =
        u32::try_from(state.name_dict.entries.len()).map_err(|_| BuildError::BinCountOverflow)?;
    let taxonomy_dict_count = u32::try_from(state.taxonomy_dict.entries.len())
        .map_err(|_| BuildError::BinCountOverflow)?;
    writer
        .write_all(&name_dict_count.to_le_bytes())
        .and_then(|_| writer.write_all(&taxonomy_dict_count.to_le_bytes()))
        .map_err(|source| BuildError::WriteFile {
            path: path.to_path_buf(),
            source,
        })?;

    for value in &name_offsets {
        writer
            .write_all(&value.to_le_bytes())
            .map_err(|source| BuildError::WriteFile {
                path: path.to_path_buf(),
                source,
            })?;
    }
    for value in &taxonomy_offsets {
        writer
            .write_all(&value.to_le_bytes())
            .map_err(|source| BuildError::WriteFile {
                path: path.to_path_buf(),
                source,
            })?;
    }
    for value in &state.genome_name_dict_ids {
        writer
            .write_all(&value.to_le_bytes())
            .map_err(|source| BuildError::WriteFile {
                path: path.to_path_buf(),
                source,
            })?;
    }
    for value in &state.genome_taxonomy_dict_ids {
        writer
            .write_all(&value.to_le_bytes())
            .map_err(|source| BuildError::WriteFile {
                path: path.to_path_buf(),
                source,
            })?;
    }

    writer
        .write_all(&(name_blob.len() as u64).to_le_bytes())
        .and_then(|_| writer.write_all(&(taxonomy_blob.len() as u64).to_le_bytes()))
        .and_then(|_| writer.write_all(&name_blob))
        .and_then(|_| writer.write_all(&taxonomy_blob))
        .and_then(|_| writer.flush())
        .map_err(|source| BuildError::WriteFile {
            path: path.to_path_buf(),
            source,
        })
}

fn crc32_file(path: &Path) -> Result<u32, BuildError> {
    let file = File::open(path).map_err(|source| BuildError::ReadFile {
        path: path.to_path_buf(),
        source,
    })?;
    let mmap = unsafe { Mmap::map(&file) }.map_err(|source| BuildError::MapFile {
        path: path.to_path_buf(),
        source,
    })?;
    let mut hasher = Hasher::new();
    hasher.update(&mmap);
    Ok(hasher.finalize())
}

#[derive(Debug, Clone, Copy)]
struct HeaderFields {
    bin_width_milli_mz: u32,
    genome_count: u64,
    total_peak_count: u64,
    mass_index_len: u64,
    genome_peaks_len: u64,
    meta_len: u64,
    crc32_mass_index: u32,
    crc32_genome_peaks: u32,
    crc32_meta: u32,
}

fn build_header_bytes(fields: HeaderFields) -> [u8; HEADER_SIZE] {
    let mass_index_offset = HEADER_SIZE as u64;
    let genome_peaks_offset = mass_index_offset + fields.mass_index_len;
    let meta_offset = genome_peaks_offset + fields.genome_peaks_len;

    let mut bytes = [0_u8; HEADER_SIZE];
    let mut cursor = 0;

    push_bytes(&mut bytes, &mut cursor, &MAGIC);
    push_u32(&mut bytes, &mut cursor, SCHEMA_VERSION);
    push_u32(&mut bytes, &mut cursor, fields.bin_width_milli_mz);
    push_u64(&mut bytes, &mut cursor, fields.genome_count);
    push_u64(&mut bytes, &mut cursor, fields.total_peak_count);
    push_u64(&mut bytes, &mut cursor, mass_index_offset);
    push_u64(&mut bytes, &mut cursor, fields.mass_index_len);
    push_u64(&mut bytes, &mut cursor, genome_peaks_offset);
    push_u64(&mut bytes, &mut cursor, fields.genome_peaks_len);
    push_u64(&mut bytes, &mut cursor, meta_offset);
    push_u64(&mut bytes, &mut cursor, fields.meta_len);
    push_u32(&mut bytes, &mut cursor, 0);
    push_u32(&mut bytes, &mut cursor, fields.crc32_mass_index);
    push_u32(&mut bytes, &mut cursor, fields.crc32_genome_peaks);
    push_u32(&mut bytes, &mut cursor, fields.crc32_meta);

    let crc32_header = crc32fast::hash(&bytes);
    bytes[80..84].copy_from_slice(&crc32_header.to_le_bytes());
    bytes
}

fn push_bytes(target: &mut [u8], cursor: &mut usize, bytes: &[u8]) {
    let end = *cursor + bytes.len();
    target[*cursor..end].copy_from_slice(bytes);
    *cursor = end;
}

fn push_u32(target: &mut [u8], cursor: &mut usize, value: u32) {
    push_bytes(target, cursor, &value.to_le_bytes());
}

fn push_u64(target: &mut [u8], cursor: &mut usize, value: u64) {
    push_bytes(target, cursor, &value.to_le_bytes());
}

#[derive(Debug)]
struct ProgressReporter {
    mode: ProgressMode,
    callback: Option<ProgressCallback>,
    progress_interval: Duration,
    last_genome_emit_at: Cell<Option<Instant>>,
    started_at: Instant,
}

impl ProgressReporter {
    fn new(
        mode: ProgressMode,
        callback: Option<ProgressCallback>,
        progress_interval: Duration,
    ) -> Self {
        Self {
            mode,
            callback,
            progress_interval,
            last_genome_emit_at: Cell::new(None),
            started_at: Instant::now(),
        }
    }

    fn emit(&self, event: BuildProgress) {
        if !self.should_emit(&event) {
            return;
        }
        if self.mode == ProgressMode::Jsonl {
            self.emit_jsonl(&event);
        }
        if let Some(callback) = self.callback.as_ref() {
            callback.invoke(event);
        }
    }

    fn should_emit(&self, event: &BuildProgress) -> bool {
        let BuildProgress::Genome { .. } = event else {
            return true;
        };

        if self.progress_interval.is_zero() {
            self.last_genome_emit_at.set(Some(Instant::now()));
            return true;
        }

        let now = Instant::now();
        match self.last_genome_emit_at.get() {
            None => {
                self.last_genome_emit_at.set(Some(now));
                true
            }
            Some(last_emit_at) if now.duration_since(last_emit_at) >= self.progress_interval => {
                self.last_genome_emit_at.set(Some(now));
                true
            }
            Some(_) => false,
        }
    }

    fn emit_jsonl(&self, event: &BuildProgress) {
        let elapsed_ms = self.started_at.elapsed().as_millis();
        match event {
            BuildProgress::Start { all_db } => {
                eprintln!(
                    "{{\"event\":\"start\",\"all_db\":\"{}\",\"elapsed_ms\":0}}",
                    json_escape(all_db.to_string_lossy().as_ref())
                );
            }
            BuildProgress::Genome {
                accession,
                genomes_processed,
                peaks_processed,
            } => {
                eprintln!(
                    "{{\"event\":\"genome\",\"genome_accession\":\"{}\",\"genomes_processed\":{},\"peaks_processed\":{},\"elapsed_ms\":{}}}",
                    json_escape(accession),
                    genomes_processed,
                    peaks_processed,
                    elapsed_ms
                );
            }
            BuildProgress::IndexStart => {
                eprintln!(
                    "{{\"event\":\"index_start\",\"elapsed_ms\":{}}}",
                    elapsed_ms
                );
            }
            BuildProgress::MetaStart => {
                eprintln!("{{\"event\":\"meta_start\",\"elapsed_ms\":{}}}", elapsed_ms);
            }
            BuildProgress::FinalizeStart => {
                eprintln!(
                    "{{\"event\":\"finalize_start\",\"elapsed_ms\":{}}}",
                    elapsed_ms
                );
            }
            BuildProgress::Done {
                genome_count,
                total_peak_count,
                bin_count,
            } => {
                eprintln!(
                    "{{\"event\":\"done\",\"genomes_processed\":{},\"peaks_processed\":{},\"bin_count\":{},\"elapsed_ms\":{}}}",
                    genome_count,
                    total_peak_count,
                    bin_count,
                    elapsed_ms
                );
            }
        }
    }
}

fn json_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn load_genes(path: &Path) -> Result<HashMap<String, u32>, BuildError> {
    let file = File::open(path).map_err(|source| BuildError::OpenMetadata {
        path: path.to_path_buf(),
        source,
    })?;
    let raw: HashMap<String, i64> = from_reader(BufReader::new(file), DeOptions::default())
        .map_err(|source| BuildError::DecodeMetadata {
            path: path.to_path_buf(),
            source,
        })?;

    Ok(raw
        .into_iter()
        .map(|(key, value)| (key, value.max(0) as u32))
        .collect())
}

fn load_names(path: &Path) -> Result<HashMap<String, String>, BuildError> {
    let file = File::open(path).map_err(|source| BuildError::OpenMetadata {
        path: path.to_path_buf(),
        source,
    })?;
    from_reader(BufReader::new(file), DeOptions::default()).map_err(|source| {
        BuildError::DecodeMetadata {
            path: path.to_path_buf(),
            source,
        }
    })
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum TaxonomyValue {
    Text(String),
    Record {
        #[serde(default)]
        id: i64,
        #[serde(default)]
        text: String,
    },
}

fn load_taxonomy(path: &Path) -> Result<HashMap<String, TaxonomyInfo>, BuildError> {
    let file = File::open(path).map_err(|source| BuildError::OpenMetadata {
        path: path.to_path_buf(),
        source,
    })?;
    let raw: HashMap<String, TaxonomyValue> =
        from_reader(BufReader::new(file), DeOptions::default()).map_err(|source| {
            BuildError::DecodeMetadata {
                path: path.to_path_buf(),
                source,
            }
        })?;

    Ok(raw
        .into_iter()
        .map(|(key, value)| {
            let info = match value {
                TaxonomyValue::Text(text) => TaxonomyInfo { id: 0, text },
                TaxonomyValue::Record { id, text } => TaxonomyInfo {
                    id: id.max(0) as u32,
                    text,
                },
            };
            (key, info)
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use tempfile::tempdir;

    #[test]
    fn progress_reporter_throttles_genome_events() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let callback = ProgressCallback::new({
            let events = Arc::clone(&events);
            move |event| {
                events.lock().expect("progress lock poisoned").push(event);
            }
        });
        let reporter =
            ProgressReporter::new(ProgressMode::None, Some(callback), Duration::from_secs(60));

        reporter.emit(BuildProgress::Genome {
            accession: "GCA_000000001.1".to_string(),
            genomes_processed: 1,
            peaks_processed: 16,
        });
        reporter.emit(BuildProgress::Genome {
            accession: "GCA_000000002.1".to_string(),
            genomes_processed: 2,
            peaks_processed: 32,
        });

        let events = events.lock().expect("progress lock poisoned");
        assert_eq!(events.len(), 1);
        assert!(matches!(
            events.first(),
            Some(BuildProgress::Genome {
                genomes_processed: 1,
                peaks_processed: 16,
                ..
            })
        ));
    }

    #[test]
    fn write_mass_index_writes_expected_offsets_and_postings() {
        let temp = tempdir().expect("create tempdir");
        let shard_dir = temp.path().join("shards");
        fs::create_dir_all(&shard_dir).expect("create shard dir");
        let mut shard_spool = ShardSpool::new(&shard_dir, 4, 1).expect("create shard spool");

        shard_spool.push(2, 10, 1).expect("push first posting");
        shard_spool.push(0, 20, 0).expect("push second posting");
        shard_spool.push(2, 11, 3).expect("push third posting");
        shard_spool.push(5, 30, 2).expect("push fourth posting");
        shard_spool.flush_all().expect("flush shard spool");

        let output = temp.path().join("mass_index.bin");
        let bin_counts = vec![1, 0, 2, 0, 0, 1];
        let bin_count =
            write_mass_index(&output, &bin_counts, &mut shard_spool).expect("write mass index");

        assert_eq!(bin_count, 6);

        let bytes = fs::read(&output).expect("read mass index");
        let parsed_bin_count =
            u32::from_le_bytes(bytes[0..4].try_into().expect("bin count slice size"));
        assert_eq!(parsed_bin_count, 6);

        let offset_bytes_end =
            4 + (usize::try_from(parsed_bin_count).expect("bin count usize") + 1) * 8;
        let offsets = bytes[4..offset_bytes_end]
            .chunks_exact(8)
            .map(|chunk| u64::from_le_bytes(chunk.try_into().expect("offset slice size")))
            .collect::<Vec<_>>();
        assert_eq!(offsets, vec![0, 1, 1, 3, 3, 3, 4]);

        let postings = bytes[offset_bytes_end..]
            .chunks_exact(std::mem::size_of::<Posting>())
            .map(|chunk| Posting {
                genome_id: u32::from_le_bytes(
                    chunk[0..4].try_into().expect("genome id slice size"),
                ),
                local_peak_idx: u32::from_le_bytes(
                    chunk[4..8].try_into().expect("local peak idx slice size"),
                ),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            postings,
            vec![
                Posting {
                    genome_id: 20,
                    local_peak_idx: 0,
                },
                Posting {
                    genome_id: 10,
                    local_peak_idx: 1,
                },
                Posting {
                    genome_id: 11,
                    local_peak_idx: 3,
                },
                Posting {
                    genome_id: 30,
                    local_peak_idx: 2,
                },
            ]
        );
    }
}
