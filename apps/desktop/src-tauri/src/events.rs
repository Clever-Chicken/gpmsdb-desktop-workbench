use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchProgressEvent {
    pub processed: usize,
    pub total: usize,
    pub current_file: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "stage", rename_all = "camelCase")]
pub enum BuildProgressEvent {
    #[serde(rename_all = "camelCase")]
    Start {
        all_db: String,
    },
    #[serde(rename_all = "camelCase")]
    Genome {
        accession: String,
        genomes_processed: u64,
        peaks_processed: u64,
    },
    IndexStart,
    MetaStart,
    FinalizeStart,
    #[serde(rename_all = "camelCase")]
    Done {
        genome_count: u64,
        total_peak_count: u64,
        bin_count: u32,
    },
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchResultRow {
    pub query_file: String,
    pub genome_id: u32,
    pub score: f32,
    pub matched_ribosomal: u32,
    pub matched_total: u32,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchResultsEvent {
    pub rows: Vec<BatchResultRow>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LogEvent {
    pub level: LogLevel,
    pub message: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Info,
    Warn,
    Error,
}
