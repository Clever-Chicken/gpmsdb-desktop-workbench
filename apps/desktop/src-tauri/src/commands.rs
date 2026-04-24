use std::{
    fs::File,
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};

use gpmsdb_builder::{build_database, BuildOptions, BuildProgress, ProgressCallback, ProgressMode};
use gpmsdb_engine::{identify, QueryPeak};
use gpmsdb_format::MappedDatabase;
use serde::Deserialize;
use tauri::{AppHandle, Emitter, State};

use crate::{
    events::{BatchProgressEvent, BatchResultRow, BatchResultsEvent, BuildProgressEvent},
    state::AppState,
};

const COARSE_LIMIT: usize = 8;
const PPM_TOLERANCE: u32 = 50;
const TOP_RESULTS_PER_QUERY: usize = 8;
const IO_BUFFER_CAPACITY: usize = 8 * 1024 * 1024;

impl<'de> Deserialize<'de> for BatchResultRow {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct BatchResultRowInput {
            query_file: String,
            genome_id: u32,
            score: f32,
            matched_ribosomal: u32,
            matched_total: u32,
        }

        let row = BatchResultRowInput::deserialize(deserializer)?;
        Ok(Self {
            query_file: row.query_file,
            genome_id: row.genome_id,
            score: row.score,
            matched_ribosomal: row.matched_ribosomal,
            matched_total: row.matched_total,
        })
    }
}

#[tauri::command]
pub fn open_database(path: String, state: State<'_, AppState>) -> Result<(), String> {
    open_database_impl(path, state.inner())
}

#[tauri::command]
pub async fn build_database_from_source(
    app: AppHandle,
    state: State<'_, AppState>,
    source_path: String,
    output_dir: String,
) -> Result<String, String> {
    let emit_app = app.clone();
    let output_dir = tauri::async_runtime::spawn_blocking(move || {
        let callback = ProgressCallback::new(move |event| {
            let payload = map_build_progress(event);
            if let Err(error) = emit_app.emit("build-progress", payload) {
                eprintln!("failed to emit build-progress: {error}");
            }
        });
        build_database(&BuildOptions {
            source_root: PathBuf::from(&source_path),
            out_dir: PathBuf::from(&output_dir),
            progress: ProgressMode::None,
            progress_callback: Some(callback),
            ..BuildOptions::default()
        })
        .map(|_| output_dir)
        .map_err(|error| error.to_string())
    })
    .await
    .map_err(|error| error.to_string())??;

    open_database_impl(output_dir.clone(), state.inner())?;
    Ok(output_dir)
}

fn map_build_progress(event: BuildProgress) -> BuildProgressEvent {
    match event {
        BuildProgress::Start { all_db } => BuildProgressEvent::Start {
            all_db: all_db.to_string_lossy().into_owned(),
        },
        BuildProgress::Genome {
            accession,
            genomes_processed,
            peaks_processed,
        } => BuildProgressEvent::Genome {
            accession,
            genomes_processed,
            peaks_processed,
        },
        BuildProgress::IndexStart => BuildProgressEvent::IndexStart,
        BuildProgress::MetaStart => BuildProgressEvent::MetaStart,
        BuildProgress::FinalizeStart => BuildProgressEvent::FinalizeStart,
        BuildProgress::Done {
            genome_count,
            total_peak_count,
            bin_count,
        } => BuildProgressEvent::Done {
            genome_count,
            total_peak_count,
            bin_count,
        },
    }
}

#[tauri::command]
pub async fn run_identification(
    app: AppHandle,
    state: State<'_, AppState>,
    query_paths: Vec<String>,
) -> Result<(), String> {
    let database_path = resolve_database_path(state.inner())?;
    if query_paths.is_empty() {
        return Err("no query files selected".to_string());
    }

    tauri::async_runtime::spawn(async move {
        let progress_app = app.clone();
        let results_app = app.clone();
        let worker = tauri::async_runtime::spawn_blocking(move || {
            run_identification_job(&database_path, &query_paths, |event| {
                if let Err(error) = progress_app.emit("batch-progress", event) {
                    eprintln!("failed to emit batch-progress: {error}");
                }
            })
        });

        match worker.await {
            Ok(Ok(results)) => {
                if let Err(error) =
                    results_app.emit("batch-results", BatchResultsEvent { rows: results })
                {
                    eprintln!("failed to emit batch-results: {error}");
                }
            }
            Ok(Err(error)) => eprintln!("batch identification failed: {error}"),
            Err(error) => eprintln!("batch worker join failed: {error}"),
        }
    });

    Ok(())
}

#[tauri::command]
pub async fn export_database_to_csv(
    state: State<'_, AppState>,
    output_path: String,
) -> Result<(), String> {
    let database_path = resolve_database_path(state.inner())?;
    tauri::async_runtime::spawn_blocking(move || {
        export_database_to_csv_job(&database_path, Path::new(&output_path))
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
pub async fn export_results_to_csv(
    rows: Vec<BatchResultRow>,
    destination: String,
) -> Result<(), String> {
    tauri::async_runtime::spawn_blocking(move || {
        export_results_to_csv_job(&rows, Path::new(&destination))
    })
    .await
    .map_err(|error| error.to_string())?
}

pub fn open_database_for_test(path: String, state: &AppState) -> Result<(), String> {
    open_database_impl(path, state)
}

pub fn build_database_from_source_for_test(
    state: &AppState,
    source_path: String,
    output_dir: String,
) -> Result<String, String> {
    build_database(&BuildOptions {
        source_root: PathBuf::from(&source_path),
        out_dir: PathBuf::from(&output_dir),
        progress: ProgressMode::None,
        ..BuildOptions::default()
    })
    .map_err(|error| error.to_string())?;

    open_database_impl(output_dir.clone(), state)?;
    Ok(output_dir)
}

#[derive(Debug)]
pub struct IdentificationRun {
    pub progress: Vec<BatchProgressEvent>,
    pub results: Vec<BatchResultRow>,
}

pub fn run_identification_for_test(
    state: &AppState,
    query_paths: Vec<String>,
) -> Result<IdentificationRun, String> {
    let database_path = resolve_database_path(state)?;
    let progress = Mutex::new(Vec::new());
    let results = run_identification_job(&database_path, &query_paths, |event| {
        progress
            .lock()
            .expect("desktop progress lock poisoned")
            .push(event);
    })?;

    Ok(IdentificationRun {
        progress: progress
            .into_inner()
            .expect("desktop progress lock poisoned"),
        results,
    })
}

pub fn export_database_to_csv_for_test(
    state: &AppState,
    output_path: String,
) -> Result<(), String> {
    let database_path = resolve_database_path(state)?;
    export_database_to_csv_job(&database_path, Path::new(&output_path))
}

fn open_database_impl(path: String, state: &AppState) -> Result<(), String> {
    let path = PathBuf::from(path);
    MappedDatabase::open(&path).map_err(|error| error.to_string())?;
    state.set_database_path(path);
    Ok(())
}

fn resolve_database_path(state: &AppState) -> Result<PathBuf, String> {
    state
        .database_path()
        .ok_or_else(|| "database not loaded".to_string())
}

fn run_identification_job<F>(
    database_path: &Path,
    query_paths: &[String],
    on_progress: F,
) -> Result<Vec<BatchResultRow>, String>
where
    F: Fn(BatchProgressEvent) + Sync + Send,
{
    if query_paths.is_empty() {
        return Err("no query files selected".to_string());
    }

    let db = MappedDatabase::open(database_path).map_err(|error| error.to_string())?;
    let total = query_paths.len();
    let mut rows = Vec::new();

    on_progress(BatchProgressEvent {
        processed: 0,
        total,
        current_file: None,
    });

    for (index, query_path) in query_paths.iter().enumerate() {
        let query_file = display_name_for_path(query_path);
        let query = load_query_file(Path::new(query_path))?;
        let results = identify(&db, &query, COARSE_LIMIT, PPM_TOLERANCE);

        rows.extend(
            results
                .into_iter()
                .take(TOP_RESULTS_PER_QUERY)
                .map(|result| BatchResultRow {
                    query_file: query_file.clone(),
                    genome_id: result.genome_id,
                    score: result.score,
                    matched_ribosomal: result.matched_ribosomal,
                    matched_total: result.matched_total,
                }),
        );

        on_progress(BatchProgressEvent {
            processed: index + 1,
            total,
            current_file: Some(query_file),
        });
    }

    Ok(rows)
}

fn load_query_file(path: &Path) -> Result<Vec<QueryPeak>, String> {
    let file = File::open(path)
        .map_err(|error| format!("failed to open query file {}: {error}", path.display()))?;
    let reader = BufReader::new(file);
    let mut peaks = Vec::new();

    for (line_no, line) in reader.lines().enumerate() {
        let line = line.map_err(|error| {
            format!("failed to read {}:{}: {error}", path.display(), line_no + 1)
        })?;
        let Some((mz, intensity)) = parse_peak_line(&line) else {
            continue;
        };
        peaks.push(QueryPeak {
            milli_mz: encode_query_mz(path, line_no + 1, mz)?,
            intensity: encode_query_intensity(path, line_no + 1, intensity)?,
        });
    }

    if peaks.is_empty() {
        return Err(format!("no peak lines parsed from {}", path.display()));
    }

    Ok(peaks)
}

fn parse_peak_line(line: &str) -> Option<(f64, f64)> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with("COM=") {
        return None;
    }

    let fields = trimmed
        .split(|ch: char| ch.is_whitespace() || ch == ',')
        .filter(|field| !field.is_empty())
        .take(2)
        .collect::<Vec<_>>();
    if fields.is_empty() {
        return None;
    }

    let mz = fields[0].parse::<f64>().ok()?;
    let intensity = if fields.len() >= 2 {
        fields[1].parse::<f64>().ok()?
    } else {
        1.0
    };

    Some((mz, intensity))
}

fn encode_query_mz(path: &Path, line_no: usize, mz: f64) -> Result<u32, String> {
    if !mz.is_finite() || mz < 0.0 {
        return Err(format!(
            "invalid m/z at {}:{}: {mz}",
            path.display(),
            line_no
        ));
    }

    let encoded = (mz * 1000.0 + 0.5).floor();
    if encoded > u32::MAX as f64 {
        return Err(format!(
            "m/z overflow at {}:{}: {mz}",
            path.display(),
            line_no
        ));
    }

    Ok(encoded as u32)
}

fn encode_query_intensity(path: &Path, line_no: usize, intensity: f64) -> Result<f32, String> {
    if !intensity.is_finite() || intensity < 0.0 || intensity > f32::MAX as f64 {
        return Err(format!(
            "invalid intensity at {}:{}: {intensity}",
            path.display(),
            line_no
        ));
    }

    Ok(intensity as f32)
}

fn export_database_to_csv_job(database_path: &Path, output_path: &Path) -> Result<(), String> {
    let db = MappedDatabase::open(database_path).map_err(|error| error.to_string())?;
    let file = File::create(output_path)
        .map_err(|error| format!("failed to create {}: {error}", output_path.display()))?;
    let mut writer = BufWriter::with_capacity(IO_BUFFER_CAPACITY, file);
    writer
        .write_all(b"genome_id,display_name,taxonomy,total_peaks,gene_count\n")
        .map_err(|error| format!("failed to write {}: {error}", output_path.display()))?;

    for genome_index in 0..db.header().genome_count {
        let genome_id = u32::try_from(genome_index)
            .map_err(|_| "genome id overflow while exporting".to_string())?;
        let metadata = db
            .genome_metadata(genome_id)
            .ok_or_else(|| format!("missing metadata for genome {genome_id}"))?;
        let line = format!(
            "{},{},{},{},{}\n",
            metadata.genome_id,
            csv_escape(metadata.display_name),
            csv_escape(metadata.taxonomy),
            metadata.total_peaks,
            metadata.gene_count
        );
        writer
            .write_all(line.as_bytes())
            .map_err(|error| format!("failed to write {}: {error}", output_path.display()))?;
    }

    writer
        .flush()
        .map_err(|error| format!("failed to flush {}: {error}", output_path.display()))
}

fn export_results_to_csv_job(rows: &[BatchResultRow], output_path: &Path) -> Result<(), String> {
    let file = File::create(output_path)
        .map_err(|error| format!("failed to create {}: {error}", output_path.display()))?;
    let mut writer = BufWriter::with_capacity(IO_BUFFER_CAPACITY, file);
    writer
        .write_all(b"query_file,genome_id,score,matched_ribosomal,matched_total\n")
        .map_err(|error| format!("failed to write {}: {error}", output_path.display()))?;

    for row in rows {
        let line = format!(
            "{},{},{:.6},{},{}\n",
            csv_escape(&row.query_file),
            row.genome_id,
            row.score,
            row.matched_ribosomal,
            row.matched_total
        );
        writer
            .write_all(line.as_bytes())
            .map_err(|error| format!("failed to write {}: {error}", output_path.display()))?;
    }

    writer
        .flush()
        .map_err(|error| format!("failed to flush {}: {error}", output_path.display()))
}

fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn display_name_for_path(path: &str) -> String {
    Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| path.to_string())
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::Path,
        time::{SystemTime, UNIX_EPOCH},
    };

    use crate::events::BatchResultRow;

    use super::{export_results_to_csv_job, load_query_file};

    #[test]
    fn load_query_file_accepts_author_text_format_without_intensity_column() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("gpmsdb-query-{unique}.txt"));
        fs::write(&path, "# comment\nCOM=demo-spectrum\n1000\n1200 7.5\n")
            .expect("write query fixture");

        let peaks = load_query_file(&path).expect("author text peak list should parse");

        assert_eq!(peaks.len(), 2);
        assert_eq!(peaks[0].milli_mz, 1_000_000);
        assert_eq!(peaks[0].intensity, 1.0);
        assert_eq!(peaks[1].milli_mz, 1_200_000);
        assert_eq!(peaks[1].intensity, 7.5);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn export_results_to_csv_writes_header_and_rows() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("gpmsdb-results-{unique}.csv"));
        let rows = vec![
            BatchResultRow {
                query_file: "plain.txt".to_string(),
                genome_id: 7,
                score: 0.5,
                matched_ribosomal: 3,
                matched_total: 9,
            },
            BatchResultRow {
                query_file: "needs,\"escape\"\n.txt".to_string(),
                genome_id: 12,
                score: 1.2345678,
                matched_ribosomal: 4,
                matched_total: 10,
            },
        ];

        export_results_to_csv_job(&rows, Path::new(&path)).expect("results csv should export");

        let contents = fs::read_to_string(&path).expect("read exported results csv");
        assert_eq!(
            contents,
            concat!(
                "query_file,genome_id,score,matched_ribosomal,matched_total\n",
                "plain.txt,7,0.500000,3,9\n",
                "\"needs,\"\"escape\"\"\n.txt\",12,1.234568,4,10\n"
            )
        );

        let _ = fs::remove_file(path);
    }
}
