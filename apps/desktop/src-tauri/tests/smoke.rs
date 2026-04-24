#[test]
fn command_state_starts_without_loaded_database() {
    let state = gpmsdb_desktop_tauri::state::AppState::default();
    assert!(state.database_path().is_none());
}

#[test]
fn desktop_backend_can_open_db_and_schedule_job() {
    let fixture_dir =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../.tmp/small-db");
    let query_a =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/query-a.mgf");
    let query_b =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/query-b.txt");
    let state = gpmsdb_desktop_tauri::state::AppState::default();

    assert!(state.database_path().is_none());

    gpmsdb_desktop_tauri::commands::open_database_for_test(
        fixture_dir.to_string_lossy().into_owned(),
        &state,
    )
    .expect("open_database should accept the small-db fixture");

    assert_eq!(state.database_path(), Some(fixture_dir.clone()));

    let run = gpmsdb_desktop_tauri::commands::run_identification_for_test(
        &state,
        vec![
            query_a.to_string_lossy().into_owned(),
            query_b.to_string_lossy().into_owned(),
        ],
    )
    .expect("run_identification should schedule successfully");
    assert!(run.progress.len() >= 3);
    assert_eq!(run.progress.first().unwrap().processed, 0);
    assert_eq!(run.progress.first().unwrap().total, 2);
    assert_eq!(
        run.progress.last().unwrap().processed,
        run.progress.last().unwrap().total
    );
    assert!(run.results.len() >= 3);
    assert_eq!(run.results[0].query_file, "query-a.mgf");
    assert_eq!(run.results[0].genome_id, 0);
    assert!(run
        .results
        .iter()
        .any(|row| row.query_file == "query-b.txt" && row.genome_id == 1));
}

#[test]
fn desktop_backend_can_build_runtime_database_from_raw_all_db() {
    let source_all_db = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../tests/fixtures/small_source/all.db");
    let out_dir = tempfile::tempdir().expect("create temp output dir");
    let state = gpmsdb_desktop_tauri::state::AppState::default();

    let built_path = gpmsdb_desktop_tauri::commands::build_database_from_source_for_test(
        &state,
        source_all_db.to_string_lossy().into_owned(),
        out_dir.path().to_string_lossy().into_owned(),
    )
    .expect("build_database_from_source should materialize a runtime database");

    assert_eq!(built_path, out_dir.path().to_string_lossy());
    assert_eq!(state.database_path(), Some(out_dir.path().to_path_buf()));

    let db = gpmsdb_format::MappedDatabase::open(out_dir.path())
        .expect("built runtime database should open");
    assert_eq!(db.header().genome_count, 2);
    assert_eq!(db.genome_peaks(0), Some(&[1_000_000, 1_200_000][..]));
}

#[test]
fn desktop_backend_can_build_runtime_database_from_related_db_file() {
    let source_db = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../tests/fixtures/small_source/genes.db");
    let out_dir = tempfile::tempdir().expect("create temp output dir");
    let state = gpmsdb_desktop_tauri::state::AppState::default();

    let built_path = gpmsdb_desktop_tauri::commands::build_database_from_source_for_test(
        &state,
        source_db.to_string_lossy().into_owned(),
        out_dir.path().to_string_lossy().into_owned(),
    )
    .expect("related db file should be enough to discover the source package");

    assert_eq!(built_path, out_dir.path().to_string_lossy());
    assert_eq!(state.database_path(), Some(out_dir.path().to_path_buf()));
}

#[test]
fn desktop_backend_can_export_database_to_csv() {
    let fixture_dir =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../.tmp/small-db");
    let export_path = std::env::temp_dir().join("gpmsdb-export-smoke.csv");
    let state = gpmsdb_desktop_tauri::state::AppState::default();

    gpmsdb_desktop_tauri::commands::open_database_for_test(
        fixture_dir.to_string_lossy().into_owned(),
        &state,
    )
    .expect("open_database should accept the small-db fixture");

    gpmsdb_desktop_tauri::commands::export_database_to_csv_for_test(
        &state,
        export_path.to_string_lossy().into_owned(),
    )
    .expect("export_database_to_csv should succeed");

    let csv = std::fs::read_to_string(&export_path).expect("exported csv should be readable");
    assert!(csv.contains("genome_id,display_name,taxonomy,total_peaks,gene_count"));
    assert!(csv.contains("0,Genome Zero,d__Bacteria;p__Shared,2,900"));
    assert!(csv.contains("1,Genome One,d__Bacteria;p__Shared,2,1100"));

    let _ = std::fs::remove_file(export_path);
}
