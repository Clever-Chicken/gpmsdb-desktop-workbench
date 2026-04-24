pub mod commands;
pub mod events;
pub mod state;

pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .manage(state::AppState::default())
        .invoke_handler(tauri::generate_handler![
            commands::open_database,
            commands::build_database_from_source,
            commands::run_identification,
            commands::export_database_to_csv,
            commands::export_results_to_csv
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
