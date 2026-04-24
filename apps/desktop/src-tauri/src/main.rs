#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    gpmsdb_desktop_tauri::run();
}
