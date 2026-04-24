import { invoke } from "@tauri-apps/api/core";
import type { BatchResultRow } from "./events";

export async function openDatabase(path: string): Promise<void> {
  await invoke<void>("open_database", { path });
}
export async function buildDatabaseFromSource(sourcePath: string, outputDir: string): Promise<string> {
  return await invoke<string>("build_database_from_source", { sourcePath, outputDir });
}
export async function runIdentification(queryPaths: string[]): Promise<void> {
  await invoke<void>("run_identification", { queryPaths });
}
export async function exportDatabaseToCsv(outputPath: string): Promise<void> {
  await invoke<void>("export_database_to_csv", { outputPath });
}
export async function exportResultsToCsv(rows: BatchResultRow[], destination: string): Promise<void> {
  await invoke<void>("export_results_to_csv", { rows, destination });
}
