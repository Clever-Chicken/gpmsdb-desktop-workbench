# GPMsDB Desktop Modernization

This repository modernizes the original GPMsDB command-line toolkit into a
cross-platform desktop application.

## Phases

1. Build a custom mmap-friendly binary database from the original pickle assets.
2. Implement a Rust runtime for coarse screening and reranked identification.
3. Integrate the runtime into a Tauri desktop application.
