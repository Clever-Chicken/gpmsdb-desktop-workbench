# Repository Guidelines

## Project Structure & Module Organization
This repository is a multi-language workspace for the GPMsDB desktop project. Core Rust crates live in `crates/`: `gpmsdb-builder` builds mmap database artifacts, `gpmsdb-format` reads them, and `gpmsdb-engine` runs identification logic. The desktop app lives in `apps/desktop`, with React/Vite UI code in `src/` and Tauri backend code in `src-tauri/src/`. Python conversion helpers are under `tools/pickle_to_mmap`, and Python tests live in `tests/`. Architecture and user docs are in `docs/`. Large local datasets in `数据/`, generated outputs in `数据输出目录/`, and scratch builds in `.tmp/` should be treated as local assets, not routine code changes.

## Build, Test, and Development Commands
- `cargo test --workspace`: run Rust tests across all crates.
- `cargo run -p gpmsdb-builder -- --source-root tests/fixtures/small_source --out-dir .tmp/small-db --progress jsonl`: build a small local database fixture.
- `pytest`: run Python tests in `tests/`.
- `corepack pnpm --dir apps/desktop test`: run Vitest UI tests.
- `corepack pnpm --dir apps/desktop tauri dev`: start the desktop app in development mode.
- `corepack pnpm --dir apps/desktop build`: produce the frontend bundle used by Tauri packaging.

## Coding Style & Naming Conventions
Use Rust stable (`rust-toolchain.toml`) and format Rust code with `cargo fmt --all`; use `snake_case` for modules/functions and `PascalCase` for structs/enums. TypeScript/React files use 2-space indentation, `PascalCase` component names such as `BatchRunPanel.tsx`, and colocated helper types under `src/lib/`. Python follows PEP 8 with 4-space indentation, type hints, and focused module names such as `convert_gpmsdb.py`.

## Testing Guidelines
Add Rust tests close to the crate they cover, and keep Python tests under `tests/tools/test_*.py`. Prefer the small fixture set in `tests/fixtures/small_source` over full data dumps. For UI changes, add or update Vitest tests such as `apps/desktop/src/App.test.tsx`. Before opening a PR, run the relevant Rust, Python, and desktop test commands for the areas you touched.

## Commit & Pull Request Guidelines
Recent history uses Conventional Commit prefixes like `feat:`, `test:`, and `chore:`. Keep commits scoped to one concern and mention the affected area when useful, for example `feat: add coarse rerank cutoff`. PRs should include a short summary, the verification commands you ran, and screenshots for desktop UI changes. Do not include generated `target/`, `node_modules/`, or scratch `.tmp/` artifacts unless the change explicitly updates a checked-in fixture or build output policy.
